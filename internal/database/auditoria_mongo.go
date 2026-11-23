package database

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"api-auditoria/internal/dominio"
	"api-auditoria/internal/service"
)

// O mesmo tipo serve de "gravador" para a projeção e de leitor para a API.
var (
	_ service.RepositorioAuditoria = (*AuditoriaMongo)(nil)
	_ service.LeituraAuditoria     = (*AuditoriaMongo)(nil)
)

// ConectarMongo abre o cliente e testa a conexão.
// Quem chama é responsável por cliente.Disconnect(ctx).
// Um cliente serve para vários bancos: cliente.Database("outro") quando for
// preciso separar estruturas em bancos diferentes.
func ConectarMongo(ctx context.Context, uri string) (*mongo.Client, error) {
	cliente, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("conectar no mongo: %w", err)
	}

	ctxPing, cancelar := context.WithTimeout(ctx, 10*time.Second)
	defer cancelar()
	if err := cliente.Ping(ctxPing, nil); err != nil {
		return nil, fmt.Errorf("mongo não respondeu: %w", err)
	}
	return cliente, nil
}

// AuditoriaMongo é o lado de LEITURA da auditoria (coleção "historico").
type AuditoriaMongo struct {
	colecao *mongo.Collection
}

// NovaAuditoriaMongo cria os índices (operação idempotente) e devolve o repositório.
func NovaAuditoriaMongo(ctx context.Context, banco *mongo.Database) (*AuditoriaMongo, error) {
	colecao := banco.Collection("historico")

	// Um índice para cada forma de consulta da API. Toda consulta tem o
	// município (o escopo é obrigatório), então ele vem SEMPRE na frente:
	// cada município lê só a "fatia" dele do índice.
	indices := []mongo.IndexModel{
		{Keys: bson.D{{Key: "municipio_id", Value: 1}, {Key: "entidade", Value: 1}, {Key: "entidade_id", Value: 1}, {Key: "ocorrido_em", Value: 1}}},
		{Keys: bson.D{{Key: "municipio_id", Value: 1}, {Key: "usuario_id", Value: 1}, {Key: "ocorrido_em", Value: -1}}},
		{Keys: bson.D{{Key: "municipio_id", Value: 1}, {Key: "acao", Value: 1}, {Key: "ocorrido_em", Value: -1}}},
		{Keys: bson.D{{Key: "municipio_id", Value: 1}, {Key: "alertas", Value: 1}, {Key: "ocorrido_em", Value: -1}}},
		{Keys: bson.D{{Key: "municipio_id", Value: 1}, {Key: "ocorrido_em", Value: -1}}},
	}
	if _, err := colecao.Indexes().CreateMany(ctx, indices); err != nil {
		return nil, fmt.Errorf("mongo: criar índices: %w", err)
	}
	return &AuditoriaMongo{colecao: colecao}, nil
}

// Os formatos no Mongo ficam separados de dominio.Registro para que detalhes
// do banco (tags bson, _id) não vazem para o domínio.

// CamposDocumento são os campos comuns à gravação e à leitura.
//
// municipio_id NÃO tem omitempty de propósito: a ação de plataforma é gravada
// com "" e o filtro do escopo de plataforma procura exatamente "".
type CamposDocumento struct {
	ID           string    `bson:"_id"`
	Acao         string    `bson:"acao"`
	Entidade     string    `bson:"entidade"`
	EntidadeID   string    `bson:"entidade_id"`
	UsuarioID    string    `bson:"usuario_id"`
	TipoUsuario  string    `bson:"tipo_usuario,omitempty"`
	MunicipioID  string    `bson:"municipio_id"`
	OcorridoEm   time.Time `bson:"ocorrido_em"`
	Motivo       string    `bson:"motivo,omitempty"`
	Origem       string    `bson:"origem,omitempty"`
	RequisicaoID string    `bson:"requisicao_id,omitempty"`
	Alertas      []string  `bson:"alertas,omitempty"`
	RecebidoEm   time.Time `bson:"recebido_em"`
}

// documentoGravado: dados vai como documento de verdade, consultável no Mongo
// (ex.: db.historico.find({"dados.depois.situacao": "aprovada"})).
//
// O ",inline" faz os campos de CamposDocumento ficarem no mesmo nível do
// documento, em vez de dentro de um subdocumento.
type documentoGravado struct {
	CamposDocumento `bson:",inline"`
	Dados           any `bson:"dados,omitempty"`
}

// documentoLido: dados vem como BSON cru, para ser convertido de volta em
// JSON sem perder a estrutura.
type documentoLido struct {
	CamposDocumento `bson:",inline"`
	Dados           bson.Raw `bson:"dados,omitempty"`
}

// Gravar faz upsert pelo _id: reprocessar a mesma mensagem não duplica.
func (a *AuditoriaMongo) Gravar(ctx context.Context, r dominio.Registro) error {
	doc, err := paraDocumento(r)
	if err != nil {
		return err
	}

	_, err = a.colecao.ReplaceOne(ctx, bson.D{{Key: "_id", Value: r.ID}}, doc,
		options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("mongo: gravar registro: %w", err)
	}
	return nil
}

func paraDocumento(r dominio.Registro) (documentoGravado, error) {
	doc := documentoGravado{CamposDocumento: CamposDocumento{
		ID:           r.ID,
		Acao:         r.Acao,
		Entidade:     r.Entidade,
		EntidadeID:   r.EntidadeID,
		UsuarioID:    r.UsuarioID,
		TipoUsuario:  r.TipoUsuario,
		MunicipioID:  r.MunicipioID,
		OcorridoEm:   r.OcorridoEm.UTC(),
		Motivo:       r.Motivo,
		Origem:       r.Origem,
		RequisicaoID: r.RequisicaoID,
		Alertas:      r.Alertas,
		RecebidoEm:   r.RecebidoEm,
	}}
	if len(r.Dados) > 0 {
		// JSON -> map[string]any -> documento BSON. O domínio já garantiu
		// que dados é um objeto JSON válido.
		// UseNumber evita que todo número vire float64 (1500 voltaria 1500.0).
		decodificador := json.NewDecoder(bytes.NewReader(r.Dados))
		decodificador.UseNumber()
		var bruto any
		if err := decodificador.Decode(&bruto); err != nil {
			return doc, fmt.Errorf("mongo: converter dados: %w", err)
		}
		doc.Dados = preservarNumeros(bruto)
	}
	return doc, nil
}

// preservarNumeros percorre o JSON decodificado e troca cada json.Number por
// int64 (se for inteiro) ou float64. Assim o Mongo guarda o tipo certo.
//
// "switch valor := v.(type)" é um type switch: descobre o tipo real guardado
// numa variável "any" e já entrega o valor convertido para esse tipo.
func preservarNumeros(v any) any {
	switch valor := v.(type) {
	case map[string]any:
		for chave, item := range valor {
			valor[chave] = preservarNumeros(item)
		}
		return valor
	case []any:
		for i, item := range valor {
			valor[i] = preservarNumeros(item)
		}
		return valor
	case json.Number:
		if inteiro, err := valor.Int64(); err == nil {
			return inteiro
		}
		if decimal, err := valor.Float64(); err == nil {
			return decimal
		}
		return valor.String() // número fora da faixa: guarda como texto para não perder
	default:
		return v // string, bool, nil: já estão certos
	}
}

func (a *AuditoriaMongo) Buscar(ctx context.Context, f dominio.Filtro) ([]dominio.Registro, error) {
	// Os valores entram como string no filtro (nunca como documento vindo do
	// cliente), o que impede injeção de operadores como {"$ne": ...}.
	//
	// O escopo vem PRIMEIRO e sempre: é ele que separa os municípios.
	consulta := bson.D{filtroDoEscopo(f.Escopo)}
	adicionar := func(campo, valor string) { // função anônima ("closure") que enxerga a variável consulta
		if valor != "" {
			consulta = append(consulta, bson.E{Key: campo, Value: valor})
		}
	}
	adicionar("usuario_id", f.UsuarioID)
	adicionar("entidade", f.Entidade)
	adicionar("entidade_id", f.EntidadeID)
	adicionar("acao", f.Acao)
	if f.ComAlerta {
		consulta = append(consulta, bson.E{Key: "alertas.0", Value: bson.D{{Key: "$exists", Value: true}}})
	}
	if periodo := faixaDeDatas(f); len(periodo) > 0 {
		consulta = append(consulta, bson.E{Key: "ocorrido_em", Value: periodo})
	}

	ordem := -1 // mais recente primeiro
	if f.Crescente {
		ordem = 1 // linha do tempo: mais antigo primeiro
	}
	opcoes := options.Find().
		SetSort(bson.D{{Key: "ocorrido_em", Value: ordem}, {Key: "_id", Value: 1}}).
		SetSkip(int64((f.Pagina - 1) * f.Limite)).
		SetLimit(int64(f.Limite))

	cursor, err := a.colecao.Find(ctx, consulta, opcoes)
	if err != nil {
		return nil, fmt.Errorf("mongo: buscar registros: %w", err)
	}

	var docs []documentoLido
	if err := cursor.All(ctx, &docs); err != nil { // All também fecha o cursor
		return nil, fmt.Errorf("mongo: ler registros: %w", err)
	}

	// make cria a fatia vazia (len 0) com a capacidade certa: a API devolve
	// [] e não null quando não há resultados.
	registros := make([]dominio.Registro, 0, len(docs))
	for _, d := range docs {
		r, err := d.paraDominio()
		if err != nil {
			return nil, err
		}
		registros = append(registros, r)
	}
	return registros, nil
}

// BuscarPorID procura pelo id E pelo escopo juntos: registro de outro
// município simplesmente não é encontrado.
func (a *AuditoriaMongo) BuscarPorID(ctx context.Context, id string, escopo dominio.Escopo) (dominio.Registro, error) {
	var doc documentoLido
	err := a.colecao.FindOne(ctx, bson.D{{Key: "_id", Value: id}, filtroDoEscopo(escopo)}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return dominio.Registro{}, dominio.ErrNaoEncontrada
	}
	if err != nil {
		return dominio.Registro{}, fmt.Errorf("mongo: buscar registro %s: %w", id, err)
	}
	return doc.paraDominio()
}

// filtroDoEscopo traduz o escopo em condição do Mongo. Plataforma = registros
// gravados com municipio_id "" (ações sem município). O service já garantiu
// que o escopo é válido; mesmo assim, um escopo vazio aqui vira "município ”",
// que só enxerga plataforma — nunca "todos".
func filtroDoEscopo(e dominio.Escopo) bson.E {
	if e.Plataforma {
		return bson.E{Key: "municipio_id", Value: ""}
	}
	return bson.E{Key: "municipio_id", Value: e.MunicipioID}
}

func faixaDeDatas(f dominio.Filtro) bson.D {
	faixa := bson.D{}
	if !f.De.IsZero() {
		faixa = append(faixa, bson.E{Key: "$gte", Value: f.De.UTC()})
	}
	if !f.Ate.IsZero() {
		faixa = append(faixa, bson.E{Key: "$lt", Value: f.Ate.UTC()})
	}
	return faixa
}

func (d documentoLido) paraDominio() (dominio.Registro, error) {
	r := dominio.Registro{
		Mensagem: dominio.Mensagem{
			ID:           d.ID,
			Acao:         d.Acao,
			Entidade:     d.Entidade,
			EntidadeID:   d.EntidadeID,
			UsuarioID:    d.UsuarioID,
			TipoUsuario:  d.TipoUsuario,
			MunicipioID:  d.MunicipioID,
			OcorridoEm:   d.OcorridoEm,
			Motivo:       d.Motivo,
			Origem:       d.Origem,
			RequisicaoID: d.RequisicaoID,
		},
		Alertas:    d.Alertas,
		RecebidoEm: d.RecebidoEm,
	}
	if len(d.Dados) > 0 {
		// BSON -> JSON comum ("relaxed": números saem como números normais).
		dados, err := bson.MarshalExtJSON(d.Dados, false, false)
		if err != nil {
			return r, fmt.Errorf("mongo: converter dados do registro %s: %w", d.ID, err)
		}
		r.Dados = json.RawMessage(dados)
	}
	return r, nil
}
