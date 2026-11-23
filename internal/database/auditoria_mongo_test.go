package database

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"api-auditoria/internal/dominio"
)

// Ida e volta sem Mongo: Registro -> documento BSON -> Registro.
// Garante que nenhum campo se perde na conversão e que "dados" volta como
// o mesmo JSON.
func TestDocumento_IdaEVolta(t *testing.T) {
	original := dominio.Registro{
		Mensagem: dominio.Mensagem{
			ID:           "msg-1",
			Acao:         "solicitacao.aprovada",
			Entidade:     "solicitacao",
			EntidadeID:   "SOL-123",
			UsuarioID:    "usuario-3",
			TipoUsuario:  "prefeitura",
			MunicipioID:  "11111111-1111-4111-8111-111111111111",
			OcorridoEm:   time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC),
			Motivo:       "documentação conferida",
			Origem:       "gestor-api",
			RequisicaoID: "req-abc-123",
			Dados:        json.RawMessage(`{"antes":{"situacao":"pendente"},"depois":{"situacao":"aprovada","valor":1500}}`),
		},
		Alertas:    []string{dominio.AlertaMotivoAusente},
		RecebidoEm: time.Date(2026, 11, 19, 10, 0, 1, 0, time.UTC),
	}

	gravado, err := paraDocumento(original)
	if err != nil {
		t.Fatal(err)
	}
	bruto, err := bson.Marshal(gravado)
	if err != nil {
		t.Fatal(err)
	}
	var lido documentoLido
	if err := bson.Unmarshal(bruto, &lido); err != nil {
		t.Fatal(err)
	}
	volta, err := lido.paraDominio()
	if err != nil {
		t.Fatal(err)
	}

	if volta.ID != original.ID || volta.Acao != original.Acao || volta.EntidadeID != original.EntidadeID ||
		volta.UsuarioID != original.UsuarioID || volta.Motivo != original.Motivo || volta.Origem != original.Origem ||
		volta.TipoUsuario != original.TipoUsuario || volta.MunicipioID != original.MunicipioID ||
		volta.RequisicaoID != original.RequisicaoID ||
		!volta.OcorridoEm.Equal(original.OcorridoEm) || !volta.RecebidoEm.Equal(original.RecebidoEm) ||
		!slices.Equal(volta.Alertas, original.Alertas) {
		t.Fatalf("campos diferentes:\n original = %+v\n volta    = %+v", original, volta)
	}

	// Compara "dados" pelo conteúdo, não pelo texto: a ordem das chaves pode mudar.
	var esperado, obtido any
	_ = json.Unmarshal(original.Dados, &esperado)
	if err := json.Unmarshal(volta.Dados, &obtido); err != nil {
		t.Fatalf("dados não voltou como JSON válido: %s", volta.Dados)
	}
	a, _ := json.Marshal(esperado)
	b, _ := json.Marshal(obtido)
	if string(a) != string(b) {
		t.Fatalf("dados = %s, queria %s", b, a)
	}

	// Inteiro tem de voltar inteiro: auditoria devolve o dado como entrou.
	if strings.Contains(string(volta.Dados), "1500.0") {
		t.Fatalf("inteiro virou decimal: %s", volta.Dados)
	}
}

// A ação de plataforma precisa ser GRAVADA com municipio_id "" (e não sem o
// campo): é exatamente isso que o filtro do escopo de plataforma procura.
func TestDocumento_PlataformaGravaMunicipioVazio(t *testing.T) {
	gravado, err := paraDocumento(dominio.Registro{Mensagem: dominio.Mensagem{ID: "p-1", Acao: "prefeitura.criada"}})
	if err != nil {
		t.Fatal(err)
	}
	bruto, err := bson.Marshal(gravado)
	if err != nil {
		t.Fatal(err)
	}
	valor, err := bson.Raw(bruto).LookupErr("municipio_id")
	if err != nil {
		t.Fatalf("municipio_id ausente no documento: %v", err)
	}
	if texto, ok := valor.StringValueOK(); !ok || texto != "" {
		t.Fatalf("municipio_id = %v, queria \"\"", valor)
	}
}

func TestFiltroDoEscopo(t *testing.T) {
	if e := filtroDoEscopo(dominio.Escopo{Plataforma: true}); e.Key != "municipio_id" || e.Value != "" {
		t.Errorf("plataforma = %+v", e)
	}
	if e := filtroDoEscopo(dominio.Escopo{MunicipioID: "m-1"}); e.Key != "municipio_id" || e.Value != "m-1" {
		t.Errorf("município = %+v", e)
	}
	// Escopo vazio (que o service nunca deixa passar) não pode virar "todos".
	if e := filtroDoEscopo(dominio.Escopo{}); e.Value != "" {
		t.Errorf("vazio = %+v, queria município \"\"", e)
	}
}
