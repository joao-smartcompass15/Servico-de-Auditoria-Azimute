// Package database guarda os adaptadores de banco: as implementações concretas
// das interfaces (portas) declaradas no pacote service.
package database

import (
	"context"
	"fmt"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"api-auditoria/internal/dominio"
	"api-auditoria/internal/service"
)

// Garante, em tempo de compilação, que o tipo satisfaz a interface.
var _ service.RepositorioAuditoria = (*AuditoriaCassandra)(nil)

// ConfigCassandra agrupa o que é preciso para conectar.
type ConfigCassandra struct {
	Hosts    []string
	Keyspace string
	Usuario  string // vazio = sem autenticação (só para ambiente local)
	Senha    string
}

// ConectarCassandra abre a sessão. Quem chama é responsável por sessao.Close().
func ConectarCassandra(cfg ConfigCassandra) (*gocql.Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Keyspace = cfg.Keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	if cfg.Usuario != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Usuario, Password: cfg.Senha}
	}

	sessao, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("conectar no cassandra %v: %w", cfg.Hosts, err)
	}
	return sessao, nil
}

// AuditoriaCassandra é o lado de ESCRITA da auditoria.
type AuditoriaCassandra struct {
	sessao *gocql.Session
}

func NovaAuditoriaCassandra(sessao *gocql.Session) *AuditoriaCassandra {
	return &AuditoriaCassandra{sessao: sessao}
}

const (
	// municipio_id, tipo_usuario e requisicao_id são colunas comuns, fora da
	// chave: os ids da plataforma são UUID globais, então (entidade,
	// entidade_id) já não colide entre municípios.
	inserirHistorico = `
INSERT INTO historico_por_entidade
  (entidade, entidade_id, ocorrido_em, id, acao, usuario_id, tipo_usuario, municipio_id,
   motivo, origem, requisicao_id, dados, alertas, recebido_em)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	inserirPorUsuario = `
INSERT INTO acoes_por_usuario
  (usuario_id, dia, ocorrido_em, id, acao, entidade, entidade_id, tipo_usuario, municipio_id,
   motivo, origem, requisicao_id, dados, alertas, recebido_em)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// Gravar escreve o registro nas duas tabelas, uma depois da outra.
//
// Por que NÃO um LOGGED BATCH (era assim até a carga de 23/11/2026): o batch
// "logado" grava antes um registro de controle (batchlog) e depois o apaga —
// o dobro de trabalho por mensagem, e foi o que estourou o tempo do Cassandra
// sob carga. A garantia que ele dava ("as duas ou nenhuma") já vem de outro
// lugar: se a segunda gravação falhar, o erro sobe, o offset NÃO é confirmado
// e a mensagem inteira é gravada de novo. Como a chave das duas tabelas inclui
// o id da mensagem, regravar só sobrescreve com o mesmo conteúdo.
func (a *AuditoriaCassandra) Gravar(ctx context.Context, r dominio.Registro) error {
	ocorrido := r.OcorridoEm.UTC()
	dia := time.Date(ocorrido.Year(), ocorrido.Month(), ocorrido.Day(), 0, 0, 0, 0, time.UTC)
	dados := string(r.Dados)

	// Os "?" são parâmetros: o driver envia os valores separados da query,
	// o que impede injeção de CQL. Nunca monte a query concatenando strings.
	err := a.sessao.Query(inserirHistorico,
		r.Entidade, r.EntidadeID, ocorrido, r.ID, r.Acao, r.UsuarioID, r.TipoUsuario, r.MunicipioID,
		r.Motivo, r.Origem, r.RequisicaoID, dados, r.Alertas, r.RecebidoEm).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("cassandra: gravar linha do tempo: %w", err)
	}

	err = a.sessao.Query(inserirPorUsuario,
		r.UsuarioID, dia, ocorrido, r.ID, r.Acao, r.Entidade, r.EntidadeID, r.TipoUsuario, r.MunicipioID,
		r.Motivo, r.Origem, r.RequisicaoID, dados, r.Alertas, r.RecebidoEm).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("cassandra: gravar ações do usuário: %w", err)
	}
	return nil
}
