// Package distribuicao entrega as ações aos usuários.
//
// TEMPORÁRIO: por enquanto só escreve no log. Quando estiver definido como o
// usuário recebe (WebSocket, SSE, push...) e quem recebe cada ação, o
// adaptador real entra aqui com o mesmo método Distribuir, e o main.go troca
// uma linha.
package distribuicao

import (
	"context"
	"log/slog"

	"api-auditoria/internal/dominio"
	"api-auditoria/internal/service"
)

var (
	_ service.DistribuidorEventos = (*DistribuidorEmLog)(nil)
	_ service.DistribuidorEventos = Ignorar{}
)

type DistribuidorEmLog struct {
	log *slog.Logger
}

func NovoDistribuidorEmLog(log *slog.Logger) *DistribuidorEmLog {
	return &DistribuidorEmLog{log: log}
}

func (d *DistribuidorEmLog) Distribuir(ctx context.Context, m dominio.Mensagem) error {
	d.log.InfoContext(ctx, "ação distribuída aos usuários (temporário: só log)",
		"id", m.ID, "acao", m.Acao, "entidade", m.Entidade, "entidade_id", m.EntidadeID)
	return nil
}

// Ignorar não faz nada. É usado pela projeção do Mongo: a distribuição já é
// feita pelo consumidor principal, e repeti-la duplicaria as notificações.
//
// struct{} vazia não ocupa memória; o receptor sem nome (Ignorar) indica que
// o método não usa o valor.
type Ignorar struct{}

func (Ignorar) Distribuir(context.Context, dominio.Mensagem) error { return nil }
