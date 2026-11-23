// Package service contém a regra de negócio. Ele não sabe que o Kafka existe
// nem qual banco é usado. Só conhece as interfaces abaixo, e por isso pode
// ser testado sem subir nada.
package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"api-auditoria/internal/dominio"
)

// RepositorioAuditoria é a "porta" de gravação da auditoria.
//
// Interfaces em Go são satisfeitas implicitamente: qualquer tipo que tenha um
// método Gravar com essa assinatura já "implementa" a interface, sem precisar
// declarar nada. Cassandra e Mongo entram aqui sem mudar o service.
type RepositorioAuditoria interface {
	Gravar(ctx context.Context, r dominio.Registro) error
}

// DistribuidorEventos é a porta que entrega as ações aos usuários.
type DistribuidorEventos interface {
	Distribuir(ctx context.Context, m dominio.Mensagem) error
}

// Processador aplica a regra do serviço a cada mensagem:
//  1. TUDO é auditado;
//  2. o catálogo decide se a ação também é distribuída aos usuários.
//
// Os campos começam com letra minúscula: são privados ao pacote.
type Processador struct {
	auditoria    RepositorioAuditoria
	distribuidor DistribuidorEventos
	catalogo     dominio.Catalogo
}

// NovoProcessador faz o papel de construtor. Go não tem construtores; a
// convenção é uma função NovoX (ou NewX) que devolve o valor pronto para uso.
func NovoProcessador(auditoria RepositorioAuditoria, distribuidor DistribuidorEventos, catalogo dominio.Catalogo) *Processador {
	return &Processador{auditoria: auditoria, distribuidor: distribuidor, catalogo: catalogo}
}

// Processar audita a mensagem e, se o catálogo mandar, distribui.
//
// O primeiro parâmetro é sempre um context.Context. Ele carrega o prazo e o
// sinal de cancelamento (ex.: o serviço está desligando) até o banco/rede.
func (p *Processador) Processar(ctx context.Context, m dominio.Mensagem) error {
	if err := m.Validar(); err != nil {
		return err
	}

	regra, noCatalogo := p.catalogo.Regra(m.Acao)

	registro := dominio.Registro{Mensagem: m, RecebidoEm: time.Now().UTC()}
	if !noCatalogo {
		registro.Alertas = append(registro.Alertas, dominio.AlertaAcaoForaDoCatalogo)
	}
	if regra.MotivoObrigatorio && strings.TrimSpace(m.Motivo) == "" {
		registro.Alertas = append(registro.Alertas, dominio.AlertaMotivoAusente)
	}

	// Audita PRIMEIRO. Se a distribuição falhar depois, a mensagem é
	// reprocessada; a gravação repetida não duplica (o id é a chave).
	if err := p.auditoria.Gravar(ctx, registro); err != nil {
		return fmt.Errorf("auditar %s: %w", m.ID, err)
	}

	if regra.Distribuir {
		if err := p.distribuidor.Distribuir(ctx, m); err != nil {
			return fmt.Errorf("distribuir %s: %w", m.ID, err)
		}
	}
	return nil
}
