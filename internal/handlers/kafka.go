// Package handlers é a porta de entrada do serviço. Aqui a entrada é o Kafka,
// mas o papel é o mesmo de um controller HTTP: receber, converter e chamar o
// service. Nenhuma regra de negócio mora aqui.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"api-auditoria/internal/dominio"
)

// Processador é o que este handler precisa do service.
//
// Convenção de Go: a interface é declarada por quem USA, não por quem
// implementa. O handler pede só o método de que precisa, e isso facilita
// testar o handler com um processador falso.
type Processador interface {
	Processar(ctx context.Context, m dominio.Mensagem) error
}

var bomUTF8 = []byte("\xef\xbb\xbf")

type ConsumidorKafka struct {
	leitor      *kafka.Reader
	processador Processador
	log         *slog.Logger

	// Espera entre as tentativas da MESMA mensagem quando o banco falha:
	// começa curta e dobra a cada falha, até o teto.
	esperaInicial time.Duration
	esperaMaxima  time.Duration
}

func NovoConsumidorKafka(leitor *kafka.Reader, processador Processador, log *slog.Logger) *ConsumidorKafka {
	return &ConsumidorKafka{
		leitor: leitor, processador: processador, log: log,
		esperaInicial: 200 * time.Millisecond,
		esperaMaxima:  30 * time.Second,
	}
}

// Rodar lê mensagens até o contexto ser cancelado (Ctrl+C ou parada do container).
//
// Garantia de entrega: o offset só é confirmado (commit) depois que a mensagem
// foi processada. Se o serviço cair no meio, a mensagem é lida de novo ao
// reiniciar. Isso é entrega "pelo menos uma vez", então a gravação precisa
// tolerar repetição; o ID da mensagem serve de chave.
func (c *ConsumidorKafka) Rodar(ctx context.Context) error {
	for { // em Go, "for" sem condição é o laço infinito (não existe "while")
		msg, err := c.leitor.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // desligamento pedido: sai sem erro
			}
			return fmt.Errorf("ler do kafka: %w", err)
		}

		if !c.insistir(ctx, msg) {
			return nil // desligou no meio: sem commit, a mensagem volta ao reiniciar
		}

		if err := c.leitor.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("confirmar offset %d: %w", msg.Offset, err)
		}
	}
}

// insistir trata a mensagem até dar certo. Devolve false só se o serviço
// estiver desligando (aí a mensagem fica sem commit e volta ao reiniciar).
//
// Falha de infraestrutura (banco lento ou fora do ar) NÃO derruba o serviço:
// a MESMA mensagem é tentada de novo, com espera crescente. Pular para a
// próxima não pode: o commit de um offset confirma também os anteriores, e esta
// mensagem se perderia. Até 23/11/2026 o erro encerrava o processo inteiro
// (API junto) para reiniciar e reler; na carga, um tempo esgotado de 1 s do
// Cassandra virava 20 s de serviço fora do ar.
//
// Pendência conhecida: um erro PERMANENTE (dado que o banco nunca aceita)
// prende a partição aqui, tentando para sempre, com aviso no log. Resolver isso
// é o tópico de erros (DLQ).
func (c *ConsumidorKafka) insistir(ctx context.Context, msg kafka.Message) bool {
	espera := c.esperaInicial
	for tentativa := 1; ; tentativa++ {
		err := c.tratar(ctx, msg)
		if err == nil {
			if tentativa > 1 {
				c.log.InfoContext(ctx, "mensagem gravada depois de falhas",
					"particao", msg.Partition, "offset", msg.Offset, "tentativas", tentativa)
			}
			return true
		}
		if ctx.Err() != nil {
			return false
		}

		c.log.WarnContext(ctx, "falha ao gravar; a mesma mensagem será tentada de novo",
			"particao", msg.Partition, "offset", msg.Offset, "tentativa", tentativa,
			"proxima_em", espera.String(), "erro", err)

		select {
		case <-ctx.Done():
			return false
		case <-time.After(espera):
		}
		espera = min(espera*2, c.esperaMaxima)
	}
}

// tratar converte e processa uma mensagem. Devolve erro só quando vale a pena
// tentar de novo. Mensagens com defeito vão para o log e são descartadas.
func (c *ConsumidorKafka) tratar(ctx context.Context, msg kafka.Message) error {
	// Alguns produtores (ex.: PowerShell no Windows) colocam um BOM UTF-8 no
	// início do texto. O encoding/json não aceita, e a RFC 8259 permite ignorar.
	corpo := bytes.TrimPrefix(msg.Value, bomUTF8)

	var m dominio.Mensagem
	if err := json.Unmarshal(corpo, &m); err != nil {
		c.log.ErrorContext(ctx, "mensagem com JSON inválido descartada",
			"particao", msg.Partition, "offset", msg.Offset, "erro", err)
		return nil
	}

	err := c.processador.Processar(ctx, m)
	if errors.Is(err, dominio.ErrMensagemInvalida) {
		c.log.ErrorContext(ctx, "mensagem inválida descartada",
			"id", m.ID, "particao", msg.Partition, "offset", msg.Offset, "erro", err)
		return nil
	}
	return err
}
