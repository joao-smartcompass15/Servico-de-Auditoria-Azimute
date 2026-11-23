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
}

func NovoConsumidorKafka(leitor *kafka.Reader, processador Processador, log *slog.Logger) *ConsumidorKafka {
	return &ConsumidorKafka{leitor: leitor, processador: processador, log: log}
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

		if err := c.tratar(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil // desligou no meio: sem commit, a mensagem volta ao reiniciar
			}
			// Falha de infraestrutura (ex.: banco fora do ar). Não confirmamos o
			// offset e paramos. Ao reiniciar, a mensagem será lida de novo.
			// Se pulássemos para a próxima e confirmássemos, esta seria perdida,
			// porque o commit de um offset confirma também os anteriores.
			return fmt.Errorf("partição %d, offset %d: %w", msg.Partition, msg.Offset, err)
		}

		if err := c.leitor.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("confirmar offset %d: %w", msg.Offset, err)
		}
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
