// Este teste fica no pacote handlers (sem _test) porque testa tratar, que é
// privado (letra minúscula). Testes no mesmo pacote enxergam o que é privado.
package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"

	"api-auditoria/internal/dominio"
)

type processadorFake struct {
	recebidas []dominio.Mensagem
	falha     error
}

func (f *processadorFake) Processar(_ context.Context, m dominio.Mensagem) error {
	f.recebidas = append(f.recebidas, m)
	return f.falha
}

// logMudo descarta a saída para não poluir o resultado do teste.
var logMudo = slog.New(slog.NewTextHandler(io.Discard, nil))

const jsonValido = `{"id":"m-1","acao":"solicitacao.criada","entidade":"solicitacao","entidade_id":"SOL-1",` +
	`"usuario_id":"usuario-1","ocorrido_em":"2026-11-19T10:00:00Z"}`

func TestTratar(t *testing.T) {
	casos := []struct {
		nome          string
		corpo         string
		falha         error
		querErro      bool
		querRecebidas int
	}{
		{"json válido chega ao processador", jsonValido, nil, false, 1},
		{"BOM do PowerShell é ignorado", "\xef\xbb\xbf" + jsonValido, nil, false, 1},
		{"json quebrado é descartado sem erro", `{isto nao e json}`, nil, false, 0},
		{"mensagem inválida é descartada sem erro", jsonValido, dominio.ErrMensagemInvalida, false, 1},
		{"falha de infraestrutura devolve erro", jsonValido, errors.New("banco fora"), true, 1},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			proc := &processadorFake{falha: c.falha}
			consumidor := NovoConsumidorKafka(nil, proc, logMudo)

			err := consumidor.tratar(t.Context(), kafka.Message{Value: []byte(c.corpo)})

			if (err != nil) != c.querErro {
				t.Fatalf("erro = %v, queria erro? %v", err, c.querErro)
			}
			if len(proc.recebidas) != c.querRecebidas {
				t.Errorf("recebidas = %d, queria %d", len(proc.recebidas), c.querRecebidas)
			}
		})
	}
}
