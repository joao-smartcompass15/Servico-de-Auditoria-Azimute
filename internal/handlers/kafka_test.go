// Este teste fica no pacote handlers (sem _test) porque testa tratar, que é
// privado (letra minúscula). Testes no mesmo pacote enxergam o que é privado.
package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

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

// processadorInstavel falha as primeiras N vezes e depois funciona — o banco
// que engasga sob carga e volta.
type processadorInstavel struct {
	falhasRestantes int
	chamadas        int
}

func (p *processadorInstavel) Processar(_ context.Context, _ dominio.Mensagem) error {
	p.chamadas++
	if p.falhasRestantes > 0 {
		p.falhasRestantes--
		return errors.New("cassandra: Operation timed out")
	}
	return nil
}

func TestInsistir_RepeteAMesmaMensagemAteGravar(t *testing.T) {
	proc := &processadorInstavel{falhasRestantes: 3}
	consumidor := NovoConsumidorKafka(nil, proc, logMudo)
	consumidor.esperaInicial = time.Millisecond // o teste não precisa esperar de verdade
	consumidor.esperaMaxima = 4 * time.Millisecond

	if !consumidor.insistir(t.Context(), kafka.Message{Value: []byte(jsonValido)}) {
		t.Fatal("insistir devolveu false, mas o processador voltou a funcionar")
	}
	if proc.chamadas != 4 {
		t.Errorf("chamadas = %d, queria 4 (3 falhas + o acerto)", proc.chamadas)
	}
}

func TestInsistir_DesligamentoInterrompeSemConfirmar(t *testing.T) {
	proc := &processadorInstavel{falhasRestantes: 1_000_000} // banco fora para sempre
	consumidor := NovoConsumidorKafka(nil, proc, logMudo)
	consumidor.esperaInicial = time.Millisecond
	consumidor.esperaMaxima = time.Millisecond

	ctx, cancelar := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelar()

	if consumidor.insistir(ctx, kafka.Message{Value: []byte(jsonValido)}) {
		t.Fatal("insistir devolveu true sem nunca ter gravado")
	}
	if proc.chamadas < 2 {
		t.Errorf("chamadas = %d: deveria ter tentado de novo antes do desligamento", proc.chamadas)
	}
}
