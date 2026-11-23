// Testes ficam no mesmo diretório, em arquivos terminados em _test.go.
// O pacote service_test (com sufixo) testa só o que é público, como um usuário
// de fora faria. Rode com: go test ./...
package service_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"api-auditoria/internal/dominio"
	"api-auditoria/internal/service"
)

// auditoriaFake substitui o banco no teste. Como tem o método Gravar,
// satisfaz service.RepositorioAuditoria automaticamente.
type auditoriaFake struct {
	gravados []dominio.Registro
	falha    error
}

func (f *auditoriaFake) Gravar(_ context.Context, r dominio.Registro) error {
	if f.falha != nil {
		return f.falha
	}
	f.gravados = append(f.gravados, r)
	return nil
}

type distribuidorFake struct {
	distribuidas []dominio.Mensagem
}

func (f *distribuidorFake) Distribuir(_ context.Context, m dominio.Mensagem) error {
	f.distribuidas = append(f.distribuidas, m)
	return nil
}

var catalogoTeste = dominio.Catalogo{
	"solicitacao.criada":   {Distribuir: true},
	"solicitacao.editada":  {Distribuir: false},
	"solicitacao.aprovada": {Distribuir: true, MotivoObrigatorio: true},
}

func mensagem(acao, motivo string) dominio.Mensagem {
	return dominio.Mensagem{
		ID:         "msg-1",
		Acao:       acao,
		Entidade:   "solicitacao",
		EntidadeID: "SOL-123",
		UsuarioID:  "usuario-1",
		OcorridoEm: time.Date(2026, 11, 19, 14, 30, 0, 0, time.UTC),
		Motivo:     motivo,
	}
}

// Teste "dirigido por tabela" (table-driven): o jeito mais comum de testar em Go.
// Cada linha da tabela é um cenário; o laço roda todos com t.Run.
func TestProcessar(t *testing.T) {
	semUsuario := mensagem("solicitacao.criada", "")
	semUsuario.UsuarioID = ""

	casos := []struct {
		nome         string
		msg          dominio.Mensagem
		querErro     error // nil = espera sucesso
		querGravados int
		querDistrib  int
		querAlertas  []string
	}{
		{"ação que distribui: audita e distribui", mensagem("solicitacao.criada", ""), nil, 1, 1, nil},
		{"ação que não distribui: só audita", mensagem("solicitacao.editada", ""), nil, 1, 0, nil},
		{"motivo obrigatório presente: sem alerta", mensagem("solicitacao.aprovada", "documentação ok"), nil, 1, 1, nil},
		{"motivo obrigatório ausente: audita com alerta", mensagem("solicitacao.aprovada", "  "), nil, 1, 1,
			[]string{dominio.AlertaMotivoAusente}},
		{"ação fora do catálogo: audita com alerta e não distribui", mensagem("algo.novo", ""), nil, 1, 0,
			[]string{dominio.AlertaAcaoForaDoCatalogo}},
		{"sem usuário (quem fez): rejeitada", semUsuario, dominio.ErrMensagemInvalida, 0, 0, nil},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			auditoria := &auditoriaFake{}
			distribuidor := &distribuidorFake{}
			p := service.NovoProcessador(auditoria, distribuidor, catalogoTeste)

			err := p.Processar(t.Context(), c.msg)

			if !errors.Is(err, c.querErro) {
				t.Fatalf("erro = %v, queria %v", err, c.querErro)
			}
			if len(auditoria.gravados) != c.querGravados {
				t.Fatalf("gravados = %d, queria %d", len(auditoria.gravados), c.querGravados)
			}
			if len(distribuidor.distribuidas) != c.querDistrib {
				t.Errorf("distribuídas = %d, queria %d", len(distribuidor.distribuidas), c.querDistrib)
			}
			if c.querGravados > 0 && !slices.Equal(auditoria.gravados[0].Alertas, c.querAlertas) {
				t.Errorf("alertas = %v, queria %v", auditoria.gravados[0].Alertas, c.querAlertas)
			}
		})
	}
}

// Se o banco falha: o erro sobe (para o consumidor tentar de novo), NÃO pode
// parecer "mensagem inválida", e nada é distribuído antes de estar auditado.
func TestProcessar_FalhaNoBanco(t *testing.T) {
	foraDoAr := errors.New("cassandra fora do ar")
	distribuidor := &distribuidorFake{}
	p := service.NovoProcessador(&auditoriaFake{falha: foraDoAr}, distribuidor, catalogoTeste)

	err := p.Processar(t.Context(), mensagem("solicitacao.criada", ""))

	if !errors.Is(err, foraDoAr) {
		t.Fatalf("erro = %v, queria que embrulhasse %v", err, foraDoAr)
	}
	if errors.Is(err, dominio.ErrMensagemInvalida) {
		t.Fatal("falha de infraestrutura não pode ser tratada como mensagem inválida")
	}
	if len(distribuidor.distribuidas) != 0 {
		t.Fatal("não pode distribuir o que não foi auditado")
	}
}
