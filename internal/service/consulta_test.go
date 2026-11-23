package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"api-auditoria/internal/dominio"
	"api-auditoria/internal/service"
)

const municipioA = "11111111-1111-4111-8111-111111111111"

var escopoA = dominio.Escopo{MunicipioID: municipioA}

// leituraFake guarda o último filtro recebido para o teste conferir, e
// devolve o registro configurado na busca por id.
type leituraFake struct {
	ultimoFiltro dominio.Filtro
	ultimoEscopo dominio.Escopo
	registro     *dominio.Registro
}

func (f *leituraFake) Buscar(_ context.Context, filtro dominio.Filtro) ([]dominio.Registro, error) {
	f.ultimoFiltro = filtro
	return nil, nil
}

func (f *leituraFake) BuscarPorID(_ context.Context, _ string, escopo dominio.Escopo) (dominio.Registro, error) {
	f.ultimoEscopo = escopo
	if f.registro == nil {
		return dominio.Registro{}, dominio.ErrNaoEncontrada
	}
	return *f.registro, nil
}

func TestListar_AplicaPadroes(t *testing.T) {
	leitura := &leituraFake{}

	if _, err := service.NovaConsulta(leitura).Listar(t.Context(), dominio.Filtro{Escopo: escopoA}); err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if leitura.ultimoFiltro.Pagina != 1 || leitura.ultimoFiltro.Limite != 50 {
		t.Errorf("filtro = %+v, queria pagina 1 e limite 50", leitura.ultimoFiltro)
	}
	if leitura.ultimoFiltro.Escopo != escopoA {
		t.Errorf("escopo = %+v, queria %+v", leitura.ultimoFiltro.Escopo, escopoA)
	}
}

func TestHistorico_EhCronologicoEFiltraAEntidade(t *testing.T) {
	leitura := &leituraFake{}

	_, err := service.NovaConsulta(leitura).Historico(t.Context(), "solicitacao", "SOL-123", dominio.Filtro{Escopo: escopoA})
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}

	f := leitura.ultimoFiltro
	if f.Entidade != "solicitacao" || f.EntidadeID != "SOL-123" || !f.Crescente {
		t.Errorf("filtro = %+v, queria solicitacao/SOL-123 em ordem crescente", f)
	}
}

func TestListar_RejeitaFiltroInvalido(t *testing.T) {
	agora := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)

	casos := []struct {
		nome   string
		filtro dominio.Filtro
	}{
		{"sem escopo", dominio.Filtro{}},
		{"escopo com município E plataforma", dominio.Filtro{Escopo: dominio.Escopo{MunicipioID: municipioA, Plataforma: true}}},
		{"limite acima do máximo", dominio.Filtro{Escopo: escopoA, Limite: 1000}},
		{"página negativa", dominio.Filtro{Escopo: escopoA, Pagina: -1}},
		{"página acima do máximo", dominio.Filtro{Escopo: escopoA, Pagina: 10_001}},
		{"período invertido", dominio.Filtro{Escopo: escopoA, De: agora, Ate: agora.Add(-time.Hour)}},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			_, err := service.NovaConsulta(&leituraFake{}).Listar(t.Context(), c.filtro)
			if !errors.Is(err, dominio.ErrFiltroInvalido) {
				t.Fatalf("erro = %v, queria ErrFiltroInvalido", err)
			}
		})
	}
}

func TestHistorico_SemEscopoEhInvalido(t *testing.T) {
	_, err := service.NovaConsulta(&leituraFake{}).Historico(t.Context(), "lote", "L-1", dominio.Filtro{})
	if !errors.Is(err, dominio.ErrFiltroInvalido) {
		t.Fatalf("erro = %v, queria ErrFiltroInvalido", err)
	}
}

func TestObter_Escopo(t *testing.T) {
	doMunicipioA := dominio.Registro{Mensagem: dominio.Mensagem{ID: "r-1", MunicipioID: municipioA}}
	daPlataforma := dominio.Registro{Mensagem: dominio.Mensagem{ID: "r-2"}}

	casos := []struct {
		nome     string
		registro dominio.Registro
		escopo   dominio.Escopo
		queroErr error
	}{
		{"mesmo município", doMunicipioA, escopoA, nil},
		{"outro município vira 404", doMunicipioA, dominio.Escopo{MunicipioID: "outro"}, dominio.ErrNaoEncontrada},
		{"plataforma não vê o do município", doMunicipioA, dominio.Escopo{Plataforma: true}, dominio.ErrNaoEncontrada},
		{"plataforma vê o de plataforma", daPlataforma, dominio.Escopo{Plataforma: true}, nil},
		{"município não vê o de plataforma", daPlataforma, escopoA, dominio.ErrNaoEncontrada},
		{"sem escopo é inválido", doMunicipioA, dominio.Escopo{}, dominio.ErrFiltroInvalido},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			// O fake devolve o registro "sem filtrar": a trava do service
			// precisa segurar sozinha, mesmo com um adaptador defeituoso.
			leitura := &leituraFake{registro: &c.registro}
			_, err := service.NovaConsulta(leitura).Obter(t.Context(), c.registro.ID, c.escopo)
			if c.queroErr == nil && err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if c.queroErr != nil && !errors.Is(err, c.queroErr) {
				t.Fatalf("erro = %v, queria %v", err, c.queroErr)
			}
		})
	}
}
