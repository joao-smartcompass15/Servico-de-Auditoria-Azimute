package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"api-auditoria/internal/dominio"
)

// Token só de teste (não é segredo de lugar nenhum).
const tokenDeTeste = "token-de-teste-com-mais-de-32-caracteres-0123"

// consultaFake registra o que a API pediu ao service.
type consultaFake struct {
	ultimoFiltro dominio.Filtro
	ultimoEscopo dominio.Escopo
	chamadas     int
}

func (c *consultaFake) Listar(_ context.Context, f dominio.Filtro) ([]dominio.Registro, error) {
	c.chamadas++
	c.ultimoFiltro = f
	return nil, nil
}

func (c *consultaFake) Historico(_ context.Context, _, _ string, f dominio.Filtro) ([]dominio.Registro, error) {
	c.chamadas++
	c.ultimoFiltro = f
	return nil, nil
}

func (c *consultaFake) Obter(_ context.Context, id string, e dominio.Escopo) (dominio.Registro, error) {
	c.chamadas++
	c.ultimoEscopo = e
	return dominio.Registro{Mensagem: dominio.Mensagem{ID: id}}, nil
}

func chamar(t *testing.T, api http.Handler, caminho, autorizacao string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, caminho, nil)
	if autorizacao != "" {
		req.Header.Set("Authorization", autorizacao)
	}
	resp := httptest.NewRecorder()
	api.ServeHTTP(resp, req)
	return resp.Code
}

func TestTokenDeServico(t *testing.T) {
	const caminho = "/api/v1/auditoria?municipio_id=m-1"

	casos := []struct {
		nome        string
		autorizacao string
		queroStatus int
	}{
		{"sem token", "", http.StatusUnauthorized},
		{"token errado", "Bearer token-errado-com-mais-de-32-caracteres-999", http.StatusUnauthorized},
		{"sem a palavra Bearer", tokenDeTeste, http.StatusUnauthorized},
		{"token certo", "Bearer " + tokenDeTeste, http.StatusOK},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			consulta := &consultaFake{}
			api := NovaAPI(consulta, tokenDeTeste, logMudo)
			if status := chamar(t, api, caminho, c.autorizacao); status != c.queroStatus {
				t.Fatalf("status = %d, queria %d", status, c.queroStatus)
			}
			if c.queroStatus == http.StatusUnauthorized && consulta.chamadas != 0 {
				t.Fatal("requisição sem token chegou ao service")
			}
		})
	}
}

func TestTokenDeServico_VazioNegaTudo(t *testing.T) {
	// Deny-by-default: sem token configurado, nem "Bearer " vazio passa.
	api := NovaAPI(&consultaFake{}, "", logMudo)
	if status := chamar(t, api, "/api/v1/auditoria?municipio_id=m-1", "Bearer "); status != http.StatusUnauthorized {
		t.Fatalf("status = %d, queria 401", status)
	}
}

func TestHealthzFicaAberta(t *testing.T) {
	api := NovaAPI(&consultaFake{}, tokenDeTeste, logMudo)
	if status := chamar(t, api, "/healthz", ""); status != http.StatusOK {
		t.Fatalf("status = %d, queria 200", status)
	}
}

func TestEscopoObrigatorio(t *testing.T) {
	autorizacao := "Bearer " + tokenDeTeste

	casos := []struct {
		nome        string
		caminho     string
		queroStatus int
	}{
		{"lista sem escopo", "/api/v1/auditoria", http.StatusBadRequest},
		{"lista com os dois", "/api/v1/auditoria?municipio_id=m-1&escopo=plataforma", http.StatusBadRequest},
		{"lista com escopo desconhecido", "/api/v1/auditoria?escopo=todos", http.StatusBadRequest},
		{"por id sem escopo", "/api/v1/auditoria/r-1", http.StatusBadRequest},
		{"histórico sem escopo", "/api/v1/entidades/lote/L-1/historico", http.StatusBadRequest},
		{"lista do município", "/api/v1/auditoria?municipio_id=m-1", http.StatusOK},
		{"lista da plataforma", "/api/v1/auditoria?escopo=plataforma", http.StatusOK},
		{"por id com município", "/api/v1/auditoria/r-1?municipio_id=m-1", http.StatusOK},
		{"histórico com município", "/api/v1/entidades/lote/L-1/historico?municipio_id=m-1", http.StatusOK},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			api := NovaAPI(&consultaFake{}, tokenDeTeste, logMudo)
			if status := chamar(t, api, c.caminho, autorizacao); status != c.queroStatus {
				t.Fatalf("status = %d, queria %d", status, c.queroStatus)
			}
		})
	}
}

func TestEscopoChegaAoService(t *testing.T) {
	autorizacao := "Bearer " + tokenDeTeste

	consulta := &consultaFake{}
	api := NovaAPI(consulta, tokenDeTeste, logMudo)

	chamar(t, api, "/api/v1/auditoria?municipio_id=m-1", autorizacao)
	if consulta.ultimoFiltro.Escopo != (dominio.Escopo{MunicipioID: "m-1"}) {
		t.Errorf("lista: escopo = %+v", consulta.ultimoFiltro.Escopo)
	}

	chamar(t, api, "/api/v1/auditoria/r-1?escopo=plataforma", autorizacao)
	if consulta.ultimoEscopo != (dominio.Escopo{Plataforma: true}) {
		t.Errorf("por id: escopo = %+v", consulta.ultimoEscopo)
	}
}
