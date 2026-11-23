// API INTERNA de consulta da auditoria.
//
// Quem chama esta API é a API do gerenciador de processos, e mais ninguém:
//   - o gerenciador confere a permissão `auditoria.ler` do usuário (RBAC por
//     rota, como em todo o sistema);
//   - o gerenciador põe o município do TOKEN do usuário na consulta
//     (`municipio_id`), ou `escopo=plataforma` para quem está sem município;
//   - o gerenciador se identifica aqui com o token de serviço
//     (`Authorization: Bearer <AUDITORIA_TOKEN_SERVICO>`).
//
// Por isso esta API NÃO entra no Kong e a porta não é publicada: ela fica na
// rede interna, alcançável só pela API do gerenciador.
package handlers

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"api-auditoria/internal/dominio"
)

// ServicoConsulta é o que a API precisa do service (interface declarada aqui,
// por quem usa).
type ServicoConsulta interface {
	Listar(ctx context.Context, f dominio.Filtro) ([]dominio.Registro, error)
	Historico(ctx context.Context, entidade, entidadeID string, f dominio.Filtro) ([]dominio.Registro, error)
	Obter(ctx context.Context, id string, escopo dominio.Escopo) (dominio.Registro, error)
}

type apiAuditoria struct {
	consulta ServicoConsulta
	log      *slog.Logger
}

// NovaAPI devolve as rotas HTTP do serviço.
//
// Desde o Go 1.22 o roteador padrão (ServeMux) entende método e parâmetro no
// caminho ("GET /auditoria/{id}"), então não precisamos de framework.
//
// tokenServico é o segredo compartilhado com a API do gerenciador. Toda rota
// /api/v1/* exige o token; só /healthz fica aberta (o Docker e o Dokploy
// precisam dela para saber se o serviço está de pé).
func NovaAPI(consulta ServicoConsulta, tokenServico string, log *slog.Logger) http.Handler {
	api := &apiAuditoria{consulta: consulta, log: log}
	protegida := exigirTokenDeServico(tokenServico, log)

	rotas := http.NewServeMux()
	rotas.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		responderJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	rotas.Handle("GET /api/v1/auditoria", protegida(api.listar))
	rotas.Handle("GET /api/v1/auditoria/{id}", protegida(api.obter))
	rotas.Handle("GET /api/v1/entidades/{entidade}/{entidade_id}/historico", protegida(api.historico))
	return rotas
}

// exigirTokenDeServico é um "middleware": uma função que recebe um handler e
// devolve outro, que primeiro confere o token e só então chama o original.
//
// A comparação usa subtle.ConstantTimeCompare sobre o SHA-256 dos dois
// valores: o tempo da resposta não depende de quantos caracteres batem (nem
// do tamanho do token), o que impede descobrir o segredo pelo cronômetro.
// Sem token configurado, nega tudo (deny-by-default).
func exigirTokenDeServico(esperado string, log *slog.Logger) func(http.HandlerFunc) http.Handler {
	resumoEsperado := sha256.Sum256([]byte(esperado))

	return func(proximo http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recebido, temBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			resumoRecebido := sha256.Sum256([]byte(recebido))

			valido := esperado != "" && temBearer &&
				subtle.ConstantTimeCompare(resumoRecebido[:], resumoEsperado[:]) == 1
			if !valido {
				// Nunca registrar o token (nem o recebido): só onde e de quem.
				log.WarnContext(r.Context(), "consulta sem token de serviço válido",
					"caminho", r.URL.Path, "origem", r.RemoteAddr)
				w.Header().Set("WWW-Authenticate", "Bearer")
				responderErro(w, http.StatusUnauthorized, "não autorizado")
				return
			}
			proximo(w, r)
		})
	}
}

// listar: GET /api/v1/auditoria?municipio_id=|escopo=plataforma&usuario_id=&entidade=&entidade_id=&acao=&com_alerta=true&de=&ate=&pagina=&limite=
// Mais recente primeiro. "de" e "ate" no formato RFC 3339 (ex.: 2026-11-19T00:00:00Z).
func (a *apiAuditoria) listar(w http.ResponseWriter, r *http.Request) {
	filtro, err := filtroDaURL(r.URL.Query())
	if err != nil {
		responderErro(w, http.StatusBadRequest, err.Error())
		return
	}
	registros, err := a.consulta.Listar(r.Context(), filtro)
	a.responderLista(w, r, registros, err)
}

// historico: GET /api/v1/entidades/solicitacao/<id>/historico?municipio_id=|escopo=plataforma
// Linha do tempo da entidade, do mais antigo ao mais novo.
func (a *apiAuditoria) historico(w http.ResponseWriter, r *http.Request) {
	filtro, err := filtroDaURL(r.URL.Query())
	if err != nil {
		responderErro(w, http.StatusBadRequest, err.Error())
		return
	}
	registros, err := a.consulta.Historico(r.Context(), r.PathValue("entidade"), r.PathValue("entidade_id"), filtro)
	a.responderLista(w, r, registros, err)
}

// obter: GET /api/v1/auditoria/{id}?municipio_id=|escopo=plataforma
// Registro de outro município responde 404, igual a inexistente.
func (a *apiAuditoria) obter(w http.ResponseWriter, r *http.Request) {
	escopo, err := escopoDaURL(r.URL.Query())
	if err != nil {
		responderErro(w, http.StatusBadRequest, err.Error())
		return
	}
	registro, err := a.consulta.Obter(r.Context(), r.PathValue("id"), escopo)
	if err != nil {
		a.traduzirErro(w, r, err)
		return
	}
	responderJSON(w, http.StatusOK, registro)
}

func (a *apiAuditoria) responderLista(w http.ResponseWriter, r *http.Request, registros []dominio.Registro, err error) {
	if err != nil {
		a.traduzirErro(w, r, err)
		return
	}
	responderJSON(w, http.StatusOK, map[string]any{"itens": registros, "quantidade": len(registros)})
}

// traduzirErro converte erro de domínio em status HTTP. Erro desconhecido vira
// 500 com mensagem genérica: o detalhe vai só para o log, nunca para o cliente.
func (a *apiAuditoria) traduzirErro(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, dominio.ErrFiltroInvalido):
		responderErro(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, dominio.ErrNaoEncontrada):
		responderErro(w, http.StatusNotFound, "registro não encontrado")
	default:
		a.log.ErrorContext(r.Context(), "falha na consulta", "caminho", r.URL.Path, "erro", err)
		responderErro(w, http.StatusInternalServerError, "erro interno")
	}
}

// escopoDaURL lê o escopo OBRIGATÓRIO: `municipio_id=<id>` ou
// `escopo=plataforma`, exatamente um dos dois.
func escopoDaURL(q url.Values) (dominio.Escopo, error) {
	municipio := q.Get("municipio_id")
	escopo := q.Get("escopo")

	switch {
	case municipio != "" && escopo != "":
		return dominio.Escopo{}, errors.New("informe municipio_id OU escopo=plataforma, não os dois")
	case municipio != "":
		if len(municipio) > 200 {
			return dominio.Escopo{}, errors.New("municipio_id longo demais")
		}
		return dominio.Escopo{MunicipioID: municipio}, nil
	case escopo == "plataforma":
		return dominio.Escopo{Plataforma: true}, nil
	case escopo != "":
		return dominio.Escopo{}, errors.New("escopo aceita só o valor 'plataforma'")
	default:
		return dominio.Escopo{}, errors.New("escopo obrigatório: informe municipio_id ou escopo=plataforma")
	}
}

// filtroDaURL só converte texto em tipos (int, bool, time). As regras
// (padrões, limites) ficam no service.
func filtroDaURL(q url.Values) (dominio.Filtro, error) {
	escopo, err := escopoDaURL(q)
	if err != nil {
		return dominio.Filtro{}, err
	}
	f := dominio.Filtro{
		Escopo:     escopo,
		UsuarioID:  q.Get("usuario_id"),
		Entidade:   q.Get("entidade"),
		EntidadeID: q.Get("entidade_id"),
		Acao:       q.Get("acao"),
		ComAlerta:  q.Get("com_alerta") == "true",
	}

	if f.Pagina, err = inteiroOpcional(q, "pagina"); err != nil {
		return f, err
	}
	if f.Limite, err = inteiroOpcional(q, "limite"); err != nil {
		return f, err
	}
	if f.De, err = dataOpcional(q, "de"); err != nil {
		return f, err
	}
	if f.Ate, err = dataOpcional(q, "ate"); err != nil {
		return f, err
	}
	return f, nil
}

func inteiroOpcional(q url.Values, chave string) (int, error) {
	texto := q.Get(chave)
	if texto == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(texto)
	if err != nil {
		return 0, fmt.Errorf("%s deve ser um número inteiro", chave)
	}
	return n, nil
}

func dataOpcional(q url.Values, chave string) (time.Time, error) {
	texto := q.Get(chave)
	if texto == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, texto)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s deve estar no formato RFC 3339 (ex.: 2026-11-19T00:00:00Z)", chave)
	}
	return t, nil
}

func responderJSON(w http.ResponseWriter, status int, corpo any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(corpo) // o _ descarta o erro de propósito: o status já foi enviado
}

func responderErro(w http.ResponseWriter, status int, mensagem string) {
	responderJSON(w, status, map[string]string{"erro": mensagem})
}

// RodarServidor sobe a API e a desliga com calma quando o ctx é cancelado,
// esperando as requisições em andamento terminarem.
func RodarServidor(ctx context.Context, endereco string, rotas http.Handler, log *slog.Logger) error {
	servidor := &http.Server{
		Addr:              endereco,
		Handler:           rotas,
		ReadHeaderTimeout: 5 * time.Second, // protege contra clientes lentos de propósito (slowloris)
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// ListenAndServe bloqueia, então roda numa goroutine e avisa pelo canal.
	falhou := make(chan error, 1)
	go func() { falhou <- servidor.ListenAndServe() }()
	log.Info("api de consulta no ar", "endereco", endereco)

	// select espera o que acontecer primeiro: erro do servidor ou pedido de desligamento.
	select {
	case err := <-falhou:
		return fmt.Errorf("servidor http: %w", err)
	case <-ctx.Done():
		prazo, cancelar := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelar()
		return servidor.Shutdown(prazo)
	}
}
