package service

import (
	"context"
	"fmt"

	"api-auditoria/internal/dominio"
)

const (
	limitePadrao = 50
	limiteMaximo = 200
	paginaMaxima = 10_000 // paginação por "pular N" fica lenta muito fundo; para além disso, filtre por período
)

// LeituraAuditoria é a porta do lado de leitura (implementada pelo Mongo).
// As duas buscas recebem o escopo: o filtro por município acontece NO BANCO,
// não depois de ler.
type LeituraAuditoria interface {
	Buscar(ctx context.Context, f dominio.Filtro) ([]dominio.Registro, error)
	BuscarPorID(ctx context.Context, id string, escopo dominio.Escopo) (dominio.Registro, error)
}

// Consulta concentra as regras de leitura: escopo, paginação, limites e período.
type Consulta struct {
	leitura LeituraAuditoria
}

func NovaConsulta(leitura LeituraAuditoria) *Consulta {
	return &Consulta{leitura: leitura}
}

// Listar aplica os padrões e limites do filtro e busca os registros,
// do mais recente para o mais antigo.
func (c *Consulta) Listar(ctx context.Context, f dominio.Filtro) ([]dominio.Registro, error) {
	f, err := normalizar(f)
	if err != nil {
		return nil, err
	}
	return c.leitura.Buscar(ctx, f)
}

// Historico devolve a linha do tempo de uma entidade (ex.: uma solicitação):
// quem pediu, quem mexeu, quem aprovou, do mais antigo ao mais novo.
func (c *Consulta) Historico(ctx context.Context, entidade, entidadeID string, f dominio.Filtro) ([]dominio.Registro, error) {
	if entidade == "" || entidadeID == "" {
		return nil, fmt.Errorf("%w: entidade e id são obrigatórios", dominio.ErrFiltroInvalido)
	}
	f.Entidade, f.EntidadeID, f.Crescente = entidade, entidadeID, true
	return c.Listar(ctx, f)
}

// Obter devolve um registro pelo id, desde que esteja dentro do escopo.
// Registro de outro município responde igual a registro inexistente: quem
// pergunta não descobre que ele existe.
func (c *Consulta) Obter(ctx context.Context, id string, escopo dominio.Escopo) (dominio.Registro, error) {
	if id == "" {
		return dominio.Registro{}, fmt.Errorf("%w: id obrigatório", dominio.ErrFiltroInvalido)
	}
	if !escopo.Valido() {
		return dominio.Registro{}, errEscopo
	}
	r, err := c.leitura.BuscarPorID(ctx, id, escopo)
	if err != nil {
		return dominio.Registro{}, err
	}
	// Segunda trava, além do filtro no banco: um adaptador com defeito não
	// pode entregar registro de outro município.
	if !escopo.Pertence(r) {
		return dominio.Registro{}, dominio.ErrNaoEncontrada
	}
	return r, nil
}

var errEscopo = fmt.Errorf("%w: informe municipio_id ou escopo=plataforma (exatamente um)", dominio.ErrFiltroInvalido)

// normalizar recebe o filtro por valor (cópia), ajusta e devolve a versão nova.
// Quem chamou não vê o original alterado: em Go, structs são copiadas.
func normalizar(f dominio.Filtro) (dominio.Filtro, error) {
	if !f.Escopo.Valido() {
		return f, errEscopo
	}
	if f.Pagina == 0 {
		f.Pagina = 1
	}
	if f.Limite == 0 {
		f.Limite = limitePadrao
	}
	switch {
	case f.Pagina < 0 || f.Pagina > paginaMaxima:
		return f, fmt.Errorf("%w: pagina deve estar entre 1 e %d", dominio.ErrFiltroInvalido, paginaMaxima)
	case f.Limite < 0 || f.Limite > limiteMaximo:
		return f, fmt.Errorf("%w: limite deve estar entre 1 e %d", dominio.ErrFiltroInvalido, limiteMaximo)
	case !f.De.IsZero() && !f.Ate.IsZero() && !f.De.Before(f.Ate):
		return f, fmt.Errorf("%w: 'de' deve ser anterior a 'ate'", dominio.ErrFiltroInvalido)
	}
	return f, nil
}
