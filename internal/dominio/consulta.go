package dominio

import (
	"errors"
	"time"
)

// Escopo diz de QUEM são os registros que a consulta pode ver. É obrigatório:
// a auditoria guarda vários municípios juntos, e nenhuma leitura atravessa
// municípios (a mesma regra da RLS do gerenciador).
//
// Exatamente um dos dois:
//   - MunicipioID preenchido: só os registros daquele município;
//   - Plataforma true: só as ações de plataforma (sem município).
type Escopo struct {
	MunicipioID string
	Plataforma  bool
}

// Valido diz se o escopo tem exatamente uma das duas formas.
func (e Escopo) Valido() bool {
	return (e.MunicipioID != "") != e.Plataforma // "!=" entre bools é o "ou exclusivo"
}

// Pertence diz se um registro está dentro do escopo. Usado para conferir a
// leitura por id: registro fora do escopo é tratado como inexistente.
func (e Escopo) Pertence(r Registro) bool {
	if e.Plataforma {
		return r.MunicipioID == ""
	}
	return r.MunicipioID == e.MunicipioID
}

// Filtro descreve uma consulta à auditoria. Campo vazio = não filtra por ele.
// O Escopo é a exceção: é obrigatório (ver acima).
type Filtro struct {
	Escopo     Escopo
	UsuarioID  string
	Entidade   string
	EntidadeID string
	Acao       string
	ComAlerta  bool      // só registros com alguma inconformidade
	De         time.Time // inclusivo
	Ate        time.Time // exclusivo
	Crescente  bool      // true = mais antigo primeiro (linha do tempo)
	Pagina     int       // começa em 1
	Limite     int       // itens por página
}

var (
	// ErrFiltroInvalido: o cliente pediu algo inválido (vira HTTP 400).
	ErrFiltroInvalido = errors.New("filtro inválido")

	// ErrNaoEncontrada: o registro pedido não existe, ou existe fora do
	// escopo de quem pediu (vira HTTP 404 nos dois casos, de propósito).
	ErrNaoEncontrada = errors.New("registro não encontrado")
)
