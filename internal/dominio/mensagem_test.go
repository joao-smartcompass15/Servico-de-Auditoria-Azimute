package dominio

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func mensagemValida() Mensagem {
	return Mensagem{
		ID:          "m-1",
		Acao:        "lote.editado",
		Entidade:    "lote",
		EntidadeID:  "L-1",
		UsuarioID:   "u-1",
		TipoUsuario: "prefeitura",
		MunicipioID: "11111111-1111-4111-8111-111111111111",
		OcorridoEm:  time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC),
	}
}

func TestValidar_CamposDoContratoV2(t *testing.T) {
	longo := strings.Repeat("x", 201)

	casos := []struct {
		nome        string
		ajustar     func(*Mensagem)
		queroValida bool
	}{
		{"completa", func(*Mensagem) {}, true},
		// Ação de plataforma: sem município, sem tipo, sem requisição.
		{"sem os opcionais", func(m *Mensagem) { m.MunicipioID, m.TipoUsuario, m.RequisicaoID = "", "", "" }, true},
		{"municipio_id longo", func(m *Mensagem) { m.MunicipioID = longo }, false},
		{"tipo_usuario longo", func(m *Mensagem) { m.TipoUsuario = longo }, false},
		{"requisicao_id longo", func(m *Mensagem) { m.RequisicaoID = longo }, false},
		{"sem usuario_id", func(m *Mensagem) { m.UsuarioID = "" }, false},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			m := mensagemValida()
			c.ajustar(&m)
			err := m.Validar()
			if c.queroValida && err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if !c.queroValida && !errors.Is(err, ErrMensagemInvalida) {
				t.Fatalf("erro = %v, queria ErrMensagemInvalida", err)
			}
		})
	}
}

func TestEscopo(t *testing.T) {
	doA := Registro{Mensagem: Mensagem{MunicipioID: "A"}}
	daPlataforma := Registro{}

	if (Escopo{}).Valido() || (Escopo{MunicipioID: "A", Plataforma: true}).Valido() {
		t.Error("escopo vazio ou duplo não pode ser válido")
	}
	if !(Escopo{MunicipioID: "A"}).Pertence(doA) || (Escopo{MunicipioID: "B"}).Pertence(doA) {
		t.Error("município: pertence errado")
	}
	if !(Escopo{Plataforma: true}).Pertence(daPlataforma) || (Escopo{Plataforma: true}).Pertence(doA) {
		t.Error("plataforma: pertence errado")
	}
}
