package config

import (
	"strings"
	"testing"
)

func TestLerCatalogo(t *testing.T) {
	casos := []struct {
		nome     string
		conteudo string
		querErro bool
	}{
		{"válido", `{"solicitacao.aprovada": {"distribuir": true, "motivo_obrigatorio": true}}`, false},
		{"campo com erro de digitação", `{"solicitacao.aprovada": {"distribuír": true}}`, true},
		{"vazio", `{}`, true},
		{"ação sem nome", `{" ": {"distribuir": true}}`, true},
		{"não é JSON", `isto não é json`, true},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			_, err := lerCatalogo(strings.NewReader(c.conteudo), "teste")
			if (err != nil) != c.querErro {
				t.Fatalf("erro = %v, queria erro? %v", err, c.querErro)
			}
		})
	}
}

// O arquivo real do projeto também precisa ser válido.
func TestCatalogoDoProjetoEhValido(t *testing.T) {
	if _, err := CarregarCatalogo("../../config/acoes.json"); err != nil {
		t.Fatal(err)
	}
}
