package config

import (
	"strings"
	"testing"
)

func TestValidar_TokenDeServico(t *testing.T) {
	base := Config{KafkaBrokers: []string{"localhost:9092"}, KafkaConsumidores: 3}

	casos := []struct {
		nome     string
		token    string
		querErro bool
	}{
		{"sem token", "", true},
		{"token curto", strings.Repeat("a", TamanhoMinimoToken-1), true},
		{"token no mínimo", strings.Repeat("a", TamanhoMinimoToken), false},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			cfg := base
			cfg.TokenServico = c.token
			if err := cfg.Validar(); (err != nil) != c.querErro {
				t.Fatalf("erro = %v, queria erro? %v", err, c.querErro)
			}
		})
	}
}

func TestKafkaConsumidores(t *testing.T) {
	token := strings.Repeat("a", TamanhoMinimoToken)

	t.Setenv("KAFKA_CONSUMIDORES", "")
	if n := Carregar().KafkaConsumidores; n != 3 {
		t.Fatalf("padrão = %d, queria 3 (as partições do tópico local)", n)
	}

	casos := []struct {
		valor    string
		querErro bool
	}{
		{"1", false},
		{"32", false},
		{"0", true},
		{"33", true},
		{"três", true}, // não é número: vira -1 e o Validar recusa
	}
	for _, c := range casos {
		t.Run(c.valor, func(t *testing.T) {
			t.Setenv("KAFKA_CONSUMIDORES", c.valor)
			cfg := Carregar()
			cfg.TokenServico = token
			if err := cfg.Validar(); (err != nil) != c.querErro {
				t.Fatalf("erro = %v, queria erro? %v", err, c.querErro)
			}
		})
	}
}

func TestCarregar_TokenNaoTemPadrao(t *testing.T) {
	t.Setenv("AUDITORIA_TOKEN_SERVICO", "")
	if Carregar().TokenServico != "" {
		t.Fatal("o token não pode ter valor padrão no código")
	}
}
