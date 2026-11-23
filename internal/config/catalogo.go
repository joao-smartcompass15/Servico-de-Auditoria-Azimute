package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"api-auditoria/internal/dominio"
)

// CarregarCatalogo lê o arquivo JSON de ações. Formato:
//
//	{
//	  "solicitacao.aprovada": { "distribuir": true, "motivo_obrigatorio": true },
//	  "solicitacao.editada":  { "distribuir": false, "motivo_obrigatorio": false }
//	}
//
// É rigoroso de propósito: um erro de digitação no arquivo ("distribuír")
// impede o serviço de subir, em vez de virar uma regra silenciosamente errada.
func CarregarCatalogo(caminho string) (dominio.Catalogo, error) {
	arquivo, err := os.Open(caminho)
	if err != nil {
		return nil, fmt.Errorf("abrir catálogo de ações: %w", err)
	}
	defer arquivo.Close() // defer: roda quando a função terminar, aconteça o que acontecer

	return lerCatalogo(arquivo, caminho)
}

// lerCatalogo recebe qualquer coisa que se possa ler (arquivo, texto em
// teste...). A interface io.Reader é o que permite isso.
func lerCatalogo(leitor io.Reader, origem string) (dominio.Catalogo, error) {
	decodificador := json.NewDecoder(leitor)
	decodificador.DisallowUnknownFields() // campo desconhecido = erro

	var catalogo dominio.Catalogo
	if err := decodificador.Decode(&catalogo); err != nil {
		return nil, fmt.Errorf("catálogo %s inválido: %w", origem, err)
	}
	if len(catalogo) == 0 {
		return nil, fmt.Errorf("catálogo %s está vazio", origem)
	}
	for acao := range catalogo {
		if strings.TrimSpace(acao) == "" {
			return nil, errors.New("catálogo tem uma ação com nome vazio")
		}
	}
	return catalogo, nil
}
