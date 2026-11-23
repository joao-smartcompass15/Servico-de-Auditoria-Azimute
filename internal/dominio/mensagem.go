// Package dominio guarda os tipos centrais do serviço.
// Ele não importa nada de Kafka, Cassandra ou HTTP: é o "núcleo" do sistema,
// e todas as outras camadas dependem dele (nunca o contrário).
package dominio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Mensagem é o contrato (v2) que o gerenciador de processos e os demais
// serviços publicam no Kafka. Responde às perguntas da auditoria: quem fez,
// quando, o quê, onde (município) e por quê.
//
// O texto entre crases depois de cada campo é uma "struct tag": ela informa
// ao pacote encoding/json o nome do campo no JSON. Campos com letra
// maiúscula são públicos (exportados); só eles são lidos pelo json.
type Mensagem struct {
	ID           string          `json:"id"`                      // único por mensagem
	Acao         string          `json:"acao"`                    // O QUE fez. ex.: "solicitacao.aprovada"
	Entidade     string          `json:"entidade"`                // sobre o quê. ex.: "solicitacao"
	EntidadeID   string          `json:"entidade_id"`             // id da entidade (UUID na plataforma)
	UsuarioID    string          `json:"usuario_id"`              // QUEM fez
	TipoUsuario  string          `json:"tipo_usuario,omitempty"`  // plataforma | prefeitura | escritorio | investidor
	MunicipioID  string          `json:"municipio_id,omitempty"`  // ONDE: vazio = ação de plataforma, sem município
	OcorridoEm   time.Time       `json:"ocorrido_em"`             // QUANDO fez (RFC 3339)
	Motivo       string          `json:"motivo,omitempty"`        // POR QUE fez
	Origem       string          `json:"origem,omitempty"`        // de onde veio. ex.: "gestor-api"
	RequisicaoID string          `json:"requisicao_id,omitempty"` // X-Request-Id, para cruzar com o log da API
	Dados        json.RawMessage `json:"dados,omitempty"`         // detalhe (rota, parâmetros, campos), sempre um objeto JSON
}

// ErrMensagemInvalida é um erro "sentinela": um valor fixo com que quem chama
// pode comparar usando errors.Is. Ele indica que a mensagem tem defeito e que
// tentar processá-la de novo não adianta.
var ErrMensagemInvalida = errors.New("mensagem inválida")

// tamanhoMaximoChave limita os campos que viram chave no Cassandra e índice no
// Mongo. Um id gigante (por erro ou ataque) não deve chegar ao banco.
const tamanhoMaximoChave = 200

// Validar confere a estrutura da mensagem: campos obrigatórios e tamanhos.
// (Regras que dependem da ação, como o motivo obrigatório, ficam no service.)
//
// Go não tem exceções: a função devolve um valor do tipo error, e nil
// significa "tudo certo". O verbo %w "embrulha" o erro sentinela dentro da
// mensagem, e errors.Is(err, ErrMensagemInvalida) continua funcionando.
func (m Mensagem) Validar() error {
	// Uma lista de pares (campo, valor) permite validar todos num laço só.
	// (Um map serviria, mas a ordem de um map em Go é aleatória, e o erro
	// mudaria de uma execução para outra.)
	obrigatorios := []struct{ campo, valor string }{
		{"id", m.ID},
		{"acao", m.Acao},
		{"entidade", m.Entidade},
		{"entidade_id", m.EntidadeID},
		{"usuario_id", m.UsuarioID},
	}
	for _, o := range obrigatorios {
		if o.valor == "" {
			return fmt.Errorf("%w: %s obrigatório", ErrMensagemInvalida, o.campo)
		}
		if len(o.valor) > tamanhoMaximoChave {
			return fmt.Errorf("%w: %s passa de %d caracteres", ErrMensagemInvalida, o.campo, tamanhoMaximoChave)
		}
	}

	// Opcionais, mas também viram chave/índice: quando vêm, valem o mesmo teto.
	opcionais := []struct{ campo, valor string }{
		{"tipo_usuario", m.TipoUsuario},
		{"municipio_id", m.MunicipioID},
		{"requisicao_id", m.RequisicaoID},
	}
	for _, o := range opcionais {
		if len(o.valor) > tamanhoMaximoChave {
			return fmt.Errorf("%w: %s passa de %d caracteres", ErrMensagemInvalida, o.campo, tamanhoMaximoChave)
		}
	}

	if m.OcorridoEm.IsZero() {
		return fmt.Errorf("%w: ocorrido_em obrigatório", ErrMensagemInvalida)
	}

	// dados é opcional, mas quando vem precisa ser um objeto: {"antes": ..., "depois": ...}
	if d := bytes.TrimSpace(m.Dados); len(d) > 0 && !bytes.Equal(d, []byte("null")) && d[0] != '{' {
		return fmt.Errorf("%w: dados deve ser um objeto JSON", ErrMensagemInvalida)
	}
	return nil
}

// Registro é o que vai para a auditoria: a mensagem original mais o que o
// consumidor constatou ao processá-la.
//
// "Mensagem" sem nome de campo é um campo EMBUTIDO (embedding): os campos de
// Mensagem passam a ser acessados direto (registro.Acao) e, no JSON, aparecem
// no mesmo nível de "alertas". É a forma de Go reaproveitar estruturas.
type Registro struct {
	Mensagem
	Alertas    []string  `json:"alertas,omitempty"`
	RecebidoEm time.Time `json:"recebido_em"`
}

// Alertas: inconformidades gravadas junto com o registro. O registro NUNCA
// deixa de ser gravado por causa delas: a ação já aconteceu no site.
const (
	AlertaAcaoForaDoCatalogo = "acao_fora_do_catalogo"
	AlertaMotivoAusente      = "motivo_obrigatorio_ausente"
)
