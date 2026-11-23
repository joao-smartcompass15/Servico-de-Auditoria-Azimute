// Package config lê a configuração das variáveis de ambiente.
// Em produção o Dokploy injeta esses valores; nada fica fixo no código.
// Os padrões abaixo servem só para rodar local com o docker-compose.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers       []string // ex.: "kafka1:9092,kafka2:9092"
	KafkaTopico        string
	KafkaGrupo         string // consumer group da gravação (Cassandra + distribuição)
	KafkaGrupoProjecao string // consumer group da projeção de leitura (Mongo)

	CassandraHosts    []string
	CassandraKeyspace string
	CassandraUsuario  string
	CassandraSenha    string

	MongoURI   string
	MongoBanco string

	HTTPEndereco string

	// TokenServico é o segredo que a API do gerenciador apresenta para
	// consultar a auditoria. É CREDENCIAL: vem do ambiente (Dokploy / .env),
	// nunca do código, e nunca vai para o log.
	TokenServico string

	CatalogoAcoes string // caminho do JSON com as regras de cada ação
}

// TamanhoMinimoToken: abaixo disso o token é adivinhável por força bruta.
const TamanhoMinimoToken = 32

// Validar recusa uma configuração que deixaria o serviço inseguro. É chamada
// no início do main: melhor não subir do que subir aberto.
func (c Config) Validar() error {
	if len(c.TokenServico) < TamanhoMinimoToken {
		return fmt.Errorf("AUDITORIA_TOKEN_SERVICO é obrigatória e precisa de pelo menos %d caracteres", TamanhoMinimoToken)
	}
	if len(c.KafkaBrokers) == 0 {
		return errors.New("KAFKA_BROKERS vazia")
	}
	return nil
}

// Carregar devolve a configuração, com valores padrão para rodar local.
func Carregar() Config {
	return Config{
		KafkaBrokers:       lista(ler("KAFKA_BROKERS", "localhost:9092,localhost:9094")),
		KafkaTopico:        ler("KAFKA_TOPICO", "azimute.mensagens"),
		KafkaGrupo:         ler("KAFKA_GRUPO", "servico-auditoria"),
		KafkaGrupoProjecao: ler("KAFKA_GRUPO_PROJECAO", "servico-auditoria-projecao"),

		CassandraHosts:    lista(ler("CASSANDRA_HOSTS", "localhost")),
		CassandraKeyspace: ler("CASSANDRA_KEYSPACE", "auditoria"),
		CassandraUsuario:  ler("CASSANDRA_USUARIO", ""),
		CassandraSenha:    ler("CASSANDRA_SENHA", ""),

		MongoURI:   ler("MONGO_URI", "mongodb://localhost:27017"),
		MongoBanco: ler("MONGO_BANCO", "auditoria"),

		HTTPEndereco: ler("HTTP_ENDERECO", ":8080"),

		TokenServico: ler("AUDITORIA_TOKEN_SERVICO", ""), // sem padrão de propósito

		CatalogoAcoes: ler("CATALOGO_ACOES", "config/acoes.json"),
	}
}

func ler(chave, padrao string) string {
	// "v, ok :=" é o padrão "comma ok" de Go: ok diz se a variável existe.
	if v, ok := os.LookupEnv(chave); ok && v != "" {
		return v
	}
	return padrao
}

// lista transforma "a, b,c" em []string{"a", "b", "c"}.
func lista(texto string) []string {
	var itens []string
	for item := range strings.SplitSeq(texto, ",") {
		if item = strings.TrimSpace(item); item != "" {
			itens = append(itens, item)
		}
	}
	return itens
}
