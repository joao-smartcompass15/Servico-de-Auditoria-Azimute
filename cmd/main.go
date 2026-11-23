// Ponto de entrada do serviço de auditoria.
//
// O main só MONTA as peças (injeção de dependência feita à mão) e liga dois
// ramos independentes, cada um esperando o próprio banco:
//
//  1. Gravação (Cassandra): lê o Kafka, audita TUDO e, se o catálogo de
//     ações mandar, distribui aos usuários.
//  2. Leitura (Mongo): a projeção lê o MESMO tópico com outro consumer group
//     e grava tudo no Mongo (o Kafka é a fonte única: os bancos não se
//     sincronizam entre si); e a API HTTP de consulta, interna — só a API do
//     gerenciador a chama, com o token de serviço e o município do usuário.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"api-auditoria/internal/config"
	"api-auditoria/internal/database"
	"api-auditoria/internal/distribuicao"
	"api-auditoria/internal/dominio"
	"api-auditoria/internal/handlers"
	"api-auditoria/internal/service"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := rodar(log); err != nil {
		log.Error("serviço encerrado com erro", "erro", err)
		os.Exit(1)
	}
	log.Info("serviço encerrado")
}

// rodar fica separado do main para podermos usar "return err" e defer.
// (os.Exit encerra na hora e pularia os defers que fecham as conexões.)
func rodar(log *slog.Logger) error {
	cfg := config.Carregar()
	if err := cfg.Validar(); err != nil {
		return err
	}

	// ctx é cancelado no Ctrl+C (SIGINT) ou quando o Docker para o container (SIGTERM).
	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	catalogo, err := config.CarregarCatalogo(cfg.CatalogoAcoes)
	if err != nil {
		return err
	}
	log.Info("catálogo de ações carregado", "arquivo", cfg.CatalogoAcoes, "acoes", len(catalogo))

	log.Info("serviço iniciado", "topico", cfg.KafkaTopico, "brokers", cfg.KafkaBrokers,
		"leitores_por_grupo", cfg.KafkaConsumidores)

	// DOIS RAMOS INDEPENDENTES, cada um esperando o PRÓPRIO banco. Até a carga
	// de 23/11/2026 o serviço conectava nos dois bancos antes de tudo: com o
	// Cassandra fora, ele não subia, e a API de consulta (que só usa o Mongo)
	// caía junto, em loop de reinício. Agora:
	//   - Cassandra fora: a gravação espera; a projeção e a API seguem no ar.
	//   - Mongo fora: a projeção e a API esperam; a gravação da auditoria segue.
	return rodarJuntas(ctx,
		func(ctx context.Context) error { return ramoGravacao(ctx, cfg, catalogo, log) },
		func(ctx context.Context) error { return ramoLeitura(ctx, cfg, catalogo, log) },
	)
}

// ramoGravacao: conecta no Cassandra (insistindo) e roda os leitores do grupo
// de gravação, que auditam e distribuem.
func ramoGravacao(ctx context.Context, cfg config.Config, catalogo dominio.Catalogo, log *slog.Logger) error {
	log = log.With("ramo", "gravacao")
	sessao, err := comInsistencia(ctx, log, "cassandra", func(context.Context) (*gocql.Session, error) {
		return database.ConectarCassandra(database.ConfigCassandra{
			Hosts:    cfg.CassandraHosts,
			Keyspace: cfg.CassandraKeyspace,
			Usuario:  cfg.CassandraUsuario,
			Senha:    cfg.CassandraSenha,
		})
	})
	if err != nil {
		return nil // só sai daqui com erro se o serviço estiver desligando
	}
	defer sessao.Close()

	gravacao := service.NovoProcessador(
		database.NovaAuditoriaCassandra(sessao),
		distribuicao.NovoDistribuidorEmLog(log), // trocar pelo canal real de entrega
		catalogo,
	)
	return rodarJuntas(ctx, leitores(cfg, cfg.KafkaGrupo, gravacao, log)...)
}

// ramoLeitura: conecta no Mongo (insistindo) e roda os leitores do grupo de
// projeção e a API de consulta.
func ramoLeitura(ctx context.Context, cfg config.Config, catalogo dominio.Catalogo, log *slog.Logger) error {
	log = log.With("ramo", "leitura")
	type mongoPronto struct {
		cliente *mongo.Client
		acoes   *database.AuditoriaMongo
	}
	m, err := comInsistencia(ctx, log, "mongo", func(ctx context.Context) (mongoPronto, error) {
		cliente, err := database.ConectarMongo(ctx, cfg.MongoURI)
		if err != nil {
			return mongoPronto{}, err
		}
		acoes, err := database.NovaAuditoriaMongo(ctx, cliente.Database(cfg.MongoBanco))
		if err != nil {
			_ = cliente.Disconnect(context.Background())
			return mongoPronto{}, err
		}
		return mongoPronto{cliente, acoes}, nil
	})
	if err != nil {
		return nil
	}
	defer m.cliente.Disconnect(context.Background())

	projecao := service.NovoProcessador(m.acoes, distribuicao.Ignorar{}, catalogo)
	api := handlers.NovaAPI(service.NovaConsulta(m.acoes), cfg.TokenServico, log)

	tarefas := leitores(cfg, cfg.KafkaGrupoProjecao, projecao, log)
	tarefas = append(tarefas, func(ctx context.Context) error {
		return handlers.RodarServidor(ctx, cfg.HTTPEndereco, api, log)
	})
	return rodarJuntas(ctx, tarefas...)
}

// leitores monta N leitores do mesmo grupo, cada um numa tarefa. O Kafka dá
// cada partição a UM leitor do grupo: as partições andam em paralelo, e dentro
// de cada uma a ordem se mantém (a mesma entidade cai sempre na mesma
// partição, pela chave). O processador é compartilhado: as sessões do
// Cassandra e do Mongo são seguras para uso concorrente, e o catálogo só é lido.
func leitores(cfg config.Config, grupo string, processador handlers.Processador, log *slog.Logger) []func(context.Context) error {
	tarefas := make([]func(context.Context) error, cfg.KafkaConsumidores)
	for i := range tarefas {
		tarefas[i] = func(ctx context.Context) error {
			leitor := novoLeitor(cfg, grupo)
			defer leitor.Close() // fechar também envia os offsets que estão na fila
			return handlers.NovoConsumidorKafka(leitor, processador, log.With("leitor", i)).Rodar(ctx)
		}
	}
	return tarefas
}

// comInsistencia tenta conectar até conseguir, com espera crescente (1 s, 2 s,
// 4 s... até 30 s). Só desiste se o serviço estiver desligando.
//
// [T any] é um parâmetro de TIPO (generics, Go 1.18+): a mesma função serve
// para a sessão do Cassandra e para o cliente do Mongo, e o compilador sabe o
// tipo exato do que volta, sem conversão.
func comInsistencia[T any](ctx context.Context, log *slog.Logger, banco string, conectar func(context.Context) (T, error)) (T, error) {
	espera := time.Second
	for tentativa := 1; ; tentativa++ {
		valor, err := conectar(ctx)
		if err == nil {
			log.Info("conectado", "banco", banco, "tentativas", tentativa)
			return valor, nil
		}
		var zero T // o "valor vazio" do tipo T, para devolver junto com o erro
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		log.Warn("banco indisponível; tentando de novo", "banco", banco,
			"tentativa", tentativa, "proxima_em", espera.String(), "erro", err)
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(espera):
		}
		espera = min(espera*2, 30*time.Second)
	}
}

func novoLeitor(cfg config.Config, grupo string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopico,
		GroupID:     grupo,
		StartOffset: kafka.FirstOffset, // grupo novo começa do início do tópico

		// Confirma os offsets em LOTE, uma vez por segundo, em vez de uma ida e
		// volta ao Kafka por mensagem (era o gargalo: ~100 mensagens/s).
		// Continua "pelo menos uma vez": só entra na fila de confirmação o que já
		// foi gravado. Se o serviço morrer antes do envio, até 1 s de mensagens é
		// relido ao voltar, e a gravação pelo id não duplica nada.
		CommitInterval: time.Second,
	})
}

// rodarJuntas roda cada tarefa na sua própria goroutine (uma "thread leve" do
// Go). Quando a primeira termina, por erro ou desligamento, cancela as outras
// e espera todas saírem antes de retornar.
//
// "tarefas ...func" é um parâmetro variádico: aceita quantas funções vierem.
func rodarJuntas(ctx context.Context, tarefas ...func(context.Context) error) error {
	ctx, cancelar := context.WithCancel(ctx)
	defer cancelar()

	// Canal com buffer do tamanho exato: nenhuma goroutine fica travada ao enviar.
	terminou := make(chan error, len(tarefas))
	for _, tarefa := range tarefas {
		go func() { terminou <- tarefa(ctx) }()
	}

	primeiroErro := <-terminou // espera a primeira terminar
	cancelar()                 // avisa as outras para pararem
	for range len(tarefas) - 1 {
		if err := <-terminou; primeiroErro == nil {
			primeiroErro = err
		}
	}
	if primeiroErro != nil {
		return fmt.Errorf("tarefa encerrada: %w", primeiroErro)
	}
	return nil
}
