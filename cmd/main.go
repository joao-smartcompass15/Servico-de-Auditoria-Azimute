// Ponto de entrada do serviço de auditoria.
//
// O main só MONTA as peças (injeção de dependência feita à mão) e liga três
// tarefas em paralelo:
//
//  1. Gravação: lê o Kafka, audita TUDO no Cassandra e, se o catálogo de
//     ações mandar, distribui aos usuários.
//  2. Projeção: lê o MESMO tópico com outro consumer group e grava tudo no
//     Mongo. Assim o Kafka é a fonte única e os dois bancos não precisam de
//     sincronização entre si.
//  3. API HTTP de consulta, que lê do Mongo. É interna: só a API do
//     gerenciador a chama, com o token de serviço e o município do usuário.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"api-auditoria/internal/config"
	"api-auditoria/internal/database"
	"api-auditoria/internal/distribuicao"
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

	// --- Conexões -----------------------------------------------------------
	sessaoCassandra, err := database.ConectarCassandra(database.ConfigCassandra{
		Hosts:    cfg.CassandraHosts,
		Keyspace: cfg.CassandraKeyspace,
		Usuario:  cfg.CassandraUsuario,
		Senha:    cfg.CassandraSenha,
	})
	if err != nil {
		return err
	}
	defer sessaoCassandra.Close()

	clienteMongo, err := database.ConectarMongo(ctx, cfg.MongoURI)
	if err != nil {
		return err
	}
	defer clienteMongo.Disconnect(context.Background())

	acoesMongo, err := database.NovaAuditoriaMongo(ctx, clienteMongo.Database(cfg.MongoBanco))
	if err != nil {
		return err
	}

	// --- Montagem: adaptadores → services → handlers ------------------------
	gravacao := service.NovoProcessador(
		database.NovaAuditoriaCassandra(sessaoCassandra),
		distribuicao.NovoDistribuidorEmLog(log), // trocar pelo canal real de entrega
		catalogo,
	)
	projecao := service.NovoProcessador(acoesMongo, distribuicao.Ignorar{}, catalogo)

	leitorGravacao := novoLeitor(cfg, cfg.KafkaGrupo)
	defer leitorGravacao.Close()
	leitorProjecao := novoLeitor(cfg, cfg.KafkaGrupoProjecao)
	defer leitorProjecao.Close()

	consumidorGravacao := handlers.NovoConsumidorKafka(leitorGravacao, gravacao, log.With("tarefa", "gravacao"))
	consumidorProjecao := handlers.NovoConsumidorKafka(leitorProjecao, projecao, log.With("tarefa", "projecao"))
	api := handlers.NovaAPI(service.NovaConsulta(acoesMongo), cfg.TokenServico, log)

	log.Info("serviço iniciado", "topico", cfg.KafkaTopico, "brokers", cfg.KafkaBrokers)

	return rodarJuntas(ctx,
		consumidorGravacao.Rodar,
		consumidorProjecao.Rodar,
		func(ctx context.Context) error { return handlers.RodarServidor(ctx, cfg.HTTPEndereco, api, log) },
	)
}

func novoLeitor(cfg config.Config, grupo string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopico,
		GroupID:     grupo,
		StartOffset: kafka.FirstOffset, // grupo novo começa do início do tópico
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
