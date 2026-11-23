// Comando carga: despeja mensagens FICTÍCIAS no tópico de auditoria para
// medir vazão e testar queda (o "espancar"). Só para ambiente local ou de
// homologação. Nunca aponte para o Kafka de produção.
//
// Uso:
//
//	go run ./cmd/carga -total 20000
//	go run ./cmd/carga -total 50000 -municipios 5 -entidades 300
//
// Cada execução usa municípios NOVOS (UUIDs aleatórios), impressos no fim. É
// por eles que se confere no Mongo se tudo chegou, sem misturar com execuções
// anteriores.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// As ações sorteadas: todas do catálogo, metade com motivo obrigatório.
var acoes = []struct {
	nome           string
	entidade       string
	exigeMotivo    bool
	foraCatalogo   bool
	distribuivel   bool
	camposEnviados []string
}{
	{"processo.ficha_editada", "processo", true, false, false, []string{"motivo", "valorTotal"}},
	{"processo.parte_incluida", "processo", true, false, false, []string{"motivo", "nome", "papel"}},
	{"lote.editado", "lote", false, false, false, []string{"nome", "prazo"}},
	{"cda.pagamento_incluido", "cda", true, false, false, []string{"motivo", "valor"}},
	{"solicitacao.criada", "solicitacao", false, false, true, []string{"tipoId", "texto"}},
	{"solicitacao.reprovada", "solicitacao", true, false, true, []string{"motivo"}},
	{"usuario.editado", "usuario", false, false, false, []string{"nome", "papeis"}},
	{"acao.inventada_pela_carga", "processo", false, true, false, []string{"x"}},
}

func main() {
	brokers := flag.String("brokers", "localhost:9092,localhost:9094", "brokers do Kafka, separados por vírgula")
	topico := flag.String("topico", "azimute.mensagens", "tópico de auditoria")
	total := flag.Int("total", 20000, "quantas mensagens enviar")
	municipios := flag.Int("municipios", 5, "quantos municípios fictícios")
	entidades := flag.Int("entidades", 200, "quantas entidades fictícias por município")
	lote := flag.Int("lote", 500, "mensagens por envio ao Kafka")
	flag.Parse()

	if strings.Contains(*brokers, "dokploy") || strings.Contains(*brokers, "prod") {
		log.Fatal("recusado: a carga é só para ambiente local ou de homologação")
	}

	// Municípios e entidades fictícios desta execução.
	ids := make([]string, *municipios)
	for i := range ids {
		ids[i] = novoUUID()
	}
	porMunicipio := make([][]string, *municipios)
	for m := range porMunicipio {
		porMunicipio[m] = make([]string, *entidades)
		for e := range porMunicipio[m] {
			porMunicipio[m][e] = novoUUID()
		}
	}

	escritor := &kafka.Writer{
		Addr:            kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:           *topico,
		Balancer:        &kafka.Hash{}, // mesma chave → mesma partição → ordem por entidade
		RequiredAcks:    kafka.RequireAll,
		BatchSize:       *lote,
		BatchTimeout:    50 * time.Millisecond,
		MaxAttempts:     20, // aguenta a troca de líder quando um broker cai
		WriteBackoffMin: 200 * time.Millisecond,
		WriteBackoffMax: 2 * time.Second,
	}
	defer escritor.Close()

	var validas, invalidas, naoJSON, semMotivo, foraCatalogo, distribuiveis, reenvios int
	inicio := time.Now()
	ctx := context.Background()
	buffer := make([]kafka.Message, 0, *lote)

	for n := 0; n < *total; n++ {
		m := sortear(*municipios)
		entidadeID := porMunicipio[m][sortear(*entidades)]
		a := acoes[sortear(len(acoes))]

		var valor []byte
		switch sorteio := sortear(1000); {
		case sorteio < 5: // 0,5%: lixo que não é JSON
			valor = []byte("{isto nao e json " + fmt.Sprint(n))
			naoJSON++
		case sorteio < 15: // 1%: sem usuario_id (inválida, descartada)
			valor = mensagem(a.nome, a.entidade, entidadeID, ids[m], "", "", a.camposEnviados)
			invalidas++
		default:
			motivo := ""
			if a.exigeMotivo && sortear(10) < 8 { // 20% das que exigem vêm sem motivo
				motivo = "conferido na carga de teste"
			}
			if a.exigeMotivo && motivo == "" {
				semMotivo++
			}
			if a.foraCatalogo {
				foraCatalogo++
			}
			if a.distribuivel {
				distribuiveis++
			}
			valor = mensagem(a.nome, a.entidade, entidadeID, ids[m], novoUUID(), motivo, a.camposEnviados)
			validas++
		}

		buffer = append(buffer, kafka.Message{Key: []byte(a.entidade + ":" + entidadeID), Value: valor})
		if len(buffer) == *lote || n == *total-1 {
			reenvios += enviarComInsistencia(ctx, escritor, buffer)
			buffer = buffer[:0]
		}
		if (n+1)%10000 == 0 {
			log.Printf("%d enviadas (%.0f/s)", n+1, float64(n+1)/time.Since(inicio).Seconds())
		}
	}

	duracao := time.Since(inicio)
	resumo := map[string]any{
		"total":                    *total,
		"validas":                  validas,
		"invalidas_sem_usuario":    invalidas,
		"nao_json":                 naoJSON,
		"com_alerta_sem_motivo":    semMotivo,
		"com_alerta_fora_catalogo": foraCatalogo,
		"distribuiveis":            distribuiveis,
		"lotes_reenviados":         reenvios,
		"segundos":                 duracao.Seconds(),
		"por_segundo":              float64(*total) / duracao.Seconds(),
		"municipios":               ids,
	}
	saida, _ := json.MarshalIndent(resumo, "", "  ")
	fmt.Println(string(saida))
	if err := os.WriteFile("carga-ultima.json", saida, 0o644); err != nil {
		log.Printf("não gravei carga-ultima.json: %v", err)
	}
}

// enviarComInsistencia manda o lote e, se o Kafka recusar (um broker caiu e o
// líder ainda está mudando), manda o LOTE INTEIRO de novo, com espera
// crescente, por até 3 minutos. Parte do lote pode ter entrado na primeira
// tentativa e entrar de novo: o consumidor grava pelo id, então não duplica.
// Devolve quantas vezes precisou reenviar.
func enviarComInsistencia(ctx context.Context, escritor *kafka.Writer, lote []kafka.Message) int {
	espera := 500 * time.Millisecond
	limite := time.Now().Add(3 * time.Minute)
	for reenvios := 0; ; reenvios++ {
		err := escritor.WriteMessages(ctx, lote...)
		if err == nil {
			return reenvios
		}
		if time.Now().After(limite) {
			log.Fatalf("envio falhou por 3 minutos seguidos: %v", resumirErro(err))
		}
		log.Printf("lote recusado (%s); reenviando em %s", resumirErro(err), espera)
		time.Sleep(espera)
		espera = min(espera*2, 10*time.Second)
	}
}

// resumirErro corta o erro do kafka-go, que repete a mesma causa por mensagem.
func resumirErro(err error) string {
	texto := err.Error()
	if len(texto) > 160 {
		texto = texto[:160] + "..."
	}
	return texto
}

// mensagem monta o JSON no contrato v2. usuario vazio = mensagem inválida.
func mensagem(acao, entidade, entidadeID, municipio, usuario, motivo string, campos []string) []byte {
	m := map[string]any{
		"id":            novoUUID(),
		"acao":          acao,
		"entidade":      entidade,
		"entidade_id":   entidadeID,
		"tipo_usuario":  "prefeitura",
		"municipio_id":  municipio,
		"ocorrido_em":   time.Now().UTC().Format(time.RFC3339Nano),
		"origem":        "carga",
		"requisicao_id": "carga-" + novoUUID()[:8],
		"dados": map[string]any{
			"rota":       "PATCH /carga/:id",
			"parametros": map[string]string{"id": entidadeID},
			"campos":     campos,
		},
	}
	if usuario != "" {
		m["usuario_id"] = usuario
	}
	if motivo != "" {
		m["motivo"] = motivo
	}
	b, _ := json.Marshal(m)
	return b
}

// sortear devolve um inteiro em [0, n). crypto/rand para não depender de semente.
func sortear(n int) int {
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		panic(err)
	}
	return int(v.Int64())
}

// novoUUID gera um UUID v4 sem dependência externa: 16 bytes aleatórios com os
// bits de versão (4) e de variante (10) ajustados.
func novoUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
