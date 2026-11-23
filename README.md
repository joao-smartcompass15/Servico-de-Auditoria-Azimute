# servico-auditoria

Consome as mensagens que o gerenciador de processos (Azimute) e os demais serviços publicam no Kafka e:

1. **Audita tudo**: quem fez, quando, o quê, em qual município e por quê. A escrita vai para o **Cassandra** e a leitura sai do **Mongo**.
2. **Distribui aos usuários** as ações que o **catálogo** (`config/acoes.json`) manda distribuir. *O canal de entrega ainda não foi definido; por enquanto a distribuição só aparece no log.*

> Banco: Cassandra + Mongo é uma decisão deste projeto e uma exceção ao padrão da empresa ("produção em PostgreSQL").

## Como funciona

```
                          ┌── grupo "servico-auditoria" ──────────▶ audita no Cassandra
gerenciador ─▶ Kafka ─────┤                                          + distribui (se o catálogo mandar)
 (saída da    (tópico     └── grupo "servico-auditoria-projecao" ─▶ grava no Mongo ──▶ API interna de consulta
  auditoria)   azimute.                                                                  ▲
               mensagens)                                     API do gerenciador ────────┘
                                                              (auditoria.ler + município do token)
```

- Os dois consumer groups leem o **mesmo tópico** de forma independente: o Kafka é a fonte única e Cassandra e Mongo não precisam se sincronizar.
- O offset só é confirmado depois que a gravação dá certo. Se o banco cair, a mensagem é relida ao reiniciar.
- As gravações usam o `id` da mensagem como chave, então reprocessar não duplica.
- Ordem: **audita primeiro, distribui depois**. Nada é distribuído sem estar auditado.

### Alertas (inconformidades)

O registro é **sempre gravado**, porque a ação já aconteceu na plataforma. Quando algo foge da regra, ele vai com um alerta:

| Alerta | Quando |
|---|---|
| `motivo_obrigatorio_ausente` | a ação exige motivo no catálogo e ele veio vazio |
| `acao_fora_do_catalogo` | a ação não está no catálogo (é auditada, mas não é distribuída) |

Mensagem sem campo obrigatório (`id`, `acao`, `entidade`, `entidade_id`, `usuario_id`, `ocorrido_em`) ou com JSON quebrado é **descartada** e registrada no log.

## Catálogo de ações (`config/acoes.json`)

Uma entrada por ação que a plataforma emite. O nome segue o padrão **`entidade.particípio`**, concordando com o gênero (decisão de 23/11/2026): `lote.criado`, `processo.ficha_editada`, `solicitacao.reprovada`, `usuario.excluido`.

```json
{
  "solicitacao.criada":     { "distribuir": true,  "motivo_obrigatorio": false },
  "processo.ficha_editada": { "distribuir": false, "motivo_obrigatorio": true }
}
```

- `motivo_obrigatorio` acompanha as rotas em que a própria plataforma já exige motivo (`@Motivo`). Marcar `true` numa ação em que a tela não pede motivo faz **toda** ocorrência dela chegar com alerta.
- É lido na subida do serviço. Campo com erro de digitação impede o serviço de subir, de propósito.
- No Docker, o arquivo vai embutido na imagem. Para trocar sem rebuild, monte outro arquivo e aponte `CATALOGO_ACOES` para ele.

## Contrato da mensagem (v2)

Publicada no tópico `azimute.mensagens`, com a chave Kafka `<entidade>:<entidade_id>` (mantém a ordem por entidade).

```json
{
  "id": "uuid único",
  "acao": "lote.editado",
  "entidade": "lote",
  "entidade_id": "uuid",
  "usuario_id": "uuid",
  "tipo_usuario": "plataforma | prefeitura | escritorio | investidor",
  "municipio_id": "uuid (ausente ou vazio = ação de plataforma, sem município)",
  "ocorrido_em": "2026-11-23T10:00:00Z",
  "motivo": "opcional",
  "origem": "gestor-api",
  "requisicao_id": "opcional (X-Request-Id)",
  "dados": { "rota": "PATCH /lote/:id", "parametros": { "id": "..." }, "campos": ["nome", "prazo"] }
}
```

- `tipo_usuario`, `municipio_id`, `motivo`, `origem`, `requisicao_id` e `dados` são opcionais. Quando vêm, os de identificação têm no máximo 200 caracteres.
- `dados` precisa ser um objeto JSON. A plataforma manda **nomes** de campo, nunca valores (podem ser dado pessoal).
- No Mongo, `dados` vira documento consultável, por exemplo `db.historico.find({"dados.rota": "PATCH /lote/:id"})`.

## API de consulta (interna)

**Quem chama é a API do gerenciador, e mais ninguém.** Ela confere a permissão `auditoria.ler` do usuário e põe o município do token dele na consulta. Por isso esta API **não entra no Kong** e a porta não é publicada em produção.

Duas travas, nas três rotas:

1. **Token de serviço**: `Authorization: Bearer <AUDITORIA_TOKEN_SERVICO>`. Sem ele, ou com ele errado, responde **401**. Só `/healthz` fica aberta.
2. **Escopo obrigatório**: `municipio_id=<id>` **ou** `escopo=plataforma`, exatamente um. Sem escopo, responde **400**. Registro de outro município responde **404**, igual a um registro que não existe.

| Rota | O que devolve |
|---|---|
| `GET /api/v1/auditoria` | lista, mais recente primeiro |
| `GET /api/v1/auditoria/{id}` | um registro |
| `GET /api/v1/entidades/{entidade}/{entidade_id}/historico` | linha do tempo da entidade, mais antigo primeiro |
| `GET /healthz` | saúde (aberta) |

Filtros da lista: `usuario_id`, `entidade`, `entidade_id`, `acao`, `com_alerta=true`, `de` e `ate` (RFC 3339; `de` inclusivo, `ate` exclusivo), `pagina` (padrão 1) e `limite` (padrão 50, máximo 200).

## Onde fica cada coisa

| Banco | Estrutura | Responde |
|---|---|---|
| Cassandra `auditoria` | `historico_por_entidade` | linha do tempo de uma entidade (quem pediu, mexeu, aprovou) |
| Cassandra `auditoria` | `acoes_por_usuario` | o que um usuário fez num dia |
| Mongo `auditoria` | coleção `historico` | todas as consultas da API (índices com `municipio_id` na frente) |

Schema do Cassandra: `schema/cassandra.cql`. Pode rodar de novo à vontade: as colunas da v2 entram com `ALTER TABLE ... ADD IF NOT EXISTS`. O `municipio_id` fica fora da chave, porque os ids da plataforma são UUID globais.

| Pasta | Papel |
|---|---|
| `cmd/` | `main.go`: monta as peças e liga as três tarefas |
| `internal/dominio` | tipos centrais (`Mensagem`, `Registro`, `Catalogo`, `Filtro`, `Escopo`) |
| `internal/service` | regra de negócio (auditar, distribuir, consultar), testável sem infraestrutura |
| `internal/handlers` | entradas: consumidor Kafka e API HTTP (token de serviço + escopo) |
| `internal/database` | adaptadores Cassandra e Mongo |
| `internal/distribuicao` | entrega aos usuários (provisório: log) |
| `internal/config` | variáveis de ambiente e catálogo |

## Kafka

### Produção: o Kafka do Dokploy

Em produção, o serviço **consome o Kafka que já roda no Dokploy**. Nada de Kafka sobe junto com ele: basta apontar `KAFKA_BROKERS` para os endereços desse Kafka. O tópico `azimute.mensagens` precisa existir lá (com replicação, se o Kafka do Dokploy tiver mais de um broker).

### Local: dois brokers e um nó de controle

O `docker-compose.yml` sobe um cluster **só para testar na máquina**:

- **`kafka1` e `kafka2`**: os brokers. Cada mensagem fica copiada nos dois.
- **`kafka-controle`**: um nó pequeno que só participa da votação (o KRaft precisa da maioria dos votantes de pé; com 3, aguenta perder 1).

Testado local (19/11/2026), derrubando o `kafka1`: as partições passaram para o `kafka2` sozinhas, a auditoria gravou a ação feita durante a queda em menos de 7 s e, ao religar, o `kafka1` recuperou as cópias em cerca de 20 s.

## Rodar local

Pré-requisito: Docker Desktop ligado e um `.env` com o token de serviço (modelo em `.env.example`; o `.env` não vai para o git).

```powershell
Copy-Item .env.example .env            # e troque o valor por um token aleatório
docker compose up -d --build           # sobe Kafka, Cassandra, Mongo e o serviço
docker compose logs -f servico-auditoria
```

O Cassandra leva cerca de um minuto para ficar pronto. Para rodar o serviço direto na máquina (útil para depurar no VS Code), defina `AUDITORIA_TOKEN_SERVICO` no terminal e:

```powershell
docker compose up -d kafka-init cassandra-init mongo
go run ./cmd
```

### Publicar mensagens de teste

```powershell
Get-Content -Encoding UTF8 exemplos\mensagens.jsonl | docker compose exec -T kafka1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka1:19092,kafka2:19092 --topic azimute.mensagens
```

> O `-Encoding UTF8` é necessário: sem ele, o PowerShell 5.1 lê o arquivo como ANSI e estraga os acentos antes de enviar.

O arquivo tem dois municípios fictícios (A = `aaaaaaaa-…-00000000000a`, B = `bbbbbbbb-…-00000000000b`): um lote criado e duas edições de ficha no A (uma **sem** motivo), uma solicitação criada e reprovada no B, uma prefeitura criada pela plataforma (sem município), uma ação fora do catálogo, uma mensagem sem `usuario_id` e uma linha que não é JSON.

### Consultar

```powershell
# Lê o token do .env para uma variável, sem mostrar na tela
$token = ((Get-Content .env | Select-String '^AUDITORIA_TOKEN_SERVICO=').Line -split '=', 2)[1]
$auth = "Authorization: Bearer $token"
$A = "aaaaaaaa-0000-4000-8000-00000000000a"

curl.exe -H $auth "http://localhost:8080/api/v1/auditoria?municipio_id=$A"
curl.exe -H $auth "http://localhost:8080/api/v1/entidades/processo/20000000-0000-4000-8000-000000000002/historico?municipio_id=$A"
curl.exe -H $auth "http://localhost:8080/api/v1/auditoria?municipio_id=$A&com_alerta=true"   # inconformidades
curl.exe -H $auth "http://localhost:8080/api/v1/auditoria?escopo=plataforma"
```

> Ao conferir no `cqlsh` via `docker compose exec -T`, os acentos aparecem quebrados na tela. O dado gravado está correto; é só a exibição do `cqlsh` sem terminal.

### Testes

```powershell
go test ./...
```

## Variáveis de ambiente

| Variável | Padrão (local) |
|---|---|
| `AUDITORIA_TOKEN_SERVICO` | **sem padrão, obrigatória** (mínimo 32 caracteres). O mesmo valor vai na API do gerenciador |
| `KAFKA_BROKERS` | `localhost:9092,localhost:9094` (produção: o Kafka do Dokploy) |
| `KAFKA_TOPICO` | `azimute.mensagens` |
| `KAFKA_GRUPO` | `servico-auditoria` |
| `KAFKA_GRUPO_PROJECAO` | `servico-auditoria-projecao` |
| `CASSANDRA_HOSTS` | `localhost` |
| `CASSANDRA_KEYSPACE` | `auditoria` |
| `CASSANDRA_USUARIO` / `CASSANDRA_SENHA` | vazio (sem autenticação) |
| `MONGO_URI` | `mongodb://localhost:27017` |
| `MONGO_BANCO` | `auditoria` |
| `HTTP_ENDERECO` | `:8080` |
| `CATALOGO_ACOES` | `config/acoes.json` (na imagem: `/config/acoes.json`) |

## Pendências antes de produção

- **Distribuição**: definir **quem recebe** cada ação e **por qual canal** (SSE, WebSocket, push) e trocar `DistribuidorEmLog` no `main.go`.
- **Kafka do Dokploy**:
  - conferir se ele exige SASL/TLS. O leitor (`kafka-go`) hoje conecta sem autenticação; se exigir, falta configurar o `Dialer` com o mecanismo e o certificado;
  - criar o tópico `azimute.mensagens` lá, com a replicação que o cluster permitir.
- **Segurança**:
  - Cassandra e Mongo com usuário e senha (via variáveis do Dokploy);
  - trocar o `AUDITORIA_TOKEN_SERVICO` de produção periodicamente (mudar nas duas pontas ao mesmo tempo).
- **Tópico de erros (DLQ)**:
  - mensagens descartadas hoje vão só para o log;
  - um erro *permanente* do banco (dado que o banco nunca aceita) faria o consumidor reiniciar em loop.

  Os dois casos pedem um tópico de erros.
- **Cassandra em produção**: ajustar a replicação do keyspace (`NetworkTopologyStrategy`).
- **Deploy**: o repositório segue Git Flow. Trabalho em `feature/*` saindo da `develop`; a `main` é produção e só recebe release. Merge na `main` = deploy no Dokploy, quando a aplicação for ligada a ela.
- **Plataforma (gerenciador de processos)**: o lado que publica está na branch `feature/auditoria-por-servico` do backend, junto com a migration 094 (outbox). Lá, `AUDITORIA_KAFKA_BROKERS` aponta para o mesmo Kafka, e `AUDITORIA_URL` + `AUDITORIA_TOKEN_SERVICO` apontam para esta API pela rede interna.
