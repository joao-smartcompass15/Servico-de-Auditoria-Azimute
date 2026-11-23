-- Keyspace da auditoria.
-- SimpleStrategy com fator 1 serve SÓ para o ambiente local (um nó).
-- Em produção use NetworkTopologyStrategy, com o fator de replicação definido
-- para o cluster de vocês.
CREATE KEYSPACE IF NOT EXISTS auditoria
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- No Cassandra, cada tabela responde a UMA pergunta. Por isso o mesmo
-- registro é gravado em duas tabelas, juntas, num BATCH.
-- Em ambas, o id na chave torna a gravação idempotente: reprocessar a mesma
-- mensagem cai na mesma linha, sem duplicar.
--
-- municipio_id fica FORA da chave: os ids da plataforma são UUID globais, então
-- (entidade, entidade_id) não colide entre municípios. Vazio = ação de
-- plataforma, sem município.

-- Pergunta: "qual a linha do tempo da solicitação X?"
-- (quem pediu, quem mexeu, quem aprovou, em ordem cronológica)
CREATE TABLE IF NOT EXISTS auditoria.historico_por_entidade (
  entidade      text,
  entidade_id   text,
  ocorrido_em   timestamp,
  id            text,
  acao          text,
  usuario_id    text,
  tipo_usuario  text,
  municipio_id  text,
  motivo        text,
  origem        text,
  requisicao_id text,
  dados         text,
  alertas       list<text>,
  recebido_em   timestamp,
  PRIMARY KEY ((entidade, entidade_id), ocorrido_em, id)
) WITH CLUSTERING ORDER BY (ocorrido_em ASC, id ASC);

-- Pergunta: "o que o usuário X fez no dia D?"
-- O dia na chave mantém cada partição com tamanho limitado.
CREATE TABLE IF NOT EXISTS auditoria.acoes_por_usuario (
  usuario_id    text,
  dia           date,
  ocorrido_em   timestamp,
  id            text,
  acao          text,
  entidade      text,
  entidade_id   text,
  tipo_usuario  text,
  municipio_id  text,
  motivo        text,
  origem        text,
  requisicao_id text,
  dados         text,
  alertas       list<text>,
  recebido_em   timestamp,
  PRIMARY KEY ((usuario_id, dia), ocorrido_em, id)
) WITH CLUSTERING ORDER BY (ocorrido_em DESC, id ASC);

-- Contrato v2 (23/11/2026): quem já tinha as tabelas da v1 ganha as colunas
-- novas aqui. "ADD IF NOT EXISTS" (Cassandra 4.1+) deixa o arquivo idempotente:
-- pode rodar quantas vezes quiser.
ALTER TABLE auditoria.historico_por_entidade ADD IF NOT EXISTS tipo_usuario text;
ALTER TABLE auditoria.historico_por_entidade ADD IF NOT EXISTS municipio_id text;
ALTER TABLE auditoria.historico_por_entidade ADD IF NOT EXISTS requisicao_id text;
ALTER TABLE auditoria.acoes_por_usuario ADD IF NOT EXISTS tipo_usuario text;
ALTER TABLE auditoria.acoes_por_usuario ADD IF NOT EXISTS municipio_id text;
ALTER TABLE auditoria.acoes_por_usuario ADD IF NOT EXISTS requisicao_id text;
