# Runbook Operacional — Financial Fraud Detection Platform

## Setup Inicial

```bash
git clone https://github.com/gpgomes/data-master-std-fraud.git
cd data-master-std-fraud
cp .env.example .env
make up
make setup
make seed-data
```

## Diagnóstico

### Verificar saúde dos serviços
```bash
make ps
make logs-kafka
make logs-spark-master
```

### Kafka: verificar tópicos e mensagens
```bash
docker compose exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic raw-transactions --from-beginning --max-messages 5
```

### MinIO: verificar buckets
```bash
# Via CLI (mc)
docker compose exec minio mc ls local/bronze/
docker compose exec minio mc ls local/silver/
```

## Procedimentos Comuns

### Reprocessar Bronze → Silver
```bash
make spark-submit-batch
```

### Reiniciar streaming com offset zerado
```bash
# Limpar checkpoints e reiniciar
docker compose exec minio mc rm --recursive --force local/checkpoints/
make spark-submit-stream
```

Apagar `local/checkpoints/` também apaga os marcadores de progresso (`stream_processor_progress/`), que precisam ser resetados junto com os `batch_id`. A saída em `silver/transactions_stream/` fica separada por `query_id` (o id do checkpoint), então o novo stream não sobrescreve a saída de execuções anteriores.

O **estado curto** do detector (`silver/_stream_state/recent_events/`, ver abaixo) não faz parte do checkpoint e sobrevive a esse reset. Para recomeçar do zero, apague-o também: `docker compose exec minio mc rm --recursive --force local/silver/_stream_state/`.

### Semântica de entrega do streaming (issue #36)

Reiniciar ou derrubar o `spark-submit-stream` no meio de um micro-batch faz o Spark reexecutar aquele mesmo `batch_id`. O `_process_batch` trata isso por etapa:

| Etapa | No replay |
|-------|-----------|
| Parquet em `silver/transactions_stream/query_id=<id>/batch_id=<n>/` | Sobrescreve a própria partição (overwrite dinâmico): sem linhas duplicadas |
| Kafka `enriched-transactions`, Kafka `fraud-alerts`, estado curto (`silver/_stream_state/recent_events/`) | Pulada se o marcador `checkpoints/stream_processor_progress/batch_<n>.<etapa>` existe; o estado não é contado em dobro |
| `alert_id` | Derivado de `transaction_id`: o mesmo alerta mantém o mesmo id |

Garantia: **sem duplicatas no Parquet**; **at-least-once nos tópicos Kafka** (o Kafka sink do Spark não é transacional, então cair entre o fim de uma etapa Kafka e a gravação do seu marcador ainda pode duplicar aquela mensagem). Consumidores de `enriched-transactions`/`fraud-alerts` devem deduplicar por `transaction_id` (ou `alert_id`).

Verificar duplicatas no Kafka (as mensagens do Kafka carregam `transaction_id`):

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic enriched-transactions --from-beginning --timeout-ms 10000 2>/dev/null \
  | grep -o '"transaction_id":"[^"]*"' | sort | uniq -d | wc -l
```

Layout novo: `silver/transactions_stream/` passou a ser particionado por `query_id` e `batch_id`. Saída gravada por versões anteriores fica solta na raiz do prefixo; apague `silver/transactions_stream/` antes de ler o conjunto todo.

### Fraud Engine no streaming (issue #46)

O `stream_processor.py` pontua cada micro-batch com o Fraud Engine (`multisignal-v2`, o mesmo
`detect` que o `make fraud-eval` mede) e mantém o Z-Score antigo (`zscore-v1`) calculado em paralelo,
só para comparação (*shadow scoring*). Quem decide o alerta é o V2.

**Ordem de execução.** O V2 lê o perfil de comportamento dos clientes por broadcast, e o perfil é
uma tabela Gold do batch. Sem ele o job **recusa iniciar** (`RuntimeError: Perfil de comportamento
não encontrado`), porque um V2 sem perfil não conhece device, rede, destinatário nem valor típico
de ninguém:

```bash
make spark-submit-batch        # Bronze → Silver
make spark-submit-silver-gold  # Silver → Gold, inclui gold/customer_behavior_profile/ (~30 s, 500 mil transações)
make spark-submit-stream       # só agora
```

Para refazer só o perfil: `docker compose exec spark-master spark-submit --master spark://localhost:7077
src/transformation/batch/silver_to_gold.py --dataset customer_behavior_profile`. O perfil usa todo o
Silver (ignora `--start-date/--end-date`) e só as linhas legítimas do histórico. Recalcule-o quando o
Silver mudar; o stream só o lê na partida, então reinicie o job depois.

**Estado curto.** Os sinais de janela (velocidade, viagem impossível, concentração de destinatários)
precisam dos eventos anteriores, que o micro-batch sozinho não tem. Entre micro-batches o job grava, só com
as colunas que o detector enxerga (nunca o rótulo), **toda a última hora** de eventos e, de 1 h até 6 h,
**só os últimos 5 eventos de cada cliente** (a viagem impossível é o único sinal que olha além de 1 h, e só para
eles; compactação da issue #56), em `silver/_stream_state/recent_events/`. Reiniciar o job continua desse estado.

**Eventos atrasados (#59).** Não há watermark: nenhum evento é descartado por chegar tarde. Um evento atrasado é pontuado contra os eventos que o antecedem **em tempo de evento** que estiverem no estado (e, por isso, a viagem impossível ainda é detectada), e sai do estado se for mais velho que 6 h em relação ao evento mais recente já visto. Com a compactação (#56), um evento com mais de 1 h de atraso só tem, do seu passado, os últimos 5 eventos de cada cliente que sobraram no estado; um evento com mais de 6 h de atraso é pontuado com pouco contexto de janela curta. O caminho antigo
(`silver/_stream_state/customer_amount_history/`, 3 colunas) ficou órfão e pode ser apagado.

**Saída.**

| Destino | O que mudou |
|---|---|
| `silver/transactions_stream/` (Parquet), `enriched-transactions` | Colunas novas do V2: `fraud_score`, `is_fraud_predicted`, `fraud_signals` (os sinais ativos), `fraud_type_predicted`, `detector_version`. O V1 segue em `z_score`, `is_anomaly`, `fraud_score_v1`, `shadow_detector_version`. **`fraud_score` agora é o do V2** |
| `fraud-alerts` | Só o que o V2 marcou. `fraud_type` é o **inferido pelos sinais** (nulo se nenhuma regra casa; o fallback fixo `MONEY_LAUNDERING` acabou), `signals` e `detector_version` são novos, `z_score` é o do V1 e pode ser nulo. `alert_reason` lista os sinais |
| Rótulos `is_fraud`/`fraud_type` | Continuam no payload e no Parquet (o gerador sintético os manda, e é com eles que se mede o detector online), mas são separados do DataFrame **antes** de qualquer detector e só voltam por `transaction_id` no fim |

Saída de execuções anteriores (schema do V1) não tem as colunas novas: apague
`silver/transactions_stream/` antes de ler o conjunto todo, como no aviso de layout acima.

Loader do Postgres (`make spark-submit-stream-postgres`): `fraud_alerts` é montado do Parquet com o
mesmo `build_fraud_alerts`. As colunas `signals` e `detector_version` ainda não existem na tabela
(issue #47), e `is_anomaly` em `stream_scored_transactions` é o do V1.

### Rodar os producers: no host ou em container (issue #48)

Os dois producers (`kafka_producer_transactions.py` e `kafka_producer_market.py`) rodam de duas formas:

```bash
# No host (usa o .venv; aceita PRODUCER_RATE_TPS=20 e as demais variáveis abaixo)
make producer-transactions
make producer-market

# Em container (perfil `producers`; sobe os dois, com 10 TPS fixos de transações no compose)
docker compose --profile producers up -d producer-transactions producer-market
docker compose --profile producers ps                       # os dois "Up", sem reinício
docker compose --profile producers stop producer-transactions producer-market
```

Conferir que publicam (as somas dos offsets das partições têm de crescer):

```bash
docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:29092 --topic raw-transactions --time -1
```

- **O código é montado, as dependências não.** O compose monta `./src` no container, então mudança em `src/` só pede reiniciar o serviço. A imagem (`docker/producer/`) instala **só** `docker/producer/requirements-producer.txt`: dependência nova de um producer entra nesse arquivo e a imagem é reconstruída (`docker compose --profile producers up -d --build ...`).
- **O producer de mercado só publica durante o pregão**: das 13h às 20h UTC (10h às 17h em Brasília), sem checar o dia da semana. Fora desse horário ele fica de pé, sem reiniciar, e `raw-market-data` não cresce (o log diz "Fora do horário de pregão" em nível DEBUG). Isso é o comportamento esperado, não uma falha.
- **`AssertionError: Libraries for lz4 compression codec not found`** e o container reiniciando em loop: a imagem não tem a lib do codec de compressão do `ProducerConfig` (`lz4`). Era o estado até a issue #48. Se voltar a acontecer, confira se `lz4` está em `docker/producer/requirements-producer.txt` e reconstrua com `--build`; o teste `tests/unit/test_producer_image_requirements.py` existe para impedir que isso chegue ao `main`.

### Dados sintéticos: perfis, fraude por episódio e ground truth (issue #43)

O gerador (`src/common/data_generator.py`) dá a cada cliente um perfil de comportamento
determinístico (devices, redes /24, destinatários frequentes, faixa de valor, horário ativo,
cidade-base), derivado só de `seed + customer_id` (`src/common/customer_profile.py`). A fraude é
gerada como **episódio** coerente com o tipo (`src/common/fraud_scenarios.py`), com ~20% de
variantes *stealth*, e o tráfego legítimo carrega ruído deliberado (*hard negatives*: troca de
celular, rede nova, viagem, compra grande, transação fora do horário habitual).

`make seed-data` grava, além de `data/sample/transactions/`, o sidecar
`data/sample/ground_truth/ground_truth.csv` (`transaction_id, episode_id, scenario, stealth,
hard_negative`). Ele fica fora de `transactions/` e fora do `TransactionEvent` de propósito: só o
avaliador do detector o consome. O `is_fraud`/`fraud_type` do evento continuam sendo o rótulo.

Variáveis do producer de transações (`docker-compose.yml`, `.env.example`):

| Variável | Padrão | Para que serve |
|---|---|---|
| `CUSTOMER_SEED` | `42` | Seed dos clientes e perfis. **Tem de bater com a do `make seed-data`** (`--seed 42`): os 1.000 clientes do stream são o prefixo dos 10.000 do batch, e é isso que faz o join com `dim_customers` encontrar o cliente |
| `GENERATOR_SEED` | `0` | Seed dos **eventos**; `0` = por horário. Não fixe: com a seed fixa cada reinício do producer repetiria os mesmos `transaction_id` |
| `PRODUCER_DIURNAL` | `false` | `true` faz o volume seguir o horário ativo dos clientes (cai de madrugada); `false` mantém o ritmo constante de `PRODUCER_RATE_TPS` |

Follow-ups de um episódio de fraude (o clone de cartão, o segundo saque do account takeover) saem
com atraso: o producer os mantém num heap e os emite na hora certa, com `timestamp` = instante de
emissão. Pedir uma execução curta do producer pode, portanto, mostrar só o início de um episódio.

Limitação conhecida: o roubo de identidade mira contas abertas nos últimos 30 dias, e a idade da
conta vem de `account_opening_date` no momento em que os clientes foram gerados. Se o `seed-data`
foi rodado há semanas, essas contas já não são "novas" para o stream.

### Avaliar os detectores de fraude (issues #44 e #45)

```bash
make fraud-eval        # ~50 s, Spark local: não precisa de Docker (mas de Java, como os testes)
make fraud-calibrate   # ~15 s: recalibra os pesos e o limiar do V2 (só quando o gerador ou os sinais mudam)
```

`make fraud-eval` gera `docs/fraud_evaluation.md` (documento versionado, sem data/hora: a mesma
configuração e as mesmas seeds geram o mesmo texto; não edite à mão). Mede **dois detectores nos
mesmos datasets**, com comparação pareada por seed:

- `zscore-v1`: o detector de streaming atual (a mesma `_enrich_and_score` de produção).
- `multisignal-v2`: o Fraud Engine (`src/transformation/fraud/`): 10 sinais (perfil do batch: valor,
  device, rede, destinatário, hora, local e idade da conta; janela curta: velocidade, viagem impossível
  e concentração de destinatários) combinados por noisy-OR, com o tipo de fraude **inferido** pelos
  sinais. Nunca lê o rótulo do evento.

O relatório traz Precision, Recall, F1, FPR, FNR, PR-AUC, alertas por 1.000 transações, Recall por
tipo e por variante stealth, FPR por tipo de hard negative, *time-to-detect* por episódio, a matriz de
confusão do tipo inferido, a tabela dos sinais (peso, frequência no legítimo e na fraude) e uma
análise de sensibilidade do V2 sem o sinal que o gerador injeta em toda a fraude (`NEW_DESTINATION`).

O protocolo usa um **replay de stream** (`TransactionStream` em tempo simulado, 1.000 clientes a
10 TPS), não o dataset do `make seed-data`: a janela do Z-Score é de 1 h por cliente, e com ~50
eventos por cliente em 180 dias o baseline existe em menos de 1% dos eventos. O V2 também recebe um
histórico de batch dos mesmos clientes (60 mil transações em 180 dias), de onde sai o perfil. O
limiar de cada detector (*recall máximo com FPR ≤ 1%*) é calibrado numa seed de validação e aplicado
em 5 seeds de teste com clientes diferentes.

**Calibração do V2.** Os pesos e o limiar do alerta ficam em `src/transformation/fraud/weights.py`,
arquivo **gerado** por `make fraud-calibrate` (busca por coordenadas na seed de validação; nunca nas
de teste). Se você mudar o gerador (`data_generator.py`, `fraud_scenarios.py`) ou os sinais
(`signals.py`, `profile.py`), rode a calibração de novo, depois `make fraud-eval`, e commite os dois
arquivos. Um teste garante que o `weights.py` versionado tem o formato do gerador.

Opções úteis (as duas CLIs): `--test-seeds 1 2`, `--duration-minutes 90`, `--customers 300`,
`--skip-batch-density`, `--profile-until <ISO>` (corte: antes dele os eventos só alimentam o estado do
detector), `--detectors zscore-v1` (só um), `--diurnal` com um `--start` noturno (exercita o sinal
`UNUSUAL_HOUR`, que a janela padrão de 4 h ao meio-dia não exercita) e `--output <arquivo>` para não
sobrescrever o documento versionado num teste rápido. Uma calibração com `--diurnal` deve usar as
mesmas opções da avaliação.

### Resetar ambiente completo
```bash
make clean
make up
make setup
make seed-data
```

## Quality Gates (Great Expectations)

Suites e checkpoints ficam em `src/governance/great_expectations/` — definidos
via código Python (`suites.py`), não editados à mão em JSON/YAML. Cobrem
`bronze_transactions`, `bronze_market_data`, `silver_transactions`,
`silver_market_data`, `gold_fact_transactions`, `gold_dim_customers`,
`gold_dim_date`, `gold_agg_daily_fraud_metrics`, `gold_customer_behavior_profile`.

Política: **gate simples** — qualquer expectativa falhando bloqueia a task do
Airflow (e portanto a DAG inteira, via `trigger_rule` padrão), não há
quarentena de linhas nem execução parcial. Recuperação = corrigir a causa e
rerodar a DAG a partir da task que falhou.

Exceção: `bronze_market_data` e `silver_market_data` são **gates opcionais**
(`OPTIONAL_DATASETS` em `runner.py`). O dado de mercado vem do yfinance/Yahoo Finance,
que devolve HTTP 429 (rate limit) conforme o IP, e sem dado no Bronze o
`bronze_to_silver.py` pula a etapa e não gera Parquet no Silver. Nesses dois datasets,
falha de expectativa ou ausência de Parquet vira **warning** ("Gate opcional falhou —
não bloqueia o pipeline") e o pipeline segue. Os gates de transações, clientes e Gold
continuam bloqueando.

### Rodar um gate manualmente

```bash
docker compose exec airflow-scheduler python -m src.governance.great_expectations.runner bronze_transactions
docker compose exec airflow-scheduler python -m src.governance.great_expectations.runner --all
```

`--all` sai com código 0 se só os gates opcionais falharem; qualquer outro gate
falhando faz sair com código 1 (confira com `echo $?`).

### Gerar e abrir os data docs

Os data docs ficam em
`src/governance/great_expectations/gx/uncommitted/data_docs/local_site/index.html`.
O diretório `uncommitted/` é ignorado pelo git (`gx/.gitignore`), então **o HTML não
existe num clone novo**: ele é gerado quando um gate roda, seja pela task `validate_*`
no Airflow ou pelo comando manual acima. Para vê-los:

```bash
docker compose exec airflow-scheduler python -m src.governance.great_expectations.runner --all
open src/governance/great_expectations/gx/uncommitted/data_docs/local_site/index.html   # macOS
```

Há uma página de validação por dataset que foi de fato validado. Datasets sem Parquet
(por exemplo os de mercado, com o Yahoo Finance limitando as requisições) não geram página.

### Diagnosticar uma falha

1. **Logs da task no Airflow** (`http://localhost:8082`, ou `docker compose logs airflow-scheduler`) — a task `validate_*` loga, por expectativa falhada, o tipo e os kwargs (ex.: `expect_column_values_to_be_unique` em `customer_key`), e a exceção `QualityGateFailed` traz a contagem total de falhas.
2. **Data docs** — renderizados em `src/governance/great_expectations/gx/uncommitted/data_docs/local_site/index.html` a cada execução (mesmo quando o gate falha — a task só levanta a exceção *depois* de publicar os docs). Abra o `index.html` localmente para ver o detalhe visual de cada expectativa, incluindo exemplos de valores que violaram a regra.
3. **Causas comuns**: schema mudou upstream (coluna removida/renomeada — ver `expect_column_to_exist` na suite correspondente), regressão no dedup do `bronze_to_silver.py` (unicidade de `transaction_id` falhando em Silver), bug real nos dados (ex.: o caso da issue #10, `customer_key` duplicado em `dim_customers`).

### Recuperação

Corrija a causa raiz (código do job upstream, ou os dados de origem) e
rerode a task/DAG no Airflow (`Clear` na task falhada). Não existe um
"forçar passagem" — o gate é intencionalmente rígido; se uma expectativa
estiver **errada** (não os dados), corrija a suite em `suites.py` e rode
`python -m src.governance.great_expectations.runner --all` para revalidar
antes de reabrir a DAG.

### Adicionar/alterar uma expectativa

Edite a função `_build_*` correspondente em `suites.py` (não os arquivos
JSON gerados em `gx/expectations/` — esses são saída, sobrescritos a cada
run) e rode os testes (`pytest tests/unit/test_great_expectations.py`) com
uma fixture que exercite a mudança.

## Serving Layer (PostgreSQL + FastAPI)

`src/serving/loaders/gold_to_postgres.py` (issue #10) carrega as 4 tabelas
Gold (`fact_transactions`, `dim_customers`, `dim_date`,
`agg_daily_fraud_metrics`) do MinIO para o Postgres via truncate+reload por
tabela — sem foreign keys entre fato e dimensões de propósito (ver
`src/serving/loaders/schema.sql`). Roda como task da DAG
`batch_transformation_pipeline`, depois do gate de qualidade do Gold.

`src/serving/api/` (issue #15, FastAPI + SQLAlchemy Core) expõe:

| Endpoint | Descrição |
|----------|-----------|
| `GET /health/live`, `GET /health/ready` | Liveness/readiness (readiness checa a conexão com o Postgres) |
| `GET /transactions`, `GET /transactions/{id}` | Transações (paginado) |
| `GET /alerts` | Alertas reais do Fraud Engine (tabela `fraud_alerts`): `fraud_score`, `fraud_signals` (a lista dos sinais que dispararam o alerta), `fraud_type` (inferido pelos sinais; **nulo** quando nenhuma regra casou, ~1/3 dos alertas), `detector_version`, o `z_score` do detector antigo (shadow, pode ser nulo) e `alert_reason`; a visão pelo rótulo do batch é `GET /transactions?is_fraud=true` |
| `GET /kpis/fraud-daily` | `agg_daily_fraud_metrics` (paginado) |

### Carregar a saída do streaming (issue #38)

`stream_to_postgres.py` lê `silver/transactions_stream/` e recarrega `stream_scored_transactions` (o veredito do Fraud Engine, o Z-Score antigo em paralelo, o rótulo do gerador e a latência) e `fraud_alerts`:

```bash
make spark-submit-stream-postgres
```

Truncate + reload idempotente: rodar de novo não duplica nada. Sem saída de streaming no Silver ele pula a carga com um aviso e sai com 0 (por isso é a última task da DAG `batch_transformation_pipeline`, sem bloqueá-la). Como o streaming ocupa todos os cores do cluster Spark local, **pare o `spark-submit-stream` (Ctrl+C) antes de rodar a carga**; o Parquet já gravado continua lá. A serving layer reflete o streaming com a defasagem da última carga.

Conferir: `SELECT COUNT(*), COUNT(fraud_score), ROUND(AVG(latency_seconds)::numeric, 2) FROM stream_scored_transactions;` e `SELECT COUNT(*) FROM fraud_alerts;` (deve bater com as mensagens de `fraud-alerts`).

**Rótulo × predição (issue #47).** Em `stream_scored_transactions`, `is_fraud` e `fraud_type` são o **rótulo do gerador sintético** (ground truth, só para medir); o que o detector decidiu está em `fraud_score`, `is_fraud_predicted`, `fraud_signals` e `fraud_type_predicted` (Fraud Engine V2, quem alerta) e, em paralelo, em `z_score`, `is_anomaly` e `fraud_score_v1` (Z-Score V1, shadow). `fraud_signals` é texto separado por vírgula (`AMOUNT_ANOMALY,NEW_DESTINATION`; vazio = nenhum sinal acima de 0,5); em `fraud_alerts` a coluna equivalente chama `signals` e a API a devolve como lista em `fraud_signals`.

**Banco já provisionado (issue #47).** Um Postgres criado antes da #47 tem as duas tabelas sem as colunas novas, e `CREATE TABLE IF NOT EXISTS` não as adiciona. O `ensure_schema`, que roda no começo de toda carga, também aplica os `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` do `schema.sql`: nada a fazer além de rodar `make spark-submit-stream-postgres` de novo. Em banco novo os `ALTER` não têm efeito. As linhas carregadas antes do upgrade ficam com as colunas novas nulas até a próxima carga (que recarrega tudo); a API devolve `fraud_signals: []` para elas. Para conferir: `\d fraud_alerts` deve listar `signals` e `detector_version`.

### Benchmark online V1 × V2 (issue #47)

`make fraud-online-eval` roda `src/serving/queries/fraud_online_benchmark.sql` no Postgres: Precision, Recall, F1, FPR e FNR do V1 (`is_anomaly`) e do V2 (`is_fraud_predicted`) **sobre os mesmos eventos** (shadow scoring), recall por tipo, quem pegou o quê, o tipo inferido × o rótulo, os sinais dos falsos positivos do V2 e a latência p50/p95/p99. Pré-requisito: o streaming rodou e a carga acima foi feita. É o par online do `make fraud-eval` (offline): o V2 aqui já vem calibrado da seed de validação, não há warm-up e os números vêm de **uma** execução (o intervalo de confiança do Recall com ~150 fraudes é de ±3 p.p.). Para uma rodada limpa, apague `silver/transactions_stream/`, `silver/_stream_state/` e `checkpoints/` antes de subir o stream.

### Rodar manualmente

```bash
make spark-submit-gold-postgres  # carrega o Gold no Postgres
make api                         # sobe a API em http://localhost:8000/docs
```

### Diagnosticar

1. **API sobe mas endpoints retornam vazio**: confirme que
   `make spark-submit-gold-postgres` já rodou — a API só lê o que está no
   Postgres, não recalcula nada.
2. **`503 database unavailable`**: Postgres não está pronto ou
   `POSTGRES_*` no `.env` não bate com o container — ver
   `make logs-postgres`.
3. **`/alerts` vazio**: `fraud_alerts` só tem dados depois que o job de streaming rodou e a carga foi executada (`make spark-submit-stream-postgres`, ver abaixo). Sem streaming a API devolve `total: 0`, não erro.
4. **`fraud_type: null` em `/alerts`**: esperado. O tipo é inferido pelos sinais e fica nulo quando nenhuma regra casa (por exemplo, `NEW_DESTINATION` sozinho com sinais fracos); não há mais tipo de fallback.
5. **`column "signals" of relation "fraud_alerts" does not exist` na carga**: o `ensure_schema` não rodou (a carga chamou o JDBC direto) ou falhou. Rode `make spark-submit-stream-postgres` inteiro, ou aplique o `src/serving/loaders/schema.sql` com `psql`.

## Catálogo de Dados

Registro leve em `src/governance/data_catalog/registry.py` (não OpenMetadata —
ver decisão em `docs/architecture.md`, tabela "Decisões Arquiteturais").
Cobre Bronze/Silver/Gold (MinIO), as 4 tabelas da serving layer (Postgres),
os 4 tópicos Kafka, e o dashboard Superset (issue #16 — ver seção
"Dashboards (Superset)" abaixo).

### Gerar o catálogo

```bash
make catalog
# equivalente:
python -m scripts.build_data_catalog --strict
```

Requer `make up && make setup` (e dados já processados até Gold, para os
prefixos MinIO/tabelas Postgres não ficarem vazios) — sem `--strict`, ou com
`--skip-validation`, o comando só renderiza o registro sem checar a infra.
Saída: `docs/data_catalog.md` (tabelas por camada + diagrama de linhagem em
Mermaid).

### Imagem da linhagem (issue #37)

`make catalog` também escreve `docs/images/data_lineage.svg`, a linhagem do registro como imagem (embutida no README, no `docs/architecture.md` e no `docs/data_catalog.md`). É gerada em Python puro (`src/governance/data_catalog/lineage_image.py`): não precisa de Graphviz, mermaid-cli nem Node, e a saída é determinística (sem data/hora), então o git só muda quando o catálogo muda.

- **Nunca edite o SVG à mão.** Mude o `registry.py` e rode `make catalog`; um teste (`test_versioned_image_matches_the_catalog`) falha no CI se a imagem versionada não bater com o registro.
- Nó novo no registro: aparece sozinho, no fim da coluna da sua camada. Para posicioná-lo melhor, ajuste `_ROW_HINTS` em `lineage_image.py` (só ordem visual; o conteúdo vem do catálogo).
- Datasets `optional=True` saem tracejados; nós sem nenhuma ligação saem com a nota "sem consumidor no catálogo" (hoje só `raw-market-data`, que ninguém consome na V1).

### Diagnosticar uma falha de validação

`--strict` sai com código 1 se algum asset não existir na infra real
(prefixo MinIO vazio, tabela Postgres ausente, tópico Kafka inexistente) ou
se alguma referência de linhagem (`upstream`) apontar para uma key
inexistente no registro. O log (`data_catalog`) indica a entry e o motivo;
o próprio `docs/data_catalog.md` gerado também marca cada asset com
❌ e o detalhe, ou 🗓️ planejado para o que ainda não foi implementado.

Exceção: assets com `optional=True` no registro (`bronze_market_data` e
`silver_market_data`, cotações do yfinance que podem vir vazias por rate limit HTTP 429
do Yahoo Finance) não derrubam o `--strict`. Se estiverem sem dados, o catálogo os marca
com ⚠️ "sem dados (opcional)", o log registra um warning e o comando sai com código 0.
`silver_transactions_stream` (a saída do streaming) também é opcional: só existe depois que o job de
streaming rodou. Um resultado normal nesta V1 local é, portanto: 20 datasets, 0 referências inválidas,
17 com ✅ (16 se o streaming nunca rodou), 2 de mercado com ⚠️ (3 sem streaming) e o dashboard
"não validado" (não há checagem de infra para dashboards).

### Adicionar/alterar um asset

Edite `CATALOG` em `registry.py` (owner, classificação, termos de
glossário — devem bater com `docs/data_dictionary.md`, checado por
`test_glossary_terms_reference_known_business_glossary` — e `upstream` para
linhagem) e rode `pytest tests/unit/test_data_catalog.py`.

## Dashboards (Superset)

Só Superset na V1 (Grafana descoped — ver `docs/architecture.md`, tabela
"Decisões Arquiteturais"). `src/serving/dashboards/{client,charts,
provision}.py` provisiona, via API REST do Superset e de forma idempotente
(roda de novo atualiza em vez de duplicar), a conexão Postgres, os
datasets (`fact_transactions`, `agg_daily_fraud_metrics`) e o dashboard
"Fraude e Transações - Visão Geral" (4 KPIs, 2 séries temporais, 1
distribuição por `fraud_type`, filtros nativos de período e tipo de
transação).

### Provisionar o dashboard

```bash
make dashboards
# equivalente:
python -m scripts.provision_superset_dashboards --verify --strict
```

Requer `make up` e dados já carregados no Postgres (`make spark-submit-batch`
+ `make spark-submit-gold-postgres`, issue #10). `--verify` refaz a query de
cada chart via `/api/v1/chart/data` e confere que retornou dado real
(não só que a criação não deu erro); `--strict` sai com código 1 se algo
não bater. Acesso: `http://localhost:8088` (admin/admin).

### Exportar snapshot versionado

```bash
make dashboards-export
```

Roda `superset export-dashboards` dentro do container e atualiza
`dashboards/superset/dashboard_configs/` com o bundle nativo do Superset
(YAML, senha do Postgres mascarada automaticamente pelo próprio export) —
regenerado a cada execução, não editado à mão.

### Diagnosticar

1. **Login falha ou container fica em `health: starting`**: veja
   `docker compose logs superset` — se aparecer `Error: No application
   module specified` ou `Username [admin]:` preso esperando input, o
   comando de boot do serviço `superset` no `docker-compose.yml` quebrou de
   novo (bug real corrigido na issue #16: indentação YAML mais funda que a
   linha-mãe faz o folding do bloco `command: >` inserir uma quebra de
   linha literal no meio do comando `bash -c`, partindo `gunicorn`/
   `create-admin` ao meio) — mantenha cada comando numa única linha lógica.
2. **Chart sem dado / "0 rows"**: confira se `make spark-submit-batch` +
   `make spark-submit-gold-postgres` já rodaram (o Postgres precisa ter
   linhas em `fact_transactions`/`agg_daily_fraud_metrics`).
3. **Charts do streaming sem dado** (latência, alertas do detector, distribuição de `fraud_score`, alertas por hora): as tabelas `stream_scored_transactions`/`fraud_alerts` estão vazias. Rode o streaming e `make spark-submit-stream-postgres`. Esses 4 charts são opcionais na verificação (`make dashboards` não falha sem eles). O `fraud_score` de `fact_transactions` (batch) continua nulo por definição.

### Validação visual (checklist manual)

Abra `http://localhost:8088/superset/dashboard/fraude-transacoes-visao-geral/`
(admin/admin) e confirme: os 7 gráficos renderizam sem erro; o filtro
"Período" e o filtro "Tipo de Transação" aparecem no topo e, ao aplicados,
os KPIs/gráficos atualizam; os números batem com uma query direta no
Postgres (ex.: `SELECT COUNT(*) FROM fact_transactions WHERE is_fraud =
true` deve bater com o KPI "Alertas de Fraude").

### Adicionar/alterar um chart

Edite `CHARTS` em `src/serving/dashboards/charts.py` (cada `ChartDef` tem
`form_data_extra`, usado para renderizar o chart na UI, e `query_extra`,
usado pelo smoke test do `--verify` — mantenha os dois coerentes) e rode
`pytest tests/unit/test_dashboards.py`, depois `make dashboards`.

## Observabilidade e SLOs (issue #55)

O Superset do dashboard de fraude mostra o **negócio**. A saúde da **plataforma** fica em quatro tabelas
append-only no mesmo Postgres (`src/observability/schema.sql`), num segundo dashboard e num relatório de
SLOs:

| Tabela | Quem grava | Uma linha por |
|---|---|---|
| `stream_batch_metrics` | `StreamMetricsListener` (um `StreamingQueryListener` no driver do stream) | micro-batch: linhas/s de entrada e processadas, duração total e do `foreachBatch`, lag do Kafka, linhas pontuadas, alertas, tamanho do estado curto, latência p50/p95/máx evento → processamento |
| `pipeline_runs` | task `record_pipeline_run` das duas DAGs (`trigger_rule="all_done"`) | execução de DAG: estado, duração, tasks que falharam |
| `quality_gate_runs` | `runner.run_gate` do Great Expectations | execução de gate: sucesso, expectativas avaliadas e com falha, se é opcional |
| `api_requests` | middleware da FastAPI, **depois** de enviar a resposta | request: método, rota (o template, `/transactions/{transaction_id}`, sem ids), status, duração |

As tabelas são criadas na primeira gravação de cada processo. **Métrica nunca derruba o pipeline:** Postgres
fora ou credencial errada viram um warning `Métrica de plataforma não gravada` no log e o dado é descartado.
Para desligar: `OBSERVABILITY_ENABLED=false` (os testes unitários desligam sozinhos). Timestamps em **UTC**
(o Postgres do projeto roda em `America/Sao_Paulo`; os `DEFAULT` usam `now() AT TIME ZONE 'UTC'`).

```bash
make dashboards     # provisiona os dois dashboards; o novo é http://localhost:8088/superset/dashboard/platform-health/
make slo-report     # SLOs das últimas 24 h: OK / VIOLADO / SEM DADOS
```

**SLOs** (metas para o ambiente local; a meta de latência mais agressiva, p95 < 5 s, é o alvo da #56):

| SLO | Meta | Fonte |
|---|---|---|
| Stream: latência evento → processamento p95 | < 12 s (o trigger é 10 s) | `stream_batch_metrics.latency_p95_s` |
| Stream: lag máximo do Kafka | < 10.000 eventos | `stream_batch_metrics.kafka_lag` |
| Stream: duração do micro-batch p95 | < 10 s (acima disso o stream acumula atraso) | `stream_batch_metrics.batch_duration_ms` |
| Pipeline: quality gates obrigatórios verdes | 100% | `quality_gate_runs` (opcionais fora) |
| Pipeline: toda DAG executada tem ao menos um sucesso | 100% | `pipeline_runs` |
| API: latência p95 | < 500 ms | `api_requests.duration_ms` |
| API: erros 5xx | < 1% | `api_requests.status_code` |

**Como ler:**

- **SEM DADOS** não é OK: o componente não rodou na janela (stream parado, DAG pausada, API sem tráfego).
- **Os primeiros micro-batches depois de subir o stream** processam o que se acumulou no Kafka enquanto ele
  estava parado, e sua latência (e duração) é alta por construção. Com poucos micro-batches na janela, eles
  dominam o p95. Para separar partida de regime:
  `... row_number() OVER (PARTITION BY run_id ORDER BY batch_id) > 3` (cada reinício tem um `run_id` novo).
- **Carga concorrente** no mesmo Docker (DAG de ingestão, jobs Spark, provisionamento do Superset) derruba a
  latência do stream: o cluster local tem 4 cores. Meça o stream sozinho.
- **Lag 0 com latência de 10 s** é o esperado: o Spark lê tudo o que chegou até o trigger; o lag só cresce
  quando o micro-batch demora mais que o trigger.
- **Gate que falhou e passou no retry:** as duas execuções ficam em `quality_gate_runs`; o SLO olha todas.
- **`numInputRows` = `rows_scored`**: antes da #55 o `isEmpty()` no micro-batch cru relia o Kafka e o Spark
  contava 1 linha a mais por batch; agora o micro-batch é cacheado antes do `isEmpty()`.

**Tabelas criadas numa versão anterior desta issue** (só no ambiente de quem testou antes do merge) podem ter
`DEFAULT now()` em hora local; `CREATE TABLE IF NOT EXISTS` não corrige. Ajuste com
`ALTER TABLE ... ALTER <coluna> SET DEFAULT (now() AT TIME ZONE 'UTC')` ou apague as quatro tabelas (são só
métricas).

## Teste de carga do stream (issue #56)

`scripts/stream_load_test.py` (`make load-test`) sobe, para cada nível de TPS, N instâncias do producer no host
(cada uma com `GENERATOR_SEED` própria: com a mesma seed, instâncias iniciadas no mesmo segundo repetiriam
`transaction_id`), espera 60 s de aquecimento, mede 240 s e para. Mede pelo Kafka (eventos/s produzidos de
fato), por `stream_batch_metrics` (linhas/s, duração do micro-batch, lag, estado, latência de cada batch e o
**tempo por etapa**) e por `docker stats` (CPU e memória do Spark). Um nível é **saturado** quando o micro-batch
p95 passa do trigger (10 s), sobra lag no fim da janela ou o stream lê menos de 90% do produzido.

```bash
make spark-submit-stream                         # terminal 1: o stream, SOZINHO (sem DAGs, jobs Spark ou Superset rodando)
make load-test LOAD_LEVELS=25,50,100,200         # terminal 2: ~30 min; resultado em data/load_test/results.json
# latência por evento (p50/p95/p99 exatos) de cada janela: pare o stream e
make spark-submit-stream-postgres
.venv/bin/python -m scripts.stream_load_test --latency-from data/load_test/results.json
```

- **Uma instância do producer entrega no máximo ~50 TPS** (envio síncrono: cada mensagem espera o ack) e ~65%
  do pedido até 25 TPS; o harness reporta o produzido medido no Kafka, não o alvo. Com 12 instâncias no host,
  ~205 TPS é o teto do gerador nesta máquina.
- **Meça em regime.** Cada nível dura minutos, mas o estado curto em produção guarda horas. Para medir o custo
  real do estado, pré-carregue-o antes de subir o stream (`scripts/seed_stream_state.py`, docstring com os
  comandos; zere também `silver/transactions_stream/` e `checkpoints/`).
- **O que olhar:** o chart "Stream - Tempo por Etapa do Micro-batch" do Platform Health mostra qual etapa cresce
  com a carga.

## CI (GitHub Actions)

`.github/workflows/ci.yml` roda em todo push/PR para `main`: job `lint` (`ruff check` + `mypy`) e job `test` (`pytest tests/unit/`, Java 17 + Python 3.11, gate de cobertura ≥70%). Reproduza o gate localmente antes de abrir PR:

```bash
make lint
make test-unit
```

Só `tests/unit/` roda no CI a cada PR — testes de integração (`tests/integration/`) exigem o stack Docker completo e continuam rodando só localmente (`make up && make setup && make test-integration`). O teste ponta a ponta tem workflow próprio (abaixo).

## Teste ponta a ponta (issue #57)

`make e2e` roda o pipeline de verdade e verifica **invariantes**, não só se algo respondeu. Usa um namespace isolado (buckets e tópicos `e2e-*`, banco `fraud_e2e`), então rodar local não apaga dado de desenvolvimento; sem `E2E=1` (o `make e2e` exporta) a suíte é pulada. Precisa de `kafka`, `minio`, `postgres` e `spark-master` de pé; os jobs Spark rodam no container em modo local. ~5 min.

```bash
make e2e                                   # local, contra a stack de pé
# relatório do que aconteceu nos cenários: data/e2e/report.json; resultado: data/e2e/junit.xml
```

No GitHub: workflow **E2E** (`.github/workflows/e2e.yml`), manual (`workflow_dispatch`) e noturno. Sobe só `zookeeper kafka minio postgres spark-master` no runner, roda `make e2e` e publica o relatório como artefato.

**Atenção local:** os cenários de falha param e religam os containers `kafka` e `postgres` da stack de desenvolvimento (~1 min cada). Não rode com um stream de desenvolvimento ou com o Superset em uso.

**Invariantes verificadas:**

| Camada | Invariante |
|---|---|
| Batch | Os mesmos `transaction_id` gerados estão no Bronze, Silver, Gold e Postgres, sem duplicata |
| Batch | `SUM(amount_brl)` igual no Silver, no Gold e no Postgres; rótulos de fraude iguais; o agregado diário soma a fato |
| Batch | Uma linha de perfil de comportamento por cliente da `dim_customers` |
| Stream | Todo evento publicado está no Parquet, e nada além deles (o JSON inválido some) |
| Stream | Nenhuma duplicata no Parquet além do evento reenviado de propósito; na serving layer, uma linha por evento |
| Stream | `enriched-transactions` tem todos os eventos; os `alert_id` do tópico `fraud-alerts` são exatamente os do Postgres |
| Stream | Métricas de plataforma gravadas (`stream_batch_metrics`) |

**Cenários de falha** (automatizados; comportamento observado na validação da issue):

| Cenário | Esperado | Observado | Recuperação |
|---|---|---|---|
| `kill -9` no stream no meio de um micro-batch (Parquet gravado, estado não) | Replay pula as etapas feitas; nada perdido nem duplicado | Pegou o meio do batch; log `Etapa do micro-batch já concluída — ignorada no replay`; 0 duplicata | Subir o stream de novo (mesmo checkpoint) |
| Linha inválida no Bronze (valor negativo, fraude sem tipo) | O gate do Bronze falha e o Silver não é tocado | `QualityGateFailed`; Silver inalterado; sem o arquivo, o gate volta a passar | Corrigir/remover o dado e rerodar |
| Evento duplicado (retry do producer) | Deduplicado no micro-batch; entre micro-batches, uma linha a mais no Parquet e uma só na serving layer | Caiu em outro micro-batch: 1.201 linhas no Parquet para 1.200 ids; Postgres com 1.200 | Nenhuma (at-least-once documentado) |
| JSON inválido no tópico | Descartado no parse, sem derrubar o micro-batch | Descartado; eventos seguintes processados | Nenhuma |
| Kafka fora do ar por 45 s com o stream rodando | O stream espera e retoma | Stream vivo depois; eventos publicados após a volta processados | Nenhuma (se o stream morrer, subir de novo: o checkpoint retoma) |
| Postgres fora do ar com o stream rodando | O stream segue (só as métricas dependem do Postgres); a carga do Postgres falha até ele voltar | Stream vivo e processando; métricas dos micro-batches desse intervalo perdidas (warning no log) | `make spark-submit-stream-postgres` depois que o Postgres volta |

## Troubleshooting

| Problema | Causa Provável | Solução |
|---------|----------------|---------|
| Kafka não conecta | Zookeeper não iniciou | `make logs-zookeeper`, aguardar healthcheck |
| MinIO 403 | Credenciais erradas | Verificar MINIO_ACCESS_KEY no .env |
| Job Spark morre com `ExecutorLostFailure` / `Command exited with code 137` (SIGKILL) | OOM killer: a VM do Docker Desktop (`docker info` mostra `Total Memory`) ficou sem memória — stack completa + 2 executores de 2G + producers/streaming. Não é bug de código | Docker Desktop → Settings → Resources → Memory: **12 GB** (Apply & restart; volumes são preservados). Enquanto isso, pare os producers e o `spark-submit-stream` antes de rodar jobs batch pesados |
| Job batch fica esperando recursos / DAG `batch_transformation_pipeline` não avança | O `spark-submit-stream` (streaming) segura os 4 cores e 4 GB do cluster (2 workers × 2 cores × 2G) | `Ctrl+C` no streaming antes de rodar batch, ou aumentar workers/cores no `docker-compose.yml` |
| Airflow DB error | PostgreSQL não pronto | Aguardar healthcheck, `make logs-postgres` |
| Container `producer-transactions`/`producer-market` reiniciando em loop com `Libraries for lz4 compression codec not found` | A imagem dos producers não tem a lib do codec de compressão (`docker/producer/requirements-producer.txt`) | Reconstruir com `docker compose --profile producers up -d --build`; ver "Rodar os producers" acima |
| GX checkpoint falha | Ver seção "Quality Gates" acima | Diagnosticar via logs da task/data docs; corrigir a causa raiz e rerodar a DAG — nunca editar os JSON gerados em `gx/expectations/` diretamente, só `suites.py` |
| `make test-unit` falha com `JAVA_GATEWAY_EXITED` | JDK ausente no PATH (PySpark local precisa de um JRE) | Instalar Java 17, ex. `brew install openjdk@17` no macOS, e garantir `JAVA_HOME`/`java` no PATH da shell |
| Superset preso em `health: starting`, logs com `Error: No application module specified` | Bug real (corrigido na issue #16): indentação mais funda que a linha-mãe no `command: >` do serviço `superset` quebra o folding do YAML, inserindo uma quebra de linha literal no meio do `gunicorn`/`create-admin` | Ver seção "Dashboards (Superset)" acima — cada comando do `bash -c` deve ficar numa única linha lógica |
| `make up` falha com `dependency failed to start: container minio is unhealthy` | Bug real (corrigido na validação end-to-end, PR #33): o healthcheck do MinIO usava `curl`, ausente na imagem `minio/minio` — falhava sempre, deixando o container `unhealthy` pra sempre e travando qualquer serviço com `depends_on: condition: service_healthy` | Já corrigido em `docker-compose.yml` (`test: ["CMD", "mc", "ready", "local"]` — `mc` vem embutido na imagem, sem precisar de alias); se reaparecer, confirme com `docker inspect minio --format='{{json .State.Health}}'` |
| `make dashboards` (ou outro alvo novo) imprime `is up to date` e não roda nada | Bug real (corrigido na validação end-to-end, PR #33): alvo ausente do `.PHONY` no Makefile e um diretório/arquivo real no repo com o mesmo nome do alvo (ex.: `dashboards/`) faz o Make tratá-lo como já satisfeito | Confirme que o alvo está listado em `.PHONY` no topo do Makefile; todo alvo novo precisa entrar lá, principalmente se o nome colidir com um diretório existente no repo |
