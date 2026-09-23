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
`gold_dim_date`, `gold_agg_daily_fraud_metrics`.

Política: **gate simples** — qualquer expectativa falhando bloqueia a task do
Airflow (e portanto a DAG inteira, via `trigger_rule` padrão), não há
quarentena de linhas nem execução parcial. Recuperação = corrigir a causa e
rerodar a DAG a partir da task que falhou.

### Rodar um gate manualmente

```bash
docker compose exec airflow-scheduler python -m src.governance.great_expectations.runner bronze_transactions
docker compose exec airflow-scheduler python -m src.governance.great_expectations.runner --all
```

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
| `GET /alerts` | `fact_transactions` filtrado por `is_fraud=true` — ver nota em `alerts.py` sobre a fonte de dados |
| `GET /kpis/fraud-daily` | `agg_daily_fraud_metrics` (paginado) |

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
3. **`/alerts` não reflete os alertas do streaming (Z-Score)**: esperado —
   `fraud-alerts` (Kafka, issue #11) não é persistido em nenhuma tabela
   consultável hoje; `/alerts` usa o rótulo `is_fraud` já carregado pelo
   batch. Ligar essa fonte exigiria um consumidor Kafka novo (mesmo gap
   documentado para `fraud_score`, issue #16).

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

### Diagnosticar uma falha de validação

`--strict` sai com código 1 se algum asset não existir na infra real
(prefixo MinIO vazio, tabela Postgres ausente, tópico Kafka inexistente) ou
se alguma referência de linhagem (`upstream`) apontar para uma key
inexistente no registro. O log (`data_catalog`) indica a entry e o motivo;
o próprio `docs/data_catalog.md` gerado também marca cada asset com
❌ e o detalhe, ou 🗓️ planejado para o que ainda não foi implementado.

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
3. **`fraud_score` sempre nulo**: esperado — só o detector de streaming
   (issue #11) popula esse campo, e ele não escreve na camada Gold
   batch/Postgres (mesmo gap de `alerts.py`, issue #15).

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

## CI (GitHub Actions)

`.github/workflows/ci.yml` roda em todo push/PR para `main`: job `lint` (`ruff check` + `mypy`) e job `test` (`pytest tests/unit/`, Java 17 + Python 3.11, gate de cobertura ≥70%). Reproduza o gate localmente antes de abrir PR:

```bash
make lint
make test-unit
```

Só `tests/unit/` roda no CI — testes de integração (`tests/integration/`) exigem o stack Docker completo e continuam rodando só localmente (`make up && make setup && make test-integration`).

## Troubleshooting

| Problema | Causa Provável | Solução |
|---------|----------------|---------|
| Kafka não conecta | Zookeeper não iniciou | `make logs-zookeeper`, aguardar healthcheck |
| MinIO 403 | Credenciais erradas | Verificar MINIO_ACCESS_KEY no .env |
| Spark OOM | Memória insuficiente | Aumentar SPARK_EXECUTOR_MEMORY no .env |
| Airflow DB error | PostgreSQL não pronto | Aguardar healthcheck, `make logs-postgres` |
| GX checkpoint falha | Ver seção "Quality Gates" acima | Diagnosticar via logs da task/data docs; corrigir a causa raiz e rerodar a DAG — nunca editar os JSON gerados em `gx/expectations/` diretamente, só `suites.py` |
| `make test-unit` falha com `JAVA_GATEWAY_EXITED` | JDK ausente no PATH (PySpark local precisa de um JRE) | Instalar Java 17, ex. `brew install openjdk@17` no macOS, e garantir `JAVA_HOME`/`java` no PATH da shell |
| CI falha no `pip install -e ".[dev]"` do job `test`, no pacote `confluent-kafka` | Runner sem a lib nativa `librdkafka` (a wheel manylinux pode não cobrir a imagem do runner) | Adicionar um step `apt-get install -y librdkafka-dev` antes do `pip install` em `.github/workflows/ci.yml`, job `test` |
| Superset preso em `health: starting`, logs com `Error: No application module specified` | Bug real (corrigido na issue #16): indentação mais funda que a linha-mãe no `command: >` do serviço `superset` quebra o folding do YAML, inserindo uma quebra de linha literal no meio do `gunicorn`/`create-admin` | Ver seção "Dashboards (Superset)" acima — cada comando do `bash -c` deve ficar numa única linha lógica |
| `make up` falha com `dependency failed to start: container minio is unhealthy` | Bug real (corrigido na validação end-to-end, PR #33): o healthcheck do MinIO usava `curl`, ausente na imagem `minio/minio` — falhava sempre, deixando o container `unhealthy` pra sempre e travando qualquer serviço com `depends_on: condition: service_healthy` | Já corrigido em `docker-compose.yml` (`test: ["CMD", "mc", "ready", "local"]` — `mc` vem embutido na imagem, sem precisar de alias); se reaparecer, confirme com `docker inspect minio --format='{{json .State.Health}}'` |
| `make dashboards` (ou outro alvo novo) imprime `is up to date` e não roda nada | Bug real (corrigido na validação end-to-end, PR #33): alvo ausente do `.PHONY` no Makefile e um diretório/arquivo real no repo com o mesmo nome do alvo (ex.: `dashboards/`) faz o Make tratá-lo como já satisfeito | Confirme que o alvo está listado em `.PHONY` no topo do Makefile; todo alvo novo precisa entrar lá, principalmente se o nome colidir com um diretório existente no repo |
