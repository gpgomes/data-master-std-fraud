# Arquitetura — Data Master: Financial Fraud Detection Platform

## Visão Geral

A plataforma segue a **Medallion Architecture** (Bronze → Silver → Gold) com suporte a processamento batch e streaming simultâneos.

## Padrão Arquitetural: Lambda

O desenho batch + streaming rodando em paralelo sobre a mesma base de dados (ver "Fluxo de Dados" abaixo) é uma **arquitetura Lambda**:

- **Batch layer** — `bronze_to_silver.py` / `silver_to_gold.py` (PySpark). Reprocessa o histórico completo (ou grandes intervalos via `--start-date/--end-date`) com alta acurácia e latência de minutos. É a fonte da verdade: se a lógica de streaming tiver um bug ou perder eventos, o batch corrige no próximo run, pois sempre reprocessa a partir do dado bruto no Bronze.
- **Speed layer** — Spark Structured Streaming consumindo `raw-transactions` do Kafka, com detecção de fraude via Z-Score em tempo real (segundos). Cobre a janela entre a última execução batch e o presente, mas com garantias mais fracas de completude/correção.
- **Serving layer** — PostgreSQL (Gold, batch) + Kafka `fraud-alerts` (streaming) + FastAPI, que expõem as duas visões para quem consome — dado histórico consolidado (batch) e alertas em tempo real (streaming).

**Por que Lambda e não Kappa?** A arquitetura Kappa elimina a camada batch e trata tudo como stream reprocessável (replay do log do zero quando a lógica muda), reduzindo a duplicação de lógica de negócio entre duas pipelines. Ela foi considerada, mas descartada por dois motivos:
1. **Auditoria/regulatório** — transações financeiras (PIX/TED/DOC) exigem uma trilha batch reprocessável e fácil de auditar; um único pipeline streaming faz esse papel com mais fricção operacional.
2. **Custo de retenção no Kafka** — Kappa exige retenção longa (ou ilimitada) no tópico para permitir replay completo do histórico; no cenário deste projeto isso é mais caro/complexo do que reprocessar Parquet no Bronze via batch.

**Trade-off aceito**: mantemos duas implementações da lógica de transformação (batch em PySpark batch job, streaming em Spark Structured Streaming) — a "dualidade de código" clássica da Lambda. É mitigado parcialmente reaproveitando os mesmos schemas (`src/common/schemas.py`) e configurações (`src/common/config.py`) entre as duas camadas.

## Fluxo de Dados

### Batch
```
yfinance / CSV → Python Collector → MinIO (Bronze/Parquet)
                                         ↓
                              PySpark Bronze→Silver Job
                                         ↓
                              MinIO (Silver/Parquet)
                                         ↓
                              PySpark Silver→Gold Job
                                         ↓
                              MinIO (Gold/Parquet) → PostgreSQL
```

Orquestrado por duas DAGs Airflow em sequência: `batch_ingestion_pipeline`
(06:00 UTC, fontes → Bronze → gate de qualidade) e
`batch_transformation_pipeline` (07:00 UTC, Bronze → Silver → gate → Gold →
gate → PostgreSQL, via `SparkSubmitOperator` intercalado com os quality
gates do Great Expectations, issue #13 — ver seção "Quality Gates" em
`docs/runbook.md`). O loader Gold→PostgreSQL
(`src/serving/loaders/gold_to_postgres.py`, issue #10) roda como a
penúltima task da segunda DAG, truncate+reload por tabela.

### Streaming
```
Simulador Python → Kafka (raw-transactions)
                        ↓
              Spark Structured Streaming
                        ↓
              Anomaly Detection (Z-Score)
                        ↓
    MinIO Silver (silver/transactions_stream/, distinto do
    silver/transactions/ do batch) + Kafka (enriched-transactions,
    todas as linhas scored) + Kafka (fraud-alerts, só as anômalas)
```

**Semântica de entrega (issue #36):** o Spark reexecuta o mesmo micro-batch se o job cair antes de gravar o commit do checkpoint, e o `foreachBatch` escreve em vários destinos sem transação. Por isso cada etapa é idempotente ou pulada no replay: o Parquet é gravado em `silver/transactions_stream/query_id=<id>/batch_id=<n>/` com overwrite dinâmico da própria partição, e cada etapa (parquet, enriched, alerts, histórico do Z-Score) grava um marcador em `checkpoints/stream_processor_progress/` que faz o replay pulá-la. O `alert_id` é derivado de `transaction_id`, então um alerta reprocessado mantém o mesmo id. Garantia final: **at-least-once nos tópicos Kafka** (o Kafka sink do Spark não é transacional; cair entre o fim de uma etapa Kafka e a escrita do seu marcador ainda pode duplicar aquela mensagem, uma janela de milissegundos) e **sem duplicatas no Parquet**. Consumidores devem deduplicar por `transaction_id`.

**Serving do streaming (issue #38):** `stream_to_postgres.py` lê `silver/transactions_stream/` e carrega `stream_scored_transactions` (uma linha por `transaction_id`, com `fraud_score`, `z_score` e `latency_seconds = processing_timestamp - produced_at`) e `fraud_alerts` (os alertas, reconstruídos com a mesma função do detector, então o `alert_id` é idêntico ao do tópico `fraud-alerts`). É um loader batch (truncate + reload, também a última task da DAG `batch_transformation_pipeline`) e não um consumidor Kafka, porque o streaming já ocupa todos os cores do cluster local. Só as colunas úteis vão ao Postgres: `device_id`, `ip_address`, contas e coordenadas ficam de fora.

## Componentes

### Ingestão
- **Kafka** — Message broker para streaming de transações e cotações
- **Python Batch** — Coleta dados via yfinance e CSV, salva no Bronze

### Armazenamento
- **MinIO** — Object storage S3-compatible com quatro buckets: bronze, silver, gold, checkpoints (`checkpointLocation` do Spark Structured Streaming — o estado de histórico entre micro-batches fica em `silver/_stream_state/`, não em `checkpoints/`)
- **Parquet** — Formato único de Silver e Gold (issue #9); ver "Decisões Arquiteturais" abaixo

### Transformação
- **PySpark Batch** — Jobs Bronze→Silver e Silver→Gold
- **Spark Structured Streaming** — Processamento em tempo real com detecção de fraude

### Governança
- **Great Expectations** — Quality gates integrados ao Airflow
- **Catálogo de dados leve** (`src/governance/data_catalog/`) — registro versionado, linhagem e classificação, validado contra a infra real; ver decisão abaixo

### Disponibilização
- **PostgreSQL** — Tabelas Gold para SQL analítico
- **FastAPI** — REST API para consultas e alertas
- **Apache Superset** — Dashboards (provisionado via API REST, `src/serving/dashboards/`; Grafana descoped da V1 — ver decisão abaixo)

## Decisões Arquiteturais

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Padrão batch+streaming | Lambda (não Kappa) | Trilha batch auditável para dados financeiros regulados; evita retenção longa/cara no Kafka que o replay completo do Kappa exigiria |
| Formato de armazenamento | Parquet puro (não Delta Lake) | Silver/Gold são reescritos por completo a cada execução (idempotente via overwrite dinâmico por partição, ver `spark_session.py`), então o log de transação ACID do Delta não agrega valor agora. O Spark session e o Makefile chegaram a configurar extensões Delta (`io.delta:delta-core_2.12:2.4.0`), mas com versão incompatível com o Spark 3.5.1 do cluster (Delta 2.4 é pra Spark 3.4) e nunca de fato usadas (todo `.write` já era `.format("parquet")`) — configuração removida na issue #9. Time travel/versionamento fica para a fase de governança (roadmap 3.6), quando justificar a complexidade extra |
| Schema Registry | Avro embutido no JSON | Simplicidade no ambiente local; substituir por Confluent Schema Registry em produção |
| Detecção de fraude | Z-Score em janelas deslizantes | Baseline simples e interpretável; extensível com ML |
| Serving layer | PostgreSQL + DuckDB | PostgreSQL para OLTP/API; DuckDB para queries analíticas ad-hoc |
| Escopo do CI (GitHub Actions) | Só lint + testes unitários, sem os testes de integração | `tests/unit/` roda 100% local (SparkSession `local[*]`, storage mockado); o stack completo (Kafka, Zookeeper, Airflow, Superset, Postgres, MinIO, cluster Spark) é pesado/lento demais para rodar em todo PR — ver `.github/workflows/ci.yml` |
| Catálogo/linhagem (issue #14) | Registro leve versionado (não OpenMetadata) | O stack oficial do OpenMetadata (server + MySQL/Postgres próprio + Elasticsearch + ingestion-Airflow) soma mais 3-4 serviços pesados aos 19 que já rodam neste `docker-compose.yml`, disputando os ~8GB alocados ao Docker no ambiente local. `src/governance/data_catalog/` cobre o mesmo objetivo (owners, tags PII, glossário, linhagem) sem subir nenhum container novo, validando cada asset contra MinIO/Postgres/Kafka reais. Um catálogo gerenciado real (OpenMetadata ou AWS Glue Data Catalog, já previsto na V2) fica para a fase cloud |
| Dashboards (issue #16) | Só Superset (Grafana descoped) | Superset já roda no `docker-compose.yml`, conectado ao mesmo Postgres da serving layer (issue #10) — cobre 100% dos KPIs pedidos sem novo container/datasource. Não há store de séries temporais (Prometheus etc.) que justifique Grafana para métricas real-time nesta V1; `dashboards/grafana/` fica como scaffold não usado. `src/serving/dashboards/` provisiona tudo via API REST do Superset (idempotente), com smoke test comparando cada chart contra uma query direta no Postgres. Streaming (issue #38): `stream_scored_transactions` e `fraud_alerts` viram datasets do Superset, com 4 charts opcionais (latência média, alertas do detector, distribuição de `fraud_score`, alertas por hora) que não falham a verificação sem dados de streaming |
| Serving do streaming (issue #38) | Loader batch `silver/transactions_stream` → Postgres, não consumidor Kafka | O streaming ocupa os 4 cores do cluster local, então um segundo job de streaming não teria recursos. O Parquet é o registro durável, e como o `alert_id` é determinístico os alertas são reconstruídos com a função do detector, dando exatamente o conteúdo do tópico `fraud-alerts` (conferido: 244 alertas, 0 diferenças de `alert_id`). Custo: a serving layer reflete o streaming com a defasagem da última carga |

## CI/CD

Job `lint` (ruff + mypy) e job `test` (`pytest tests/unit/`, gate de cobertura ≥70%) rodam em paralelo a cada push/PR para `main`, via `.github/workflows/ci.yml`. Reflete só a camada de qualidade de código da futura Fase 6 do roadmap (`CaseFinancialDataLakeHouse.md`, item 6.1) — o deploy automatizado para AWS ainda não existe.
