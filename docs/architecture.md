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
(06:00 UTC, fontes → Bronze) e `batch_transformation_pipeline` (07:00 UTC,
Bronze → Silver → Gold, via `SparkSubmitOperator`). O loader Gold→PostgreSQL
ainda não existe (issue #10).

### Streaming
```
Simulador Python → Kafka (raw-transactions)
                        ↓
              Spark Structured Streaming
                        ↓
              Anomaly Detection (Z-Score)
                        ↓
         MinIO Silver + Kafka (fraud-alerts)
```

## Componentes

### Ingestão
- **Kafka** — Message broker para streaming de transações e cotações
- **Python Batch** — Coleta dados via yfinance e CSV, salva no Bronze

### Armazenamento
- **MinIO** — Object storage S3-compatible com três buckets: bronze, silver, gold
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
- **Superset / Grafana** — Dashboards

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

## CI/CD

Job `lint` (ruff + mypy) e job `test` (`pytest tests/unit/`, gate de cobertura ≥70%) rodam em paralelo a cada push/PR para `main`, via `.github/workflows/ci.yml`. Reflete só a camada de qualidade de código da futura Fase 6 do roadmap (`CaseFinancialDataLakeHouse.md`, item 6.1) — o deploy automatizado para AWS ainda não existe.
