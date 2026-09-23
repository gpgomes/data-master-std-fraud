# Catálogo de Dados

_Gerado em 2026-09-23T19:05:20.718509+00:00 por `python -m scripts.build_data_catalog`._

Substitui o OpenMetadata completo na V1 local (decisão documentada em `docs/architecture.md`) — ver definições de campo e o glossário de negócio completo em [`docs/data_dictionary.md`](data_dictionary.md).

## Bronze

| Dataset | Localização | Owner | Classificação | Glossário | Status |
|---------|-------------|-------|---------------|-----------|--------|
| **Bronze — Transações**<br>Transações financeiras raw, conforme recebidas do Kafka. | `s3://bronze/transactions/` | Data Engineering | PII, Confidencial | — | ✅ ok |
| **Bronze — Market Data**<br>Cotações OHLCV coletadas via yfinance. | `s3://bronze/market_data/` | Data Engineering | Público | VWAP, Volatilidade | ⚠️ sem dados (opcional): nenhum objeto encontrado em s3://bronze/market_data/ |

## Silver

| Dataset | Localização | Owner | Classificação | Glossário | Status |
|---------|-------------|-------|---------------|-----------|--------|
| **Silver — Transações**<br>Transações limpas, deduplicadas, timestamps normalizados para UTC. | `s3://silver/transactions/` | Data Engineering | PII, Confidencial | — | ✅ ok |
| **Silver — Market Data**<br>Cotações enriquecidas com retorno diário e price range. | `s3://silver/market_data/` | Data Engineering | Público | VWAP, Volatilidade | ⚠️ sem dados (opcional): nenhum objeto encontrado em s3://silver/market_data/ |

## Gold

| Dataset | Localização | Owner | Classificação | Glossário | Status |
|---------|-------------|-------|---------------|-----------|--------|
| **Gold — Fato Transações**<br>Tabela fato de transações — grão: uma linha por transação. | `s3://gold/fact_transactions/` | Analytics Engineering | PII, Confidencial | Fraud Score | ✅ ok |
| **Gold — Dimensão Clientes**<br>Dimensão de clientes (SCD2, apenas registro corrente). | `s3://gold/dim_customers/` | Analytics Engineering | PII, Confidencial | — | ✅ ok |
| **Gold — Dimensão Data**<br>Dimensão de calendário derivada das datas distintas do Silver. | `s3://gold/dim_date/` | Analytics Engineering | Público | — | ✅ ok |
| **Gold — Métricas Diárias de Fraude**<br>Agregação diária de volume e taxa de fraude por tipo de transação. | `s3://gold/agg_daily_fraud_metrics/` | Analytics Engineering | Confidencial | Fraud Score | ✅ ok |

## Serving (PostgreSQL)

| Dataset | Localização | Owner | Classificação | Glossário | Status |
|---------|-------------|-------|---------------|-----------|--------|
| **Postgres — fact_transactions**<br>Espelho da tabela fato Gold, carregado via truncate+reload (issue #10). | `fact_transactions` | Analytics Engineering | PII, Confidencial | Fraud Score | ✅ ok |
| **Postgres — dim_customers**<br>Espelho da dimensão de clientes Gold. | `dim_customers` | Analytics Engineering | PII, Confidencial | — | ✅ ok |
| **Postgres — dim_date**<br>Espelho da dimensão de calendário Gold. | `dim_date` | Analytics Engineering | Público | — | ✅ ok |
| **Postgres — agg_daily_fraud_metrics**<br>Espelho da agregação diária de fraude Gold, consumido pela API (issue #15). | `agg_daily_fraud_metrics` | Analytics Engineering | Confidencial | Fraud Score | ✅ ok |

## Streaming (Kafka)

| Dataset | Localização | Owner | Classificação | Glossário | Status |
|---------|-------------|-------|---------------|-----------|--------|
| **Kafka — raw-transactions**<br>Transações publicadas em tempo real pelo producer. | `raw-transactions` | Data Engineering | PII, Confidencial | — | ✅ ok |
| **Kafka — raw-market-data**<br>Cotações publicadas em tempo real pelo producer. | `raw-market-data` | Data Engineering | Público | VWAP | ✅ ok |
| **Kafka — enriched-transactions**<br>Transações com fraud_score anexado pelo StreamProcessor (issue #11). | `enriched-transactions` | Data Engineering | PII, Confidencial | Z-Score, Fraud Score | ✅ ok |
| **Kafka — fraud-alerts**<br>Alertas de fraude confirmados pelo detector Z-Score (issue #11). | `fraud-alerts` | Fraud Analytics | PII, Confidencial | Z-Score, Fraud Score, Velocity Check | ✅ ok |

## Dashboards

| Dataset | Localização | Owner | Classificação | Glossário | Status |
|---------|-------------|-------|---------------|-----------|--------|
| **Dashboard — Visão Geral de Fraude**<br>KPIs de volume, valor, taxa de fraude e alertas (Superset — Grafana descoped, issue #16). | `http://localhost:8088/superset/dashboard/fraude-transacoes-visao-geral/` | Fraud Analytics | Confidencial | Fraud Score | — não validado |

## Linhagem

```mermaid
graph LR
    bronze_transactions["Bronze — Transações"]
    bronze_market_data["Bronze — Market Data"]
    silver_transactions["Silver — Transações"]
    silver_market_data["Silver — Market Data"]
    gold_fact_transactions["Gold — Fato Transações"]
    gold_dim_customers["Gold — Dimensão Clientes"]
    gold_dim_date["Gold — Dimensão Data"]
    gold_agg_daily_fraud_metrics["Gold — Métricas Diárias de Fraude"]
    serving_fact_transactions["Postgres — fact_transactions"]
    serving_dim_customers["Postgres — dim_customers"]
    serving_dim_date["Postgres — dim_date"]
    serving_agg_daily_fraud_metrics["Postgres — agg_daily_fraud_metrics"]
    kafka_raw_transactions["Kafka — raw-transactions"]
    kafka_raw_market_data["Kafka — raw-market-data"]
    kafka_enriched_transactions["Kafka — enriched-transactions"]
    kafka_fraud_alerts["Kafka — fraud-alerts"]
    dashboard_fraud_overview["Dashboard — Visão Geral de Fraude"]
    bronze_transactions --> silver_transactions
    bronze_market_data --> silver_market_data
    silver_transactions --> gold_fact_transactions
    silver_transactions --> gold_dim_customers
    silver_transactions --> gold_dim_date
    gold_fact_transactions --> gold_agg_daily_fraud_metrics
    gold_fact_transactions --> serving_fact_transactions
    gold_dim_customers --> serving_dim_customers
    gold_dim_date --> serving_dim_date
    gold_agg_daily_fraud_metrics --> serving_agg_daily_fraud_metrics
    kafka_raw_transactions --> kafka_enriched_transactions
    kafka_enriched_transactions --> kafka_fraud_alerts
    serving_fact_transactions --> dashboard_fraud_overview
    serving_agg_daily_fraud_metrics --> dashboard_fraud_overview
```
