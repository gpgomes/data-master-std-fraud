# Arquitetura — Data Master: Financial Fraud Detection Platform

## Visão Geral

A plataforma segue a **Medallion Architecture** (Bronze → Silver → Gold) com suporte a processamento batch e streaming simultâneos.

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
- **Delta Lake** — Versionamento e time travel sobre Parquet

### Transformação
- **PySpark Batch** — Jobs Bronze→Silver e Silver→Gold
- **Spark Structured Streaming** — Processamento em tempo real com detecção de fraude

### Governança
- **Great Expectations** — Quality gates integrados ao Airflow
- **OpenMetadata** — Catálogo, linhagem e glossário

### Disponibilização
- **PostgreSQL** — Tabelas Gold para SQL analítico
- **FastAPI** — REST API para consultas e alertas
- **Superset / Grafana** — Dashboards

## Decisões Arquiteturais

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Formato de armazenamento | Parquet + Delta Lake | Compressão eficiente, schema evolution, time travel |
| Schema Registry | Avro embutido no JSON | Simplicidade no ambiente local; substituir por Confluent Schema Registry em produção |
| Detecção de fraude | Z-Score em janelas deslizantes | Baseline simples e interpretável; extensível com ML |
| Serving layer | PostgreSQL + DuckDB | PostgreSQL para OLTP/API; DuckDB para queries analíticas ad-hoc |
