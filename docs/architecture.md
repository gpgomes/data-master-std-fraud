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
| Padrão batch+streaming | Lambda (não Kappa) | Trilha batch auditável para dados financeiros regulados; evita retenção longa/cara no Kafka que o replay completo do Kappa exigiria |
| Formato de armazenamento | Parquet + Delta Lake | Compressão eficiente, schema evolution, time travel |
| Schema Registry | Avro embutido no JSON | Simplicidade no ambiente local; substituir por Confluent Schema Registry em produção |
| Detecção de fraude | Z-Score em janelas deslizantes | Baseline simples e interpretável; extensível com ML |
| Serving layer | PostgreSQL + DuckDB | PostgreSQL para OLTP/API; DuckDB para queries analíticas ad-hoc |
