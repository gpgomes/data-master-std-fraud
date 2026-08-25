# Data Master — Financial Fraud Detection Platform

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/kafka-3.6-black.svg)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/airflow-2.8-green.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-compose-blue.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Plataforma de dados end-to-end para ingestão, transformação, governança e visualização de dados financeiros com foco em **detecção de fraudes em transações**. Arquitetura Medallion (Bronze → Silver → Gold) com processamento batch e streaming simultâneos.

---

## Visão Geral

**Cenário de negócio:** Uma instituição financeira precisa consolidar dados de mercado em tempo real (cotações, trades) com dados transacionais internos (pagamentos, transferências via PIX/TED/DOC) para detectar fraudes, gerar dashboards analíticos e cumprir requisitos regulatórios de governança e linhagem de dados.

### Arquitetura

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                                   │
│  ┌──────────────┐  ┌──────────────────┐  ┌─────────────────┐             │
│  │ Yahoo Finance│  │ Simulador Python  │  │ Datasets CSV    │             │
│  │ (yfinance)   │  │ (Transações/PIX) │  │ (Históricos)    │             │
│  └──────┬───────┘  └────────┬─────────┘  └────────┬────────┘             │
│         │                   │                     │                      │
├─────────▼───────────────────▼──────────────────────▼─────────────────────┤
│                      CAMADA DE INGESTÃO                                  │
│  ┌────────────────────┐      ┌────────────────────────────┐               │
│  │  Apache Kafka      │      │  Python Batch Ingestion    │               │
│  │  (Streaming)       │      │  (Airflow DAGs)            │               │
│  └────────┬───────────┘      └──────────┬─────────────────┘               │
│           │                             │                                 │
├───────────▼─────────────────────────────▼─────────────────────────────────┤
│                    DATA LAKE — MinIO (S3-compatible)                      │
│  ┌──────────────┐       ┌──────────────┐       ┌──────────────┐           │
│  │   BRONZE     │──────▶│   SILVER     │──────▶│    GOLD      │           │
│  │  (Raw)       │       │  (Cleaned)   │       │  (Business)  │           │
│  │  JSON/CSV    │       │  Parquet     │       │  Parquet     │           │
│  └──────────────┘       └──────────────┘       └──────────────┘           │
├────────────────────────────────────────────────────────────────────────────┤
│                  CAMADA DE TRANSFORMAÇÃO (PySpark)                        │
│  ┌───────────────────────┐    ┌──────────────────────────────┐             │
│  │  Batch (PySpark)      │    │  Spark Structured Streaming  │             │
│  │  Bronze→Silver→Gold   │    │  Detecção de Anomalias       │             │
│  └───────────────────────┘    └──────────────────────────────┘             │
├────────────────────────────────────────────────────────────────────────────┤
│                  CAMADA DE GOVERNANÇA                                     │
│  ┌─────────────────────┐  ┌──────────────┐  ┌───────────────────────┐     │
│  │ Great Expectations  │  │ OpenMetadata │  │ Delta Lake            │     │
│  │ (Data Quality)      │  │ (Catálogo +  │  │ (Versionamento)       │     │
│  │                     │  │  Linhagem)   │  │                       │     │
│  └─────────────────────┘  └──────────────┘  └───────────────────────┘     │
├────────────────────────────────────────────────────────────────────────────┤
│                  CAMADA DE DISPONIBILIZAÇÃO                               │
│  ┌──────────────────────┐  ┌──────────────┐  ┌──────────────────────┐     │
│  │ PostgreSQL / DuckDB  │  │  FastAPI     │  │ Grafana / Superset   │     │
│  │ (Serving Layer)      │  │  (REST API)  │  │ (Dashboards)         │     │
│  └──────────────────────┘  └──────────────┘  └──────────────────────┘     │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Stack Tecnológico

### V1 — Desenvolvimento Local

| Camada | Tecnologia | Função |
|--------|-----------|--------|
| Ingestão Streaming | Apache Kafka (Docker) | Message broker para eventos em tempo real |
| Ingestão Batch | Python + yfinance | Coleta de dados via API e CSVs |
| Orquestração | Apache Airflow (Docker) | Agendamento e monitoramento de DAGs |
| Processamento Batch | PySpark (local mode) | Transformações Bronze → Silver → Gold |
| Processamento Streaming | Spark Structured Streaming | Consumo do Kafka e detecção de fraudes |
| Armazenamento | MinIO (Docker) | Object storage compatível com S3 |
| Serving Layer | PostgreSQL + DuckDB | Consulta analítica das camadas Gold |
| Qualidade de Dados | Great Expectations | Validações e expectativas nos dados |
| Catálogo/Linhagem | OpenMetadata (Docker) | Data catalog e data lineage |
| Visualização | Apache Superset / Grafana | Dashboards analíticos |
| Containerização | Docker + Docker Compose | Toda a infra local containerizada |
| Linguagem | Python 3.11+ | Toda a lógica de negócio |

### V2 — Cloud AWS

| Camada | Tecnologia |
|--------|-----------|
| Streaming | Amazon MSK (Managed Kafka) |
| Batch | AWS Lambda + EventBridge |
| Orquestração | Amazon MWAA (Managed Airflow) |
| Processamento | AWS EMR (Spark) |
| Armazenamento | Amazon S3 |
| Serving | Amazon Athena + Redshift Serverless |
| Catálogo | AWS Glue Data Catalog |
| Visualização | Amazon QuickSight |
| IaC | Terraform |
| CI/CD | GitHub Actions |

---

## Estrutura do Repositório

```
data-master-std-fraud/
├── README.md
├── docker-compose.yml              # Orquestração de todos os serviços
├── .env.example                    # Variáveis de ambiente (copie para .env)
├── Makefile                        # Comandos úteis
├── pyproject.toml                  # Configuração Python e dependências
│
├── src/                            # Código-fonte principal
│   ├── ingestion/
│   │   ├── batch/                  # Coleta batch via APIs e CSVs
│   │   └── streaming/              # Kafka producers
│   ├── transformation/
│   │   ├── batch/                  # PySpark jobs (Bronze→Silver→Gold)
│   │   └── streaming/              # Spark Structured Streaming + detecção de fraude
│   ├── serving/
│   │   ├── api/                    # FastAPI REST API
│   │   └── loaders/                # Carregadores Gold→PostgreSQL
│   ├── governance/
│   │   ├── great_expectations/     # Suites de qualidade de dados
│   │   └── data_catalog/           # Configuração OpenMetadata
│   └── common/                     # Utilitários compartilhados
│       ├── config.py               # Configurações centralizadas
│       ├── logger.py               # Logger estruturado
│       └── schemas.py              # Schemas Pydantic / PySpark
│
├── dags/                           # Airflow DAGs
├── dashboards/                     # Configurações Superset e Grafana
├── docker/                         # Dockerfiles customizados
├── terraform/                      # IaC para AWS (V2)
├── tests/
│   ├── unit/                       # Testes unitários
│   ├── integration/                # Testes de integração
│   └── conftest.py
├── scripts/                        # Scripts de setup e utilitários
└── docs/                           # Documentação adicional
```

---

## Setup Local

### Pré-requisitos

- Docker Desktop 24+ com Docker Compose v2
- Python 3.11+
- Java 17 (JDK) — necessário para `make test-unit`/`make test`, que rodam PySpark em modo local
- Make (GNU Make)
- 8GB RAM disponível para os containers

### Início Rápido

```bash
# 1. Clonar o repositório
git clone https://github.com/gpgomes/data-master-std-fraud.git
cd data-master-std-fraud

# 2. Configurar variáveis de ambiente
cp .env.example .env

# 3. Subir toda a infraestrutura
make up

# 4. Aguardar os serviços ficarem prontos (~2 min) e inicializar
make setup

# 5. Gerar dados de exemplo
make seed-data

# 6. Executar pipeline batch
make spark-submit-batch

# 7. Iniciar streaming (em outro terminal)
make spark-submit-stream
```

### Acessos Locais

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow | http://localhost:8082 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| API (Swagger) | http://localhost:8000/docs | — |
| Spark UI | http://localhost:8081 | — |
| OpenMetadata | http://localhost:8585 | admin / admin |

### Comandos Make Disponíveis

```bash
make up                  # Subir todos os containers
make down                # Derrubar todos os containers
make setup               # Inicializar buckets MinIO e tópicos Kafka
make test                # Executar todos os testes (pytest)
make lint                # Verificar código (ruff + black)
make spark-submit-batch  # Submeter job batch PySpark
make spark-submit-stream # Submeter job streaming PySpark
make seed-data           # Gerar dados sintéticos de exemplo
make clean               # Limpar volumes e dados temporários
```

---

## Detalhamento dos Dados

### Tópicos Kafka

| Tópico | Descrição | Producer |
|--------|-----------|---------|
| `raw-transactions` | Transações financeiras em tempo real | kafka_producer_transactions.py |
| `raw-market-data` | Cotações e trades de mercado | kafka_producer_market.py |
| `enriched-transactions` | Transações enriquecidas com score | stream_processor.py |
| `fraud-alerts` | Alertas de transações fraudulentas | anomaly_detector.py |

### Camadas do Data Lake

| Camada | Formato | Particionamento | Conteúdo |
|--------|---------|----------------|---------|
| Bronze | JSON / CSV | `year/month/day` | Dados raw sem transformação |
| Silver | Parquet | `year/month/day` | Dados limpos, deduplicados, tipados |
| Gold | Parquet | Por domínio | Agregações, Star Schema, KPIs |

### Tipos de Fraude Detectados

- **ACCOUNT_TAKEOVER** — Acesso e operações por terceiros na conta
- **CARD_CLONING** — Uso de cartão clonado em localização diferente
- **IDENTITY_THEFT** — Operações com dados de identidade roubados
- **MONEY_LAUNDERING** — Padrão de lavagem (smurfing, layering)
- **SOCIAL_ENGINEERING** — Fraude via engenharia social (PIX falso)

---

## Governança de Dados

### Great Expectations — Quality Gates

- **Bronze:** Schema validation, completude de campos obrigatórios, freshness check
- **Silver:** Unicidade de chaves, ranges de valores, consistência referencial
- **Gold:** Integridade de agregações, SLAs de atualização

### OpenMetadata — Catálogo e Linhagem

- Catálogo centralizado de todos os datasets (Bronze, Silver, Gold)
- Linhagem automática: fonte → Bronze → Silver → Gold → Dashboard
- Business Glossary: VWAP, Volatilidade, Fraud Score, etc.
- Tags de classificação: PII, Confidencial, Público

---

## Contribuição

Este projeto segue o fluxo de desenvolvimento por fases descrito em `CaseFinancialDataLakeHouse.md`. Para contribuir, abra uma issue ou pull request referenciando a fase correspondente.

---

## Licença

MIT License — veja [LICENSE](LICENSE) para detalhes.
