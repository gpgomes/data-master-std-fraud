# Case de Engenharia de Dados — Financial Data Lakehouse

## Proposta: Plataforma de Dados Financeiros com Arquitetura Medallion

**Domínio:** Análise de Mercado Financeiro e Detecção de Anomalias em Transações  
**Perfil:** Engenheiro de Dados Senior — Instituição Financeira  
**Duração estimada:** 12 semanas (3 meses)

---

## 1. Visão Geral do Case

O projeto consiste em construir uma **plataforma de dados end-to-end** para ingestão, transformação, governança e visualização de dados financeiros de mercado (ações, câmbio) e dados transacionais simulados. A arquitetura segue o padrão **Medallion (Bronze → Silver → Gold)** e contempla processamento **batch e streaming** simultaneamente.

**Cenário de negócio:** Uma instituição financeira precisa consolidar dados de mercado em tempo real (cotações, trades) com dados transacionais internos (pagamentos, transferências) para gerar dashboards analíticos, detectar anomalias e cumprir requisitos regulatórios de governança e linhagem de dados.

---

## 2. Arquitetura de Referência

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐                 │
│  │ APIs Mercado │  │ Simulador de │  │ Datasets CSV    │                 │
│  │ (Yahoo Fin,  │  │ Transações   │  │ (Históricos)    │                 │
│  │  Alpha Vant.)│  │ (Python)     │  │                 │                 │
│  └──────┬───────┘  └──────┬───────┘  └────────┬────────┘                 │
│         │                 │                   │                          │
├─────────▼─────────────────▼────────────────────▼─────────────────────────┤
│                      CAMADA DE INGESTÃO                                  │
│  ┌────────────────────┐  ┌───────────────────────────┐                   │
│  │  Apache Kafka      │  │  Python Batch Ingestion   │                   │
│  │  (Streaming)       │  │  (Airflow/Cron)           │                   │
│  └────────┬───────────┘  └──────────┬────────────────┘                   │
│           │                         │                                    │
├───────────▼─────────────────────────▼────────────────────────────────────┤
│                    CAMADA DE ARMAZENAMENTO                               │
│  ┌──────────────────────────────────────────────────────────┐            │
│  │                  Data Lake (MinIO / S3)                  │            │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐            │            │
│  │  │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │            │            │
│  │  │ (Raw)    │    │(Cleaned) │    │(Business)│            │            │
│  │  │ JSON/CSV │    │ Parquet  │    │ Parquet  │            │            │
│  │  └──────────┘    └──────────┘    └──────────┘            │            │
│  └──────────────────────────────────────────────────────────┘            │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│                  CAMADA DE TRANSFORMAÇÃO                                 │
│  ┌──────────────────────┐   ┌───────────────────────────┐                │
│  │  PySpark (Batch)     │   │  Spark Structured         │                │
│  │  - Limpeza           │   │  Streaming (Real-time)    │                │
│  │  - Deduplicação      │   │  - Enriquecimento         │                │
│  │  - Agregações        │   │  - Detecção de anomalias  │                │
│  └──────────────────────┘   └───────────────────────────┘                │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│                  CAMADA DE GOVERNANÇA                                    │
│  ┌────────────────────┐  ┌──────────────┐  ┌────────────────────┐        │
│  │ Great Expectations │  │ OpenMetadata │  │ Delta Lake /       │        │
│  │ (Data Quality)     │  │ (Catálogo +  │  │ Versionamento      │        │
│  │                    │  │  Linhagem)   │  │                    │        │
│  └────────────────────┘  └──────────────┘  └────────────────────┘        │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│                  CAMADA DE DISPONIBILIZAÇÃO                              │
│  ┌─────────────────────┐  ┌──────────────┐  ┌────────────────────┐       │
│  │ PostgreSQL / DuckDB │  │ API REST     │  │ Grafana /          │       │
│  │ (Serving Layer)     │  │ (FastAPI)    │  │ Apache Superset    │       │
│  └─────────────────────┘  └──────────────┘  └────────────────────┘       │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Repositórios de Referência no GitHub

Os repositórios abaixo servem como base de inspiração e código reutilizável:

### 3.1 Streaming & Market Data
- **RSKriegs/finnhub-streaming-data-pipeline** — Pipeline de streaming com Finnhub, Kafka, Spark, Cassandra, Grafana, Docker e Terraform. Excelente referência de arquitetura completa com infraestrutura como código.
- **radoslawkrolikowski/financial-market-data-analysis** — Processamento de dados financeiros em tempo real com Kafka, PySpark Structured Streaming e MariaDB.
- **ElfatihZiad/realtime-market-data-pipeline** — Pipeline de dados financeiros com Kafka, Cassandra e Bokeh para visualização.
- **dogukannulu/kafka_spark_structured_streaming** — Projeto end-to-end com Kafka, Spark Structured Streaming, Airflow, Docker e Cassandra.

### 3.2 Batch & Lakehouse
- **hoangsonww/End-to-End-Data-Pipeline** — Pipeline production-ready com Kafka, Spark (batch + streaming), Airflow, Great Expectations, Terraform, MinIO e Grafana. Possui governança com Apache Atlas e ML com MLflow.
- **ankurchavda/streamify** — Pipeline com Kafka, Spark Streaming, dbt, Docker, Airflow e Terraform. Referência para arquitetura híbrida batch/streaming.
- **ajupton/big-data-engineering-project** — ETL com Airflow e Spark usando AWS S3 e EMR, bom exemplo de pipeline batch em nuvem.

### 3.3 Governança & Qualidade
- **Great Expectations (greatexpectations.io)** — Framework open-source Python para validação de qualidade de dados. Licença Apache 2.0.
- **OpenMetadata (open-metadata.org)** — Plataforma open-source para catálogo de dados, linhagem e governança com 90+ conectores.

---

## 4. Stack Tecnológico Detalhado

### Versão 1 — Desenvolvimento Local (Semanas 1–6)

| Camada | Tecnologia | Função |
|--------|-----------|--------|
| Ingestão Streaming | Apache Kafka (Docker) | Message broker para eventos em tempo real |
| Ingestão Batch | Python + requests/yfinance | Coleta de dados via API e CSVs |
| Orquestração | Apache Airflow (Docker) | Agendamento e monitoramento de DAGs |
| Processamento Batch | PySpark (local mode) | Transformações Bronze → Silver → Gold |
| Processamento Streaming | Spark Structured Streaming | Consumo do Kafka e enriquecimento |
| Armazenamento | MinIO (Docker) | Object storage compatível com S3 |
| Serving Layer | PostgreSQL + DuckDB | Consulta analítica das camadas Gold |
| Qualidade de Dados | Great Expectations | Validações e expectativas nos dados |
| Catálogo/Linhagem | OpenMetadata (Docker) | Data catalog e data lineage |
| Visualização | Apache Superset ou Grafana | Dashboards analíticos |
| Containerização | Docker + Docker Compose | Toda a infra local containerizada |
| Linguagem | Python 3.11+ | Toda a lógica de negócio |

### Versão 2 — Cloud AWS (Semanas 7–12)

| Camada | Tecnologia | Função |
|--------|-----------|--------|
| Ingestão Streaming | Amazon MSK (Managed Kafka) | Kafka gerenciado |
| Ingestão Batch | AWS Lambda + EventBridge | Triggers de coleta serverless |
| Orquestração | Amazon MWAA (Managed Airflow) | Airflow gerenciado |
| Processamento | AWS EMR (Spark) | Cluster Spark para batch e streaming |
| Armazenamento | Amazon S3 | Data Lake com particionamento |
| Serving Layer | Amazon Athena + Redshift Serverless | SQL analítico serverless |
| Qualidade de Dados | Great Expectations no EMR | Validações integradas ao pipeline |
| Catálogo/Linhagem | AWS Glue Data Catalog | Catálogo nativo AWS |
| Visualização | Amazon QuickSight | Dashboards nativos AWS |
| IaC | Terraform | Provisioning de toda a infra AWS |
| CI/CD | GitHub Actions | Deploy automatizado |

---

## 5. Detalhamento dos Componentes

### 5.1 Ingestão de Dados

**Batch (Python):**
- Script Python usando `yfinance` para coleta diária de cotações históricas (OHLCV).
- Ingestão de datasets CSV públicos simulando transações financeiras (ex: Brazilian E-Commerce da Olist no Kaggle, adaptado para contexto financeiro).
- DAG do Airflow orquestrando a coleta, validação e escrita no MinIO/S3 na camada Bronze.

**Streaming (Kafka + Python):**
- Producer Python conectando a WebSocket de dados de mercado (Finnhub ou simulador) e publicando no tópico Kafka `market-trades`.
- Segundo producer simulando transações bancárias em tempo real no tópico `bank-transactions`.
- Spark Structured Streaming consumindo ambos os tópicos, aplicando transformações e escrevendo na camada Silver.

### 5.2 Transformação de Dados

**Camada Bronze → Silver (PySpark):**
- Parsing de JSON/CSV para schema estruturado.
- Deduplicação por chave primária + timestamp.
- Tratamento de nulos e outliers.
- Normalização de tipos (moedas, datas, fusos horários).
- Escrita em formato Parquet com particionamento por data.

**Camada Silver → Gold (PySpark):**
- Agregações de negócio: volume diário por ativo, VWAP, médias móveis.
- Cálculo de indicadores: volatilidade, spread, concentração por setor.
- Tabelas dimensionais: dim_ativos, dim_clientes, dim_categorias.
- Tabelas fato: fato_trades, fato_transacoes, fato_anomalias.
- Modelo Star Schema para consumo analítico.

**Detecção de anomalias (Streaming):**
- Z-Score em janelas deslizantes para identificar transações atípicas.
- Regras de negócio para flagging: valor > 3σ da média, frequência anormal, horários incomuns.

### 5.3 Governança de Dados

**Great Expectations — Qualidade:**
- Expectations definidas para cada camada:
  - Bronze: schema validation, completude de campos obrigatórios, freshness check.
  - Silver: unicidade de chaves, ranges de valores, consistência referencial.
  - Gold: integridade de agregações, SLAs de atualização.
- Data Docs gerados automaticamente com relatórios HTML de qualidade.
- Integração com Airflow: DAGs falham se expectations não forem atendidas.

**OpenMetadata — Catálogo e Linhagem:**
- Catálogo centralizado de todos os datasets (Bronze, Silver, Gold).
- Linhagem automática: visualização do fluxo de dados desde a fonte até o dashboard.
- Business Glossary: definições padronizadas de métricas (VWAP, Volatilidade, etc.).
- Tags de classificação: PII, Confidencial, Público.
- Ownership de datasets por equipe/pessoa.

**Versionamento e Auditoria:**
- Delta Lake (ou Iceberg) para versionamento das tabelas, time travel e rollback.
- Logs de auditoria: quem acessou, quando, quais transformações foram aplicadas.

### 5.4 Disponibilização de Dados

- **PostgreSQL/DuckDB:** Tabelas Gold carregadas para consulta SQL direta.
- **API REST (FastAPI):** Endpoints para consulta de dados agregados, status de pipelines e métricas de qualidade.
- **Athena/Redshift (V2):** Consulta direta sobre Parquet no S3 via SQL.

### 5.5 Visualização de Dados

**Dashboards propostos (Superset/Grafana na V1, QuickSight na V2):**

1. **Market Overview:** Cotações em tempo real, volume de negociação, top movers.
2. **Transaction Monitor:** Volume de transações, distribuição por tipo/valor, alertas de anomalia.
3. **Data Quality Scorecard:** Status das validações por camada, tendências de qualidade, SLA compliance.
4. **Pipeline Health:** Status dos DAGs, latência de processamento, throughput.

---

## 6. Estrutura do Repositório

```
financial-data-lakehouse/
├── README.md
├── docker-compose.yml              # Orquestração de todos os serviços
├── .env.example                     # Variáveis de ambiente
├── Makefile                         # Comandos úteis (make up, make test, etc.)
│
├── terraform/                       # IaC para AWS (V2)
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── modules/
│   │   ├── s3/
│   │   ├── msk/
│   │   ├── emr/
│   │   ├── mwaa/
│   │   ├── glue/
│   │   └── networking/
│   └── environments/
│       ├── dev/
│       └── prod/
│
├── src/
│   ├── ingestion/
│   │   ├── batch/
│   │   │   ├── market_data_collector.py
│   │   │   ├── transaction_loader.py
│   │   │   └── utils.py
│   │   └── streaming/
│   │       ├── kafka_producer_market.py
│   │       ├── kafka_producer_transactions.py
│   │       └── schemas/
│   │           ├── trade_event.avsc
│   │           └── transaction_event.avsc
│   │
│   ├── transformation/
│   │   ├── batch/
│   │   │   ├── bronze_to_silver.py         # PySpark job
│   │   │   ├── silver_to_gold.py           # PySpark job
│   │   │   └── schemas.py
│   │   └── streaming/
│   │       ├── stream_processor.py          # Spark Structured Streaming
│   │       └── anomaly_detector.py
│   │
│   ├── serving/
│   │   ├── api/
│   │   │   ├── main.py                     # FastAPI app
│   │   │   ├── routes/
│   │   │   └── models/
│   │   └── loaders/
│   │       └── gold_to_postgres.py
│   │
│   └── governance/
│       ├── great_expectations/
│       │   ├── great_expectations.yml
│       │   ├── expectations/
│       │   │   ├── bronze_market_data.json
│       │   │   ├── silver_trades.json
│       │   │   └── gold_aggregations.json
│       │   └── checkpoints/
│       └── data_catalog/
│           └── openmetadata_config.yml
│
├── dags/                            # Airflow DAGs
│   ├── dag_batch_ingestion.py
│   ├── dag_transformation.py
│   ├── dag_quality_checks.py
│   └── dag_gold_loader.py
│
├── dashboards/
│   ├── superset/
│   │   └── dashboard_configs/
│   └── grafana/
│       └── provisioning/
│
├── docker/
│   ├── spark/Dockerfile
│   ├── airflow/Dockerfile
│   ├── producer/Dockerfile
│   └── api/Dockerfile
│
├── tests/
│   ├── unit/
│   │   ├── test_transformations.py
│   │   └── test_anomaly_detector.py
│   ├── integration/
│   │   ├── test_pipeline_e2e.py
│   │   └── test_kafka_spark.py
│   └── conftest.py
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── runbook.md
│   └── images/
│
├── scripts/
│   ├── setup_local.sh
│   ├── generate_sample_data.py
│   └── seed_openmetadata.py
│
├── requirements.txt
├── pyproject.toml
└── .github/
    └── workflows/
        ├── ci.yml
        └── cd_deploy_aws.yml
```

---

## 7. Plano de Implementação Sequencial

### Fase 1 — Fundação Local (Semanas 1–2)

**Objetivo:** Infraestrutura base funcionando com Docker Compose.

| # | Atividade | Entregável |
|---|-----------|-----------|
| 1.1 | Setup do repositório, pyproject.toml, Makefile | Repo configurado |
| 1.2 | Docker Compose com Kafka (Confluent), Zookeeper, MinIO, PostgreSQL | `docker-compose up` funcional |
| 1.3 | Producer Python enviando dados simulados para Kafka | Tópicos com dados fluindo |
| 1.4 | Script batch coletando dados históricos (yfinance) e salvando no MinIO (Bronze) | Dados no bucket Bronze |
| 1.5 | Airflow local com Docker, DAG de teste | Airflow UI acessível |

### Fase 2 — Transformações PySpark (Semanas 3–4)

**Objetivo:** Pipeline batch e streaming funcionando com Medallion Architecture.

| # | Atividade | Entregável |
|---|-----------|-----------|
| 2.1 | PySpark job: Bronze → Silver (limpeza, dedup, parsing) | Job executando em local mode |
| 2.2 | PySpark job: Silver → Gold (agregações, star schema) | Tabelas Gold em Parquet |
| 2.3 | Spark Structured Streaming consumindo Kafka → Silver | Stream processando em tempo real |
| 2.4 | Anomaly detector no streaming (Z-Score windowed) | Anomalias flagged e escritas |
| 2.5 | DAGs do Airflow orquestrando batch pipeline | Pipeline batch agendado |

### Fase 3 — Governança de Dados (Semanas 5–6)

**Objetivo:** Qualidade, catálogo e linhagem implementados.

| # | Atividade | Entregável |
|---|-----------|-----------|
| 3.1 | Great Expectations: expectations para Bronze, Silver, Gold | Suites de validação |
| 3.2 | Integração GX com Airflow (checkpoint como task) | DAGs com quality gates |
| 3.3 | Data Docs com relatórios de qualidade | Relatórios HTML acessíveis |
| 3.4 | OpenMetadata via Docker: catálogo dos datasets | Catálogo com todos os assets |
| 3.5 | Configurar linhagem, glossário e tags no OpenMetadata | Linhagem visual |
| 3.6 | Delta Lake / Iceberg para versionamento (time travel) | Tabelas versionadas |

### Fase 4 — Disponibilização e Visualização (Semanas 7–8)

**Objetivo:** Dados acessíveis via SQL, API e dashboards.

| # | Atividade | Entregável |
|---|-----------|-----------|
| 4.1 | Loader Gold → PostgreSQL | Tabelas Gold no Postgres |
| 4.2 | API FastAPI com endpoints de consulta | API documentada (Swagger) |
| 4.3 | Superset/Grafana: Dashboard Market Overview | Dashboard funcional |
| 4.4 | Dashboard Transaction Monitor com alertas | Painel de transações |
| 4.5 | Dashboard Data Quality Scorecard | Métricas de qualidade |
| 4.6 | Testes unitários e de integração | Cobertura > 70% |

### Fase 5 — Migração para AWS (Semanas 9–10)

**Objetivo:** Infraestrutura provisionada via Terraform na AWS.

| # | Atividade | Entregável |
|---|-----------|-----------|
| 5.1 | Módulos Terraform: VPC, S3, IAM | Rede e storage na AWS |
| 5.2 | Terraform: Amazon MSK (Kafka gerenciado) | Kafka na nuvem |
| 5.3 | Terraform: EMR cluster com Spark | Cluster Spark |
| 5.4 | Terraform: MWAA (Airflow gerenciado) | Airflow na AWS |
| 5.5 | Terraform: Glue Data Catalog | Catálogo AWS |
| 5.6 | Deploy dos jobs PySpark no EMR | Pipelines rodando na cloud |

### Fase 6 — Refinamento e Apresentação (Semanas 11–12)

**Objetivo:** Polimento, documentação e preparação para a banca.

| # | Atividade | Entregável |
|---|-----------|-----------|
| 6.1 | CI/CD com GitHub Actions (lint, test, deploy) | Pipeline CI/CD |
| 6.2 | Athena + QuickSight para visualização AWS | Dashboards na nuvem |
| 6.3 | Documentação completa (README, architecture, runbook) | Docs finalizados |
| 6.4 | Data dictionary e glossário de negócio | Dicionário de dados |
| 6.5 | Load test e ajustes de performance | Resultados de stress test |
| 6.6 | Preparação da apresentação para a banca | Slides e demo |

---

## 8. Cronograma Visual — 12 Semanas

```
Semana:  1    2    3    4    5    6    7    8    9   10   11   12
         ├────┼────┼────┼────┼────┼────┼────┼────┼────┼────┼────┤

Fase 1   ████████                                              Fundação Local
         Infra Docker, Kafka, MinIO, Airflow, Ingestão

Fase 2             ████████                                    Transformações
                   PySpark Batch + Streaming, Medallion

Fase 3                       ████████                          Governança
                             GX, OpenMetadata, Delta Lake

Fase 4                                 ████████                Disponibilização
                                       Postgres, API, Dashboards

Fase 5                                           ████████      Cloud AWS
                                                 Terraform, MSK, EMR, MWAA

Fase 6                                                     ████████  Refinamento
                                                           CI/CD, Docs, Apresentação
```

---

## 9. Critérios de Avaliação pela Banca

Este case foi desenhado para atender os seguintes critérios de completude e significância:

**Ingestão:** Demonstra capacidade de ingestão batch (APIs, CSVs) e streaming (Kafka) com orquestração via Airflow, cobrindo os dois paradigmas essenciais em engenharia de dados.

**Transformação:** PySpark em modo batch e Spark Structured Streaming, aplicando Medallion Architecture com transformações progressivas (Bronze → Silver → Gold), modelagem dimensional (Star Schema) e detecção de anomalias.

**Disponibilização:** Dados expostos via múltiplos canais — SQL direto (PostgreSQL/Athena), API REST (FastAPI) e dashboards — atendendo diferentes perfis de consumidores de dados.

**Governança:** Great Expectations para qualidade, OpenMetadata para catálogo e linhagem, Delta Lake para versionamento. Cobre os pilares de data quality, data catalog, data lineage e auditoria.

**Visualização:** Dashboards interativos cobrindo operações de mercado, monitoramento de transações, saúde dos pipelines e scorecard de qualidade de dados.

**Tecnologias exigidas:** Python, PySpark, Kafka (streaming), Airflow (batch), Docker, Terraform e AWS (S3, EMR, MSK, MWAA, Glue, Athena, QuickSight) — todos presentes e aplicados em contexto real.

---

## 10. Diferenciais para a Banca

1. **Arquitetura híbrida batch + streaming** em um único projeto, demonstrando versatilidade.
2. **Duas versões (local + cloud)** mostrando capacidade de evolução e migração.
3. **Governança como cidadão de primeira classe**, não um add-on — integrada aos pipelines com quality gates.
4. **Infraestrutura como código** com Terraform modularizado, reprodutível e versionado.
5. **Detecção de anomalias em tempo real**, trazendo valor de negócio concreto para o contexto financeiro.
6. **Testes automatizados** (unitários + integração) e CI/CD completo.
7. **Documentação rica**: data dictionary, runbook operacional, diagramas de arquitetura.

---

## 11. Datasets Públicos Sugeridos

- **Yahoo Finance API (yfinance):** Dados OHLCV de ações e índices — gratuito e sem autenticação.
- **Finnhub.io:** WebSocket de trades em tempo real — free tier com API key.
- **Alpha Vantage:** Dados de mercado via REST API — free tier.
- **Olist Brazilian E-Commerce (Kaggle):** Pode ser adaptado para simular transações bancárias.
- **Synthetic Financial Datasets (Kaggle):** Datasets de transações sintéticas para detecção de fraude (ex: PaySim).

---

## 12. Referências e Recursos

- Repositório Streamify (Kafka + Spark + Terraform): github.com/ankurchavda/streamify
- End-to-End Data Pipeline: github.com/hoangsonww/End-to-End-Data-Pipeline
- Finnhub Streaming Pipeline: github.com/RSKriegs/finnhub-streaming-data-pipeline
- Financial Market Data Analysis: github.com/radoslawkrolikowski/financial-market-data-analysis
- Real-time Market Data Pipeline: github.com/ElfatihZiad/realtime-market-data-pipeline
- Great Expectations: greatexpectations.io
- OpenMetadata: open-metadata.org
- Medallion Architecture (Databricks): databricks.com/glossary/medallion-architecture
- Open-Source DE Projects (Simon Späti): ssp.sh/brain/open-source-data-engineering-projects