# Catálogo de Prompts — Data Master: Financial Fraud Detection Platform

**Repositório:** `https://github.com/gpgomes/data-master-std-fraud`
**Objetivo:** Executar cada passo do case de engenharia de dados via prompts para o Claude.

> **Como usar:** Copie cada prompt na ordem sequencial e envie ao Claude (preferencialmente no Claude com acesso a ferramentas/computador). Cada prompt é autossuficiente e referencia o estado esperado do passo anterior.

---

## FASE 1 — FUNDAÇÃO LOCAL (Semanas 1–2)

---

### PROMPT 1.1 — Estrutura do Repositório e Configuração Base

```
Estou construindo um case de engenharia de dados chamado "Data Master - Financial Fraud Detection Platform" no repositório https://github.com/gpgomes/data-master-std-fraud.

Crie a estrutura completa do repositório com todos os diretórios e arquivos de configuração base. O projeto é uma plataforma de dados financeiros com arquitetura Medallion (Bronze → Silver → Gold) para detecção de fraudes em transações.

Crie os seguintes arquivos:

1. **README.md** — Documentação completa do projeto com:
   - Visão geral e objetivo (plataforma de dados financeiros para detecção de fraude)
   - Diagrama de arquitetura em ASCII/Mermaid
   - Stack tecnológico (Python, PySpark, Kafka, Airflow, Docker, Terraform, AWS)
   - Instruções de setup local
   - Estrutura de diretórios explicada

2. **pyproject.toml** — Configuração Python com:
   - Python 3.11+
   - Dependências: pyspark, kafka-python, great-expectations, fastapi, uvicorn, yfinance, pandas, pyarrow, delta-spark, requests, faker, pytest, black, ruff, mypy
   - Scripts de lint e test

3. **Makefile** — Com targets:
   - `make up` (docker-compose up)
   - `make down` (docker-compose down)
   - `make test` (pytest)
   - `make lint` (ruff + black)
   - `make spark-submit-batch` (submeter job batch)
   - `make spark-submit-stream` (submeter job streaming)
   - `make seed-data` (gerar dados de exemplo)
   - `make clean` (limpar volumes e dados temporários)

4. **.env.example** — Variáveis de ambiente para todos os serviços

5. **.gitignore** — Para Python, Docker, Spark, Terraform, IDE

6. **Estrutura de diretórios** (criar com arquivos __init__.py onde necessário):
```
data-master-std-fraud/
├── src/
│   ├── ingestion/
│   │   ├── batch/
│   │   └── streaming/
│   ├── transformation/
│   │   ├── batch/
│   │   └── streaming/
│   ├── serving/
│   │   ├── api/
│   │   └── loaders/
│   ├── governance/
│   │   ├── great_expectations/
│   │   └── data_catalog/
│   └── common/
│       ├── config.py
│       ├── logger.py
│       └── schemas.py
├── dags/
├── dashboards/
├── docker/
├── terraform/
├── tests/
│   ├── unit/
│   └── integration/
├── scripts/
└── docs/
```

Gere todos os arquivos com conteúdo funcional, não apenas placeholders.
```

---

### PROMPT 1.2 — Docker Compose com Infraestrutura Local

```
No repositório data-master-std-fraud, crie o arquivo docker-compose.yml completo para desenvolvimento local.

O Docker Compose deve orquestrar os seguintes serviços:

1. **Apache Kafka + Zookeeper (Confluent)**
   - Zookeeper na porta 2181
   - Kafka broker na porta 9092 (interno) e 29092 (externo)
   - Kafka UI (provectus/kafka-ui) na porta 8080
   - Criar automaticamente os tópicos: `raw-transactions`, `raw-market-data`, `enriched-transactions`, `fraud-alerts`

2. **MinIO (S3-compatible)**
   - Porta 9000 (API) e 9001 (Console)
   - Criar automaticamente os buckets: `bronze`, `silver`, `gold`, `checkpoints`
   - Credenciais: minioadmin/minioadmin

3. **Apache Spark**
   - Spark Master na porta 7077, Web UI na 8081
   - 2 Spark Workers com 2 cores e 2GB RAM cada
   - Imagem customizada com Delta Lake e Kafka connector

4. **Apache Airflow**
   - Webserver na porta 8082
   - Scheduler
   - PostgreSQL como metadata database
   - Montar pasta `dags/` como volume
   - Admin user: admin/admin

5. **PostgreSQL** (serving layer)
   - Porta 5432
   - Database: fraud_analytics
   - User: datamaster / Password: datamaster123

6. **Apache Superset**
   - Porta 8088
   - Conectado ao PostgreSQL

7. **OpenMetadata** (se viável em Docker Compose, senão deixar como opcional)
   - Porta 8585

Crie também:
- `docker/spark/Dockerfile` — Imagem Spark customizada com PySpark, Delta Lake, Kafka connector, Great Expectations, e as dependências Python do projeto
- `docker/airflow/Dockerfile` — Imagem Airflow com providers do Spark e S3
- `scripts/setup_local.sh` — Script que inicializa buckets no MinIO e tópicos no Kafka após o docker-compose subir
- `scripts/wait-for-it.sh` — Script para aguardar serviços ficarem prontos

Garanta que todos os serviços estejam na mesma rede Docker `data-master-network` e usem healthchecks.
```

---

### PROMPT 1.3 — Gerador de Dados Sintéticos de Transações Financeiras

```
No repositório data-master-std-fraud, crie o módulo de geração de dados sintéticos em `scripts/generate_sample_data.py` e `src/common/data_generator.py`.

O gerador deve criar datasets realistas de transações financeiras para simular fraudes. Use a biblioteca Faker e lógica customizada.

**Dados a gerar:**

1. **Transações financeiras (CSV/JSON)** — Para ingestão batch:
   - transaction_id (UUID)
   - customer_id (UUID)
   - timestamp (datetime com timezone)
   - amount (float, distribuição log-normal: maioria pequena, alguns valores altos)
   - currency (BRL, USD, EUR — 80% BRL)
   - transaction_type (PIX, TED, DOC, CARTAO_CREDITO, CARTAO_DEBITO, BOLETO)
   - merchant_category (ALIMENTACAO, TRANSPORTE, SAUDE, EDUCACAO, LAZER, TRANSFERENCIA, SAQUE, SERVICOS)
   - origin_account, destination_account
   - origin_bank, destination_bank
   - channel (APP_MOBILE, INTERNET_BANKING, AGENCIA, ATM, API)
   - device_id, ip_address, geolocation (lat, lon)
   - is_fraud (boolean — 2-3% das transações são fraude)
   - fraud_type (quando fraude: ACCOUNT_TAKEOVER, CARD_CLONING, IDENTITY_THEFT, MONEY_LAUNDERING, SOCIAL_ENGINEERING, null quando não é fraude)

2. **Padrões de fraude realistas:**
   - Transações de madrugada (00h-05h) em valores altos
   - Múltiplas transações em curto intervalo de tempo (velocity check)
   - Geolocalização impossível (duas transações em cidades distantes em minutos)
   - Valores redondos e altos em merchant_category incomum
   - Transações para contas novas/desconhecidas

3. **Dados de clientes (CSV)** — Tabela dimensional:
   - customer_id, name, cpf (mascarado), birth_date, gender
   - account_opening_date, risk_score, segment (VAREJO, ALTA_RENDA, PRIVATE)
   - city, state, country

4. **Dados de mercado (CSV/JSON)** — Para ingestão batch:
   - Cotações simuladas de ativos (PETR4, VALE3, ITUB4, BBDC4, ABEV3, etc.)
   - Campos: symbol, date, open, high, low, close, volume, adjusted_close

**Requisitos:**
- Gerar 500.000 transações para 10.000 clientes cobrindo 6 meses
- Gerar 1.000 registros de cotações (10 ativos × 100 dias)
- Salvar em `data/sample/` com particionamento por data (YYYY/MM/DD)
- Incluir seed para reprodutibilidade
- Incluir CLI com argparse para parametrizar quantidade de registros
- Print de estatísticas ao final (total de registros, % fraude, distribuição por tipo)
```

---

### PROMPT 1.4 — Kafka Producers (Ingestão Streaming)

```
No repositório data-master-std-fraud, crie os Kafka producers para ingestão de dados em streaming.

**Arquivo 1: `src/ingestion/streaming/kafka_producer_transactions.py`**
- Producer Python que simula transações financeiras em tempo real
- Usa o data_generator do prompt anterior para gerar transações uma a uma
- Publica no tópico Kafka `raw-transactions`
- Formato: JSON serializado com schema Avro embutido
- Rate configurável via variável de ambiente (default: 10 transações/segundo)
- Adiciona header com timestamp de produção e source_system
- Log estruturado com loguru
- Graceful shutdown com signal handler
- Métricas: contador de mensagens enviadas, erros, latência média

**Arquivo 2: `src/ingestion/streaming/kafka_producer_market.py`**
- Producer que simula dados de mercado em tempo real
- Gera tick-by-tick de cotações (preço, volume, bid, ask, spread)
- Publica no tópico Kafka `raw-market-data`
- Simula horário de pregão (10h-17h) com volume maior na abertura e fechamento
- Rate: 50 eventos/segundo

**Arquivo 3: `src/ingestion/streaming/schemas/`**
- `transaction_event.avsc` — Schema Avro para transações
- `market_event.avsc` — Schema Avro para dados de mercado

**Arquivo 4: `src/ingestion/streaming/producer_config.py`**
- Configuração centralizada do Kafka producer
- Parâmetros: bootstrap_servers, acks, retries, batch_size, linger_ms, compression
- Configuração otimizada para throughput vs latência

**Arquivo 5: `docker/producer/Dockerfile`**
- Container para rodar os producers
- Baseado em python:3.11-slim
- Instala dependências necessárias
- Entrypoint configurável via variável de ambiente para escolher qual producer rodar

Adicione também ao docker-compose.yml um serviço `producer-transactions` e `producer-market` que usam esse Dockerfile.

Crie testes unitários em `tests/unit/test_producers.py` cobrindo:
- Serialização correta das mensagens
- Geração de dados dentro dos ranges esperados
- Configuração do producer
```

---

### PROMPT 1.5 — Ingestão Batch (Python + Airflow)

```
No repositório data-master-std-fraud, crie o módulo de ingestão batch e as DAGs do Airflow.

**Arquivo 1: `src/ingestion/batch/market_data_collector.py`**
- Classe MarketDataCollector que coleta dados via yfinance
- Método collect_daily(): busca OHLCV dos últimos N dias para uma lista configurável de ativos brasileiros (PETR4.SA, VALE3.SA, ITUB4.SA, BBDC4.SA, ABEV3.SA, WEGE3.SA, RENT3.SA, BBAS3.SA, MGLU3.SA, LREN3.SA)
- Salva em formato Parquet particionado por data no MinIO bucket `bronze/market_data/`
- Usa boto3 com endpoint do MinIO
- Tratamento de erros com retry (tenacity)
- Log estruturado

**Arquivo 2: `src/ingestion/batch/transaction_loader.py`**
- Classe TransactionLoader que carrega CSVs de transações do diretório `data/sample/`
- Método load_to_bronze(): lê CSVs, adiciona metadados (ingestion_timestamp, source_file, batch_id) e salva como Parquet no MinIO bucket `bronze/transactions/`
- Particionamento por ano/mês/dia
- Idempotência: verifica se o arquivo já foi processado antes de reingerir

**Arquivo 3: `src/ingestion/batch/customer_loader.py`**
- Carrega dados dimensionais de clientes para `bronze/customers/`
- Aplica SCD Type 2 simplificado (valid_from, valid_to, is_current)

**Arquivo 4: `src/common/storage.py`**
- Classe MinIOClient wrapper para operações com MinIO/S3
- Métodos: upload_parquet, download_parquet, list_objects, check_exists
- Configurável para MinIO local ou S3 na AWS (via variável de ambiente)

**Arquivo 5: `dags/dag_batch_ingestion.py`**
- DAG do Airflow: `batch_ingestion_pipeline`
- Schedule: diário às 06:00 UTC
- Tasks:
  1. `check_source_availability` — Sensor que verifica se há novos dados
  2. `ingest_market_data` — PythonOperator chamando MarketDataCollector
  3. `ingest_transactions` — PythonOperator chamando TransactionLoader
  4. `ingest_customers` — PythonOperator chamando CustomerLoader
  5. `validate_bronze_data` — Task que roda Great Expectations checkpoint (placeholder por enquanto)
  6. `notify_completion` — Log de conclusão
- Dependências: check → [market, transactions, customers] → validate → notify
- Default args com retries=2, retry_delay=5min
- Tags: ['ingestion', 'batch', 'bronze']

**Arquivo 6: `dags/dag_seed_data.py`**
- DAG manual (sem schedule) para popular dados iniciais
- Chama generate_sample_data.py para criar os datasets de exemplo
- Útil para primeiro setup

Crie testes em `tests/unit/test_batch_ingestion.py` e `tests/integration/test_minio_upload.py`.
```

---

## FASE 2 — TRANSFORMAÇÕES PYSPARK (Semanas 3–4)

---

### PROMPT 2.1 — Transformação Bronze → Silver (PySpark Batch)

```
No repositório data-master-std-fraud, crie o job PySpark de transformação da camada Bronze para Silver.

**Arquivo: `src/transformation/batch/bronze_to_silver.py`**

Crie uma classe BronzeToSilverTransformer com os seguintes métodos e transformações:

**1. transform_transactions():**
- Ler Parquet de `s3a://bronze/transactions/`
- Limpeza:
  - Remover registros com transaction_id nulo ou duplicado
  - Deduplicação por (transaction_id, timestamp) mantendo o mais recente
  - Cast de tipos: amount para DecimalType(18,2), timestamp para TimestampType
  - Normalização de currency: uppercase, validação contra lista permitida
  - Tratamento de nulos: preencher channel com "UNKNOWN", merchant_category com "OUTROS"
- Enriquecimento:
  - Adicionar coluna `transaction_date` (date extraído do timestamp)
  - Adicionar coluna `transaction_hour` (hora do dia)
  - Adicionar coluna `is_business_hours` (boolean: 08h-18h)
  - Adicionar coluna `amount_brl` (converter USD/EUR para BRL usando taxa fixa configurável)
  - Adicionar coluna `processing_timestamp` (quando foi processado)
- Salvar em Parquet no MinIO: `s3a://silver/transactions/` particionado por transaction_date
- Usar Delta Lake se possível (delta format) para suportar MERGE/UPSERT

**2. transform_market_data():**
- Ler Parquet de `s3a://bronze/market_data/`
- Limpeza: remover registros com OHLCV inválidos (negativos, volume zero)
- Cálculos:
  - `daily_return` = (close - open) / open
  - `intraday_range` = (high - low) / low
  - `vwap` (Volume Weighted Average Price) se tick data disponível
  - Média móvel simples de 5, 10, 20 dias
- Salvar em `s3a://silver/market_data/`

**3. transform_customers():**
- Ler de `s3a://bronze/customers/`
- Mascaramento de CPF (manter apenas últimos 4 dígitos: ***.***.XXX-XX)
- Cálculo de idade a partir de birth_date
- Categorização por faixa etária
- Salvar em `s3a://silver/customers/`

**Arquivo: `src/common/spark_session.py`**
- Factory para criar SparkSession configurada com:
  - Delta Lake support
  - S3/MinIO connectivity (hadoop-aws)
  - Configurações de memória e paralelismo para dev local
  - Configuração para Kafka connector

**Arquivo: `src/common/schemas.py`**
- StructType schemas para cada entidade (transactions, market_data, customers)
- Schemas Bronze e Silver separados

**Requisitos:**
- Job deve ser idempotente (rodar múltiplas vezes sem duplicar dados)
- Logging com contadores: registros lidos, descartados, escritos
- Métricas de qualidade inline: % nulos por coluna, % dedup
- CLI com argparse para parametrizar datas (--start-date, --end-date)
- Testes unitários em `tests/unit/test_bronze_to_silver.py` com dados mock usando PySpark local
```

---

### PROMPT 2.2 — Transformação Silver → Gold (PySpark Batch — Star Schema)

```
No repositório data-master-std-fraud, crie o job PySpark de transformação da camada Silver para Gold com modelagem Star Schema.

**Arquivo: `src/transformation/batch/silver_to_gold.py`**

Crie uma classe SilverToGoldTransformer que constrói o modelo dimensional:

**TABELAS DIMENSIONAIS (Type 2 SCD simplificado):**

1. `dim_customers` — Dimensão de clientes:
   - customer_key (surrogate key, bigint autoincrement via monotonically_increasing_id)
   - customer_id (natural key)
   - name, age_group, segment, city, state
   - risk_score_category (LOW: 0-30, MEDIUM: 31-60, HIGH: 61-80, CRITICAL: 81-100)
   - valid_from, valid_to, is_current

2. `dim_date` — Dimensão de tempo:
   - date_key (YYYYMMDD integer)
   - full_date, year, quarter, month, month_name, week_of_year
   - day_of_month, day_of_week, day_name
   - is_weekend, is_business_day, is_holiday_br (lista hardcoded de feriados nacionais)

3. `dim_transaction_type` — Dimensão tipo de transação:
   - type_key, transaction_type, category (DIGITAL, PRESENCIAL, TRANSFERENCIA)
   - is_instant (PIX=true, demais=false)

4. `dim_merchant` — Dimensão de estabelecimento:
   - merchant_key, merchant_category, merchant_group (agrupamento de alto nível)

**TABELAS FATO:**

5. `fact_transactions` — Fato de transações (granularidade: 1 transação):
   - transaction_id
   - customer_key (FK), date_key (FK), type_key (FK), merchant_key (FK)
   - amount_brl, currency, channel
   - is_fraud, fraud_type
   - is_business_hours, transaction_hour

6. `fact_daily_summary` — Fato agregada diária:
   - date_key, customer_key
   - total_transactions, total_amount, avg_amount
   - max_amount, min_amount, distinct_merchants
   - fraud_count, fraud_amount
   - first_transaction_hour, last_transaction_hour

7. `fact_fraud_analysis` — Fato analítica de fraudes:
   - date_key
   - total_transactions, total_fraud, fraud_rate
   - fraud_by_type (struct/map), fraud_by_channel
   - avg_fraud_amount, max_fraud_amount
   - fraud_by_hour_distribution (array de 24 posições)

**Arquivo: `dags/dag_transformation.py`**
- DAG: `transformation_pipeline`
- Schedule: diário às 08:00 UTC (após ingestão)
- Tasks:
  1. `bronze_to_silver_transactions`
  2. `bronze_to_silver_market`
  3. `bronze_to_silver_customers`
  4. `silver_to_gold_dimensions` (gera dims)
  5. `silver_to_gold_facts` (gera fatos — depende das dims)
  6. `validate_gold_data` (placeholder GX)
  7. `load_to_postgres` (placeholder)
- Usar SparkSubmitOperator ou BashOperator com spark-submit

Salvar tudo em `s3a://gold/` com subpastas por tabela.
Crie testes em `tests/unit/test_silver_to_gold.py`.
```

---

### PROMPT 2.3 — Spark Structured Streaming (Processamento em Tempo Real)

```
No repositório data-master-std-fraud, crie o módulo de processamento streaming com Spark Structured Streaming.

**Arquivo: `src/transformation/streaming/stream_processor.py`**

Crie uma classe StreamProcessor com os seguintes pipelines:

**1. Pipeline de Enriquecimento de Transações:**
- Ler do tópico Kafka `raw-transactions` com Spark Structured Streaming
- Deserializar JSON
- Aplicar as mesmas transformações de limpeza do Bronze → Silver (reutilizar lógica)
- Fazer join com dimensão de clientes (carregada como DataFrame estático, broadcast join)
- Enriquecer com:
  - `customer_segment`
  - `customer_risk_score`
  - `customer_city`
- Escrever no tópico Kafka `enriched-transactions` E no MinIO `s3a://silver/transactions_stream/`
- Checkpoint em `s3a://checkpoints/stream_enrichment/`
- Trigger: processingTime = "10 seconds"
- Watermark: 1 hora no campo timestamp

**2. Pipeline de Detecção de Anomalias:**
- Ler do tópico `enriched-transactions`
- Aplicar detecção de anomalias em micro-batches:

**Arquivo: `src/transformation/streaming/anomaly_detector.py`**

Classe AnomalyDetector com regras:

a) **Velocity Check:**
   - Janela deslizante de 10 minutos por customer_id
   - Flag se > 5 transações na janela
   
b) **Amount Anomaly:**
   - Calcular média e desvio padrão por customer_id (janela de 24h)
   - Flag se amount > média + 3σ
   
c) **Geographic Anomaly:**
   - Calcular distância entre transações consecutivas do mesmo cliente
   - Flag se distância > 500km em intervalo < 1h (velocidade impossível)
   
d) **Time Anomaly:**
   - Flag transações entre 00h-05h com amount > R$ 5.000
   
e) **Pattern Anomaly:**
   - Flag se mesma account recebe de > 10 origens diferentes em 1h

- Cada regra gera um `anomaly_score` (0 a 1)
- Score final = média ponderada dos scores
- Se score > 0.7, publicar no tópico `fraud-alerts`
- Todos os resultados (fraude ou não) vão para `s3a://silver/anomaly_scores/`

**Arquivo: `src/transformation/streaming/fraud_alert_publisher.py`**
- Consome `fraud-alerts` e:
  - Loga o alerta
  - (Futuro: envia notificação, aciona workflow)

Crie testes unitários em `tests/unit/test_anomaly_detector.py` com casos para cada regra.
Crie teste de integração em `tests/integration/test_kafka_spark.py` que:
- Publica N mensagens no Kafka
- Verifica que o streaming processou corretamente
- Verifica que anomalias foram detectadas
```

---

### PROMPT 2.4 — DAGs do Airflow Completas (Orquestração)

```
No repositório data-master-std-fraud, refine e complete todas as DAGs do Airflow para orquestrar o pipeline end-to-end.

**Arquivo: `dags/dag_batch_ingestion.py`** (refinar o existente)
- Adicionar SLA de 2 horas
- Adicionar callback on_failure que loga detalhes do erro
- Adicionar ExternalTaskSensor que aguarda conclusão de dependências externas quando aplicável

**Arquivo: `dags/dag_transformation.py`** (refinar o existente)
- Usar SparkSubmitOperator configurado para submeter ao Spark Master local
  - spark_binary: /opt/spark/bin/spark-submit
  - connection_id: spark_default
  - Jars: delta-core, kafka-connector, hadoop-aws
- Separar em dois sub-fluxos:
  - Batch: bronze→silver→gold (sequencial)
  - Streaming: iniciar/parar stream processors

**Arquivo: `dags/dag_quality_checks.py`** (novo)
- DAG: `data_quality_pipeline`
- Schedule: diário às 10:00 UTC (após transformações)
- Tasks:
  1. `gx_check_bronze` — Rodar Great Expectations na camada Bronze
  2. `gx_check_silver` — Rodar Great Expectations na camada Silver
  3. `gx_check_gold` — Rodar Great Expectations na camada Gold
  4. `generate_data_docs` — Gerar relatórios HTML de qualidade
  5. `alert_on_failure` — Enviar alerta se algum check falhar
- Cada task usa PythonOperator chamando a classe de governança

**Arquivo: `dags/dag_gold_loader.py`** (novo)
- DAG: `gold_to_serving`
- Schedule: diário às 11:00 UTC
- Tasks:
  1. `load_dim_customers` — Carregar dimensão no PostgreSQL
  2. `load_dim_date` — Carregar dimensão de data
  3. `load_fact_transactions` — Carregar fatos (incremental, apenas novos)
  4. `load_fact_daily_summary` — Carregar resumo diário
  5. `load_fact_fraud_analysis` — Carregar análise de fraude
  6. `refresh_materialized_views` — Atualizar views materializadas no Postgres
  7. `notify_consumers` — Log de conclusão

**Arquivo: `dags/common/spark_config.py`**
- Configurações compartilhadas para SparkSubmitOperator
- Connection strings, jars, configurações de memória

**Arquivo: `dags/common/alerts.py`**
- Funções de callback para alertas (on_failure, on_success, on_retry)
- Integração futura com Slack/email

Garanta que todas as DAGs:
- Tenham docstrings descrevendo o propósito
- Usem tags para filtro na UI
- Tenham timeout configurado
- Sejam idempotentes (podem rodar múltiplas vezes)
- Tenham testes em `tests/unit/test_dags.py` verificando importação e estrutura
```

---

## FASE 3 — GOVERNANÇA DE DADOS (Semanas 5–6)

---

### PROMPT 3.1 — Great Expectations (Data Quality Framework)

```
No repositório data-master-std-fraud, implemente o framework de qualidade de dados com Great Expectations.

**Arquivo: `src/governance/great_expectations/great_expectations.yml`**
- Configuração do GX com:
  - Datasource para MinIO/S3 (Parquet via PySpark)
  - Datasource para PostgreSQL
  - Data Docs site estático em `s3a://gold/data_docs/`

**Suites de Expectations:**

**1. `src/governance/great_expectations/expectations/bronze_transactions.json`**
- expect_column_values_to_not_be_null: transaction_id, timestamp, amount
- expect_column_values_to_be_unique: transaction_id
- expect_column_values_to_be_between: amount (0.01, 10_000_000)
- expect_column_values_to_be_in_set: currency (BRL, USD, EUR)
- expect_column_values_to_be_in_set: transaction_type (PIX, TED, DOC, etc.)
- expect_table_row_count_to_be_between: (1, 10_000_000) por partição diária
- expect_column_values_to_match_regex: transaction_id (UUID format)
- expect_column_values_to_be_dateutil_parseable: timestamp

**2. `src/governance/great_expectations/expectations/silver_transactions.json`**
- Todas as expectations do Bronze +
- expect_column_values_to_not_be_null: amount_brl, transaction_date, is_business_hours
- expect_column_pair_values_A_to_be_greater_than_B: amount_brl >= 0.01
- expect_column_values_to_be_in_set: transaction_hour (0..23)
- expect_compound_columns_to_be_unique: [transaction_id, timestamp]
- expect_column_mean_to_be_between: amount_brl (100, 10000) — sanity check
- expect_column_proportion_of_unique_values_to_be_between: customer_id (0.001, 1)

**3. `src/governance/great_expectations/expectations/gold_fact_transactions.json`**
- expect_column_values_to_not_be_null: todas as FKs (customer_key, date_key, etc.)
- expect_column_values_to_be_in_set para FKs (validar integridade referencial)
- expect_column_values_to_be_between: fraud_rate (0, 0.10) — flag se > 10% fraude
- expect_table_row_count_to_be_between por dia
- expect_column_stdev_to_be_between: amount (sanity check)

**4. `src/governance/great_expectations/expectations/gold_daily_summary.json`**
- expect_column_values_to_not_be_null: date_key, customer_key, total_transactions
- expect_column_values_to_be_between: total_transactions (1, 10000) por cliente/dia
- expect_column_values_A_to_be_greater_than_B: max_amount >= avg_amount

**Arquivo: `src/governance/great_expectations/checkpoints/`**
- `checkpoint_bronze.yml` — Roda suite bronze
- `checkpoint_silver.yml` — Roda suite silver
- `checkpoint_gold.yml` — Roda suite gold

**Arquivo: `src/governance/quality_runner.py`**
- Classe DataQualityRunner que:
  - Carrega o contexto GX
  - Executa checkpoints por camada
  - Retorna resultados estruturados (pass/fail/warnings)
  - Gera Data Docs
  - Expõe métricas para monitoramento

Crie testes em `tests/unit/test_quality_runner.py`.
```

---

### PROMPT 3.2 — Catálogo de Dados e Linhagem (OpenMetadata)

```
No repositório data-master-std-fraud, configure o OpenMetadata como catálogo de dados e sistema de linhagem.

**Arquivo: `src/governance/data_catalog/openmetadata_config.yml`**
- Configuração dos conectores para:
  - MinIO/S3 (Data Lake)
  - PostgreSQL (Serving Layer)
  - Kafka (Streaming)
  - Airflow (Pipeline metadata)

**Arquivo: `src/governance/data_catalog/catalog_setup.py`**
- Script Python usando a SDK do OpenMetadata para:
  1. Criar o Database Service para PostgreSQL
  2. Criar o Storage Service para MinIO
  3. Criar o Messaging Service para Kafka
  4. Criar o Pipeline Service para Airflow
  5. Registrar todas as tabelas/datasets:
     - bronze/transactions, bronze/market_data, bronze/customers
     - silver/transactions, silver/market_data, silver/customers
     - gold/dim_customers, gold/dim_date, gold/fact_transactions, etc.
  6. Para cada tabela, registrar:
     - Schema (colunas, tipos, descrições)
     - Tags de classificação: PII, FINANCIAL, CONFIDENTIAL, PUBLIC
     - Owner (team: data-engineering)

**Arquivo: `src/governance/data_catalog/lineage_setup.py`**
- Script para criar a linhagem de dados:
  - bronze/transactions → (PySpark Bronze→Silver) → silver/transactions
  - silver/transactions → (PySpark Silver→Gold) → gold/fact_transactions
  - silver/customers → (PySpark Silver→Gold) → gold/dim_customers
  - gold/fact_transactions → (Loader) → PostgreSQL.fact_transactions
  - Kafka.raw-transactions → (Spark Streaming) → silver/transactions_stream
  - Kafka.enriched-transactions → (Anomaly Detector) → Kafka.fraud-alerts

**Arquivo: `src/governance/data_catalog/glossary_setup.py`**
- Criar Business Glossary com termos:
  - VWAP: Volume Weighted Average Price — definição, fórmula, owner
  - Fraud Rate: Taxa de fraude — definição, cálculo, threshold
  - SCD Type 2: Slowly Changing Dimension — explicação
  - Medallion Architecture: Bronze/Silver/Gold — explicação
  - PIX, TED, DOC: definições dos tipos de transação
  - Z-Score: método de detecção de anomalias

**Arquivo: `scripts/seed_openmetadata.py`**
- Script CLI que executa catalog_setup, lineage_setup e glossary_setup em sequência
- Verifica se OpenMetadata está disponível antes de executar
- Idempotente (pode rodar múltiplas vezes)

**Adicionar ao docker-compose.yml:**
- Serviço OpenMetadata (se não adicionado antes) ou instruções para rodar separadamente

Crie documentação em `docs/data_catalog.md` explicando:
- Como acessar o catálogo (URL, credenciais)
- Como navegar pela linhagem
- Como adicionar novas tags e termos ao glossário
```

---

### PROMPT 3.3 — Versionamento de Dados e Auditoria

```
No repositório data-master-std-fraud, implemente versionamento de dados com Delta Lake e auditoria.

**Arquivo: `src/governance/delta_manager.py`**
- Classe DeltaTableManager com métodos:
  1. `create_delta_table(path, schema)` — Inicializar Delta Table
  2. `upsert(path, new_data, merge_keys)` — MERGE/UPSERT com Delta Lake
  3. `time_travel(path, version=None, timestamp=None)` — Ler versão específica
  4. `get_history(path, limit=20)` — Retornar histórico de operações
  5. `vacuum(path, retention_hours=168)` — Limpar versões antigas (7 dias)
  6. `optimize(path)` — Compactar arquivos pequenos (Z-Order quando possível)

**Arquivo: `src/governance/audit_logger.py`**
- Classe AuditLogger que registra:
  - Cada execução de pipeline: job_id, start_time, end_time, status, records_processed
  - Cada transformação: input_count, output_count, dropped_count, error_count
  - Cada validação GX: checkpoint_name, result, expectations_met, expectations_failed
  - Cada acesso de dados: who (service/user), what (table), when, action (read/write)
- Salvar logs em:
  - Delta Table: `s3a://gold/audit_logs/` (para análise)
  - PostgreSQL: tabela `audit.pipeline_runs` (para consulta rápida)
  - JSON local: `/tmp/audit/` (fallback)

**Arquivo: `src/governance/data_retention.py`**
- Classe DataRetentionPolicy:
  - Bronze: reter por 90 dias, depois arquivar
  - Silver: reter por 1 ano
  - Gold: reter indefinidamente
  - Método `apply_retention()` que deleta partições antigas conforme política
  - Integração com Airflow DAG para execução periódica

**Arquivo: `dags/dag_governance.py`**
- DAG: `governance_pipeline`
- Schedule: semanal (domingos às 02:00 UTC)
- Tasks:
  1. `delta_vacuum_bronze` — Vacuum nas tabelas Bronze
  2. `delta_vacuum_silver` — Vacuum nas tabelas Silver
  3. `delta_optimize_gold` — Optimize nas tabelas Gold
  4. `apply_retention_policies` — Executar políticas de retenção
  5. `generate_audit_report` — Gerar relatório de auditoria semanal
  6. `sync_data_catalog` — Atualizar metadados no OpenMetadata

Crie testes em `tests/unit/test_delta_manager.py` e `tests/unit/test_audit_logger.py`.
```

---

## FASE 4 — DISPONIBILIZAÇÃO E VISUALIZAÇÃO (Semanas 7–8)

---

### PROMPT 4.1 — Serving Layer (PostgreSQL + Loader)

```
No repositório data-master-std-fraud, crie o módulo de carga para a serving layer no PostgreSQL.

**Arquivo: `src/serving/loaders/postgres_schema.sql`**
- DDL completo para o PostgreSQL database `fraud_analytics`:
  - Schema `dimensional`: dim_customers, dim_date, dim_transaction_type, dim_merchant
  - Schema `facts`: fact_transactions, fact_daily_summary, fact_fraud_analysis
  - Schema `audit`: pipeline_runs, data_quality_results
  - Schema `api`: views materializadas para a API
  - Índices otimizados para queries analíticas
  - Constraints de integridade referencial
  - Particionamento da fact_transactions por mês (PostgreSQL native partitioning)
  - Comentários em todas as tabelas e colunas (COMMENT ON)

**Arquivo: `src/serving/loaders/gold_to_postgres.py`**
- Classe GoldToPostgresLoader:
  1. `load_dimension(table_name, gold_path)` — Truncate & Load para dimensões
  2. `load_fact_incremental(table_name, gold_path, watermark_column)` — Load incremental para fatos (apenas registros novos desde último load)
  3. `refresh_materialized_views()` — Atualizar views materializadas
  4. `get_load_watermark(table_name)` — Retornar timestamp do último load
- Usar PySpark para ler Parquet e JDBC write para PostgreSQL
- Batch size configurável para evitar OOM
- Tratamento de erros com rollback em caso de falha
- Registro no audit log

**Arquivo: `src/serving/loaders/materialized_views.sql`**
- Views materializadas para consumo da API e dashboards:
  1. `mv_fraud_summary_daily` — Resumo de fraudes por dia
  2. `mv_fraud_by_type` — Fraudes agrupadas por tipo
  3. `mv_customer_risk_profile` — Perfil de risco por cliente
  4. `mv_transaction_volume_hourly` — Volume por hora (para dashboard real-time)
  5. `mv_top_fraud_customers` — Top clientes com mais fraudes
  6. `mv_channel_analysis` — Análise por canal

Crie testes em `tests/integration/test_postgres_loader.py`.
```

---

### PROMPT 4.2 — API REST (FastAPI)

```
No repositório data-master-std-fraud, crie a API REST com FastAPI para disponibilizar dados da plataforma.

**Arquivo: `src/serving/api/main.py`**
- FastAPI app com:
  - Título: "Data Master - Fraud Analytics API"
  - Versão: 1.0.0
  - Documentação automática (Swagger + ReDoc)
  - CORS configurado
  - Health check endpoint

**Arquivo: `src/serving/api/routes/fraud.py`**
- Endpoints de análise de fraude:
  - `GET /api/v1/fraud/summary` — Resumo geral (total transações, total fraudes, fraud rate, período)
  - `GET /api/v1/fraud/daily?start_date=&end_date=` — Fraudes por dia no período
  - `GET /api/v1/fraud/by-type` — Distribuição de fraudes por tipo
  - `GET /api/v1/fraud/by-channel` — Distribuição por canal
  - `GET /api/v1/fraud/alerts?limit=50` — Últimos alertas de fraude
  - `GET /api/v1/fraud/customer/{customer_id}` — Histórico de fraudes do cliente

**Arquivo: `src/serving/api/routes/transactions.py`**
- Endpoints de transações:
  - `GET /api/v1/transactions/volume?granularity=hourly|daily` — Volume de transações
  - `GET /api/v1/transactions/stats` — Estatísticas gerais
  - `GET /api/v1/transactions/customer/{customer_id}?limit=100` — Transações do cliente

**Arquivo: `src/serving/api/routes/quality.py`**
- Endpoints de qualidade de dados:
  - `GET /api/v1/quality/status` — Status geral (última execução GX, resultados)
  - `GET /api/v1/quality/history?days=30` — Histórico de validações
  - `GET /api/v1/quality/pipeline-health` — Status dos pipelines Airflow

**Arquivo: `src/serving/api/routes/market.py`**
- Endpoints de mercado:
  - `GET /api/v1/market/quotes?symbols=PETR4,VALE3` — Últimas cotações
  - `GET /api/v1/market/history/{symbol}?days=30` — Histórico de cotações

**Arquivo: `src/serving/api/models/`**
- Pydantic models para request/response de cada endpoint
- Schemas com validação, exemplos e documentação

**Arquivo: `src/serving/api/database.py`**
- Configuração do SQLAlchemy async para PostgreSQL
- Connection pool
- Dependency injection para FastAPI

**Arquivo: `docker/api/Dockerfile`**
- Container para a API
- Baseado em python:3.11-slim
- Expõe porta 8000
- Healthcheck configurado

Adicionar serviço `api` ao docker-compose.yml na porta 8000.

Crie testes em `tests/unit/test_api.py` usando TestClient do FastAPI.
```

---

### PROMPT 4.3 — Dashboards de Visualização

```
No repositório data-master-std-fraud, crie a configuração dos dashboards no Apache Superset.

**Arquivo: `dashboards/superset/superset_config.py`**
- Configuração do Superset:
  - Secret key
  - Database connections (PostgreSQL fraud_analytics)
  - Configurações de cache
  - Feature flags habilitados

**Arquivo: `dashboards/superset/setup_dashboards.py`**
- Script Python usando a API do Superset para criar programaticamente:

**Dashboard 1: "Fraud Command Center"**
- KPI Cards no topo:
  - Total de transações (hoje)
  - Total de fraudes detectadas (hoje)
  - Fraud Rate (%)
  - Valor total em fraudes (R$)
- Gráfico de linha: Fraudes por hora (últimas 24h)
- Gráfico de barras: Fraudes por tipo (ACCOUNT_TAKEOVER, CARD_CLONING, etc.)
- Gráfico de pizza: Fraudes por canal (APP_MOBILE, INTERNET_BANKING, etc.)
- Tabela: Últimas 20 transações fraudulentas com detalhes
- Mapa de calor: Fraudes por hora do dia × dia da semana
- Filtros: período, tipo de fraude, canal, segmento de cliente

**Dashboard 2: "Transaction Monitor"**
- KPI Cards: volume total, valor total, ticket médio, clientes ativos
- Gráfico de área: Volume de transações por hora
- Gráfico de barras empilhadas: Volume por tipo de transação ao longo do tempo
- Treemap: Distribuição por merchant_category
- Tabela com sparklines: Top 10 clientes por volume

**Dashboard 3: "Data Quality Scorecard"**
- KPI Cards: % de checks passando, total de expectations, último run
- Gráfico de linha: Evolução da qualidade ao longo do tempo
- Tabela: Detalhes de cada expectation (status, camada, última verificação)
- Barras horizontais: Completude por coluna nas camadas Bronze, Silver, Gold
- Indicadores semáforo (verde/amarelo/vermelho) por dataset

**Dashboard 4: "Market Analytics"**
- Candlestick chart para cotações dos ativos (se suportado, senão line chart)
- Volume de negociação por ativo
- Retornos diários comparativos
- Correlação entre ativos (heatmap)

**Arquivo: `dashboards/superset/datasets.sql`**
- Queries SQL para cada dataset/chart usado nos dashboards
- Otimizadas para performance

**Arquivo alternativo: `dashboards/grafana/provisioning/`**
- Se preferir Grafana como alternativa:
  - `datasources/postgres.yml` — Datasource do PostgreSQL
  - `dashboards/dashboard.yml` — Provider de dashboards
  - `dashboards/fraud_command_center.json` — Dashboard JSON exportado
  - `dashboards/pipeline_health.json` — Saúde dos pipelines

Crie documentação em `docs/dashboards.md` com screenshots simulados (descrições textuais de cada painel).
```

---

### PROMPT 4.4 — Testes Automatizados (Unitários e Integração)

```
No repositório data-master-std-fraud, complete a suíte de testes automatizados.

**Arquivo: `tests/conftest.py`**
- Fixtures compartilhadas:
  - `spark_session` — SparkSession para testes (local[2], minimal config)
  - `sample_transactions_df` — DataFrame com 1000 transações de teste
  - `sample_customers_df` — DataFrame com 100 clientes
  - `sample_market_df` — DataFrame com dados de mercado
  - `postgres_connection` — Connection string para test database
  - `minio_client` — Cliente MinIO para testes de integração
  - `kafka_producer/consumer` — Para testes de streaming

**Arquivo: `tests/unit/test_transformations.py`**
- Testar BronzeToSilverTransformer:
  - test_deduplication_removes_duplicates
  - test_null_handling_fills_defaults
  - test_currency_normalization
  - test_amount_conversion_to_brl
  - test_business_hours_flag
  - test_invalid_records_are_filtered

- Testar SilverToGoldTransformer:
  - test_dim_date_generation
  - test_dim_customers_scd2
  - test_fact_transactions_joins
  - test_daily_summary_aggregation
  - test_fraud_analysis_metrics

**Arquivo: `tests/unit/test_anomaly_detector.py`**
- test_velocity_check_flags_high_frequency
- test_velocity_check_allows_normal_frequency
- test_amount_anomaly_flags_outlier
- test_amount_anomaly_allows_normal
- test_geographic_anomaly_flags_impossible_travel
- test_time_anomaly_flags_late_night_high_value
- test_combined_score_calculation
- test_threshold_triggers_alert

**Arquivo: `tests/unit/test_api.py`**
- Testar cada endpoint da FastAPI:
  - test_health_check
  - test_fraud_summary_returns_valid_data
  - test_fraud_daily_with_date_range
  - test_fraud_by_type_distribution
  - test_transactions_volume
  - test_quality_status
  - test_invalid_customer_id_returns_404
  - test_invalid_date_range_returns_422

**Arquivo: `tests/unit/test_dags.py`**
- test_all_dags_import_without_errors
- test_dag_batch_ingestion_structure
- test_dag_transformation_task_dependencies
- test_dag_quality_checks_structure
- test_no_import_errors_in_any_dag

**Arquivo: `tests/integration/test_pipeline_e2e.py`**
- Teste end-to-end (requer Docker):
  1. Gerar dados de teste
  2. Carregar no MinIO (Bronze)
  3. Executar Bronze → Silver
  4. Executar Silver → Gold
  5. Carregar no PostgreSQL
  6. Consultar API e validar resultado
  7. Verificar que GX checks passam

**Arquivo: `tests/integration/test_kafka_spark.py`**
- Teste de streaming (requer Docker):
  1. Publicar 100 mensagens no Kafka
  2. Iniciar Spark Streaming
  3. Aguardar processamento (timeout 60s)
  4. Verificar dados no MinIO Silver
  5. Verificar alertas de fraude no tópico correto

**Arquivo: `.github/workflows/ci.yml`**
- GitHub Actions workflow:
  - Trigger: push e PR para main
  - Jobs:
    1. `lint` — ruff + black check
    2. `unit-tests` — pytest tests/unit com PySpark local
    3. `integration-tests` — pytest tests/integration com docker-compose
  - Cache de dependências pip
  - Upload de coverage report
```

---

## FASE 5 — MIGRAÇÃO PARA AWS (Semanas 9–10)

---

### PROMPT 5.1 — Terraform: Networking e Storage (S3)

```
No repositório data-master-std-fraud, crie os módulos Terraform para a infraestrutura base na AWS.

**Estrutura:**
```
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── versions.tf
├── environments/
│   ├── dev/
│   │   ├── main.tf
│   │   ├── terraform.tfvars
│   │   └── backend.tf
│   └── prod/
│       ├── main.tf
│       ├── terraform.tfvars
│       └── backend.tf
└── modules/
    ├── networking/
    ├── s3/
    ├── iam/
    ├── msk/
    ├── emr/
    ├── mwaa/
    └── glue/
```

**Módulo: `terraform/modules/networking/`**
- VPC com CIDR 10.0.0.0/16
- 3 subnets públicas (10.0.1.0/24, 10.0.2.0/24, 10.0.3.0/24)
- 3 subnets privadas (10.0.10.0/24, 10.0.20.0/24, 10.0.30.0/24)
- Internet Gateway
- NAT Gateway (1 para dev, 3 para prod)
- Route tables
- Security Groups:
  - sg_kafka: portas 9092, 9094
  - sg_spark: portas 7077, 8080, 4040
  - sg_airflow: porta 8080
  - sg_postgres: porta 5432
  - sg_api: porta 8000
- Tags padrão: project=data-master, environment=var.environment, managed_by=terraform

**Módulo: `terraform/modules/s3/`**
- Buckets S3:
  - `data-master-{env}-bronze` — Raw data
  - `data-master-{env}-silver` — Cleaned data
  - `data-master-{env}-gold` — Business data
  - `data-master-{env}-checkpoints` — Spark checkpoints
  - `data-master-{env}-artifacts` — JARs, scripts, configs
  - `data-master-{env}-logs` — Logs de pipeline
- Configurações:
  - Versionamento habilitado em silver e gold
  - Lifecycle rules: Bronze → Glacier após 90 dias, delete após 365
  - Server-side encryption (AES-256)
  - Block public access
  - Bucket policy para acesso via roles específicas
- Outputs: bucket ARNs e nomes

**Módulo: `terraform/modules/iam/`**
- IAM Roles:
  - `data-master-emr-role` — Para EMR cluster (acesso a S3, CloudWatch, Glue)
  - `data-master-mwaa-role` — Para Airflow (acesso a S3, EMR, Glue, CloudWatch)
  - `data-master-glue-role` — Para Glue Crawler (acesso a S3, Glue Catalog)
  - `data-master-lambda-role` — Para Lambda functions
- IAM Policies seguindo princípio de menor privilégio
- Instance profiles para EC2/EMR

**Arquivo: `terraform/versions.tf`**
- Required providers: aws ~> 5.0, random ~> 3.0
- Required Terraform version >= 1.5
- Backend S3 para state (com DynamoDB locking)

**Arquivo: `terraform/environments/dev/terraform.tfvars`**
- environment = "dev"
- region = "us-east-1"
- Configurações menores (NAT Gateway single, instâncias menores)

Crie documentação em `docs/infrastructure.md` com instruções de deploy.
```

---

### PROMPT 5.2 — Terraform: MSK (Kafka Gerenciado)

```
No repositório data-master-std-fraud, crie o módulo Terraform para Amazon MSK (Managed Kafka).

**Módulo: `terraform/modules/msk/`**

**Arquivo: `terraform/modules/msk/main.tf`**
- Cluster MSK:
  - Nome: data-master-{env}-kafka
  - Versão Kafka: 3.5.1
  - Número de brokers: 2 (dev), 3 (prod)
  - Tipo de instância: kafka.m5.large (dev), kafka.m5.2xlarge (prod)
  - Storage: 100GB (dev), 500GB (prod), auto-scaling habilitado
  - Subnets: usar as privadas do módulo networking
  - Security group: sg_kafka
  - Configuração:
    - auto.create.topics.enable = true
    - default.replication.factor = 2
    - min.insync.replicas = 1 (dev), 2 (prod)
    - log.retention.hours = 168 (7 dias)
    - num.partitions = 3
  - Encryption:
    - In-transit: TLS
    - At-rest: AWS managed KMS key
  - Monitoring:
    - Enhanced monitoring: PER_TOPIC_PER_BROKER
    - CloudWatch logs habilitados
    - JMX exporter habilitado
  - Authentication: IAM + SASL/SCRAM

**Arquivo: `terraform/modules/msk/variables.tf`**
- kafka_version, instance_type, number_of_brokers
- storage_size, vpc_id, subnet_ids, security_group_ids
- environment, tags

**Arquivo: `terraform/modules/msk/outputs.tf`**
- bootstrap_brokers_tls
- bootstrap_brokers_sasl_iam
- zookeeper_connect_string
- cluster_arn
- cluster_name

**Adicionar ao `terraform/environments/dev/main.tf`:**
- Instanciar módulo MSK com parâmetros de dev

Crie também `scripts/aws/create_kafka_topics.sh` para criar os tópicos necessários no MSK após deploy.
```

---

### PROMPT 5.3 — Terraform: EMR (Spark Cluster)

```
No repositório data-master-std-fraud, crie o módulo Terraform para Amazon EMR.

**Módulo: `terraform/modules/emr/`**

**Arquivo: `terraform/modules/emr/main.tf`**
- EMR Cluster:
  - Nome: data-master-{env}-spark
  - Release label: emr-7.0.0 (com Spark 3.5, Delta Lake support)
  - Aplicações: Spark, Hadoop, Hive, Livy
  - Master node: m5.xlarge (1 instância)
  - Core nodes: m5.xlarge (2 dev, 4 prod), auto-scaling habilitado
  - Task nodes (spot): m5.xlarge, auto-scaling 0-4
  - Log URI: s3://data-master-{env}-logs/emr/
  - Bootstrap actions:
    - Instalar dependências Python (requirements.txt do projeto)
    - Configurar Delta Lake
    - Configurar conectividade com MSK
  - Configurações Spark:
    - spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
    - spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
    - spark.hadoop.fs.s3a.endpoint = s3.amazonaws.com
    - spark.dynamicAllocation.enabled = true
  - Steps (jobs a serem submetidos):
    - Step template para bronze_to_silver.py
    - Step template para silver_to_gold.py
  - Security: usar IAM role do módulo IAM
  - Tags padronizadas

**Arquivo: `terraform/modules/emr/bootstrap.sh`**
- Script de bootstrap para instalar dependências no cluster EMR
- pip install das bibliotecas Python do projeto
- Configuração de variáveis de ambiente

**Arquivo: `terraform/modules/emr/variables.tf`**
- release_label, master_instance_type, core_instance_type
- core_instance_count, spot_bid_price
- subnet_id, security_group_ids, iam_role_arn
- log_uri, bootstrap_script_s3_path
- environment, tags

**Arquivo: `terraform/modules/emr/outputs.tf`**
- cluster_id, master_public_dns, cluster_arn

Crie também `scripts/aws/submit_emr_step.sh` para submeter jobs PySpark ao EMR.
```

---

### PROMPT 5.4 — Terraform: MWAA, Glue e Athena

```
No repositório data-master-std-fraud, crie os módulos Terraform para MWAA (Airflow), Glue e Athena.

**Módulo: `terraform/modules/mwaa/`**
- MWAA Environment:
  - Nome: data-master-{env}-airflow
  - Versão Airflow: 2.8.1
  - Classe: mw1.small (dev), mw1.medium (prod)
  - Max workers: 2 (dev), 10 (prod)
  - DAGs S3 path: s3://data-master-{env}-artifacts/dags/
  - Requirements: s3://data-master-{env}-artifacts/requirements.txt
  - Plugins: s3://data-master-{env}-artifacts/plugins.zip (se necessário)
  - Logging: CloudWatch, nível INFO
  - Networking: subnets privadas, security group dedicado
  - Environment variables:
    - ENVIRONMENT, S3_BRONZE_BUCKET, S3_SILVER_BUCKET, S3_GOLD_BUCKET
    - MSK_BOOTSTRAP_BROKERS, EMR_CLUSTER_ID
    - POSTGRES_HOST, POSTGRES_DB
  - IAM role com acesso a S3, EMR, Glue, MSK, CloudWatch

**Módulo: `terraform/modules/glue/`**
- Glue Data Catalog:
  - Database: data_master_{env}
  - Crawlers:
    - `bronze-crawler` — Crawl s3://bronze/ → catálogo
    - `silver-crawler` — Crawl s3://silver/ → catálogo
    - `gold-crawler` — Crawl s3://gold/ → catálogo
  - Schedule: diário para cada crawler
  - Classifier para Parquet e Delta Lake
  - IAM role com acesso a S3 e Glue

- Athena:
  - Workgroup: data-master-{env}
  - Output location: s3://data-master-{env}-logs/athena/
  - Named queries pré-configuradas:
    - "Fraud Summary" — Query para resumo de fraudes
    - "Daily Volume" — Volume diário de transações
    - "Quality Check" — Métricas de qualidade

**Módulo: `terraform/modules/monitoring/`** (bônus)
- CloudWatch Alarms:
  - EMR cluster health
  - MSK broker CPU > 80%
  - MWAA task failures
  - S3 bucket size alerts
- CloudWatch Dashboard: visão consolidada dos serviços

Atualize `terraform/environments/dev/main.tf` para instanciar todos os módulos com parâmetros de dev.
Crie `terraform/environments/dev/backend.tf` com backend S3 + DynamoDB locking.
```

---

### PROMPT 5.5 — Adaptação dos Pipelines para AWS

```
No repositório data-master-std-fraud, adapte o código dos pipelines para funcionar tanto localmente (Docker) quanto na AWS.

**Arquivo: `src/common/config.py`** (refatorar)
- Classe Settings usando pydantic-settings:
  - Detectar ambiente: LOCAL ou AWS (via variável ENVIRONMENT)
  - Se LOCAL: usar endpoints MinIO, Kafka local, PostgreSQL local
  - Se AWS: usar S3, MSK, RDS/Redshift, Glue Catalog
  - Todas as configurações lidas de variáveis de ambiente com defaults para local
  - Subclasses: KafkaConfig, SparkConfig, StorageConfig, DatabaseConfig

**Arquivo: `src/common/spark_session.py`** (refatorar)
- Factory SparkSessionBuilder:
  - Se LOCAL: Spark local com MinIO connector
  - Se AWS: Spark com S3, Glue Catalog, Delta Lake configs para EMR
  - Configurações de performance ajustadas por ambiente

**Arquivo: `src/common/storage.py`** (refatorar)
- Abstração de storage que funciona com MinIO e S3:
  - Mesma interface, backend diferente
  - Se LOCAL: endpoint MinIO
  - Se AWS: endpoint S3 nativo

**Arquivo: `dags/` (adaptar todas as DAGs)**
- Adaptar para usar SparkSubmitOperator (local) ou EmrAddStepsOperator (AWS)
- Usar Airflow Variables para configurações de ambiente
- Usar Airflow Connections configuradas para cada serviço

**Arquivo: `scripts/aws/deploy_dags.sh`**
- Script para sincronizar DAGs para S3 (para MWAA)
- aws s3 sync dags/ s3://data-master-{env}-artifacts/dags/

**Arquivo: `scripts/aws/deploy_spark_jobs.sh`**
- Script para fazer upload dos jobs PySpark para S3
- aws s3 sync src/ s3://data-master-{env}-artifacts/spark-jobs/

**Arquivo: `.github/workflows/cd_deploy_aws.yml`**
- GitHub Actions para deploy na AWS:
  - Trigger: push na branch main com tag v*
  - Jobs:
    1. `test` — Rodar testes unitários
    2. `build` — Build do Docker image da API, push para ECR
    3. `deploy-infra` — Terraform apply (com approval manual)
    4. `deploy-dags` — Sync DAGs para S3
    5. `deploy-spark-jobs` — Upload jobs para S3
    6. `deploy-api` — Deploy da API (ECS ou Lambda)
  - Secrets: AWS credentials, Terraform state config

Crie documentação em `docs/deployment.md` explicando:
- Como fazer deploy local vs AWS
- Variáveis de ambiente necessárias
- Checklist pré-deploy
- Rollback procedure
```

---

## FASE 6 — REFINAMENTO E APRESENTAÇÃO (Semanas 11–12)

---

### PROMPT 6.1 — CI/CD Completo e Code Quality

```
No repositório data-master-std-fraud, implemente o pipeline de CI/CD completo e ferramentas de qualidade de código.

**Arquivo: `.github/workflows/ci.yml`** (finalizar)
- Workflow: "CI Pipeline"
- Triggers: push (main, develop), pull_request (main)
- Jobs:

1. `code-quality`:
   - ruff check src/ tests/
   - black --check src/ tests/
   - mypy src/ --ignore-missing-imports
   - bandit -r src/ (security scan)

2. `unit-tests`:
   - Setup Python 3.11
   - Install dependencies
   - pytest tests/unit/ -v --cov=src --cov-report=xml
   - Upload coverage para Codecov (ou similar)

3. `integration-tests`:
   - docker-compose up -d (infra necessária)
   - Wait for services
   - pytest tests/integration/ -v --timeout=120
   - docker-compose down

4. `dag-validation`:
   - python -c "import dags" (verificar import de todas as DAGs)
   - Airflow dags validate (se possível)

5. `security-scan`:
   - pip-audit (vulnerabilidades em dependências)
   - trivy para scan de Docker images

**Arquivo: `.github/workflows/cd_deploy_aws.yml`** (finalizar)
- Workflow: "CD Deploy to AWS"
- Trigger: push de tags v*.*.*
- Jobs com environments (dev, prod) e approval gates

**Arquivo: `.pre-commit-config.yaml`**
- Hooks: ruff, black, mypy, trailing-whitespace, end-of-file-fixer, check-yaml, check-json
- Configuração para rodar antes de cada commit

**Arquivo: `ruff.toml`**
- Configuração do Ruff linter
- Line length: 120
- Rules: E, F, W, I (imports), B (bugbear), S (bandit)
- Exclude: .venv, build, dist

**Arquivo: `mypy.ini`**
- Configuração do MyPy
- strict mode para src/
- ignore_missing_imports para bibliotecas sem type stubs

Atualize o README.md com badges de CI/CD, coverage, e instruções de contribuição.
```

---

### PROMPT 6.2 — Documentação Completa do Projeto

```
No repositório data-master-std-fraud, crie toda a documentação necessária para a apresentação à banca.

**Arquivo: `docs/architecture.md`**
- Diagrama de arquitetura completo (Mermaid)
- Descrição de cada camada (Ingestão, Transformação, Governança, Disponibilização, Visualização)
- Fluxo de dados end-to-end
- Decisões de design e trade-offs
- Diagrama de deploy (local vs AWS)
- Requisitos não-funcionais atendidos (escalabilidade, resiliência, observabilidade)

**Arquivo: `docs/data_dictionary.md`**
- Dicionário de dados completo para TODAS as tabelas:
  - Nome da tabela, descrição, camada (Bronze/Silver/Gold)
  - Para cada coluna: nome, tipo, descrição, exemplo, nullable, PII flag
  - Chaves primárias e estrangeiras
  - Regras de negócio aplicadas
  - Volume estimado e taxa de crescimento

**Arquivo: `docs/runbook.md`**
- Guia operacional:
  - Como iniciar o ambiente local (passo a passo)
  - Como parar e reiniciar serviços
  - Troubleshooting comum (Kafka não conecta, Spark OOM, Airflow DAG falha)
  - Como reprocessar dados de um dia específico
  - Como adicionar uma nova fonte de dados
  - Como criar uma nova expectation no GX
  - Como adicionar um novo dashboard
  - Procedures de backup e recovery
  - Contatos e escalation

**Arquivo: `docs/data_governance.md`**
- Estratégia de governança implementada:
  - Modelo de qualidade de dados (framework GX)
  - Catálogo de dados (OpenMetadata)
  - Linhagem de dados (fluxo visual)
  - Políticas de retenção
  - Classificação de dados (PII, Confidencial)
  - Auditoria e compliance
  - Business Glossary

**Arquivo: `docs/presentation/`**
- `slides_outline.md` — Outline da apresentação para a banca:
  1. Contexto e problema de negócio (2 min)
  2. Arquitetura e stack tecnológico (5 min)
  3. Demo: Ingestão batch + streaming (3 min)
  4. Demo: Transformações PySpark + Medallion (3 min)
  5. Demo: Governança — GX + Catálogo + Linhagem (3 min)
  6. Demo: API + Dashboards (3 min)
  7. Infraestrutura AWS + Terraform (3 min)
  8. CI/CD e testes (2 min)
  9. Diferenciais e conclusão (2 min)
  10. Perguntas (10 min)
  Total: ~35 minutos + Q&A

- `demo_script.md` — Roteiro passo a passo da demo:
  - Comandos exatos a executar
  - URLs para abrir
  - Dados esperados em cada tela
  - Pontos de talking para cada etapa

**Arquivo: `docs/api_guide.md`**
- Guia de uso da API com exemplos curl para cada endpoint
- Exemplos de responses
- Códigos de erro

Atualize o `README.md` principal com:
- Badges (CI, coverage, license, Python version)
- Quick start (3 passos para rodar)
- Links para toda a documentação
- Diagrama de arquitetura inline
- Seção de contribuição
- License (MIT)
```

---

### PROMPT 6.3 — Performance, Otimização e Polimento Final

```
No repositório data-master-std-fraud, faça o polimento final do projeto com otimizações de performance e ajustes finais.

**Arquivo: `scripts/load_test.py`**
- Script de load test usando locust ou script customizado:
  - Simular 100 transações/segundo no Kafka producer por 10 minutos
  - Medir latência do streaming (tempo entre produção e consumo)
  - Medir latência da API (requests por segundo, p95, p99)
  - Medir tempo de execução dos jobs batch
  - Gerar relatório com métricas

**Arquivo: `src/common/performance.py`**
- Otimizações PySpark documentadas:
  - Configuração de particionamento ideal (repartition vs coalesce)
  - Broadcast join para dimensões pequenas
  - Caching de DataFrames reutilizados
  - Configuração de shuffle partitions
  - AQE (Adaptive Query Execution) habilitado
  - Predicate pushdown em leituras Parquet
  - Z-Order para Delta Lake nas colunas mais filtradas

**Revisões finais em todos os arquivos:**
1. Verificar que TODO os imports estão corretos
2. Verificar que todas as docstrings estão preenchidas
3. Verificar que todos os logs usam logging estruturado
4. Verificar que não há secrets hardcoded (usar .env ou AWS Secrets Manager)
5. Verificar que todos os testes passam
6. Verificar que docker-compose up sobe sem erros
7. Verificar que make lint passa sem warnings

**Arquivo: `CHANGELOG.md`**
- Registro de todas as versões:
  - v1.0.0 — Versão Local: Ingestão, Transformação, Governança, API, Dashboards
  - v2.0.0 — Versão Cloud: Terraform, AWS services, CI/CD

**Arquivo: `LICENSE`**
- MIT License

**Arquivo: `CONTRIBUTING.md`**
- Guia de contribuição:
  - Como configurar o ambiente de desenvolvimento
  - Padrões de código (ruff, black, type hints)
  - Padrões de commit (conventional commits)
  - Processo de pull request
  - Como rodar testes localmente

Faça uma revisão final do README.md garantindo que um desenvolvedor consegue clonar o repositório e ter o ambiente funcionando em menos de 15 minutos com apenas:
```bash
git clone https://github.com/gpgomes/data-master-std-fraud.git
cd data-master-std-fraud
cp .env.example .env
make up
make seed-data
# Abrir http://localhost:8082 (Airflow)
# Abrir http://localhost:8088 (Superset)
# Abrir http://localhost:8000/docs (API)
```
```

---

## RESUMO DE EXECUÇÃO

| Prompt | Fase | Semanas | Entregável Principal |
|--------|------|---------|---------------------|
| 1.1 | Fundação | 1 | Estrutura do repositório |
| 1.2 | Fundação | 1 | Docker Compose completo |
| 1.3 | Fundação | 1-2 | Gerador de dados sintéticos |
| 1.4 | Fundação | 2 | Kafka producers (streaming) |
| 1.5 | Fundação | 2 | Ingestão batch + DAGs Airflow |
| 2.1 | Transformação | 3 | PySpark Bronze → Silver |
| 2.2 | Transformação | 3-4 | PySpark Silver → Gold (Star Schema) |
| 2.3 | Transformação | 4 | Spark Structured Streaming + Anomaly Detection |
| 2.4 | Transformação | 4 | DAGs completas de orquestração |
| 3.1 | Governança | 5 | Great Expectations (Data Quality) |
| 3.2 | Governança | 5-6 | OpenMetadata (Catálogo + Linhagem) |
| 3.3 | Governança | 6 | Delta Lake + Auditoria |
| 4.1 | Disponibilização | 7 | PostgreSQL serving layer |
| 4.2 | Disponibilização | 7 | API REST FastAPI |
| 4.3 | Disponibilização | 8 | Dashboards Superset/Grafana |
| 4.4 | Disponibilização | 8 | Testes automatizados completos |
| 5.1 | Cloud AWS | 9 | Terraform: VPC, S3, IAM |
| 5.2 | Cloud AWS | 9 | Terraform: MSK (Kafka) |
| 5.3 | Cloud AWS | 9-10 | Terraform: EMR (Spark) |
| 5.4 | Cloud AWS | 10 | Terraform: MWAA, Glue, Athena |
| 5.5 | Cloud AWS | 10 | Adaptação dos pipelines para AWS |
| 6.1 | Refinamento | 11 | CI/CD completo |
| 6.2 | Refinamento | 11-12 | Documentação completa |
| 6.3 | Refinamento | 12 | Performance + polimento final |

---

## DICAS DE USO

1. **Execute os prompts na ordem** — cada um assume que o anterior foi completado.
2. **Revise o output de cada prompt** antes de prosseguir ao próximo.
3. **Commite no GitHub após cada prompt** para manter o histórico de evolução.
4. **Teste localmente** (`make up`, `make test`) após cada fase completa.
5. **Adapte conforme necessário** — se a banca pedir foco em algo específico, aprofunde naquele prompt.
6. **Use `make lint`** regularmente para manter a qualidade do código.
7. **Documente decisões** — se mudar algo do plano original, registre o motivo no CHANGELOG.
