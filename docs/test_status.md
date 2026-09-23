# Status dos Testes — Financial Fraud Detection Platform

> Tabela incrementada ao longo do desenvolvimento. Status atualizado conforme os testes são executados.

## Legenda

| Status | Significado |
|--------|-------------|
| NOK | Não executado / Falhou |
| OK | Passou |
| N/A | Não aplicável |

## Índice

Ordem cronológica de execução (não de número de issue). Cada seção tem um
checklist completo correspondente em `docs/`.

| Seção | IDs | Checklist completo |
|-------|-----|---------------------|
| Step 1.3 — Data Generator | `1.3-*` | `docs/testes_step_1.3.txt` |
| Step 1.4 — Kafka Producers | `1.4-*` | `docs/testes_step_1.4.txt` |
| Step 1.5 — Ingestão Batch + Airflow DAGs | `1.5-*` | `docs/testes_step_1.5.txt` |
| Step 1.6 — Transformação Bronze → Silver | `1.6-*` | `docs/testes_step_1.6.txt` |
| Step 1.7 — Transformação Silver → Gold | `1.7-*` | `docs/testes_step_1.7.txt` |
| Issue #7 — Reprodutibilidade do Ambiente Local | `7-*` | `docs/testes_issue_7.txt` |
| Step 1.8 — Privacidade, Qualidade e Semântica dos Dados | `1.8-*` | `docs/testes_step_1.8.txt` |
| Issue #8 — Contrato de Dados Ingestion ↔ Transformação | `8-*` | `docs/testes_issue_8.txt` |
| Issue #10 — Loader Gold para PostgreSQL | `10-*` | `docs/testes_issue_10.txt` |
| Issue #11 — Streaming Spark e Detecção de Fraude | `11-*` | `docs/testes_issue_11.txt` |
| Issue #15 — API FastAPI para Consultas e Alertas | `15-*` | `docs/testes_issue_15.txt` |
| Issue #13 — Quality Gates com Great Expectations | `13-*` | `docs/testes_issue_13.txt` |
| Issue #14 — Catálogo de Dados Leve | `14-*` | `docs/testes_issue_14.txt` |
| Issue #16 — Dashboards Operacionais/Analíticos com Superset | `16-*` | `docs/testes_issue_16.txt` |
| Issue #18 — Atualização de Documentação | `18-*` | `docs/testes_issue_18.txt` |
| Validação End-to-End da Stack Completa | `E2E-*` | `docs/testes_e2e_validation.txt` |
| Validação Manual V1 (checklist da banca) | `V1-*` | seção "Validação Manual V1" abaixo |
| Issue #38 — Serving do streaming (`fraud_score` e alertas) | `38-*` | seção "Issue #38" abaixo |
| Issue #37 — Imagem SVG da linhagem do catálogo | `37-*` | seção "Issue #37" abaixo |

---

## Step 1.3 — Data Generator

### 1. Testes Unitários Automatizados

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.3-1.1 | Suite completa: `pytest tests/unit/test_data_generator.py tests/unit/test_schemas.py -v` | Todos os testes PASSED, sem erros ou warnings críticos | OK |
| 1.3-1.2 | Cobertura: `pytest tests/unit/ --cov=src/common --cov-report=term-missing` | Cobertura >= 70% em `data_generator.py` e `schemas.py` | OK |

### 2. Geração de Dados — Smoke Test (volume pequeno)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.3-2.1 | Executar script com 1.000 transações, 100 clientes, 10 dias de mercado, seed 42 | Taxa de fraude entre 1–5%, PIX dominante (~45%), 4 tipos de fraude | OK |
| 1.3-2.2 | Verificar arquivos CSV gerados em `data/test_smoke/` | `customers.csv`, `transactions/YYYY/MM/DD/transactions.csv`, `market_data/YYYY/MM/DD/market.csv` presentes | OK |
| 1.3-2.3 | Verificar conteúdo do `customers.csv` (100 linhas) | 100 registros com todas as colunas; `country="BR"`, `segment` em {VAREJO, ALTA_RENDA, PRIVATE}, `risk_score` em [0, 100] | OK |
| 1.3-2.4 | Verificar conteúdo do `transactions.csv` | Todas as colunas presentes; `amount > 0`; `fraud_type` preenchido somente quando `is_fraud=True` | OK |
| 1.3-2.5 | Verificar conteúdo do `market.csv` | Colunas corretas; `high >= low`; preços > 0; 10 símbolos B3 presentes | OK |

### 3. Reprodutibilidade

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.3-3.1 | Gerar dois datasets com `--seed 99` e comparar `customers.csv` | Nenhuma diferença entre os arquivos (`diff` vazio) | OK |
| 1.3-3.2 | Gerar com `--seed 1` e `--seed 2` | `customer_id` diferentes entre os dois datasets | OK |

### 4. Compatibilidade com Schemas Pydantic

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.3-4.1 | Validar 100 transações geradas contra `TransactionEvent` via Pydantic | Validados: 100/100 — Erros: 0 | OK |

### 5. Geração em Volume Completo

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.3-5.1 | `make seed-data` (500k transações) | Sem erros; ~12.500 fraudes (2,5%); ~1.000 cotações; execução em 30–120s; total = 500.000 transações | OK |

---

## Step 1.4 — Kafka Producers

### 1. Testes Unitários — `tests/unit/test_producers.py`

#### TestProducerConfig

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.4-PC-01 | `test_default_values` | `acks="1"`, `retries=3`, `batch_size=16384`, `linger_ms=10`, `compression="lz4"` | OK |
| 1.4-PC-02 | `test_to_kafka_python_dict_keys` | Dict contém todas as chaves esperadas pelo kafka-python | OK |
| 1.4-PC-03 | `test_value_serializer_encodes_utf8` | Serializer de valor codifica string para bytes UTF-8 | OK |
| 1.4-PC-04 | `test_key_serializer_encodes_utf8` | Serializer de chave retorna `None` para `None` | OK |
| 1.4-PC-05 | `test_low_latency_profile` | `LOW_LATENCY_CONFIG`: `linger_ms=0`, `batch_size=1` | OK |
| 1.4-PC-06 | `test_high_throughput_profile` | `HIGH_THROUGHPUT_CONFIG`: `linger_ms=50`, `batch_size=65536` | OK |

#### TestMessageSerialization

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.4-MS-01 | `test_transaction_message_is_valid_json` | Mensagem serializada é JSON válido e parseável | OK |
| 1.4-MS-02 | `test_transaction_required_fields` | Todos os campos obrigatórios presentes no evento | OK |
| 1.4-MS-03 | `test_transaction_type_valid` | `transaction_type` é valor válido do enum `TransactionType` | OK |
| 1.4-MS-04 | `test_amount_within_range` | `amount` entre R$ 1,00 e R$ 500.000,00 | OK |
| 1.4-MS-05 | `test_currency_distribution_brl_dominant` | BRL representa >= 70% das transações geradas | OK |
| 1.4-MS-06 | `test_market_tick_serialize` | Tick de mercado serializa para JSON com `symbol` e `price > 0` | OK |

#### TestTickSimulator

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.4-TS-01 | `test_tick_required_fields` | Tick contém: `event_id`, `symbol`, `timestamp`, `price`, `volume`, `bid`, `ask`, `spread` | OK |
| 1.4-TS-02 | `test_tick_spread_is_ask_minus_bid` | `ask >= bid` e `spread >= 0` para todos os símbolos | OK |
| 1.4-TS-03 | `test_tick_price_positive` | Preço sempre > 0 ao longo de 20 ticks | OK |
| 1.4-TS-04 | `test_tick_volume_positive` | Volume sempre >= 1 | OK |
| 1.4-TS-05 | `test_tick_price_evolves` | Preço varia (random walk) ao longo de 50 ticks | OK |
| 1.4-TS-06 | `test_all_symbols_generate_ticks` | Todos os símbolos de `MARKET_SYMBOLS` geram ticks válidos | OK |

#### TestProducerRun

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.4-PR-01 | `test_transactions_producer_sends_and_stops` | Producer envia ao menos 1 mensagem e chama `flush()` ao parar | OK |
| 1.4-PR-02 | `test_market_producer_sends_and_stops` | Market producer envia ao menos 1 tick e chama `flush()` ao parar | OK |

### 2. Testes de Integração (infra real)

> Pré-requisito: `make up && make setup`

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.4-INT-01 | `make producer-transactions` — verificar no Kafka UI | Tópico `raw-transactions` recebendo mensagens; ~10 TPS; campos e headers (`produced_at`, `source_system`) presentes | OK |
| 1.4-INT-02 | `make producer-market` — verificar no Kafka UI | Tópico `raw-market-data` recebendo ticks; ~50 TPS; campos e headers presentes | OK |

### 3. Validações Manuais

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.4-MAN-01 | Graceful shutdown (Ctrl+C) | Log "Producer encerrado" com totais de mensagens enviadas e erros | OK |
| 1.4-MAN-02 | Headers Kafka via Kafka UI | Headers `produced_at` e `source_system` presentes em cada mensagem | OK |
| 1.4-MAN-03 | Market producer fora do horário de pregão | Loga "Fora do horário de pregão" e aguarda 60s sem enviar mensagens | NOK |
| 1.4-MAN-04 | Particionamento por chave — transações | Transações do mesmo `customer_id` vão para a mesma partição | NOK |
| 1.4-MAN-05 | Particionamento por chave — market data | Ticks do mesmo `symbol` vão para a mesma partição | NOK |
| 1.4-MAN-06 | Perfis `LOW_LATENCY_CONFIG` e `HIGH_THROUGHPUT_CONFIG` | Valores de `linger_ms` e `batch_size` conforme especificação | NOK |

### 4. Cobertura

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.4-COV-01 | `make test-cov` → `htmlcov/index.html` | Cobertura >= 70% em `src/ingestion/streaming/` | OK |

---

## Step 1.5 — Ingestão Batch + Airflow DAGs

> Checklist completo: `docs/testes_step_1.5.txt`

### 1. Testes Unitários — `tests/unit/test_batch_ingestion.py`

#### TestMarketDataCollector

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.5-MC-01 | `test_normalize_adds_required_columns` | Colunas symbol, date, open/high/low/close, volume, ingestion_timestamp, source_system presentes | OK |
| 1.5-MC-02 | `test_normalize_date_format` | Data no formato YYYY-MM-DD | OK |
| 1.5-MC-03 | `test_collect_daily_skips_existing_partitions` | Partições já existentes são ignoradas (idempotência) | OK |
| 1.5-MC-04 | `test_collect_daily_uploads_new_partitions` | 2 datas × 1 ticker → 2 uploads | OK |
| 1.5-MC-05 | `test_collect_daily_handles_empty_response` | Resposta vazia do yfinance → 0 registros, sem upload | OK |
| 1.5-MC-06 | `test_collect_daily_handles_exception` | Exceção na API → 0 para o ticker, sem propagação | OK |

#### TestTransactionLoader

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.5-TL-01 | `test_load_to_bronze_ingests_csv` | CSV com 2 linhas → 1 upload, total = 2 | OK |
| 1.5-TL-02 | `test_load_to_bronze_skips_existing` | Arquivo já no MinIO → upload não chamado, resultado = 0 | OK |
| 1.5-TL-03 | `test_load_to_bronze_empty_dir` | Diretório vazio → dicionário vazio, sem upload | OK |
| 1.5-TL-04 | `test_add_metadata_adds_columns` | Colunas ingestion_timestamp, source_file, batch_id adicionadas | OK |
| 1.5-TL-05 | `test_build_key_partitioned` | Chave contém year=YYYY/month=MM/day=DD/ | OK |
| 1.5-TL-06 | `test_build_key_fallback` | Caminho sem padrão → chave com fallback por timestamp | OK |

#### TestCustomerLoader

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.5-CL-01 | `test_load_to_bronze_ingests_customers` | 2 clientes ingeridos, 1 upload | OK |
| 1.5-CL-02 | `test_load_to_bronze_skips_existing` | Snapshot já existente → sem upload | OK |
| 1.5-CL-03 | `test_load_to_bronze_missing_file` | Arquivo ausente → 0 sem erro | OK |
| 1.5-CL-04 | `test_apply_scd2_adds_columns` | valid_from, valid_to=9999-12-31, is_current=True, ingestion_timestamp, batch_id | OK |
| 1.5-CL-05 | `test_build_key_contains_today` | Chave contém data de hoje (YYYY-MM-DD) | OK |

### 2. Testes de Integração — `tests/integration/test_minio_upload.py`

> Pré-requisito: `make up && make setup`

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.5-INT-01 | `test_upload_and_download_parquet` | Upload → Download round-trip com colunas e linhas idênticas | OK |
| 1.5-INT-02 | `test_check_exists_true_after_upload` | check_exists retorna True após upload | OK |
| 1.5-INT-03 | `test_check_exists_false_for_nonexistent` | check_exists retorna False para objeto inexistente | OK |
| 1.5-INT-04 | `test_list_objects_returns_uploaded` | list_objects retorna chave do objeto enviado | OK |
| 1.5-INT-05 | `test_upload_parquet_bytes` | upload_parquet_bytes funciona para bytes serializados | OK |
| 1.5-INT-06 | `test_load_to_bronze_e2e` (TransactionLoader) | 10 transações no CSV → total = 10 no MinIO | OK |
| 1.5-INT-07 | `test_idempotency_no_double_ingestion` | Segunda chamada ingere 0 registros | OK |
| 1.5-INT-08 | `test_collect_daily_e2e` (MarketDataCollector) | collect_daily com mock yfinance salva no MinIO | OK |

### 3. Testes Manuais — Airflow UI

> Acesse: http://localhost:8082 (admin / admin)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.5-AW-01 | DAG `batch_ingestion_pipeline` visível na lista | DAG aparece sem import errors | OK |
| 1.5-AW-02 | DAG `seed_sample_data` visível na lista | DAG aparece sem import errors | OK |
| 1.5-AW-03 | Import Errors = 0 no painel do Airflow | Sem erros de importação | OK |
| 1.5-AW-04 | Trigger manual de `seed_sample_data` | Executa sem erros; gera arquivos em data/sample/ | OK |
| 1.5-AW-05 | Trigger manual de `batch_ingestion_pipeline` | Todos os tasks verdes; grafo de dependências correto | OK |
| 1.5-AW-06 | XCom dos tasks de ingestão | Contagem de registros nos XComs | OK |

### 4. Testes Manuais — MinIO

> Acesse: http://localhost:9001 (minioadmin / minioadmin)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.5-MN-01 | Buckets bronze/silver/gold/checkpoints existem | 4 buckets visíveis | OK |
| 1.5-MN-02 | Parquet em bronze/market_data/ após pipeline | bronze/market_data/date=YYYY-MM-DD/<ticker>.parquet | OK |
| 1.5-MN-03 | Parquet em bronze/transactions/ após pipeline | bronze/transactions/year=.../month=.../day=.../transactions.parquet | OK |
| 1.5-MN-04 | Parquet em bronze/customers/ após pipeline | bronze/customers/snapshot_date=YYYY-MM-DD/customers.parquet | OK |
| 1.5-MN-05 | Re-execução sem duplicatas (idempotência E2E) | Mesmos arquivos, sem duplicação de registros | OK |

### 5. Cobertura

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.5-COV-01 | `pytest tests/unit/test_batch_ingestion.py` | 17/17 testes PASSED | OK |
| 1.5-COV-02 | Cobertura dos novos módulos (batch ingestion) | customer_loader=100%, market_data_collector=92%, transaction_loader=96% | OK |
| 1.5-COV-03 | `make lint` sem erros | ruff + mypy passam | OK |

---

## Step 1.6 — Transformação Bronze → Silver (PySpark Batch)

> Checklist completo: `docs/testes_step_1.6.txt`

### 1. Testes Unitários — `tests/unit/test_bronze_to_silver.py`

#### TestCleanTransactions

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-CT-01 | `test_removes_null_transaction_id` | Registros com transaction_id=null removidos | OK |
| 1.6-CT-02 | `test_casts_amount_to_decimal` | amount cast para DecimalType(18,2) com arredondamento correto | OK |
| 1.6-CT-03 | `test_normalizes_currency_to_uppercase` | 'brl' normalizado para 'BRL' | OK |
| 1.6-CT-04 | `test_filters_invalid_currency` | Moedas fora de {BRL, USD, EUR} removidas | OK |
| 1.6-CT-05 | `test_fills_null_channel` | channel=null preenchido com 'UNKNOWN' | OK |
| 1.6-CT-06 | `test_fills_null_merchant_category` | merchant_category=null preenchido com 'OUTROS' | OK |
| 1.6-CT-07 | `test_deduplication_keeps_most_recent` | Duplicatas por transaction_id: mantém o mais recente | OK |
| 1.6-CT-08 | `test_no_rows_discarded_for_clean_data` | 5 registros válidos → 5 registros na saída | OK |

#### TestEnrichTransactions

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-ET-01 | `test_adds_transaction_date` | transaction_date = '2024-06-15' | OK |
| 1.6-ET-02 | `test_adds_transaction_hour` | transaction_hour = 14 para timestamp às 14:30 | OK |
| 1.6-ET-03 | `test_is_business_hours_true_at_10h` | is_business_hours=True para hora 10 | OK |
| 1.6-ET-04 | `test_is_business_hours_false_at_22h` | is_business_hours=False para hora 22 | OK |
| 1.6-ET-05 | `test_amount_brl_same_for_brl` | amount_brl = amount para BRL | OK |
| 1.6-ET-06 | `test_amount_brl_converted_for_usd` | amount_brl = amount × 5.0 para USD | OK |
| 1.6-ET-07 | `test_amount_brl_converted_for_eur` | amount_brl = amount × 5.4 para EUR | OK |
| 1.6-ET-08 | `test_adds_processing_timestamp` | processing_timestamp não é null | OK |
| 1.6-ET-09 | `test_custom_fx_rates` | Taxa customizada USD=6.0 → 10 USD = 60 BRL | OK |

#### TestCleanMarketData

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-CM-01 | `test_removes_zero_volume` | Registro com volume=0 removido | OK |
| 1.6-CM-02 | `test_removes_negative_close` | Registro com close<0 removido | OK |
| 1.6-CM-03 | `test_valid_records_not_removed` | 5 registros válidos → 5 registros na saída | OK |

#### TestCalculateMarketIndicators

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-MI-01 | `test_daily_return_positive` | daily_return = (close−open)/open | OK |
| 1.6-MI-02 | `test_daily_return_negative` | daily_return negativo para close<open | OK |
| 1.6-MI-03 | `test_intraday_range` | intraday_range = (high−low)/low | OK |
| 1.6-MI-04 | `test_sma_columns_present` | Colunas sma_5, sma_10, sma_20 presentes | OK |
| 1.6-MI-05 | `test_sma_5_correct_value` | SMA-5 de [10,20,30,40,50] = 30.0 | OK |
| 1.6-MI-06 | `test_adds_processing_timestamp` | processing_timestamp não é null | OK |

#### TestTransformCustomers

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-TC-01 | `test_age_is_calculated` | Idade calculada a partir de birth_date | OK |
| 1.6-TC-02 | `test_age_group_18_24` | birth_date=2005 → age_group='18-24' | OK |
| 1.6-TC-03 | `test_age_group_65_plus` | birth_date=1950 → age_group='65+' | OK |
| 1.6-TC-04 | `test_age_group_25_34` | birth_date=1993 → age_group in {'25-34','35-44'} | OK |
| 1.6-TC-05 | `test_processing_timestamp_added` | processing_timestamp não é null | OK |
| 1.6-TC-06 | `test_cpf_masked_preserved` | cpf_masked preservado sem alteração | OK |

#### TestFilterByDate

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-FD-01 | `test_filter_start_date` | start_date='2024-06-10' → 2 de 3 registros | OK |
| 1.6-FD-02 | `test_filter_end_date` | end_date='2024-06-12' → 2 de 3 registros | OK |
| 1.6-FD-03 | `test_filter_date_range` | range [09..12] → 1 de 3 registros | OK |
| 1.6-FD-04 | `test_no_filter_when_dates_are_none` | Sem filtro → todos os 3 registros | OK |

#### TestPublicMethodsMocked

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-PM-01 | `test_transform_transactions_returns_metrics` | rows_read=3, rows_written<=3 | OK |
| 1.6-PM-02 | `test_transform_market_data_returns_metrics` | rows_read=2, rows_written presente | OK |
| 1.6-PM-03 | `test_transform_customers_returns_metrics` | rows_read=4, rows_discarded=0 | OK |

#### TestConstants

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.6-CO-01 | `test_valid_currencies_contain_expected` | {BRL, USD, EUR} ⊆ _VALID_CURRENCIES | OK |
| 1.6-CO-02 | `test_default_fx_brl_is_one` | _DEFAULT_FX_RATES['BRL'] == 1.0 | OK |
| 1.6-CO-03 | `test_default_fx_usd_positive` | _DEFAULT_FX_RATES['USD'] > 0 | OK |
| 1.6-CO-04 | `test_default_fx_eur_positive` | _DEFAULT_FX_RATES['EUR'] > 0 | OK |

### 2. Testes de Integração (requer `make up && make setup`)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.6-INT-01 | `make spark-submit-batch` (transactions) | Parquet em silver/transactions/transaction_date=YYYY-MM-DD/ | OK |
| 1.6-INT-02 | `make spark-submit-batch` (market_data) | Parquet em silver/market_data/date=YYYY-MM-DD/ com sma_5/10/20 | OK¹ |
| 1.6-INT-03 | `make spark-submit-batch` (customers) | Parquet em silver/customers/ com colunas age e age_group | OK |
| 1.6-INT-04 | Idempotência — rodar job duas vezes | Segunda execução sobrescreve sem duplicatas | OK |
| 1.6-INT-05 | Verificar Silver no MinIO Console | silver/transactions/, silver/market_data/, silver/customers/ presentes | OK |
| 1.6-INT-06 | Filtro de datas via CLI | `--start-date 2024-01-01 --end-date 2024-01-31` filtra corretamente | OK² |

¹ `ingest_market_data` (yfinance, step 1.5) só retornou 1 registro real neste ambiente sandbox — job Bronze→Silver processou corretamente, mas o volume não permite validar a tendência da SMA (ver 1.6-MAN-05).

> **Errata (2026-09-23):** a causa não era o relógio do sandbox: o Yahoo Finance devolve HTTP 429 (rate limit) ao IP do ambiente. Ver a seção "Validação Manual V1" no fim deste arquivo.
² Bug real encontrado e corrigido nesta rodada: `spark.sql.sources.partitionOverwriteMode` estava no modo estático (padrão do Spark), então rodar o job com `--start-date/--end-date` apagava **todas** as partições de `silver/transactions/`, não só as do intervalo filtrado. Corrigido em `src/common/spark_session.py` (modo `dynamic`) e revalidado: reprocessar 4 dias específicos preservou as outras 291 partições intactas.

### 3. Validações Manuais dos Dados Transformados

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.6-MAN-01 | Coluna `transaction_date` no Silver | DATE type, sem nulls | OK |
| 1.6-MAN-02 | Coluna `amount_brl` sempre > 0 | Conversão BRL/USD/EUR correta | OK (0 violações em 506.146 linhas) |
| 1.6-MAN-03 | Distribuição de `is_business_hours` | ~40-50% TRUE | OK (41,4% TRUE — 209.775/506.146) |
| 1.6-MAN-04 | Sem moedas inválidas no Silver | Apenas BRL, USD, EUR | OK |
| 1.6-MAN-05 | Médias móveis consistentes (tendência alta) | sma_5 ≈ sma_10 ≈ sma_20 com diferença esperada | N/A — Bronze só tem 1 registro real de mercado neste ambiente (ver nota ¹ acima); lógica de SMA já coberta pelos testes unitários 1.6-MI-04/05 |
| 1.6-MAN-06 | `age_group` cobre faixas corretas | 18-24, 25-34, 35-44, 45-54, 55-64, 65+ | OK (todas as 6 faixas presentes) |
| 1.6-MAN-07 | `cpf_masked` no Silver | Formato ***.***.***-XX preservado | **NOK — bug encontrado** |

**Bug 1.6-MAN-07**: `cpf_masked` chega ao Silver como `***.***. 196-00` em vez de `***.***.***-00`. Causa raiz em `src/common/data_generator.py:107-109` (step 1.3, já em `main`, fora do escopo deste branch):
1. O formato implementado (`***.***.XXX-XX`) revela os últimos 5 dígitos do CPF, não 2 como documentado/esperado — risco de exposição de PII (LGPD) maior que o pretendido.
2. Bug de digitação: há um espaço literal no f-string (`f"***.***. {cpf_digits[6:9]}-{cpf_digits[9:11]}"`) antes do terceiro grupo.

`bronze_to_silver.py` está correto — apenas repassa o valor sem alterar (teste 1.6-TC-06 confirma isso). A correção pertence ao gerador de dados (step 1.3) e não foi aplicada aqui por estar fora do escopo do step 1.6; recomenda-se abrir tarefa separada.

### 4. Cobertura

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.6-COV-01 | `pytest tests/unit/test_bronze_to_silver.py` | 43/43 testes PASSED | OK |
| 1.6-COV-02 | Cobertura `bronze_to_silver.py` | >= 80% | OK (83%) |
| 1.6-COV-03 | Cobertura geral (`src/`) | >= 70% | OK (86% — 113/113 testes da suíte completa passaram) |
| 1.6-COV-04 | `ruff check` nos novos módulos | All checks passed! | OK |
| 1.6-COV-05 | `mypy` nos novos módulos | Success: no issues found | OK |

### 5. Bugs de infraestrutura corrigidos durante os testes de integração

Nenhum destes pertence à lógica de negócio testada nos itens acima, mas todos bloqueavam `make spark-submit-batch` de rodar; correções aplicadas para viabilizar os testes:

| Arquivo | Problema | Correção |
|---------|----------|----------|
| `docker/spark/Dockerfile` | `/home/spark` não existia na imagem `apache/spark:3.5.1` → Ivy falhava ao resolver pacotes (`--packages`) | `mkdir -p /home/spark && chown spark:spark /home/spark` |
| `docker/spark/Dockerfile` | `src/` não estava no `PYTHONPATH` do container → `ModuleNotFoundError: No module named 'src'` | `ENV PYTHONPATH=/opt/spark/work-dir` |
| `src/common/config.py`, `src/common/logger.py` | Imagem Spark roda Python 3.8 (projeto alvo é 3.11+); anotações `list[str]` / `str \| None` sem `from __future__ import annotations` quebram no import | Adicionado `from __future__ import annotations` |
| `src/ingestion/batch/transaction_loader.py` | Partições diárias sem nenhuma fraude faziam o pandas inferir `fraud_type` como `float64` (coluna 100% NaN) em vez de string, causando `SchemaColumnConvertNotSupportedException` ao ler múltiplas partições juntas no Spark | Cast explícito `df["fraud_type"].astype("string")` antes de gravar Parquet |
| `src/common/spark_session.py` | Overwrite estático apagava partições fora do filtro de data (ver nota ² acima) | `spark.sql.sources.partitionOverwriteMode=dynamic` |

Além disso, `docker-compose.yml` teve as portas do Postgres (5432→5433) e MinIO (9000→9002) remapeadas por conflito com containers de **outro projeto** (`protege_*`) já rodando na máquina — não é um bug do projeto, específico deste ambiente local.

---

## Step 1.7 — Transformação Silver → Gold (PySpark Batch)

Job `silver_to_gold.py`: constrói um star schema (`dim_customers`, `dim_date`,
`fact_transactions`) e uma agregação diária de métricas de fraude
(`agg_daily_fraud_metrics`) a partir da camada Silver. Detalhes de schema em
`docs/data_dictionary.md`.

### 1. Testes Unitários — `tests/unit/test_silver_to_gold.py`

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 1.7-DC | `TestBuildDimCustomers` | Filtra `is_current`, renomeia `customer_id`→`customer_key`, preserva atributos, adiciona `processing_timestamp` (4 testes) | OK |
| 1.7-DD | `TestBuildDimDate` | Componentes de calendário (year/month/day/quarter/day_of_week/day_name/week_of_year/is_weekend), sem perda de datas distintas (8 testes) | OK |
| 1.7-FT | `TestBuildFactTransactions` | Renomeia chaves, preserva medidas, grão de 1 linha por transação, `processing_timestamp`, `fraud_score` ausente tratado como nulo (5 testes) | OK |
| 1.7-AG | `TestBuildAggDailyFraudMetrics` | Agrupamento por data+tipo, `total_transactions`, `total_amount_brl`, `fraud_count`/`fraud_rate` (5 testes) | OK |
| 1.7-FD | `TestFilterByDate` | Filtro start/end/range/sem filtro (4 testes) | OK |
| 1.7-PM | `TestPublicMethodsMocked` | Métodos públicos com leitura/escrita mockadas, métricas retornadas corretamente (4 testes) | OK |

**Total: 30/30 testes PASSED** (29 originais + `test_handles_missing_fraud_score_column`, adicionado após bug encontrado em 1.7-INT-03).

### 2. Cobertura

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.7-COV-01 | `pytest tests/unit/test_silver_to_gold.py` | Todos os testes PASSED | OK (30/30) |
| 1.7-COV-02 | Cobertura `silver_to_gold.py` | >= 80% | OK (80%) |
| 1.7-COV-03 | Suíte completa (`pytest tests/unit/`) | Sem regressão | OK (142/142 PASSED, cobertura geral 85%) |
| 1.7-COV-04 | `ruff check` | All checks passed! | OK |
| 1.7-COV-05 | `mypy` | Success: no issues found | OK |

### 3. Testes de Integração (requer `make up && make setup` + Silver populado pelo step 1.6)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.7-INT-01 | `make spark-submit-silver-gold` (dim_customers) | Parquet em gold/dim_customers/ | OK (rows_read=11000, rows_written=11000) |
| 1.7-INT-02 | `make spark-submit-silver-gold` (dim_date) | Parquet em gold/dim_date/ | OK (295 datas distintas) |
| 1.7-INT-03 | `make spark-submit-silver-gold` (fact_transactions) | Parquet em gold/fact_transactions/date_key=YYYY-MM-DD/ | OK (506.146 linhas, 295 partições) |
| 1.7-INT-04 | `make spark-submit-silver-gold` (agg_daily_fraud_metrics) | Parquet em gold/agg_daily_fraud_metrics/date_key=YYYY-MM-DD/ | OK (1.745 grupos, 295 partições) |
| 1.7-INT-05 | Idempotência — rodar job duas vezes | Segunda execução sobrescreve sem duplicatas | OK — métricas idênticas na 2ª execução, contagem de arquivos inalterada |
| 1.7-INT-06 | Filtro de datas via CLI (`--dataset fact_transactions --start-date --end-date`) | Filtra corretamente sem afetar outras partições/dim_date | OK — 8.227 linhas no intervalo 2026-08-01..04; 295 partições totais preservadas (overwrite dinâmico herdado do step 1.6) |

**Bug encontrado e corrigido durante 1.7-INT-03**: `_build_fact_transactions` selecionava a coluna `fraud_score` incondicionalmente, mas ela não existe em `silver/transactions` no batch puro (só é populada pelo streaming, roadmap 2.3/2.4 — ainda não implementado). O job quebrava com `UNRESOLVED_COLUMN.WITH_SUGGESTION`. Corrigido em `silver_to_gold.py` para tratar `fraud_score` como opcional (nulo quando a coluna está ausente do schema de entrada); teste unitário `test_handles_missing_fraud_score_column` adicionado para cobrir o caso.

### 4. Validações Manuais dos Dados Transformados

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.7-MAN-01 | `dim_customers` só tem clientes correntes | Contagem == clientes com `is_current=True` no Silver | OK (11.000 == 11.000) |
| 1.7-MAN-02 | `dim_date` cobre todas as datas de `fact_transactions` | Nenhum `date_key` órfão (join fact↔dim_date sem nulls) | OK (0 órfãos) |
| 1.7-MAN-03 | `fact_transactions.amount_brl` bate com Silver | Soma agregada igual à soma em `silver/transactions` | OK (diferença ~3.6e-7, arredondamento de ponto flutuante) |
| 1.7-MAN-04 | `agg_daily_fraud_metrics` bate com a fato | `sum(total_transactions)` == `count(fact_transactions)` | OK (506.146 == 506.146) |
| 1.7-MAN-05 | `agg_daily_fraud_metrics.fraud_rate` coerente | `fraud_rate` entre 0 e 1 para todos os grupos | OK (min=0.0, max=1.0, média=2,67%) |

Validação extra (não prevista no checklist original, adicionada durante a execução): `fact_transactions.customer_key` sem correspondência em `dim_customers` → 0 órfãos.

### 5. Cobertura (após correção do bug de fraud_score)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.7-COV-01b | Suíte completa (`pytest tests/unit/`) | Sem regressão | OK (143/143 PASSED, cobertura geral 85%) |
| 1.7-COV-04b | `ruff check` | All checks passed! | OK |
| 1.7-COV-05b | `mypy` | Success: no issues found | OK |

### 6. Fechamento da issue #9 (executado em 2026-09-03)

A issue #9 pedia duas coisas além do job em si (já validado acima): decidir e
aplicar consistentemente Parquet ou Delta Lake, e integrar o job a uma DAG
Airflow. Checklist completo em `docs/testes_step_1.7.txt`.

**Parquet vs. Delta Lake** — decisão (validada com o usuário): manter Parquet
puro. O Spark session e o Makefile configuravam extensões Delta com versão
incompatível com o cluster (`delta-core_2.12:2.4.0` é pra Spark 3.4; o cluster
roda Spark 3.5.1) e nunca usada de fato (todo `.write` já era `.format("parquet")`).
Como Silver/Gold são reescritos por completo a cada execução (idempotente via
overwrite dinâmico por partição), o log ACID do Delta não agrega valor agora.
Config Delta removida de `spark_session.py`, `Makefile` (3 targets) e
`pyproject.toml` (dependência `delta-spark` não usada em lugar nenhum do código).

**DAG Airflow** (`dags/dag_batch_transformation.py`) — `bronze_to_silver >>
silver_to_gold >> notify_completion` via `SparkSubmitOperator`, schedule 07:00
UTC. O container do Airflow não tinha JVM/PySpark; adicionado
`openjdk-17-jre-headless` + `pyspark==3.5.1` + `PYTHONPATH=/opt/airflow` em
`docker/airflow/Dockerfile`, e `AIRFLOW_CONN_SPARK_DEFAULT` em `docker-compose.yml`.

Dois bugs só apareceram rodando via Airflow (não no `spark-submit` direto
contra o `spark-master`, onde o driver já tem todos os jars):

| Bug | Causa | Correção |
|-----|-------|----------|
| `spark-submit --master spark-master:7077` (sem `spark://`) | `SparkSubmitHook` monta `master = f"{host}:{port}"` — não reconstrói esquema | Connection como JSON (não URI), com `host="spark://spark-master"` |
| `ClassNotFoundException: S3AFileSystem` | Driver roda em modo client dentro do container do Airflow, sem os jars `hadoop-aws`/`aws-java-sdk-bundle` da imagem customizada do Spark | `packages=` com esses 2 jars nas duas `SparkSubmitOperator` (resolvidos via Ivy) |

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 9-DAG-01 | DAG aparece no Airflow sem import errors | Sem erros | OK |
| 9-DAG-02 | Trigger manual — pipeline completa de ponta a ponta | 3 tasks success | OK (após os 2 bugs acima corrigidos) |
| 9-DAG-03 | `gold/` populado ao final da DAG | 4 tabelas, partições corretas | OK — 295 partições em `fact_transactions`/`agg_daily_fraud_metrics` |

Execução real (`manual__2026-09-03T18:09:02+00:00`): `bronze_to_silver` →
transactions=506.146, market_data=1, customers=71.000; `silver_to_gold` → Gold
completo. Durante a execução houve ~13 min de falhas de heartbeat por DNS
intermitente no container (`could not translate host name postgres-airflow`)
— instabilidade pontual do Docker Desktop/WSL2 local, autorresolvida, não é
bug do projeto.

---

## Issue #7 — Reprodutibilidade do Ambiente Local

> Este trabalho foi commitado originalmente como "step-1.7", em paralelo e sem
> conhecimento do step 1.7 acima (Silver → Gold) — os dois branches
> `feature/step_1.7` colidiram no mesmo número. Renomeado aqui para "Issue #7"
> e os IDs de teste de `1.7-*` para `ISSUE7-*` para eliminar a ambiguidade
> (ex.: `1.7-DC-01` tinha dois significados diferentes). Nenhum código foi
> alterado, só a documentação.
>
> Checklist completo: `docs/testes_issue_7.txt`

### 1. Testes Unitários — `tests/unit/test_environment_consistency.py`

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| ISSUE7-DC-01 | `test_no_obsolete_version_attribute` | `docker-compose.yml` sem atributo `version:` top-level | OK |
| ISSUE7-DC-02 | `test_minio_external_port_default_matches_env_example` | Porta default de `${MINIO_EXTERNAL_PORT:-9000}` bate com `.env.example` | OK |
| ISSUE7-DC-03 | `test_postgres_external_port_default_matches_env_example` | Porta default de `${POSTGRES_EXTERNAL_PORT:-5432}` bate com `.env.example` | OK |
| ISSUE7-CFG-01 | `test_minio_endpoint_default_matches_env_example` | `MinIOSettings().endpoint` == `MINIO_ENDPOINT` do `.env.example` | OK |
| ISSUE7-CFG-02 | `test_postgres_port_default_matches_env_example` | `PostgresSettings().port` == `POSTGRES_PORT` do `.env.example` | OK |
| ISSUE7-PY-01 | `test_python_variable_is_not_windows_path` | `PYTHON` do Makefile sem `\` (caminho Windows) | OK |

### 2. Validação Docker Compose

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|--------------------|--------|
| ISSUE7-CMP-01 | `docker compose config --quiet` sem override | MinIO publica 9000, Postgres publica 5432 | OK |
| ISSUE7-CMP-02 | Com `MINIO_EXTERNAL_PORT`/`POSTGRES_EXTERNAL_PORT` customizados | Portas respeitadas (ex.: 9099/5499) | OK |

### 3. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|--------------------|--------|
| ISSUE7-SUITE-01 | `make test-unit` (todos os steps) | 119/119 PASSED, cobertura >= 70% | OK (86,42%) |
| ISSUE7-LIN-01 | `ruff check` | All checks passed! | OK |
| ISSUE7-LIN-02 | `mypy` | Success: no issues found | OK |

### 4. Validação Manual — Ambiente do Zero (macOS)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|--------------------|--------|
| ISSUE7-MAN-01 | `.venv` criado com Python 3.11 e `make` resolve o interpretador sem config manual | `make test-unit` usa `.venv/bin/python` automaticamente | OK |
| ISSUE7-MAN-02 | `make test-unit` sem JRE instalado | Falha com `JAVA_GATEWAY_EXITED`, documentado no runbook | OK |
| ISSUE7-MAN-03 | Serviços acessíveis nas portas documentadas após `make up` | MinIO (9001/9000), Postgres (5432) sem remapeamento manual | OK |

### 5. Lacunas encontradas e corrigidas nesta rodada

| Arquivo | Problema | Correção |
|---------|----------|----------|
| `Makefile` | `PYTHON := .venv\Scripts\python` — caminho Windows hardcoded, quebrava em macOS/Linux | Detecção de `.venv/bin/python` com fallback para `python3`, sobrescrevível |
| `docker-compose.yml` | Portas do MinIO (9002:9000) e Postgres (5433:5432) remapeadas divergiam de `.env.example`/`config.py` | Parametrizadas via `${MINIO_EXTERNAL_PORT:-9000}`/`${POSTGRES_EXTERNAL_PORT:-5432}` |
| `docker-compose.yml` | Atributo `version: "3.9"` obsoleto no Compose v2 | Removido |
| `README.md` | Pré-requisito de Java (necessário para PySpark local) não documentado — `make test-unit` falhava sem pista em máquina limpa | Documentado no README e no runbook (`docs/runbook.md`) |

Fora de escopo (pertence a outras issues): nomes de variável divergentes para taxa dos producers (issue #8) e máscara de CPF incorreta em `data_generator.py` (issue #12).

PR: [#19](https://github.com/gpgomes/data-master-std-fraud/pull/19) — Closes #7.

---

## Step 1.8 — Privacidade, Qualidade e Semântica dos Dados

> Checklist completo: `docs/testes_step_1.8.txt`

### 1. Testes Unitários

| ID | Teste | Resultado Esperado | Status |
|----|-------|--------------------|--------|
| 1.8-CPF-01 | `test_cpf_masked_format` | `cpf_masked` bate com `***.***.***-XX` para todo cliente gerado | OK |
| 1.8-CPF-02 | `test_customers_validate_against_pydantic_model` | Todo cliente gerado valida contra `CustomerRecord` sem erro | OK |
| 1.8-FS-01 | `test_fraud_score_never_set_by_generator` | `fraud_score` ausente/None em toda transação gerada (campo derivado da detecção, não da geração) | OK |
| 1.8-VAL-01 | `test_valid_cpf_masked_accepted` | `"***.***.***-42"` aceito pelo validador | OK |
| 1.8-VAL-02 | `test_invalid_cpf_masked_rejected[...]` (5 casos) | `ValidationError` para espaço literal, excesso de dígitos, separador errado, dígito único, CPF sem máscara | OK |

### 2. Lint, Suíte Completa e Validação Manual

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|--------------------|--------|
| 1.8-LIN-01/02 | `make lint` | ruff + mypy sem erros | OK |
| 1.8-SUITE-01 | `make test-unit` | 128/128 PASSED, cobertura >= 70% | OK (86,53%) |
| 1.8-MAN-01 | Geração manual de 5 clientes | `cpf_masked` no formato `***.***.***-XX` | OK |
| 1.8-MAN-02 | Revisão de todos os `logger.*` em `src/ingestion/` e `src/transformation/` | Nenhum log interpola nome/CPF/IP/device_id/coordenadas | OK |
| 1.8-MAN-03 | Revisão de `tests/conftest.py` | Sem PII real, apenas ids sintéticos | OK |

### 3. Bugs/lacunas encontrados e corrigidos

| Arquivo | Problema | Correção |
|---------|----------|----------|
| `src/common/data_generator.py:109` | `cpf_masked` expunha 5 dígitos do CPF (não 2) e tinha espaço literal no f-string — bug já identificado no step 1.6 (`1.6-MAN-07`) mas não corrigido naquele branch | Reescrito para `***.***.***-{últimos 2 dígitos}` |
| `src/common/schemas.py` — `CustomerRecord` | Nenhuma validação de formato para `cpf_masked` no schema | Adicionado `CPF_MASKED_PATTERN` + `field_validator` |
| `src/common/schemas.py` — `TransactionEvent.fraud_score` | Semântica não documentada (geração vs. detecção) | Documentado como campo derivado, sempre nulo na origem; travado por teste |
| `docs/testes_step_1.3.txt`, `docs/test_status.md` | Referenciavam segmento "PREMIUM", que nunca existiu no enum `CustomerSegment` | Corrigido para `ALTA_RENDA` |

Fora de escopo: contrato de dados entre producers/schemas (issue #8) e implementação real do cálculo de `fraud_score` no streaming (issue #11).

---

## Issue #8 — Contrato de Dados Ingestion ↔ Transformação (executado em 2026-09-03)

> Checklist completo: `docs/testes_issue_8.txt`

Objetivo: eliminar divergências entre producers Kafka, modelos Pydantic, schemas Avro e Spark.

**Achados da investigação** (antes de qualquer mudança):
1. `MarketTradeEvent` tinha campo `source`, mas o producer de mercado e o Avro (já corretos) sempre mandaram `source_system`+`produced_at` — validar o payload real contra o modelo antigo descartava o valor real (caía no default `"simulator"`) e perdia `produced_at`. Sem impacto em runtime hoje porque nada ainda consome essas mensagens (só passa a importar na issue #11).
2. `TransactionEvent` nem declarava `produced_at`/`source_system` — mesmo problema, mais brando (Pydantic ignora campo extra por padrão).
3. `.env`/`.env.example` documentavam `TRANSACTION_PRODUCER_RATE`/`MARKET_PRODUCER_RATE`, mas o código sempre leu `PRODUCER_RATE_TPS`/`MARKET_PRODUCER_RATE_TPS` — e `docker-compose.yml` já usava os nomes certos (hardcoded nos serviços `producer-transactions`/`producer-market`). Só o `.env.example` estava desalinhado.
4. Nenhum producer validava o payload contra o modelo Pydantic antes de publicar — só `json.dumps` de um dict cru.

**Mudanças**: `MarketTradeEvent.source` → `source_system` + `produced_at` novo; `TransactionEvent` ganhou os mesmos dois campos (opcionais — `None` para eventos de batch, que não passam pelo producer); `TRANSACTION_SPARK_SCHEMA`/`MARKET_TRADE_SPARK_SCHEMA` atualizados; os dois producers agora constroem e validam um `TransactionEvent`/`MarketTradeEvent` de verdade em `_build_message()` antes de serializar (antes só mesclavam dicts); `.env`/`.env.example` corrigidos pros nomes que o código/compose já usam, com `GENERATOR_SEED` documentado.

### 1. Testes — `tests/unit/test_data_contract.py`

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 8-VAL | `TestProducerValidatesPayload` | `_build_message` valida e preserva `produced_at`/`source_system` reais, payload continua JSON válido (4 testes) | OK |
| 8-TX | `TestTransactionEventContract` | Campos do Avro cobertos pelo Pydantic, DDL Spark atualizado, round-trip sem perda, 200 transações do `DataGenerator` real passam na validação (4 testes) | OK |
| 8-MKT | `TestMarketTradeEventContract` | Campos do Avro cobertos, `source` removido/`source_system` presente, DDL Spark atualizado, round-trip sem perda (4 testes) | OK |
| 8-ENV | `TestCanonicalEnvVarNames` | Producers leem os nomes canônicos; `.env.example` e `docker-compose.yml` documentam os mesmos nomes (4 testes) | OK |

**Total: 16/16 testes PASSED.**

### 2. Validação manual contra Kafka real

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 8-MAN-01 | Rodar producer de transações contra Kafka real, consumir 1 mensagem | `produced_at`/`source_system` corretos no payload | OK — `"produced_at": "2026-09-03T20:46:28...", "source_system": "transaction-simulator"` |

Producer de mercado não testado contra Kafka real (só roda em horário de pregão B3, 13h–20h UTC, fora da janela na hora do teste) — coberto pelos testes unitários com `TickSimulator` real (8-VAL-02, 8-MKT-04).

### 3. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 8-SUITE-01 | `pytest tests/unit/` | Sem regressão | OK (172/173 PASSED — 1 falha local pré-existente de `.env`, não relacionada) |
| 8-LIN-01 | `ruff check` | All checks passed! | OK |
| 8-LIN-02 | `mypy` | Success: no issues found | OK |

---

## Issue #10 — Loader Gold para PostgreSQL (executado em 2026-09-03)

> Checklist completo: `docs/testes_issue_10.txt`

Objetivo: disponibilizar `dim_customers`, `dim_date`, `fact_transactions` e
`agg_daily_fraud_metrics` no PostgreSQL da serving layer. Decisão de
arquitetura (confirmada com o usuário): PySpark + JDBC — o loader roda dentro
do cluster Spark (mesmo padrão de `bronze_to_silver.py`/`silver_to_gold.py`),
lendo o Gold via `spark.read.parquet` e escrevendo via
`spark.write.format("jdbc")`. Estratégia de carga: truncate + reload completo
por tabela a cada execução (sempre idempotente; sem upsert real — os volumes
atuais, maior tabela ~500k linhas, não justificam merge via JDBC).

**Arquivos criados**: `src/serving/loaders/gold_to_postgres.py`
(`GoldToPostgresLoader`), `src/serving/loaders/schema.sql` (DDL das 4
tabelas, sem FKs — truncate+reload por tabela dispensa ordenação/CASCADE),
`tests/unit/test_gold_to_postgres.py` (11 testes).

**Arquivos modificados**: `docker/spark/Dockerfile` (driver JDBC
`postgresql-42.7.3.jar` + `psycopg2-binary`), `Makefile` (target
`spark-submit-gold-postgres`), `dags/dag_batch_transformation.py` (task
`load_gold_postgres`, pipeline agora `bronze_to_silver >> silver_to_gold >>
load_gold_postgres >> notify_completion`).

### 1. Testes Unitários — `tests/unit/test_gold_to_postgres.py`

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 10-JDBC | `TestJdbcConfig` | URL JDBC usa host interno (sem credenciais hardcoded); options de escrita incluem `truncate=true`/driver/user/password (2 testes) | OK |
| 10-DDL | `TestEnsureSchema` | DDL idempotente executado e commitado; falha → rollback + exceção repropagada (2 testes) | OK |
| 10-LOAD | `TestLoadTable` | Métricas `rows_read`/`rows_written`; falha de leitura (MinIO) e de escrita (Postgres) logadas e repropagadas (4 testes) | OK |
| 10-ALL | `TestRunAll` | `ensure_schema` chamado 1x; métricas das 4 tabelas presentes (1 teste) | OK |
| 10-SQL | `TestSchemaSQL` | `schema.sql` declara as 4 tabelas; sem foreign keys (2 testes) | OK |

**Total: 11/11 testes PASSED.**

### 2. Bug real encontrado e corrigido — duplicidade em `dim_customers`

A primeira carga real (`make spark-submit-gold-postgres`) falhou com
`duplicate key value violates unique constraint "dim_customers_pkey"` — na
tabela recém-truncada, ou seja, o **Gold Parquet já continha `customer_key`
duplicado**; só passou a ser detectado agora porque é a primeira vez que uma
PK é imposta sobre `dim_customers` (Parquet não tem constraint).

Investigação: 71.000 linhas no Gold para só 10.000 `customer_key` distintos,
com atributos (`name`/`birth_date`/`segment`) **diferentes** entre as
"duplicatas" do mesmo cliente — versões conflitantes, não cópias idênticas.
Causa raiz: `src/ingestion/batch/customer_loader.py` implementa um SCD2
simplificado que grava um snapshot diário e marca `is_current=True` em todas
as linhas, mas **nunca fecha o snapshot anterior**. Como `make seed-data`
rodou em vários dias diferentes ao longo do projeto, cada dia deixou um
snapshot inteiro "corrente" para sempre, e `silver_to_gold.py` só filtrava
`is_current=True` sem deduplicar por `customer_id`.

**Correção**: `_build_dim_customers` (`silver_to_gold.py`) agora deduplica
por `customer_id` mantendo a linha de maior `ingestion_timestamp` (Window +
`row_number`). Reprocessamento confirmou: `rows_read=71000,
rows_discarded=61000, rows_written=10000`. Teste unitário adicionado
(`test_dedups_multiple_current_snapshots_keeping_latest`). A causa raiz de
fundo (`customer_loader.py` não fechar snapshots antigos) não foi alterada —
gap de design do SCD2 simplificado, fora do escopo desta issue; a correção
aplicada garante a invariante que toda dimensão Gold exige (unicidade por
chave), para qualquer consumidor.

### 3. Teste de Integração Real (Docker + PostgreSQL real)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 10-INT-01/04 | `make spark-submit-gold-postgres` (4 tabelas) | Métricas batem com o Gold Parquet | OK (10.000 / 295 / 506.146 / 1.745) |
| 10-INT-05/06 | Tabelas + contagens via `psql` | 4 tabelas, contagens 1:1 com o job | OK |
| 10-IDEM-01/02 | 2ª execução — idempotência | Mesmas métricas; `count(*) == count(distinct <chave>)` nas 4 tabelas | OK — 0 duplicatas |
| 10-DAG-01/02 | DAG sem import errors; `airflow tasks test ... load_gold_postgres` | Task SUCCESS, mesmas métricas | OK |
| 10-IDEM-03 | 3ª execução (via Airflow) — idempotência | 0 duplicatas | OK |

Achado sem impacto real, investigado durante o rebuild da imagem Spark: o
`pip install` puxou `pyspark==3.5.9` via dependência transitiva não fixada de
`delta-spark==3.1.0`, divergindo do `3.5.1` da imagem base
(`apache/spark:3.5.1`). Confirmado que `spark-submit` continua resolvendo
`pyspark` via `$SPARK_HOME/python` (bundled, compatível com os JARs
Scala/JVM) — o `3.5.9` do pip fica órfão em site-packages, nunca importado
pelos jobs.

### 4. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 10-SUITE-01 | `pytest tests/unit/` | Sem regressão | OK (185/186 PASSED — 1 falha local pré-existente de `.env`, não relacionada) |
| 10-LIN-01 | `ruff check` | All checks passed! | OK |
| 10-LIN-02 | `mypy` | Success: no issues found | OK |

---

## Issue #11 — Streaming Spark e Detecção de Fraude (executado em 2026-09-03)

> Checklist completo: `docs/testes_issue_11.txt`

Objetivo: implementar a speed layer da arquitetura Lambda — `StreamProcessor`
consome `raw-transactions` via Spark Structured Streaming, enriquece com
`dim_customers` (Gold) e detecta anomalias de valor via Z-Score numa janela
deslizante de 1h por cliente, publicando `enriched-transactions`/
`fraud-alerts` e persistindo em `s3a://silver/transactions_stream/` (caminho
separado do batch — evita conflito entre overwrite por partição e append
contínuo).

**Decisão de arquitetura** (confirmada com o usuário): janela deslizante real
recalculada a cada micro-batch, não uma baseline estática pré-calculada.
Como `foreachBatch` só entrega as linhas do micro-batch atual, o histórico de
amount/timestamp por cliente é persistido em
`s3a://silver/_stream_state/customer_amount_history/` e recarregado a cada
trigger (leitura→processa→grava), dando o contexto cross-micro-batch
necessário — sem isso, um trigger de 10s quase nunca teria 2+ transações do
mesmo cliente para formar uma baseline.

**Arquivos criados**: `src/transformation/streaming/stream_processor.py`
(`StreamProcessor`), `tests/unit/test_stream_processor.py` (20 testes). O
target `make spark-submit-stream` já existia (scaffolding do step 1.2) e não
precisou de alteração.

### 1. Testes Unitários — `tests/unit/test_stream_processor.py`

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 11-PARSE | `TestParseRawKafkaBatch` | JSON válido parseado; JSON malformado/sem `transaction_id` descartado sem derrubar o batch (3 testes) | OK |
| 11-SCORE | `TestEnrichAndScore` | Histórico insuficiente → z_score nulo; salto de valor → anomalia + fraud_score=1.0; janela de 1h respeitada; **histórico persistido de micro-batches anteriores detecta anomalia numa transação isolada** (ponto central do design); enriquecimento via `dim_customers` (9 testes) | OK |
| 11-HIST | `TestPersistHistory` | Poda de histórico fora da janela; leitura com path ausente retorna vazio sem erro (3 testes) | OK |
| 11-ALERT | `TestBuildFraudAlerts` | Só anomalias incluídas; `fraud_type` reaproveita rótulo de origem ou usa fallback; `alert_reason` menciona Z-Score/limiar; payload valida contra `FraudAlert` (Pydantic, round-trip) (5 testes) | OK |

**Total: 20/20 testes PASSED.**

### 2. Bug real encontrado e corrigido — `schemas.py` incompatível com Python 3.8

A primeira execução real falhou na importação: `ImportError: cannot import
name 'StrEnum' from 'enum'` — `stream_processor.py` é o **primeiro** job
PySpark deste projeto a de fato importar `src.common.schemas` (jobs batch
anteriores usavam DDLs inline). A imagem custom do Spark
(`apache/spark:3.5.1`) roda Python 3.8, mas `schemas.py` usa `from enum
import StrEnum` (stdlib só desde 3.11) e sintaxe `X | None` nos campos
Pydantic (o Pydantic resolve as anotações em runtime para construir
validators — falha em < 3.10 mesmo com `from __future__ import
annotations`). Incompatibilidade latente desde a issue #8, nunca detectada
por falta de um job Spark que importasse o módulo.

**Correção**: shim condicional (`StrEnum` real em 3.11+, subclasse
`str`+`Enum` equivalente abaixo disso) e todos os campos opcionais trocados
de `X | None` para `Optional[X]` (typing) — compatível com 3.8, mesmo
comportamento em runtime (`use_enum_values=True` já armazenava a string, não
a instância do enum). `pyproject.toml` ganhou um `per-file-ignore` de ruff
documentado para essas linhas (as regras `pyupgrade` assumem o alvo 3.11+ do
projeto e sinalizariam o shim como desatualizado). Validado dentro do
container real (Python 3.8.10): import e validação do `TransactionEvent`
funcionando.

### 3. Teste de Integração Real (Docker + Kafka + MinIO)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 11-INT-01/03 | Producer real (10 tps) + stream_processor consumindo backlog e mensagens novas | 0 erros; batch 0 processou 2.407 linhas de backlog; batches seguintes ~10/batch | OK |
| 11-INT-04/06 | `enriched-transactions` recebendo mensagens reais; `silver/transactions_stream/` e `_stream_state/customer_amount_history/` populados | OK — 153 objetos (2.0MiB) no Silver; histórico persistido e sobrescrito a cada batch | OK |
| 11-ANOM-01/02 | Rajada determinística injetada (6 transações normais + 1 de R$50.000 para o mesmo cliente) → `fraud-alerts` | OK — alerta real: `z_score=7056.93`, `fraud_score=1.0`, `fraud_type=MONEY_LAUNDERING` (fallback), `alert_reason` com Z-Score/limiar/janela | OK |
| 11-CKPT-01/04 | `kill -TERM` no job + restart — checkpoint deve resumir sem reprocessar o já commitado | OK — restart retomou do batch 14 (único offset planejado mas não commitado no momento do kill, replay padrão do Spark), `rows=76` — **não** reprocessou as ~5.480 mensagens já commitadas nos batches 0-13 | OK |

Achado sem impacto (não é bug, comportamento esperado dos geradores deste
projeto): os `customer_id` do producer streaming (pool de 1.000, seed
não-determinística por execução) não têm interseção com os 10.000
`customer_id` de `gold/dim_customers` (seed=42 fixo) — o LEFT JOIN de
enriquecimento funciona corretamente, mas nesta rodada retornou null para
`customer_segment`/`risk_score`/`city` em todas as linhas (omitidos do JSON
por padrão pelo `to_json`).

### 4. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 11-SUITE-01 | `pytest tests/unit/` | Sem regressão | OK (205/206 PASSED — 1 falha local pré-existente de `.env`, não relacionada) |
| 11-LIN-01 | `ruff check` | All checks passed! | OK |
| 11-LIN-02 | `mypy` | Success: no issues found | OK |

---

## Issue #15 — API FastAPI para Consultas e Alertas (executado em 2026-09-04)

> Checklist completo: `docs/testes_issue_15.txt`

Objetivo: implementar a serving API referenciada no README/Makefile —
consultas de transações, KPIs de fraude e alertas recentes sobre o Postgres
carregado pela issue #10, com paginação, filtros, validação de entrada e
health/readiness checks.

**Decisões de arquitetura** (confirmadas com o usuário):

1. **Fonte de "alertas recentes"**: `fact_transactions` filtrado por
   `is_fraud=true` (rótulo já carregado pela issue #10), não uma tabela nova
   alimentada pelo tópico Kafka `fraud-alerts` do detector de streaming
   (issue #11) — esse último exigiria escrever e manter um consumidor Kafka
   novo, fora do escopo de "criar rotas" desta issue. Documentado no
   docstring de `routes/alerts.py`.
2. **Containerização**: `make api` (host, `--reload`) já cobre o critério de
   aceite; `docker/api/Dockerfile` + serviço `api` no `docker-compose.yml`
   foram adicionados como alternativa containerizada, atrás de um profile
   dedicado (`docker compose --profile api up -d api`) — não sobe com
   `make up` por padrão, mesmo padrão dos serviços `producer-*`. Resolve o
   Postgres via `POSTGRES_HOST=postgres` (override de ambiente no serviço),
   sem duplicar lógica de conexão com o host (`POSTGRES_HOST=localhost`
   do `.env`).

**Arquivos criados**: `src/serving/api/{db.py, main.py}`,
`src/serving/api/models/schemas.py` (`Page[T]`, `PaginationParams`,
`TransactionOut`, `DailyFraudMetricOut`), `src/serving/api/repository.py`
(SQL parametrizado via `sqlalchemy.text`, sem interpolação de string),
`src/serving/api/routes/{health,transactions,alerts,kpis}.py`,
`docker/api/Dockerfile`, `tests/unit/test_api.py` (29 testes).

### 1. Testes Unitários — `tests/unit/test_api.py`

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 15-REPO | `TestRepository*` | Filtros por cliente/data/status, paginação sem sobreposição, contagem consistente com listagem — contra SQLite in-memory (SQL real, não mocks) (12 testes) | OK |
| 15-HEALTH | `TestHealthRoutes` | `/health/live` nunca toca o banco; `/health/ready` distingue banco disponível (200) de indisponível (503) (3 testes) | OK |
| 15-TX | `TestTransactionRoutes` | Envelope paginado (items/total/page/page_size); página além dos dados retorna vazio sem erro; `page_size` acima do teto → 422; 404 para id inexistente; 503 em falha real de query (9 testes) | OK |
| 15-ALERT / 15-KPI | `TestAlertRoutes` / `TestKpiRoutes` | Só transações fraudulentas; filtro por tipo; 503 em falha de banco (5 testes) | OK |

**Total: 29/29 testes PASSED.**

### 2. Teste de Integração Real (Docker + Postgres real)

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 15-INT-01/09 | `make api` local contra Postgres real (506.146 transações, 1.745 métricas diárias) — `/docs`, `/health/*`, `/transactions` (sem filtro, `is_fraud`, intervalo de data), 404, 422 | Payloads reais corretos em todos os casos | OK |
| 15-INT-10/11 | `/alerts` e `/kpis/fraud-daily` | `total=12697` (alertas) e `total=1745` (KPIs), dados reais | OK |
| 15-FAIL-01/04 | `docker compose stop postgres` com API rodando → `/health/ready` e `/transactions` → `start postgres` de novo | 503 em ambos durante a queda (sem stack trace exposto); recuperou sozinho ao religar (`pool_pre_ping=True`), sem reiniciar a API | OK |
| 15-DOCKER-01/03 | `docker compose --profile api up -d --build api` (não sobe com `make up` puro); endpoints via porta externa 8000 | Build ~103s (imagem lean, sem pyspark/great-expectations); container Healthy; mesmos resultados da versão local, resolveu `POSTGRES_HOST=postgres` corretamente | OK |

### 3. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 15-SUITE-01 | `pytest tests/unit/` | Sem regressão | OK (234/235 PASSED — 1 falha local pré-existente de `.env`, não relacionada) |
| 15-LIN-01 | `ruff check` | All checks passed! | OK |
| 15-LIN-02 | `mypy` | Success: no issues found | OK |

---

## Issue #13 — Quality Gates com Great Expectations (executado em 2026-09-04)

> Checklist completo: `docs/testes_issue_13.txt`

Objetivo: substituir a validação placeholder do Bronze por controles de
qualidade executáveis (Great Expectations) para Bronze, Silver e Gold,
integrados às DAGs com política de falha explícita.

**Decisão de arquitetura** (confirmada com o usuário): **gate simples** —
qualquer expectativa falhando bloqueia a task do Airflow (e portanto a DAG),
sem quarentena de linhas nem execução parcial. "Quarentena" fica como trilha
de auditoria (data docs + logs), não como dataset separado. Recuperação =
corrigir a causa raiz e rerodar a DAG.

**Arquivos criados**: `src/governance/great_expectations/{context,datasets,
suites,checkpoints,runner}.py` (suites via API Python, não YAML/JSON à mão),
`gx/{expectations,checkpoints}/` + `great_expectations.yml` (artefatos
versionados, gerados pela execução dos módulos — `gx/uncommitted/`
gitignored pelo próprio GX), `tests/unit/test_great_expectations.py` (17
testes) e `tests/unit/test_gx_datasets.py` (8 testes, sem dependência do
GX). 8 suites cobrindo `bronze_transactions`, `bronze_market_data`,
`silver_transactions`, `silver_market_data`, `gold_fact_transactions`,
`gold_dim_customers`, `gold_dim_date`, `gold_agg_daily_fraud_metrics`.

**Arquivos modificados**: `dags/dag_batch_ingestion.py` (placeholder →
gate real), `dags/dag_batch_transformation.py` (2 tasks novas:
`validate_silver_data` e `validate_gold_data`), `docker/airflow/Dockerfile`
(`great-expectations==0.18.13` + `s3fs` — primeira vez que algo importa GX
de fato no container), `docs/runbook.md` (seção "Quality Gates" nova).

### 1. Testes Unitários

Nota de ambiente: great-expectations não importa em Python >= 3.13 (camada
`pydantic.v1` interna quebra em runtimes novos — limitação do próprio
Pydantic). `test_great_expectations.py` detecta isso e pula o módulo
inteiro (`pytest.skip(allow_module_level=True)`) na suíte local (Python
3.14); CI usa Python 3.11 e não é afetado. Validado via venv Python 3.10
dedicado e, mais autoritativamente, contra o container real do Airflow
(Python 3.11) — ver seção 2.

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 13-GX | `test_great_expectations.py` | Para cada um dos 8 datasets, fixture válida passa e fixture inválida (violando 1 expectativa conhecida) falha deterministicamente — inclui duplicidade de PK em `gold_dim_customers` (o mesmo tipo de bug real da issue #10) (17 testes) | OK |
| 13-DS | `test_gx_datasets.py` | `_parse_hive_partitions`/`_resolve` — regressão direta de um bug real (seção 2) (8 testes) | OK |

**Total: 25/25 testes novos PASSED.**

### 2. Bugs reais encontrados e corrigidos durante o teste de integração

A primeira execução real (`runner.py --all` contra MinIO real) revelou três
bugs genuínos — nenhum hipotético, todos só surgiram porque essa foi a
primeira vez que algo leu Bronze/Silver/Gold inteiros via pandas (os jobs
Spark existentes toleram essas inconsistências, mascarando-as):

1. **Colisão de schema em `bronze/market_data`**: `pd.read_parquet` no
   diretório falhava (`ArrowTypeError: Unable to merge`) — a coluna `date`
   existe TANTO como partição Hive (nome do diretório) QUANTO dentro do
   arquivo, e o leitor de dataset particionado do pyarrow não concilia as
   duas versões. Corrigido lendo cada arquivo individualmente pelo caminho
   completo (evita a descoberta de partição do pyarrow) e concatenando via
   pandas.
2. **Perda silenciosa de coluna de partição**: a correção acima, sozinha,
   quebrou `gold_fact_transactions`/`gold_agg_daily_fraud_metrics`/
   `silver_market_data` — gravados com `.partitionBy(...)` do Spark, que
   NÃO duplica a coluna de partição dentro do arquivo (convenção padrão),
   então `date_key`/`date` ficavam ausentes do DataFrame. Corrigido com
   `_parse_hive_partitions()`, que reconstrói a coluna a partir do caminho
   do arquivo — só quando ainda não existe (preserva a correção #1).
3. **Comparação timezone-naive vs. aware na checagem de freshness**:
   `silver_transactions` falhava mesmo com dado genuinamente fresco — a
   coluna vem `datetime64[ns]` (sem timezone) do parquet, mas os bounds
   gerados tinham offset UTC explícito, e o GX falha essa comparação sem
   erro explícito. Corrigido gerando bounds sem tzinfo.

Achado adicional (não é bug, é postura de dados): GX habilita telemetria
anônima por padrão — desabilitada explicitamente em `context.py`,
consistente com a postura já estabelecida do projeto sobre dados (issues
#8/#9).

### 3. Teste de Integração Real (Docker) e Critério "DAG Bloqueia"

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 13-INT-01/08 | 8 gates contra dados reais (após as 3 correções) — 164 expectativas no total | 0 falhas em todos os 8 datasets | OK |
| 13-DAG-02/04 | `airflow tasks test` real para `validate_bronze_data`/`validate_silver_data`/`validate_gold_data` | SUCCESS em todos, sem erros de import de DAG | OK |
| 13-BLOCK-01/03 | Backup de `gold/dim_customers/` → injeção de `customer_key` duplicado → `airflow tasks test validate_gold_data` → restauração | Task **FALHOU** de verdade (`QualityGateFailed`, Airflow marcou `UP_FOR_RETRY`, bloqueando `load_gold_postgres` downstream); após restaurar, voltou a passar (17/17) | OK |
| 13-DOCS-01/02 | Data docs (`gx/uncommitted/data_docs/local_site/`) publicados a cada execução, com página por suite e por validação | OK | OK |
| 13-VERS-01 | `gx/expectations/*.json` (8) + `gx/checkpoints/*.yml` (8) versionados em git | OK | OK |

### 4. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 13-SUITE-01 | `pytest tests/unit/` (Python 3.14 local) | Sem regressão | OK (242/244 PASSED, 1 skipped — GX indisponível em Python 3.14, ver nota — 1 falha local pré-existente de `.env`, não relacionada) |
| 13-LIN-01 | `ruff check` | All checks passed! | OK |
| 13-LIN-02 | `mypy` | Success: no issues found | OK |

## Issue #14 — Catálogo de Dados Leve (executado em 2026-09-04)

> Checklist completo: `docs/testes_issue_14.txt`

Objetivo: implementar o catálogo e a linhagem anunciados no README/
architecture.md para os datasets da plataforma.

**Decisão de arquitetura** (confirmada com o usuário): **descopar o
OpenMetadata completo da V1 local**. O stack oficial dele (server + MySQL/
Postgres próprio + Elasticsearch + ingestion-Airflow) soma mais 3-4
serviços pesados aos 19 que já rodam em `docker-compose.yml`, competindo
pelos ~8GB alocados ao Docker neste ambiente. No lugar: um registro
declarativo em Python (`src/governance/data_catalog/`), validado contra a
infra real (MinIO/Postgres/Kafka — não um documento estático) e renderizado
em `docs/data_catalog.md` (tabelas por camada + diagrama de linhagem
Mermaid). Nenhum container novo. Um catálogo gerenciado real (OpenMetadata
ou AWS Glue Data Catalog, já previsto na V2) fica para a fase cloud.

**Arquivos criados**: `src/governance/data_catalog/{registry,validator,
render}.py` (17 `CatalogEntry`: Bronze/Silver/Gold no MinIO, 4 tabelas
Postgres da serving layer, 4 tópicos Kafka, 1 dashboard `status="planejado"`
— issue #16 ainda não existe), `scripts/build_data_catalog.py` (CLI —
`make catalog`), `tests/unit/test_data_catalog.py` (16 testes),
`docs/data_catalog.md` (artefato gerado e versionado).

**Arquivos modificados**: `Makefile` (`seed-openmetadata`, que apontava
para um script inexistente, virou `catalog`), `README.md`/
`docs/architecture.md`/`CLAUDE.md`/`docs/runbook.md` (decisão documentada),
`.env.example` (removidos `OPENMETADATA_*`, nunca ligados a nenhuma classe
de `Settings`).

### 1. Testes Unitários

Só lógica pura (registro, integridade de linhagem, renderização) — sem
depender de MinIO/Postgres/Kafka reais.

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 14-REG | `TestRegistry` | Sem keys duplicadas, toda referência `upstream` resolve, entries de dashboard corretamente marcadas `planejado` (5 testes) | OK |
| 14-VAL | `TestValidateReferences` | Catálogo real sem referência quebrada; referência quebrada sintética é detectada (2 testes) | OK |
| 14-REN | `TestRenderMarkdown` | Tabelas por camada, diagrama Mermaid, status ✅/❌/planejado/não-validado, termos de glossário são subconjunto do já documentado em `docs/data_dictionary.md` (7 testes) | OK |

**Total: 16/16 testes novos PASSED.**

### 2. Teste de Integração Real (Docker)

Achado real durante a validação: `.env` local do usuário tem
`MINIO_ENDPOINT=http://localhost:9002`, divergente do mapeamento real do
container (`docker port minio` confirmou só 9000-9001 expostos, nada
escutando em 9002) e do default em `.env.example`. Perguntado ao usuário —
confirmou que é proposital, não corrigido (fora do escopo da issue). O
validador tratou isso corretamente: capturou a exceção por asset, reportou
`False` com o detalhe, e `--strict` saiu com código 1 — mesmo
desalinhamento que já explica a falha pré-existente e não relacionada em
`test_environment_consistency.py` (ver issue #13 e seção 4 abaixo).

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 14-INT-01/03 | MinIO (8 prefixos) + Postgres (4 tabelas) + Kafka (4 tópicos), com o endpoint MinIO correto (override de processo, sem tocar no `.env` do usuário) | Todos os 16 assets não-dashboard existem na infra real | OK — `docs/data_catalog.md`: 16x "✅ ok", 1x "🗓️ planejado", 0x "❌" |
| 14-INT-04 | `--strict` com endpoint MinIO errado (achado real) vs. correto | Falha real (exit 1) com porta errada; sucesso real (exit 0) com porta certa | OK — provado nos dois sentidos |

### 3. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 14-SUITE-01 | `pytest tests/unit/ --cov=src` (Python 3.14 local) | Sem regressão | OK (258/259 PASSED, 1 skipped — GX indisponível em Python 3.14, issue #13 — 1 falha local pré-existente de `.env`, não relacionada, ver seção 2) |
| 14-COV-01 | Cobertura total | ≥70% | OK (75,87%) |
| 14-LIN-01 | `ruff check .` | All checks passed! | OK |
| 14-LIN-02 | `mypy src/ scripts/` | Success: no issues found | OK |

## Issue #16 — Dashboards Operacionais/Analíticos com Superset (executado em 2026-09-05)

> Checklist completo: `docs/testes_issue_16.txt`

Objetivo: disponibilizar visualizações úteis sobre transações e fraude a
partir da camada Gold/serving layer (Postgres).

**Decisão de arquitetura** (confirmada com o usuário): **só Superset**,
Grafana descoped da V1 local — já roda no `docker-compose.yml`, conectado
ao mesmo Postgres da serving layer (issue #10), cobre 100% dos KPIs
pedidos sem novo container/datasource, e não há store de séries temporais
(Prometheus etc.) que justifique Grafana para métricas real-time neste
ambiente. `dashboards/grafana/` fica como scaffold não usado.

**Bug real encontrado e corrigido**: o serviço `superset` do
`docker-compose.yml` nunca subiu corretamente desde o commit inicial —
indentação mais funda que a linha-mãe no bloco `command: >` quebra o
folding do YAML, inserindo uma quebra de linha literal no meio do
`create-admin` (rodava sem nenhuma flag, caindo num prompt interativo
travado) e do `gunicorn` (rodava sem o módulo da app —
`Error: No application module specified.`, healthcheck perpetuamente
`starting`). Confirmado via `docker compose config --format json` +
`docker compose logs`; corrigido colocando cada comando numa única linha
lógica, sem mudar nenhuma flag/valor.

**Achado real de dados**: `fraud_score` está sempre `NULL` nas 506.146
linhas de `fact_transactions` — só é populado pelo detector de streaming
(issue #11), que não escreve na camada Gold batch/Postgres. Mesma causa
raiz do gap já documentado em `alerts.py` (issue #15). Por isso: sem chart
de distribuição de `fraud_score`, e o KPI de "latência" pedido na issue
também documentado como fora do escopo pela mesma razão — os KPIs de
fraude realmente disponíveis (`is_fraud`, `fraud_type`,
`fraud_count`/`fraud_rate`) são os usados no dashboard.

**Arquivos criados**: `src/serving/dashboards/{client,charts,
provision}.py` (cliente REST + registro declarativo de 7 charts +
orquestração idempotente), `scripts/provision_superset_dashboards.py`
(`make dashboards`), `tests/unit/test_dashboards.py` (14 testes),
`dashboards/superset/dashboard_configs/` (snapshot exportado do Superset
real via `make dashboards-export`).

**Arquivos modificados**: `docker-compose.yml` (fix do bug acima),
`src/common/config.py` (`SupersetSettings`), `Makefile` (`dashboards`,
`dashboards-export`), README.md/`docs/architecture.md`/`docs/runbook.md`/
`CLAUDE.md`/`.env.example` (decisão documentada, comandos, credenciais via
env).

### 1. Testes Unitários

Só lógica pura (registro de charts, construção de position_json/native
filters, paginação do client) — sem depender de um Superset real rodando.

| ID | Classe | Descrição | Status |
|----|--------|-----------|--------|
| 16-CHT | `TestChartsRegistry` | Sem `slice_name` duplicado, todo `dataset_table` conhecido, todo chart tem metric(s), `DATASETS` bate com o schema real (4 testes) | OK |
| 16-POS | `TestBuildPositionJson` | Layout em linhas (4+2+1 para 7 charts), `CHART-{id}` referencia o id correto, `ROOT_ID`/`GRID_ID` presentes (4 testes) | OK |
| 16-FLT | `TestBuildNativeFilters` | 2 filtros nativos (Período em `date_key`, Tipo de Transação), miram os datasets corretos (3 testes) | OK |
| 16-CLI | `TestSupersetClientFindOne` | Paginação de busca por nome (achar na 1ª página, não achar, paginar até achar), `session.get` mockado (3 testes) | OK |

**Total: 14/14 testes novos PASSED.**

### 2. Teste de Integração Real (Docker)

Antes de codificar `charts.py`, cada query foi validada via
`/api/v1/chart/data` do Superset e cruzada contra `psql` direto: contagem
total (506.146 = 506.146), taxa de fraude ponderada (2,5085...% idêntico
nos dois), contagem de alertas (`is_fraud=true`: 12.697 = 12.697),
distribuição por `fraud_type` (soma das 5 categorias = 12.697), volume por
`transaction_type` (soma das 6 categorias = 506.146), série temporal
diária (295 dias, `granularity=date_key`).

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 16-PROV-01/06 | `make dashboards --verify --strict`: 1 database + 2 datasets + 1 dashboard + 7 charts provisionados via API REST, `--verify` refazendo cada query real | Todos os 7 charts retornam dado real (rowcount ≥ 1), exit 0 | OK |
| 16-IDEM-01/02 | 2ª execução do mesmo comando | 0 objetos criados (tudo achado e atualizado), contagem final sem duplicatas (1/2/1/7) | OK |
| 16-EXP-01/03 | `make dashboards-export`: export nativo do Superset copiado para `dashboards/superset/dashboard_configs/` | Bundle YAML gerado, senha do Postgres mascarada (`XXXXXXXXXX`), diretório temporário limpo | OK |

### 3. Validação Visual (Checklist Manual)

Critério de aceite "smoke test ou checklist reproduzível de validação
visual" — a parte de renderização/interação de filtros não é automatizável
via `curl`; checklist em `docs/testes_issue_16.txt` seção 4 (abrir
`http://localhost:8088/superset/dashboard/fraude-transacoes-visao-geral/`,
confirmar os 7 gráficos renderizando e os filtros nativos atualizando o
dashboard).

### 4. Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 16-SUITE-01 | `pytest tests/unit/ --cov=src` (Python 3.14 local) | Sem regressão | OK (272/273 PASSED, 1 skipped — GX indisponível em Python 3.14, issue #13 — 1 falha local pré-existente de `.env`, não relacionada, ver issues #13/#14) |
| 16-COV-01 | Cobertura total | ≥70% | OK (73,26%) |
| 16-LIN-01 | `ruff check .` | All checks passed! | OK |
| 16-LIN-02 | `mypy src/ scripts/` | Success: no issues found | OK |

## Issue #18 — Atualizar Documentação para Refletir o Estado Implementado (executado em 2026-09-07)

> Checklist completo: `docs/testes_issue_18.txt`

Objetivo: alinhar README.md, `docs/architecture.md`, `docs/runbook.md`,
`docs/test_status.md` e `CLAUDE.md` com o que existe de verdade, separando
planejado/implementado/validado. Diferente das issues anteriores, sem
pipeline/serviço novo — o "teste" é uma auditoria mecânica: cruzar cada
`make X`/caminho/porta citado contra o Makefile, `docker-compose.yml`, as
DAGs e o código-fonte reais.

**Auditoria mecânica**: todo `make X` citado nos 4 docs corresponde a um
target real (25 targets, incluindo o wildcard `logs-%`); todo caminho de
arquivo citado em backticks existe; toda porta/URL bate com
`docker-compose.yml`.

**9 achados reais corrigidos** (+ 1 achado correlato fora dos 5 arquivos
nomeados, mesma causa raiz; + 1 achado no próprio Makefile durante a
auditoria):

1. `docs/architecture.md` dizia que o loader Gold→PostgreSQL "ainda não
   existe (issue #10)" — issue #10 está mergeada há várias issues.
2. `docs/runbook.md` dizia "issue #16 ainda não existe" na seção do
   catálogo (escrito durante a issue #14, antes do dashboard existir).
3. `CLAUDE.md` "Implementation Phases" apontava `current: feature/step_1.2`
   (branch de semanas atrás) e nenhuma fase tinha status — adicionado
   ✅/🚧/⬜ por fase; Fase 3 ainda listava "Delta Lake" como entregável,
   mas foi explicitamente removido/nunca usado (issue #9) — corrigido.
4. Diagramas de streaming (README, CLAUDE.md, architecture.md) mostravam
   só "MinIO Silver + Kafka fraud-alerts" — conferido contra
   `stream_processor.py::_process_batch`: grava em **três** lugares
   (`silver/transactions_stream/`, `enriched-transactions`,
   `fraud-alerts`) — os 3 diagramas corrigidos.
5. README citava `anomaly_detector.py` como producer de `fraud-alerts` —
   esse arquivo nunca existiu; a lógica real está em `stream_processor.py`.
6. README listava "PostgreSQL + DuckDB" na serving layer — `duckdb` é
   dependência solta em `pyproject.toml`, nunca importada em `src/`
   (confirmado via grep) — corrigido para descrever a realidade.
7. README "Comandos Make Disponíveis" listava 12 dos 25 targets reais do
   Makefile (faltavam `install`, `format`, `test-cov`,
   `test-integration`, `spark-submit-silver-gold`,
   `spark-submit-gold-postgres`, `producer-transactions`,
   `producer-market`, `api`, `dashboards`, `dashboards-export`, `ps`,
   `logs`, `clean-data`) — regenerada com todos os 25.
8. README "Início Rápido" parava no streaming (passo 7) — nunca chegava
   no loader Postgres, na API ou nos dashboards, apesar de todos serem
   caminhos funcionais hoje — estendido para 10 passos.
9. Achado correlato: `src/governance/data_catalog/registry.py` ainda
   marcava o dashboard como `status="planejado"`/`location=""` — issue
   #16 já o implementou; corrigido, e `docs/data_catalog.md` regenerado
   contra a infra real (16x "✅ ok", 0x "planejado", era 1x antes).
10. Achado no Makefile (não um dos 5 arquivos, mas causa raiz do erro no
    README/CLAUDE.md): `spark-submit-batch`'s comentário de ajuda dizia
    "Bronze → Silver → Gold", mas `BATCH_JOB` aponta só para
    `bronze_to_silver.py` — Gold é sempre um comando separado
    (`spark-submit-silver-gold`). Corrigido na fonte.

**Testes afetados**: a correção do item 9 quebrou 2 dos 16 testes de
`test_data_catalog.py`, que assumiam `status="planejado"` como fato
permanente do catálogo real — reescritos para testar a via de
renderização "planejado" com uma entry sintética (`dataclasses.replace`),
mesmo padrão já usado por `test_detects_broken_upstream_reference`.
Resultado: 16/16 PASSED (4 testes reescritos, mesma contagem total).

### Suíte Completa e Lint

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 18-SUITE-01 | `pytest tests/unit/ --cov=src` (Python 3.14 local) | Sem regressão | OK (272/273 PASSED, 1 skipped — GX indisponível em Python 3.14, issue #13 — 1 falha local pré-existente de `.env`, não relacionada) |
| 18-COV-01 | Cobertura total | ≥70% | OK (73,21%) |
| 18-LIN-01 | `ruff check .` | All checks passed! | OK |
| 18-LIN-02 | `mypy src/ scripts/` | Success: no issues found | OK |

---

## Validação End-to-End da Stack Completa (executado em 2026-09-07)

> Checklist completo: `docs/testes_e2e_validation.txt`
> PR: [#33](https://github.com/gpgomes/data-master-std-fraud/pull/33) — não é uma issue numerada; validação ad-hoc pedida entre sessões, depois da issue #18.

Objetivo: rodar a plataforma inteira junto, com dados reais de ponta a
ponta — infra → seed → batch (Bronze→Silver→Gold→Postgres) → streaming →
API → quality gates → catálogo → dashboards — em vez de validar cada
componente isolado, como as issues anteriores fizeram. Todas as issues
#7–#16/#18 já estavam mergeadas antes desta rodada; #17 (Terraform) segue
como a única aberta.

**Dados usados**: `make seed-data` gerou 500.000 transações (2,51%
fraude), 10.000 clientes, 1.000 cotações — mesma seed/volume documentado
no step 1.3.

**2 bugs reais de infraestrutura encontrados e corrigidos** (únicos que
exigiram mudança de código; o resto do achados abaixo são limitações de
ambiente, não bugs):

1. `docker-compose.yml` — healthcheck do MinIO usava `curl`, ausente na
   imagem `minio/minio:RELEASE.2024-03-30T09-41-56Z` (`exec: "curl":
   executable file not found in $PATH`, confirmado via
   `docker inspect minio --format='{{json .State.Health}}'`). O
   healthcheck falhava sempre, deixando o container permanentemente
   `unhealthy` e bloqueando `make up` (serviços com
   `depends_on: condition: service_healthy` nunca sobem). Corrigido para
   `mc ready local` — `mc` já vem embutido na imagem e resolve a
   instância local sem precisar de `mc alias set` antes (confirmado com
   `docker exec minio mc ready local` → `The cluster is ready`, exit 0).
2. `Makefile` — `.PHONY` não tinha sido atualizado desde a issue #7,
   faltando praticamente todos os alvos adicionados depois (`catalog`,
   `dashboards`, `dashboards-export`, `test-unit`, `test-integration`,
   `test-cov`, `install`, `spark-submit-silver-gold`,
   `spark-submit-gold-postgres`, `producer-transactions`,
   `producer-market`, `api`, `clean-data`). Isso quebrava
   silenciosamente `make dashboards` especificamente: como existe um
   **diretório real** `dashboards/` no repo, o Make tratava o alvo como
   já satisfeito e imprimia `make: 'dashboards' is up to date` sem rodar
   a recipe — o Superset nunca era provisionado. Corrigido incluindo
   todos os 25 alvos reais no `.PHONY`.

**Limitações de ambiente encontradas, sem correção de código** (causa
raiz já documentada em runs anteriores, não é regressão):

3. `ingest_market_data` (Airflow) não retornou dados reais — o coletor
   chama yfinance para o dia corrente, e não existem cotações reais para
   2026 (data do sandbox). Mesma causa raiz do step 1.6
   (`docs/test_status.md`, nota da seção Step 1.6). `validate_bronze_data`
   (GX gate) falhou como consequência direta — `FileNotFoundError:
   Nenhum arquivo Parquet encontrado em s3://bronze/market_data/` — e
   isso é o comportamento correto do gate (falhar alto quando não há
   dado nenhum), não um bug a suavizar.
4. `ensure_suites()` (GX) reescreve `bronze_transactions.json`/
   `silver_transactions.json` com uma nova janela de freshness relativa
   a "agora" toda vez que um gate roda — já documentado em
   `docs/runbook.md` ("Adicionar/alterar uma expectativa": "esses são
   saída, sobrescritos a cada run"). Confirmado nesta rodada: rodar os
   gates localmente deixa esses 2 arquivos como `modified` no
   `git status`, mesmo sem nenhuma mudança de regra de negócio — comitado
   à parte em `037d68f`, fora do escopo do PR #33.

### Pipeline batch, ponta a ponta

| ID | Etapa | Resultado |
|----|-------|-----------|
| E2E-BATCH-01 | Bronze→Silver, `transactions` | 500.000 lidas, 0 descartadas, 500.000 gravadas |
| E2E-BATCH-02 | Bronze→Silver, `customers` | 10.000 lidas, 0 descartadas, 10.000 gravadas |
| E2E-BATCH-03 | Silver→Gold, `dim_customers` | 10.000 linhas |
| E2E-BATCH-04 | Silver→Gold, `dim_date` | 182 linhas |
| E2E-BATCH-05 | Silver→Gold, `fact_transactions` | 500.000 linhas |
| E2E-BATCH-06 | Silver→Gold, `agg_daily_fraud_metrics` | 1.089 grupos (data × tipo) |
| E2E-BATCH-07 | Gold→Postgres, as 4 tabelas | `rows_read == rows_written` nas 4, truncate+reload OK |

### Streaming, API, governança e dashboards

| ID | Componente | Resultado |
|----|-----------|-----------|
| E2E-STR-01 | Producer (`kafka_producer_transactions`, 20 tps) + `stream_processor.py` | 8+ micro-batches processados sem erro; `enriched-transactions` recebendo todas as linhas scored (`is_anomaly` calculado); `silver/transactions_stream/` gravando Parquet a cada trigger; checkpoints em `checkpoints/stream_processor/` |
| E2E-STR-02 | Anomalias no Z-Score | 0 detectadas — esperado: janela é por `customer_id`, e com 10k clientes e baixo throughput cada cliente acumula pouco histórico na janela; não é falha da lógica (já coberta isoladamente pelos testes unitários da issue #11) |
| E2E-API-01 | `GET /health/live`, `/health/ready` | `{"status":"ok"}` / `{"status":"ready"}` |
| E2E-API-02 | `GET /transactions`, `/kpis/fraud-daily`, `/alerts` | Dados reais do Postgres recém-carregado; `fraud_score` corretamente `null` (issue #12/#16 — campo derivado do streaming, que não escreve no Gold batch) |
| E2E-GX-01 | 6 quality gates (`bronze_transactions`, `silver_transactions`, `gold_fact_transactions`, `gold_dim_customers`, `gold_dim_date`, `gold_agg_daily_fraud_metrics`) | 6/6 passaram, 0 de ~130 expectations falhando, contra dados reais (não fixtures) |
| E2E-CAT-01 | `make catalog --strict` | 17 datasets catalogados, 0 referências de linhagem inválidas; sinalizou corretamente os 2 assets de `market_data` sem dados reais (achado 3 acima) — saída não commitada (reflete só o estado transitório desta sessão) |
| E2E-DASH-01 | `make dashboards --verify --strict` (só depois do fix do `.PHONY`) | 2 datasets, 7 charts, 0 falhas de verificação contra Postgres real (ex.: Volume=500000, Taxa de Fraude=2,5078%, Alertas=12539) |

### Resultado

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| E2E-CMP-01 | `docker compose config --quiet` (com o fix do MinIO) | Válido, MinIO `healthy` | OK |
| E2E-LIN-01 | `make lint` | ruff + mypy sem erros | OK |
| E2E-FIX-01 | PR #33 mergeado, branch deletada | `fix/docker-makefile-e2e-validation` → `main` | OK |


---

## Validação Manual V1 — checklist da banca (executado em 2026-09-23)

Roteiro: planilha de validação manual (13 áreas). Ambiente reiniciado do zero (`docker compose down -v` + `make up` + `make setup` + `make seed-data`). Os comandos longos/interativos foram executados manualmente; cada resultado foi conferido por leitura direta (Kafka, MinIO, Postgres, Spark master, Airflow, Superset) e as falhas foram diagnosticadas e corrigidas.

### Resultado por área

| Área | Resultado |
|------|-----------|
| 1. Infraestrutura | OK (8/8): 11 containers healthy, 4 buckets, 4 tópicos, 3 DAGs sem import errors, Spark master com 2 workers |
| 2. Ingestão batch | OK (4/4); `bronze/market_data` vazio (Yahoo 429), tratado como opcional |
| 3. Ingestão streaming | OK (3/3) |
| 4. Transformação batch | OK (5/5): Silver 500.000 transações e 10.000 clientes, Gold com 4 tabelas |
| 5. Streaming e fraude | OK (5/5), 5.5 com ressalva at-least-once (issue #36) |
| 6. Orquestração | OK (4/4): as 6 tasks da `batch_transformation_pipeline` em success |
| 7. Great Expectations | OK (4/4): 6 datasets passam, 2 de mercado em warning; falha injetada (`customer_key` duplicado) barrou o pipeline e a restauração o liberou |
| 8. Catálogo | OK (4/4): 17 datasets, 0 referências inválidas; 14 ✅ + 2 ⚠️ (mercado) + 1 não validado (dashboard) |
| 9. Serving | OK (8/8): Postgres com 500.000 transações; API coerente com o Postgres |
| 10. Dashboards | OK (8/8): 7 charts, KPIs iguais ao Postgres (500.000; R$ 933.479.448,51; 2,5078%; 12.539) |
| 11. CI e qualidade | OK (5/5): lint limpo; 336 testes unitários (82,68%); 8 de integração; as PRs #39 e #40 passaram Lint e Unit tests no GitHub Actions |
| 12. Segurança | OK (4/4): `.env` fora do git e do histórico, sem chaves reais, SQL da API parametrizado (injeção testada) |
| 13. Documentação | OK (10/10): itens revisados e a rodada limpa do zero (13.10) concluída; ver abaixo |

### Bugs encontrados e corrigidos

| ID | Área | Problema | Causa raiz | Correção |
|----|------|----------|------------|----------|
| V1-BUG-01 | 2 | `validate_bronze_data` derrubava a DAG de ingestão | Yahoo Finance devolve HTTP 429; `bronze/market_data` fica sem Parquet (diagnóstico anterior "relógio em 2026" estava errado) | Gates de mercado opcionais (`OPTIONAL_DATASETS`, `run_gate_optional` em `runner.py`) |
| V1-BUG-02 | 4 | `bronze_to_silver` quebrava com `PATH_NOT_FOUND` e nunca processava clientes | `transform_market_data` lia um prefixo inexistente sem tratar | Etapa pulada com warning e métricas zeradas |
| V1-BUG-03 | 6 | `validate_silver_data` falhava (sem Parquet de mercado no Silver) | Consequência do V1-BUG-02 | Mesmo tratamento opcional no gate do Silver; scheduler reiniciado (tasks são forks do scheduler) |
| V1-BUG-04 | 4 | Executor Spark morto (`exit code 137`, `ExecutorLostFailure`) | VM do Docker Desktop com 7,75 GB (OOM killer) | Docker Desktop com 12 GB; README e runbook atualizados |
| V1-BUG-05 | 5 | `fraud_score` sempre nulo, `fraud-alerts` vazio | Simulador com timestamps aleatórios em 180 dias vs janela de 1h em tempo de evento | Producer carimba `timestamp=agora` (teste incluído) |
| V1-BUG-06 | 5 | 62 mensagens duplicadas em `enriched-transactions` e 5 em `fraud-alerts` após reiniciar o streaming | At-least-once do `foreachBatch` → Kafka (`alert_id` é `uuid()`) | Corrigido na issue #36: replay idempotente por etapa (Parquet por `query_id/batch_id`, marcadores de progresso, `alert_id` determinístico). Provado ao vivo: 4 `kill -9` no meio de batches, 0 duplicatas no Parquet, no `enriched-transactions` (3.098 msgs) e no `fraud-alerts` (244) |
| V1-BUG-07 | 7 | `runner --all` (item 7.1) falhava só pelos gates de mercado | Não distinguia gates opcionais | `--all` sai com 0 se só os opcionais falharem |
| V1-BUG-08 | 8 | `make catalog` (`--strict`) saía com código 1 | Assets de mercado ausentes tratados como obrigatórios | Campo `optional` no registro; ⚠️ "sem dados (opcional)" |
| V1-BUG-09 | 10 | Dashboard Superset criado como rascunho ("Draft") | Provisionamento nunca publicava | `published: true` no `finalize_dashboard` (teste incluído) |
| V1-BUG-10 | 11 | Testes de integração deixavam linhas falsas em `bronze/transactions/` e criariam `bronze/market_data/` | Escreviam no lago real sem limpeza | Bucket temporário por execução, apagado ao final; `bronze/` comprovadamente idêntico antes/depois |
| V1-BUG-11 | 12 | `.env.example` com 14 variáveis sem uso (incl. Delta Lake, descartado na issue #9) | Resíduo de fases anteriores | Removidas; notas sobre chaves fixas do compose e bloco AWS reservado |
| V1-BUG-12 | 13 | Dependência `duckdb` nunca importada | Resíduo do plano original | Removida do `pyproject.toml` |

### Rodada limpa do zero (13.10, 2026-09-23)

Feita no `main` já com as correções das PRs #33 a #40: `docker compose down -v`, `data/sample/` apagado, `.env` regenerado a partir do `.env.example` e nenhuma DAG despausada. Cada resultado abaixo foi comparado com o esperado.

| Etapa | Resultado |
|-------|-----------|
| `make up` + `make setup` + `make seed-data` | 12 containers healthy em 42 s, 4 buckets, 4 tópicos, 500.000 transações (12.539 fraudes), 10.000 clientes |
| Airflow: `batch_ingestion_pipeline` | 6 tasks em success (~40 s); `bronze/market_data` vazio (Yahoo 429), gate opcional só avisa |
| Airflow: `batch_transformation_pipeline` | 7 tasks em success (~3 min) incluindo `load_stream_postgres`, que pulou a carga sem falhar (streaming ainda não tinha rodado) |
| `bronze_to_silver` | 500.000 lidas = 500.000 gravadas, 0 descartadas; clientes 10.000 |
| Postgres | `fact_transactions` 500.000 (12.539 fraudes, 2,5078%), `dim_customers` 10.000, `dim_date` 182 |
| Streaming (~4,5 min) + `make spark-submit-stream-postgres` | 1.997 transações pontuadas (531 com `fraud_score`), 137 alertas, latência média 5,28 s; 0 duplicatas no Parquet, no `enriched-transactions` e no `fraud-alerts` |
| `alert_id` Kafka x Postgres | 137 = 137, 0 diferenças |
| `runner --all` (GX) | exit 0: 6 gates OK, 130 expectativas, 0 falhas |
| `make catalog` | exit 0: 20 datasets (17 ✅, 2 ⚠️ de mercado, 1 dashboard não validado) |
| `make dashboards` | exit 0: 11 charts, 0 verificações falhadas; valores iguais ao Postgres (500.000; R$ 933.479.448,51; 2,5078%; 12.539; latência 5,2828; 137 alertas) |
| API | `/health/ready` ready; `/transactions` 500.000; `/alerts` 137; `/kpis/fraud-daily` 1.087; `/transactions?is_fraud=true` 12.539 |
| `make lint`, `make test-unit`, `make test-integration` | limpo; 336 passed (82,68%); 8 passed com `bronze/` idêntico antes e depois |

Nenhum bug novo apareceu nesta rodada.

Achado: na sessão anterior o `bronze/transactions` tinha 197 partições e o `bronze_to_silver` descartava 42.637 linhas; do zero são 181 partições e 0 descartes. A diferença é atribuível a partições antigas em `data/sample/`, que `make seed-data` não limpa (só `make clean` limpa). Em um clone novo isso não acontece.

### Achados sem correção de código (registrados)

- **Issue #36:** deduplicação do reprocessamento de micro-batch do streaming (resolvida em branch próprio; janela residual at-least-once nos tópicos Kafka documentada).
- **Issue #37:** gerar imagem (SVG) da linhagem do catálogo ao final da validação.
- **Issue #38:** persistir `fraud_score`/alertas do streaming no Postgres e expor na API/Superset (gap de score e latência).
- O run `scheduled__...` que o Airflow cria ao despausar uma DAG (catchup do último intervalo) roda além do manual; com `max_active_runs=1`, o manual espera.
- A paginação da API é por `page`/`page_size` (não existe `limit`).
- Os data docs do GX (`uncommitted/`) não existem num clone novo: só após rodar um gate.
- O streaming ocupa todo o cluster Spark: pare-o antes de rodar batch.

---

## Issue #38 — Serving do streaming: `fraud_score` e alertas no Postgres, API e Superset (executado em 2026-09-23)

Fecha o gap de "score" e de "latência" da V1: a saída do detector de streaming passa a ser consultável na serving layer.

**Decisão de arquitetura:** loader batch `silver/transactions_stream` → Postgres (`stream_to_postgres.py`, truncate + reload), e não um consumidor Kafka, porque o streaming já ocupa os 4 cores do cluster local. Os alertas são reconstruídos com `build_fraud_alerts` (a mesma função do detector), então o `alert_id` é idêntico ao do tópico Kafka.

### Testes automatizados

| ID | Descrição | Resultado |
|----|-----------|-----------|
| 38-LDR | `tests/unit/test_stream_to_postgres.py` (14): latência e bucket, colunas sensíveis fora, dedup por `transaction_id` entre `query_id`, alertas só de anomalias com `alert_id` determinístico e `processed_at` da linha, carga pulada sem dado, colunas iguais ao DDL | OK |
| 38-API | `tests/unit/test_api.py`: `/alerts` lê `fraud_alerts` (não o rótulo), ordenação, filtros por cliente e data (fim inclusivo), tabela vazia, injeção SQL, 422 e 503 | OK (37 no arquivo) |
| 38-SUP | `tests/unit/test_dashboards.py` (21): 4 datasets, charts de streaming opcionais, layout com 11 charts, verificação opcional sem dado | OK |
| 38-CAT | `tests/unit/test_data_catalog.py`: assets opcionais incluem `silver_transactions_stream` | OK |

### Validação ao vivo (MinIO, Kafka, Postgres, Airflow e Superset reais)

| ID | Descrição | Resultado |
|----|-----------|-----------|
| 38-INT-01 | `make spark-submit-stream-postgres` | Exit 0: 3.098 transações pontuadas (1.319 com `fraud_score`), 244 alertas |
| 38-INT-02 | `alert_id` do Postgres vs tópico `fraud-alerts` | 244 = 244, 0 diferenças |
| 38-INT-03 | Latência evento→processamento | média 7,85 s (mín 0,25 s, máx 40,99 s, com reinícios do teste do #36) |
| 38-INT-04 | `GET /alerts` na API viva | 244 alertas com `z_score`, `fraud_score` e `alert_reason`; filtros por cliente e data; `/transactions?is_fraud=true` segue com 12.539 |
| 38-INT-05 | `make dashboards` | Exit 0: 11 charts, 0 verificações falhadas; latência média 7,8498, 244 alertas e distribuição de score iguais ao Postgres |
| 38-INT-06 | `airflow dags test batch_transformation_pipeline` | 7 tasks em success, incluindo `load_stream_postgres` |
| 38-INT-07 | `make catalog` | Exit 0: 20 datasets (17 ✅, 2 ⚠️ de mercado, 1 dashboard não validado) |
| 38-INT-08 | `make dashboards-export` | Snapshot com 11 charts e 4 datasets |

Nota: os charts criados por API não guardam `query_context`, então `GET /api/v1/chart/{id}/data/` devolve 400 ("Chart has no query context saved") para qualquer chart do dashboard, novo ou antigo. A conferência dos valores usa `POST /api/v1/chart/data` com a mesma query do provisionador.

---

## Issue #37 — Imagem SVG da linhagem do catálogo (executado em 2026-09-23)

Decisão de ferramenta: gerador em **Python puro** (`src/governance/data_catalog/lineage_image.py`), sem Graphviz nem mermaid-cli. Nenhum dos dois estava instalado (o mermaid-cli ainda exigiria baixar um Chromium), e uma dependência de sistema quebraria o "roda igual num clone novo e no CI". A saída é determinística (sem data/hora), então o git só muda quando o catálogo muda.

| ID | Descrição | Resultado |
|----|-----------|-----------|
| 37-GEN | `tests/unit/test_lineage_image.py` (15): XML válido com título/descrição acessíveis, todos os 20 nós e todas as 19 dependências desenhados, opcionais tracejados, saída determinística, nó novo sem dica de layout, upstream desconhecido ignorado, escape de XML, nota nos nós sem ligação, faixa de streaming abaixo do batch, sem nós sobrepostos | OK |
| 37-SYNC | `test_versioned_image_matches_the_catalog`: a imagem versionada é igual à gerada agora do registro (falha no CI se esquecerem de rodar `make catalog`) | OK |
| 37-MD | `render_markdown` embute a imagem acima do bloco Mermaid; `make catalog` grava o SVG junto do Markdown | OK |
| 37-INT | `make catalog` na infra real: exit 0, gera `docs/images/data_lineage.svg` (17,9 KB) | OK |
| 37-VIS | Conferência visual do render (SVG convertido para PNG): colunas Bronze a Dashboard, faixa Batch e faixa Streaming, opcionais tracejados, legenda | OK |

Falha visual encontrada e corrigida durante a conferência: (1) a faixa "Streaming" cobria os nós de Market Data do batch porque o dashboard, que descende do Kafka, entrava no cálculo da faixa; (2) a seta do Silver do streaming para a tabela `fraud_alerts` passava por trás do nó Kafka `fraud-alerts`, parecendo que o Kafka alimentava o Postgres. Ambos corrigidos, com testes de layout.

Achado sobre o catálogo: o tópico `raw-market-data` aparece isolado porque **ninguém o consome** na V1 (o Spark Streaming lê só `raw-transactions`, e o Bronze de mercado vem do yfinance). Não é um bug do registro: a descrição do asset agora diz isso e a imagem anota "sem consumidor no catálogo".
