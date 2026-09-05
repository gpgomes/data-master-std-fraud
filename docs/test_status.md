# Status dos Testes — Financial Fraud Detection Platform

> Tabela incrementada ao longo do desenvolvimento. Status atualizado conforme os testes são executados.

## Legenda

| Status | Significado |
|--------|-------------|
| NOK | Não executado / Falhou |
| OK | Passou |
| N/A | Não aplicável |

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
