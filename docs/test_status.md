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
| 1.3-2.3 | Verificar conteúdo do `customers.csv` (100 linhas) | 100 registros com todas as colunas; `country="BR"`, `segment` em {VAREJO, PREMIUM, PRIVATE}, `risk_score` em [0, 100] | OK |
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

> Checklist completo: `docs/step_1.5_test_checklist.txt`

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
