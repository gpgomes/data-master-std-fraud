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
| 1.4-INT-01 | `make producer-transactions` — verificar no Kafka UI | Tópico `raw-transactions` recebendo mensagens; ~10 TPS; campos e headers (`produced_at`, `source_system`) presentes | NOK |
| 1.4-INT-02 | `make producer-market` — verificar no Kafka UI | Tópico `raw-market-data` recebendo ticks; ~50 TPS; campos e headers presentes | NOK |

### 3. Validações Manuais

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.4-MAN-01 | Graceful shutdown (Ctrl+C) | Log "Producer encerrado" com totais de mensagens enviadas e erros | NOK |
| 1.4-MAN-02 | Headers Kafka via Kafka UI | Headers `produced_at` e `source_system` presentes em cada mensagem | NOK |
| 1.4-MAN-03 | Market producer fora do horário de pregão | Loga "Fora do horário de pregão" e aguarda 60s sem enviar mensagens | NOK |
| 1.4-MAN-04 | Particionamento por chave — transações | Transações do mesmo `customer_id` vão para a mesma partição | NOK |
| 1.4-MAN-05 | Particionamento por chave — market data | Ticks do mesmo `symbol` vão para a mesma partição | NOK |
| 1.4-MAN-06 | Perfis `LOW_LATENCY_CONFIG` e `HIGH_THROUGHPUT_CONFIG` | Valores de `linger_ms` e `batch_size` conforme especificação | NOK |

### 4. Cobertura

| ID | Descrição | Resultado Esperado | Status |
|----|-----------|-------------------|--------|
| 1.4-COV-01 | `make test-cov` → `htmlcov/index.html` | Cobertura >= 70% em `src/ingestion/streaming/` | NOK |
