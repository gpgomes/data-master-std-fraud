# Dicionário de Dados — Financial Fraud Detection Platform

## Bronze Layer

### bronze/transactions/
Dados raw de transações financeiras conforme recebidos dos producers Kafka.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| transaction_id | string | UUID único da transação |
| customer_id | string | UUID do cliente |
| timestamp | timestamp | Data/hora com timezone (UTC) |
| amount | double | Valor em moeda local |
| currency | string | BRL, USD, EUR |
| transaction_type | string | PIX, TED, DOC, CARTAO_CREDITO, CARTAO_DEBITO, BOLETO |
| merchant_category | string | Categoria do estabelecimento |
| origin_account | string | Conta de origem |
| destination_account | string | Conta de destino |
| origin_bank | string | Banco de origem |
| destination_bank | string | Banco de destino |
| channel | string | Canal de operação |
| device_id | string | ID do dispositivo (pode ser nulo) |
| ip_address | string | IP do cliente (pode ser nulo) |
| latitude | double | Latitude do cliente (pode ser nulo) |
| longitude | double | Longitude do cliente (pode ser nulo) |
| is_fraud | boolean | Flag de fraude (label, ground truth conhecida na origem) |
| fraud_type | string | Tipo de fraude (nulo se não for fraude) |
| fraud_score | double | Score de risco (0-1); sempre nulo no Bronze — campo derivado, populado pela detecção de fraude (streaming/Z-Score), nunca pela geração/ingestão |

### bronze/market_data/
Dados OHLCV de ativos conforme coletados via yfinance.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| symbol | string | Ticker do ativo (ex: PETR4.SA) |
| date | date | Data da cotação |
| open | double | Preço de abertura |
| high | double | Preço máximo |
| low | double | Preço mínimo |
| close | double | Preço de fechamento |
| volume | long | Volume negociado |
| adjusted_close | double | Preço de fechamento ajustado |

## Silver Layer

### silver/transactions/
Transações limpas, deduplicadas e com tipos normalizados. Particionado por `year/month/day`.

Diferenças do Bronze:
- Sem duplicatas (dedup por `transaction_id`)
- Nulls tratados conforme regras de negócio
- `timestamp` normalizado para UTC
- `amount` em BRL (moedas convertidas com taxa do dia)
- Colunas de auditoria: `ingestion_timestamp`, `processing_timestamp`

### silver/market_data/
Cotações limpas e enriquecidas com indicadores básicos.

Colunas adicionais:
- `daily_return`: retorno percentual diário
- `price_range`: diferença high - low

## Gold Layer

### gold/fato_transacoes/
Tabela fato de transações — modelo Star Schema.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| transaction_id | string | PK |
| customer_key | string | FK dim_clientes |
| date_key | date | FK dim_data |
| amount_brl | double | Valor em BRL |
| transaction_type | string | Tipo |
| is_fraud | boolean | Flag de fraude |
| fraud_score | double | Score de risco (0-1) |

### gold/dim_clientes/
Dimensão de clientes com atributos para análise.

### gold/fato_anomalias/
Registros de anomalias detectadas no streaming com Z-Score e contexto.

## Glossário de Negócio

| Termo | Definição |
|-------|-----------|
| **VWAP** | Volume-Weighted Average Price — preço médio ponderado pelo volume |
| **Volatilidade** | Desvio padrão dos retornos diários em uma janela de N dias |
| **Fraud Score** | Probabilidade de fraude calculada pelo detector de streaming (0=legítimo, 1=fraude). Campo derivado: nulo em Bronze/geração, populado apenas a partir da detecção (streaming/Z-Score). Não confundir com `is_fraud`, que é o rótulo de ground truth conhecido na origem dos dados |
| **Z-Score** | Número de desvios padrão da média — usado para detectar outliers de valor |
| **Velocity Check** | Verificação de frequência anormal de transações em curto intervalo |
| **Account Takeover** | Acesso não autorizado e operações em conta alheia |
| **Smurfing** | Fragmentação de grandes valores em transações menores para evitar detecção |
