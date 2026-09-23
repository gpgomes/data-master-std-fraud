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

Star schema gerado pelo job `silver_to_gold.py` a partir da camada Silver.
Chaves de dimensão reutilizam as chaves naturais do Silver (`customer_id` →
`customer_key`, `transaction_date` → `date_key`) — não há geração de surrogate
keys sintéticas nesta etapa.

### gold/fact_transactions/
Tabela fato de transações — grão: uma linha por transação. Particionado por `date_key`.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| transaction_id | string | PK |
| customer_key | string | FK gold/dim_customers |
| date_key | date | FK gold/dim_date |
| amount_brl | double | Valor em BRL |
| currency | string | Moeda original |
| transaction_type | string | PIX, TED, DOC, ... |
| channel | string | Canal de operação |
| merchant_category | string | Categoria do estabelecimento |
| is_fraud | boolean | Flag de fraude |
| fraud_type | string | Tipo de fraude (nulo se não for fraude) |
| fraud_score | double | Score de risco (0-1); populado pelo streaming, nulo no batch puro |

### gold/dim_customers/
Dimensão de clientes com atributos para análise. Mantém apenas o registro
corrente (`is_current=True`) do Silver (SCD2).

| Campo | Tipo | Descrição |
|-------|------|-----------|
| customer_key | string | PK |
| name, cpf_masked, gender, birth_date | string | Atributos de identificação |
| age, age_group | int, string | Idade e faixa etária |
| segment | string | VAREJO, ALTA_RENDA, PRIVATE |
| city, state, country | string | Localização |
| risk_score | double | Score de risco cadastral (0-100) |
| account_opening_date | string | Data de abertura da conta |

### gold/dim_date/
Dimensão de calendário, derivada das datas distintas presentes em
`silver/transactions`. Reconstruída por completo a cada execução (não
particionada, não respeita `--start-date/--end-date`), para nunca deixar a FK
`date_key` da fato órfã em reprocessamentos parciais.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| date_key | date | PK |
| year, month, day, quarter | int | Componentes da data |
| day_of_week | int | 1=domingo ... 7=sábado (convenção Spark) |
| day_name | string | Nome do dia da semana |
| week_of_year | int | Semana do ano |
| is_weekend | boolean | Sábado ou domingo |

### gold/agg_daily_fraud_metrics/
Agregação diária de volume e taxa de fraude por dia e tipo de transação.
Particionado por `date_key`.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| date_key | date | Data |
| transaction_type | string | Tipo de transação |
| total_transactions | long | Total de transações no grupo |
| total_amount_brl | double | Soma de amount_brl |
| avg_amount_brl | double | Ticket médio |
| fraud_count | long | Total de transações fraudulentas |
| fraud_rate | double | fraud_count / total_transactions |

### gold/fato_anomalias/ *(ainda não implementado)*
Registros de anomalias detectadas no streaming com Z-Score e contexto —
depende do job de streaming (roadmap 2.3/2.4), não incluído no batch Silver→Gold.

## Serving Layer — streaming (issue #38)

Carregadas em PostgreSQL por `src/serving/loaders/stream_to_postgres.py` a partir de `silver/transactions_stream/` (truncate + reload).

### stream_scored_transactions
Uma linha por `transaction_id` processada pelo detector de streaming.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| transaction_id | string | PK |
| customer_id | string | Cliente (id do evento, não é FK para `dim_customers`: o simulador gera clientes próprios) |
| event_time | timestamp | Horário do evento (`timestamp` da transação) |
| amount, currency, transaction_type, channel, merchant_category | — | Atributos da transação |
| is_fraud, fraud_type | boolean, string | Rótulo sintético do gerador (não é a decisão do detector) |
| z_score | double | Z-Score do valor sobre a janela de 1h do cliente; nulo sem baseline (menos de 2 transações na janela) |
| fraud_score | double | `min(abs(z_score) / 6, 1)`; nulo quando `z_score` é nulo |
| fraud_score_bucket | double | `fraud_score` arredondado a 0,1 (para o histograma) |
| is_anomaly | boolean | `abs(z_score) > 3` |
| produced_at | timestamp | Quando o producer emitiu o evento |
| processing_timestamp | timestamp | Quando o Spark processou o micro-batch |
| latency_seconds | double | `processing_timestamp - produced_at` |

Não carrega `device_id`, `ip_address`, contas nem coordenadas.

### fraud_alerts
Os alertas do detector, o mesmo conteúdo do tópico Kafka `fraud-alerts`.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| alert_id | string | PK, UUID determinístico derivado de `transaction_id` (idêntico ao do Kafka) |
| transaction_id | string | Única: um alerta por transação |
| customer_id, event_time, amount | — | Dados da transação alertada |
| fraud_type | string | Rótulo do gerador quando existe; senão `MONEY_LAUNDERING` (fallback fixo) |
| fraud_score, z_score | double | Score e Z-Score que dispararam o alerta |
| alert_reason | string | Texto com o Z-Score, o limiar e a janela |
| processed_at | timestamp | `processing_timestamp` da linha (não a hora da carga) |

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
