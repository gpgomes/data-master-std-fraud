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
| fraud_score | double | Score de risco (0-1); sempre nulo no Bronze — campo derivado, populado pela detecção de fraude (streaming, Fraud Engine), nunca pela geração/ingestão |

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

### silver/transactions_stream/
Saída do job de streaming (`stream_processor.py`), **distinta** de `silver/transactions/` (que é do
batch). Uma linha por `transaction_id` de cada micro-batch, particionada por `query_id` (o id do
checkpoint) e `batch_id`. Só existe depois que o streaming rodou (issues #11 e #46).

| Grupo | Colunas | Descrição |
|-------|---------|-----------|
| Transação | `transaction_id`, `customer_id`, `timestamp`, `amount`, `currency`, `transaction_type`, `merchant_category`, `origin_account`, `destination_account`, `origin_bank`, `destination_bank`, `channel`, `device_id`, `ip_address`, `latitude`, `longitude`, `produced_at`, `source_system` | O evento como chegou do Kafka |
| Enriquecimento | `customer_segment`, `customer_risk_score`, `customer_city` | Join broadcast com `gold/dim_customers` |
| **Rótulo (ground truth)** | `is_fraud`, `fraud_type` | O que o gerador sintético sabe. Viaja no payload do Kafka e serve **só para medir** o detector: é separado do DataFrame antes de qualquer detector e voltado por `transaction_id` no fim |
| **Predição — Fraud Engine V2** (quem alerta) | `fraud_score`, `is_fraud_predicted`, `fraud_signals`, `fraud_type_predicted`, `detector_version` | Score noisy-OR em [0, 1]; alerta quando `fraud_score` passa do limiar calibrado; `fraud_signals` lista os sinais ativos; `fraud_type_predicted` é inferido pelos sinais (nulo se nenhuma regra casa) |
| **Predição — Z-Score V1** (shadow) | `z_score`, `is_anomaly`, `fraud_score_v1`, `shadow_detector_version` | O detector antigo, mantido só para comparação online |
| Auditoria | `processing_timestamp`, `query_id`, `batch_id` | Quando o Spark processou o micro-batch e de qual execução ele veio |

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

### gold/customer_behavior_profile/
Perfil de comportamento por cliente (issue #46): o que é "normal" para cada um, aprendido do histórico
legítimo. Grão: **uma linha por cliente** de `gold/dim_customers`. É a camada longa da arquitetura
Lambda: o batch calcula (varre meses de transações), o streaming lê por broadcast e só calcula a janela
curta. Usa só as linhas com `is_fraud = false` do Silver (rótulos históricos existem depois da
confirmação da fraude); o streaming nunca lê o rótulo do evento que está pontuando.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| customer_id | string | PK (mesma chave de `dim_customers.customer_key`) |
| has_profile | boolean | `n_history >= 3`. Sem isso o valor cai para o prior do segmento e os sinais de "conhecido" ficam neutros |
| n_history | long | Transações legítimas usadas |
| mu_log, sigma_log | double | Média e desvio de `ln(amount)`, encolhidos ao prior do segmento (k = 5); `sigma_log` tem piso de 0,3 |
| known_devices | array\<string\> | Devices já usados |
| known_ip_prefixes | array\<string\> | Redes /24 já usadas |
| known_destinations | array\<string\> | Destinatários frequentes (≥ 2 usos, até 50) |
| night_share | double | Fração das transações de madrugada (0–5 h, horário de São Paulo); 1,0 sem histórico |
| home_lat, home_lon | double | Mediana das coordenadas; nulas sem histórico |
| account_opening_date | date | Abertura da conta, de `dim_customers` |

Validada pelo gate `gold_customer_behavior_profile` do Great Expectations (não nulos, cliente único,
faixas de μ/σ e de coordenadas, `has_profile` coerente com `n_history`).

### gold/fato_anomalias/ *(nunca implementado)*
Ideia original de uma tabela Gold de anomalias detectadas no streaming. Não existe: os alertas ficam em
`silver/transactions_stream/` (Parquet), no tópico Kafka `fraud-alerts` e na tabela `fraud_alerts` do
Postgres (abaixo).

## Serving Layer — streaming (issue #38)

Carregadas em PostgreSQL por `src/serving/loaders/stream_to_postgres.py` a partir de `silver/transactions_stream/` (truncate + reload).

### stream_scored_transactions
Uma linha por `transaction_id` processada pelo detector de streaming.

**Rótulo × predição.** `is_fraud` e `fraud_type` são o **rótulo do gerador sintético** (ground truth):
servem para medir o detector, nunca entram no scoring. O que o detector decidiu está nas colunas de
predição, e as duas gerações de detector ficam lado a lado (issue #47):

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| transaction_id | string | PK |
| customer_id | string | Cliente (id do evento) |
| event_time | timestamp | Horário do evento (`timestamp` da transação) |
| amount, currency, transaction_type, channel, merchant_category | — | Atributos da transação |
| is_fraud, fraud_type | boolean, string | **Rótulo** sintético do gerador (não é a decisão do detector) |
| fraud_score | double | **Fraud Engine V2**: score noisy-OR dos sinais ativos, em [0, 1]; 0 = nenhum sinal |
| fraud_score_bucket | double | `fraud_score` arredondado a 0,1 (para o histograma) |
| is_fraud_predicted | boolean | **V2**: `fraud_score` acima do limiar calibrado (`weights.py`); é o que gera o alerta |
| fraud_signals | string | **V2**: sinais ativos separados por vírgula (ex.: `AMOUNT_ANOMALY,NEW_DESTINATION`); vazio = nenhum sinal acima de 0,5 |
| fraud_type_predicted | string | **V2**: tipo inferido pelos sinais; nulo se nenhuma regra casou |
| detector_version | string | `multisignal-v2` |
| z_score | double | **Z-Score V1** (shadow): valor sobre a janela de 1h do cliente; nulo sem baseline (menos de 2 transações na janela) |
| is_anomaly | boolean | **V1**: `abs(z_score) > 3` |
| fraud_score_v1 | double | **V1**: `min(abs(z_score) / 6, 1)`; nulo quando `z_score` é nulo |
| shadow_detector_version | string | `zscore-v1` |
| produced_at | timestamp | Quando o producer emitiu o evento |
| processing_timestamp | timestamp | Quando o Spark processou o micro-batch |
| latency_seconds | double | `processing_timestamp - produced_at` |

Não carrega `device_id`, `ip_address`, contas nem coordenadas. Em bancos provisionados antes da
issue #47 as colunas do V2 chegam por `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (aplicado por
`ensure_schema` a cada carga), e as linhas carregadas antes disso ficam com elas nulas até a próxima
carga.

### fraud_alerts
Os alertas do Fraud Engine V2, o mesmo conteúdo do tópico Kafka `fraud-alerts`.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| alert_id | string | PK, UUID determinístico derivado de `transaction_id` (idêntico ao do Kafka) |
| transaction_id | string | Única: um alerta por transação |
| customer_id, event_time, amount | — | Dados da transação alertada |
| fraud_type | string | Tipo **inferido pelos sinais** (nunca o rótulo do gerador); nulo se nenhuma regra casou. Não há mais tipo de fallback |
| fraud_score | double | Score noisy-OR do V2 |
| z_score | double | Z-Score do V1 (shadow), para comparação; nulo sem baseline |
| alert_reason | string | `Sinais: <lista> \| score <x> \| <detector_version>`; sem nenhum sinal acima de 0,5 diz "combinação de sinais fracos" |
| signals | string | Sinais ativos separados por vírgula (a API devolve como lista em `fraud_signals`) |
| detector_version | string | `multisignal-v2` |
| processed_at | timestamp | `processing_timestamp` da linha (não a hora da carga) |

### Sinais do Fraud Engine
Cada sinal vale de 0 a 1 (binário, exceto `AMOUNT_ANOMALY`, que é graduado) e entra no score com o
peso calibrado em `src/transformation/fraud/weights.py`. Aparece em `fraud_signals` quando vale ≥ 0,5.

| Sinal | Dispara quando | Estado |
|-------|----------------|--------|
| `AMOUNT_ANOMALY` | `ln(amount)` está muito acima do típico do cliente (z entre 1,5 e 4) | Perfil |
| `NEW_DEVICE` | device fora de `known_devices` | Perfil |
| `NEW_IP` | rede /24 fora de `known_ip_prefixes` | Perfil |
| `GEO_FAR_FROM_HOME` | a mais de 500 km do centro do cliente | Perfil |
| `UNUSUAL_HOUR` | madrugada, para quem quase não transaciona nela | Perfil |
| `NEW_DESTINATION` | destinatário fora de `known_destinations` | Perfil |
| `ACCOUNT_AGE_LOW` | conta aberta há menos de 30 dias | Perfil |
| `TX_VELOCITY` | 10 ou mais transações em 10 min | Janela curta |
| `GEO_VELOCITY` | nenhum dos últimos 5 eventos (6 h) é uma origem plausível (≤ 900 km/h ou ≤ 100 km) | Janela curta |
| `RECIPIENT_CONCENTRATION` | 3 ou mais remetentes para a mesma conta em 1 h | Janela curta |

## Observabilidade da plataforma (issue #55)

Tabelas **append-only** no Postgres da serving layer (`src/observability/schema.sql`), criadas na primeira
gravação. Timestamps em UTC, sem fuso. Não é dado de negócio: é o que responde "a plataforma está saudável?".

### stream_batch_metrics
Uma linha por micro-batch do stream (PK `query_id`, `batch_id`: o replay reescreve a linha).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| query_id, run_id | string | Id do checkpoint e da execução (um `run_id` novo a cada reinício) |
| batch_id, batch_timestamp | long, timestamp | Micro-batch e quando começou (Spark) |
| num_input_rows | long | Linhas lidas do Kafka |
| input_rows_per_second, processed_rows_per_second | double | Taxa de chegada × taxa de processamento |
| batch_duration_ms, add_batch_ms, get_offset_ms | long | Duração total, do `foreachBatch` e da consulta de offsets |
| kafka_lag | long | Soma, por partição, de (último offset disponível − offset processado) |
| rows_scored, alerts | long | Linhas pontuadas (após deduplicar por `transaction_id`) e alertas do V2 |
| state_rows | long | Linhas do estado curto lidas no micro-batch (última 1 h completa + os últimos 5 eventos de cada cliente até 6 h, issue #56) |
| latency_p50_s, latency_p95_s, latency_max_s | double | Latência evento → processamento (`processing_timestamp − produced_at`) dentro do micro-batch |
| state_load_ms, score_ms, parquet_ms, kafka_ms, state_write_ms | long | Tempo de cada etapa do `foreachBatch` (issue #56): ler o estado curto, pontuar (V2 + V1 + contagens), gravar o Parquet, publicar no Kafka e gravar o estado. Uma etapa pulada no replay dá ~0 |

### pipeline_runs
Uma linha por execução de DAG (PK `dag_id`, `run_id`): `state` (`success`/`failed`), `start_date`, `end_date`,
`duration_seconds` e `failed_tasks` (lista separada por vírgula).

### quality_gate_runs
Uma linha por execução de gate do Great Expectations: `dataset`, `success`, `total_expectations`,
`failed_expectations`, `optional` (gates de mercado) e `run_at`. Gate opcional sem dados aparece com
`success = false` e contagens nulas.

### api_requests
Uma linha por request: `requested_at`, `method`, `route` (o template da rota, sem ids; `unmatched` para URL
inexistente), `status_code` e `duration_ms`.

## Glossário de Negócio

| Termo | Definição |
|-------|-----------|
| **VWAP** | Volume-Weighted Average Price — preço médio ponderado pelo volume |
| **Volatilidade** | Desvio padrão dos retornos diários em uma janela de N dias |
| **Fraud Score** | Score de risco (0=legítimo, 1=fraude) calculado pelo detector de streaming. Campo derivado: nulo em Bronze/geração, populado apenas a partir da detecção. No streaming é o do Fraud Engine (noisy-OR dos sinais); o do Z-Score antigo é `fraud_score_v1`. Não é probabilidade calibrada. Não confundir com `is_fraud`, que é o rótulo de ground truth conhecido na origem dos dados |
| **Z-Score** | Número de desvios padrão da média — usado para detectar outliers de valor. Foi o único detector do streaming até a issue #46; hoje roda em paralelo (*shadow*) como linha de base |
| **Fraud Engine** | Detector multi-signal (`src/transformation/fraud/`): 10 sinais de comportamento (valor, device, rede, destinatário, hora, local, idade da conta, velocidade, viagem impossível, concentração de destinatários) combinados por noisy-OR (`1 − Π(1 − wᵢ·sᵢ)`), com pesos e limiar calibrados numa seed de validação (recall máximo com FPR ≤ 1%). Sem modelo treinado: o motivo de cada alerta sai dos sinais |
| **Perfil de Comportamento** | O que é "normal" para um cliente (valor típico, devices, redes, destinatários, horário, local), calculado pelo batch em `gold/customer_behavior_profile/` e lido por broadcast pelo streaming |
| **Shadow Scoring** | Rodar um detector novo e o antigo sobre os mesmos eventos, com só o novo alertando, para comparar os dois online sem risco |
| **Rótulo (ground truth)** | `is_fraud`/`fraud_type` do gerador sintético. Serve para medir o detector, que nunca o lê. Não é a decisão do detector: essa está em `is_fraud_predicted`/`fraud_type_predicted` |
| **SLO** | *Service Level Objective*: meta mensurável de um indicador da plataforma numa janela (ex.: latência p95 do stream < 12 s nas últimas 24 h). No projeto: `make slo-report` (issue #55) |
| **Velocity Check** | Verificação de frequência anormal de transações em curto intervalo |
| **Account Takeover** | Acesso não autorizado e operações em conta alheia |
| **Smurfing** | Fragmentação de grandes valores em transações menores para evitar detecção |
