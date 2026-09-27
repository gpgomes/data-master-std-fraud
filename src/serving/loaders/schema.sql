-- DDL das tabelas da serving layer (issue #10) — espelha o star schema do Gold
-- (docs/data_dictionary.md). Executado de forma idempotente (CREATE TABLE IF
-- NOT EXISTS) pelo GoldToPostgresLoader antes de cada carga.
--
-- Sem foreign keys entre fato e dimensões de propósito: a estratégia de carga
-- é truncate + reload por tabela (ver gold_to_postgres.py), e FKs forçariam
-- uma ordem de truncamento entre tabelas ou CASCADE — a integridade
-- referencial já é garantida upstream pelo job silver_to_gold.py.

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key          VARCHAR PRIMARY KEY,
    name                   VARCHAR,
    cpf_masked             VARCHAR,
    gender                 VARCHAR,
    birth_date             VARCHAR,
    age                    INTEGER,
    age_group              VARCHAR,
    segment                VARCHAR,
    city                   VARCHAR,
    state                  VARCHAR,
    country                VARCHAR,
    risk_score              DOUBLE PRECISION,
    account_opening_date   VARCHAR,
    processing_timestamp   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        DATE PRIMARY KEY,
    year             INTEGER,
    month            INTEGER,
    day              INTEGER,
    quarter          INTEGER,
    day_of_week      INTEGER,
    day_name         VARCHAR,
    week_of_year     INTEGER,
    is_weekend       BOOLEAN,
    processing_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      VARCHAR PRIMARY KEY,
    customer_key         VARCHAR,
    date_key             DATE,
    amount_brl           DOUBLE PRECISION,
    currency             VARCHAR,
    transaction_type     VARCHAR,
    channel              VARCHAR,
    merchant_category    VARCHAR,
    is_fraud             BOOLEAN,
    fraud_type           VARCHAR,
    fraud_score          DOUBLE PRECISION,
    processing_timestamp TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_fact_transactions_date_key ON fact_transactions (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_transactions_customer_key ON fact_transactions (customer_key);

CREATE TABLE IF NOT EXISTS agg_daily_fraud_metrics (
    date_key             DATE,
    transaction_type     VARCHAR,
    total_transactions   BIGINT,
    total_amount_brl     DOUBLE PRECISION,
    avg_amount_brl       DOUBLE PRECISION,
    fraud_count          BIGINT,
    fraud_rate           DOUBLE PRECISION,
    processing_timestamp TIMESTAMP,
    PRIMARY KEY (date_key, transaction_type)
);

-- ── Streaming (issue #38) ─────────────────────────────────────────────────────
-- Saída do detector de fraude, carregada de silver/transactions_stream/ por
-- stream_to_postgres.py (truncate + reload). Só as colunas úteis à consulta:
-- device_id, ip_address, contas e coordenadas do Parquet de origem não vão para a
-- serving layer.
--
-- Rótulo x predição (issue #47). `is_fraud` e `fraud_type` são o RÓTULO do gerador
-- sintético (ground truth): servem só para medir o detector, nunca entram no scoring.
-- O que o detector decidiu está nas colunas de predição:
--   * Fraud Engine V2 (`multisignal-v2`, quem alerta): fraud_score, is_fraud_predicted,
--     fraud_signals, fraud_type_predicted, detector_version;
--   * Z-Score V1 (`zscore-v1`, shadow, só para comparação): z_score, is_anomaly,
--     fraud_score_v1, shadow_detector_version.
-- `fraud_signals` é a lista dos sinais ativos separada por vírgula (VARCHAR, não array:
-- a API é testada em SQLite e o Superset lê texto simples); vazio = nenhum sinal.

CREATE TABLE IF NOT EXISTS stream_scored_transactions (
    transaction_id       VARCHAR PRIMARY KEY,
    customer_id          VARCHAR,
    event_time           TIMESTAMP,
    amount               DOUBLE PRECISION,
    currency             VARCHAR,
    transaction_type     VARCHAR,
    channel              VARCHAR,
    merchant_category    VARCHAR,
    is_fraud             BOOLEAN,
    fraud_type           VARCHAR,
    fraud_score          DOUBLE PRECISION,
    fraud_score_bucket   DOUBLE PRECISION,
    is_fraud_predicted   BOOLEAN,
    fraud_signals        VARCHAR,
    fraud_type_predicted VARCHAR,
    detector_version     VARCHAR,
    z_score              DOUBLE PRECISION,
    is_anomaly           BOOLEAN,
    fraud_score_v1       DOUBLE PRECISION,
    shadow_detector_version VARCHAR,
    produced_at          TIMESTAMP,
    processing_timestamp TIMESTAMP,
    latency_seconds      DOUBLE PRECISION
);

-- Bancos provisionados antes da issue #47 já têm a tabela sem as colunas do Fraud Engine, e o
-- CREATE TABLE IF NOT EXISTS acima não as adiciona. Estes ALTER são idempotentes e rodam a cada
-- carga: em banco novo não fazem nada. Toda coluna criada depois da issue #38 entra nos dois
-- lugares (CREATE TABLE e ALTER); um teste confere.
ALTER TABLE stream_scored_transactions ADD COLUMN IF NOT EXISTS is_fraud_predicted BOOLEAN;
ALTER TABLE stream_scored_transactions ADD COLUMN IF NOT EXISTS fraud_signals VARCHAR;
ALTER TABLE stream_scored_transactions ADD COLUMN IF NOT EXISTS fraud_type_predicted VARCHAR;
ALTER TABLE stream_scored_transactions ADD COLUMN IF NOT EXISTS detector_version VARCHAR;
ALTER TABLE stream_scored_transactions ADD COLUMN IF NOT EXISTS fraud_score_v1 DOUBLE PRECISION;
ALTER TABLE stream_scored_transactions ADD COLUMN IF NOT EXISTS shadow_detector_version VARCHAR;

CREATE INDEX IF NOT EXISTS ix_stream_scored_event_time ON stream_scored_transactions (event_time);
CREATE INDEX IF NOT EXISTS ix_stream_scored_customer_id ON stream_scored_transactions (customer_id);

-- Alertas do detector (mesmos do tópico Kafka fraud-alerts; alert_id é
-- determinístico a partir de transaction_id, ver stream_processor.py). Alerta é o que o
-- Fraud Engine V2 marcou: `fraud_type` é o tipo INFERIDO pelos sinais (nulo se nenhuma regra
-- casou, nunca o rótulo), `signals` é a lista dos sinais ativos separada por vírgula e
-- `z_score` é o do detector antigo (shadow), que pode ser nulo.
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id       VARCHAR PRIMARY KEY,
    transaction_id VARCHAR UNIQUE,
    customer_id    VARCHAR,
    event_time     TIMESTAMP,
    amount         DOUBLE PRECISION,
    fraud_type     VARCHAR,
    fraud_score    DOUBLE PRECISION,
    z_score        DOUBLE PRECISION,
    alert_reason   VARCHAR,
    signals        VARCHAR,
    detector_version VARCHAR,
    processed_at   TIMESTAMP
);

ALTER TABLE fraud_alerts ADD COLUMN IF NOT EXISTS signals VARCHAR;
ALTER TABLE fraud_alerts ADD COLUMN IF NOT EXISTS detector_version VARCHAR;

CREATE INDEX IF NOT EXISTS ix_fraud_alerts_processed_at ON fraud_alerts (processed_at);
CREATE INDEX IF NOT EXISTS ix_fraud_alerts_customer_id ON fraud_alerts (customer_id);
