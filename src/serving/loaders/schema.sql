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
-- Saída do detector de fraude (Z-Score, issue #11), carregada de
-- silver/transactions_stream/ por stream_to_postgres.py (truncate + reload).
-- Só as colunas úteis à consulta: device_id, ip_address, contas e coordenadas do
-- Parquet de origem não vão para a serving layer.

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
    z_score              DOUBLE PRECISION,
    fraud_score          DOUBLE PRECISION,
    fraud_score_bucket   DOUBLE PRECISION,
    is_anomaly           BOOLEAN,
    produced_at          TIMESTAMP,
    processing_timestamp TIMESTAMP,
    latency_seconds      DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS ix_stream_scored_event_time ON stream_scored_transactions (event_time);
CREATE INDEX IF NOT EXISTS ix_stream_scored_customer_id ON stream_scored_transactions (customer_id);

-- Alertas do detector (mesmos do tópico Kafka fraud-alerts; alert_id é
-- determinístico a partir de transaction_id, ver stream_processor.py).
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
    processed_at   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_fraud_alerts_processed_at ON fraud_alerts (processed_at);
CREATE INDEX IF NOT EXISTS ix_fraud_alerts_customer_id ON fraud_alerts (customer_id);
