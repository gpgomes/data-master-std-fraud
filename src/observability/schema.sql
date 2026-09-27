-- Métricas de plataforma (issue #55), no mesmo Postgres da serving layer. Tabelas
-- append-only: diferente das tabelas de serving (truncate + reload), aqui o histórico É o dado.
-- Aplicado por MetricsStore.ensure_schema() na primeira gravação de cada processo.
--
-- Todos os timestamps são UTC sem fuso (TIMESTAMP): o do Spark e o do Airflow já chegam em UTC, e
-- os DEFAULT usam `now() AT TIME ZONE 'UTC'` porque o Postgres do projeto roda com
-- timezone=America/Sao_Paulo e `now()` puro gravaria hora local.

-- Uma linha por micro-batch do stream, gravada pelo StreamingQueryListener no driver.
CREATE TABLE IF NOT EXISTS stream_batch_metrics (
    query_id                  VARCHAR,
    run_id                    VARCHAR,
    batch_id                  BIGINT,
    batch_timestamp           TIMESTAMP,
    num_input_rows            BIGINT,
    input_rows_per_second     DOUBLE PRECISION,
    processed_rows_per_second DOUBLE PRECISION,
    batch_duration_ms         BIGINT,
    add_batch_ms              BIGINT,
    get_offset_ms             BIGINT,
    kafka_lag                 BIGINT,
    rows_scored               BIGINT,
    alerts                    BIGINT,
    state_rows                BIGINT,
    latency_p50_s             DOUBLE PRECISION,
    latency_p95_s             DOUBLE PRECISION,
    latency_max_s             DOUBLE PRECISION,
    recorded_at               TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (query_id, batch_id)
);

-- Uma linha por execução de quality gate do Great Expectations.
CREATE TABLE IF NOT EXISTS quality_gate_runs (
    id                   BIGSERIAL PRIMARY KEY,
    dataset              VARCHAR,
    success              BOOLEAN,
    total_expectations   INTEGER,
    failed_expectations  INTEGER,
    optional             BOOLEAN,
    run_at               TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC')
);

-- Uma linha por execução de DAG, gravada pela última task (roda mesmo com falha upstream).
CREATE TABLE IF NOT EXISTS pipeline_runs (
    dag_id            VARCHAR,
    run_id            VARCHAR,
    state             VARCHAR,
    start_date        TIMESTAMP,
    end_date          TIMESTAMP,
    duration_seconds  DOUBLE PRECISION,
    failed_tasks      VARCHAR,
    recorded_at       TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (dag_id, run_id)
);

-- Uma linha por request da API, gravada depois da resposta (não entra na latência medida).
CREATE TABLE IF NOT EXISTS api_requests (
    id           BIGSERIAL PRIMARY KEY,
    requested_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC'),
    method       VARCHAR,
    route        VARCHAR,
    status_code  INTEGER,
    duration_ms  DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS ix_stream_batch_metrics_ts ON stream_batch_metrics (batch_timestamp);
CREATE INDEX IF NOT EXISTS ix_quality_gate_runs_run_at ON quality_gate_runs (run_at);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_start ON pipeline_runs (start_date);
CREATE INDEX IF NOT EXISTS ix_api_requests_at ON api_requests (requested_at);
