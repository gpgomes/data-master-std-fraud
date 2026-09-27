-- Relatório de SLOs da plataforma (issue #55). Janela: últimas 24 h.
--
-- Todas as colunas de tempo são UTC (ver src/observability/schema.sql), então a janela também.
--
-- Uso: make slo-report (requer as tabelas de métricas de src/observability/schema.sql, que o
-- stream, as DAGs, os gates e a API preenchem).
--
-- Metas "realistas para o ambiente local" (decisão da issue #55): o trigger do stream é de 10 s,
-- então a latência evento -> processamento não fica abaixo de ~10 s por construção. A meta
-- mais agressiva (p95 < 5 s) é o alvo da otimização da issue #56.
--
-- Status: OK (dentro da meta), VIOLADO (fora) ou SEM DADOS (nada na janela: o componente não
-- rodou, o que também é informação).

\pset null '-'
\pset footer off

\echo
\echo '== SLOs da plataforma (últimas 24 h) =='
WITH w AS (SELECT (now() AT TIME ZONE 'UTC') - interval '24 hours' AS since),
stream AS (
    SELECT count(*) AS n,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_p95_s)     AS latency_p95_s,
           max(kafka_lag)                                                  AS max_lag,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY batch_duration_ms) AS duration_p95_ms
    FROM stream_batch_metrics, w
    WHERE batch_timestamp >= w.since
),
gates AS (
    SELECT count(*) AS n, count(*) FILTER (WHERE success) AS ok
    FROM quality_gate_runs, w
    WHERE run_at >= w.since AND NOT COALESCE(optional, false)
),
dags AS (
    SELECT count(DISTINCT dag_id) AS n,
           count(DISTINCT dag_id) FILTER (WHERE state = 'success') AS with_success
    FROM pipeline_runs, w
    WHERE start_date >= w.since
),
api AS (
    SELECT count(*) AS n,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
           100.0 * count(*) FILTER (WHERE status_code >= 500) / NULLIF(count(*), 0) AS err_pct
    FROM api_requests, w
    WHERE requested_at >= w.since
),
slo(ordem, slo, meta, medido, amostras, ok) AS (
    SELECT 1, 'Stream: latência evento -> processamento p95', '< 12 s',
           round(latency_p95_s::numeric, 2) || ' s', n, latency_p95_s < 12 FROM stream
    UNION ALL
    SELECT 2, 'Stream: lag máximo do Kafka', '< 10.000 eventos',
           max_lag::text, n, max_lag < 10000 FROM stream
    UNION ALL
    SELECT 3, 'Stream: duração do micro-batch p95', '< 10 s',
           round((duration_p95_ms / 1000.0)::numeric, 2) || ' s', n, duration_p95_ms < 10000 FROM stream
    UNION ALL
    SELECT 4, 'Pipeline: quality gates obrigatórios verdes', '100%',
           round(100.0 * ok / NULLIF(n, 0), 1) || '%', n, ok = n FROM gates
    UNION ALL
    SELECT 5, 'Pipeline: toda DAG executada tem sucesso', '100% das DAGs',
           with_success || ' de ' || n, n, with_success = n FROM dags
    UNION ALL
    SELECT 6, 'API: latência p95', '< 500 ms',
           round(p95_ms::numeric, 1) || ' ms', n, p95_ms < 500 FROM api
    UNION ALL
    SELECT 7, 'API: erros 5xx', '< 1%',
           round(err_pct::numeric, 2) || '%', n, err_pct < 1 FROM api
)
SELECT slo, meta, medido, amostras,
       CASE WHEN amostras = 0 THEN 'SEM DADOS' WHEN ok THEN 'OK' ELSE 'VIOLADO' END AS status
FROM slo
ORDER BY ordem;

\echo
\echo '== Pipeline: última execução de cada DAG =='
SELECT DISTINCT ON (dag_id) dag_id, state, start_date, round(duration_seconds::numeric, 1) AS duracao_s,
       failed_tasks AS tasks_com_falha
FROM pipeline_runs
ORDER BY dag_id, start_date DESC;

\echo
\echo '== Quality gates com falha nas últimas 24 h =='
-- Os gates opcionais (mercado, yfinance) aparecem marcados: falham sem bloquear o pipeline e não
-- contam no SLO de gates.
SELECT dataset, COALESCE(optional, false) AS opcional, count(*) AS falhas, max(run_at) AS ultima
FROM quality_gate_runs
WHERE NOT success AND run_at >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
GROUP BY dataset, COALESCE(optional, false)
ORDER BY opcional, falhas DESC, dataset;
