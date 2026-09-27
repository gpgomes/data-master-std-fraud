-- Benchmark ONLINE do detector de fraude: Z-Score V1 x Fraud Engine V2 (issue #47).
--
-- Roda sobre `stream_scored_transactions`, que guarda os dois vereditos lado a lado para os
-- mesmos eventos (shadow scoring, issue #46):
--   * V1 (`zscore-v1`, shadow):        is_anomaly
--   * V2 (`multisignal-v2`, alerta):   is_fraud_predicted, fraud_score, fraud_type_predicted
--   * rótulo do gerador (ground truth): is_fraud, fraud_type. Só para medir: o detector nunca o lê.
--
-- Uso (precisa do streaming rodado e carregado: `make spark-submit-stream-postgres`):
--   make fraud-online-eval
--
-- É o par online do `make fraud-eval` (offline, docs/fraud_evaluation.md). As diferenças que
-- importam ao comparar os dois: aqui não há warm-up nem seeds separadas (o V2 já vem calibrado da
-- seed de validação), o dado é uma execução só, e a prevalência de fraude é a do producer.
--
-- Métricas: Precision = TP/(TP+FP), Recall = TP/(TP+FN), F1 = 2TP/(2TP+FP+FN),
-- FPR = FP/(FP+TN), FNR = FN/(TP+FN). Latência = processing_timestamp - produced_at.

\pset null '-'
\pset footer off

\echo
\echo '== 1. Volume =='
SELECT count(*)                                             AS eventos,
       count(*) FILTER (WHERE is_fraud)                     AS fraudes_rotulo,
       round(100.0 * count(*) FILTER (WHERE is_fraud) / NULLIF(count(*), 0), 2) AS prevalencia_pct,
       count(*) FILTER (WHERE is_fraud_predicted IS NULL)   AS sem_veredito_v2,
       min(event_time)                                      AS primeiro_evento,
       max(event_time)                                      AS ultimo_evento
FROM stream_scored_transactions;

\echo
\echo '== 2. Precision, Recall, F1, FPR por detector (mesmos eventos) =='
WITH verdicts AS (
    SELECT 'zscore-v1 (shadow)' AS detector, COALESCE(is_fraud, false) AS label,
           COALESCE(is_anomaly, false) AS alert
    FROM stream_scored_transactions
    UNION ALL
    SELECT 'multisignal-v2', COALESCE(is_fraud, false), COALESCE(is_fraud_predicted, false)
    FROM stream_scored_transactions
), confusion AS (
    SELECT detector,
           count(*) FILTER (WHERE alert AND label)         AS tp,
           count(*) FILTER (WHERE alert AND NOT label)     AS fp,
           count(*) FILTER (WHERE NOT alert AND label)     AS fn,
           count(*) FILTER (WHERE NOT alert AND NOT label) AS tn
    FROM verdicts
    GROUP BY detector
)
SELECT detector, tp, fp, fn, tn,
       round(100.0 * tp / NULLIF(tp + fp, 0), 1)             AS precision_pct,
       round(100.0 * tp / NULLIF(tp + fn, 0), 1)             AS recall_pct,
       round(200.0 * tp / NULLIF(2 * tp + fp + fn, 0), 1)    AS f1_pct,
       round(100.0 * fp / NULLIF(fp + tn, 0), 2)             AS fpr_pct,
       round(100.0 * fn / NULLIF(tp + fn, 0), 1)             AS fnr_pct,
       round(1000.0 * (tp + fp) / NULLIF(tp + fp + fn + tn, 0), 1) AS alertas_por_1000
FROM confusion
ORDER BY detector DESC;

\echo
\echo '== 3. Recall por tipo de fraude (rótulo), V1 x V2 =='
SELECT fraud_type                                                        AS tipo_rotulo,
       count(*)                                                          AS fraudes,
       round(100.0 * count(*) FILTER (WHERE COALESCE(is_anomaly, false)) / count(*), 1)
                                                                         AS recall_v1_pct,
       round(100.0 * count(*) FILTER (WHERE COALESCE(is_fraud_predicted, false)) / count(*), 1)
                                                                         AS recall_v2_pct
FROM stream_scored_transactions
WHERE is_fraud
GROUP BY fraud_type
ORDER BY fraud_type;

\echo
\echo '== 4. Quem pegou o quê (só fraudes) =='
SELECT count(*) FILTER (WHERE is_fraud_predicted AND is_anomaly)         AS ambos,
       count(*) FILTER (WHERE is_fraud_predicted AND NOT COALESCE(is_anomaly, false)) AS so_v2,
       count(*) FILTER (WHERE NOT COALESCE(is_fraud_predicted, false) AND is_anomaly) AS so_v1,
       count(*) FILTER (WHERE NOT COALESCE(is_fraud_predicted, false)
                          AND NOT COALESCE(is_anomaly, false))           AS nenhum
FROM stream_scored_transactions
WHERE is_fraud;

\echo
\echo '== 5. Tipo inferido pelo V2 x tipo do rótulo (fraudes que o V2 alertou) =='
SELECT fraud_type AS tipo_rotulo, COALESCE(fraud_type_predicted, '(sem tipo)') AS tipo_inferido,
       count(*) AS alertas
FROM stream_scored_transactions
WHERE is_fraud AND is_fraud_predicted
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

SELECT count(*) FILTER (WHERE fraud_type = fraud_type_predicted)  AS tipo_correto,
       count(*) FILTER (WHERE fraud_type_predicted IS NULL)        AS sem_tipo,
       count(*)                                                    AS fraudes_alertadas,
       round(100.0 * count(*) FILTER (WHERE fraud_type = fraud_type_predicted) / NULLIF(count(*), 0), 1)
                                                                   AS tipo_correto_pct
FROM stream_scored_transactions
WHERE is_fraud AND is_fraud_predicted;

\echo
\echo '== 6. Falsos positivos do V2 (legítimas alertadas): sinais mais frequentes =='
SELECT signal, count(*) AS falsos_positivos
FROM stream_scored_transactions,
     LATERAL unnest(string_to_array(NULLIF(fraud_signals, ''), ',')) AS signal
WHERE NOT is_fraud AND is_fraud_predicted
GROUP BY signal
ORDER BY falsos_positivos DESC, signal;

\echo
\echo '== 7. Latência evento -> processamento (s), p50 / p95 / p99 =='
SELECT count(*)                                                         AS eventos,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) AS p50,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) AS p95,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_seconds)::numeric, 2) AS p99,
       round(avg(latency_seconds)::numeric, 2)                          AS media
FROM stream_scored_transactions
WHERE latency_seconds IS NOT NULL;

\echo
\echo '== 8. Versões dos detectores nos dados =='
SELECT detector_version AS alerta, shadow_detector_version AS shadow, count(*) AS eventos
FROM stream_scored_transactions
GROUP BY 1, 2;
