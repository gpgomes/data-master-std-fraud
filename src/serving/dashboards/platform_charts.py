"""Dashboard "Platform Health" (issue #55): saúde da plataforma, separada dos KPIs de negócio.

Lê as tabelas de métricas de `src/observability/schema.sql`: `stream_batch_metrics` (uma linha
por micro-batch, do StreamingQueryListener), `pipeline_runs` (uma por execução de DAG),
`quality_gate_runs` (uma por gate do GX) e `api_requests` (uma por request da API). Todos os charts
são `optional=True`: as tabelas só têm dado depois que o stream, as DAGs ou a API rodaram.

As metas de SLO não ficam aqui, e sim em `src/serving/queries/slo_report.sql` (`make slo-report`):
o dashboard mostra a série, o relatório diz se cumpre.
"""

from __future__ import annotations

from src.serving.dashboards.charts import ChartDef

PLATFORM_DASHBOARD_TITLE = "Platform Health"
PLATFORM_DASHBOARD_SLUG = "platform-health"
PLATFORM_DATASETS = ("stream_batch_metrics", "pipeline_runs", "quality_gate_runs", "api_requests")
# 3 KPIs do stream; 3 séries do stream; execuções das DAGs + gates com falha; 2 KPIs e a série da API
PLATFORM_ROW_SIZES = (3, 3, 2, 3)


def _sql(expression: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def _kpi(slice_name: str, table: str, metric: dict, description: str, where: str | None = None):
    form: dict = {"metric": metric}
    query: dict = {"metrics": [metric]}
    if where:
        form["adhoc_filters"] = [
            {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": where}
        ]
        query["extras"] = {"where": where}
    return ChartDef(
        slice_name=slice_name,
        viz_type="big_number_total",
        dataset_table=table,
        form_data_extra=form,
        query_extra=query,
        description=description,
        optional=True,
    )


def _timeseries(
    slice_name: str, table: str, time_col: str, metrics: list, description: str, groupby=None
):
    groupby = groupby or []
    return ChartDef(
        slice_name=slice_name,
        viz_type="echarts_timeseries_line",
        dataset_table=table,
        form_data_extra={
            "metrics": metrics,
            "groupby": groupby,
            "granularity_sqla": time_col,
            "time_grain_sqla": "PT1M",
        },
        query_extra={
            "metrics": metrics,
            "groupby": groupby,
            "is_timeseries": True,
            "granularity": time_col,
            "time_grain_sqla": "PT1M",
            "extras": {"time_grain_sqla": "PT1M"},
        },
        description=description,
        optional=True,
    )


STREAM_LATENCY_P95 = _sql(
    "percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_p95_s)", "Latencia p95 (s)"
)
BATCH_DURATION_P95 = _sql(
    "percentile_cont(0.95) WITHIN GROUP (ORDER BY batch_duration_ms)",
    "Duracao p95 do micro-batch (ms)",
)
MAX_LAG = _sql("MAX(kafka_lag)", "Lag maximo (eventos)")
API_P95 = _sql("percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)", "API p95 (ms)")
API_5XX_PCT = _sql(
    "SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) * 100",
    "Erros 5xx (%)",
)

PLATFORM_CHARTS: tuple[ChartDef, ...] = (
    _kpi(
        "Stream - Latencia p95 (s)",
        "stream_batch_metrics",
        STREAM_LATENCY_P95,
        "p95, entre os micro-batches, da latência p95 evento -> processamento de cada um.",
    ),
    _kpi(
        "Stream - Duracao p95 do Micro-batch (ms)",
        "stream_batch_metrics",
        BATCH_DURATION_P95,
        "p95 de triggerExecution (duração total do micro-batch) reportado pelo Spark.",
    ),
    _kpi(
        "Stream - Lag Maximo do Kafka",
        "stream_batch_metrics",
        MAX_LAG,
        "Maior lag observado: soma, por partição, de (último offset disponível - offset processado).",
    ),
    _timeseries(
        "Stream - Latencia e Duracao por Minuto",
        "stream_batch_metrics",
        "batch_timestamp",
        [
            _sql("AVG(latency_p95_s)", "Latencia p95 (s)"),
            _sql("AVG(batch_duration_ms) / 1000.0", "Duracao do micro-batch (s)"),
        ],
        "Média por minuto da latência p95 dos micro-batches e da sua duração.",
    ),
    _timeseries(
        "Stream - Throughput por Minuto",
        "stream_batch_metrics",
        "batch_timestamp",
        [
            _sql("AVG(input_rows_per_second)", "Entrada (linhas/s)"),
            _sql("AVG(processed_rows_per_second)", "Processadas (linhas/s)"),
        ],
        "Linhas/s de entrada x processadas: processadas abaixo da entrada por muito tempo = lag crescendo.",
    ),
    _timeseries(
        "Stream - Lag e Estado por Minuto",
        "stream_batch_metrics",
        "batch_timestamp",
        [_sql("MAX(kafka_lag)", "Lag (eventos)"), _sql("MAX(state_rows)", "Estado curto (linhas)")],
        "Lag do consumidor e tamanho do estado curto de 6 h lido a cada micro-batch.",
    ),
    ChartDef(
        slice_name="Pipeline - Execucoes por DAG",
        viz_type="table",
        dataset_table="pipeline_runs",
        form_data_extra={
            "query_mode": "aggregate",
            "groupby": ["dag_id", "state"],
            "metrics": [
                "count",
                _sql("AVG(duration_seconds)", "Duracao media (s)"),
                _sql("MAX(start_date)", "Ultima execucao"),
            ],
        },
        query_extra={
            "columns": ["dag_id", "state"],
            "metrics": [
                "count",
                _sql("AVG(duration_seconds)", "Duracao media (s)"),
                _sql("MAX(start_date)", "Ultima execucao"),
            ],
        },
        description="Execuções por DAG e estado, com a duração média e a mais recente.",
        optional=True,
    ),
    _kpi(
        "Pipeline - Quality Gates com Falha",
        "quality_gate_runs",
        {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Gates com falha"},
        "Execuções de gates obrigatórios (não opcionais) que falharam.",
        where="success = false AND NOT COALESCE(optional, false)",
    ),
    _kpi("API - Latencia p95 (ms)", "api_requests", API_P95, "p95 da duração das requests da API."),
    _kpi(
        "API - Erros 5xx (%)", "api_requests", API_5XX_PCT, "Fração das requests com status >= 500."
    ),
    _timeseries(
        "API - Requests por Minuto",
        "api_requests",
        "requested_at",
        ["count"],
        "Requests por minuto, por rota.",
        groupby=["route"],
    ),
)
