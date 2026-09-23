"""Definição declarativa dos datasets/charts/dashboard do Superset (issue #16).

Cada `ChartDef` carrega duas formas do mesmo cálculo, deliberadamente
separadas:

- `form_data_extra`: mesclado nos `params` armazenados no chart (o que o
  Superset usa para renderizar o Explore/editor de gráfico na UI).
- `query_extra`: mesclado no `queries[0]` enviado a `/api/v1/chart/data`
  pelo smoke test (`provision.py::verify_chart`) — usado para *provar* que o
  gráfico retorna o dado real correto, comparando contra uma query direta
  no Postgres, sem depender de como o Superset traduz `params` -> query
  internamente.

Todos os valores abaixo (contagens, taxa de fraude, distribuição por
`fraud_type`) foram verificados manualmente contra `psql` real antes de
serem codificados aqui — ver `docs/testes_issue_16.txt`.

Streaming (issue #38): `fraud_score` continua sempre NULL em `fact_transactions` (o
batch nunca o calcula), mas o detector de streaming agora é carregado no Postgres em
`stream_scored_transactions` e `fraud_alerts` (`make spark-submit-stream-postgres`). Os
charts que leem essas tabelas são `optional=True`: sem dado de streaming a verificação
não os trata como falha.
"""

from __future__ import annotations

from dataclasses import dataclass

FRAUD_RATE_PCT_SQL = "SUM(fraud_count)::float / NULLIF(SUM(total_transactions), 0) * 100"
FRAUD_RATE_PCT_FACT_SQL = "SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) * 100"

DATASETS = ("fact_transactions", "agg_daily_fraud_metrics", "stream_scored_transactions", "fraud_alerts")


@dataclass(frozen=True)
class ChartDef:
    slice_name: str
    viz_type: str
    dataset_table: str
    form_data_extra: dict
    query_extra: dict
    description: str
    optional: bool = False  # depende de dado de streaming: 0 linhas na verificação não é falha


CHARTS: tuple[ChartDef, ...] = (
    ChartDef(
        slice_name="KPI - Volume de Transacoes",
        viz_type="big_number_total",
        dataset_table="agg_daily_fraud_metrics",
        form_data_extra={"metric": {"expressionType": "SIMPLE", "column": {"column_name": "total_transactions"}, "aggregate": "SUM", "label": "Volume"}},
        query_extra={"metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_transactions"}, "aggregate": "SUM", "label": "Volume"}]},
        description="Total de transacoes no periodo (SUM(total_transactions)).",
    ),
    ChartDef(
        slice_name="KPI - Valor Total (R$)",
        viz_type="big_number_total",
        dataset_table="agg_daily_fraud_metrics",
        form_data_extra={"metric": {"expressionType": "SIMPLE", "column": {"column_name": "total_amount_brl"}, "aggregate": "SUM", "label": "Valor Total (R$)"}},
        query_extra={"metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_amount_brl"}, "aggregate": "SUM", "label": "Valor Total (R$)"}]},
        description="Soma de amount_brl no periodo (SUM(total_amount_brl)).",
    ),
    ChartDef(
        slice_name="KPI - Taxa de Fraude (%)",
        viz_type="big_number_total",
        dataset_table="agg_daily_fraud_metrics",
        form_data_extra={"metric": {"expressionType": "SQL", "sqlExpression": FRAUD_RATE_PCT_SQL, "label": "Taxa de Fraude (%)"}},
        query_extra={"metrics": [{"expressionType": "SQL", "sqlExpression": FRAUD_RATE_PCT_SQL, "label": "Taxa de Fraude (%)"}]},
        description="fraud_count / total_transactions * 100, ponderado pelo periodo.",
    ),
    ChartDef(
        slice_name="KPI - Alertas de Fraude",
        viz_type="big_number_total",
        dataset_table="fact_transactions",
        form_data_extra={"metric": "count", "adhoc_filters": [{"clause": "WHERE", "expressionType": "SQL", "sqlExpression": "is_fraud = true"}]},
        query_extra={"metrics": ["count"], "extras": {"where": "is_fraud = true"}},
        description="COUNT(*) WHERE is_fraud = true — transacoes com fraude confirmada (fact_transactions).",
    ),
    ChartDef(
        slice_name="Volume Diario por Tipo de Transacao",
        viz_type="echarts_timeseries_bar",
        dataset_table="agg_daily_fraud_metrics",
        form_data_extra={
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_transactions"}, "aggregate": "SUM", "label": "Volume"}],
            "groupby": ["transaction_type"],
            "granularity_sqla": "date_key",
            "time_grain_sqla": "P1D",
        },
        query_extra={
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_transactions"}, "aggregate": "SUM", "label": "Volume"}],
            "groupby": ["transaction_type"],
            "is_timeseries": True,
            "granularity": "date_key",
            "time_grain_sqla": "P1D",
            "extras": {"time_grain_sqla": "P1D"},
        },
        description="SUM(total_transactions) por dia, agrupado por transaction_type.",
    ),
    ChartDef(
        slice_name="Taxa de Fraude Diaria (%)",
        viz_type="echarts_timeseries_line",
        dataset_table="agg_daily_fraud_metrics",
        form_data_extra={
            "metrics": [{"expressionType": "SQL", "sqlExpression": FRAUD_RATE_PCT_SQL, "label": "Taxa de Fraude (%)"}],
            "granularity_sqla": "date_key",
            "time_grain_sqla": "P1D",
        },
        query_extra={
            "metrics": [{"expressionType": "SQL", "sqlExpression": FRAUD_RATE_PCT_SQL, "label": "Taxa de Fraude (%)"}],
            "is_timeseries": True,
            "granularity": "date_key",
            "time_grain_sqla": "P1D",
            "extras": {"time_grain_sqla": "P1D"},
        },
        description="Taxa de fraude diaria (fraud_count/total_transactions * 100).",
    ),
    ChartDef(
        slice_name="Distribuicao de Fraude por Tipo",
        viz_type="pie",
        dataset_table="fact_transactions",
        form_data_extra={
            "metric": "count",
            "groupby": ["fraud_type"],
            "adhoc_filters": [{"clause": "WHERE", "expressionType": "SQL", "sqlExpression": "is_fraud = true"}],
        },
        query_extra={"metrics": ["count"], "groupby": ["fraud_type"], "extras": {"where": "is_fraud = true"}},
        description="COUNT(*) WHERE is_fraud = true, agrupado por fraud_type.",
    ),
    ChartDef(
        slice_name="KPI - Latencia Media do Streaming (s)",
        viz_type="big_number_total",
        dataset_table="stream_scored_transactions",
        form_data_extra={"metric": {"expressionType": "SIMPLE", "column": {"column_name": "latency_seconds"}, "aggregate": "AVG", "label": "Latencia Media (s)"}},
        query_extra={"metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "latency_seconds"}, "aggregate": "AVG", "label": "Latencia Media (s)"}]},
        description="AVG(latency_seconds): tempo entre a producao do evento e o processamento pelo detector (processing_timestamp - produced_at).",
        optional=True,
    ),
    ChartDef(
        slice_name="KPI - Alertas do Detector",
        viz_type="big_number_total",
        dataset_table="fraud_alerts",
        form_data_extra={"metric": "count"},
        query_extra={"metrics": ["count"]},
        description="COUNT(*) em fraud_alerts: alertas de Z-Score emitidos pelo detector de streaming (nao e o rotulo is_fraud do batch).",
        optional=True,
    ),
    ChartDef(
        slice_name="Distribuicao do Fraud Score (Streaming)",
        viz_type="echarts_timeseries_bar",
        dataset_table="stream_scored_transactions",
        form_data_extra={
            "x_axis": "fraud_score_bucket",
            "metrics": ["count"],
            "groupby": [],
            "adhoc_filters": [{"clause": "WHERE", "expressionType": "SQL", "sqlExpression": "fraud_score IS NOT NULL"}],
        },
        query_extra={"metrics": ["count"], "columns": ["fraud_score_bucket"], "extras": {"where": "fraud_score IS NOT NULL"}},
        description="COUNT(*) por faixa de fraud_score (arredondado a 0,1) entre as transacoes pontuadas; sem score = sem baseline de 2 transacoes na janela de 1h.",
        optional=True,
    ),
    ChartDef(
        slice_name="Alertas por Hora (Streaming)",
        viz_type="echarts_timeseries_bar",
        dataset_table="fraud_alerts",
        form_data_extra={
            "metrics": ["count"],
            "groupby": [],
            "granularity_sqla": "processed_at",
            "time_grain_sqla": "PT1H",
        },
        query_extra={
            "metrics": ["count"],
            "is_timeseries": True,
            "granularity": "processed_at",
            "time_grain_sqla": "PT1H",
            "extras": {"time_grain_sqla": "PT1H"},
        },
        description="Alertas do detector por hora de processamento (COUNT(*) em fraud_alerts).",
        optional=True,
    ),
)
