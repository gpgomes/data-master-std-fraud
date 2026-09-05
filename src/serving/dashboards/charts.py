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

Nota: o campo `fraud_score` (mencionado no KPI "score" da issue) está
sempre NULL nas 506k linhas de `fact_transactions` carregadas no Postgres —
só é populado pelo detector de streaming (issue #11), que nunca escreve na
camada Gold batch/Postgres. Mesma causa raiz do gap de "latência" (ver
issue #14 e `alerts.py`, issue #15). Por isso não há um chart de
distribuição de `fraud_score`; "alertas"/"taxa de fraude" (`is_fraud`,
`fraud_type`) são os KPIs de fraude realmente disponíveis.
"""

from __future__ import annotations

from dataclasses import dataclass

FRAUD_RATE_PCT_SQL = "SUM(fraud_count)::float / NULLIF(SUM(total_transactions), 0) * 100"
FRAUD_RATE_PCT_FACT_SQL = "SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) * 100"

DATASETS = ("fact_transactions", "agg_daily_fraud_metrics")


@dataclass(frozen=True)
class ChartDef:
    slice_name: str
    viz_type: str
    dataset_table: str
    form_data_extra: dict
    query_extra: dict
    description: str


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
)
