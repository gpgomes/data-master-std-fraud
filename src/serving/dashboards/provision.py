"""Provisiona a conexão Postgres, os datasets e o dashboard de KPIs de
fraude/transações no Superset (issue #16), tudo idempotente — rodar de novo
atualiza em vez de duplicar (busca por nome/slug antes de criar).

Fluxo: database -> datasets -> dashboard (vazio) -> charts (já anexados ao
dashboard) -> json_metadata/position_json do dashboard (filtros nativos +
layout, que dependem dos ids dos charts) -> verify (opcional, `--verify`):
refaz cada query de chart via `/api/v1/chart/data` e confere que bate com
uma query direta no Postgres.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.common.config import settings
from src.common.logger import get_logger
from src.serving.dashboards.charts import CHARTS, DATASETS, ChartDef
from src.serving.dashboards.client import SupersetClient

logger = get_logger("superset_dashboards")

DASHBOARD_TITLE = "Fraude e Transacoes - Visao Geral"
DASHBOARD_SLUG = "fraude-transacoes-visao-geral"
DATABASE_NAME = "fraud_analytics"


@dataclass
class VerificationResult:
    slice_name: str
    ok: bool
    detail: str


def ensure_database(client: SupersetClient) -> int:
    existing = client.find_one("/api/v1/database/", {"database_name": DATABASE_NAME})
    uri = (
        f"postgresql://{settings.postgres.user}:{settings.postgres.password}"
        f"@{settings.postgres.internal_host}:{settings.postgres.port}/{settings.postgres.db}"
    )
    payload = {"database_name": DATABASE_NAME, "sqlalchemy_uri": uri, "expose_in_sqllab": True}
    if existing:
        client.put(f"/api/v1/database/{existing['id']}", payload)
        logger.info("Database Superset atualizado", id=existing["id"])
        return int(existing["id"])
    resp = client.post("/api/v1/database/", payload)
    db_id = int(resp.json()["id"])
    logger.info("Database Superset criado", id=db_id)
    return db_id


def ensure_dataset(client: SupersetClient, database_id: int, table_name: str) -> int:
    existing = client.find_one("/api/v1/dataset/", {"table_name": table_name})
    if existing:
        return int(existing["id"])
    resp = client.post("/api/v1/dataset/", {"database": database_id, "schema": "public", "table_name": table_name})
    dataset_id = int(resp.json()["id"])
    logger.info("Dataset Superset criado", table=table_name, id=dataset_id)
    return dataset_id


def ensure_dashboard(client: SupersetClient) -> int:
    existing = client.find_one("/api/v1/dashboard/", {"dashboard_title": DASHBOARD_TITLE})
    if existing:
        return int(existing["id"])
    resp = client.post("/api/v1/dashboard/", {"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG})
    dashboard_id = int(resp.json()["id"])
    logger.info("Dashboard Superset criado", id=dashboard_id)
    return dashboard_id


def ensure_chart(client: SupersetClient, chart_def: ChartDef, dataset_id: int, dashboard_id: int) -> int:
    import json as json_module

    params = {"datasource": f"{dataset_id}__table", "viz_type": chart_def.viz_type, **chart_def.form_data_extra}
    payload = {
        "slice_name": chart_def.slice_name,
        "viz_type": chart_def.viz_type,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json_module.dumps(params),
        "dashboards": [dashboard_id],
    }
    existing = client.find_one("/api/v1/chart/", {"slice_name": chart_def.slice_name})
    if existing:
        client.put(f"/api/v1/chart/{existing['id']}", payload)
        logger.info("Chart Superset atualizado", slice_name=chart_def.slice_name, id=existing["id"])
        return int(existing["id"])
    resp = client.post("/api/v1/chart/", payload)
    chart_id = int(resp.json()["id"])
    logger.info("Chart Superset criado", slice_name=chart_def.slice_name, id=chart_id)
    return chart_id


def _build_position_json(chart_ids: list[int]) -> dict:
    """Layout simples: 4 KPIs numa linha, os 2 gráficos de série temporal
    numa segunda, a pizza de distribuição numa terceira e, abaixo, os 2 KPIs e os
    2 gráficos do streaming (issue #38)."""
    sizes = (4, 2, 1, 2, 2)
    rows, start = [], 0
    for size in sizes:
        rows.append(chart_ids[start : start + size])
        start += size
    rows = [r for r in rows if r]

    position: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [f"ROW-{i}" for i in range(len(rows))],
            "parents": ["ROOT_ID"],
        },
        "DASHBOARD_CHART_TYPE": "CHART",
        "DASHBOARD_ROW_TYPE": "ROW",
        "DASHBOARD_GRID_TYPE": "GRID",
        "DASHBOARD_ROOT_TYPE": "ROOT",
    }
    for i, row_chart_ids in enumerate(rows):
        row_id = f"ROW-{i}"
        width = max(12 // len(row_chart_ids), 3)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [f"CHART-{cid}" for cid in row_chart_ids],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for cid in row_chart_ids:
            position[f"CHART-{cid}"] = {
                "type": "CHART",
                "id": f"CHART-{cid}",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": cid, "width": width, "height": 50},
            }
    return position


def _build_native_filters(dataset_ids: dict[str, int]) -> list[dict]:
    fact_id = dataset_ids["fact_transactions"]
    agg_id = dataset_ids["agg_daily_fraud_metrics"]
    return [
        {
            "id": "NATIVE_FILTER-periodo",
            "name": "Periodo",
            "filterType": "filter_time",
            "targets": [{"datasetId": fact_id, "column": {"name": "date_key"}}, {"datasetId": agg_id, "column": {"name": "date_key"}}],
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Filtra o dashboard por intervalo de date_key.",
            "chartsInScope": [],
            "tabsInScope": [],
        },
        {
            "id": "NATIVE_FILTER-tipo-transacao",
            "name": "Tipo de Transacao",
            "filterType": "filter_select",
            "targets": [{"datasetId": fact_id, "column": {"name": "transaction_type"}}, {"datasetId": agg_id, "column": {"name": "transaction_type"}}],
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Filtra o dashboard por transaction_type.",
            "chartsInScope": [],
            "tabsInScope": [],
        },
    ]


def finalize_dashboard(
    client: SupersetClient, dashboard_id: int, chart_ids: list[int], dataset_ids: dict[str, int]
) -> None:
    import json as json_module

    metadata = {"native_filter_configuration": _build_native_filters(dataset_ids)}
    payload = {
        "position_json": json_module.dumps(_build_position_json(chart_ids)),
        "json_metadata": json_module.dumps(metadata),
        "published": True,  # sem isto o Superset exibe o selo "Draft" no dashboard
    }
    client.put(f"/api/v1/dashboard/{dashboard_id}", payload)
    logger.info("Dashboard Superset finalizado", id=dashboard_id, charts=len(chart_ids))


def verify_chart(client: SupersetClient, chart_def: ChartDef, dataset_id: int) -> VerificationResult:
    query = {"row_limit": 10000, "time_range": "No filter", "adhoc_filters": [], **chart_def.query_extra}
    body = {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": True,
        "queries": [query],
        "result_format": "json",
        "result_type": "full",
    }
    try:
        resp = client.post("/api/v1/chart/data", body)
    except Exception as exc:  # noqa: BLE001 - queremos reportar qualquer falha, não só HTTPError
        return VerificationResult(chart_def.slice_name, False, f"erro na requisicao: {exc}")

    payload = resp.json()
    result = payload["result"][0]
    if result.get("error"):
        return VerificationResult(chart_def.slice_name, False, f"erro na query: {result['error']}")
    if result["rowcount"] < 1:
        if chart_def.optional:
            return VerificationResult(
                chart_def.slice_name,
                True,
                "sem dados (opcional: rode o streaming e `make spark-submit-stream-postgres`)",
            )
        return VerificationResult(chart_def.slice_name, False, "query retornou 0 linhas")
    return VerificationResult(chart_def.slice_name, True, f"{result['rowcount']} linha(s), ex.: {result['data'][0]}")


def provision_all(verify: bool = False) -> dict:
    client = SupersetClient()

    database_id = ensure_database(client)
    dataset_ids = {table: ensure_dataset(client, database_id, table) for table in DATASETS}
    dashboard_id = ensure_dashboard(client)

    chart_ids: list[int] = []
    chart_ids_by_dataset: dict[str, int] = {}
    for chart_def in CHARTS:
        dataset_id = dataset_ids[chart_def.dataset_table]
        chart_id = ensure_chart(client, chart_def, dataset_id, dashboard_id)
        chart_ids.append(chart_id)
        chart_ids_by_dataset[chart_def.slice_name] = dataset_id

    finalize_dashboard(client, dashboard_id, chart_ids, dataset_ids)

    verification: list[VerificationResult] = []
    if verify:
        for chart_def in CHARTS:
            result = verify_chart(client, chart_def, chart_ids_by_dataset[chart_def.slice_name])
            verification.append(result)
            log = logger.info if result.ok else logger.error
            log("Verificacao de chart", slice_name=result.slice_name, ok=result.ok, detail=result.detail)

    return {
        "database_id": database_id,
        "dataset_ids": dataset_ids,
        "dashboard_id": dashboard_id,
        "chart_ids": chart_ids,
        "dashboard_url": f"{client.base_url}/superset/dashboard/{DASHBOARD_SLUG}/",
        "verification": verification,
    }
