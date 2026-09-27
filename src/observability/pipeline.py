"""Registro de cada execução de DAG em `pipeline_runs` (issue #55).

Chamado por uma task `record_pipeline_run` com `trigger_rule="all_done"`, que roda mesmo quando
uma task anterior falhou: é justamente a execução com falha que mais interessa registrar.

Cuidado com o estado da DAG: no Airflow a execução só falha se alguma task **folha** falhar. Por
isso a task de registro nunca fica a jusante do `notify_completion`: as duas são folhas, e se
algo falhar o `notify_completion` fica `upstream_failed` e a DAG continua marcada como falha.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.observability.store import MetricsStore

FAILED_STATES = frozenset({"failed", "upstream_failed"})


def pipeline_run_row(dag_run: Any, own_task_id: str, now: datetime | None = None) -> dict:
    """Linha de `pipeline_runs` a partir de um `DagRun` do Airflow. Função pura (sem banco)."""
    end = now or datetime.now(tz=UTC)
    failed = sorted(
        ti.task_id
        for ti in dag_run.get_task_instances()
        if ti.task_id != own_task_id and str(ti.state) in FAILED_STATES
    )
    start = dag_run.start_date
    duration = (end - start).total_seconds() if start is not None else None
    return {
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "state": "failed" if failed else "success",
        "start_date": start.replace(tzinfo=None) if start is not None else None,
        "end_date": end.replace(tzinfo=None),
        "duration_seconds": duration,
        "failed_tasks": ",".join(failed) or None,
    }


def record_pipeline_run(store: MetricsStore | None = None, **context: Any) -> dict:
    """Callable da task `record_pipeline_run` das DAGs."""
    row = pipeline_run_row(context["dag_run"], context["task_instance"].task_id)
    (store or MetricsStore()).record_pipeline_run(row)
    return row
