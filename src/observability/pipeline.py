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


def pipeline_run_row(
    dag_id: str,
    run_id: str,
    start_date: datetime | None,
    task_states: dict[str, Any],
    own_task_id: str,
    now: datetime | None = None,
) -> dict:
    """Linha de `pipeline_runs` a partir do estado das tasks da execução. Função pura (sem banco)."""
    end = now or datetime.now(tz=UTC)
    failed = sorted(
        task_id
        for task_id, state in task_states.items()
        if task_id != own_task_id and str(state) in FAILED_STATES
    )
    duration = (end - start_date).total_seconds() if start_date is not None else None
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": "failed" if failed else "success",
        "start_date": start_date.replace(tzinfo=None) if start_date is not None else None,
        "end_date": end.replace(tzinfo=None),
        "duration_seconds": duration,
        "failed_tasks": ",".join(failed) or None,
    }


def record_pipeline_run(store: MetricsStore | None = None, **context: Any) -> dict:
    """Callable da task `record_pipeline_run` das DAGs.

    No Airflow 3 a task não acessa o banco de metadados (o `dag_run.get_task_instances()` do 2.x
    não existe mais): o estado das outras tasks vem do Task SDK, que pergunta à execution API do
    api-server (issue #69). A resposta é `{run_id: {task_id: estado}}`.
    """
    dag_run, ti = context["dag_run"], context["ti"]
    states = ti.get_task_states(dag_id=dag_run.dag_id, run_ids=[dag_run.run_id])
    row = pipeline_run_row(
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        start_date=dag_run.start_date,
        task_states=states.get(dag_run.run_id, {}),
        own_task_id=ti.task_id,
    )
    (store or MetricsStore()).record_pipeline_run(row)
    return row
