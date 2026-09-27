"""API FastAPI da serving layer (issue #15) — consultas de transações, KPIs
de fraude e alertas recentes sobre o Postgres carregado pela issue #10.

Execução:
    make api
    python -m uvicorn src.serving.api.main:app --reload
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from starlette.background import BackgroundTask, BackgroundTasks
from starlette.concurrency import run_in_threadpool

from src.observability.store import MetricsStore
from src.serving.api.routes import alerts, health, kpis, transactions

app = FastAPI(
    title="Data Master — Fraud Detection Serving API",
    description="Consultas de transações, KPIs de fraude e alertas recentes.",
    version="1.0.0",
)

app.include_router(health.router)
app.include_router(transactions.router)
app.include_router(alerts.router)
app.include_router(kpis.router)

# ── Métricas por request (issue #55) ─────────────────────────────────────────────

metrics_store = MetricsStore()


def _route_template(request: Request) -> str:
    """O caminho da rota (`/transactions/{transaction_id}`), não o da URL: agrupa por endpoint e
    não vaza ids para a tabela de métricas. Requests sem rota (404) caem em `unmatched`."""
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"


def _attach_after_response(response: Response, row: dict) -> None:
    """Agenda a gravação para depois que a resposta foi enviada (background task do Starlette),
    preservando uma background task que a rota já tenha definido."""
    record = BackgroundTask(metrics_store.record_api_request, row)
    if response.background is None:
        response.background = record
        return
    tasks = BackgroundTasks()
    tasks.add_task(response.background)
    tasks.add_task(metrics_store.record_api_request, row)
    response.background = tasks


@app.middleware("http")
async def record_request_metrics(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Mede a latência e o status de cada request e grava em `api_requests` **depois** de enviar a
    resposta: a gravação não entra na latência medida nem atrasa o cliente. Uma exceção não
    tratada é registrada como status 500 e propagada."""
    started = time.perf_counter()

    def row(status: int) -> dict:
        return {
            "method": request.method,
            "route": _route_template(request),
            "status_code": status,
            "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        }

    try:
        response = await call_next(request)
    except Exception:
        if metrics_store.enabled:
            await run_in_threadpool(metrics_store.record_api_request, row(500))
        raise
    if metrics_store.enabled:
        _attach_after_response(response, row(response.status_code))
    return response
