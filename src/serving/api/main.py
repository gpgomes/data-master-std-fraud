"""API FastAPI da serving layer (issue #15) — consultas de transações, KPIs
de fraude e alertas recentes sobre o Postgres carregado pela issue #10.

Segurança (issue #58): toda rota fora de `/health` exige `X-API-Key` (ver `security.py`), e cada
request a essas rotas, autorizada ou não, entra no log de auditoria `api_access_audit`.

Execução:
    make api
    python -m uvicorn src.serving.api.main:app --reload
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import Depends, FastAPI, Request, Response
from starlette.background import BackgroundTasks
from starlette.concurrency import run_in_threadpool

from src.observability.store import MetricsStore
from src.serving.api.routes import alerts, health, kpis, transactions
from src.serving.api.security import require_api_key

app = FastAPI(
    title="Data Master — Fraud Detection Serving API",
    description="Consultas de transações, KPIs de fraude e alertas recentes.",
    version="1.0.0",
)

# Health checks abertos (orquestradores e load balancers os consultam sem credencial); o resto exige
# a API key.
app.include_router(health.router)
_protected = [Depends(require_api_key)]
app.include_router(transactions.router, dependencies=_protected)
app.include_router(alerts.router, dependencies=_protected)
app.include_router(kpis.router, dependencies=_protected)

# ── Métricas por request (issue #55) ─────────────────────────────────────────────

metrics_store = MetricsStore()


def _route_template(request: Request) -> str:
    """O caminho da rota (`/transactions/{transaction_id}`), não o da URL: agrupa por endpoint e
    não vaza ids para a tabela de métricas. Requests sem rota (404) caem em `unmatched`."""
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"


_QUERY_MAX_CHARS = 500


def _audit_row(request: Request, status: int) -> dict | None:
    """Linha do log de auditoria (issue #58), ou None para os health checks.

    Registra quem (o id da chave, nunca a chave), o quê (rota e parâmetros de consulta) e o
    resultado, inclusive os 401: tentativas sem credencial também são acesso a auditar."""
    route = _route_template(request)
    if route.startswith("/health"):
        return None
    return {
        "api_key_id": getattr(request.state, "api_key_id", None),
        "method": request.method,
        "route": route,
        "query": request.url.query[:_QUERY_MAX_CHARS] or None,
        "status_code": status,
        "client_host": request.client.host if request.client else None,
    }


def _attach_after_response(response: Response, row: dict, audit: dict | None = None) -> None:
    """Agenda as gravações para depois que a resposta foi enviada (background task do Starlette),
    preservando uma background task que a rota já tenha definido."""
    tasks = BackgroundTasks()
    if response.background is not None:
        tasks.add_task(response.background)
    tasks.add_task(metrics_store.record_api_request, row)
    if audit is not None:
        tasks.add_task(metrics_store.record_api_access, audit)
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
            audit = _audit_row(request, 500)
            if audit is not None:
                await run_in_threadpool(metrics_store.record_api_access, audit)
        raise
    if metrics_store.enabled:
        _attach_after_response(
            response, row(response.status_code), _audit_row(request, response.status_code)
        )
    return response
