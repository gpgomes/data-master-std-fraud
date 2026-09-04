"""API FastAPI da serving layer (issue #15) — consultas de transações, KPIs
de fraude e alertas recentes sobre o Postgres carregado pela issue #10.

Execução:
    make api
    python -m uvicorn src.serving.api.main:app --reload
"""

from __future__ import annotations

from fastapi import FastAPI

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
