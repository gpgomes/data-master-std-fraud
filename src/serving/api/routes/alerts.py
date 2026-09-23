"""Alertas do detector de fraude de streaming (Z-Score, issue #11).

Fonte de dados (issue #38): tabela `fraud_alerts` no Postgres, carregada de
`silver/transactions_stream/` por `stream_to_postgres.py` — os mesmos alertas do tópico
Kafka `fraud-alerts`, com `z_score`, `fraud_score` e `alert_reason`. A tabela só tem dados
depois que o job de streaming rodou e a carga foi executada (`make spark-submit-stream-postgres`).

A visão pelo **rótulo** de fraude do batch (o `is_fraud` do gerador, sem score) continua
disponível em `GET /transactions?is_fraud=true`.
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.serving.api import repository
from src.serving.api.db import get_db
from src.serving.api.models.schemas import AlertOut, Page, PaginationParams

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=Page[AlertOut])
def list_alerts(
    start_date: date | None = None,
    end_date: date | None = None,
    customer_id: str | None = None,
    pagination: PaginationParams = Depends(),
    db: Session = Depends(get_db),
) -> Page[AlertOut]:
    try:
        total = repository.count_alerts(
            db, start_date=start_date, end_date=end_date, customer_id=customer_id
        )
        rows = repository.list_alerts(
            db,
            start_date=start_date,
            end_date=end_date,
            customer_id=customer_id,
            limit=pagination.limit,
            offset=pagination.offset,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc

    return Page(
        items=[AlertOut(**row) for row in rows],
        total=total,
        page=pagination.page,
        page_size=pagination.page_size,
    )
