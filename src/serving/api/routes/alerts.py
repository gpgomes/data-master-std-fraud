"""Alertas de fraude recentes.

Fonte de dados (decisão issue #15): `fact_transactions` filtrado por
`is_fraud=true` — o rótulo de fraude já carregado no Postgres pela issue #10
(batch). O detector de streaming (issue #11) publica alertas de Z-Score no
tópico Kafka `fraud-alerts`, mas não os persiste em nenhuma tabela
consultável hoje; ligar essa fonte exigiria um consumidor Kafka novo, fora do
escopo desta issue (ver docs/test_status.md).
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.serving.api import repository
from src.serving.api.db import get_db
from src.serving.api.models.schemas import Page, PaginationParams, TransactionOut

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=Page[TransactionOut])
def list_alerts(
    start_date: date | None = None,
    end_date: date | None = None,
    customer_id: str | None = None,
    pagination: PaginationParams = Depends(),
    db: Session = Depends(get_db),
) -> Page[TransactionOut]:
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
        items=[TransactionOut(**row) for row in rows],
        total=total,
        page=pagination.page,
        page_size=pagination.page_size,
    )
