"""KPIs diários de fraude (agg_daily_fraud_metrics)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.serving.api import repository
from src.serving.api.db import get_db
from src.serving.api.models.schemas import DailyFraudMetricOut, Page, PaginationParams

router = APIRouter(prefix="/kpis", tags=["kpis"])


@router.get("/fraud-daily", response_model=Page[DailyFraudMetricOut])
def list_daily_fraud_metrics(
    start_date: date | None = None,
    end_date: date | None = None,
    transaction_type: str | None = None,
    pagination: PaginationParams = Depends(),
    db: Session = Depends(get_db),
) -> Page[DailyFraudMetricOut]:
    try:
        total = repository.count_daily_fraud_metrics(
            db, start_date=start_date, end_date=end_date, transaction_type=transaction_type
        )
        rows = repository.list_daily_fraud_metrics(
            db,
            start_date=start_date,
            end_date=end_date,
            transaction_type=transaction_type,
            limit=pagination.limit,
            offset=pagination.offset,
        )
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc

    return Page(
        items=[DailyFraudMetricOut(**row) for row in rows],
        total=total,
        page=pagination.page,
        page_size=pagination.page_size,
    )
