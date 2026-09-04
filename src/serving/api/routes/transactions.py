"""Consulta de transações (fact_transactions)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.serving.api import repository
from src.serving.api.db import get_db
from src.serving.api.models.schemas import Page, PaginationParams, TransactionOut

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.get("", response_model=Page[TransactionOut])
def list_transactions(
    start_date: date | None = None,
    end_date: date | None = None,
    customer_id: str | None = None,
    transaction_type: str | None = None,
    is_fraud: bool | None = None,
    pagination: PaginationParams = Depends(),
    db: Session = Depends(get_db),
) -> Page[TransactionOut]:
    try:
        total = repository.count_transactions(
            db,
            start_date=start_date,
            end_date=end_date,
            customer_id=customer_id,
            transaction_type=transaction_type,
            is_fraud=is_fraud,
        )
        rows = repository.list_transactions(
            db,
            start_date=start_date,
            end_date=end_date,
            customer_id=customer_id,
            transaction_type=transaction_type,
            is_fraud=is_fraud,
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


@router.get("/{transaction_id}", response_model=TransactionOut)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)) -> TransactionOut:
    try:
        row = repository.get_transaction(db, transaction_id)
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc

    if row is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return TransactionOut(**row)
