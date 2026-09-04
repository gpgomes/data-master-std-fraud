"""Consultas SQL contra a serving layer (fact_transactions, agg_daily_fraud_metrics).

SQL parametrizado via `sqlalchemy.text` (nunca interpolação de string) — os
filtros vêm de query params da API, então precisam ser tratados como entrada
não confiável.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

_TRANSACTION_COLUMNS = """
    transaction_id, customer_key, date_key, amount_brl, currency,
    transaction_type, channel, merchant_category, is_fraud, fraud_type,
    fraud_score, processing_timestamp
"""


def _transaction_filters(
    *,
    start_date: date | None,
    end_date: date | None,
    customer_id: str | None,
    transaction_type: str | None,
    is_fraud: bool | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if start_date is not None:
        clauses.append("date_key >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        clauses.append("date_key <= :end_date")
        params["end_date"] = end_date
    if customer_id is not None:
        clauses.append("customer_key = :customer_id")
        params["customer_id"] = customer_id
    if transaction_type is not None:
        clauses.append("transaction_type = :transaction_type")
        params["transaction_type"] = transaction_type
    if is_fraud is not None:
        clauses.append("is_fraud = :is_fraud")
        params["is_fraud"] = is_fraud
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def count_transactions(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    customer_id: str | None = None,
    transaction_type: str | None = None,
    is_fraud: bool | None = None,
) -> int:
    where, params = _transaction_filters(
        start_date=start_date,
        end_date=end_date,
        customer_id=customer_id,
        transaction_type=transaction_type,
        is_fraud=is_fraud,
    )
    row = db.execute(text(f"SELECT count(*) FROM fact_transactions {where}"), params).first()
    return int(row[0]) if row else 0


def list_transactions(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    customer_id: str | None = None,
    transaction_type: str | None = None,
    is_fraud: bool | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[RowMapping]:
    where, params = _transaction_filters(
        start_date=start_date,
        end_date=end_date,
        customer_id=customer_id,
        transaction_type=transaction_type,
        is_fraud=is_fraud,
    )
    params = {**params, "limit": limit, "offset": offset}
    sql = f"""
        SELECT {_TRANSACTION_COLUMNS}
        FROM fact_transactions
        {where}
        ORDER BY date_key DESC, transaction_id
        LIMIT :limit OFFSET :offset
    """
    return list(db.execute(text(sql), params).mappings().all())


def get_transaction(db: Session, transaction_id: str) -> RowMapping | None:
    sql = f"SELECT {_TRANSACTION_COLUMNS} FROM fact_transactions WHERE transaction_id = :id"
    return db.execute(text(sql), {"id": transaction_id}).mappings().first()


# ── Alertas (fact_transactions com is_fraud=true — ver docs/test_status.md,
# issue #15: o detector de streaming, issue #11, publica no Kafka mas não
# persiste em nenhuma tabela consultável hoje) ───────────────────────────────


def count_alerts(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    customer_id: str | None = None,
) -> int:
    return count_transactions(
        db, start_date=start_date, end_date=end_date, customer_id=customer_id, is_fraud=True
    )


def list_alerts(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    customer_id: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[RowMapping]:
    return list_transactions(
        db,
        start_date=start_date,
        end_date=end_date,
        customer_id=customer_id,
        is_fraud=True,
        limit=limit,
        offset=offset,
    )


# ── KPIs diários de fraude (agg_daily_fraud_metrics) ────────────────────────────


def _kpi_filters(
    *, start_date: date | None, end_date: date | None, transaction_type: str | None
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if start_date is not None:
        clauses.append("date_key >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        clauses.append("date_key <= :end_date")
        params["end_date"] = end_date
    if transaction_type is not None:
        clauses.append("transaction_type = :transaction_type")
        params["transaction_type"] = transaction_type
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def count_daily_fraud_metrics(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    transaction_type: str | None = None,
) -> int:
    where, params = _kpi_filters(
        start_date=start_date, end_date=end_date, transaction_type=transaction_type
    )
    row = db.execute(
        text(f"SELECT count(*) FROM agg_daily_fraud_metrics {where}"), params
    ).first()
    return int(row[0]) if row else 0


def list_daily_fraud_metrics(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    transaction_type: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[RowMapping]:
    where, params = _kpi_filters(
        start_date=start_date, end_date=end_date, transaction_type=transaction_type
    )
    params = {**params, "limit": limit, "offset": offset}
    sql = f"""
        SELECT date_key, transaction_type, total_transactions, total_amount_brl,
               avg_amount_brl, fraud_count, fraud_rate, processing_timestamp
        FROM agg_daily_fraud_metrics
        {where}
        ORDER BY date_key DESC, transaction_type
        LIMIT :limit OFFSET :offset
    """
    return list(db.execute(text(sql), params).mappings().all())
