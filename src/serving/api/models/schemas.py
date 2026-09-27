"""Modelos Pydantic de resposta da API (issue #15)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Generic, TypeVar

from fastapi import Query
from pydantic import BaseModel, Field, field_validator

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    """Envelope de paginação comum a todas as listagens."""

    items: list[T]
    total: int
    page: int
    page_size: int


class PaginationParams:
    """Dependency de paginação (page 1-indexed, page_size limitado a 100)."""

    def __init__(
        self,
        page: int = Query(default=1, ge=1, description="Página (1-indexed)"),
        page_size: int = Query(default=20, ge=1, le=100, description="Itens por página (máx. 100)"),
    ) -> None:
        self.page = page
        self.page_size = page_size

    @property
    def limit(self) -> int:
        return self.page_size

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


class TransactionOut(BaseModel):
    """Uma linha de `fact_transactions` (star schema Gold, issue #10)."""

    transaction_id: str
    customer_key: str | None = None
    date_key: date | None = None
    amount_brl: float | None = None
    currency: str | None = None
    transaction_type: str | None = None
    channel: str | None = None
    merchant_category: str | None = None
    is_fraud: bool | None = None
    fraud_type: str | None = None
    fraud_score: float | None = None
    processing_timestamp: datetime | None = None


class AlertOut(BaseModel):
    """Um alerta do Fraud Engine de streaming (`fraud_alerts`, issues #38 e #47).

    `fraud_type` é o tipo **inferido pelos sinais** (nulo quando nenhuma regra casou; nunca o
    rótulo do gerador). `fraud_signals` lista os sinais ativos que dispararam o alerta (a coluna
    `signals` da tabela, guardada como texto separado por vírgula). `z_score` é o do detector
    antigo, que segue calculado em paralelo, e pode ser nulo.
    """

    alert_id: str
    transaction_id: str
    customer_id: str | None = None
    event_time: datetime | None = None
    amount: float | None = None
    fraud_type: str | None = None
    fraud_score: float | None = None
    z_score: float | None = None
    alert_reason: str | None = None
    fraud_signals: list[str] = Field(default_factory=list)
    detector_version: str | None = None
    processed_at: datetime | None = None

    @field_validator("fraud_signals", mode="before")
    @classmethod
    def _split_signals(cls, value: object) -> object:
        """A tabela guarda os sinais como `"A,B"`; vazio ou nulo (linha antiga) vira lista vazia."""
        if value is None:
            return []
        if isinstance(value, str):
            return [signal for signal in (part.strip() for part in value.split(",")) if signal]
        return value


class DailyFraudMetricOut(BaseModel):
    """Uma linha de `agg_daily_fraud_metrics`."""

    date_key: date
    transaction_type: str
    total_transactions: int
    total_amount_brl: float
    avg_amount_brl: float
    fraud_count: int
    fraud_rate: float
    processing_timestamp: datetime | None = None
