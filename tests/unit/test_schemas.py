"""Testes unitários para os schemas Pydantic."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.common.schemas import (
    Channel,
    Currency,
    MarketTradeEvent,
    MerchantCategory,
    TransactionEvent,
    TransactionType,
)


class TestTransactionEvent:
    def test_valid_transaction(self, sample_transaction):
        assert sample_transaction.transaction_id == "tx-001"
        assert sample_transaction.amount == 1500.00
        assert sample_transaction.is_fraud is False

    def test_fraud_transaction_requires_fraud_type(self):
        with pytest.raises(ValidationError, match="fraud_type"):
            TransactionEvent(
                transaction_id="tx-bad",
                customer_id="cust-bad",
                timestamp=datetime.now(tz=UTC),
                amount=100.0,
                currency=Currency.BRL,
                transaction_type=TransactionType.PIX,
                merchant_category=MerchantCategory.TRANSFERENCIA,
                origin_account="0001",
                destination_account="0002",
                origin_bank="BankA",
                destination_bank="BankB",
                channel=Channel.APP_MOBILE,
                is_fraud=True,
                fraud_type=None,
            )

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            TransactionEvent(
                transaction_id="tx-neg",
                customer_id="cust-neg",
                timestamp=datetime.now(tz=UTC),
                amount=-50.0,
                currency=Currency.BRL,
                transaction_type=TransactionType.PIX,
                merchant_category=MerchantCategory.SERVICOS,
                origin_account="0001",
                destination_account="0002",
                origin_bank="BankA",
                destination_bank="BankB",
                channel=Channel.APP_MOBILE,
            )

    def test_fraud_score_range(self, sample_fraud_transaction):
        assert 0.0 <= sample_fraud_transaction.fraud_score <= 1.0

    def test_serialization(self, sample_transaction):
        data = sample_transaction.model_dump()
        assert "transaction_id" in data
        assert "timestamp" in data


class TestMarketTradeEvent:
    def test_valid_trade(self, sample_market_trade):
        assert sample_market_trade.symbol == "PETR4.SA"
        assert sample_market_trade.spread == pytest.approx(0.10)

    def test_negative_price_rejected(self):
        with pytest.raises(ValidationError):
            MarketTradeEvent(
                event_id="e-001",
                symbol="VALE3.SA",
                timestamp=datetime.now(tz=UTC),
                price=-10.0,
                volume=100,
                bid=10.0,
                ask=10.1,
                spread=0.1,
            )
