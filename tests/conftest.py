"""Fixtures compartilhadas para os testes."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from src.common.schemas import (
    Channel,
    Currency,
    FraudType,
    MarketTradeEvent,
    MerchantCategory,
    TransactionEvent,
    TransactionType,
)


@pytest.fixture
def sample_transaction() -> TransactionEvent:
    return TransactionEvent(
        transaction_id="tx-001",
        customer_id="cust-001",
        timestamp=datetime(2024, 6, 15, 14, 30, 0, tzinfo=UTC),
        amount=1500.00,
        currency=Currency.BRL,
        transaction_type=TransactionType.PIX,
        merchant_category=MerchantCategory.TRANSFERENCIA,
        origin_account="0001-12345-6",
        destination_account="0002-98765-4",
        origin_bank="Banco do Brasil",
        destination_bank="Itaú",
        channel=Channel.APP_MOBILE,
        device_id="device-abc123",
        ip_address="192.168.1.100",
        latitude=-23.5505,
        longitude=-46.6333,
        is_fraud=False,
    )


@pytest.fixture
def sample_fraud_transaction() -> TransactionEvent:
    return TransactionEvent(
        transaction_id="tx-fraud-001",
        customer_id="cust-002",
        timestamp=datetime(2024, 6, 15, 3, 15, 0, tzinfo=UTC),
        amount=49999.99,
        currency=Currency.BRL,
        transaction_type=TransactionType.TED,
        merchant_category=MerchantCategory.TRANSFERENCIA,
        origin_account="0001-99999-9",
        destination_account="0003-11111-1",
        origin_bank="Caixa",
        destination_bank="Nubank",
        channel=Channel.INTERNET_BANKING,
        is_fraud=True,
        fraud_type=FraudType.ACCOUNT_TAKEOVER,
        fraud_score=0.95,
    )


@pytest.fixture
def sample_market_trade() -> MarketTradeEvent:
    return MarketTradeEvent(
        event_id="trade-001",
        symbol="PETR4.SA",
        timestamp=datetime(2024, 6, 15, 14, 30, 0, tzinfo=UTC),
        price=38.50,
        volume=1000,
        bid=38.45,
        ask=38.55,
        spread=0.10,
        source="simulator",
    )


@pytest.fixture
def mock_kafka_producer():
    producer = MagicMock()
    producer.send.return_value = MagicMock()
    producer.flush.return_value = None
    return producer


@pytest.fixture
def mock_minio_client():
    client = MagicMock()
    client.put_object.return_value = None
    client.bucket_exists.return_value = True
    return client
