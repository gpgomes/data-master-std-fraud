"""Testes unitários para os Kafka producers."""

import json
from datetime import datetime, timezone
from threading import Event
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.streaming.producer_config import (
    HIGH_THROUGHPUT_CONFIG,
    LOW_LATENCY_CONFIG,
    ProducerConfig,
)
from src.ingestion.streaming.kafka_producer_market import TickSimulator, _serialize
from src.common.schemas import Channel, Currency, FraudType, MerchantCategory, TransactionType


# ── ProducerConfig ─────────────────────────────────────────────────────────────

class TestProducerConfig:
    def test_default_values(self) -> None:
        cfg = ProducerConfig()
        assert cfg.acks == "1"
        assert cfg.retries == 3
        assert cfg.batch_size == 16_384
        assert cfg.linger_ms == 10
        assert cfg.compression_type == "lz4"

    def test_to_kafka_python_dict_keys(self) -> None:
        cfg = ProducerConfig()
        d = cfg.to_kafka_python_dict()
        assert "bootstrap_servers" in d
        assert "acks" in d
        assert "retries" in d
        assert "batch_size" in d
        assert "linger_ms" in d
        assert "compression_type" in d
        assert callable(d["value_serializer"])
        assert callable(d["key_serializer"])

    def test_value_serializer_encodes_utf8(self) -> None:
        cfg = ProducerConfig()
        ser = cfg.to_kafka_python_dict()["value_serializer"]
        result = ser("hello")
        assert result == b"hello"

    def test_key_serializer_encodes_utf8(self) -> None:
        cfg = ProducerConfig()
        ser = cfg.to_kafka_python_dict()["key_serializer"]
        assert ser("key-abc") == b"key-abc"
        assert ser(None) is None

    def test_low_latency_profile(self) -> None:
        assert LOW_LATENCY_CONFIG.linger_ms == 0
        assert LOW_LATENCY_CONFIG.batch_size == 1

    def test_high_throughput_profile(self) -> None:
        assert HIGH_THROUGHPUT_CONFIG.linger_ms == 50
        assert HIGH_THROUGHPUT_CONFIG.batch_size == 65_536


# ── Serialização de mensagens ──────────────────────────────────────────────────

class TestMessageSerialization:
    def test_transaction_message_is_valid_json(self) -> None:
        from src.common.data_generator import DataGenerator

        gen = DataGenerator(seed=42)
        customers = gen.generate_customers(n=10)
        txs = gen.generate_transactions(customers, n=1)
        tx = txs[0]

        msg = {
            **tx,
            "produced_at": datetime.now(tz=timezone.utc).isoformat(),
            "source_system": "test",
        }
        serialized = json.dumps(msg, default=str)
        parsed = json.loads(serialized)

        assert parsed["transaction_id"] == tx["transaction_id"]
        assert parsed["source_system"] == "test"
        assert "produced_at" in parsed

    def test_transaction_required_fields(self) -> None:
        from src.common.data_generator import DataGenerator

        gen = DataGenerator(seed=1)
        customers = gen.generate_customers(n=5)
        txs = gen.generate_transactions(customers, n=5)

        required = {
            "transaction_id", "customer_id", "timestamp", "amount",
            "currency", "transaction_type", "merchant_category",
            "origin_account", "destination_account", "origin_bank",
            "destination_bank", "channel", "is_fraud",
        }
        for tx in txs:
            assert required.issubset(tx.keys())

    def test_transaction_type_valid(self) -> None:
        from src.common.data_generator import DataGenerator

        valid = {t.value for t in TransactionType}
        gen = DataGenerator(seed=2)
        customers = gen.generate_customers(n=10)
        txs = gen.generate_transactions(customers, n=50)
        for tx in txs:
            assert tx["transaction_type"] in valid

    def test_amount_within_range(self) -> None:
        from src.common.data_generator import DataGenerator

        gen = DataGenerator(seed=3)
        customers = gen.generate_customers(n=10)
        txs = gen.generate_transactions(customers, n=200)
        for tx in txs:
            assert 1.0 <= tx["amount"] <= 500_000.0

    def test_currency_distribution_brl_dominant(self) -> None:
        from src.common.data_generator import DataGenerator

        gen = DataGenerator(seed=4)
        customers = gen.generate_customers(n=50)
        txs = gen.generate_transactions(customers, n=1_000)
        brl_count = sum(1 for t in txs if t["currency"] == "BRL")
        assert brl_count / len(txs) >= 0.70

    def test_market_tick_serialize(self) -> None:
        sim = TickSimulator()
        tick = sim.next_tick("PETR4.SA")
        serialized = _serialize(tick)
        parsed = json.loads(serialized)
        assert parsed["symbol"] == "PETR4.SA"
        assert parsed["price"] > 0
        assert "event_id" in parsed


# ── TickSimulator ──────────────────────────────────────────────────────────────

class TestTickSimulator:
    def test_tick_required_fields(self) -> None:
        sim = TickSimulator()
        tick = sim.next_tick("VALE3.SA")
        required = {"event_id", "symbol", "timestamp", "price", "volume", "bid", "ask", "spread"}
        assert required.issubset(tick.keys())

    def test_tick_spread_is_ask_minus_bid(self) -> None:
        sim = TickSimulator()
        for symbol in ["PETR4.SA", "ITUB4.SA", "ABEV3.SA"]:
            tick = sim.next_tick(symbol)
            assert tick["ask"] >= tick["bid"]
            assert tick["spread"] >= 0

    def test_tick_price_positive(self) -> None:
        sim = TickSimulator()
        for _ in range(20):
            tick = sim.next_tick("MGLU3.SA")
            assert tick["price"] > 0

    def test_tick_volume_positive(self) -> None:
        sim = TickSimulator()
        for _ in range(10):
            tick = sim.next_tick("WEGE3.SA")
            assert tick["volume"] >= 1

    def test_tick_price_evolves(self) -> None:
        """Preço deve variar ao longo de múltiplos ticks."""
        sim = TickSimulator()
        prices = [sim.next_tick("PETR4.SA")["price"] for _ in range(50)]
        assert len(set(prices)) > 1

    def test_all_symbols_generate_ticks(self) -> None:
        from src.common.data_generator import MARKET_SYMBOLS

        sim = TickSimulator()
        for symbol in MARKET_SYMBOLS:
            tick = sim.next_tick(symbol)
            assert tick["symbol"] == symbol


# ── Producer run com mock Kafka ────────────────────────────────────────────────

class TestProducerRun:
    def test_transactions_producer_sends_and_stops(self) -> None:
        """Producer deve enviar ao menos uma mensagem e parar ao sinalizar."""
        import threading
        import time

        stop = Event()

        mock_future = MagicMock()
        mock_future.get.return_value = None
        mock_producer = MagicMock()
        mock_producer.send.return_value = mock_future

        # KafkaProducer é importado via 'from kafka import KafkaProducer' dentro de run()
        with patch("kafka.KafkaProducer", return_value=mock_producer):
            def _run():
                from src.ingestion.streaming.kafka_producer_transactions import run
                run(stop_event=stop)

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            time.sleep(0.3)
            stop.set()
            t.join(timeout=5)

        assert mock_producer.send.called
        assert mock_producer.flush.called

    def test_market_producer_sends_and_stops(self) -> None:
        """Market producer deve enviar ao menos um tick e parar."""
        import threading
        import time

        stop = Event()

        mock_future = MagicMock()
        mock_future.get.return_value = None
        mock_producer = MagicMock()
        mock_producer.send.return_value = mock_future

        with patch("kafka.KafkaProducer", return_value=mock_producer), \
             patch("src.ingestion.streaming.kafka_producer_market._is_pregao", return_value=True):
            def _run():
                from src.ingestion.streaming.kafka_producer_market import run
                run(stop_event=stop)

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            time.sleep(0.3)
            stop.set()
            t.join(timeout=5)

        assert mock_producer.send.called
        assert mock_producer.flush.called
