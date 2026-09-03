"""Testes unitários para o módulo de geração de dados sintéticos."""

from datetime import UTC, datetime

import pytest

from src.common.data_generator import MARKET_SYMBOLS, DataGenerator
from src.common.schemas import (
    CPF_MASKED_PATTERN,
    Channel,
    Currency,
    CustomerRecord,
    CustomerSegment,
    FraudType,
    MerchantCategory,
    TransactionType,
)


@pytest.fixture
def gen() -> DataGenerator:
    return DataGenerator(seed=42)


@pytest.fixture
def small_customers(gen: DataGenerator) -> list[dict]:
    return gen.generate_customers(n=100)


class TestGenerateCustomers:
    def test_returns_correct_count(self, gen: DataGenerator) -> None:
        customers = gen.generate_customers(n=50)
        assert len(customers) == 50

    def test_required_fields_present(self, small_customers: list[dict]) -> None:
        required = {
            "customer_id",
            "name",
            "cpf_masked",
            "birth_date",
            "gender",
            "account_opening_date",
            "risk_score",
            "segment",
            "city",
            "state",
            "country",
        }
        for customer in small_customers:
            assert required.issubset(customer.keys())

    def test_unique_customer_ids(self, small_customers: list[dict]) -> None:
        ids = [c["customer_id"] for c in small_customers]
        assert len(ids) == len(set(ids))

    def test_risk_score_range(self, small_customers: list[dict]) -> None:
        for c in small_customers:
            assert 0.0 <= c["risk_score"] <= 100.0

    def test_valid_segment(self, small_customers: list[dict]) -> None:
        valid_segments = {s.value for s in CustomerSegment}
        for c in small_customers:
            assert c["segment"] in valid_segments

    def test_country_is_br(self, small_customers: list[dict]) -> None:
        for c in small_customers:
            assert c["country"] == "BR"

    def test_gender_values(self, small_customers: list[dict]) -> None:
        for c in small_customers:
            assert c["gender"] in ("M", "F")

    def test_reproducibility(self) -> None:
        g1 = DataGenerator(seed=99)
        g2 = DataGenerator(seed=99)
        c1 = g1.generate_customers(n=10)
        c2 = g2.generate_customers(n=10)
        assert [c["customer_id"] for c in c1] == [c["customer_id"] for c in c2]

    def test_cpf_masked_format(self, small_customers: list[dict]) -> None:
        """Regressão: cpf_masked não pode ter espaço nem expor mais que os 2 últimos dígitos."""
        for c in small_customers:
            assert CPF_MASKED_PATTERN.match(c["cpf_masked"]), c["cpf_masked"]

    def test_customers_validate_against_pydantic_model(
        self, small_customers: list[dict]
    ) -> None:
        for c in small_customers:
            CustomerRecord(**c)


class TestGenerateTransactions:
    def test_returns_correct_count(self, gen: DataGenerator, small_customers: list[dict]) -> None:
        txs = gen.generate_transactions(small_customers, n=200)
        assert len(txs) == 200

    def test_required_fields_present(self, gen: DataGenerator, small_customers: list[dict]) -> None:
        txs = gen.generate_transactions(small_customers, n=10)
        required = {
            "transaction_id",
            "customer_id",
            "timestamp",
            "amount",
            "currency",
            "transaction_type",
            "merchant_category",
            "origin_account",
            "destination_account",
            "origin_bank",
            "destination_bank",
            "channel",
            "is_fraud",
            "fraud_type",
        }
        for tx in txs:
            assert required.issubset(tx.keys())

    def test_unique_transaction_ids(self, gen: DataGenerator, small_customers: list[dict]) -> None:
        txs = gen.generate_transactions(small_customers, n=500)
        ids = [t["transaction_id"] for t in txs]
        assert len(ids) == len(set(ids))

    def test_amount_positive(self, gen: DataGenerator, small_customers: list[dict]) -> None:
        txs = gen.generate_transactions(small_customers, n=200)
        for tx in txs:
            assert tx["amount"] > 0

    def test_fraud_rate_approximately_correct(
        self, gen: DataGenerator, small_customers: list[dict]
    ) -> None:
        txs = gen.generate_transactions(small_customers, n=5_000)
        fraud_count = sum(1 for t in txs if t["is_fraud"])
        fraud_rate = fraud_count / len(txs)
        # Tolera variação: entre 1% e 5%
        assert 0.01 <= fraud_rate <= 0.05

    def test_fraud_has_fraud_type(self, gen: DataGenerator, small_customers: list[dict]) -> None:
        txs = gen.generate_transactions(small_customers, n=2_000)
        valid_fraud_types = {f.value for f in FraudType}
        for tx in txs:
            if tx["is_fraud"]:
                assert tx["fraud_type"] in valid_fraud_types
            else:
                assert tx["fraud_type"] is None

    def test_valid_transaction_type(
        self, gen: DataGenerator, small_customers: list[dict]
    ) -> None:
        txs = gen.generate_transactions(small_customers, n=100)
        valid_types = {t.value for t in TransactionType}
        for tx in txs:
            assert tx["transaction_type"] in valid_types

    def test_valid_merchant_category(
        self, gen: DataGenerator, small_customers: list[dict]
    ) -> None:
        txs = gen.generate_transactions(small_customers, n=100)
        valid_cats = {m.value for m in MerchantCategory}
        for tx in txs:
            assert tx["merchant_category"] in valid_cats

    def test_valid_currency(self, gen: DataGenerator, small_customers: list[dict]) -> None:
        txs = gen.generate_transactions(small_customers, n=100)
        valid_currencies = {c.value for c in Currency}
        for tx in txs:
            assert tx["currency"] in valid_currencies

    def test_valid_channel(self, gen: DataGenerator, small_customers: list[dict]) -> None:
        txs = gen.generate_transactions(small_customers, n=100)
        valid_channels = {c.value for c in Channel}
        for tx in txs:
            assert tx["channel"] in valid_channels

    def test_timestamp_within_range(
        self, gen: DataGenerator, small_customers: list[dict]
    ) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 6, 30, tzinfo=UTC)
        txs = gen.generate_transactions(small_customers, n=100, start_date=start, end_date=end)
        for tx in txs:
            ts = datetime.fromisoformat(tx["timestamp"])
            assert start <= ts <= end

    def test_customer_ids_from_input(
        self, gen: DataGenerator, small_customers: list[dict]
    ) -> None:
        valid_ids = {c["customer_id"] for c in small_customers}
        txs = gen.generate_transactions(small_customers, n=100)
        for tx in txs:
            assert tx["customer_id"] in valid_ids

    def test_fraud_score_never_set_by_generator(
        self, gen: DataGenerator, small_customers: list[dict]
    ) -> None:
        """fraud_score é derivado da detecção de fraude (streaming), não da geração/ingestão.

        A geração só conhece o ground truth (is_fraud/fraud_type); popular fraud_score
        aqui vazaria o rótulo para dentro do próprio dado bruto.
        """
        txs = gen.generate_transactions(small_customers, n=200)
        for tx in txs:
            assert "fraud_score" not in tx or tx["fraud_score"] is None


class TestGenerateMarketData:
    def test_returns_correct_count(self, gen: DataGenerator) -> None:
        records = gen.generate_market_data(n_days=10)
        assert len(records) == len(MARKET_SYMBOLS) * 10

    def test_custom_symbols(self, gen: DataGenerator) -> None:
        symbols = ["PETR4.SA", "VALE3.SA"]
        records = gen.generate_market_data(symbols=symbols, n_days=5)
        assert len(records) == 10

    def test_required_fields(self, gen: DataGenerator) -> None:
        records = gen.generate_market_data(n_days=5)
        required = {"symbol", "date", "open", "high", "low", "close", "volume", "adjusted_close"}
        for rec in records:
            assert required.issubset(rec.keys())

    def test_ohlcv_consistency(self, gen: DataGenerator) -> None:
        records = gen.generate_market_data(n_days=20)
        for rec in records:
            assert rec["high"] >= rec["low"]
            assert rec["open"] > 0
            assert rec["close"] > 0
            assert rec["volume"] >= 0

    def test_valid_symbols(self, gen: DataGenerator) -> None:
        records = gen.generate_market_data(n_days=5)
        returned_symbols = {r["symbol"] for r in records}
        assert returned_symbols == set(MARKET_SYMBOLS)

    def test_reproducibility(self) -> None:
        g1 = DataGenerator(seed=7)
        g2 = DataGenerator(seed=7)
        r1 = g1.generate_market_data(n_days=5)
        r2 = g2.generate_market_data(n_days=5)
        assert [r["close"] for r in r1] == [r["close"] for r in r2]
