"""Testes unitários para BronzeToSilverTransformer.

Todos os testes rodam em modo PySpark local (sem MinIO). DataFrames são
criados via arquivos JSON temporários para evitar incompatibilidade do
cloudpickle com Python 3.14 (RecursionError: Stack overflow).
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transformation.batch.bronze_to_silver import (
    _DEFAULT_FX_RATES,
    _VALID_CURRENCIES,
    BronzeToSilverTransformer,
)

# ── Fixtures de SparkSession ──────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local compartilhada por todos os testes da sessão."""
    session = (
        SparkSession.builder.appName("test-bronze-to-silver")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def transformer(spark: SparkSession) -> BronzeToSilverTransformer:
    return BronzeToSilverTransformer(spark=spark)


# ── Schemas explícitos ────────────────────────────────────────────────────────

_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("origin_account", StringType(), True),
        StructField("destination_account", StringType(), True),
        StructField("origin_bank", StringType(), True),
        StructField("destination_bank", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),
        StructField("fraud_score", DoubleType(), True),
    ]
)

_MARKET_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("date", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("adjusted_close", DoubleType(), True),
    ]
)

_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("cpf_masked", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("account_opening_date", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("segment", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("batch_id", StringType(), True),
    ]
)

_DATE_SCHEMA = StructType([StructField("transaction_date", DateType(), True)])


# ── Helpers para criar DataFrames via JSON temp file ──────────────────────────
# Evita cloudpickle (incompatível com Python 3.14) ao passar data diretamente.


def _serialize(value):
    """Converte tipos Python não serializáveis por JSON."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _df_from_rows(spark: SparkSession, rows: list[dict], schema: StructType):
    """Cria DataFrame a partir de lista de dicts via arquivo JSON temporário.

    Materializa o DataFrame em memória (cache) antes de deletar o arquivo para
    evitar SparkFileNotFoundException com lazy evaluation.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        for row in rows:
            serialized = {k: _serialize(v) for k, v in row.items() if v is not None}
            f.write(json.dumps(serialized) + "\n")
        tmp_path = f.name

    try:
        df = spark.read.schema(schema).json(tmp_path)
        df = df.cache()
        df.count()  # força materialização antes de deletar o arquivo
        return df
    finally:
        os.unlink(tmp_path)


# ── Dados de exemplo ──────────────────────────────────────────────────────────


def _sample_transaction(**overrides) -> dict:
    base: dict = {
        "transaction_id": "tx-001",
        "customer_id": "cust-001",
        "timestamp": datetime(2024, 6, 15, 14, 30, 0),
        "amount": 1500.00,
        "currency": "BRL",
        "transaction_type": "PIX",
        "merchant_category": "ALIMENTACAO",
        "origin_account": "0001-12345",
        "destination_account": "0002-67890",
        "origin_bank": "Banco do Brasil",
        "destination_bank": "Itaú",
        "channel": "APP_MOBILE",
        "device_id": "dev-001",
        "ip_address": "192.168.1.1",
        "latitude": -23.55,
        "longitude": -46.63,
        "is_fraud": False,
        "fraud_type": None,
        "fraud_score": None,
    }
    base.update(overrides)
    return base


def _sample_market_row(**overrides) -> dict:
    base: dict = {
        "symbol": "PETR4.SA",
        "date": date(2024, 6, 15),
        "open": 38.0,
        "high": 39.5,
        "low": 37.5,
        "close": 39.0,
        "volume": 1_000_000,
        "adjusted_close": 39.0,
    }
    base.update(overrides)
    return base


def _sample_customer(**overrides) -> dict:
    base: dict = {
        "customer_id": "cust-001",
        "name": "João Silva",
        "cpf_masked": "***.***.***.01",
        "birth_date": "1985-03-20",
        "gender": "M",
        "account_opening_date": "2015-01-10",
        "risk_score": 42.5,
        "segment": "VAREJO",
        "city": "São Paulo",
        "state": "SP",
        "country": "BR",
        "valid_from": "2024-06-15",
        "valid_to": "9999-12-31",
        "is_current": True,
        "ingestion_timestamp": "2024-06-15T06:00:00",
        "batch_id": "batch-001",
    }
    base.update(overrides)
    return base


def _make_tx(spark, rows):
    return _df_from_rows(spark, rows, _TRANSACTION_SCHEMA)


def _make_mkt(spark, rows):
    return _df_from_rows(spark, rows, _MARKET_SCHEMA)


def _make_cust(spark, rows):
    return _df_from_rows(spark, rows, _CUSTOMER_SCHEMA)


# ── TestCleanTransactions ─────────────────────────────────────────────────────


class TestCleanTransactions:
    def test_removes_null_transaction_id(self, spark: SparkSession, transformer):
        rows = [
            _sample_transaction(transaction_id=None),
            _sample_transaction(transaction_id="tx-001"),
        ]
        df = _make_tx(spark, rows)
        result = transformer._clean_transactions(df)
        assert result.count() == 1
        assert result.first()["transaction_id"] == "tx-001"

    def test_casts_amount_to_decimal(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(amount=1234.567)])
        result = transformer._clean_transactions(df)
        amount = result.first()["amount"]
        assert float(str(amount)) == pytest.approx(1234.57, rel=1e-2)

    def test_normalizes_currency_to_uppercase(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(currency="brl")])
        result = transformer._clean_transactions(df)
        assert result.first()["currency"] == "BRL"

    def test_filters_invalid_currency(self, spark: SparkSession, transformer):
        rows = [
            _sample_transaction(transaction_id="tx-valid", currency="BRL"),
            _sample_transaction(transaction_id="tx-invalid", currency="JPY"),
        ]
        df = _make_tx(spark, rows)
        result = transformer._clean_transactions(df)
        assert result.count() == 1
        assert result.first()["transaction_id"] == "tx-valid"

    def test_fills_null_channel(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(channel=None)])
        result = transformer._clean_transactions(df)
        assert result.first()["channel"] == "UNKNOWN"

    def test_fills_null_merchant_category(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(merchant_category=None)])
        result = transformer._clean_transactions(df)
        assert result.first()["merchant_category"] == "OUTROS"

    def test_deduplication_keeps_most_recent(self, spark: SparkSession, transformer):
        rows = [
            _sample_transaction(
                transaction_id="tx-dup",
                timestamp=datetime(2024, 6, 15, 10, 0, 0),
                amount=100.0,
            ),
            _sample_transaction(
                transaction_id="tx-dup",
                timestamp=datetime(2024, 6, 15, 12, 0, 0),
                amount=200.0,
            ),
        ]
        df = _make_tx(spark, rows)
        result = transformer._clean_transactions(df)
        assert result.count() == 1
        assert float(str(result.first()["amount"])) == pytest.approx(200.0, rel=1e-2)

    def test_no_rows_discarded_for_clean_data(self, spark: SparkSession, transformer):
        rows = [_sample_transaction(transaction_id=f"tx-{i}") for i in range(5)]
        df = _make_tx(spark, rows)
        result = transformer._clean_transactions(df)
        assert result.count() == 5


# ── TestEnrichTransactions ────────────────────────────────────────────────────


class TestEnrichTransactions:
    def test_adds_transaction_date(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(timestamp=datetime(2024, 6, 15, 14, 30))])
        result = transformer._enrich_transactions(df)
        assert str(result.first()["transaction_date"]) == "2024-06-15"

    def test_adds_transaction_hour(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(timestamp=datetime(2024, 6, 15, 14, 30))])
        result = transformer._enrich_transactions(df)
        assert result.first()["transaction_hour"] == 14

    def test_is_business_hours_true_at_10h(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(timestamp=datetime(2024, 6, 15, 10, 0))])
        result = transformer._enrich_transactions(df)
        assert result.first()["is_business_hours"] is True

    def test_is_business_hours_false_at_22h(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(timestamp=datetime(2024, 6, 15, 22, 0))])
        result = transformer._enrich_transactions(df)
        assert result.first()["is_business_hours"] is False

    def test_amount_brl_same_for_brl(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(amount=1000.0, currency="BRL")])
        result = transformer._enrich_transactions(df)
        assert result.first()["amount_brl"] == pytest.approx(1000.0)

    def test_amount_brl_converted_for_usd(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(amount=100.0, currency="USD")])
        result = transformer._enrich_transactions(df)
        expected = 100.0 * _DEFAULT_FX_RATES["USD"]
        assert result.first()["amount_brl"] == pytest.approx(expected)

    def test_amount_brl_converted_for_eur(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction(amount=50.0, currency="EUR")])
        result = transformer._enrich_transactions(df)
        expected = 50.0 * _DEFAULT_FX_RATES["EUR"]
        assert result.first()["amount_brl"] == pytest.approx(expected)

    def test_adds_processing_timestamp(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_transaction()])
        result = transformer._enrich_transactions(df)
        assert result.first()["processing_timestamp"] is not None

    def test_custom_fx_rates(self, spark: SparkSession):
        custom_rates = {"BRL": 1.0, "USD": 6.0, "EUR": 6.5}
        t = BronzeToSilverTransformer(spark=spark, fx_rates=custom_rates)
        df = _make_tx(spark, [_sample_transaction(amount=10.0, currency="USD")])
        result = t._enrich_transactions(df)
        assert result.first()["amount_brl"] == pytest.approx(60.0)


# ── TestCleanMarketData ───────────────────────────────────────────────────────


class TestCleanMarketData:
    def test_removes_zero_volume(self, spark: SparkSession, transformer):
        rows = [
            _sample_market_row(symbol="PETR4.SA", volume=0),
            _sample_market_row(symbol="VALE3.SA", volume=500_000),
        ]
        df = _make_mkt(spark, rows)
        result = transformer._clean_market_data(df)
        assert result.count() == 1
        assert result.first()["symbol"] == "VALE3.SA"

    def test_removes_negative_close(self, spark: SparkSession, transformer):
        rows = [
            _sample_market_row(symbol="ITUB4.SA", close=-1.0),
            _sample_market_row(symbol="BBDC4.SA", close=20.0),
        ]
        df = _make_mkt(spark, rows)
        result = transformer._clean_market_data(df)
        assert result.count() == 1
        assert result.first()["symbol"] == "BBDC4.SA"

    def test_valid_records_not_removed(self, spark: SparkSession, transformer):
        rows = [_sample_market_row(symbol=f"SYM{i}.SA") for i in range(5)]
        df = _make_mkt(spark, rows)
        result = transformer._clean_market_data(df)
        assert result.count() == 5


# ── TestCalculateMarketIndicators ─────────────────────────────────────────────


class TestCalculateMarketIndicators:
    def test_daily_return_positive(self, spark: SparkSession, transformer):
        df = _make_mkt(spark, [_sample_market_row(open=38.0, close=40.0)])
        result = transformer._calculate_market_indicators(df)
        expected = (40.0 - 38.0) / 38.0
        assert result.first()["daily_return"] == pytest.approx(expected, rel=1e-4)

    def test_daily_return_negative(self, spark: SparkSession, transformer):
        df = _make_mkt(spark, [_sample_market_row(open=40.0, close=38.0)])
        result = transformer._calculate_market_indicators(df)
        expected = (38.0 - 40.0) / 40.0
        assert result.first()["daily_return"] == pytest.approx(expected, rel=1e-4)

    def test_intraday_range(self, spark: SparkSession, transformer):
        df = _make_mkt(spark, [_sample_market_row(high=42.0, low=38.0)])
        result = transformer._calculate_market_indicators(df)
        expected = (42.0 - 38.0) / 38.0
        assert result.first()["intraday_range"] == pytest.approx(expected, rel=1e-4)

    def test_sma_columns_present(self, spark: SparkSession, transformer):
        rows = [
            _sample_market_row(symbol="PETR4.SA", date=date(2024, 1, i + 1), close=float(30 + i))
            for i in range(25)
        ]
        df = _make_mkt(spark, rows)
        result = transformer._calculate_market_indicators(df)
        assert "sma_5" in result.columns
        assert "sma_10" in result.columns
        assert "sma_20" in result.columns

    def test_sma_5_correct_value(self, spark: SparkSession, transformer):
        closes = [10.0, 20.0, 30.0, 40.0, 50.0]
        rows = [
            _sample_market_row(symbol="TEST.SA", date=date(2024, 1, i + 1), close=c)
            for i, c in enumerate(closes)
        ]
        df = _make_mkt(spark, rows)
        result = transformer._calculate_market_indicators(df).orderBy("date")
        last_sma5 = result.collect()[-1]["sma_5"]
        assert last_sma5 == pytest.approx(sum(closes) / 5, rel=1e-4)

    def test_adds_processing_timestamp(self, spark: SparkSession, transformer):
        df = _make_mkt(spark, [_sample_market_row()])
        result = transformer._calculate_market_indicators(df)
        assert result.first()["processing_timestamp"] is not None


# ── TestTransformCustomers ────────────────────────────────────────────────────


class TestTransformCustomers:
    def test_age_is_calculated(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_customer(birth_date="1990-01-01")])
        result = transformer._transform_customer_data(df)
        age = result.first()["age"]
        assert 30 <= age <= 40

    def test_age_group_18_24(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_customer(birth_date="2005-01-01")])
        result = transformer._transform_customer_data(df)
        assert result.first()["age_group"] == "18-24"

    def test_age_group_65_plus(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_customer(birth_date="1950-01-01")])
        result = transformer._transform_customer_data(df)
        assert result.first()["age_group"] == "65+"

    def test_age_group_25_34(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_customer(birth_date="1993-06-01")])
        result = transformer._transform_customer_data(df)
        assert result.first()["age_group"] in ("25-34", "35-44")

    def test_processing_timestamp_added(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_customer()])
        result = transformer._transform_customer_data(df)
        assert result.first()["processing_timestamp"] is not None

    def test_cpf_masked_preserved(self, spark: SparkSession, transformer):
        masked = "***.***.***.42"
        df = _make_cust(spark, [_sample_customer(cpf_masked=masked)])
        result = transformer._transform_customer_data(df)
        assert result.first()["cpf_masked"] == masked


# ── TestFilterByDate ──────────────────────────────────────────────────────────


class TestFilterByDate:
    def _make_dated_df(self, spark, dates: list[str]):
        rows = [{"transaction_date": d} for d in dates]
        return _df_from_rows(spark, rows, _DATE_SCHEMA)

    def test_filter_start_date(self, spark: SparkSession):
        t = BronzeToSilverTransformer(spark=spark, start_date="2024-06-10")
        df = self._make_dated_df(spark, ["2024-06-08", "2024-06-10", "2024-06-15"])
        result = t._filter_by_date(df, "transaction_date")
        assert result.count() == 2

    def test_filter_end_date(self, spark: SparkSession):
        t = BronzeToSilverTransformer(spark=spark, end_date="2024-06-12")
        df = self._make_dated_df(spark, ["2024-06-08", "2024-06-10", "2024-06-15"])
        result = t._filter_by_date(df, "transaction_date")
        assert result.count() == 2

    def test_filter_date_range(self, spark: SparkSession):
        t = BronzeToSilverTransformer(
            spark=spark, start_date="2024-06-09", end_date="2024-06-12"
        )
        df = self._make_dated_df(spark, ["2024-06-08", "2024-06-10", "2024-06-15"])
        result = t._filter_by_date(df, "transaction_date")
        assert result.count() == 1

    def test_no_filter_when_dates_are_none(self, spark: SparkSession, transformer):
        df = self._make_dated_df(spark, ["2024-01-01", "2024-06-01", "2024-12-31"])
        result = transformer._filter_by_date(df, "transaction_date")
        assert result.count() == 3


# ── TestPublicMethodsMocked ───────────────────────────────────────────────────


class TestPublicMethodsMocked:
    """Testa os métodos públicos mockando leitura/escrita no MinIO.

    `spark.read` é uma property que cria um novo DataFrameReader a cada acesso,
    por isso o mock é feito no nível da classe (pyspark.sql.DataFrameReader.parquet).
    """

    def test_transform_transactions_returns_metrics(self, spark: SparkSession):
        rows = [_sample_transaction(transaction_id=f"tx-{i}") for i in range(3)]
        mock_df = _make_tx(spark, rows)

        t = BronzeToSilverTransformer(spark=spark)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = t.transform_transactions()

        assert metrics["rows_read"] == 3
        assert metrics["rows_written"] <= 3
        assert "rows_discarded" in metrics

    def test_transform_market_data_returns_metrics(self, spark: SparkSession):
        rows = [_sample_market_row(symbol=f"SYM{i}.SA") for i in range(2)]
        mock_df = _make_mkt(spark, rows)

        t = BronzeToSilverTransformer(spark=spark)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = t.transform_market_data()

        assert metrics["rows_read"] == 2
        assert "rows_written" in metrics

    def test_transform_customers_returns_metrics(self, spark: SparkSession):
        rows = [_sample_customer(customer_id=f"cust-{i}") for i in range(4)]
        mock_df = _make_cust(spark, rows)

        t = BronzeToSilverTransformer(spark=spark)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = t.transform_customers()

        assert metrics["rows_read"] == 4
        assert metrics["rows_discarded"] == 0


# ── TestConstants ─────────────────────────────────────────────────────────────


class TestConstants:
    def test_valid_currencies_contain_expected(self):
        assert "BRL" in _VALID_CURRENCIES
        assert "USD" in _VALID_CURRENCIES
        assert "EUR" in _VALID_CURRENCIES

    def test_default_fx_brl_is_one(self):
        assert _DEFAULT_FX_RATES["BRL"] == 1.0

    def test_default_fx_usd_positive(self):
        assert _DEFAULT_FX_RATES["USD"] > 0

    def test_default_fx_eur_positive(self):
        assert _DEFAULT_FX_RATES["EUR"] > 0
