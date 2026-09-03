"""Testes unitários para SilverToGoldTransformer.

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
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.batch.silver_to_gold import SilverToGoldTransformer

# ── Fixtures de SparkSession ──────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local compartilhada por todos os testes da sessão."""
    session = (
        SparkSession.builder.appName("test-silver-to-gold")
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
def transformer(spark: SparkSession) -> SilverToGoldTransformer:
    return SilverToGoldTransformer(spark=spark)


# ── Schemas explícitos ────────────────────────────────────────────────────────

_SILVER_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("amount_brl", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),
        StructField("fraud_score", DoubleType(), True),
    ]
)

_SILVER_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("cpf_masked", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("age_group", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("account_opening_date", StringType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)

_DATE_SCHEMA = StructType([StructField("transaction_date", DateType(), True)])

# Schema real do batch: fraud_score não existe (só é escrito pelo streaming,
# ainda não implementado). Usado para reproduzir o bug encontrado em produção.
_SILVER_TRANSACTION_SCHEMA_NO_FRAUD_SCORE = StructType(
    [f for f in _SILVER_TRANSACTION_SCHEMA.fields if f.name != "fraud_score"]
)


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


def _sample_silver_transaction(**overrides) -> dict:
    base: dict = {
        "transaction_id": "tx-001",
        "customer_id": "cust-001",
        "transaction_date": date(2024, 6, 15),
        "amount_brl": 1500.0,
        "currency": "BRL",
        "transaction_type": "PIX",
        "channel": "APP_MOBILE",
        "merchant_category": "ALIMENTACAO",
        "is_fraud": False,
        "fraud_type": None,
        "fraud_score": None,
    }
    base.update(overrides)
    return base


def _sample_silver_customer(**overrides) -> dict:
    base: dict = {
        "customer_id": "cust-001",
        "name": "João Silva",
        "cpf_masked": "***.***.***-01",
        "gender": "M",
        "birth_date": "1985-03-20",
        "age": 39,
        "age_group": "35-44",
        "segment": "VAREJO",
        "city": "São Paulo",
        "state": "SP",
        "country": "BR",
        "risk_score": 42.5,
        "account_opening_date": "2015-01-10",
        "is_current": True,
    }
    base.update(overrides)
    return base


def _make_tx(spark, rows):
    return _df_from_rows(spark, rows, _SILVER_TRANSACTION_SCHEMA)


def _make_cust(spark, rows):
    return _df_from_rows(spark, rows, _SILVER_CUSTOMER_SCHEMA)


# ── TestBuildDimCustomers ──────────────────────────────────────────────────────


class TestBuildDimCustomers:
    def test_keeps_only_current_customers(self, spark: SparkSession, transformer):
        rows = [
            _sample_silver_customer(customer_id="cust-current", is_current=True),
            _sample_silver_customer(customer_id="cust-old", is_current=False),
        ]
        df = _make_cust(spark, rows)
        result = transformer._build_dim_customers(df)
        assert result.count() == 1
        assert result.first()["customer_key"] == "cust-current"

    def test_renames_customer_id_to_customer_key(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_silver_customer()])
        result = transformer._build_dim_customers(df)
        assert "customer_key" in result.columns
        assert "customer_id" not in result.columns

    def test_preserves_dimension_attributes(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_silver_customer(segment="PRIVATE", city="Recife")])
        result = transformer._build_dim_customers(df)
        row = result.first()
        assert row["segment"] == "PRIVATE"
        assert row["city"] == "Recife"

    def test_adds_processing_timestamp(self, spark: SparkSession, transformer):
        df = _make_cust(spark, [_sample_silver_customer()])
        result = transformer._build_dim_customers(df)
        assert result.first()["processing_timestamp"] is not None


# ── TestBuildDimDate ────────────────────────────────────────────────────────────


class TestBuildDimDate:
    def _make_dates(self, spark, dates: list[date]):
        rows = [{"transaction_date": d} for d in dates]
        return _df_from_rows(spark, rows, _DATE_SCHEMA)

    def test_renames_transaction_date_to_date_key(self, spark: SparkSession, transformer):
        df = self._make_dates(spark, [date(2024, 6, 15)])
        result = transformer._build_dim_date(df)
        assert "date_key" in result.columns

    def test_year_month_day_correct(self, spark: SparkSession, transformer):
        df = self._make_dates(spark, [date(2024, 6, 15)])
        row = transformer._build_dim_date(df).first()
        assert row["year"] == 2024
        assert row["month"] == 6
        assert row["day"] == 15

    def test_quarter_correct(self, spark: SparkSession, transformer):
        df = self._make_dates(spark, [date(2024, 11, 1)])
        row = transformer._build_dim_date(df).first()
        assert row["quarter"] == 4

    def test_is_weekend_true_for_saturday(self, spark: SparkSession, transformer):
        # 2024-06-15 é um sábado
        df = self._make_dates(spark, [date(2024, 6, 15)])
        row = transformer._build_dim_date(df).first()
        assert row["is_weekend"] is True

    def test_is_weekend_false_for_weekday(self, spark: SparkSession, transformer):
        # 2024-06-12 é uma quarta-feira
        df = self._make_dates(spark, [date(2024, 6, 12)])
        row = transformer._build_dim_date(df).first()
        assert row["is_weekend"] is False

    def test_week_of_year_present(self, spark: SparkSession, transformer):
        df = self._make_dates(spark, [date(2024, 6, 15)])
        row = transformer._build_dim_date(df).first()
        assert row["week_of_year"] is not None

    def test_day_name_present(self, spark: SparkSession, transformer):
        df = self._make_dates(spark, [date(2024, 6, 15)])
        row = transformer._build_dim_date(df).first()
        assert row["day_name"] is not None

    def test_no_duplicate_dates_lost(self, spark: SparkSession, transformer):
        dates = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
        df = self._make_dates(spark, dates)
        result = transformer._build_dim_date(df)
        assert result.count() == 3


# ── TestBuildFactTransactions ───────────────────────────────────────────────────


class TestBuildFactTransactions:
    def test_renames_keys(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_silver_transaction()])
        result = transformer._build_fact_transactions(df)
        assert "customer_key" in result.columns
        assert "date_key" in result.columns
        assert "customer_id" not in result.columns
        assert "transaction_date" not in result.columns

    def test_preserves_measures(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_silver_transaction(amount_brl=999.9, is_fraud=True)])
        row = transformer._build_fact_transactions(df).first()
        assert row["amount_brl"] == pytest.approx(999.9)
        assert row["is_fraud"] is True

    def test_one_row_per_transaction(self, spark: SparkSession, transformer):
        rows = [_sample_silver_transaction(transaction_id=f"tx-{i}") for i in range(5)]
        df = _make_tx(spark, rows)
        result = transformer._build_fact_transactions(df)
        assert result.count() == 5

    def test_adds_processing_timestamp(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_silver_transaction()])
        result = transformer._build_fact_transactions(df)
        assert result.first()["processing_timestamp"] is not None

    def test_handles_missing_fraud_score_column(self, spark: SparkSession, transformer):
        """fraud_score só existe no Silver depois que o streaming for implementado;
        no batch puro a coluna está ausente (não apenas nula) — não pode quebrar."""
        rows = [_sample_silver_transaction()]
        for row in rows:
            row.pop("fraud_score", None)
        df = _df_from_rows(spark, rows, _SILVER_TRANSACTION_SCHEMA_NO_FRAUD_SCORE)
        result = transformer._build_fact_transactions(df)
        assert "fraud_score" in result.columns
        assert result.first()["fraud_score"] is None


# ── TestBuildAggDailyFraudMetrics ───────────────────────────────────────────────


class TestBuildAggDailyFraudMetrics:
    def test_groups_by_date_and_type(self, spark: SparkSession, transformer):
        rows = [
            _sample_silver_transaction(
                transaction_id="tx-1", transaction_type="PIX", transaction_date=date(2024, 6, 15)
            ),
            _sample_silver_transaction(
                transaction_id="tx-2", transaction_type="TED", transaction_date=date(2024, 6, 15)
            ),
        ]
        df = _make_tx(spark, rows)
        result = transformer._build_agg_daily_fraud_metrics(df)
        assert result.count() == 2

    def test_total_transactions_correct(self, spark: SparkSession, transformer):
        rows = [
            _sample_silver_transaction(transaction_id=f"tx-{i}", transaction_type="PIX")
            for i in range(4)
        ]
        df = _make_tx(spark, rows)
        row = transformer._build_agg_daily_fraud_metrics(df).first()
        assert row["total_transactions"] == 4

    def test_total_amount_brl_correct(self, spark: SparkSession, transformer):
        rows = [
            _sample_silver_transaction(transaction_id="tx-1", amount_brl=100.0),
            _sample_silver_transaction(transaction_id="tx-2", amount_brl=200.0),
        ]
        df = _make_tx(spark, rows)
        row = transformer._build_agg_daily_fraud_metrics(df).first()
        assert row["total_amount_brl"] == pytest.approx(300.0)

    def test_fraud_count_and_rate(self, spark: SparkSession, transformer):
        rows = [
            _sample_silver_transaction(transaction_id="tx-1", is_fraud=True),
            _sample_silver_transaction(transaction_id="tx-2", is_fraud=False),
            _sample_silver_transaction(transaction_id="tx-3", is_fraud=False),
            _sample_silver_transaction(transaction_id="tx-4", is_fraud=False),
        ]
        df = _make_tx(spark, rows)
        row = transformer._build_agg_daily_fraud_metrics(df).first()
        assert row["fraud_count"] == 1
        assert row["fraud_rate"] == pytest.approx(0.25)

    def test_adds_processing_timestamp(self, spark: SparkSession, transformer):
        df = _make_tx(spark, [_sample_silver_transaction()])
        result = transformer._build_agg_daily_fraud_metrics(df)
        assert result.first()["processing_timestamp"] is not None


# ── TestFilterByDate ──────────────────────────────────────────────────────────


class TestFilterByDate:
    def _make_dated_df(self, spark, dates: list[str]):
        rows = [{"transaction_date": d} for d in dates]
        return _df_from_rows(spark, rows, _DATE_SCHEMA)

    def test_filter_start_date(self, spark: SparkSession):
        t = SilverToGoldTransformer(spark=spark, start_date="2024-06-10")
        df = self._make_dated_df(spark, ["2024-06-08", "2024-06-10", "2024-06-15"])
        result = t._filter_by_date(df, "transaction_date")
        assert result.count() == 2

    def test_filter_end_date(self, spark: SparkSession):
        t = SilverToGoldTransformer(spark=spark, end_date="2024-06-12")
        df = self._make_dated_df(spark, ["2024-06-08", "2024-06-10", "2024-06-15"])
        result = t._filter_by_date(df, "transaction_date")
        assert result.count() == 2

    def test_filter_date_range(self, spark: SparkSession):
        t = SilverToGoldTransformer(spark=spark, start_date="2024-06-09", end_date="2024-06-12")
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

    def test_transform_dim_customers_returns_metrics(self, spark: SparkSession):
        rows = [
            _sample_silver_customer(customer_id="cust-1", is_current=True),
            _sample_silver_customer(customer_id="cust-2", is_current=False),
        ]
        mock_df = _make_cust(spark, rows)

        t = SilverToGoldTransformer(spark=spark)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = t.transform_dim_customers()

        assert metrics["rows_read"] == 2
        assert metrics["rows_written"] == 1
        assert metrics["rows_discarded"] == 1

    def test_transform_dim_date_returns_metrics(self, spark: SparkSession):
        rows = [_sample_silver_transaction(transaction_id=f"tx-{i}") for i in range(3)]
        mock_df = _make_tx(spark, rows)

        t = SilverToGoldTransformer(spark=spark)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = t.transform_dim_date()

        # todas as transações usam a mesma transaction_date por padrão
        assert metrics["rows_written"] == 1

    def test_transform_fact_transactions_returns_metrics(self, spark: SparkSession):
        rows = [_sample_silver_transaction(transaction_id=f"tx-{i}") for i in range(3)]
        mock_df = _make_tx(spark, rows)

        t = SilverToGoldTransformer(spark=spark)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = t.transform_fact_transactions()

        assert metrics["rows_read"] == 3
        assert metrics["rows_written"] == 3
        assert metrics["rows_discarded"] == 0

    def test_transform_agg_daily_fraud_metrics_returns_metrics(self, spark: SparkSession):
        rows = [_sample_silver_transaction(transaction_id=f"tx-{i}") for i in range(3)]
        mock_df = _make_tx(spark, rows)

        t = SilverToGoldTransformer(spark=spark)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = t.transform_agg_daily_fraud_metrics()

        assert metrics["rows_read"] == 3
        assert metrics["rows_written"] == 1  # mesma data + mesmo tipo => 1 grupo
