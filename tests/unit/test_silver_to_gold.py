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
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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
        StructField("ingestion_timestamp", StringType(), True),
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
        "ingestion_timestamp": "2024-06-15T10:00:00+00:00",
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

    def test_dedups_multiple_current_snapshots_keeping_latest(
        self, spark: SparkSession, transformer
    ):
        """SCD2 simplificado do CustomerLoader não fecha snapshots antigos —
        mais de um registro is_current=True pode existir para o mesmo
        customer_id (um por dia em que `make seed-data` rodou)."""
        rows = [
            _sample_silver_customer(
                customer_id="cust-001",
                segment="VAREJO",
                ingestion_timestamp="2024-06-10T10:00:00+00:00",
            ),
            _sample_silver_customer(
                customer_id="cust-001",
                segment="PRIVATE",
                ingestion_timestamp="2024-06-15T10:00:00+00:00",
            ),
        ]
        df = _make_cust(spark, rows)
        result = transformer._build_dim_customers(df)
        assert result.count() == 1
        assert result.first()["segment"] == "PRIVATE"


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


# ── TestCustomerBehaviorProfile (issue #46) ───────────────────────────────────

# O Silver de transações que o perfil lê: `amount` é DECIMAL(18,2) (bronze_to_silver) e o restante
# são as colunas que o detector usa. É o shape real, não o `_SILVER_TRANSACTION_SCHEMA` de fatos.
_SILVER_EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("destination_account", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
    ]
)

# `gold/dim_customers`: a chave é `customer_key` e a data de abertura ainda é texto.
_DIM_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_key", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("account_opening_date", StringType(), True),
    ]
)


def _silver_event(i: int, customer: str = "cust-1", **overrides) -> dict:
    base: dict = {
        "transaction_id": f"tx-{customer}-{i}",
        "customer_id": customer,
        "timestamp": f"2026-01-{10 + i:02d}T15:00:00",
        "amount": 100.0 + i,
        "device_id": "dev-home",
        "ip_address": "177.10.20.30",
        "latitude": -23.55,
        "longitude": -46.63,
        "destination_account": "acc-mother",
        "is_fraud": False,
    }
    base.update(overrides)
    return base


def _dim_row(customer: str, **overrides) -> dict:
    base = {"customer_key": customer, "segment": "VAREJO", "account_opening_date": "2020-01-01"}
    base.update(overrides)
    return base


class TestBuildCustomerBehaviorProfile:
    @staticmethod
    def _profile(spark, events, customers):
        return SilverToGoldTransformer._build_customer_behavior_profile(
            _df_from_rows(spark, events, _SILVER_EVENT_SCHEMA),
            _df_from_rows(spark, customers, _DIM_CUSTOMERS_SCHEMA),
        )

    def test_one_row_per_customer_of_the_dimension(self, spark):
        events = [_silver_event(i) for i in range(5)]
        profile = self._profile(spark, events, [_dim_row("cust-1"), _dim_row("cust-2")])
        assert sorted(r["customer_id"] for r in profile.collect()) == ["cust-1", "cust-2"]

    def test_learns_what_is_normal_from_the_legitimate_history(self, spark):
        events = [_silver_event(i) for i in range(5)]
        row = self._profile(spark, events, [_dim_row("cust-1")]).first()
        assert row["has_profile"] is True
        assert row["n_history"] == 5
        assert row["known_devices"] == ["dev-home"]
        assert row["known_ip_prefixes"] == ["177.10.20"]
        assert row["known_destinations"] == ["acc-mother"]
        assert row["home_lat"] == pytest.approx(-23.55)
        assert 4.4 < row["mu_log"] < 4.8  # ln(100..104) com o Silver em DECIMAL(18,2)

    def test_fraud_rows_do_not_teach_the_profile(self, spark):
        events = [
            *[_silver_event(i) for i in range(5)],
            _silver_event(
                9, is_fraud=True, device_id="dev-thief", destination_account="acc-mule", amount=9000.0
            ),
        ]
        row = self._profile(spark, events, [_dim_row("cust-1")]).first()
        assert row["n_history"] == 5
        assert row["known_devices"] == ["dev-home"]
        assert row["mu_log"] < 5.0  # ln(9000) = 9,1 não entrou

    def test_account_opening_date_becomes_a_date(self, spark):
        events = [_silver_event(i) for i in range(5)]
        row = self._profile(spark, events, [_dim_row("cust-1")]).first()
        assert row["account_opening_date"] == date(2020, 1, 1)

    def test_customer_without_history_still_gets_a_neutral_row(self, spark):
        """Cliente novo (sem transação legítima) sai com `has_profile = false`, não some da tabela."""
        events = [_silver_event(i) for i in range(5)]
        rows = {
            r["customer_id"]: r
            for r in self._profile(spark, events, [_dim_row("cust-1"), _dim_row("new")]).collect()
        }
        assert rows["new"]["has_profile"] is False
        assert rows["new"]["n_history"] in (0, None)

    def test_columns_match_the_profile_schema_the_stream_reads(self, spark):
        from src.transformation.fraud.profile import PROFILE_SCHEMA

        profile = self._profile(spark, [_silver_event(0)], [_dim_row("cust-1")])
        assert profile.columns == [f.name for f in PROFILE_SCHEMA.fields]


class TestTransformCustomerBehaviorProfile:
    """Lê `silver/transactions/` e `gold/dim_customers/` e grava `gold/customer_behavior_profile/`
    de verdade (Parquet em diretório temporário), sem mocks de I/O."""

    @staticmethod
    def _transformer(spark, root) -> SilverToGoldTransformer:
        t = SilverToGoldTransformer(spark=spark)
        t._silver = f"{root.as_uri()}/silver"
        t._gold = f"{root.as_uri()}/gold"
        return t

    @staticmethod
    def _seed(spark, root, events, customers) -> None:
        _df_from_rows(spark, events, _SILVER_EVENT_SCHEMA).write.parquet(
            f"{root.as_uri()}/silver/transactions/"
        )
        _df_from_rows(spark, customers, _DIM_CUSTOMERS_SCHEMA).write.parquet(
            f"{root.as_uri()}/gold/dim_customers/"
        )

    def test_writes_the_profile_to_gold_and_reports_metrics(self, spark, tmp_path):
        events = [_silver_event(i) for i in range(5)]
        self._seed(spark, tmp_path, events, [_dim_row("cust-1"), _dim_row("new")])

        metrics = self._transformer(spark, tmp_path).transform_customer_behavior_profile()

        assert metrics == {"rows_read": 5, "rows_written": 2, "customers_with_profile": 1}
        written = spark.read.parquet(f"{tmp_path.as_uri()}/gold/customer_behavior_profile/")
        assert sorted(r["customer_id"] for r in written.collect()) == ["cust-1", "new"]

    def test_the_output_is_what_the_stream_processor_loads(self, spark, tmp_path):
        """Fecha o ciclo: o que o batch grava é lido pelo `StreamProcessor` sem conversão."""
        from src.transformation.streaming.stream_processor import StreamProcessor

        self._seed(spark, tmp_path, [_silver_event(i) for i in range(5)], [_dim_row("cust-1")])
        self._transformer(spark, tmp_path).transform_customer_behavior_profile()

        stream = StreamProcessor(spark=spark, dim_customers=_df_from_rows(spark, [], _DIM_CUSTOMERS_SCHEMA))
        stream._gold = f"{tmp_path.as_uri()}/gold"
        stream._load_static_inputs()

        assert stream._profile.first()["known_devices"] == ["dev-home"]

    def test_ignores_the_date_range_and_uses_the_whole_history(self, spark, tmp_path):
        """Um perfil parcial esconderia devices que o cliente usa há meses."""
        events = [_silver_event(i) for i in range(5)]
        self._seed(spark, tmp_path, events, [_dim_row("cust-1")])
        t = self._transformer(spark, tmp_path)
        t.start_date = t.end_date = "2026-01-14"

        assert t.transform_customer_behavior_profile()["rows_read"] == 5

    def test_is_part_of_run_all_and_runs_last(self, spark):
        t = SilverToGoldTransformer(spark=spark)
        order: list[str] = []
        names = (
            "transform_dim_customers",
            "transform_dim_date",
            "transform_fact_transactions",
            "transform_agg_daily_fraud_metrics",
            "transform_customer_behavior_profile",
        )
        with patch.multiple(
            t, **{n: (lambda n=n: order.append(n) or {}) for n in names}
        ):
            result = t.run_all()

        assert order == list(names)  # o perfil lê a dim_customers: depende dela
        assert "customer_behavior_profile" in result
