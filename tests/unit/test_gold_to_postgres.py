"""Testes unitários para GoldToPostgresLoader (issue #10).

Roda em modo PySpark local (sem infra Docker) e mocka psycopg2 (DDL) e o
DataFrameWriter/Reader do Spark (leitura do Gold + escrita JDBC).
"""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.common.config import settings
from src.serving.loaders.gold_to_postgres import (
    _DATASETS,
    _SCHEMA_SQL_PATH,
    GoldToPostgresLoader,
)

# ── Fixtures de SparkSession ──────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("test-gold-to-postgres")
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
def loader(spark: SparkSession) -> GoldToPostgresLoader:
    return GoldToPostgresLoader(spark=spark)


# ── Helper para criar DataFrames via JSON temp file ────────────────────────────
# Mesmo padrão de test_silver_to_gold.py — evita cloudpickle (incompatível
# com Python 3.14) ao passar dados diretamente para spark.createDataFrame.

_SAMPLE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("value", DoubleType(), True),
    ]
)


def _make_sample_df(spark: SparkSession, n: int = 3):
    rows = [{"id": f"row-{i}", "value": float(i)} for i in range(n)]
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
        tmp_path = f.name
    try:
        df = spark.read.schema(_SAMPLE_SCHEMA).json(tmp_path)
        df = df.cache()
        df.count()
        return df
    finally:
        os.unlink(tmp_path)


# ── TestJdbcConfig ──────────────────────────────────────────────────────────────


class TestJdbcConfig:
    def test_jdbc_url_uses_internal_host(self, loader: GoldToPostgresLoader):
        assert settings.postgres.internal_host in loader._jdbc_url
        assert str(settings.postgres.port) in loader._jdbc_url
        assert settings.postgres.db in loader._jdbc_url
        assert loader._jdbc_url.startswith("jdbc:postgresql://")

    def test_write_options_include_truncate_and_driver(self, loader: GoldToPostgresLoader):
        opts = loader._jdbc_write_options("dim_customers")
        assert opts["dbtable"] == "dim_customers"
        assert opts["truncate"] == "true"
        assert opts["driver"] == "org.postgresql.Driver"
        assert opts["user"] == settings.postgres.user
        assert opts["password"] == settings.postgres.password


# ── TestEnsureSchema ──────────────────────────────────────────────────────────


class TestEnsureSchema:
    def test_executes_ddl_and_commits(self, loader: GoldToPostgresLoader):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
            loader.ensure_schema()

        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_once()
        executed_sql = mock_cursor.execute.call_args[0][0]
        for table in ("dim_customers", "dim_date", "fact_transactions", "agg_daily_fraud_metrics"):
            assert f"CREATE TABLE IF NOT EXISTS {table}" in executed_sql
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_rolls_back_and_reraises_on_failure(self, loader: GoldToPostgresLoader):
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.side_effect = RuntimeError("boom")

        with patch("psycopg2.connect", return_value=mock_conn), pytest.raises(RuntimeError):
            loader.ensure_schema()

        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()


# ── TestLoadTable (métodos públicos mockados) ──────────────────────────────────


class TestLoadTable:
    """Mocka leitura do Gold (MinIO) e escrita JDBC (Postgres)."""

    def test_load_dim_customers_returns_metrics(self, spark: SparkSession, loader):
        mock_df = _make_sample_df(spark, n=4)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = loader.load_dim_customers()

        assert metrics == {"rows_read": 4, "rows_written": 4}

    def test_load_fact_transactions_returns_metrics(self, spark: SparkSession, loader):
        mock_df = _make_sample_df(spark, n=10)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save"),
        ):
            metrics = loader.load_fact_transactions()

        assert metrics == {"rows_read": 10, "rows_written": 10}

    def test_read_failure_is_logged_and_reraised(self, loader: GoldToPostgresLoader):
        with (
            patch("pyspark.sql.DataFrameReader.parquet", side_effect=RuntimeError("s3 down")),
            pytest.raises(RuntimeError, match="s3 down"),
        ):
            loader.load_dim_date()

    def test_write_failure_is_logged_and_reraised(self, spark: SparkSession, loader):
        mock_df = _make_sample_df(spark, n=2)
        with (
            patch("pyspark.sql.DataFrameReader.parquet", return_value=mock_df),
            patch("pyspark.sql.DataFrameWriter.save", side_effect=RuntimeError("connection refused")),
            pytest.raises(RuntimeError, match="connection refused"),
        ):
            loader.load_agg_daily_fraud_metrics()


# ── TestRunAll ──────────────────────────────────────────────────────────────────


class TestRunAll:
    def test_run_all_ensures_schema_and_loads_all_datasets(self, loader: GoldToPostgresLoader):
        with (
            patch.object(loader, "ensure_schema") as mock_ensure,
            patch.object(loader, "load_dim_customers", return_value={"rows_read": 1, "rows_written": 1}),
            patch.object(loader, "load_dim_date", return_value={"rows_read": 2, "rows_written": 2}),
            patch.object(
                loader, "load_fact_transactions", return_value={"rows_read": 3, "rows_written": 3}
            ),
            patch.object(
                loader,
                "load_agg_daily_fraud_metrics",
                return_value={"rows_read": 4, "rows_written": 4},
            ),
        ):
            metrics = loader.run_all()

        mock_ensure.assert_called_once()
        assert set(metrics.keys()) == set(_DATASETS)
        assert metrics["fact_transactions"]["rows_read"] == 3


# ── TestSchemaSQL ────────────────────────────────────────────────────────────────


class TestSchemaSQL:
    def test_schema_sql_declares_all_four_tables(self):
        content = _SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        for table in ("dim_customers", "dim_date", "fact_transactions", "agg_daily_fraud_metrics"):
            assert f"CREATE TABLE IF NOT EXISTS {table}" in content

    def test_schema_sql_has_no_foreign_keys(self):
        """Decisão documentada: sem FK entre fato e dimensões (ver comentário
        no topo do schema.sql) — evita ordenação/CASCADE no truncate+reload."""
        sql_only = "\n".join(
            line
            for line in _SCHEMA_SQL_PATH.read_text(encoding="utf-8").splitlines()
            if not line.strip().startswith("--")
        ).upper()
        assert "REFERENCES" not in sql_only
        assert "FOREIGN KEY" not in sql_only
