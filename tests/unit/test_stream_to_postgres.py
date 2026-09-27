"""Testes para o loader streaming -> PostgreSQL (issue #38).

PySpark local com Parquet em diretório temporário; só a escrita JDBC é mockada
(mesmo padrão de `test_gold_to_postgres.py`).
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.serving.loaders.stream_to_postgres import StreamToPostgresLoader

_SCHEMA_SQL_PATH = Path(__file__).parents[2] / "src" / "serving" / "loaders" / "schema.sql"

_SCORED_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),
        # Fraud Engine (V2, quem alerta) e Z-Score antigo em paralelo (shadow), issue #46
        StructField("fraud_score", DoubleType(), True),
        StructField("is_fraud_predicted", BooleanType(), True),
        StructField("fraud_signals", ArrayType(StringType()), True),
        StructField("fraud_type_predicted", StringType(), True),
        StructField("detector_version", StringType(), True),
        StructField("z_score", DoubleType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("fraud_score_v1", DoubleType(), True),
        StructField("shadow_detector_version", StringType(), True),
        StructField("produced_at", TimestampType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        # Campos que NÃO devem chegar à serving layer:
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("test-stream-to-postgres")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _row(**overrides) -> dict:
    base = {
        "transaction_id": f"tx-{uuid.uuid4().hex[:8]}",
        "customer_id": "cust-1",
        "timestamp": "2026-09-23T10:00:00Z",
        "amount": 100.0,
        "currency": "BRL",
        "transaction_type": "PIX",
        "channel": "APP_MOBILE",
        "merchant_category": "ALIMENTACAO",
        "is_fraud": False,
        "fraud_type": None,
        "fraud_score": 0.0,
        "is_fraud_predicted": False,
        "fraud_signals": [],
        "fraud_type_predicted": None,
        "detector_version": "multisignal-v2",
        "z_score": None,
        "is_anomaly": False,
        "fraud_score_v1": None,
        "shadow_detector_version": "zscore-v1",
        "produced_at": "2026-09-23T10:00:00Z",
        "processing_timestamp": "2026-09-23T10:00:05Z",
        "device_id": "dev-1",
        "ip_address": "10.0.0.1",
    }
    base.update(overrides)
    return base


def _df(spark: SparkSession, rows: list[dict]):
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        for row in rows:
            f.write(json.dumps({k: v for k, v in row.items() if v is not None}) + "\n")
        path = f.name
    try:
        df = spark.read.schema(_SCORED_SCHEMA).json(path).cache()
        df.count()
        return df
    finally:
        os.unlink(path)


def _write_stream_parquet(spark, root: Path, query_id: str, rows: list[dict]) -> None:
    (
        _df(spark, rows)
        .withColumn("query_id", F.lit(query_id))
        .write.mode("append")
        .partitionBy("query_id")
        .parquet(f"{root.as_uri()}/silver/transactions_stream/")
    )


def _loader(spark: SparkSession, root: Path) -> StreamToPostgresLoader:
    loader = StreamToPostgresLoader(spark)
    loader._silver = f"{root.as_uri()}/silver"
    return loader


def StreamToPostgres_scored(spark, rows):  # noqa: N802 - atalho de teste
    return StreamToPostgresLoader._to_scored_table(_df(spark, rows))


class TestScoredTable:
    def test_computes_latency_and_score_bucket(self, spark):
        df = _df(
            spark,
            [
                _row(
                    produced_at="2026-09-23T10:00:00Z",
                    processing_timestamp="2026-09-23T10:00:04.5Z",
                    fraud_score=0.4567,
                )
            ],
        )
        row = StreamToPostgresLoader._to_scored_table(df).first()

        assert row["latency_seconds"] == pytest.approx(4.5)
        assert row["fraud_score_bucket"] == pytest.approx(0.5)
        assert row["event_time"] is not None

    def test_null_score_has_null_bucket(self, spark):
        row = StreamToPostgresLoader._to_scored_table(
            _df(spark, [_row(fraud_score=None)])
        ).first()
        assert row["fraud_score"] is None
        assert row["fraud_score_bucket"] is None

    def test_the_score_column_is_the_engines_and_the_zscore_stays_as_shadow(self, spark):
        row = StreamToPostgresLoader._to_scored_table(
            _df(spark, [_row(fraud_score=0.975, z_score=2.0, fraud_score_v1=0.33)])
        ).first()
        assert row["fraud_score"] == pytest.approx(0.975)
        assert row["z_score"] == pytest.approx(2.0)

    def test_carries_the_engine_verdict_and_the_shadow_side_by_side(self, spark):
        row = StreamToPostgresLoader._to_scored_table(
            _df(
                spark,
                [
                    _row(
                        is_fraud=True,
                        fraud_type="CARD_CLONING",
                        fraud_score=0.975,
                        is_fraud_predicted=True,
                        fraud_signals=["GEO_VELOCITY", "NEW_DESTINATION"],
                        fraud_type_predicted="CARD_CLONING",
                        z_score=1.2,
                        is_anomaly=False,
                        fraud_score_v1=0.2,
                    )
                ],
            )
        ).first()
        assert row["is_fraud_predicted"] is True
        assert row["fraud_type_predicted"] == "CARD_CLONING"
        assert row["detector_version"] == "multisignal-v2"
        assert row["is_anomaly"] is False and row["fraud_score_v1"] == pytest.approx(0.2)
        assert row["shadow_detector_version"] == "zscore-v1"

    def test_signals_become_a_comma_separated_string(self, spark):
        rows = {
            r["transaction_id"]: r["fraud_signals"]
            for r in StreamToPostgres_scored(
                spark,
                [
                    _row(transaction_id="two", fraud_signals=["GEO_VELOCITY", "NEW_DESTINATION"]),
                    _row(transaction_id="none", fraud_signals=[]),
                ],
            ).collect()
        }
        assert rows == {"two": "GEO_VELOCITY,NEW_DESTINATION", "none": ""}

    def test_the_label_and_the_prediction_are_separate_columns(self, spark):
        """`is_fraud`/`fraud_type` são o rótulo; a predição não os sobrescreve nem os lê."""
        row = StreamToPostgres_scored(
            spark,
            [
                _row(
                    is_fraud=False,
                    fraud_type=None,
                    is_fraud_predicted=True,
                    fraud_type_predicted="ACCOUNT_TAKEOVER",
                )
            ],
        ).first()
        assert row["is_fraud"] is False and row["fraud_type"] is None
        assert row["is_fraud_predicted"] is True
        assert row["fraud_type_predicted"] == "ACCOUNT_TAKEOVER"

    def test_drops_sensitive_source_columns(self, spark):
        columns = set(StreamToPostgresLoader._to_scored_table(_df(spark, [_row()])).columns)
        assert not columns & {"device_id", "ip_address", "origin_account", "latitude"}

    def test_columns_match_the_postgres_table(self, spark):
        columns = StreamToPostgresLoader._to_scored_table(_df(spark, [_row()])).columns
        ddl = _SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        table_sql = ddl.split("stream_scored_transactions (")[1].split(");")[0]
        table_columns = [line.split()[0] for line in table_sql.strip().splitlines() if line.strip()]
        assert columns == table_columns


class TestAlertsTable:
    _ROWS = [
        _row(transaction_id="tx-normal"),
        _row(transaction_id="tx-only-zscore", is_anomaly=True, z_score=8.0, fraud_score_v1=1.0),
        _row(
            transaction_id="tx-alert",
            is_fraud=True,
            fraud_type="CARD_CLONING",  # rótulo do gerador: não pode virar o tipo do alerta
            is_fraud_predicted=True,
            fraud_score=0.975,
            fraud_signals=["NEW_DESTINATION", "AMOUNT_ANOMALY"],
            fraud_type_predicted="SOCIAL_ENGINEERING",
            z_score=8.5,
            is_anomaly=True,
            processing_timestamp="2026-09-23T10:00:07Z",
        ),
    ]

    def test_only_the_engines_predictions_become_alerts(self, spark):
        alerts = StreamToPostgresLoader._to_alerts_table(_df(spark, self._ROWS))
        assert [r["transaction_id"] for r in alerts.collect()] == ["tx-alert"]

    def test_the_alert_type_is_the_predicted_one_never_the_label(self, spark):
        alert = StreamToPostgresLoader._to_alerts_table(_df(spark, self._ROWS)).first()
        assert alert["fraud_type"] == "SOCIAL_ENGINEERING"
        assert alert["fraud_score"] == pytest.approx(0.975)
        assert "NEW_DESTINATION" in alert["alert_reason"]

    def test_alert_id_is_deterministic_and_matches_kafka_alert_id(self, spark):
        first = StreamToPostgresLoader._to_alerts_table(_df(spark, self._ROWS)).first()
        second = StreamToPostgresLoader._to_alerts_table(_df(spark, self._ROWS)).first()
        assert first["alert_id"] == second["alert_id"]
        assert str(uuid.UUID(first["alert_id"])) == first["alert_id"]

    def test_processed_at_is_the_row_processing_timestamp(self, spark):
        alert = StreamToPostgresLoader._to_alerts_table(_df(spark, self._ROWS)).first()
        assert alert["processed_at"].second == 7  # 10:00:07, não a hora da carga

    def test_the_alert_carries_the_signals_and_the_detector_version(self, spark):
        alert = StreamToPostgresLoader._to_alerts_table(_df(spark, self._ROWS)).first()
        assert alert["signals"] == "NEW_DESTINATION,AMOUNT_ANOMALY"
        assert alert["detector_version"] == "multisignal-v2"
        assert alert["z_score"] == pytest.approx(8.5)  # o do V1, em paralelo

    def test_an_alert_without_signals_or_type_is_still_loaded(self, spark):
        """Combinação de sinais fracos (nenhum ≥ 0,5): lista vazia e sem tipo inferido."""
        rows = [
            _row(
                transaction_id="tx-weak",
                is_fraud_predicted=True,
                fraud_score=0.93,
                fraud_signals=[],
                fraud_type_predicted=None,
            )
        ]
        alert = StreamToPostgresLoader._to_alerts_table(_df(spark, rows)).first()
        assert alert["signals"] == ""
        assert alert["fraud_type"] is None

    def test_columns_match_the_postgres_table(self, spark):
        columns = StreamToPostgresLoader._to_alerts_table(_df(spark, self._ROWS)).columns
        ddl = _SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        table_sql = ddl.split("fraud_alerts (")[1].split(");")[0]
        table_columns = [line.split()[0] for line in table_sql.strip().splitlines() if line.strip()]
        assert columns == table_columns


class TestReadScored:
    def test_returns_none_when_streaming_never_ran(self, spark, tmp_path):
        assert _loader(spark, tmp_path)._read_scored() is None

    def test_keeps_latest_row_when_transaction_repeats_across_queries(self, spark, tmp_path):
        _write_stream_parquet(
            spark, tmp_path, "query-A",
            [_row(transaction_id="tx-dup", amount=100.0, processing_timestamp="2026-09-23T10:00:05Z")],
        )
        _write_stream_parquet(
            spark, tmp_path, "query-B",
            [
                _row(transaction_id="tx-dup", amount=100.0, processing_timestamp="2026-09-23T11:00:05Z"),
                _row(transaction_id="tx-other"),
            ],
        )
        df = _loader(spark, tmp_path)._read_scored()

        assert df.count() == 2
        latest = df.filter("transaction_id = 'tx-dup'").first()
        assert latest["query_id"] == "query-B"


class TestLoad:
    def test_skips_load_without_streaming_data(self, spark, tmp_path):
        loader = _loader(spark, tmp_path)
        with patch.object(StreamToPostgresLoader, "_write_table") as write:
            assert loader.load_stream_scored_transactions() == {"rows_read": 0, "rows_written": 0}
            assert loader.load_fraud_alerts() == {"rows_read": 0, "rows_written": 0}
        write.assert_not_called()

    def test_loads_scored_transactions_and_alerts_into_their_tables(self, spark, tmp_path):
        _write_stream_parquet(
            spark, tmp_path, "query-A",
            [
                _row(transaction_id="tx-1"),
                _row(
                    transaction_id="tx-2",
                    is_fraud_predicted=True,
                    fraud_signals=["NEW_DESTINATION"],
                    fraud_score=0.97,
                ),
            ],
        )
        loader = _loader(spark, tmp_path)
        loaded: dict[str, int] = {}

        def fake_write(_self, df, table):
            loaded[table] = df.count()
            return {"rows_read": loaded[table], "rows_written": loaded[table]}

        with patch.object(StreamToPostgresLoader, "_write_table", fake_write):
            loader.load_stream_scored_transactions()
            loader.load_fraud_alerts()

        assert loaded == {"stream_scored_transactions": 2, "fraud_alerts": 1}

    def test_run_all_ensures_schema_first(self, spark, tmp_path):
        loader = _loader(spark, tmp_path)
        calls: list[str] = []
        with (
            patch.object(loader, "ensure_schema", side_effect=lambda: calls.append("schema")),
            patch.object(
                loader,
                "load_stream_scored_transactions",
                side_effect=lambda: calls.append("scored") or {},
            ),
            patch.object(loader, "load_fraud_alerts", side_effect=lambda: calls.append("alerts") or {}),
        ):
            loader.run_all()
        assert calls == ["schema", "scored", "alerts"]


# Colunas que as duas tabelas de streaming já tinham na issue #38, antes do Fraud Engine.
_BASELINE_38 = {
    "stream_scored_transactions": {
        "transaction_id", "customer_id", "event_time", "amount", "currency", "transaction_type",
        "channel", "merchant_category", "is_fraud", "fraud_type", "z_score", "fraud_score",
        "fraud_score_bucket", "is_anomaly", "produced_at", "processing_timestamp",
        "latency_seconds",
    },
    "fraud_alerts": {
        "alert_id", "transaction_id", "customer_id", "event_time", "amount", "fraud_type",
        "fraud_score", "z_score", "alert_reason", "processed_at",
    },
}


def _create_columns(ddl: str, table: str) -> dict[str, str]:
    body = ddl.split(f"CREATE TABLE IF NOT EXISTS {table} (")[1].split(");")[0]
    columns = {}
    for line in body.strip().splitlines():
        parts = line.strip().rstrip(",").split()
        if parts and not parts[0].startswith("--"):
            columns[parts[0]] = " ".join(parts[1:]).upper()
    return columns


def _alter_columns(ddl: str, table: str) -> dict[str, str]:
    import re

    pattern = rf"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS (\w+) ([A-Z ]+);"
    return {name: kind.strip() for name, kind in re.findall(pattern, ddl)}


class TestUpgradeOfProvisionedDatabases:
    """`CREATE TABLE IF NOT EXISTS` não adiciona coluna a uma tabela que já existe: um banco
    provisionado antes da issue #47 precisa dos `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`."""

    _DDL = _SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    @pytest.mark.parametrize("table", ["stream_scored_transactions", "fraud_alerts"])
    def test_every_column_added_after_issue_38_has_an_idempotent_alter(self, table):
        created = _create_columns(self._DDL, table)
        added_since_38 = set(created) - _BASELINE_38[table]
        assert added_since_38, "a issue #47 deveria ter adicionado colunas"
        assert set(_alter_columns(self._DDL, table)) == added_since_38

    @pytest.mark.parametrize("table", ["stream_scored_transactions", "fraud_alerts"])
    def test_the_alter_has_the_same_type_as_the_create(self, table):
        created = _create_columns(self._DDL, table)
        for name, kind in _alter_columns(self._DDL, table).items():
            assert kind == created[name], f"{table}.{name}"

    def test_no_alter_is_missing_if_not_exists(self):
        alters = [
            line for line in self._DDL.splitlines() if line.strip().startswith("ALTER TABLE")
        ]
        assert alters
        assert all("ADD COLUMN IF NOT EXISTS" in line for line in alters)

    def test_no_baseline_column_was_dropped_or_retyped(self):
        for table, baseline in _BASELINE_38.items():
            assert baseline <= set(_create_columns(self._DDL, table))

    def test_ensure_schema_runs_the_whole_file_in_one_transaction(self):
        from unittest.mock import MagicMock

        loader = StreamToPostgresLoader.__new__(StreamToPostgresLoader)
        conn = MagicMock()
        with patch("src.serving.loaders.gold_to_postgres.psycopg2.connect", return_value=conn):
            loader.ensure_schema()
        executed = conn.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
        assert "ALTER TABLE fraud_alerts ADD COLUMN IF NOT EXISTS signals" in executed
        conn.commit.assert_called_once()
        conn.close.assert_called_once()


class TestSchemaSql:
    def test_declares_streaming_tables_without_foreign_keys(self):
        content = _SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        for table in ("stream_scored_transactions", "fraud_alerts"):
            assert f"CREATE TABLE IF NOT EXISTS {table}" in content
        assert "REFERENCES" not in content.upper()
