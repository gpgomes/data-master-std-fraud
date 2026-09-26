"""Testes unitários para StreamProcessor (issue #11).

Roda em modo PySpark local (sem infra Docker/Kafka real). O núcleo de
transformação (`_parse_raw_kafka_batch`, `_enrich_and_score`,
`_build_fraud_alerts`) é testado como funções puras sobre DataFrames
estáticos — o mesmo shape de dado que chega em cada micro-batch dentro de
`foreachBatch`, então a lógica testada aqui é exatamente a que roda em
produção.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.schemas import FraudAlert
from src.transformation.streaming.stream_processor import (
    DEFAULT_FRAUD_TYPE_FALLBACK,
    DETECTOR_VERSION,
    Z_SCORE_THRESHOLD,
    StreamProcessor,
)

# ── Fixtures ────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("test-stream-processor")
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
def processor(spark: SparkSession) -> StreamProcessor:
    return StreamProcessor(spark=spark)


# ── Helpers para criar DataFrames via JSON temp file ──────────────────────────
# Evita cloudpickle (incompatível com Python 3.14) ao passar dados diretamente.


def _serialize(value):
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def _df_from_rows(spark: SparkSession, rows: list[dict], schema: StructType):
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
        df.count()
        return df
    finally:
        os.unlink(tmp_path)


_KAFKA_RAW_SCHEMA = StructType(
    [StructField("key", StringType(), True), StructField("value", StringType(), True)]
)

_TX_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("fraud_type", StringType(), True),
    ]
)

_DIM_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_key", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("risk_score", DoubleType(), True),
        StructField("city", StringType(), True),
    ]
)


def _make_kafka_raw(spark, rows):
    return _df_from_rows(spark, rows, _KAFKA_RAW_SCHEMA)


def _make_tx(spark, rows):
    return _df_from_rows(spark, rows, _TX_SCHEMA)


def _make_dim_customers(spark, rows):
    return _df_from_rows(spark, rows, _DIM_CUSTOMERS_SCHEMA)


_HISTORY_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


def _empty_history(spark: SparkSession):
    return _df_from_rows(spark, [], _HISTORY_SCHEMA)


def _make_history(spark, rows):
    return _df_from_rows(spark, rows, _HISTORY_SCHEMA)


def _valid_tx_json(**overrides) -> str:
    base = {
        "transaction_id": "tx-001",
        "customer_id": "cust-001",
        "timestamp": "2024-06-15T10:00:00Z",
        "amount": 150.0,
        "currency": "BRL",
        "transaction_type": "PIX",
        "merchant_category": "ALIMENTACAO",
        "origin_account": "acc-1",
        "destination_account": "acc-2",
        "origin_bank": "001",
        "destination_bank": "237",
        "channel": "APP_MOBILE",
        "is_fraud": False,
    }
    base.update(overrides)
    return json.dumps(base)


# ── TestParseRawKafkaBatch ──────────────────────────────────────────────────────


class TestParseRawKafkaBatch:
    def test_valid_json_parses_correctly(self, spark: SparkSession, processor: StreamProcessor):
        raw = _make_kafka_raw(spark, [{"key": "cust-001", "value": _valid_tx_json()}])
        result = processor._parse_raw_kafka_batch(raw)
        assert result.count() == 1
        row = result.first()
        assert row["transaction_id"] == "tx-001"
        assert row["amount"] == 150.0

    def test_malformed_json_is_dropped(self, spark: SparkSession, processor: StreamProcessor):
        raw = _make_kafka_raw(
            spark,
            [
                {"key": "cust-001", "value": _valid_tx_json()},
                {"key": "cust-002", "value": "not valid json {{{"},
            ],
        )
        result = processor._parse_raw_kafka_batch(raw)
        assert result.count() == 1
        assert result.first()["transaction_id"] == "tx-001"

    def test_missing_transaction_id_is_dropped(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        payload = json.loads(_valid_tx_json())
        del payload["transaction_id"]
        raw = _make_kafka_raw(spark, [{"key": "cust-001", "value": json.dumps(payload)}])
        result = processor._parse_raw_kafka_batch(raw)
        assert result.count() == 0


# ── TestEnrichAndScore ──────────────────────────────────────────────────────────


class TestEnrichAndScore:
    def test_insufficient_history_yields_null_z_score(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        rows = [
            _tx(customer_id="cust-1", ts="2024-06-15T10:00:00Z", amount=100.0),
            _tx(customer_id="cust-1", ts="2024-06-15T10:10:00Z", amount=110.0),
        ]
        df = _make_tx(spark, rows)
        result = (
            processor._enrich_and_score(df, _empty_history(spark)).orderBy("timestamp").collect()
        )
        # Primeira transação: 0 no histórico. Segunda: 1 no histórico (< mínimo 2).
        assert result[0]["z_score"] is None
        assert result[1]["z_score"] is None
        assert result[0]["is_anomaly"] is False
        assert result[1]["is_anomaly"] is False

    def test_clear_amount_spike_flagged_as_anomaly(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        rows = [
            _tx(customer_id="cust-1", ts="2024-06-15T10:00:00Z", amount=100.0),
            _tx(customer_id="cust-1", ts="2024-06-15T10:10:00Z", amount=110.0),
            _tx(customer_id="cust-1", ts="2024-06-15T10:20:00Z", amount=105.0),
            _tx(customer_id="cust-1", ts="2024-06-15T10:30:00Z", amount=1000.0),
        ]
        df = _make_tx(spark, rows)
        result = (
            processor._enrich_and_score(df, _empty_history(spark)).orderBy("timestamp").collect()
        )
        spike_row = result[3]
        assert spike_row["amount"] == 1000.0
        assert spike_row["z_score"] > Z_SCORE_THRESHOLD
        assert spike_row["is_anomaly"] is True
        assert spike_row["fraud_score"] == 1.0  # clipado no teto

        stable_row = result[2]
        assert stable_row["is_anomaly"] is False

    def test_window_excludes_transactions_outside_range(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        """Histórico fora da janela de 1h não deve contar, mesmo com valores
        muito discrepantes (confirma que a janela é limitada no tempo)."""
        rows = [
            _tx(customer_id="cust-2", ts="2024-06-15T08:00:00Z", amount=50.0),
            _tx(customer_id="cust-2", ts="2024-06-15T08:05:00Z", amount=55.0),
            # 2h depois — fora da janela de 3600s das duas transações acima
            _tx(customer_id="cust-2", ts="2024-06-15T10:00:00Z", amount=5000.0),
        ]
        df = _make_tx(spark, rows)
        result = (
            processor._enrich_and_score(df, _empty_history(spark)).orderBy("timestamp").collect()
        )
        late_row = result[2]
        assert late_row["amount"] == 5000.0
        assert late_row["z_score"] is None  # sem histórico dentro da janela
        assert late_row["is_anomaly"] is False

    def test_uses_persisted_history_from_previous_micro_batch(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        """O ponto central do design: um micro-batch com uma ÚNICA transação
        consegue detectar anomalia porque o histórico de micro-batches
        anteriores (persistido) fornece a baseline — sem isso, uma transação
        isolada nunca teria >= 2 no histórico dentro do próprio batch."""
        history = _make_history(
            spark,
            [
                {"customer_id": "cust-3", "timestamp": "2024-06-15T10:00:00Z", "amount": 100.0},
                {"customer_id": "cust-3", "timestamp": "2024-06-15T10:10:00Z", "amount": 110.0},
                {"customer_id": "cust-3", "timestamp": "2024-06-15T10:20:00Z", "amount": 105.0},
            ],
        )
        current_batch = _make_tx(
            spark, [_tx(customer_id="cust-3", ts="2024-06-15T10:30:00Z", amount=1000.0)]
        )
        result = processor._enrich_and_score(current_batch, history).first()
        assert result["z_score"] > Z_SCORE_THRESHOLD
        assert result["is_anomaly"] is True

    def test_history_outside_window_is_ignored(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        history = _make_history(
            spark, [{"customer_id": "cust-4", "timestamp": "2024-06-15T07:00:00Z", "amount": 50.0}]
        )
        # só 1 ponto de histórico dentro da janela (min=2) -> ainda insuficiente
        current_batch = _make_tx(
            spark, [_tx(customer_id="cust-4", ts="2024-06-15T10:00:00Z", amount=5000.0)]
        )
        result = processor._enrich_and_score(current_batch, history).first()
        assert result["z_score"] is None
        assert result["is_anomaly"] is False

    def test_history_rows_are_not_included_in_output(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        """`_enrich_and_score` retorna só as linhas do current_batch — o
        histórico é contexto, não deve vazar para a saída."""
        history = _make_history(
            spark, [{"customer_id": "cust-5", "timestamp": "2024-06-15T09:00:00Z", "amount": 50.0}]
        )
        current_batch = _make_tx(
            spark, [_tx(customer_id="cust-5", ts="2024-06-15T10:00:00Z", amount=60.0)]
        )
        result = processor._enrich_and_score(current_batch, history)
        assert result.count() == 1

    def test_enriches_with_dim_customers_when_provided(self, spark: SparkSession):
        dim = _make_dim_customers(
            spark, [{"customer_key": "cust-1", "segment": "PRIVATE", "risk_score": 90.0, "city": "SP"}]
        )
        proc = StreamProcessor(spark=spark, dim_customers=dim)
        df = _make_tx(
            spark, [_tx(customer_id="cust-1", ts="2024-06-15T10:00:00Z", amount=100.0)]
        )
        result = proc._enrich_and_score(df, _empty_history(spark)).first()
        assert result["customer_segment"] == "PRIVATE"
        assert result["customer_risk_score"] == 90.0
        assert result["customer_city"] == "SP"

    def test_no_enrichment_columns_without_dim_customers(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        df = _make_tx(
            spark, [_tx(customer_id="cust-1", ts="2024-06-15T10:00:00Z", amount=100.0)]
        )
        result = processor._enrich_and_score(df, _empty_history(spark))
        assert "customer_segment" not in result.columns

    def test_adds_processing_timestamp(self, spark: SparkSession, processor: StreamProcessor):
        df = _make_tx(
            spark, [_tx(customer_id="cust-1", ts="2024-06-15T10:00:00Z", amount=100.0)]
        )
        result = processor._enrich_and_score(df, _empty_history(spark)).first()
        assert result["processing_timestamp"] is not None


# ── TestPersistHistory ────────────────────────────────────────────────────────


class TestPersistHistory:
    """`_persist_history` é só um wrapper de I/O sobre `_compute_pruned_history`
    (exercitado no teste de integração real — escrita local via Spark no
    Windows precisa de winutils.exe, indisponível neste ambiente de teste).
    A lógica de poda em si é testada diretamente, sem I/O."""

    def test_prunes_entries_outside_window(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        history = _make_history(
            spark,
            [
                # 2h antes do evento mais recente — fora da janela de 1h
                {"customer_id": "cust-6", "timestamp": "2024-06-15T08:00:00Z", "amount": 50.0},
            ],
        )
        current_batch = _make_tx(
            spark, [_tx(customer_id="cust-6", ts="2024-06-15T10:00:00Z", amount=60.0)]
        )
        pruned = processor._compute_pruned_history(current_batch, history)
        assert pruned.count() == 1
        assert pruned.first()["amount"] == 60.0

    def test_keeps_entries_inside_window(self, spark: SparkSession, processor: StreamProcessor):
        history = _make_history(
            spark, [{"customer_id": "cust-7", "timestamp": "2024-06-15T09:30:00Z", "amount": 42.0}]
        )
        current_batch = _make_tx(
            spark, [_tx(customer_id="cust-7", ts="2024-06-15T10:00:00Z", amount=45.0)]
        )
        pruned = processor._compute_pruned_history(current_batch, history)
        assert pruned.count() == 2

    def test_load_history_returns_empty_when_path_missing(
        self, spark: SparkSession, processor: StreamProcessor, tmp_path
    ):
        processor._history_path = str(tmp_path / "does-not-exist")
        result = processor._load_history()
        assert result.count() == 0
        assert set(result.columns) == {"customer_id", "timestamp", "amount"}


def _tx(**overrides) -> dict:
    base = {
        "transaction_id": f"tx-{overrides.get('ts', 'x')}-{overrides.get('customer_id', 'x')}",
        "customer_id": "cust-1",
        "amount": 100.0,
        "fraud_type": None,
    }
    ts = overrides.pop("ts", "2024-06-15T10:00:00Z")
    base["timestamp"] = ts
    base.update(overrides)
    return base


# ── TestNoLabelLeakage ──────────────────────────────────────────────────────────


_LABELLED_TX_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),
    ]
)


class TestNoLabelLeakage:
    """O detector não pode olhar o rótulo de fraude do evento (issue #43, F0).

    `is_fraud`/`fraud_type` são o ground truth do gerador: se o score ou a anomalia dependessem
    deles, qualquer métrica de qualidade do detector seria vazamento.
    """

    _AMOUNTS = [100.0, 110.0, 105.0, 1000.0, 120.0]

    def _rows(self, *, labelled: bool) -> list[dict]:
        rows = []
        for i, amount in enumerate(self._AMOUNTS):
            is_fraud = amount > 500
            rows.append(
                {
                    "transaction_id": f"tx-{i}",
                    "customer_id": "cust-1",
                    "timestamp": f"2024-06-15T10:{i * 10:02d}:00Z",
                    "amount": amount,
                    "is_fraud": is_fraud if labelled else None,
                    "fraud_type": ("CARD_CLONING" if is_fraud else None) if labelled else None,
                }
            )
        return rows

    def _scored(self, spark, processor, *, labelled: bool):
        df = _df_from_rows(spark, self._rows(labelled=labelled), _LABELLED_TX_SCHEMA)
        scored = processor._enrich_and_score(df, _empty_history(spark))
        return scored.orderBy("timestamp").select("z_score", "fraud_score", "is_anomaly").collect()

    def test_scores_and_anomalies_do_not_depend_on_the_labels(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        with_labels = self._scored(spark, processor, labelled=True)
        without_labels = self._scored(spark, processor, labelled=False)
        assert with_labels == without_labels
        # o teste não é vazio: o pico de valor é de fato sinalizado nas duas execuções
        assert any(row["is_anomaly"] for row in with_labels)

    def test_detector_version_identifies_the_zscore_detector(self):
        assert DETECTOR_VERSION == "zscore-v1"


# ── TestBuildFraudAlerts ─────────────────────────────────────────────────────────


class TestBuildFraudAlerts:
    def _scored_row(self, **overrides) -> dict:
        base = {
            "transaction_id": "tx-alert-1",
            "customer_id": "cust-1",
            "timestamp": "2024-06-15T10:30:00Z",
            "amount": 1000.0,
            "fraud_type": None,
            "z_score": 179.0,
            "fraud_score": 1.0,
            "is_anomaly": True,
        }
        base.update(overrides)
        return base

    def _scored_df(self, spark, rows):
        schema = StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("amount", DoubleType(), True),
                StructField("fraud_type", StringType(), True),
                StructField("z_score", DoubleType(), True),
                StructField("fraud_score", DoubleType(), True),
                StructField("is_anomaly", BooleanType(), True),
            ]
        )
        return _df_from_rows(spark, rows, schema)

    def test_only_anomalies_included(self, spark: SparkSession, processor: StreamProcessor):
        rows = [
            self._scored_row(),
            self._scored_row(transaction_id="tx-normal", is_anomaly=False),
        ]
        df = self._scored_df(spark, rows)
        alerts = processor._build_fraud_alerts(df)
        assert alerts.count() == 1
        assert alerts.first()["transaction_id"] == "tx-alert-1"

    def test_uses_source_fraud_type_when_present(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        rows = [self._scored_row(fraud_type="CARD_CLONING")]
        df = self._scored_df(spark, rows)
        alerts = processor._build_fraud_alerts(df)
        assert alerts.first()["fraud_type"] == "CARD_CLONING"

    def test_falls_back_to_default_fraud_type_when_absent(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        rows = [self._scored_row(fraud_type=None)]
        df = self._scored_df(spark, rows)
        alerts = processor._build_fraud_alerts(df)
        assert alerts.first()["fraud_type"] == DEFAULT_FRAUD_TYPE_FALLBACK

    def test_alert_reason_mentions_z_score_and_threshold(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        rows = [self._scored_row()]
        df = self._scored_df(spark, rows)
        reason = processor._build_fraud_alerts(df).first()["alert_reason"]
        assert "Z-Score" in reason
        assert str(Z_SCORE_THRESHOLD) in reason

    def test_alert_row_validates_against_fraud_alert_model(
        self, spark: SparkSession, processor: StreamProcessor
    ):
        rows = [self._scored_row(fraud_type="CARD_CLONING")]
        df = self._scored_df(spark, rows)
        row = processor._build_fraud_alerts(df).first().asDict()
        # Round-trip: o payload que seria publicado no tópico fraud-alerts
        # precisa validar contra o contrato Pydantic FraudAlert (issue #8).
        alert = FraudAlert(**row)
        assert alert.transaction_id == "tx-alert-1"
        assert alert.fraud_type == "CARD_CLONING"


# ── TestIdempotentMicroBatch (issue #36) ─────────────────────────────────────────


class TestIdempotentMicroBatch:
    """O Spark reexecuta o mesmo `batch_id` se o job cai antes do commit do checkpoint.
    Cada etapa do `_process_batch` deve ser idempotente ou pulada no replay."""

    _BATCH = [
        _tx(customer_id="cust-9", ts="2024-06-15T10:00:00Z", amount=100.0),
        _tx(customer_id="cust-9", ts="2024-06-15T10:01:00Z", amount=110.0),
        _tx(customer_id="cust-9", ts="2024-06-15T10:02:00Z", amount=90.0),
        _tx(customer_id="cust-9", ts="2024-06-15T10:03:00Z", amount=5000.0),  # anomalia
    ]

    @staticmethod
    def _local_processor(spark: SparkSession, root, query_id: str = "query-A") -> StreamProcessor:
        base = root.as_uri()
        p = StreamProcessor(spark=spark)
        p._silver = f"{base}/silver"
        p._checkpoints = f"{base}/checkpoints"
        p._history_path = f"{base}/silver/_stream_state/customer_amount_history/"
        p._progress_path = f"{base}/checkpoints/stream_processor_progress"
        p._query_id = query_id
        return p

    @classmethod
    def _new_batch(cls, spark: SparkSession):
        # DataFrame novo a cada execução, como no foreachBatch real: `_process_batch` faz
        # unpersist no fim, e o helper de teste lê de um JSON temporário já apagado.
        return _make_tx(spark, cls._BATCH)

    @staticmethod
    def _silver_stream(spark: SparkSession, p: StreamProcessor):
        return spark.read.parquet(f"{p._silver}/transactions_stream/")

    def test_replayed_batch_does_not_duplicate_parquet_or_kafka(self, spark, tmp_path):
        from unittest.mock import patch

        p = self._local_processor(spark, tmp_path)
        with patch.object(p, "_write_to_kafka") as kafka:
            p._process_batch(self._new_batch(spark), 7)
            p._process_batch(self._new_batch(spark), 7)  # replay pós-restart

        out = self._silver_stream(spark, p)
        assert out.count() == 4
        assert out.select("transaction_id").distinct().count() == 4
        assert kafka.call_count == 2  # enriched + alerts, uma vez cada

    def test_crash_after_parquet_then_replay_does_not_duplicate_parquet(self, spark, tmp_path):
        from unittest.mock import patch

        p = self._local_processor(spark, tmp_path)
        with patch.object(p, "_write_to_kafka", side_effect=RuntimeError("kafka fora")):
            with pytest.raises(RuntimeError):
                p._process_batch(self._new_batch(spark), 3)
        assert self._silver_stream(spark, p).count() == 4  # parquet já tinha sido gravado

        with patch.object(p, "_write_to_kafka") as kafka:
            p._process_batch(self._new_batch(spark), 3)  # replay: pula o parquet, refaz o Kafka

        assert self._silver_stream(spark, p).count() == 4
        assert kafka.call_count == 2

    def test_history_is_not_counted_twice_on_replay(self, spark, tmp_path):
        from unittest.mock import patch

        p = self._local_processor(spark, tmp_path)
        with patch.object(p, "_write_to_kafka"):
            p._process_batch(self._new_batch(spark), 5)
            p._process_batch(self._new_batch(spark), 5)

        assert spark.read.parquet(p._history_path).count() == 4

    def test_new_checkpoint_does_not_overwrite_previous_run_output(self, spark, tmp_path):
        from unittest.mock import patch

        first = self._local_processor(spark, tmp_path, query_id="query-A")
        second = self._local_processor(spark, tmp_path, query_id="query-B")
        # checkpoint recriado: marcadores somem junto e o batch_id volta a 0
        second._progress_path = f"{tmp_path.as_uri()}/checkpoints-novo/stream_processor_progress"
        other = [{**row, "transaction_id": f"novo-{i}"} for i, row in enumerate(self._BATCH)]

        with patch.object(first, "_write_to_kafka"), patch.object(second, "_write_to_kafka"):
            first._process_batch(_make_tx(spark, self._BATCH), 0)
            second._process_batch(_make_tx(spark, other), 0)

        out = self._silver_stream(spark, first)
        assert out.count() == 8
        assert {r["query_id"] for r in out.select("query_id").distinct().collect()} == {
            "query-A",
            "query-B",
        }

    def test_alert_id_is_deterministic_for_the_same_transaction(self, spark, processor):
        import uuid

        scored = processor._enrich_and_score(_make_tx(spark, self._BATCH), _empty_history(spark))
        first = [r["alert_id"] for r in processor._build_fraud_alerts(scored).collect()]
        second = [r["alert_id"] for r in processor._build_fraud_alerts(scored).collect()]

        assert len(first) == 1
        assert first == second
        assert str(uuid.UUID(first[0])) == first[0]

    def test_alert_ids_differ_between_transactions(self, spark, processor):
        rows = self._BATCH + [
            _tx(customer_id="cust-10", ts="2024-06-15T10:00:00Z", amount=100.0),
            _tx(customer_id="cust-10", ts="2024-06-15T10:01:00Z", amount=110.0),
            _tx(customer_id="cust-10", ts="2024-06-15T10:02:00Z", amount=90.0),
            _tx(customer_id="cust-10", ts="2024-06-15T10:03:00Z", amount=7000.0),
        ]
        scored = processor._enrich_and_score(_make_tx(spark, rows), _empty_history(spark))
        ids = [r["alert_id"] for r in processor._build_fraud_alerts(scored).collect()]

        assert len(ids) == 2
        assert len(set(ids)) == 2

    def test_old_progress_markers_are_pruned(self, spark, tmp_path):
        from src.transformation.streaming.stream_processor import PROGRESS_RETENTION_BATCHES

        p = self._local_processor(spark, tmp_path)
        p._mark_stage_done(50, "history")
        p._mark_stage_done(51, "history")

        p._prune_markers(50 + PROGRESS_RETENTION_BATCHES)

        assert not p._stage_done(50, "history")
        assert p._stage_done(51, "history")
