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
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.schemas import FraudAlert
from src.transformation.fraud.detector import detect
from src.transformation.fraud.signals import EVENT_COLUMNS, SIGNALS
from src.transformation.streaming import stream_processor as sp
from src.transformation.streaming.stream_processor import (
    DETECTOR_VERSION,
    LABEL_COLUMNS,
    STATE_HORIZON_SECONDS,
    Z_SCORE_THRESHOLD,
    StreamProcessor,
)
from tests.unit.fraud_helpers import SALVADOR, event, profile_df, profile_row

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
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("destination_account", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
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


# ── Estado curto (issue #46) ────────────────────────────────────────────────────


def _state_row(customer: str, ts: str, amount: float = 100.0, **extra) -> dict:
    return {
        "transaction_id": f"st-{customer}-{ts}",
        "customer_id": customer,
        "timestamp": ts,
        "amount": amount,
        **extra,
    }


class TestShortState:
    """O estado entre micro-batches guarda as últimas 6 h de eventos, só com as colunas que o
    detector enxerga. `_persist_history` é um wrapper de I/O sobre `_compute_pruned_history`,
    que é testado diretamente, sem I/O."""

    def _state(self, spark, rows):
        return _df_from_rows(spark, rows, sp._STATE_SCHEMA)

    def test_horizon_covers_the_widest_signal_window(self):
        assert STATE_HORIZON_SECONDS == 6 * 3600  # a janela da viagem impossível

    def test_state_schema_is_exactly_what_the_detector_sees(self):
        assert [f.name for f in sp._STATE_SCHEMA.fields] == list(EVENT_COLUMNS)

    def test_prunes_entries_older_than_the_horizon(self, spark, processor):
        history = self._state(spark, [_state_row("cust-6", "2024-06-15T03:00:00Z")])  # 7 h antes
        current = _make_tx(spark, [_tx(customer_id="cust-6", ts="2024-06-15T10:00:00Z", amount=60.0)])
        pruned = processor._compute_pruned_history(current, history)
        assert pruned.count() == 1
        assert pruned.first()["amount"] == 60.0

    def test_keeps_entries_inside_the_horizon(self, spark, processor):
        history = self._state(spark, [_state_row("cust-7", "2024-06-15T06:00:00Z", 42.0)])  # 4 h
        current = _make_tx(spark, [_tx(customer_id="cust-7", ts="2024-06-15T10:00:00Z", amount=45.0)])
        assert processor._compute_pruned_history(current, history).count() == 2

    def test_the_boundary_is_inclusive(self, spark, processor):
        history = self._state(spark, [_state_row("c", "2024-06-15T04:00:00Z")])  # exatamente 6 h
        current = _make_tx(spark, [_tx(customer_id="c", ts="2024-06-15T10:00:00Z")])
        assert processor._compute_pruned_history(current, history).count() == 2

    def test_state_never_keeps_the_label(self, spark, processor):
        """O micro-batch traz `is_fraud`/`fraud_type`; o estado só guarda o que o detector vê."""
        current = _make_tx(
            spark,
            [_tx(customer_id="c", ts="2024-06-15T10:00:00Z", is_fraud=True, fraud_type="CARD_CLONING")],
        )
        pruned = processor._compute_pruned_history(current, processor._empty_history_df())
        assert pruned.columns == list(EVENT_COLUMNS)
        assert not set(LABEL_COLUMNS) & set(pruned.columns)

    def test_load_history_returns_empty_when_path_missing(self, spark, processor, tmp_path):
        processor._history_path = str(tmp_path / "does-not-exist")
        result = processor._load_history()
        assert result.count() == 0
        assert result.columns == list(EVENT_COLUMNS)

    def test_state_lives_in_a_new_path(self, processor):
        """O Parquet antigo (3 colunas) é incompatível: o estado novo tem caminho próprio."""
        assert processor._history_path.endswith("/_stream_state/recent_events/")
        assert "customer_amount_history" not in processor._history_path

    def test_v1_still_reads_its_three_columns_from_the_wider_state(self, spark, processor):
        wide = self._state(
            spark,
            [_state_row("cust-8", f"2024-06-15T10:0{i}:00Z", 100.0 + i, device_id="dev") for i in range(3)],
        )
        current = _make_tx(spark, [_tx(customer_id="cust-8", ts="2024-06-15T10:30:00Z", amount=100.0)])
        result = processor._enrich_and_score(current, wide.select("customer_id", "timestamp", "amount"))
        assert result.first()["z_score"] is not None  # o baseline veio do estado


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


# ── Fraud Engine no stream (issue #46) ──────────────────────────────────────────

# Pesos de teste, independentes da calibração versionada em `weights.py` (que muda a cada
# `make fraud-calibrate`). Nenhum sinal sozinho alerta; `NEW_DESTINATION` + qualquer outro alerta.
TEST_WEIGHTS = {
    **{name: 0.0 for name in SIGNALS},
    "NEW_DESTINATION": 0.9,
    "AMOUNT_ANOMALY": 0.75,
    "GEO_VELOCITY": 0.75,
    "RECIPIENT_CONCENTRATION": 0.75,
}
TEST_THRESHOLD = 0.95  # 0,9 sozinho fica abaixo; 1 − 0,1 · 0,25 = 0,975 alerta

_T0 = "2026-03-02T12:00:00"


def _engine_processor(spark: SparkSession, *customers: str) -> StreamProcessor:
    """Processor com perfil injetado (valor típico R$ 100, device/destino conhecidos, casa em SP)."""
    profiles = profile_df(spark, *[profile_row(c) for c in customers or ("c1",)])
    return StreamProcessor(
        spark=spark, profile=profiles, weights=TEST_WEIGHTS, threshold=TEST_THRESHOLD
    )


def _legit(i, customer: str = "c1", ts: str = _T0, amount: float = 100.0, **kw) -> dict:
    return event(i, customer, ts, amount, is_fraud=False, **kw)


def _fraud(i, customer: str = "c1", ts: str = "2026-03-02T12:06:00", amount: float = 5000.0, **kw):
    """Valor muito acima do típico para um destinatário novo: AMOUNT_ANOMALY + NEW_DESTINATION."""
    kw.setdefault("dest", "acc-new")
    return event(
        i, customer, ts, amount, is_fraud=True, fraud_type="SOCIAL_ENGINEERING", **kw
    )


# ── TestScoreBatch ──────────────────────────────────────────────────────────────


class TestScoreBatch:
    """O V2 decide; o V1 segue calculado em paralelo (shadow); o rótulo só volta no fim."""

    def _scored(self, spark, rows, history=None, processor=None):
        p = processor or _engine_processor(spark)
        return p._score_batch(_make_tx(spark, rows), history or p._empty_history_df())

    def test_output_carries_both_detectors_and_the_label(self, spark):
        scored = self._scored(spark, [_legit(1), _fraud(2)])
        assert {
            "z_score",
            "is_anomaly",
            "fraud_score_v1",
            "shadow_detector_version",
            "fraud_score",
            "is_fraud_predicted",
            "fraud_signals",
            "fraud_type_predicted",
            "detector_version",
            "is_fraud",
            "fraud_type",
            "processing_timestamp",
        } <= set(scored.columns)

    def test_one_row_per_transaction(self, spark):
        rows = [_legit(1), _legit(2, ts="2026-03-02T12:01:00"), _fraud(3)]
        assert self._scored(spark, rows).count() == 3

    def test_versions_identify_who_alerts_and_who_is_the_shadow(self, spark):
        row = self._scored(spark, [_legit(1)]).first()
        assert row["detector_version"] == "multisignal-v2"
        assert row["shadow_detector_version"] == DETECTOR_VERSION == "zscore-v1"

    def test_the_engine_flags_a_fraud_the_zscore_cannot_see(self, spark):
        """Primeiro evento do cliente: o V1 não tem baseline (z nulo); o V2 conhece o perfil."""
        row = self._scored(spark, [_fraud(1)]).first()
        assert row["z_score"] is None and row["is_anomaly"] is False
        assert row["is_fraud_predicted"] is True
        assert row["fraud_score"] == pytest.approx(0.975)
        assert set(row["fraud_signals"]) == {"AMOUNT_ANOMALY", "NEW_DESTINATION"}
        assert row["fraud_type_predicted"] == "SOCIAL_ENGINEERING"

    def test_legitimate_traffic_is_not_flagged(self, spark):
        row = self._scored(spark, [_legit(1)]).first()
        assert row["is_fraud_predicted"] is False
        assert row["fraud_score"] == pytest.approx(0.0)
        assert list(row["fraud_signals"]) == []
        assert row["fraud_type_predicted"] is None

    def test_fraud_score_is_the_engines_not_the_zscores(self, spark):
        rows = [
            _legit(1, ts="2026-03-02T11:50:00", amount=100.0),
            _legit(2, ts="2026-03-02T11:52:00", amount=110.0),
            _legit(3, ts="2026-03-02T11:54:00", amount=90.0),
            _fraud(4),
        ]
        row = self._scored(spark, rows).filter("transaction_id = 't4'").first()
        assert row["fraud_score_v1"] == pytest.approx(1.0)  # o Z-Score do V1, clipado no teto
        assert row["fraud_score"] == pytest.approx(0.975)  # o do V2

    def test_the_payload_fraud_score_is_dropped(self, spark):
        """`fraud_score` do payload é sempre nulo (campo derivado): não pode colidir com o do V2."""
        schema = StructType([*_TX_SCHEMA.fields, StructField("fraud_score", DoubleType(), True)])
        rows = [{**_legit(1), "fraud_score": 0.99}]
        p = _engine_processor(spark)
        scored = p._score_batch(_df_from_rows(spark, rows, schema), p._empty_history_df())
        assert scored.columns.count("fraud_score") == 1
        assert scored.first()["fraud_score"] == pytest.approx(0.0)

    def test_runs_without_the_label_columns(self, spark):
        """Um produtor que não manda `is_fraud`/`fraud_type` (dado real) também é pontuado."""
        p = _engine_processor(spark)
        batch = _make_tx(spark, [_legit(1), _fraud(2)]).drop(*LABEL_COLUMNS)
        scored = p._score_batch(batch, p._empty_history_df())
        assert not set(LABEL_COLUMNS) & set(scored.columns)
        assert scored.filter("is_fraud_predicted").count() == 1

    def test_runs_without_a_profile_and_stays_quiet(self, spark, processor):
        """Sem perfil o V2 fica cego (nada é "conhecido"), mas o micro-batch não quebra."""
        scored = processor._score_batch(
            _make_tx(spark, [_legit(1), _fraud(2)]), processor._empty_history_df()
        )
        assert scored.count() == 2

    def test_geo_velocity_uses_the_short_state(self, spark):
        """Viagem impossível só aparece com o evento anterior, que veio de outro micro-batch."""
        p = _engine_processor(spark)
        previous = _df_from_rows(spark, [_legit(1, ts="2026-03-02T12:00:00")], sp._STATE_SCHEMA)
        jump = event(2, "c1", "2026-03-02T12:15:00", 100.0, at=SALVADOR, dest="acc-new")

        with_state = p._score_batch(_make_tx(spark, [jump]), previous).first()
        without_state = p._score_batch(_make_tx(spark, [jump]), p._empty_history_df()).first()

        assert "GEO_VELOCITY" in with_state["fraud_signals"]
        assert with_state["is_fraud_predicted"] is True
        assert "GEO_VELOCITY" not in without_state["fraud_signals"]
        assert without_state["is_fraud_predicted"] is False


# ── TestStreamNoLabelLeakage ────────────────────────────────────────────────────


class TestStreamNoLabelLeakage:
    """O caminho de scoring do stream não pode olhar o rótulo (issue #46, decisão D1).

    O rótulo segue no payload do Kafka (o gerador sintético o manda), mas `_score_batch` o tira do
    DataFrame antes de qualquer detector. Verdadeiro, invertido ou ausente: o mesmo resultado.
    """

    _OUTPUT = (
        "fraud_score",
        "is_fraud_predicted",
        "fraud_signals",
        "fraud_type_predicted",
        "z_score",
        "is_anomaly",
        "fraud_score_v1",
    )

    def _rows(self, mode: str) -> list[dict]:
        base = [
            _legit(1, ts="2026-03-02T11:50:00", amount=100.0),
            _legit(2, ts="2026-03-02T11:52:00", amount=110.0),
            _legit(3, ts="2026-03-02T11:54:00", amount=90.0),
            _fraud(4),
            _legit(5, "c1", "2026-03-02T12:30:00", dest="acc-known"),
        ]
        rows = []
        for row in base:
            if mode == "inverted":
                row = {
                    **row,
                    "is_fraud": not row["is_fraud"],
                    "fraud_type": None if row["is_fraud"] else "CARD_CLONING",
                }
            elif mode == "null":
                row = {**row, "is_fraud": None, "fraud_type": None}
            rows.append(row)
        return rows

    def _result(self, spark, mode: str):
        p = _engine_processor(spark)
        scored = p._score_batch(_make_tx(spark, self._rows(mode)), p._empty_history_df())
        return [
            tuple(_plain(row[c]) for c in self._OUTPUT)
            for row in scored.orderBy("transaction_id").collect()
        ]

    def test_scores_signals_and_alerts_do_not_depend_on_the_labels(self, spark):
        truth = self._result(spark, "true")
        assert self._result(spark, "inverted") == truth
        assert self._result(spark, "null") == truth
        # o teste não é vazio: há alerta do V2 e a anomalia do V1 na mesma execução
        assert any(row[1] for row in truth)
        assert any(row[5] for row in truth)

    def test_the_label_is_reattached_unchanged_by_transaction_id(self, spark):
        p = _engine_processor(spark)
        rows = self._rows("inverted")
        scored = p._score_batch(_make_tx(spark, rows), p._empty_history_df())
        expected = {r["transaction_id"]: (r["is_fraud"], r["fraud_type"]) for r in rows}
        got = {r["transaction_id"]: (r["is_fraud"], r["fraud_type"]) for r in scored.collect()}
        assert got == expected  # o rótulo viaja para o Parquet (avaliação online), sem ser lido


def _plain(value):
    """Valor comparável: arrays viram lista ordenada e floats, arredondados."""
    if isinstance(value, list | tuple):
        return sorted(value)
    if isinstance(value, float):
        return round(value, 9)
    return value


# ── TestBuildFraudAlerts ─────────────────────────────────────────────────────────

_SCORED_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),  # rótulo do gerador
        StructField("z_score", DoubleType(), True),
        StructField("fraud_score", DoubleType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("is_fraud_predicted", BooleanType(), True),
        StructField("fraud_signals", ArrayType(StringType()), True),
        StructField("fraud_type_predicted", StringType(), True),
        StructField("detector_version", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
    ]
)


class TestBuildFraudAlerts:
    def _scored_row(self, **overrides) -> dict:
        base = {
            "transaction_id": "tx-alert-1",
            "customer_id": "cust-1",
            "timestamp": "2024-06-15T10:30:00Z",
            "amount": 1000.0,
            "is_fraud": True,
            "fraud_type": "CARD_CLONING",  # rótulo: não pode aparecer no alerta
            "z_score": 179.0,
            "fraud_score": 0.98,
            "is_anomaly": True,
            "is_fraud_predicted": True,
            "fraud_signals": ["NEW_DEVICE", "GEO_FAR_FROM_HOME"],
            "fraud_type_predicted": "ACCOUNT_TAKEOVER",
            "detector_version": "multisignal-v2",
            "processing_timestamp": "2024-06-15T10:30:07Z",
        }
        base.update(overrides)
        return base

    def _alerts(self, spark, processor, *rows):
        return processor._build_fraud_alerts(_df_from_rows(spark, list(rows), _SCORED_SCHEMA))

    def test_only_what_the_engine_predicted_becomes_an_alert(self, spark, processor):
        alerts = self._alerts(
            spark,
            processor,
            self._scored_row(),
            self._scored_row(transaction_id="tx-normal", is_fraud_predicted=False),
        )
        assert [r["transaction_id"] for r in alerts.collect()] == ["tx-alert-1"]

    def test_the_v2_decides_not_the_zscore_shadow(self, spark, processor):
        """Anomalia do V1 sem predição do V2 não alerta; predição do V2 sem anomalia alerta."""
        alerts = self._alerts(
            spark,
            processor,
            self._scored_row(transaction_id="only-v1", is_anomaly=True, is_fraud_predicted=False),
            self._scored_row(
                transaction_id="only-v2", is_anomaly=False, z_score=None, is_fraud_predicted=True
            ),
        )
        assert [r["transaction_id"] for r in alerts.collect()] == ["only-v2"]

    def test_fraud_type_is_the_predicted_one_never_the_label(self, spark, processor):
        alert = self._alerts(spark, processor, self._scored_row()).first()
        assert alert["fraud_type"] == "ACCOUNT_TAKEOVER"  # o rótulo dizia CARD_CLONING

    def test_fraud_type_is_null_when_no_rule_matched_and_there_is_no_fallback(
        self, spark, processor
    ):
        alert = self._alerts(
            spark, processor, self._scored_row(fraud_type_predicted=None, fraud_type="MONEY_LAUNDERING")
        ).first()
        assert alert["fraud_type"] is None
        assert not hasattr(sp, "DEFAULT_FRAUD_TYPE_FALLBACK")

    def test_the_alert_never_carries_the_label(self, spark, processor):
        columns = set(self._alerts(spark, processor, self._scored_row()).columns)
        assert not columns & {"is_fraud", "fraud_type_predicted", "is_fraud_predicted"}

    def test_alert_reason_lists_the_signals_score_and_detector(self, spark, processor):
        reason = self._alerts(spark, processor, self._scored_row()).first()["alert_reason"]
        assert reason == "Sinais: NEW_DEVICE, GEO_FAR_FROM_HOME | score 0.98 | multisignal-v2"

    def test_alert_reason_when_only_weak_signals_combined(self, spark, processor):
        reason = self._alerts(
            spark, processor, self._scored_row(fraud_signals=[], fraud_type_predicted=None)
        ).first()["alert_reason"]
        assert "combinação de sinais fracos" in reason
        assert "multisignal-v2" in reason

    def test_alert_exposes_signals_detector_and_shadow_zscore(self, spark, processor):
        alert = self._alerts(spark, processor, self._scored_row()).first()
        assert list(alert["signals"]) == ["NEW_DEVICE", "GEO_FAR_FROM_HOME"]
        assert alert["detector_version"] == "multisignal-v2"
        assert alert["z_score"] == pytest.approx(179.0)

    def test_zscore_can_be_null(self, spark, processor):
        alert = self._alerts(spark, processor, self._scored_row(z_score=None)).first()
        assert alert["z_score"] is None

    def test_processed_at_is_the_row_processing_timestamp(self, spark, processor):
        alert = self._alerts(spark, processor, self._scored_row()).first()
        assert alert["processed_at"].second == 7

    def test_alert_row_validates_against_fraud_alert_model(self, spark, processor):
        # Round-trip: o payload que seria publicado em `fraud-alerts` precisa validar contra o
        # contrato Pydantic FraudAlert (issue #8).
        row = self._alerts(spark, processor, self._scored_row()).first().asDict()
        alert = FraudAlert(**row)
        assert alert.transaction_id == "tx-alert-1"
        assert alert.fraud_type == "ACCOUNT_TAKEOVER"
        assert alert.signals == ["NEW_DEVICE", "GEO_FAR_FROM_HOME"]
        assert alert.detector_version == "multisignal-v2"

    def test_alert_without_a_type_validates_against_the_model(self, spark, processor):
        row = self._alerts(
            spark, processor, self._scored_row(fraud_type_predicted=None, z_score=None)
        ).first().asDict()
        alert = FraudAlert(**row)
        assert alert.fraud_type is None and alert.z_score is None


# ── TestIdempotentMicroBatch (issue #36) ─────────────────────────────────────────


class TestIdempotentMicroBatch:
    """O Spark reexecuta o mesmo `batch_id` se o job cai antes do commit do checkpoint.
    Cada etapa do `_process_batch` deve ser idempotente ou pulada no replay."""

    _BATCH = [
        _legit(1, ts="2026-03-02T10:00:00", amount=100.0),
        _legit(2, ts="2026-03-02T10:01:00", amount=110.0),
        _legit(3, ts="2026-03-02T10:02:00", amount=90.0),
        _fraud(4, ts="2026-03-02T10:03:00"),  # o único alerta do V2
    ]

    @staticmethod
    def _local_processor(spark: SparkSession, root, query_id: str = "query-A") -> StreamProcessor:
        base = root.as_uri()
        p = _engine_processor(spark, "c1", "c2")
        p._silver = f"{base}/silver"
        p._checkpoints = f"{base}/checkpoints"
        p._history_path = f"{base}/silver/_stream_state/recent_events/"
        p._progress_path = f"{base}/checkpoints/stream_processor_progress"
        p._query_id = query_id
        return p

    @classmethod
    def _new_batch(cls, spark: SparkSession):
        # DataFrame novo a cada execução, como no foreachBatch real.
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

    def test_only_the_engines_alert_is_published(self, spark, tmp_path):
        from unittest.mock import patch

        p = self._local_processor(spark, tmp_path)
        published: dict[str, list[str]] = {}

        def capture(df, topic, key_col):
            published[topic] = [r["transaction_id"] for r in df.collect()]

        with patch.object(p, "_write_to_kafka", side_effect=capture):
            p._process_batch(self._new_batch(spark), 0)

        assert sorted(published[sp.settings.kafka.topic_enriched]) == ["t1", "t2", "t3", "t4"]
        assert published[sp.settings.kafka.topic_fraud_alerts] == ["t4"]

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

    def test_the_state_written_by_a_batch_feeds_the_next_one_even_after_a_restart(
        self, spark, tmp_path
    ):
        """Reiniciar o job (processor novo, mesmo caminho de estado) não zera a janela curta: a
        viagem impossível é vista contra um evento gravado pelo processo anterior."""
        from unittest.mock import patch

        first = self._local_processor(spark, tmp_path)
        with patch.object(first, "_write_to_kafka"):
            first._process_batch(_make_tx(spark, [_legit(1, ts="2026-03-02T12:00:00")]), 0)

        restarted = self._local_processor(spark, tmp_path)
        jump = event(2, "c1", "2026-03-02T12:15:00", 100.0, at=SALVADOR, dest="acc-new", is_fraud=True)
        with patch.object(restarted, "_write_to_kafka") as kafka:
            restarted._process_batch(_make_tx(spark, [jump]), 1)

        alerted = self._silver_stream(spark, restarted).filter("transaction_id = 't2'").first()
        assert "GEO_VELOCITY" in alerted["fraud_signals"]
        assert alerted["is_fraud_predicted"] is True
        assert kafka.call_count == 2
        assert spark.read.parquet(restarted._history_path).count() == 2

    def test_duplicate_events_in_a_micro_batch_are_scored_and_stored_once(self, spark, tmp_path):
        """O at-least-once do Kafka pode repetir um evento; os joins de volta precisam ser 1:1."""
        from unittest.mock import patch

        p = self._local_processor(spark, tmp_path)
        with patch.object(p, "_write_to_kafka") as kafka:
            p._process_batch(_make_tx(spark, [*self._BATCH, self._BATCH[-1]]), 0)

        assert self._silver_stream(spark, p).count() == 4
        assert spark.read.parquet(p._history_path).count() == 4
        assert kafka.call_count == 2

    def test_state_is_persisted_without_the_label(self, spark, tmp_path):
        from unittest.mock import patch

        p = self._local_processor(spark, tmp_path)
        with patch.object(p, "_write_to_kafka"):
            p._process_batch(self._new_batch(spark), 0)

        state = spark.read.parquet(p._history_path)
        assert not set(LABEL_COLUMNS) & set(state.columns)
        assert set(state.columns) == set(EVENT_COLUMNS)

    def test_the_parquet_keeps_both_detectors_and_the_label(self, spark, tmp_path):
        from unittest.mock import patch

        p = self._local_processor(spark, tmp_path)
        with patch.object(p, "_write_to_kafka"):
            p._process_batch(self._new_batch(spark), 0)

        out = self._silver_stream(spark, p)
        assert {"fraud_score", "fraud_signals", "fraud_type_predicted", "detector_version"} <= set(
            out.columns
        )
        assert {"z_score", "fraud_score_v1", "shadow_detector_version"} <= set(out.columns)
        assert {"is_fraud", "fraud_type"} <= set(out.columns)

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

    def test_alert_id_is_deterministic_for_the_same_transaction(self, spark):
        import uuid

        p = _engine_processor(spark, "c1", "c2")
        scored = p._score_batch(_make_tx(spark, self._BATCH), p._empty_history_df())
        first = [r["alert_id"] for r in p._build_fraud_alerts(scored).collect()]
        second = [r["alert_id"] for r in p._build_fraud_alerts(scored).collect()]

        assert len(first) == 1
        assert first == second
        assert str(uuid.UUID(first[0])) == first[0]

    def test_alert_ids_differ_between_transactions(self, spark):
        p = _engine_processor(spark, "c1", "c2")
        rows = [*self._BATCH, _fraud(9, "c2", ts="2026-03-02T10:03:00", amount=7000.0)]
        scored = p._score_batch(_make_tx(spark, rows), p._empty_history_df())
        ids = [r["alert_id"] for r in p._build_fraud_alerts(scored).collect()]

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

    def test_stages_are_the_four_of_issue_36(self):
        assert sp._STAGES == ("parquet", "enriched", "alerts", "history")


# ── TestBatchStreamParity ───────────────────────────────────────────────────────


class TestBatchStreamParity:
    """Processar o dataset em vários micro-batches, com o estado persistido entre eles, dá o mesmo
    resultado que processar tudo de uma vez: é o que faz o stream valer o que o harness mediu."""

    @staticmethod
    def _events() -> list[dict]:
        return [
            _legit(1, "c1", "2026-03-02T12:00:00"),
            _legit(2, "c1", "2026-03-02T12:02:00", 95.0),
            _legit(3, "c1", "2026-03-02T12:04:00", 105.0),
            _fraud(4, "c1", "2026-03-02T12:06:00"),
            # quatro remetentes para a mesma conta em 6 min, em micro-batches diferentes
            _legit(5, "c2", "2026-03-02T12:10:00", dest="acc-mule"),
            _legit(6, "c3", "2026-03-02T12:12:00", dest="acc-mule"),
            _legit(7, "c4", "2026-03-02T12:14:00", dest="acc-mule"),
            _legit(8, "c5", "2026-03-02T12:16:00", dest="acc-mule"),
            # viagem impossível: SP → Salvador em 14 min
            event(9, "c1", "2026-03-02T12:20:00", 100.0, at=SALVADOR, dest="acc-new2"),
            _legit(10, "c1", "2026-03-02T12:30:00"),
            _legit(11, "c1", "2026-03-02T12:31:00", 98.0),
        ]

    @staticmethod
    def _reference(spark, rows, profile, p):
        events = _make_tx(spark, rows).select(*EVENT_COLUMNS)
        v2 = detect(events, profile, weights=TEST_WEIGHTS, threshold=TEST_THRESHOLD)
        v1 = p._enrich_and_score(events, p._empty_history_df())
        return {
            r["transaction_id"]: r for r in v2.join(v1.select("transaction_id", "z_score"), "transaction_id").collect()
        }

    def test_chunked_stream_matches_the_whole_dataset(self, spark, tmp_path):
        from unittest.mock import patch

        rows = self._events()
        p = TestIdempotentMicroBatch._local_processor(spark, tmp_path)
        p._profile = profile_df(spark, *[profile_row(c) for c in ("c1", "c2", "c3", "c4", "c5")])
        reference = self._reference(spark, rows, p._profile, p)

        with patch.object(p, "_write_to_kafka"):
            for batch_id, start in enumerate(range(0, len(rows), 3)):
                p._process_batch(_make_tx(spark, rows[start : start + 3]), batch_id)
        streamed = {
            r["transaction_id"]: r
            for r in TestIdempotentMicroBatch._silver_stream(spark, p).collect()
        }

        # o teste não é vazio: as janelas curtas de fato disparam, e alertam
        assert any("GEO_VELOCITY" in r["fraud_signals"] for r in reference.values())
        assert any("RECIPIENT_CONCENTRATION" in r["fraud_signals"] for r in reference.values())
        assert sum(r["is_fraud_predicted"] for r in reference.values()) >= 3

        assert streamed.keys() == reference.keys()
        for tx_id, expected in reference.items():
            got = streamed[tx_id]
            assert got["fraud_score"] == pytest.approx(expected["fraud_score"]), tx_id
            assert got["is_fraud_predicted"] == expected["is_fraud_predicted"], tx_id
            assert sorted(got["fraud_signals"]) == sorted(expected["fraud_signals"]), tx_id
            assert got["fraud_type_predicted"] == expected["fraud_type_predicted"], tx_id
            if expected["z_score"] is None:
                assert got["z_score"] is None, tx_id
            else:
                assert got["z_score"] == pytest.approx(expected["z_score"]), tx_id


# ── TestLoadStaticInputs ────────────────────────────────────────────────────────


class TestLoadStaticInputs:
    @staticmethod
    def _processor(spark, root, **kwargs) -> StreamProcessor:
        p = StreamProcessor(spark=spark, **kwargs)
        p._gold = f"{root.as_uri()}/gold"
        return p

    @staticmethod
    def _dim(spark):
        return _make_dim_customers(
            spark, [{"customer_key": "c1", "segment": "PRIVATE", "risk_score": 1.0, "city": "SP"}]
        )

    def test_a_missing_profile_fails_with_an_actionable_message(self, spark, tmp_path):
        p = self._processor(spark, tmp_path, dim_customers=self._dim(spark))
        with pytest.raises(RuntimeError, match="Rode o batch antes do streaming"):
            p._load_static_inputs()

    def test_loads_the_profile_from_gold(self, spark, tmp_path):
        profile_df(spark, profile_row("c1")).write.parquet(
            f"{tmp_path.as_uri()}/gold/customer_behavior_profile/"
        )
        p = self._processor(spark, tmp_path, dim_customers=self._dim(spark))
        p._load_static_inputs()
        assert [r["customer_id"] for r in p._profile.collect()] == ["c1"]

    def test_does_not_replace_injected_inputs(self, spark, tmp_path):
        injected = profile_df(spark, profile_row("c1"))
        dim = self._dim(spark)
        p = self._processor(spark, tmp_path, dim_customers=dim, profile=injected)
        p._load_static_inputs()  # não existe nada em gold/: seria erro se tentasse ler
        assert p._profile is injected
        assert p._dim_customers is dim

    def test_run_loads_the_static_inputs_before_reading_the_stream(self, spark):
        from unittest.mock import patch

        class _Stop(Exception):
            pass

        calls: list[str] = []

        def read(**_kwargs):
            calls.append("stream")
            raise _Stop

        p = StreamProcessor(spark=spark)
        with (
            patch.object(p, "_load_static_inputs", side_effect=lambda: calls.append("static")),
            patch.object(p, "read_transactions_stream", side_effect=read),
            pytest.raises(_Stop),
        ):
            p.run()
        assert calls == ["static", "stream"]
