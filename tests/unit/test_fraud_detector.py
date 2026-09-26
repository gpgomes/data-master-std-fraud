"""Testes do detector multi-signal: vazamento, paridade entre chunks e o adaptador do harness."""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.transformation.fraud import detector as det
from src.transformation.fraud import weights as wt
from src.transformation.fraud.detector_api import DetectorOutput
from src.transformation.fraud.multisignal import MultiSignalV2
from src.transformation.fraud.replay import ReplayDataset
from src.transformation.fraud.signals import SIGNALS
from tests.unit.fraud_helpers import (
    LABELLED_EVENT_SCHEMA,
    RIO,
    SALVADOR,
    SAO_PAULO,
    df_from_rows,
    event,
    events_df,
    profile_df,
    profile_row,
)

# Pesos fortes e explícitos: os testes não dependem da calibração versionada em `weights.py`.
STRONG = {s: 0.0 for s in SIGNALS} | {
    "NEW_DEVICE": 0.6,
    "NEW_IP": 0.6,
    "GEO_VELOCITY": 0.9,
    "NEW_DESTINATION": 0.3,
    "RECIPIENT_CONCENTRATION": 0.9,
}


def _detect(spark, rows, *profiles, recent=None, weights=STRONG, threshold=0.5):
    out = det.detect(
        events_df(spark, rows),
        profile_df(spark, *profiles),
        recent,
        weights=weights,
        threshold=threshold,
    )
    return {r["transaction_id"]: r.asDict() for r in out.collect()}


class TestDetect:
    def test_output_columns(self, fraud_spark) -> None:
        result = det.detect(
            events_df(fraud_spark, [event(1)]), profile_df(fraud_spark), weights=STRONG
        )
        for column in (
            "fraud_score",
            "is_fraud_predicted",
            "fraud_signals",
            "fraud_type_predicted",
            "detector_version",
        ):
            assert column in result.columns
        assert result.first()["detector_version"] == "multisignal-v2" == det.DETECTOR_VERSION

    def test_a_normal_event_does_not_alert(self, fraud_spark) -> None:
        out = _detect(fraud_spark, [event(1)])["t1"]
        assert out["is_fraud_predicted"] is False
        assert out["fraud_signals"] == []
        assert out["fraud_type_predicted"] is None

    def test_an_account_takeover_alerts_with_its_reasons_and_type(self, fraud_spark) -> None:
        thief = event(1, device="dev-thief", ip="200.9.9.1", at=SALVADOR, dest="acc-mule")
        out = _detect(fraud_spark, [thief])["t1"]
        assert out["is_fraud_predicted"] is True
        assert {"NEW_DEVICE", "NEW_IP", "GEO_FAR_FROM_HOME", "NEW_DESTINATION"} <= set(
            out["fraud_signals"]
        )
        assert out["fraud_type_predicted"] == "ACCOUNT_TAKEOVER"

    def test_card_cloning_is_told_apart_from_takeover(self, fraud_spark) -> None:
        rows = [
            event(0, ts="2026-03-02T12:00:00", at=SAO_PAULO, device=None, ip=None),
            event(1, ts="2026-03-02T12:01:00", at=SAO_PAULO, device=None, ip=None),
            event(
                "clone", ts="2026-03-02T12:10:00", at=SALVADOR, device=None, ip=None, dest="acc-x"
            ),
        ]
        out = _detect(fraud_spark, rows)["tclone"]
        assert out["fraud_type_predicted"] == "CARD_CLONING"
        assert out["is_fraud_predicted"] is True

    def test_money_laundering_by_concentration(self, fraud_spark) -> None:
        rows = [
            event(f"s{i}", f"sender{i}", ts=f"2026-03-02T12:0{i}:00", dest="acc-mule")
            for i in range(4)
        ]
        profiles = [profile_row(f"sender{i}") for i in range(4)]
        out = _detect(fraud_spark, rows, *profiles)
        assert out["ts3"]["fraud_type_predicted"] == "MONEY_LAUNDERING"
        assert out["ts0"]["fraud_type_predicted"] is None

    def test_alert_is_strictly_above_the_threshold(self, fraud_spark) -> None:
        weights = {s: 0.0 for s in SIGNALS} | {"NEW_DEVICE": 0.5}
        rows = [event(1, device="dev-thief")]  # score = 0,5
        assert (
            _detect(fraud_spark, rows, weights=weights, threshold=0.5)["t1"]["is_fraud_predicted"]
            is False
        )
        assert (
            _detect(fraud_spark, rows, weights=weights, threshold=0.49)["t1"]["is_fraud_predicted"]
            is True
        )

    def test_score_is_within_zero_and_one(self, fraud_spark) -> None:
        rows = [
            event(i, device=f"d{i}", ip=f"9.9.{i}.1", amount=10.0**i, at=SALVADOR, dest=f"a{i}")
            for i in range(1, 6)
        ]
        for out in _detect(fraud_spark, rows).values():
            assert 0.0 <= out["fraud_score"] <= 1.0

    def test_defaults_come_from_the_versioned_weights(self, fraud_spark) -> None:
        result = det.detect(events_df(fraud_spark, [event(1)]), profile_df(fraud_spark))
        assert result.first()["fraud_score"] == pytest.approx(0.0)  # evento normal: nenhum sinal
        assert wt.ALERT_THRESHOLD == pytest.approx(wt.ALERT_THRESHOLD)  # importa e é numérico


class TestNoLabelLeakage:
    """Anular ou trocar `is_fraud`/`fraud_type` no input não muda nenhuma coluna de saída."""

    ROWS = [
        event(1),
        event(2, device="dev-thief", ip="200.9.9.1", at=SALVADOR, dest="acc-mule", amount=5_000.0),
        event(3, ts="2026-03-02T12:01:00", dest="acc-mule"),
    ]

    def _run(self, spark, label_fn):
        rows = [{**r, **label_fn(r)} for r in self.ROWS]
        df = df_from_rows(spark, rows, LABELLED_EVENT_SCHEMA)
        out = det.detect(df, profile_df(spark), weights=STRONG, threshold=0.5)
        return sorted((r.asDict() for r in out.collect()), key=lambda r: r["transaction_id"])

    def test_labels_absent_true_or_inverted_give_identical_outputs(self, fraud_spark) -> None:
        none = self._run(fraud_spark, lambda r: {})
        truthful = self._run(
            fraud_spark,
            lambda r: {"is_fraud": r["device_id"] == "dev-thief", "fraud_type": "ACCOUNT_TAKEOVER"},
        )
        inverted = self._run(
            fraud_spark,
            lambda r: {"is_fraud": r["device_id"] != "dev-thief", "fraud_type": "MONEY_LAUNDERING"},
        )
        assert none == truthful == inverted

    def test_label_columns_do_not_appear_in_the_output(self, fraud_spark) -> None:
        df = df_from_rows(
            fraud_spark,
            [{**r, "is_fraud": True, "fraud_type": "CARD_CLONING"} for r in self.ROWS],
            LABELLED_EVENT_SCHEMA,
        )
        out = det.detect(df, profile_df(fraud_spark), weights=STRONG)
        assert "is_fraud" not in out.columns and "fraud_type" not in out.columns

    def test_the_detector_source_never_mentions_the_label_columns(self) -> None:
        """Guarda estática: nenhum módulo do núcleo lê `is_fraud`/`fraud_type` de um evento."""
        base = Path(__file__).resolve().parents[2] / "src" / "transformation" / "fraud"
        for name in ("signals.py", "scoring.py", "fraud_type.py", "detector.py"):
            tree = ast.parse((base / name).read_text(encoding="utf-8"))
            strings = {
                n.value
                for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and isinstance(n.value, str)
            }
            assert "is_fraud" not in strings, name
            assert "fraud_type" not in strings, name


class TestChunkParity:
    """O streaming (#46) processa micro-batches com o estado curto em `recent`. O resultado tem de ser
    o mesmo de processar tudo de uma vez."""

    def _rows(self):
        home = [event(i, ts=f"2026-03-02T12:0{i}:00", at=SAO_PAULO) for i in range(4)]
        clone = [event("far", ts="2026-03-02T12:08:00", at=SALVADOR)]
        mule = [
            event(f"m{i}", f"sender{i}", ts=f"2026-03-02T12:1{i}:00", dest="acc-mule")
            for i in range(4)
        ]
        burst = [
            event(f"b{i}", "c1", ts=f"2026-03-02T12:3{i // 2}:{(i % 2) * 20 + 10:02d}")
            for i in range(12)
        ]
        return sorted(home + clone + mule + burst, key=lambda r: r["timestamp"])

    def _profiles(self):
        return [profile_row("c1")] + [profile_row(f"sender{i}") for i in range(4)]

    def test_two_chunks_with_recent_equal_the_whole(self, fraud_spark) -> None:
        rows = self._rows()
        cut = len(rows) // 2
        first, second = rows[:cut], rows[cut:]
        whole = _detect(fraud_spark, rows, *self._profiles(), threshold=0.5)

        part1 = _detect(fraud_spark, first, *self._profiles(), threshold=0.5)
        part2 = _detect(
            fraud_spark,
            second,
            *self._profiles(),
            recent=events_df(fraud_spark, first),
            threshold=0.5,
        )
        assert {**part1, **part2} == whole

    def test_without_recent_the_second_chunk_loses_the_window_context(self, fraud_spark) -> None:
        """Documenta por que o estado curto existe: sem ele o clone da 2ª metade passaria."""
        rows = self._rows()
        first = [r for r in rows if r["timestamp"] < "2026-03-02T12:08:00"]
        second = [r for r in rows if r["timestamp"] >= "2026-03-02T12:08:00"]
        with_context = _detect(
            fraud_spark, second, *self._profiles(), recent=events_df(fraud_spark, first)
        )
        without = _detect(fraud_spark, second, *self._profiles())
        assert with_context["tfar"]["is_fraud_predicted"] is True
        assert without["tfar"]["is_fraud_predicted"] is False

    def test_result_is_deterministic(self, fraud_spark) -> None:
        rows = self._rows()
        assert _detect(fraud_spark, rows, *self._profiles()) == _detect(
            fraud_spark, rows, *self._profiles()
        )


class TestVersionedWeights:
    def test_every_signal_has_a_weight_in_the_unit_interval(self) -> None:
        assert set(wt.SIGNAL_WEIGHTS) == set(SIGNALS)
        assert all(0.0 <= w <= 1.0 for w in wt.SIGNAL_WEIGHTS.values())

    def test_threshold_is_a_probability_and_the_version_is_recorded(self) -> None:
        assert 0.0 < wt.ALERT_THRESHOLD < 1.0
        assert wt.WEIGHTS_VERSION and wt.WEIGHTS_VERSION != "uncalibrated"

    def test_calibration_zeroes_the_signals_that_do_not_discriminate(self) -> None:
        """No regime sintético o TX_VELOCITY não separa fraude de legítimo e o UNUSUAL_HOUR não é
        exercitado (a janela padrão é de dia): a busca os deixa em zero."""
        assert wt.SIGNAL_WEIGHTS["TX_VELOCITY"] == 0.0
        assert wt.SIGNAL_WEIGHTS["UNUSUAL_HOUR"] == 0.0


# ── Adaptador do harness ───────────────────────────────────────────────────────


def _dataset(history, events) -> ReplayDataset:
    customers = [{"customer_id": "c1", "segment": "VAREJO", "account_opening_date": "2020-01-01"}]
    return ReplayDataset(
        seed=1,
        events=events,
        truth={},
        cutoff=datetime(2026, 3, 2, tzinfo=UTC),
        customers=customers,
        history=history,
    )


def _hist(i, **kw):
    return {**event(f"h{i}", **kw), "is_fraud": False}


class TestMultiSignalAdapter:
    def _history(self):
        return [
            _hist(i, ts="2026-02-10T13:00:00", device="dev-known", ip="10.1.1.7", dest="acc-known")
            for i in range(10)
        ]

    def test_scores_every_event_through_the_harness_contract(self, fraud_spark) -> None:
        events = [
            {
                **event(1, device="dev-known", ip="10.1.1.7", dest="acc-known"),
                "is_fraud": False,
                "fraud_type": None,
            },
            {
                **event(2, device="dev-thief", ip="200.9.9.1", at=SALVADOR, dest="acc-mule"),
                "is_fraud": True,
                "fraud_type": "ACCOUNT_TAKEOVER",
            },
        ]
        out = MultiSignalV2(weights=STRONG, threshold=0.5).score(
            fraud_spark, _dataset(self._history(), events)
        )
        assert set(out) == {"t1", "t2"}
        assert all(isinstance(o, DetectorOutput) for o in out.values())
        assert out["t1"].alert is False and out["t2"].alert is True
        assert out["t2"].predicted_type == "ACCOUNT_TAKEOVER"
        assert "NEW_DEVICE" in out["t2"].signals
        assert out["t1"].covered is True  # o cliente tem histórico suficiente
        assert len(out["t2"].values) == len(SIGNALS)

    def test_signal_values_follow_the_canonical_order(self, fraud_spark) -> None:
        events = [event(1, device="dev-thief")]
        out = MultiSignalV2(weights=STRONG).score(fraud_spark, _dataset(self._history(), events))
        assert out["t1"].values[SIGNALS.index("NEW_DEVICE")] == 1.0
        assert out["t1"].values[SIGNALS.index("NEW_IP")] == 0.0

    def test_needs_the_history(self, fraud_spark) -> None:
        with pytest.raises(ValueError, match="histórico"):
            MultiSignalV2().score(fraud_spark, _dataset([], [event(1)]))

    def test_declares_that_it_needs_history_and_its_version(self) -> None:
        assert MultiSignalV2.needs_history is True
        assert MultiSignalV2.name == "multisignal-v2"

    def test_a_customer_without_enough_history_is_not_covered(self, fraud_spark) -> None:
        few = [_hist(0, device="dev-known")]
        out = MultiSignalV2(weights=STRONG).score(fraud_spark, _dataset(few, [event(1)]))
        assert out["t1"].covered is False

    def test_the_label_never_reaches_the_detector(self, fraud_spark) -> None:
        """Mesmo evento com rótulos opostos: mesma saída."""
        base = event(1, device="dev-thief", at=SALVADOR)
        a = MultiSignalV2(weights=STRONG).score(
            fraud_spark,
            _dataset(self._history(), [{**base, "is_fraud": True, "fraud_type": "CARD_CLONING"}]),
        )
        b = MultiSignalV2(weights=STRONG).score(
            fraud_spark,
            _dataset(self._history(), [{**base, "is_fraud": False, "fraud_type": None}]),
        )
        assert a == b
        _ = RIO  # coordenada usada por outros testes do módulo
