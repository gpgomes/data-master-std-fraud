"""Testes do harness de avaliação: replay, protocolo, relatório e o detector V1 (issue #44)."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from pyspark.sql import SparkSession

from src.transformation.fraud import evaluate as ev
from src.transformation.fraud.replay import (
    ReplayConfig,
    ReplayDataset,
    simulate_batch,
    simulate_stream,
)
from src.transformation.fraud.report import render_report

SMALL = ReplayConfig(n_customers=60, rate_tps=5.0, duration_minutes=30, warmup_minutes=10)


# ── Replay ─────────────────────────────────────────────────────────────────────


class TestSimulateStream:
    @pytest.fixture(scope="class")
    def dataset(self) -> ReplayDataset:
        return simulate_stream(7, SMALL)

    def test_same_seed_and_config_give_the_same_events_and_truth(self, dataset) -> None:
        again = simulate_stream(7, SMALL)
        assert [e["transaction_id"] for e in again.events] == [
            e["transaction_id"] for e in dataset.events
        ]
        assert again.truth == dataset.truth

    def test_another_seed_gives_other_events_and_customers(self, dataset) -> None:
        other = simulate_stream(8, SMALL)
        assert {e["customer_id"] for e in other.events}.isdisjoint(
            {e["customer_id"] for e in dataset.events}
        )

    def test_events_are_sorted_by_time_and_start_at_the_configured_instant(self, dataset) -> None:
        stamps = [e["timestamp"] for e in dataset.events]
        assert stamps == sorted(stamps)
        assert stamps[0] >= SMALL.start.isoformat()

    def test_at_least_one_event_per_tick(self, dataset) -> None:
        assert len(dataset.events) >= int(SMALL.duration_minutes * 60 * SMALL.rate_tps)

    def test_fraud_share_is_close_to_the_generator_target(self, dataset) -> None:
        share = sum(e["is_fraud"] for e in dataset.events) / len(dataset.events)
        assert 0.012 <= share <= 0.04

    def test_every_fraud_event_has_a_truth_row_with_episode_and_scenario(self, dataset) -> None:
        for event in dataset.events:
            row = dataset.truth.get(event["transaction_id"])
            if event["is_fraud"]:
                assert row is not None
                assert row["episode_id"].startswith("ep-")
                assert row["scenario"] == event["fraud_type"]
            elif row is not None:
                assert row["episode_id"] == "" and row["hard_negative"]

    def test_truth_only_references_emitted_transactions(self, dataset) -> None:
        emitted = {e["transaction_id"] for e in dataset.events}
        assert set(dataset.truth) <= emitted

    def test_cutoff_is_start_plus_warmup(self, dataset) -> None:
        assert dataset.cutoff == SMALL.start + timedelta(minutes=10)
        assert SMALL.cutoff == dataset.cutoff

    def test_identity_theft_exists_because_customers_use_a_fixed_reference_date(self) -> None:
        """Sem `reference_date` fixa não haveria contas recentes numa janela no passado."""
        dataset = simulate_stream(
            7, ReplayConfig(n_customers=400, rate_tps=8.0, duration_minutes=40)
        )
        assert any(e["fraud_type"] == "IDENTITY_THEFT" for e in dataset.events)


class TestSimulateBatch:
    def test_density_and_determinism(self) -> None:
        end = datetime(2026, 3, 2, tzinfo=UTC)
        a = simulate_batch(3, 100, 2_000, end)
        b = simulate_batch(3, 100, 2_000, end)
        assert len(a.events) == 2_000
        assert [e["transaction_id"] for e in a.events] == [e["transaction_id"] for e in b.events]
        assert a.cutoff == end - timedelta(days=180)
        assert a.label == "batch"


# ── Dataset avaliado ───────────────────────────────────────────────────────────


def _event(i: int, ts: str, *, fraud: str | None = None) -> dict:
    return {
        "transaction_id": f"t{i}",
        "customer_id": "c1",
        "timestamp": f"2026-03-02T{ts}+00:00",
        "amount": 100.0,
        "is_fraud": fraud is not None,
        "fraud_type": fraud,
    }


def _handmade() -> tuple[ReplayDataset, dict[str, ev.DetectorOutput]]:
    events = [
        _event(0, "12:00:00"),  # antes do corte
        _event(1, "12:10:00"),
        _event(2, "12:11:00"),
        _event(3, "12:12:00", fraud="ACCOUNT_TAKEOVER"),
        _event(4, "12:13:00", fraud="ACCOUNT_TAKEOVER"),
        _event(5, "12:20:00", fraud="CARD_CLONING"),
        _event(6, "12:21:00"),
    ]
    truth = {
        "t1": {"episode_id": "", "scenario": "", "stealth": False, "hard_negative": "new_device"},
        "t3": {
            "episode_id": "ep-1",
            "scenario": "ACCOUNT_TAKEOVER",
            "stealth": False,
            "hard_negative": "",
        },
        "t4": {
            "episode_id": "ep-1",
            "scenario": "ACCOUNT_TAKEOVER",
            "stealth": False,
            "hard_negative": "",
        },
        "t5": {
            "episode_id": "ep-2",
            "scenario": "CARD_CLONING",
            "stealth": True,
            "hard_negative": "",
        },
        "t6": {
            "episode_id": "",
            "scenario": "",
            "stealth": False,
            "hard_negative": "travel;off_hours",
        },
    }
    cutoff = datetime(2026, 3, 2, 12, 10, tzinfo=UTC)
    outputs = {
        "t0": ev.DetectorOutput(0.9, True, True),
        "t1": ev.DetectorOutput(0.1, False, True),
        "t2": ev.DetectorOutput(0.0, False, False),
        "t3": ev.DetectorOutput(0.9, True, True),
        "t4": ev.DetectorOutput(0.4, False, True),
        "t5": ev.DetectorOutput(0.2, False, True),
        "t6": ev.DetectorOutput(0.7, True, True),
    }
    return ReplayDataset(1, events, truth, cutoff), outputs


class TestScoreDataset:
    def test_only_events_after_the_cutoff_are_evaluated(self) -> None:
        dataset, outputs = _handmade()
        ds = ev.score_dataset(dataset, outputs)
        assert len(ds.y) == 6
        assert ds.y.tolist() == [0, 0, 1, 1, 1, 0]
        assert ds.deployed_alert.tolist() == [False, False, True, False, False, True]
        assert ds.covered.tolist() == [True, False, True, True, True, True]

    def test_ground_truth_is_joined_by_transaction_id(self) -> None:
        ds = ev.score_dataset(*_handmade())
        assert ds.fraud_type.tolist() == [
            "",
            "",
            "ACCOUNT_TAKEOVER",
            "ACCOUNT_TAKEOVER",
            "CARD_CLONING",
            "",
        ]
        assert ds.episode_id.tolist() == ["", "", "ep-1", "ep-1", "ep-2", ""]
        assert ds.stealth.tolist() == [False, False, False, False, True, False]

    def test_hard_negative_masks_follow_the_sidecar(self) -> None:
        ds = ev.score_dataset(*_handmade())
        assert ds.hard_negative["new_device"].tolist() == [True, False, False, False, False, False]
        assert ds.hard_negative["travel"].tolist() == [False, False, False, False, False, True]
        assert ds.hard_negative["off_hours"].tolist() == [False, False, False, False, False, True]
        assert not ds.hard_negative["new_ip"].any()
        assert ds.hard_negative[ev.NO_HARD_NEGATIVE].tolist()[1] is True

    def test_fraud_rows_never_count_as_hard_negatives(self) -> None:
        dataset, outputs = _handmade()
        dataset.truth["t3"]["hard_negative"] = "new_device"  # ruído no sidecar
        ds = ev.score_dataset(dataset, outputs)
        assert not ds.hard_negative["new_device"][2]


class TestSummarizeAndAggregate:
    def test_summary_by_hand(self) -> None:
        ds = ev.score_dataset(*_handmade())
        s = ev.summarize(ds, ds.deployed_alert)
        # alertou t3 (fraude) e t6 (legítimo); t4 e t5 são falsos negativos
        assert (s["tp"], s["fp"], s["fn"], s["tn"]) == (1, 1, 2, 2)
        assert s["precision"] == pytest.approx(0.5)
        assert s["recall"] == pytest.approx(1 / 3)
        assert s["fpr"] == pytest.approx(1 / 3)
        assert s["alerts_per_1000"] == pytest.approx(2 / 6 * 1000)
        assert s["by_type"] == {"ACCOUNT_TAKEOVER": (2, 0.5), "CARD_CLONING": (1, 0.0)}
        assert s["by_scenario"]["ACCOUNT_TAKEOVER|normal"] == (2, 0.5)
        assert s["by_scenario"]["CARD_CLONING|stealth"] == (1, 0.0)
        assert s["by_hard_negative"]["new_device"] == (1, 0.0)
        assert s["by_hard_negative"]["travel"] == (1, 1.0)
        assert s["by_hard_negative"][ev.NO_HARD_NEGATIVE] == (1, 0.0)
        assert s["by_hard_negative"]["big_purchase"][0] == 0
        assert s["episodes"]["episodes"] == 2
        assert s["episodes"]["recall"] == pytest.approx(0.5)  # ep-1 detectado, ep-2 não
        assert s["episodes"]["ttd_median"] == 0.0  # o 1º evento de ep-1 já alertou

    def test_a_different_decision_changes_the_summary(self) -> None:
        ds = ev.score_dataset(*_handmade())
        s = ev.summarize(ds, ds.score > 0.3)  # alerta em t3 t4 t6
        assert (s["tp"], s["fp"]) == (2, 1)
        assert s["episodes"]["recall"] == pytest.approx(0.5)

    def test_aggregate_over_seeds(self) -> None:
        ds = ev.score_dataset(*_handmade())
        s = ev.summarize(ds, ds.deployed_alert)
        agg = ev.aggregate([s, s])
        assert agg["precision"] == (pytest.approx(0.5), 0.0)
        assert agg["by_type"]["ACCOUNT_TAKEOVER"]["n"] == 4  # soma das seeds
        assert agg["by_type"]["ACCOUNT_TAKEOVER"]["value"] == (pytest.approx(0.5), 0.0)
        assert agg["episodes"]["recall"] == (pytest.approx(0.5), 0.0)


# ── Protocolo (com um detector falso, sem Spark) ───────────────────────────────


class AmountDetector:
    """Detector de brinquedo, só para exercitar o protocolo sem Spark."""

    name = "fake-amount"

    def score_events(self, spark, events):
        return {
            e["transaction_id"]: ev.DetectorOutput(
                score=min(e["amount"] / 1000, 1.0), alert=e["amount"] > 500, covered=True
            )
            for e in events
        }


class TestRunProtocol:
    CFG = ev.EvalConfig(
        replay=SMALL, validation_seed=100, test_seeds=(1, 2), include_batch_density=False
    )

    @pytest.fixture(scope="class")
    def results(self) -> dict:
        return ev.run_protocol(None, self.CFG, AmountDetector())

    def test_structure(self, results) -> None:
        assert results["detector"] == "fake-amount"
        assert set(results["conditions"]) == {
            ev.DEPLOYED,
            *(ev.condition_key(f) for f in ev.OPERATING_FPRS),
        }
        assert [t["seed"] for t in results["tests"]] == [1, 2]
        assert results["validation"]["seed"] == 100
        assert results["batch_density"] is None
        assert results["config"]["test_seeds"] == [1, 2]

    def test_thresholds_are_calibrated_on_validation_and_get_looser_with_the_target(
        self, results
    ) -> None:
        t = results["thresholds"]
        assert t[0.005] >= t[0.01] >= t[0.02]

    def test_a_looser_fpr_target_never_lowers_recall(self, results) -> None:
        recalls = [
            results["conditions"][ev.condition_key(f)]["recall"][0] for f in ev.OPERATING_FPRS
        ]
        assert recalls == sorted(recalls)

    def test_measured_fpr_stays_near_the_target_on_unseen_seeds(self, results) -> None:
        for fpr in ev.OPERATING_FPRS:
            measured = results["conditions"][ev.condition_key(fpr)]["fpr"][0]
            assert measured <= fpr + 0.03

    def test_ranking_metrics_and_coverage(self, results) -> None:
        assert 0.0 <= results["ranking"]["pr_auc"][0] <= 1.0
        assert results["coverage"][0] == 1.0

    def test_the_test_seeds_are_not_used_to_calibrate(self, results) -> None:
        """O limiar vem só da seed de validação: mudar as seeds de teste não o altera."""
        other = ev.run_protocol(
            None,
            ev.EvalConfig(
                replay=SMALL, validation_seed=100, test_seeds=(3,), include_batch_density=False
            ),
            AmountDetector(),
        )
        assert other["thresholds"] == results["thresholds"]

    def test_same_config_renders_the_same_document(self, results) -> None:
        again = ev.run_protocol(None, self.CFG, AmountDetector())
        assert render_report(again) == render_report(results)


# ── Relatório ──────────────────────────────────────────────────────────────────


class TestRenderReport:
    @pytest.fixture(scope="class")
    def text(self) -> str:
        cfg = ev.EvalConfig(
            replay=SMALL, validation_seed=100, test_seeds=(1, 2), include_batch_density=False
        )
        return render_report(ev.run_protocol(None, cfg, AmountDetector()))

    def test_has_every_section(self, text) -> None:
        for heading in (
            "## Resumo",
            "## Protocolo",
            "## Resultados",
            "## Onde o detector acerta e erra",
            "## Cobertura por densidade de eventos",
            "## Hipótese: Precision perto de 50%",
            "## Limitações e circularidade",
        ):
            assert heading in text

    def test_declares_the_circularity_and_the_relative_gain(self, text) -> None:
        assert "Circularidade" in text
        assert "relativo" in text

    def test_has_no_timestamp_of_generation_and_no_absolute_path(self, text) -> None:
        assert "/Users/" not in text
        assert "Gerado em" not in text

    def test_uses_brazilian_number_format(self, text) -> None:
        assert "Precision" in text and "%" in text
        assert "n/d" in text or "," in text

    def test_hypothesis_verdict_is_stated(self, text) -> None:
        assert "confirmada" in text or "refutada" in text

    def test_batch_density_row_appears_when_measured(self) -> None:
        results = ev.run_protocol(
            None,
            ev.EvalConfig(
                replay=SMALL, validation_seed=100, test_seeds=(1,), include_batch_density=False
            ),
            AmountDetector(),
        )
        results["batch_density"] = {
            "seed": 1, "events": 1000, "fraud": 25, "episodes": 10, "coverage": 0.004,
            "customers": 20, "events_per_customer": 50.0, "alerts": 2, "recall": 0.03, "precision": 0.5,
        }  # fmt: skip
        text = render_report(results)
        assert "0,40%" in text  # cobertura de 0,4%
        assert "só enxerga fraude na densidade do streaming" in text

    def test_verdict_rule(self) -> None:
        from src.transformation.fraud.report import _hypothesis_verdict

        assert _hypothesis_verdict(0.51)[0] is True
        assert _hypothesis_verdict(0.60)[0] is True
        assert _hypothesis_verdict(0.098)[0] is False
        assert "refutada" in _hypothesis_verdict(0.098)[1]


# ── CLI ────────────────────────────────────────────────────────────────────────


class TestBuildConfig:
    def test_defaults_match_the_documented_protocol(self) -> None:
        cfg = ev.build_config(ev._parse_args([]))
        assert cfg.replay.n_customers == 1_000
        assert cfg.replay.rate_tps == 10.0
        assert cfg.replay.warmup_minutes == 60
        assert cfg.test_seeds == (1, 2, 3, 4, 5)
        assert cfg.validation_seed == 1000
        assert cfg.include_batch_density is True
        assert cfg.replay.start.tzinfo is not None

    def test_profile_until_overrides_the_warmup(self) -> None:
        cfg = ev.build_config(
            ev._parse_args(
                ["--start", "2026-03-02T12:00:00", "--profile-until", "2026-03-02T13:30:00"]
            )
        )
        assert cfg.replay.warmup_minutes == 90
        assert cfg.replay.cutoff == datetime(2026, 3, 2, 13, 30, tzinfo=UTC)

    def test_skip_batch_density_and_custom_seeds(self) -> None:
        cfg = ev.build_config(ev._parse_args(["--skip-batch-density", "--test-seeds", "7", "8"]))
        assert cfg.include_batch_density is False
        assert cfg.test_seeds == (7, 8)

    def test_condition_key_is_stable(self) -> None:
        assert ev.condition_key(0.005) == "fpr_0.005"
        assert ev.condition_key(0.01) == "fpr_0.01"


# ── O detector V1 sobre o _enrich_and_score real (Spark local) ─────────────────


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("test-fraud-eval")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _tx(i: int, customer: str, ts: str, amount: float, **extra) -> dict:
    return {
        "transaction_id": f"t{i}",
        "customer_id": customer,
        "timestamp": f"2024-06-15T{ts}+00:00",
        "amount": amount,
        **extra,
    }


class TestZScoreV1:
    EVENTS = [
        _tx(0, "c1", "10:00:00", 100.0),
        _tx(1, "c1", "10:10:00", 110.0),
        _tx(2, "c1", "10:20:00", 105.0),
        _tx(3, "c1", "10:30:00", 1000.0),  # pico de valor
        _tx(4, "c2", "10:05:00", 50.0),  # outro cliente, sem histórico
    ]

    def test_flags_the_spike_and_reports_missing_baselines(self, spark) -> None:
        out = ev.ZScoreV1().score_events(spark, self.EVENTS)
        assert out["t0"] == ev.DetectorOutput(0.0, False, False)  # sem baseline
        assert out["t1"].covered is False  # só 1 evento na janela (mínimo 2)
        assert out["t2"].covered is True and out["t2"].alert is False
        assert out["t3"].alert is True and out["t3"].score > 3.0
        assert out["t4"] == ev.DetectorOutput(0.0, False, False)

    def test_score_is_the_absolute_z_and_zero_without_baseline(self, spark) -> None:
        out = ev.ZScoreV1().score_events(spark, self.EVENTS)
        assert all(o.score >= 0 for o in out.values())
        assert all(o.score == 0.0 for o in out.values() if not o.covered)

    def test_never_reads_the_fraud_labels(self, spark) -> None:
        plain = ev.ZScoreV1().score_events(spark, self.EVENTS)
        labelled = ev.ZScoreV1().score_events(
            spark,
            [
                {**e, "is_fraud": e["amount"] > 500, "fraud_type": "CARD_CLONING"}
                for e in self.EVENTS
            ],
        )
        assert labelled == plain

    def test_is_the_detector_that_runs_in_production(self) -> None:
        from src.transformation.streaming.stream_processor import DETECTOR_VERSION

        assert ev.ZScoreV1.name == DETECTOR_VERSION == "zscore-v1"

    def test_end_to_end_on_a_replay(self, spark) -> None:
        dataset = simulate_stream(11, SMALL)
        ds = ev.score_dataset(dataset, ev.ZScoreV1().score_events(spark, dataset.events))
        assert len(ds.y) > 0
        assert np.isfinite(ds.score).all()
        assert ds.covered.mean() > 0.5  # a densidade do stream forma baseline
        assert 0.0 < ds.deployed_alert.mean() < 0.2
        assert not math.isnan(ev.summarize(ds, ds.deployed_alert)["recall"])
