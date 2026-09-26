"""Testes do harness de avaliação: replay, protocolo, relatório e o detector V1 (issue #44)."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from pyspark.sql import SparkSession

from src.transformation.fraud import evaluate as ev
from src.transformation.fraud.multisignal import MultiSignalV2
from src.transformation.fraud.replay import (
    ReplayConfig,
    ReplayDataset,
    simulate_batch,
    simulate_history,
    simulate_stream,
)
from src.transformation.fraud.report import render_report
from src.transformation.fraud.scoring import noisy_or_np
from src.transformation.fraud.signals import SIGNALS

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


# ── Protocolo (com detectores falsos, sem Spark) ───────────────────────────────


class AmountDetector:
    """Detector de brinquedo (só o valor), para exercitar o protocolo sem Spark."""

    name = "zscore-v1"  # o nome do V1, para o relatório montar as seções dele
    needs_history = False
    infers_type = False

    def score(self, spark, dataset):
        return {
            e["transaction_id"]: ev.DetectorOutput(
                score=min(e["amount"] / 1000, 1.0), alert=e["amount"] > 500, covered=True
            )
            for e in dataset.events
        }


class SignalDetector:
    """Detector de brinquedo com sinais, tipo previsto e pesos (como o V2), sem Spark."""

    name = "multisignal-v2"
    needs_history = True
    infers_type = True
    weights = {s: 0.5 for s in SIGNALS}
    threshold = 0.6

    def score(self, spark, dataset):
        assert dataset.history, "o protocolo tem de anexar o histórico a quem precisa dele"
        i_dest, i_amount = SIGNALS.index("NEW_DESTINATION"), SIGNALS.index("AMOUNT_ANOMALY")
        out = {}
        for e in dataset.events:
            values = [0.0] * len(SIGNALS)
            values[i_dest] = 1.0 if e["amount"] > 200 or e["is_fraud"] else 0.0
            values[i_amount] = min(e["amount"] / 1000, 1.0)
            score = 1 - (1 - 0.5 * values[i_dest]) * (1 - 0.5 * values[i_amount])
            out[e["transaction_id"]] = ev.DetectorOutput(
                score=score,
                alert=score > self.threshold,
                covered=True,
                predicted_type="ACCOUNT_TAKEOVER" if e["amount"] > 800 else None,
                signals=tuple(s for s, v in zip(SIGNALS, values, strict=True) if v >= 0.5),
                values=tuple(values),
            )
        return out


CFG = ev.EvalConfig(
    replay=ReplayConfig(
        n_customers=60,
        rate_tps=5.0,
        duration_minutes=30,
        warmup_minutes=10,
        history_transactions=3_000,
    ),
    validation_seed=100,
    test_seeds=(1, 2),
    include_batch_density=False,
)


class TestRunProtocol:
    @pytest.fixture(scope="class")
    def results(self) -> dict:
        return ev.run_protocol(None, CFG, [AmountDetector(), SignalDetector()])

    def test_structure(self, results) -> None:
        assert [d["name"] for d in results["detectors"]] == ["zscore-v1", "multisignal-v2"]
        assert [t["seed"] for t in results["tests"]] == [1, 2]
        assert results["validation"]["seed"] == 100
        assert results["batch_density"] is None
        assert results["config"]["test_seeds"] == [1, 2]
        for detector in results["detectors"]:
            assert set(detector["conditions"]) == {
                ev.DEPLOYED,
                *(ev.condition_key(f) for f in ev.OPERATING_FPRS),
            }

    def test_history_is_generated_only_for_detectors_that_need_it(self, results) -> None:
        assert results["config"]["history_transactions"] == 3_000
        only_v1 = ev.run_protocol(None, CFG, [AmountDetector()])
        assert only_v1["config"]["history_transactions"] is None

    def test_thresholds_are_calibrated_on_validation_and_get_looser_with_the_target(
        self, results
    ) -> None:
        for detector in results["detectors"]:
            t = detector["thresholds"]
            assert t[0.005] >= t[0.01] >= t[0.02]

    def test_a_looser_fpr_target_never_lowers_recall(self, results) -> None:
        for detector in results["detectors"]:
            recalls = [
                detector["conditions"][ev.condition_key(f)]["recall"][0] for f in ev.OPERATING_FPRS
            ]
            assert recalls == sorted(recalls)

    def test_measured_fpr_stays_near_the_target_on_unseen_seeds(self, results) -> None:
        detector = results["detectors"][0]
        for fpr in ev.OPERATING_FPRS:
            assert detector["conditions"][ev.condition_key(fpr)]["fpr"][0] <= fpr + 0.03

    def test_ranking_metrics_and_coverage(self, results) -> None:
        for detector in results["detectors"]:
            assert 0.0 <= detector["ranking"]["pr_auc"][0] <= 1.0
            assert detector["coverage"][0] == 1.0

    def test_the_test_seeds_are_not_used_to_calibrate(self, results) -> None:
        """O limiar vem só da seed de validação: mudar as seeds de teste não o altera."""
        other = ev.run_protocol(
            None,
            ev.EvalConfig(
                replay=CFG.replay, validation_seed=100, test_seeds=(3,), include_batch_density=False
            ),
            [AmountDetector(), SignalDetector()],
        )
        assert [d["thresholds"] for d in other["detectors"]] == [
            d["thresholds"] for d in results["detectors"]
        ]

    def test_paired_comparison_is_by_seed(self, results) -> None:
        (comparison,) = results["comparison"]
        assert (
            comparison["baseline"] == "zscore-v1" and comparison["challenger"] == "multisignal-v2"
        )
        base, chal = results["detectors"]
        deployed = comparison["conditions"][ev.DEPLOYED]
        expected = [
            c[ev.DEPLOYED]["f1"] - b[ev.DEPLOYED]["f1"]
            for b, c in zip(base["per_seed"], chal["per_seed"], strict=True)
        ]
        assert deployed["f1"][0] == pytest.approx(sum(expected) / len(expected))
        assert deployed["seeds"] == 2
        assert deployed["wins"] == sum(1 for d in expected if d > 0)

    def test_type_confusion_and_signal_tables_only_for_the_detector_with_signals(
        self, results
    ) -> None:
        v1, v2 = results["detectors"]
        assert v1["type_confusion"] is None and v1["signals"] is None and v1["sensitivity"] is None
        assert set(v2["type_confusion"]) == set(ev.FRAUD_TYPES)
        assert set(v2["signals"]) == set(SIGNALS)
        assert v2["sensitivity"]["signal"] == ev.SENSITIVITY_SIGNAL
        assert v2["alert_threshold"] == 0.6

    def test_same_config_renders_the_same_document(self, results) -> None:
        again = ev.run_protocol(None, CFG, [AmountDetector(), SignalDetector()])
        assert render_report(again) == render_report(results)


# ── Matriz de tipo, sinais, comparação e sensibilidade ─────────────────────────


def _signalled(y, fraud_type, predicted, values, alert):
    n = len(y)
    return ev.ScoredDataset(
        seed=1,
        y=np.array(y, dtype=np.int8),
        score=np.array(alert, dtype=float),
        deployed_alert=np.array(alert, dtype=bool),
        covered=np.ones(n, dtype=bool),
        fraud_type=np.array(fraud_type, dtype=str),
        stealth=np.zeros(n, dtype=bool),
        episode_id=np.array([f"ep-{i}" if v else "" for i, v in enumerate(y)], dtype=str),
        epoch=np.arange(n, dtype=float),
        hard_negative={"nenhum": np.array([not v for v in y], dtype=bool)},
        predicted_type=np.array(predicted, dtype=str),
        signal_values=np.array(values, dtype=float),
    )


class TestTypeConfusion:
    def test_counts_alerted_fraud_by_true_and_predicted_type(self) -> None:
        # 4 fraudes alertadas (ATO→ATO, ATO→clone, clone→nenhuma, lavagem→lavagem), 1 não alertada, 1 legítimo
        ds = _signalled(
            y=[1, 1, 1, 1, 1, 0],
            fraud_type=[
                "ACCOUNT_TAKEOVER",
                "ACCOUNT_TAKEOVER",
                "CARD_CLONING",
                "MONEY_LAUNDERING",
                "CARD_CLONING",
                "",
            ],
            predicted=[
                "ACCOUNT_TAKEOVER",
                "CARD_CLONING",
                "",
                "MONEY_LAUNDERING",
                "CARD_CLONING",
                "ACCOUNT_TAKEOVER",
            ],
            values=[[0.0] * len(SIGNALS)] * 6,
            alert=[1, 1, 1, 1, 0, 1],
        )
        conf = ev.type_confusion(ds, ds.deployed_alert)
        assert conf["ACCOUNT_TAKEOVER"]["ACCOUNT_TAKEOVER"] == 1
        assert conf["ACCOUNT_TAKEOVER"]["CARD_CLONING"] == 1
        assert conf["CARD_CLONING"][ev.NO_TYPE] == 1
        assert conf["MONEY_LAUNDERING"]["MONEY_LAUNDERING"] == 1
        assert (
            sum(sum(row.values()) for row in conf.values()) == 4
        )  # a fraude não alertada e o legítimo ficam de fora

    def test_every_true_type_has_every_column(self) -> None:
        ds = _signalled([1], ["CARD_CLONING"], ["CARD_CLONING"], [[0.0] * len(SIGNALS)], [1])
        conf = ev.type_confusion(ds, ds.deployed_alert)
        assert set(conf) == set(ev.FRAUD_TYPES)
        assert all(set(row) == {*ev.FRAUD_TYPES, ev.NO_TYPE} for row in conf.values())

    def test_sum_over_seeds(self) -> None:
        a = {t: {p: 0 for p in (*ev.FRAUD_TYPES, ev.NO_TYPE)} for t in ev.FRAUD_TYPES}
        a["CARD_CLONING"]["CARD_CLONING"] = 2
        b = {t: {p: 0 for p in (*ev.FRAUD_TYPES, ev.NO_TYPE)} for t in ev.FRAUD_TYPES}
        b["CARD_CLONING"]["CARD_CLONING"] = 3
        b["CARD_CLONING"][ev.NO_TYPE] = 1
        total = ev._sum_confusions([a, b])
        assert total["CARD_CLONING"]["CARD_CLONING"] == 5 and total["CARD_CLONING"][ev.NO_TYPE] == 1


class TestSignalDiagnostics:
    def test_rate_of_each_signal_in_legit_fraud_and_by_type(self) -> None:
        j = SIGNALS.index("GEO_VELOCITY")
        values = np.zeros((6, len(SIGNALS)))
        values[[0, 1], j] = 1.0  # ativo em 2 das 3 fraudes
        values[5, j] = 0.4  # abaixo de 0,5: não conta como ativo
        ds = _signalled(
            y=[1, 1, 1, 0, 0, 0],
            fraud_type=["CARD_CLONING", "CARD_CLONING", "ACCOUNT_TAKEOVER", "", "", ""],
            predicted=[""] * 6,
            values=values,
            alert=[1, 1, 1, 0, 0, 0],
        )
        out = ev.signal_diagnostics([ds], {s: 0.5 for s in SIGNALS})["GEO_VELOCITY"]
        assert out["fraud"] == pytest.approx(2 / 3)
        assert out["legit"] == pytest.approx(0.0)
        assert out["by_type"]["CARD_CLONING"] == pytest.approx(1.0)
        assert out["by_type"]["ACCOUNT_TAKEOVER"] == pytest.approx(0.0)
        assert out["weight"] == 0.5

    def test_a_signal_that_lights_up_the_same_in_both_classes_does_not_discriminate(self) -> None:
        j = SIGNALS.index("TX_VELOCITY")
        values = np.zeros((4, len(SIGNALS)))
        values[:, j] = 1.0
        ds = _signalled(
            [1, 1, 0, 0], ["CARD_CLONING"] * 2 + [""] * 2, [""] * 4, values, [1, 1, 0, 0]
        )
        out = ev.signal_diagnostics([ds], {s: 0.0 for s in SIGNALS})["TX_VELOCITY"]
        assert out["legit"] == out["fraud"] == 1.0


class TestCompare:
    def _detector(self, name, f1s):
        return {
            "name": name,
            "per_seed": [
                {
                    c: {"precision": f, "recall": f, "f1": f}
                    for c in (ev.DEPLOYED, ev.condition_key(ev.PRIMARY_FPR))
                }
                for f in f1s
            ],
        }

    def test_paired_difference_and_wins(self) -> None:
        base = self._detector("a", [0.40, 0.50, 0.60])
        chal = self._detector("b", [0.50, 0.45, 0.90])  # +0,10, −0,05, +0,30
        out = ev.compare(base, chal)["conditions"][ev.DEPLOYED]
        assert out["f1"][0] == pytest.approx((0.10 - 0.05 + 0.30) / 3)
        assert out["wins"] == 2 and out["seeds"] == 3
        assert out["f1"][1] == pytest.approx(np.std([0.10, -0.05, 0.30], ddof=1))

    def test_names_and_both_conditions(self) -> None:
        out = ev.compare(self._detector("a", [0.1]), self._detector("b", [0.2]))
        assert (out["baseline"], out["challenger"]) == ("a", "b")
        assert set(out["conditions"]) == {ev.DEPLOYED, ev.condition_key(ev.PRIMARY_FPR)}


class TestSensitivity:
    def _datasets(self, seed):
        """Duas colunas: NEW_DESTINATION (a essencial, perfeita) e AMOUNT_ANOMALY (fraca)."""
        rng = np.random.default_rng(seed)
        n = 3000
        y = (rng.random(n) < 0.06).astype(int)
        values = np.zeros((n, len(SIGNALS)))
        values[:, SIGNALS.index("NEW_DESTINATION")] = np.where(y == 1, 1.0, (rng.random(n) < 0.02))
        values[:, SIGNALS.index("AMOUNT_ANOMALY")] = np.where(
            y == 1, rng.random(n) < 0.3, rng.random(n) < 0.05
        )
        return _signalled(
            list(y), ["CARD_CLONING" if v else "" for v in y], [""] * n, values, list(np.zeros(n))
        )

    def test_removing_the_essential_signal_lowers_recall(self) -> None:
        validation, tests = self._datasets(0), [self._datasets(1), self._datasets(2)]
        full = ev.summarize(
            tests[0], noisy_or_np(tests[0].signal_values, {s: 0.9 for s in SIGNALS}) > 0.85
        )
        result = ev.sensitivity_without(validation, tests, "NEW_DESTINATION")
        assert result["signal"] == "NEW_DESTINATION"
        assert result["recall"][0] < full["recall"]
        assert set(result["by_type"]) >= {"CARD_CLONING"}

    def test_the_masked_signal_is_never_used(self) -> None:
        """Mesmo que o dataset de teste tenha o sinal ligado, a variante recalibrada o ignora."""
        validation, tests = self._datasets(0), [self._datasets(1)]
        a = ev.sensitivity_without(validation, tests, "NEW_DESTINATION")
        tests[0].signal_values[:, SIGNALS.index("NEW_DESTINATION")] = 0.0
        b = ev.sensitivity_without(validation, tests, "NEW_DESTINATION")
        assert a["recall"] == b["recall"] and a["precision"] == b["precision"]


# ── Relatório ──────────────────────────────────────────────────────────────────


class TestRenderReport:
    @pytest.fixture(scope="class")
    def results(self) -> dict:
        return ev.run_protocol(None, CFG, [AmountDetector(), SignalDetector()])

    @pytest.fixture(scope="class")
    def text(self, results) -> str:
        return render_report(results)

    def test_has_every_section(self, text) -> None:
        for heading in (
            "## Resumo",
            "## Protocolo",
            "## Comparação entre detectores",
            "### Diferença pareada por seed",
            "## Detector `zscore-v1`",
            "## Detector `multisignal-v2`",
            "### Tipo inferido: matriz de confusão",
            "### Os sinais",
            "### Sensibilidade: o V2 sem `NEW_DESTINATION`",
            "## Cobertura por densidade de eventos",
            "## Hipótese do V1: Precision perto de 50%",
            "## Limitações e circularidade",
        ):
            assert heading in text, heading

    def test_shows_where_the_v2_still_errs(self, text) -> None:
        v2 = text[text.index("## Detector `multisignal-v2`") :]
        for part in (
            "Recall por tipo de fraude",
            "Recall por cenário",
            "hard negative",
            "Tipo verdadeiro",
        ):
            assert part in v2

    def test_confusion_matrix_has_the_five_types_and_the_none_column(self, text) -> None:
        matrix = text[text.index("### Tipo inferido") : text.index("### Os sinais")]
        for label in (
            "Account takeover",
            "Clone de cartão",
            "Roubo de identidade",
            "Lavagem",
            "Engenharia social",
            "nenhuma",
        ):
            assert label in matrix

    def test_signal_table_lists_every_signal(self, text) -> None:
        table = text[text.index("### Os sinais") : text.index("### Sensibilidade")]
        for name in SIGNALS:
            assert f"`{name}`" in table

    def test_declares_the_circularity_and_the_relative_gain(self, text) -> None:
        assert "Circularidade" in text and "relativo" in text
        assert "destinatário é a assinatura mais óbvia" in text
        assert "não** dá uma estimativa do Recall real" in text

    def test_no_timestamp_of_generation_and_no_absolute_path(self, text) -> None:
        assert "/Users/" not in text and "Gerado em" not in text

    def test_hypothesis_verdict_is_stated(self, text) -> None:
        assert "confirmada" in text or "refutada" in text

    def test_summary_mentions_the_paired_gain_and_the_sensitivity(self, text) -> None:
        summary = text[text.index("## Resumo") : text.index("## Protocolo")]
        assert "pareado por seed" in summary and "Circularidade dimensionada" in summary

    def test_v2_only_sections_disappear_without_the_v2(self) -> None:
        only_v1 = render_report(ev.run_protocol(None, CFG, [AmountDetector()]))
        assert "### Os sinais" not in only_v1 and "Sensibilidade" not in only_v1
        assert "Diferença pareada" not in only_v1
        assert "TX_VELOCITY" not in only_v1

    def test_batch_density_row_appears_when_measured(self, results) -> None:
        results = {**results, "batch_density": {
            "seed": 1, "events": 1000, "fraud": 25, "episodes": 10, "coverage": 0.004,
            "customers": 20, "events_per_customer": 50.0, "alerts": 2, "recall": 0.03, "precision": 0.5,
        }}  # fmt: skip
        text = render_report(results)
        assert "0,40%" in text
        assert "só enxerga fraude na densidade do streaming" in text

    def test_verdict_rule(self) -> None:
        from src.transformation.fraud.report import _hypothesis_verdict

        assert _hypothesis_verdict(0.51)[0] is True
        assert _hypothesis_verdict(0.60)[0] is True
        assert _hypothesis_verdict(0.098)[0] is False
        assert "refutada" in _hypothesis_verdict(0.098)[1]


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


# ── Histórico e ritmo diurno do replay ─────────────────────────────────────────


class TestSimulateHistory:
    CFG_H = ReplayConfig(
        n_customers=60,
        rate_tps=5.0,
        duration_minutes=10,
        warmup_minutes=5,
        history_transactions=2_000,
    )

    def test_same_customers_as_the_replay_and_deterministic(self) -> None:
        stream = simulate_stream(7, self.CFG_H)
        history = simulate_history(7, self.CFG_H)
        assert len(history) == 2_000
        assert {e["customer_id"] for e in history} <= {c["customer_id"] for c in stream.customers}
        assert [e["transaction_id"] for e in history] == [
            e["transaction_id"] for e in simulate_history(7, self.CFG_H)
        ]

    def test_history_ids_never_collide_with_the_replay(self) -> None:
        stream = simulate_stream(7, self.CFG_H)
        history = simulate_history(7, self.CFG_H)
        assert {e["transaction_id"] for e in history}.isdisjoint(
            {e["transaction_id"] for e in stream.events}
        )

    def test_history_ends_where_the_replay_starts(self) -> None:
        history = simulate_history(7, self.CFG_H)
        start = self.CFG_H.start
        stamps = [datetime.fromisoformat(e["timestamp"]) for e in history]
        assert max(stamps) <= start
        assert min(stamps) >= start - timedelta(days=self.CFG_H.history_days)

    def test_history_is_labelled_so_the_profile_can_drop_the_fraud(self) -> None:
        history = simulate_history(7, self.CFG_H)
        assert any(e["is_fraud"] for e in history) and any(not e["is_fraud"] for e in history)

    def test_different_seeds_give_different_customers(self) -> None:
        a = {e["customer_id"] for e in simulate_history(7, self.CFG_H)}
        b = {e["customer_id"] for e in simulate_history(8, self.CFG_H)}
        assert a.isdisjoint(b)

    def test_the_replay_carries_its_customers(self) -> None:
        stream = simulate_stream(7, self.CFG_H)
        assert len(stream.customers) == 60
        assert {"customer_id", "segment", "account_opening_date"} <= set(stream.customers[0])


class TestDiurnalReplay:
    def test_night_replay_emits_far_fewer_events_than_the_constant_one(self) -> None:
        night = ReplayConfig(
            n_customers=100, rate_tps=5.0, duration_minutes=10, warmup_minutes=2,
            start=datetime(2026, 3, 2, 7, 0, tzinfo=UTC),  # 04h locais: ninguém ativo
        )  # fmt: skip
        constant = simulate_stream(3, night)
        diurnal = simulate_stream(3, ReplayConfig(**{**night.__dict__, "diurnal": True}))
        assert len(diurnal.events) < 0.5 * len(constant.events)

    def test_the_default_is_the_constant_rate(self) -> None:
        assert ReplayConfig().diurnal is False


# ── CLI: opções novas ──────────────────────────────────────────────────────────


class TestNewCliOptions:
    def test_diurnal_and_history_size(self) -> None:
        cfg = ev.build_config(ev._parse_args(["--diurnal", "--history-transactions", "1234"]))
        assert cfg.replay.diurnal is True
        assert cfg.replay.history_transactions == 1234

    def test_both_detectors_by_default_and_the_v1_is_the_baseline(self) -> None:
        args = ev._parse_args([])
        assert args.detectors == ["zscore-v1", "multisignal-v2"]
        detectors = ev._make_detectors(args.detectors)
        assert [d.name for d in detectors] == ["zscore-v1", "multisignal-v2"]

    def test_can_pick_a_single_detector(self) -> None:
        assert [
            d.name
            for d in ev._make_detectors(ev._parse_args(["--detectors", "zscore-v1"]).detectors)
        ] == ["zscore-v1"]

    def test_unknown_detector_is_rejected(self) -> None:
        with pytest.raises(SystemExit):
            ev._parse_args(["--detectors", "nope"])


# ── Os dois detectores reais, de ponta a ponta (Spark local) ───────────────────


class TestRealDetectorsEndToEnd:
    @pytest.fixture(scope="class")
    def results(self, spark) -> dict:
        cfg = ev.EvalConfig(
            replay=ReplayConfig(
                n_customers=120,
                rate_tps=8.0,
                duration_minutes=45,
                warmup_minutes=15,
                history_transactions=10_000,
            ),
            validation_seed=200,
            test_seeds=(11, 12),
            include_batch_density=False,
        )
        return ev.run_protocol(spark, cfg, [ev.ZScoreV1(), MultiSignalV2()])

    def test_both_detectors_are_measured_on_the_same_events(self, results) -> None:
        assert [d["name"] for d in results["detectors"]] == ["zscore-v1", "multisignal-v2"]
        assert len(results["comparison"]) == 1

    def test_the_v2_has_a_profile_for_almost_every_customer(self, results) -> None:
        v1, v2 = results["detectors"]
        assert v2["coverage"][0] > 0.9

    def test_the_v2_ranks_fraud_better_than_the_v1(self, results) -> None:
        v1, v2 = results["detectors"]
        assert v2["ranking"]["pr_auc"][0] > v1["ranking"]["pr_auc"][0]

    def test_the_v2_reports_types_signals_and_sensitivity(self, results) -> None:
        v2 = results["detectors"][1]
        assert v2["type_confusion"] and v2["signals"] and v2["sensitivity"]
        assert (
            v2["signals"]["GEO_VELOCITY"]["legit"] < 0.01
        )  # sinal limpo: quase não acende no legítimo

    def test_the_report_renders_from_real_results(self, results) -> None:
        text = render_report(results)
        assert "multisignal-v2" in text and "Tipo verdadeiro" in text
