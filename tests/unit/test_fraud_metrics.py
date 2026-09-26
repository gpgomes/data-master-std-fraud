"""Testes das métricas de avaliação de detectores, com casos calculados à mão (issue #44)."""

import math

import numpy as np
import pytest

from src.transformation.fraud import metrics


class TestConfusionAndRates:
    # 3 fraudes (alertou 2) e 4 legítimos (alertou 1): tp=2 fn=1 fp=1 tn=3
    Y = [1, 1, 1, 0, 0, 0, 0]
    ALERT = [1, 1, 0, 1, 0, 0, 0]

    def test_confusion_matrix(self) -> None:
        assert metrics.confusion(self.Y, self.ALERT) == {"tp": 2, "fp": 1, "tn": 3, "fn": 1}

    def test_rates_by_hand(self) -> None:
        r = metrics.rates(metrics.confusion(self.Y, self.ALERT))
        assert r["precision"] == pytest.approx(2 / 3)
        assert r["recall"] == pytest.approx(2 / 3)
        assert r["f1"] == pytest.approx(2 / 3)
        assert r["fpr"] == pytest.approx(1 / 4)
        assert r["fnr"] == pytest.approx(1 / 3)

    def test_f1_is_the_harmonic_mean(self) -> None:
        # tp=1 fp=3 fn=1 tn=5: precision 1/4, recall 1/2, f1 = 2*(1/4)*(1/2)/(3/4) = 1/3
        r = metrics.rates({"tp": 1, "fp": 3, "tn": 5, "fn": 1})
        assert r["precision"] == pytest.approx(0.25)
        assert r["recall"] == pytest.approx(0.5)
        assert r["f1"] == pytest.approx(1 / 3)

    def test_no_alerts_means_precision_is_undefined_not_zero(self) -> None:
        r = metrics.rates(metrics.confusion([1, 0, 0], [0, 0, 0]))
        assert math.isnan(r["precision"])
        assert math.isnan(r["f1"])
        assert r["recall"] == 0.0
        assert r["fpr"] == 0.0

    def test_no_fraud_means_recall_is_undefined(self) -> None:
        r = metrics.rates(metrics.confusion([0, 0], [1, 0]))
        assert math.isnan(r["recall"])
        assert r["precision"] == 0.0
        assert r["fpr"] == pytest.approx(0.5)

    def test_zero_precision_and_recall_gives_zero_f1(self) -> None:
        assert metrics.rates({"tp": 0, "fp": 2, "tn": 0, "fn": 2})["f1"] == 0.0

    def test_alerts_per_1000(self) -> None:
        assert metrics.alerts_per_1000([1, 0, 0, 0] * 250) == pytest.approx(250.0)
        assert metrics.alerts_per_1000([0] * 500 + [1] * 5) == pytest.approx(5 / 505 * 1000)
        assert math.isnan(metrics.alerts_per_1000([]))


class TestAveragePrecision:
    def test_perfect_ranking_is_one(self) -> None:
        assert metrics.average_precision([1, 1, 0, 0], [0.9, 0.8, 0.2, 0.1]) == pytest.approx(1.0)

    def test_hand_computed_case(self) -> None:
        # ordem: 0.9(+) 0.8(-) 0.7(+) 0.6(-): precisão 1, 1/2, 2/3, 1/2 e recall 1/2, 1/2, 1, 1
        # AP = 1/2*1 + 0 + 1/2*(2/3) + 0 = 5/6
        ap = metrics.average_precision([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.6])
        assert ap == pytest.approx(5 / 6)

    def test_worst_ranking(self) -> None:
        # a única fraude fica por último entre 4: precisão 1/4 no recall 1
        assert metrics.average_precision([0, 0, 0, 1], [0.9, 0.8, 0.7, 0.1]) == pytest.approx(0.25)

    def test_all_ties_equal_the_prevalence(self) -> None:
        assert metrics.average_precision([1, 0, 0, 0], [0.5] * 4) == pytest.approx(0.25)

    def test_ties_enter_the_same_threshold(self) -> None:
        # (+) e (-) empatados no topo formam um único ponto: precisão 1/2, recall 1
        assert metrics.average_precision([1, 0, 0], [0.9, 0.9, 0.1]) == pytest.approx(0.5)

    def test_no_positives_is_undefined(self) -> None:
        assert math.isnan(metrics.average_precision([0, 0, 0], [0.1, 0.2, 0.3]))

    def test_accepts_numpy_arrays(self) -> None:
        ap = metrics.average_precision(np.array([1, 0, 1, 0]), np.array([0.9, 0.8, 0.7, 0.6]))
        assert ap == pytest.approx(5 / 6)


class TestThresholdAndRecallAtFpr:
    # 10 legítimos com score 0.1 … 1.0 e 3 fraudes com score 0.95, 0.85 e 0.3
    NEG = [round(0.1 * i, 1) for i in range(1, 11)]
    Y = [0] * 10 + [1, 1, 1]
    SCORE = NEG + [0.95, 0.85, 0.3]

    def test_threshold_tolerates_the_allowed_false_positives(self) -> None:
        # alvo 20%: até 2 dos 10 legítimos podem alertar (1.0 e 0.9), então alerta só acima de 0.8
        assert metrics.threshold_for_fpr(self.Y, self.SCORE, 0.2) == pytest.approx(0.8)

    def test_threshold_realizes_an_fpr_within_the_target(self) -> None:
        for target in (0.05, 0.1, 0.2, 0.5):
            t = metrics.threshold_for_fpr(self.Y, self.SCORE, target)
            fp = sum(1 for y, s in zip(self.Y, self.SCORE, strict=True) if y == 0 and s > t)
            assert fp / 10 <= target + 1e-9

    def test_zero_tolerance_alerts_on_nothing_legit(self) -> None:
        assert metrics.threshold_for_fpr(self.Y, self.SCORE, 0.0) == pytest.approx(1.0)

    def test_target_that_accepts_everyone_alerts_on_everything(self) -> None:
        assert metrics.threshold_for_fpr(self.Y, self.SCORE, 1.0) == -math.inf

    def test_ties_at_the_threshold_do_not_break_the_target(self) -> None:
        y, s = [0, 0, 0, 0, 1], [0.5, 0.5, 0.5, 0.1, 0.9]
        t = metrics.threshold_for_fpr(y, s, 0.25)  # tolera 1 de 4
        assert t == pytest.approx(0.5)
        assert sum(1 for yy, ss in zip(y, s, strict=True) if yy == 0 and ss > t) == 0

    def test_recall_at_fpr_by_hand(self) -> None:
        # limiar 0.8: das 3 fraudes, 0.95 e 0.85 passam e 0.3 não
        assert metrics.recall_at_fpr(self.Y, self.SCORE, 0.2) == pytest.approx(2 / 3)

    def test_recall_at_fpr_without_fraud_is_undefined(self) -> None:
        assert math.isnan(metrics.recall_at_fpr([0, 0], [0.1, 0.2], 0.1))


class TestGroupRates:
    def test_alert_rate(self) -> None:
        alert = np.array([1, 0, 1, 1], dtype=bool)
        assert metrics.alert_rate(alert, np.array([1, 1, 0, 0], dtype=bool)) == (2, 0.5)
        n, rate = metrics.alert_rate(alert, np.zeros(4, dtype=bool))
        assert n == 0 and math.isnan(rate)

    def test_group_alert_rates_on_the_masked_rows(self) -> None:
        alert = [1, 0, 1, 1, 0, 1]
        labels = ["A", "A", "B", "B", "B", "A"]
        mask = [1, 1, 1, 1, 1, 0]  # a última linha fica de fora
        assert metrics.group_alert_rates(alert, labels, mask) == {
            "A": (2, 0.5),
            "B": (3, pytest.approx(2 / 3)),
        }


class TestEpisodeDetection:
    def test_time_to_detect_by_hand(self) -> None:
        # A: 1º evento em 100, 1º alerta em 130 (ttd 30). B: nunca alerta. C: alerta no 1º (ttd 0)
        result = metrics.episode_detection(
            ["A", "A", "A", "B", "B", "C"],
            [100, 130, 160, 200, 240, 500],
            [0, 1, 1, 0, 0, 1],
        )
        assert result["episodes"] == 3
        assert result["detected"] == 2
        assert result["recall"] == pytest.approx(2 / 3)
        assert result["ttd_median"] == pytest.approx(15.0)  # mediana de {30, 0}
        assert result["ttd_p90"] == pytest.approx(27.0)  # percentil 90 de {0, 30}

    def test_ttd_is_measured_from_the_first_fraud_event_not_the_first_row(self) -> None:
        result = metrics.episode_detection(["A", "A"], [200, 100], [1, 0])  # fora de ordem
        assert result["ttd_median"] == pytest.approx(100.0)

    def test_no_episode_detected(self) -> None:
        result = metrics.episode_detection(["A", "B"], [1, 2], [0, 0])
        assert result["recall"] == 0.0
        assert math.isnan(result["ttd_median"])
        assert math.isnan(result["ttd_p90"])

    def test_no_episodes(self) -> None:
        result = metrics.episode_detection([], [], [])
        assert result["episodes"] == 0
        assert math.isnan(result["recall"])


class TestMeanSd:
    def test_sample_standard_deviation(self) -> None:
        mean, sd = metrics.mean_sd([1.0, 2.0, 3.0])
        assert mean == pytest.approx(2.0)
        assert sd == pytest.approx(1.0)  # n − 1

    def test_ignores_nan(self) -> None:
        assert metrics.mean_sd([1.0, math.nan, 3.0]) == (
            pytest.approx(2.0),
            pytest.approx(math.sqrt(2)),
        )

    def test_single_value_has_zero_deviation(self) -> None:
        assert metrics.mean_sd([0.4]) == (0.4, 0.0)

    def test_nothing_left_is_undefined(self) -> None:
        mean, sd = metrics.mean_sd([math.nan])
        assert math.isnan(mean) and math.isnan(sd)
