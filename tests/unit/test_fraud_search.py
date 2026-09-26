"""Testes da busca dos pesos (numpy) e da geração de `weights.py` (issue #45)."""

from __future__ import annotations

import math

import numpy as np
import pytest

from src.transformation.fraud import calibrate as cal
from src.transformation.fraud import search
from src.transformation.fraud.scoring import noisy_or_np
from src.transformation.fraud.signals import SIGNALS


def _toy(n=4000, seed=0):
    """Três colunas: 0 = sinal perfeito (só na fraude), 1 = ruído, 2 = sinal fraco e ruidoso."""
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.05).astype(int)
    perfect = y.astype(float)
    noise = (rng.random(n) < 0.3).astype(float)
    weak = np.where(y == 1, rng.random(n) < 0.5, rng.random(n) < 0.1).astype(float)
    return np.column_stack([perfect, noise, weak]), y


class TestObjectiveAndComparison:
    def test_objective_is_recall_at_fpr_and_pr_auc(self) -> None:
        matrix, y = _toy()
        recall, ap = search.objective(matrix, y, np.array([0.9, 0.0, 0.0]))
        assert recall == pytest.approx(
            1.0
        )  # o sinal perfeito pega toda a fraude sem falso positivo
        assert ap == pytest.approx(1.0)

    def test_better_prefers_recall_then_pr_auc(self) -> None:
        assert search._better((0.9, 0.1), (0.8, 0.9))
        assert search._better((0.8, 0.5), (0.8, 0.4))
        assert not search._better((0.8, 0.4), (0.8, 0.4))
        assert not search._better((0.7, 0.99), (0.8, 0.1))

    def test_better_ignores_differences_below_the_tolerance(self) -> None:
        assert not search._better((0.8 + 1e-12, 0.4), (0.8, 0.4))


class TestCoordinateAscent:
    def test_finds_the_perfect_signal_and_drops_the_noise(self) -> None:
        matrix, y = _toy()
        weights, best, start = search.coordinate_ascent(matrix, y)
        assert best[0] == pytest.approx(1.0)
        assert best[0] >= start[0]
        assert 0.0 < weights[0] <= 0.6  # o sinal perfeito fica com o menor peso que basta
        assert weights[1] == 0.0  # o ruído é podado
        assert weights[2] == 0.0  # o sinal fraco não acrescenta nada ao perfeito

    def test_never_ends_worse_than_the_start(self) -> None:
        rng = np.random.default_rng(5)
        matrix = (rng.random((3000, 4)) < 0.3).astype(float)
        y = (rng.random(3000) < 0.05).astype(int)
        _, best, start = search.coordinate_ascent(matrix, y)
        assert best == start or search._better(best, start)

    def test_weights_stay_on_the_grid(self) -> None:
        matrix, y = _toy()
        weights, _, _ = search.coordinate_ascent(matrix, y)
        assert all(float(w) in search.GRID for w in weights)

    def test_is_deterministic(self) -> None:
        matrix, y = _toy()
        a = search.coordinate_ascent(matrix, y)
        b = search.coordinate_ascent(matrix, y)
        assert (a[0] == b[0]).all() and a[1] == b[1]

    def test_target_fpr_changes_the_objective(self) -> None:
        matrix, y = _toy()
        tight = search.objective(matrix, y, np.array([0.0, 0.5, 0.5]), target_fpr=0.001)
        loose = search.objective(matrix, y, np.array([0.0, 0.5, 0.5]), target_fpr=0.5)
        assert loose[0] >= tight[0]

    def test_prunes_a_redundant_signal_to_the_lowest_weight_that_keeps_the_objective(self) -> None:
        matrix, y = _toy()
        both = np.column_stack([matrix[:, 0], matrix[:, 0]])  # duas cópias do mesmo sinal
        weights, best, _ = search.coordinate_ascent(both, y)
        assert best[0] == pytest.approx(1.0)
        assert sorted(weights)[0] == 0.0  # uma das cópias sai com peso zero


# ── weights.py gerado ──────────────────────────────────────────────────────────


def _calibration(**overrides) -> cal.Calibration:
    base = {
        "weights": {name: round(0.05 * (i + 1), 2) for i, name in enumerate(SIGNALS)},
        "threshold": 0.9123,
        "objective": (0.9441, 0.9397),
        "initial_objective": (0.7605, 0.7632),
        "seed": 1000,
        "events": 109613,
        "fraud": 2751,
    }
    base.update(overrides)
    return cal.Calibration(**base)


class TestRenderWeightsModule:
    def test_generated_module_is_valid_python_with_the_same_values(self) -> None:
        c = _calibration()
        namespace: dict = {}
        exec(compile(cal.render_weights_module(c), "weights.py", "exec"), namespace)  # noqa: S102
        assert namespace["SIGNAL_WEIGHTS"] == c.weights
        assert namespace["ALERT_THRESHOLD"] == c.threshold
        assert namespace["WEIGHTS_VERSION"] == "seed-1000"

    def test_output_is_deterministic_and_has_no_timestamp(self) -> None:
        c = _calibration()
        text = cal.render_weights_module(c)
        assert text == cal.render_weights_module(c)
        assert "202" not in text.split('"""')[2]  # nenhum ano/data fora do docstring de cálculo

    def test_lists_every_signal_in_the_canonical_order(self) -> None:
        text = cal.render_weights_module(_calibration())
        positions = [text.index(f'"{name}"') for name in SIGNALS]
        assert positions == sorted(positions)

    def test_records_what_was_optimised(self) -> None:
        text = cal.render_weights_module(_calibration())
        assert "0.9441" in text and "seed 1000" in text
        assert "não edite à mão" in text.lower() or "Não edite à mão" in text

    def test_the_committed_weights_file_matches_the_generator_format(self) -> None:
        """O `weights.py` versionado foi gerado por `render_weights_module`, não escrito à mão."""
        from src.transformation.fraud import weights as wt

        c = cal.Calibration(
            weights=dict(wt.SIGNAL_WEIGHTS),
            threshold=wt.ALERT_THRESHOLD,
            objective=(0.0, 0.0),
            initial_objective=(0.0, 0.0),
            seed=1000,
            events=0,
            fraud=0,
        )
        rendered = cal.render_weights_module(c)
        committed = cal.DEFAULT_OUTPUT.read_text(encoding="utf-8")
        # mesma estrutura de código depois do docstring (que traz os números da calibração)
        assert rendered.split('"""')[2] == committed.split('"""')[2]


class TestCalibrateCli:
    def test_defaults_are_the_documented_protocol(self) -> None:
        args = cal._parse_args([])
        assert args.validation_seed == 1000
        assert args.customers == 1000 and args.rate_tps == 10.0
        assert args.dry_run is False
        assert args.output == cal.DEFAULT_OUTPUT

    def test_noisy_or_weights_from_the_search_reproduce_the_objective(self) -> None:
        matrix, y = _toy()
        weights, best, _ = search.coordinate_ascent(matrix, y)
        assert search.objective(matrix, y, weights) == best
        assert math.isfinite(noisy_or_np(matrix, weights).sum())
