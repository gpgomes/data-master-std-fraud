"""Busca dos pesos do score noisy-OR (issues #45).

Numpy puro, sem Spark: recebe a matriz de sinais (eventos × sinais) e os rótulos, e devolve os pesos
que maximizam a regra de operação da série: **máximo recall com FPR ≤ alvo**, com a PR-AUC como
desempate. Busca por coordenadas: para cada sinal, testa os valores de `GRID` mantendo os outros
fixos, e repete até nenhum peso melhorar. Depois tenta reduzir cada peso ao menor valor que mantém o
objetivo (um sinal que não ajuda fica com peso zero).

Usada pela calibração (`calibrate.py`, que grava `weights.py`) e pela análise de sensibilidade do
relatório (`evaluate.py`), que recalibra o detector sem um sinal.
"""

from __future__ import annotations

import numpy as np

from src.transformation.fraud import metrics
from src.transformation.fraud.scoring import noisy_or_np

DEFAULT_TARGET_FPR = 0.01

GRID = (0.0, 0.05, 0.1, 0.2, 0.3, 0.45, 0.6, 0.75, 0.9, 0.97)
INITIAL_WEIGHT = 0.3
MAX_PASSES = 6
TOLERANCE = 1e-9

Objective = tuple[float, float]  # (recall com FPR ≤ alvo, PR-AUC)


def objective(
    matrix: np.ndarray, y: np.ndarray, weights: np.ndarray, target_fpr: float = DEFAULT_TARGET_FPR
) -> Objective:
    score = noisy_or_np(matrix, weights)
    return metrics.recall_at_fpr(y, score, target_fpr), metrics.average_precision(y, score)


def _better(new: Objective, old: Objective) -> bool:
    if new[0] > old[0] + TOLERANCE:
        return True
    return abs(new[0] - old[0]) <= TOLERANCE and new[1] > old[1] + TOLERANCE


def coordinate_ascent(
    matrix: np.ndarray,
    y: np.ndarray,
    target_fpr: float = DEFAULT_TARGET_FPR,
    grid: tuple[float, ...] = GRID,
    max_passes: int = MAX_PASSES,
    initial: float = INITIAL_WEIGHT,
) -> tuple[np.ndarray, Objective, Objective]:
    """Pesos que maximizam `objective`. Devolve (pesos, objetivo final, objetivo inicial)."""
    weights = np.full(matrix.shape[1], initial, dtype=float)
    start = best = objective(matrix, y, weights, target_fpr)
    for _ in range(max_passes):
        improved = False
        for j in range(len(weights)):
            for value in grid:
                if value == weights[j]:
                    continue
                candidate = weights.copy()
                candidate[j] = value
                score = objective(matrix, y, candidate, target_fpr)
                if _better(score, best):
                    weights, best, improved = candidate, score, True
        if not improved:
            break

    # Poda: o menor peso que mantém o objetivo. Um sinal que não ajuda sai com zero. Repete até
    # estabilizar: podar o ruído de um sinal libera a poda dos que vieram antes dele.
    for _ in range(max_passes):
        pruned = False
        for j in range(len(weights)):
            for value in sorted(grid):
                if value >= weights[j]:
                    break
                candidate = weights.copy()
                candidate[j] = value
                score = objective(matrix, y, candidate, target_fpr)
                if not _better(best, score):  # não piorou
                    weights, best, pruned = candidate, score, True
                    break
        if not pruned:
            break
    return weights, best, start
