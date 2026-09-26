"""Métricas de avaliação de detectores de fraude, em numpy puro (issue #44).

Convenções: `y_true` é 1 para fraude e 0 para legítimo; `score` maior significa mais arriscado; e
um detector "alerta" quando `score > threshold` (estrito). Sem dependência de Spark nem de sklearn:
tudo aqui é testável com casos calculados à mão.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np

NAN = float("nan")


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else NAN


def confusion(
    y_true: Sequence[int] | np.ndarray, alert: Sequence[bool] | np.ndarray
) -> dict[str, int]:
    """Matriz de confusão: verdadeiros/falsos positivos e negativos."""
    y = np.asarray(y_true).astype(bool)
    a = np.asarray(alert).astype(bool)
    return {
        "tp": int(np.sum(y & a)),
        "fp": int(np.sum(~y & a)),
        "tn": int(np.sum(~y & ~a)),
        "fn": int(np.sum(y & ~a)),
    }


def rates(conf: dict[str, int]) -> dict[str, float]:
    """Precision, Recall, F1, FPR e FNR a partir da matriz de confusão.

    Razão com denominador zero (por exemplo Precision sem nenhum alerta) vira NaN, não zero:
    "não definido" não é o mesmo que "péssimo".
    """
    tp, fp, tn, fn = conf["tp"], conf["fp"], conf["tn"], conf["fn"]
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    if math.isnan(precision) or math.isnan(recall):
        f1 = NAN
    elif precision + recall == 0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "fpr": _ratio(fp, fp + tn),
        "fnr": _ratio(fn, tp + fn),
    }


def alerts_per_1000(alert: Sequence[bool] | np.ndarray) -> float:
    """Carga operacional: alertas a cada 1.000 transações."""
    a = np.asarray(alert).astype(bool)
    return float(a.sum() / len(a) * 1000) if len(a) else NAN


def average_precision(
    y_true: Sequence[int] | np.ndarray, score: Sequence[float] | np.ndarray
) -> float:
    """Área sob a curva Precision × Recall (média ponderada das precisões, como o sklearn).

    Empates de score entram juntos no mesmo limiar. Sem nenhuma fraude devolve NaN.
    """
    y = np.asarray(y_true).astype(int)
    s = np.asarray(score, dtype=float)
    if y.sum() == 0:
        return NAN
    order = np.argsort(-s, kind="stable")
    y, s = y[order], s[order]
    tps = np.cumsum(y)
    fps = np.cumsum(1 - y)
    # último índice de cada valor distinto de score: um ponto da curva por limiar
    distinct = np.r_[np.where(np.diff(s))[0], len(s) - 1]
    tps, fps = tps[distinct], fps[distinct]
    precision = tps / (tps + fps)
    recall = tps / tps[-1]
    return float(np.sum(np.diff(np.r_[0.0, recall]) * precision))


def threshold_for_fpr(
    y_true: Sequence[int] | np.ndarray, score: Sequence[float] | np.ndarray, target_fpr: float
) -> float:
    """Menor limiar (alerta se `score > limiar`) cujo FPR não passa de `target_fpr`.

    Com `n` legítimos, tolera até `floor(target_fpr * n)` falsos positivos. Sem legítimos, ou com
    alvo que aceita todos, devolve -inf (alerta em tudo).
    """
    y = np.asarray(y_true).astype(bool)
    s = np.asarray(score, dtype=float)
    negatives = np.sort(s[~y])[::-1]
    allowed = int(math.floor(target_fpr * len(negatives)))
    if allowed >= len(negatives):
        return -math.inf
    return float(negatives[allowed])


def recall_at_fpr(
    y_true: Sequence[int] | np.ndarray, score: Sequence[float] | np.ndarray, target_fpr: float
) -> float:
    """Maior Recall possível com FPR ≤ `target_fpr` no próprio conjunto (qualidade do ranking)."""
    y = np.asarray(y_true).astype(bool)
    s = np.asarray(score, dtype=float)
    if not y.any():
        return NAN
    threshold = threshold_for_fpr(y, s, target_fpr)
    return float(np.mean(s[y] > threshold))


def alert_rate(alert: np.ndarray, mask: np.ndarray) -> tuple[int, float]:
    """(n, fração alertada) das linhas de `mask`; a fração é NaN se `mask` está vazia."""
    mask = np.asarray(mask).astype(bool)
    n = int(mask.sum())
    return n, (float(np.asarray(alert).astype(bool)[mask].mean()) if n else NAN)


def group_alert_rates(
    alert: Sequence[bool] | np.ndarray,
    labels: Sequence[str] | np.ndarray,
    mask: Sequence[bool] | np.ndarray,
) -> dict[str, tuple[int, float]]:
    """Nas linhas de `mask`, agrupa por `labels` e devolve {grupo: (n, fração alertada)}.

    Com `mask` = fraudes é o Recall por grupo; com `mask` = legítimos é o FPR por grupo.
    """
    a = np.asarray(alert).astype(bool)
    lab = np.asarray(labels)
    m = np.asarray(mask).astype(bool)
    return {group: alert_rate(a, m & (lab == group)) for group in sorted(set(lab[m].tolist()))}


def episode_detection(
    episode_ids: Sequence[str] | np.ndarray,
    epochs: Sequence[float] | np.ndarray,
    alert: Sequence[bool] | np.ndarray,
) -> dict[str, float]:
    """Detecção por episódio de fraude e *time-to-detect*.

    Os argumentos são alinhados e cobrem só os eventos fraudulentos. Um episódio é detectado se
    algum dos seus eventos alertou; o *time-to-detect* é o intervalo, em segundos, entre o primeiro
    evento fraudulento e o primeiro evento alertado. Mediana e p90 consideram só os detectados.
    """
    ids = np.asarray(episode_ids)
    t = np.asarray(epochs, dtype=float)
    a = np.asarray(alert).astype(bool)
    ttds: list[float] = []
    episodes = detected = 0
    for episode in sorted(set(ids.tolist())):
        sel = ids == episode
        episodes += 1
        alerted = t[sel & a]
        if len(alerted):
            detected += 1
            ttds.append(float(alerted.min() - t[sel].min()))
    return {
        "episodes": float(episodes),
        "detected": float(detected),
        "recall": _ratio(detected, episodes),
        "ttd_median": float(np.median(ttds)) if ttds else NAN,
        "ttd_p90": float(np.percentile(ttds, 90)) if ttds else NAN,
    }


def mean_sd(values: Sequence[float]) -> tuple[float, float]:
    """Média e desvio padrão amostral (n − 1) ignorando NaN; (NaN, NaN) se não sobrou nada."""
    arr = np.asarray([v for v in values if not math.isnan(v)], dtype=float)
    if len(arr) == 0:
        return NAN, NAN
    return float(arr.mean()), float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
