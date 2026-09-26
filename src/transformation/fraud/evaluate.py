"""Avaliação dos detectores de fraude: Precision, Recall, FPR e onde eles erram (issues #44 e #45).

Mede o detector de streaming atual (`zscore-v1`, o `_enrich_and_score` de produção) e o Fraud Engine
multi-signal (`multisignal-v2`) contra o ground truth do gerador sintético, nos **mesmos datasets**, e
grava `docs/fraud_evaluation.md`. O V1 é a "régua" registrada na #44; o V2 tem de superá-la, e a
comparação é pareada por seed.

Protocolo (o que garante que o número não é ajustado no conjunto que o reporta):
  - **replay de stream** (`replay.py`): o `TransactionStream` do producer em tempo simulado, com a
    densidade em que o detector roda de verdade. Os eventos até o corte (`--profile-until`, ou
    `start + --warmup-minutes`) só alimentam o estado do detector; a avaliação usa o que vem depois;
  - **1 seed de validação e N seeds de teste**, todas com clientes diferentes. O limiar da regra de
    operação (*recall máximo com FPR ≤ alvo*) é calibrado na validação e aplicado nos testes;
  - resultados como média ± desvio padrão sobre as seeds de teste;
  - o V1 é reportado no limiar em que roda (|z| > 3) **e** nessa regra de operação, para a
    comparação com o V2 ser justa.

Execução (requer Java para o Spark local; ver `make fraud-eval`):
    python -m src.transformation.fraud.evaluate
    python -m src.transformation.fraud.evaluate --test-seeds 1 2 --duration-minutes 90
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.common.schemas import FraudType
from src.common.spark_session import create_spark_session
from src.transformation.fraud import metrics
from src.transformation.fraud.detector_api import Detector, DetectorOutput
from src.transformation.fraud.multisignal import MultiSignalV2
from src.transformation.fraud.replay import (
    ReplayConfig,
    ReplayDataset,
    simulate_batch,
    simulate_history,
    simulate_stream,
)
from src.transformation.fraud.scoring import SIGNAL_ACTIVE, noisy_or_np
from src.transformation.fraud.search import coordinate_ascent
from src.transformation.fraud.signals import NEW_DESTINATION, SIGNALS
from src.transformation.streaming.stream_processor import (
    DETECTOR_VERSION,
    Z_SCORE_MIN_TRANSACTIONS,
    Z_SCORE_THRESHOLD,
    Z_SCORE_WINDOW_SECONDS,
    StreamProcessor,
)

FRAUD_TYPES = tuple(t.value for t in FraudType)
HARD_NEGATIVES = ("new_device", "new_ip", "travel", "big_purchase", "off_hours")
NO_HARD_NEGATIVE = "nenhum"
OPERATING_FPRS = (0.005, 0.01, 0.02)
PRIMARY_FPR = 0.01
DEPLOYED = "deployed"
NO_TYPE = "nenhuma"  # nenhuma regra de tipo casou
# Sinal cujo peso é reavaliado sem ele (sensibilidade): o gerador o injeta em 100% da fraude.
SENSITIVITY_SIGNAL = NEW_DESTINATION
HYPOTHESIS_PRECISION = 0.50  # hipótese da issue #44: a Precision do V1 fica perto de 50%
HYPOTHESIS_TOLERANCE = 0.10
ANALYTIC_FPR_RANGE = (0.016, 0.046)  # FPR estimado na issue #44 (população; baseline amostral)


def condition_key(target_fpr: float) -> str:
    return f"fpr_{target_fpr:g}"


# ── Detectores ─────────────────────────────────────────────────────────────────


_ZSCORE_INPUT = ("transaction_id", "customer_id", "timestamp", "amount")
_ZSCORE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


class ZScoreV1:
    """O detector de streaming atual, chamado pela mesma função pura que o roda em produção.

    O dataset inteiro entra como `current` e o histórico persistido começa vazio: a janela de 1 h
    por cliente é montada pelo próprio `_enrich_and_score`, sobre os eventos em ordem de tempo.
    O detector não recebe o rótulo (só id, cliente, instante e valor). O score de ranking é |z|,
    e um evento sem baseline (menos de `Z_SCORE_MIN_TRANSACTIONS` na janela) fica com score 0.
    """

    name = DETECTOR_VERSION
    needs_history = False  # a janela curta se forma nos próprios eventos
    infers_type = False  # o tipo do alerta do V1 vem do rótulo, não é previsão

    def score(self, spark: SparkSession, dataset: ReplayDataset) -> dict[str, DetectorOutput]:
        return self.score_events(spark, dataset.events)

    def score_events(
        self, spark: SparkSession, events: list[dict[str, Any]]
    ) -> dict[str, DetectorOutput]:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "events.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                for event in events:
                    f.write(json.dumps({k: event[k] for k in _ZSCORE_INPUT}) + "\n")
            # Via arquivo, não `createDataFrame`: o caminho de RDD local quebra no Python 3.14.
            df = spark.read.schema(_ZSCORE_SCHEMA).json(path)
            processor = StreamProcessor(spark)
            scored = processor._enrich_and_score(df, processor._empty_history_df())
            rows = scored.select("transaction_id", "z_score", "is_anomaly").collect()
        return {
            r["transaction_id"]: DetectorOutput(
                score=abs(r["z_score"]) if r["z_score"] is not None else 0.0,
                alert=bool(r["is_anomaly"]),
                covered=r["z_score"] is not None,
            )
            for r in rows
        }


# ── Dataset avaliado ───────────────────────────────────────────────────────────


@dataclass
class ScoredDataset:
    """Eventos avaliados (depois do corte) alinhados por posição com a saída do detector."""

    seed: int
    y: np.ndarray  # 1 = fraude
    score: np.ndarray
    deployed_alert: np.ndarray
    covered: np.ndarray
    fraud_type: np.ndarray  # "" nos legítimos
    stealth: np.ndarray
    episode_id: np.ndarray  # "" nos legítimos
    epoch: np.ndarray
    hard_negative: dict[str, np.ndarray] = field(default_factory=dict)  # tipo → máscara
    predicted_type: np.ndarray = field(default_factory=lambda: np.array([], dtype=str))
    # (eventos × sinais) na ordem de `SIGNALS`; só os detectores com sinais preenchem.
    signal_values: np.ndarray | None = None


def score_dataset(dataset: ReplayDataset, outputs: dict[str, DetectorOutput]) -> ScoredDataset:
    """Junta a saída do detector com o ground truth, só para os eventos depois do corte."""
    cutoff = dataset.cutoff.isoformat()
    rows = [e for e in dataset.events if e["timestamp"] >= cutoff]

    y, score, alert, covered = [], [], [], []
    fraud_type, stealth, episode, epoch, hn_text = [], [], [], [], []
    predicted, values = [], []
    for event in rows:
        out = outputs[event["transaction_id"]]
        truth = dataset.truth.get(event["transaction_id"], {})
        is_fraud = bool(event["is_fraud"])
        y.append(int(is_fraud))
        score.append(out.score)
        alert.append(out.alert)
        covered.append(out.covered)
        fraud_type.append(event["fraud_type"] or "")
        stealth.append(bool(truth.get("stealth")) if is_fraud else False)
        episode.append(str(truth.get("episode_id", "")) if is_fraud else "")
        epoch.append(datetime.fromisoformat(event["timestamp"]).timestamp())
        hn_text.append("" if is_fraud else str(truth.get("hard_negative", "")))
        predicted.append(out.predicted_type or "")
        values.append(out.values)

    hard_negative = {
        kind: np.array([kind in text.split(";") for text in hn_text], dtype=bool)
        for kind in HARD_NEGATIVES
    }
    hard_negative[NO_HARD_NEGATIVE] = np.array([text == "" for text in hn_text], dtype=bool)
    return ScoredDataset(
        seed=dataset.seed,
        y=np.array(y, dtype=np.int8),
        score=np.array(score, dtype=float),
        deployed_alert=np.array(alert, dtype=bool),
        covered=np.array(covered, dtype=bool),
        fraud_type=np.array(fraud_type, dtype=str),
        stealth=np.array(stealth, dtype=bool),
        episode_id=np.array(episode, dtype=str),
        epoch=np.array(epoch, dtype=float),
        hard_negative=hard_negative,
        predicted_type=np.array(predicted, dtype=str),
        signal_values=np.array(values, dtype=float) if values and values[0] else None,
    )


def summarize(ds: ScoredDataset, alert: np.ndarray) -> dict[str, Any]:
    """Todas as métricas de uma decisão de alerta (`alert`) sobre um dataset."""
    fraud = ds.y == 1
    legit = ~fraud
    conf = metrics.confusion(ds.y, alert)
    summary: dict[str, Any] = {
        **conf,
        **metrics.rates(conf),
        "alerts_per_1000": metrics.alerts_per_1000(alert),
    }
    summary["by_type"] = metrics.group_alert_rates(alert, ds.fraud_type, fraud)
    scenario = np.array(
        [
            f"{t}|{'stealth' if s else 'normal'}"
            for t, s in zip(ds.fraud_type, ds.stealth, strict=True)
        ]
    )
    summary["by_scenario"] = metrics.group_alert_rates(alert, scenario, fraud)
    summary["by_hard_negative"] = {
        kind: metrics.alert_rate(alert, legit & mask) for kind, mask in ds.hard_negative.items()
    }
    summary["episodes"] = metrics.episode_detection(
        ds.episode_id[fraud], ds.epoch[fraud], alert[fraud]
    )
    return summary


def type_confusion(ds: ScoredDataset, alert: np.ndarray) -> dict[str, dict[str, int]]:
    """Entre as fraudes alertadas, quantas de cada tipo verdadeiro receberam cada tipo previsto.

    Só o V2 infere tipo; quando nenhuma regra casa o tipo previsto é `NO_TYPE`.
    """
    counts = {t: {p: 0 for p in (*FRAUD_TYPES, NO_TYPE)} for t in FRAUD_TYPES}
    selected = (ds.y == 1) & alert
    for true, pred in zip(ds.fraud_type[selected], ds.predicted_type[selected], strict=True):
        counts[str(true)][str(pred) or NO_TYPE] += 1
    return counts


def signal_diagnostics(
    scored: list[ScoredDataset], weights: dict[str, float]
) -> dict[str, dict[str, Any]]:
    """Por sinal: o peso, e com que frequência ele está ativo no legítimo, na fraude e por tipo.

    Um sinal útil acende muito mais na fraude do que no legítimo; um sinal que acende igual nos
    dois (como o `TX_VELOCITY` no ritmo sintético de 36 eventos/h por cliente) não discrimina.
    """
    out: dict[str, dict[str, Any]] = {}
    for j, name in enumerate(SIGNALS):
        legit, fraud = [], []
        by_type: dict[str, list[float]] = {t: [] for t in FRAUD_TYPES}
        for ds in scored:
            if ds.signal_values is None:
                continue
            on = ds.signal_values[:, j] >= SIGNAL_ACTIVE
            legit.append(float(on[ds.y == 0].mean()))
            fraud.append(float(on[ds.y == 1].mean()))
            for t in FRAUD_TYPES:
                mask = ds.fraud_type == t
                if mask.any():
                    by_type[t].append(float(on[mask].mean()))
        out[name] = {
            "weight": weights.get(name),
            "legit": metrics.mean_sd(legit)[0],
            "fraud": metrics.mean_sd(fraud)[0],
            "by_type": {t: metrics.mean_sd(v)[0] for t, v in by_type.items()},
        }
    return out


def sensitivity_without(
    validation: ScoredDataset, tests: list[ScoredDataset], signal: str
) -> dict[str, Any]:
    """O detector sem `signal`, recalibrado na validação e medido nas seeds de teste.

    Responde "quanto o V2 depende deste sinal?" no mesmo protocolo: zera a coluna do sinal, refaz a
    busca de pesos e do limiar só na validação, e aplica nas seeds de teste. É a forma honesta de
    dimensionar a circularidade quando o gerador injeta um sinal em toda a fraude.
    """
    assert validation.signal_values is not None
    j = SIGNALS.index(signal)

    def masked(matrix: np.ndarray) -> np.ndarray:
        out = matrix.copy()
        out[:, j] = 0.0
        return out

    weights, _, _ = coordinate_ascent(masked(validation.signal_values), validation.y, PRIMARY_FPR)
    threshold = metrics.threshold_for_fpr(
        validation.y, noisy_or_np(masked(validation.signal_values), weights), PRIMARY_FPR
    )
    summaries = []
    for ds in tests:
        assert ds.signal_values is not None
        score = noisy_or_np(masked(ds.signal_values), weights)
        summaries.append(summarize(ds, score > threshold))
    agg = aggregate(summaries)
    return {
        "signal": signal,
        "precision": agg["precision"],
        "recall": agg["recall"],
        "f1": agg["f1"],
        "fpr": agg["fpr"],
        "by_type": agg["by_type"],
    }


_SCALARS = ("precision", "recall", "f1", "fpr", "fnr", "alerts_per_1000", "tp", "fp", "tn", "fn")
_EPISODE_KEYS = ("episodes", "detected", "recall", "ttd_median", "ttd_p90")


def aggregate(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """Média ± desvio das métricas escalares e por grupo, sobre as seeds de teste."""
    out: dict[str, Any] = {k: metrics.mean_sd([s[k] for s in summaries]) for k in _SCALARS}
    for group in ("by_type", "by_scenario", "by_hard_negative"):
        names = sorted({g for s in summaries for g in s[group]})
        out[group] = {
            name: {
                "n": sum(s[group][name][0] for s in summaries if name in s[group]),
                "value": metrics.mean_sd(
                    [s[group][name][1] for s in summaries if name in s[group]]
                ),
            }
            for name in names
        }
    out["episodes"] = {
        k: metrics.mean_sd([s["episodes"][k] for s in summaries]) for k in _EPISODE_KEYS
    }
    return out


# ── Protocolo ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class EvalConfig:
    replay: ReplayConfig = field(default_factory=ReplayConfig)
    validation_seed: int = 1000
    test_seeds: tuple[int, ...] = (1, 2, 3, 4, 5)
    include_batch_density: bool = True
    batch_seed: int = 1
    batch_customers: int = 4_000
    batch_transactions: int = 200_000


def _log(message: str) -> None:
    print(f"[fraud-eval] {message}", flush=True)


def _score(spark: SparkSession, detector: Detector, dataset: ReplayDataset) -> ScoredDataset:
    _log(f"{dataset.label} seed={dataset.seed}: {len(dataset.events):,} eventos, pontuando…")
    return score_dataset(dataset, detector.score(spark, dataset))


def _dataset_stats(ds: ScoredDataset) -> dict[str, Any]:
    return {
        "seed": ds.seed,
        "events": int(len(ds.y)),
        "fraud": int(ds.y.sum()),
        "episodes": int(len({e for e in ds.episode_id.tolist() if e})),
        "coverage": float(ds.covered.mean()) if len(ds.y) else metrics.NAN,
    }


def evaluate_batch_density(
    spark: SparkSession, cfg: EvalConfig, detector: Detector
) -> dict[str, Any]:
    """Mesmo detector no regime do `make seed-data`: mostra o que a janela curta vê ali."""
    dataset = simulate_batch(
        cfg.batch_seed, cfg.batch_customers, cfg.batch_transactions, end=cfg.replay.start
    )
    ds = _score(spark, detector, dataset)
    summary = summarize(ds, ds.deployed_alert)
    return {
        **_dataset_stats(ds),
        "customers": cfg.batch_customers,
        "events_per_customer": cfg.batch_transactions / cfg.batch_customers,
        "alerts": int(ds.deployed_alert.sum()),
        "recall": summary["recall"],
        "precision": summary["precision"],
    }


def _sum_confusions(confusions: list[dict[str, dict[str, int]]]) -> dict[str, dict[str, int]]:
    total = {t: {p: 0 for p in (*FRAUD_TYPES, NO_TYPE)} for t in FRAUD_TYPES}
    for confusion in confusions:
        for true, row in confusion.items():
            for pred, n in row.items():
                total[true][pred] += n
    return total


def _evaluate_detector(
    spark: SparkSession,
    detector: Detector,
    validation_raw: ReplayDataset,
    tests_raw: list[ReplayDataset],
) -> tuple[dict[str, Any], ScoredDataset, list[ScoredDataset]]:
    """Calibra o limiar na validação e mede nas seeds de teste, para um detector."""
    validation = _score(spark, detector, validation_raw)
    thresholds = {
        fpr: metrics.threshold_for_fpr(validation.y, validation.score, fpr)
        for fpr in OPERATING_FPRS
    }

    per_condition: dict[str, list[dict[str, Any]]] = {
        DEPLOYED: [],
        **{condition_key(f): [] for f in OPERATING_FPRS},
    }
    per_seed: list[dict[str, dict[str, float]]] = []
    scored_tests: list[ScoredDataset] = []
    pr_auc: list[float] = []
    recall_at_fpr: list[float] = []
    confusions: list[dict[str, dict[str, int]]] = []
    for raw in tests_raw:
        ds = _score(spark, detector, raw)
        scored_tests.append(ds)
        summaries = {DEPLOYED: summarize(ds, ds.deployed_alert)}
        for fpr in OPERATING_FPRS:
            summaries[condition_key(fpr)] = summarize(ds, ds.score > thresholds[fpr])
        for name, summary in summaries.items():
            per_condition[name].append(summary)
        per_seed.append(
            {n: {m: s_[m] for m in ("precision", "recall", "f1")} for n, s_ in summaries.items()}
        )
        pr_auc.append(metrics.average_precision(ds.y, ds.score))
        recall_at_fpr.append(metrics.recall_at_fpr(ds.y, ds.score, PRIMARY_FPR))
        if detector.infers_type:
            confusions.append(type_confusion(ds, ds.deployed_alert))

    weights = getattr(detector, "weights", None)
    result: dict[str, Any] = {
        "name": detector.name,
        "thresholds": thresholds,
        "conditions": {name: aggregate(items) for name, items in per_condition.items()},
        "per_seed": per_seed,
        "ranking": {
            "pr_auc": metrics.mean_sd(pr_auc),
            "recall_at_fpr": metrics.mean_sd(recall_at_fpr),
            "target_fpr": PRIMARY_FPR,
        },
        "coverage": metrics.mean_sd(
            [float(ds.covered.mean()) if len(ds.y) else metrics.NAN for ds in scored_tests]
        ),
        "type_confusion": _sum_confusions(confusions) if confusions else None,
        "weights": dict(weights) if weights else None,
        "signals": signal_diagnostics(scored_tests, dict(weights)) if weights else None,
        "sensitivity": (
            sensitivity_without(validation, scored_tests, SENSITIVITY_SIGNAL)
            if validation.signal_values is not None
            else None
        ),
        "alert_threshold": getattr(detector, "threshold", None),
    }
    return result, validation, scored_tests


def compare(baseline: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    """Diferença pareada por seed (desafiante − linha de base), em Precision, Recall e F1."""
    out: dict[str, Any] = {
        "baseline": baseline["name"],
        "challenger": challenger["name"],
        "conditions": {},
    }
    for cond in (DEPLOYED, condition_key(PRIMARY_FPR)):
        deltas = {
            m: [
                c[cond][m] - b[cond][m]
                for b, c in zip(baseline["per_seed"], challenger["per_seed"], strict=True)
            ]
            for m in ("precision", "recall", "f1")
        }
        out["conditions"][cond] = {
            **{m: metrics.mean_sd(v) for m, v in deltas.items()},
            "wins": sum(1 for d in deltas["f1"] if d > 0),
            "seeds": len(deltas["f1"]),
        }
    return out


def run_protocol(spark: SparkSession, cfg: EvalConfig, detectors: list[Detector]) -> dict[str, Any]:
    """Mede todos os `detectors` nos mesmos datasets. Devolve o que o relatório renderiza.

    O limiar da regra de operação é calibrado, por detector, só na seed de validação. O primeiro
    detector da lista é a linha de base das comparações.
    """
    need_history = any(d.needs_history for d in detectors)

    def build(seed: int) -> ReplayDataset:
        dataset = simulate_stream(seed, cfg.replay)
        if need_history:
            dataset.history = simulate_history(seed, cfg.replay)
        return dataset

    validation_raw = build(cfg.validation_seed)
    tests_raw = [build(seed) for seed in cfg.test_seeds]

    detector_results: list[dict[str, Any]] = []
    shared: tuple[ScoredDataset, list[ScoredDataset]] | None = None
    for detector in detectors:
        result, validation, scored_tests = _evaluate_detector(
            spark, detector, validation_raw, tests_raw
        )
        detector_results.append(result)
        shared = shared or (validation, scored_tests)
    assert shared is not None
    validation, scored_tests = shared

    results: dict[str, Any] = {
        "config": {
            "n_customers": cfg.replay.n_customers,
            "rate_tps": cfg.replay.rate_tps,
            "duration_minutes": cfg.replay.duration_minutes,
            "warmup_minutes": cfg.replay.warmup_minutes,
            "start": cfg.replay.start.isoformat(),
            "cutoff": cfg.replay.cutoff.isoformat(),
            "diurnal": cfg.replay.diurnal,
            "history_transactions": cfg.replay.history_transactions if need_history else None,
            "history_days": cfg.replay.history_days if need_history else None,
            "validation_seed": cfg.validation_seed,
            "test_seeds": list(cfg.test_seeds),
            "z_threshold": Z_SCORE_THRESHOLD,
            "z_window_seconds": Z_SCORE_WINDOW_SECONDS,
            "z_min_transactions": Z_SCORE_MIN_TRANSACTIONS,
        },
        "validation": _dataset_stats(validation),
        "tests": [_dataset_stats(ds) for ds in scored_tests],
        "detectors": detector_results,
        "comparison": [compare(detector_results[0], other) for other in detector_results[1:]],
        "batch_density": None,
    }
    if cfg.include_batch_density:
        light = next((d for d in detectors if not d.needs_history), None)
        if light is not None:
            results["batch_density"] = evaluate_batch_density(spark, cfg, light)
    return results


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    defaults = ReplayConfig()
    eval_defaults = EvalConfig()
    parser = argparse.ArgumentParser(
        description="Avalia o detector de fraude (Precision, Recall, FPR) e gera o relatório."
    )
    parser.add_argument("--customers", type=int, default=defaults.n_customers)
    parser.add_argument("--rate-tps", type=float, default=defaults.rate_tps)
    parser.add_argument("--duration-minutes", type=int, default=defaults.duration_minutes)
    parser.add_argument("--warmup-minutes", type=int, default=defaults.warmup_minutes)
    parser.add_argument(
        "--profile-until",
        type=datetime.fromisoformat,
        default=None,
        help="Corte (ISO 8601): antes dele os eventos só alimentam o estado do detector; a "
        "avaliação usa o que vem depois. Padrão: início + --warmup-minutes.",
    )
    parser.add_argument("--start", type=datetime.fromisoformat, default=defaults.start)
    parser.add_argument("--validation-seed", type=int, default=eval_defaults.validation_seed)
    parser.add_argument("--test-seeds", type=int, nargs="+", default=list(eval_defaults.test_seeds))
    parser.add_argument(
        "--diurnal",
        action="store_true",
        help="Ritmo diurno no replay (o volume segue o horário ativo dos clientes). Use com --start "
        "numa janela que passe pela madrugada para exercitar o sinal UNUSUAL_HOUR.",
    )
    parser.add_argument("--history-transactions", type=int, default=defaults.history_transactions)
    parser.add_argument(
        "--detectors",
        nargs="+",
        choices=[ZScoreV1.name, MultiSignalV2.name],
        default=[ZScoreV1.name, MultiSignalV2.name],
        help="Detectores a medir; o primeiro é a linha de base das comparações.",
    )
    parser.add_argument("--skip-batch-density", action="store_true")
    parser.add_argument("--batch-customers", type=int, default=eval_defaults.batch_customers)
    parser.add_argument("--batch-transactions", type=int, default=eval_defaults.batch_transactions)
    parser.add_argument("--output", type=Path, default=Path("docs/fraud_evaluation.md"))
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> EvalConfig:
    start = args.start if args.start.tzinfo else args.start.replace(tzinfo=UTC)
    warmup = args.warmup_minutes
    if args.profile_until is not None:
        cutoff = (
            args.profile_until
            if args.profile_until.tzinfo
            else args.profile_until.replace(tzinfo=UTC)
        )
        warmup = int((cutoff - start) / timedelta(minutes=1))
    return EvalConfig(
        replay=ReplayConfig(
            n_customers=args.customers,
            rate_tps=args.rate_tps,
            duration_minutes=args.duration_minutes,
            warmup_minutes=warmup,
            start=start,
            diurnal=args.diurnal,
            history_transactions=args.history_transactions,
        ),
        validation_seed=args.validation_seed,
        test_seeds=tuple(args.test_seeds),
        include_batch_density=not args.skip_batch_density,
        batch_customers=args.batch_customers,
        batch_transactions=args.batch_transactions,
    )


def _make_detectors(names: list[str]) -> list[Detector]:
    factories: dict[str, type[Detector]] = {ZScoreV1.name: ZScoreV1, MultiSignalV2.name: MultiSignalV2}  # type: ignore[dict-item]
    return [factories[name]() for name in names]


def main(argv: list[str] | None = None) -> None:
    from src.transformation.fraud.report import render_report

    args = _parse_args(argv)
    cfg = build_config(args)
    spark = create_spark_session(app_name="fraud_eval", local_mode=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    results = run_protocol(spark, cfg, _make_detectors(args.detectors))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_report(results), encoding="utf-8")
    for detector in results["detectors"]:
        deployed = detector["conditions"][DEPLOYED]
        _log(
            f"{detector['name']}: precisão={deployed['precision'][0]:.3f} "
            f"recall={deployed['recall'][0]:.3f} f1={deployed['f1'][0]:.3f}"
        )
    _log(f"relatório em {args.output}")
    if any(math.isnan(d["conditions"][DEPLOYED]["precision"][0]) for d in results["detectors"]):
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
