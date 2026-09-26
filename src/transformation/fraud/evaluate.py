"""Avaliação do detector de fraude: Precision, Recall, FPR e onde ele erra (issue #44).

Mede o detector de streaming atual (`zscore-v1`, o `_enrich_and_score` de produção) contra o ground
truth do gerador sintético, e grava `docs/fraud_evaluation.md`. É a "régua" que o Fraud Engine
multi-signal (#45) precisa superar: o "antes" fica registrado aqui.

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
from typing import Any, Protocol

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.common.schemas import FraudType
from src.common.spark_session import create_spark_session
from src.transformation.fraud import metrics
from src.transformation.fraud.replay import (
    ReplayConfig,
    ReplayDataset,
    simulate_batch,
    simulate_stream,
)
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
HYPOTHESIS_PRECISION = 0.50  # hipótese da issue #44: a Precision do V1 fica perto de 50%
HYPOTHESIS_TOLERANCE = 0.10
ANALYTIC_FPR_RANGE = (0.016, 0.046)  # FPR estimado na issue #44 (população; baseline amostral)


def condition_key(target_fpr: float) -> str:
    return f"fpr_{target_fpr:g}"


# ── Detectores ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DetectorOutput:
    """Saída de um detector para um evento."""

    score: float  # ranking: maior = mais arriscado
    alert: bool  # decisão do detector como implantado
    covered: bool  # havia baseline/contexto suficiente para avaliar o evento


class Detector(Protocol):
    """Contrato que o V2 (#45) também vai implementar."""

    name: str

    def score_events(
        self, spark: SparkSession, events: list[dict[str, Any]]
    ) -> dict[str, DetectorOutput]: ...


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


def score_dataset(dataset: ReplayDataset, outputs: dict[str, DetectorOutput]) -> ScoredDataset:
    """Junta a saída do detector com o ground truth, só para os eventos depois do corte."""
    cutoff = dataset.cutoff.isoformat()
    rows = [e for e in dataset.events if e["timestamp"] >= cutoff]

    y, score, alert, covered = [], [], [], []
    fraud_type, stealth, episode, epoch, hn_text = [], [], [], [], []
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
    return score_dataset(dataset, detector.score_events(spark, dataset.events))


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


def run_protocol(spark: SparkSession, cfg: EvalConfig, detector: Detector) -> dict[str, Any]:
    """Valida o limiar, mede nas seeds de teste e agrega. Devolve o que o relatório renderiza."""
    validation = _score(spark, detector, simulate_stream(cfg.validation_seed, cfg.replay))
    thresholds = {
        fpr: metrics.threshold_for_fpr(validation.y, validation.score, fpr)
        for fpr in OPERATING_FPRS
    }

    per_condition: dict[str, list[dict[str, Any]]] = {
        DEPLOYED: [],
        **{condition_key(f): [] for f in OPERATING_FPRS},
    }
    test_stats: list[dict[str, Any]] = []
    pr_auc: list[float] = []
    recall_at_fpr: list[float] = []
    for seed in cfg.test_seeds:
        ds = _score(spark, detector, simulate_stream(seed, cfg.replay))
        test_stats.append(_dataset_stats(ds))
        per_condition[DEPLOYED].append(summarize(ds, ds.deployed_alert))
        for fpr in OPERATING_FPRS:
            per_condition[condition_key(fpr)].append(summarize(ds, ds.score > thresholds[fpr]))
        pr_auc.append(metrics.average_precision(ds.y, ds.score))
        recall_at_fpr.append(metrics.recall_at_fpr(ds.y, ds.score, PRIMARY_FPR))

    results: dict[str, Any] = {
        "detector": detector.name,
        "config": {
            "n_customers": cfg.replay.n_customers,
            "rate_tps": cfg.replay.rate_tps,
            "duration_minutes": cfg.replay.duration_minutes,
            "warmup_minutes": cfg.replay.warmup_minutes,
            "start": cfg.replay.start.isoformat(),
            "cutoff": cfg.replay.cutoff.isoformat(),
            "validation_seed": cfg.validation_seed,
            "test_seeds": list(cfg.test_seeds),
            "z_threshold": Z_SCORE_THRESHOLD,
            "z_window_seconds": Z_SCORE_WINDOW_SECONDS,
            "z_min_transactions": Z_SCORE_MIN_TRANSACTIONS,
        },
        "thresholds": thresholds,
        "validation": _dataset_stats(validation),
        "tests": test_stats,
        "conditions": {name: aggregate(items) for name, items in per_condition.items()},
        "ranking": {
            "pr_auc": metrics.mean_sd(pr_auc),
            "recall_at_fpr": metrics.mean_sd(recall_at_fpr),
            "target_fpr": PRIMARY_FPR,
        },
        "coverage": metrics.mean_sd([s["coverage"] for s in test_stats]),
        "batch_density": None,
    }
    if cfg.include_batch_density:
        results["batch_density"] = evaluate_batch_density(spark, cfg, detector)
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
        ),
        validation_seed=args.validation_seed,
        test_seeds=tuple(args.test_seeds),
        include_batch_density=not args.skip_batch_density,
        batch_customers=args.batch_customers,
        batch_transactions=args.batch_transactions,
    )


def main(argv: list[str] | None = None) -> None:
    from src.transformation.fraud.report import render_report

    args = _parse_args(argv)
    cfg = build_config(args)
    spark = create_spark_session(app_name="fraud_eval", local_mode=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    results = run_protocol(spark, cfg, ZScoreV1())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_report(results), encoding="utf-8")
    deployed = results["conditions"][DEPLOYED]
    precision = deployed["precision"][0]
    recall = deployed["recall"][0]
    _log(
        f"|z|>{Z_SCORE_THRESHOLD:g}: precisão={precision:.3f} recall={recall:.3f} "
        f"| relatório em {args.output}"
    )
    if math.isnan(precision):
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
