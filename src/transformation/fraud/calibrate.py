"""Calibração dos pesos e do limiar do detector multi-signal (issue #45).

Busca os pesos `wᵢ` do score noisy-OR na **seed de validação** e grava `weights.py`. Nunca toca nas
seeds de teste: o número que o relatório mostra não pode ter sido ajustado no conjunto que o reporta.

Objetivo (a regra de operação da série): **máximo recall com FPR ≤ 1%**, com a PR-AUC como
desempate. Busca por coordenadas: para cada sinal, testa os valores de `GRID` mantendo os outros
fixos, e repete até nenhum peso melhorar. Depois tenta reduzir cada peso ao menor valor que mantém
o objetivo (um sinal que não ajuda fica com peso zero). Os sinais vêm da mesma função de produção
(`compute_signals`); só a combinação é refeita em numpy, com a mesma conta do Spark.

Execução (requer Java para o Spark local; ver `make fraud-calibrate`):
    python -m src.transformation.fraud.calibrate            # grava weights.py
    python -m src.transformation.fraud.calibrate --dry-run  # só imprime
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from src.common.spark_session import create_spark_session
from src.transformation.fraud import metrics
from src.transformation.fraud.evaluate import PRIMARY_FPR, score_dataset
from src.transformation.fraud.multisignal import MultiSignalV2
from src.transformation.fraud.replay import ReplayConfig, simulate_history, simulate_stream
from src.transformation.fraud.scoring import noisy_or_np
from src.transformation.fraud.search import INITIAL_WEIGHT, Objective, coordinate_ascent
from src.transformation.fraud.signals import SIGNALS

DEFAULT_OUTPUT = Path(__file__).with_name("weights.py")


@dataclass(frozen=True)
class Calibration:
    weights: dict[str, float]
    threshold: float
    objective: Objective
    initial_objective: Objective
    seed: int
    events: int
    fraud: int


def calibrate(spark: SparkSession, replay: ReplayConfig, seed: int) -> Calibration:
    """Calibra os pesos e o limiar de alerta na seed de validação."""
    dataset = simulate_stream(seed, replay)
    dataset.history = simulate_history(seed, replay)
    scored = score_dataset(dataset, MultiSignalV2().score(spark, dataset))
    assert scored.signal_values is not None
    weights, best, start = coordinate_ascent(scored.signal_values, scored.y)
    threshold = metrics.threshold_for_fpr(
        scored.y, noisy_or_np(scored.signal_values, weights), PRIMARY_FPR
    )
    return Calibration(
        weights={name: round(float(w), 2) for name, w in zip(SIGNALS, weights, strict=True)},
        threshold=round(float(threshold), 4),
        objective=best,
        initial_objective=start,
        seed=seed,
        events=int(len(scored.y)),
        fraud=int(scored.y.sum()),
    )


def render_weights_module(cal: Calibration) -> str:
    """Conteúdo de `weights.py`: determinístico (sem data), então o git só muda se os pesos mudam."""
    lines = "\n".join(f'    "{name}": {cal.weights[name]:.2f},' for name in SIGNALS)
    return f'''"""Pesos dos sinais e limiar de alerta do detector multi-signal (issue #45).

Arquivo **gerado** por `make fraud-calibrate` (`python -m src.transformation.fraud.calibrate`), que
busca os pesos na seed de validação, nunca nas seeds de teste. Não edite à mão: mude o código dos
sinais ou o gerador, rode a calibração de novo e commite o resultado.

Os pesos entram no score como `1 − Π(1 − wᵢ·sᵢ)` (noisy-OR): cada sinal ativo é uma evidência
independente, o score fica em [0, 1] e nunca diminui quando aparece um sinal a mais.

Calibração: seed {cal.seed}, {cal.events:,} eventos avaliados ({cal.fraud:,} fraudes). Objetivo
(recall máximo com FPR ≤ {PRIMARY_FPR:.0%}): recall {cal.objective[0]:.4f}, PR-AUC {cal.objective[1]:.4f}
(pesos iniciais uniformes de {INITIAL_WEIGHT}: recall {cal.initial_objective[0]:.4f}, PR-AUC {cal.initial_objective[1]:.4f}).
"""

WEIGHTS_VERSION = "seed-{cal.seed}"

SIGNAL_WEIGHTS: dict[str, float] = {{
{lines}
}}

# Alerta quando `fraud_score > ALERT_THRESHOLD`: o menor limiar com FPR ≤ {PRIMARY_FPR:.0%} na validação.
ALERT_THRESHOLD = {cal.threshold}
'''


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    defaults = ReplayConfig()
    parser = argparse.ArgumentParser(
        description="Calibra os pesos e o limiar do detector multi-signal na seed de validação."
    )
    parser.add_argument("--customers", type=int, default=defaults.n_customers)
    parser.add_argument("--rate-tps", type=float, default=defaults.rate_tps)
    parser.add_argument("--duration-minutes", type=int, default=defaults.duration_minutes)
    parser.add_argument("--warmup-minutes", type=int, default=defaults.warmup_minutes)
    parser.add_argument("--start", type=datetime.fromisoformat, default=defaults.start)
    parser.add_argument("--diurnal", action="store_true")
    parser.add_argument("--history-transactions", type=int, default=defaults.history_transactions)
    parser.add_argument("--validation-seed", type=int, default=1000)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--dry-run", action="store_true", help="Só imprime; não grava o arquivo.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    start = args.start if args.start.tzinfo else args.start.replace(tzinfo=UTC)
    replay = ReplayConfig(
        n_customers=args.customers,
        rate_tps=args.rate_tps,
        duration_minutes=args.duration_minutes,
        warmup_minutes=args.warmup_minutes,
        start=start,
        diurnal=args.diurnal,
        history_transactions=args.history_transactions,
    )
    spark = create_spark_session(app_name="fraud_calibrate", local_mode=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    cal = calibrate(spark, replay, args.validation_seed)

    print(f"[fraud-calibrate] seed {cal.seed}: {cal.events:,} eventos, {cal.fraud:,} fraudes")
    print(
        f"[fraud-calibrate] recall@FPR≤{PRIMARY_FPR:.0%}: {cal.initial_objective[0]:.4f} → "
        f"{cal.objective[0]:.4f} | PR-AUC {cal.initial_objective[1]:.4f} → {cal.objective[1]:.4f}"
    )
    for name in SIGNALS:
        print(f"[fraud-calibrate]   {name:<26}{cal.weights[name]:.2f}")
    print(f"[fraud-calibrate] limiar de alerta: {cal.threshold}")
    if args.dry_run:
        return
    args.output.write_text(render_weights_module(cal), encoding="utf-8")
    print(f"[fraud-calibrate] gravado em {args.output}")


if __name__ == "__main__":
    main(sys.argv[1:])
