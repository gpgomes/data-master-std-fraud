"""Fraud Engine multi-signal: o detector `multisignal-v2` (issue #45).

`detect` junta as três peças puras: os sinais (`signals.py`), o score noisy-OR (`scoring.py`) e o
tipo inferido (`fraud_type.py`). Saída, além das colunas de entrada:

  - `fraud_score` ∈ [0, 1] e `is_fraud_predicted` (`fraud_score > ALERT_THRESHOLD`);
  - `fraud_signals`: array com os sinais ativos, o motivo do alerta;
  - `fraud_type_predicted`: tipo inferido pelos sinais, nulo se nenhuma regra casa;
  - `detector_version = "multisignal-v2"`.

O detector **nunca lê o rótulo**: `compute_signals` só enxerga `EVENT_COLUMNS`. O avaliador chama
com o dataset inteiro como `events`; o streaming (#46) chama com o micro-batch e o estado curto em
`recent`.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformation.fraud.fraud_type import predict_fraud_type
from src.transformation.fraud.scoring import active_signals, noisy_or
from src.transformation.fraud.signals import compute_signals
from src.transformation.fraud.weights import ALERT_THRESHOLD, SIGNAL_WEIGHTS

DETECTOR_VERSION = "multisignal-v2"


def detect(
    events: DataFrame,
    profile: DataFrame,
    recent: DataFrame | None = None,
    weights: Mapping[str, float] | None = None,
    threshold: float | None = None,
) -> DataFrame:
    """Pontua `events` com o perfil dos clientes. `weights` e `threshold` padrão vêm de `weights.py`."""
    w = SIGNAL_WEIGHTS if weights is None else weights
    limit = ALERT_THRESHOLD if threshold is None else threshold
    return (
        compute_signals(events, profile, recent)
        .withColumn("fraud_score", noisy_or(w))
        .withColumn("is_fraud_predicted", F.col("fraud_score") > F.lit(limit))
        .withColumn("fraud_signals", active_signals())
        .withColumn("fraud_type_predicted", predict_fraud_type())
        .withColumn("detector_version", F.lit(DETECTOR_VERSION))
    )
