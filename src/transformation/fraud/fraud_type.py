"""Tipo de fraude inferido pelos sinais (issue #45).

Regras com prioridade (a primeira que casa vence) e **nulo** quando nenhuma casa. Nunca copia o
rótulo do evento: é exatamente isso que o V1 fazia (`stream_processor.py`, com um fallback fixo).

| Tipo | Regra |
|---|---|
| `ACCOUNT_TAKEOVER` | `NEW_DEVICE` ∧ (`NEW_IP` ∨ `GEO_FAR_FROM_HOME`) |
| `CARD_CLONING` | `GEO_VELOCITY` ∧ ¬`NEW_DEVICE` |
| `IDENTITY_THEFT` | `ACCOUNT_AGE_LOW` ∧ (`NEW_DEVICE` ∨ `AMOUNT_ANOMALY`) |
| `MONEY_LAUNDERING` | `RECIPIENT_CONCENTRATION` ∨ (`NEW_DESTINATION` ∧ `TX_VELOCITY`) |
| `SOCIAL_ENGINEERING` | ¬`NEW_DEVICE` ∧ ¬`NEW_IP` ∧ `NEW_DESTINATION` ∧ `AMOUNT_ANOMALY` |

`SOCIAL_ENGINEERING` (vítima autenticada no próprio device e rede) é o tipo mais difícil e deve ter o
menor Recall: o que o distingue de um pagamento legítimo grande é só o destinatário novo.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from src.common.schemas import FraudType
from src.transformation.fraud.scoring import SIGNAL_ACTIVE
from src.transformation.fraud.signals import (
    ACCOUNT_AGE_LOW,
    AMOUNT_ANOMALY,
    GEO_FAR_FROM_HOME,
    GEO_VELOCITY,
    NEW_DESTINATION,
    NEW_DEVICE,
    NEW_IP,
    RECIPIENT_CONCENTRATION,
    TX_VELOCITY,
    signal_column,
)


def _on(name: str) -> Column:
    return F.col(signal_column(name)) >= SIGNAL_ACTIVE


def predict_fraud_type() -> Column:
    """Coluna `fraud_type_predicted` (string ou nula) a partir das colunas `sig_*`."""
    return (
        F.when(
            _on(NEW_DEVICE) & (_on(NEW_IP) | _on(GEO_FAR_FROM_HOME)),
            F.lit(FraudType.ACCOUNT_TAKEOVER.value),
        )
        .when(_on(GEO_VELOCITY) & ~_on(NEW_DEVICE), F.lit(FraudType.CARD_CLONING.value))
        .when(
            _on(ACCOUNT_AGE_LOW) & (_on(NEW_DEVICE) | _on(AMOUNT_ANOMALY)),
            F.lit(FraudType.IDENTITY_THEFT.value),
        )
        .when(
            _on(RECIPIENT_CONCENTRATION) | (_on(NEW_DESTINATION) & _on(TX_VELOCITY)),
            F.lit(FraudType.MONEY_LAUNDERING.value),
        )
        .when(
            ~_on(NEW_DEVICE) & ~_on(NEW_IP) & _on(NEW_DESTINATION) & _on(AMOUNT_ANOMALY),
            F.lit(FraudType.SOCIAL_ENGINEERING.value),
        )
        .otherwise(F.lit(None).cast("string"))
    )
