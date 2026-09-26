"""Contrato dos detectores avaliados pelo harness (issues #44 e #45).

Um detector recebe um `ReplayDataset` (eventos, e o histórico/clientes quando precisa de perfil) e
devolve uma saída por `transaction_id`. Vive num módulo à parte para o V1 (`evaluate.py`) e o V2
(`multisignal.py`) não dependerem um do outro.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from src.transformation.fraud.replay import ReplayDataset


@dataclass(frozen=True)
class DetectorOutput:
    """Saída de um detector para um evento."""

    score: float  # ranking: maior = mais arriscado
    alert: bool  # decisão do detector como implantado
    covered: bool  # havia baseline/contexto suficiente para avaliar o evento
    predicted_type: str | None = None  # tipo inferido (só o V2 infere)
    signals: tuple[str, ...] = ()  # sinais ativos, o motivo do alerta (só o V2)
    values: tuple[float, ...] = ()  # valor de cada sinal, na ordem de `SIGNALS` (só o V2)


class Detector(Protocol):
    """Um detector avaliável. `needs_history` diz se o harness deve gerar o histórico do perfil."""

    name: str
    needs_history: bool
    infers_type: bool  # se o detector prevê o tipo de fraude (só o V2); senão não há matriz de tipo

    def score(self, spark: SparkSession, dataset: ReplayDataset) -> dict[str, DetectorOutput]: ...
