"""Score noisy-OR sobre os sinais (issue #45).

`score = 1 − Π(1 − wᵢ·sᵢ)`. Cada sinal `sᵢ` ∈ [0, 1] é uma evidência independente de peso `wᵢ`; o
score fica em [0, 1], nunca cai quando um sinal a mais se acende, e o motivo do alerta sai de graça
(a lista de sinais ativos). Existe em duas formas com a mesma conta: `Column` (Spark) e numpy (a
calibração dos pesos), e um teste garante que dão o mesmo número.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import reduce
from typing import TYPE_CHECKING

from pyspark.sql import Column
from pyspark.sql import functions as F

from src.transformation.fraud.signals import SIGNALS, signal_column

if TYPE_CHECKING:  # numpy só é importado dentro de `noisy_or_np` (ver abaixo)
    import numpy as np

SIGNAL_ACTIVE = (
    0.5  # um sinal graduado conta como "ativo" (aparece em `fraud_signals`) a partir daqui
)


def noisy_or(weights: Mapping[str, float]) -> Column:
    """Coluna `fraud_score` a partir das colunas `sig_*` do DataFrame."""
    missing = set(SIGNALS) - set(weights)
    if missing:
        raise ValueError(f"pesos faltando para os sinais: {sorted(missing)}")
    survive = [1.0 - float(weights[name]) * F.col(signal_column(name)) for name in SIGNALS]
    return F.lit(1.0) - reduce(lambda a, b: a * b, survive)


def active_signals() -> Column:
    """Array com o nome dos sinais ativos (`sig_* ≥ SIGNAL_ACTIVE`), na ordem de `SIGNALS`."""
    names = [F.when(F.col(signal_column(s)) >= SIGNAL_ACTIVE, F.lit(s)) for s in SIGNALS]
    return F.filter(F.array(*names), lambda x: x.isNotNull())


def noisy_or_np(
    matrix: np.ndarray, weights: Mapping[str, float] | Sequence[float] | np.ndarray
) -> np.ndarray:
    """O mesmo score, em numpy: `matrix` é (n_eventos × n_sinais) na ordem de `SIGNALS`.

    O numpy é importado aqui dentro de propósito: este módulo é carregado pelo job de streaming,
    que roda no container do Spark (Python 3.8, **sem numpy**). Só a calibração e o avaliador,
    que rodam fora do container, chamam esta função.
    """
    import numpy as np

    w = (
        np.array([weights[s] for s in SIGNALS], dtype=float)
        if isinstance(weights, Mapping)
        else np.asarray(weights, dtype=float)
    )
    score: np.ndarray = 1.0 - np.prod(1.0 - matrix * w, axis=1)
    return score
