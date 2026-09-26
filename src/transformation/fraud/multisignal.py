"""Adaptador do Fraud Engine (`multisignal-v2`) para o harness de avaliação (issue #45).

Lê os três conjuntos do `ReplayDataset` (eventos do replay, histórico de batch e clientes), monta o
perfil com `build_profiles`, pontua com `detect` e devolve uma saída por `transaction_id`. O
detector só vê `EVENT_COLUMNS`: o rótulo dos eventos nunca chega ao Spark do lado do detector.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transformation.fraud.detector import DETECTOR_VERSION, detect
from src.transformation.fraud.detector_api import DetectorOutput
from src.transformation.fraud.profile import build_profiles
from src.transformation.fraud.replay import ReplayDataset
from src.transformation.fraud.signals import SIGNALS, signal_column
from src.transformation.fraud.weights import ALERT_THRESHOLD, SIGNAL_WEIGHTS

_EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("destination_account", StringType(), True),
    ]
)
_HISTORY_SCHEMA = StructType([*_EVENT_SCHEMA.fields, StructField("is_fraud", BooleanType(), True)])
_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("account_opening_date", DateType(), True),
    ]
)


def _read(
    spark: SparkSession, tmp: str, name: str, rows: list[dict[str, Any]], schema: StructType
) -> DataFrame:
    """Grava as linhas como JSON e lê com schema. Via arquivo, não `createDataFrame`: o caminho de
    RDD local quebra no Python 3.14. Só as colunas do schema saem: nenhum campo extra (o rótulo dos
    eventos, por exemplo) chega ao DataFrame."""
    path = os.path.join(tmp, f"{name}.jsonl")
    columns = [f.name for f in schema.fields]
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps({c: row.get(c) for c in columns}) + "\n")
    return spark.read.schema(schema).json(path)


class MultiSignalV2:
    """O Fraud Engine multi-signal, pelo mesmo contrato do V1."""

    name = DETECTOR_VERSION
    needs_history = True  # o perfil vem do histórico de batch
    infers_type = True  # as regras de `fraud_type.py` inferem o tipo pelos sinais

    def __init__(
        self, weights: Mapping[str, float] | None = None, threshold: float | None = None
    ) -> None:
        self.weights = dict(SIGNAL_WEIGHTS if weights is None else weights)
        self.threshold = ALERT_THRESHOLD if threshold is None else threshold

    def score(self, spark: SparkSession, dataset: ReplayDataset) -> dict[str, DetectorOutput]:
        if not dataset.history:
            raise ValueError(
                "o detector com perfil precisa de `dataset.history` (attach do histórico)"
            )
        with tempfile.TemporaryDirectory() as tmp:
            events = _read(spark, tmp, "events", dataset.events, _EVENT_SCHEMA)
            history = _read(spark, tmp, "history", dataset.history, _HISTORY_SCHEMA)
            customers = _read(spark, tmp, "customers", dataset.customers, _CUSTOMER_SCHEMA)
            scored = detect(
                events,
                build_profiles(history, customers),
                weights=self.weights,
                threshold=self.threshold,
            )
            columns = [signal_column(s) for s in SIGNALS]
            rows = scored.select(
                "transaction_id",
                "fraud_score",
                "is_fraud_predicted",
                "fraud_type_predicted",
                "fraud_signals",
                "has_profile",
                *columns,
            ).collect()
        return {
            r["transaction_id"]: DetectorOutput(
                score=float(r["fraud_score"]),
                alert=bool(r["is_fraud_predicted"]),
                covered=bool(r["has_profile"]),
                predicted_type=r["fraud_type_predicted"],
                signals=tuple(r["fraud_signals"]),
                values=tuple(float(r[c]) for c in columns),
            )
            for r in rows
        }
