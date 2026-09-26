"""Construtores de dados de teste do Fraud Engine: eventos, histórico, clientes e perfis.

Os DataFrames saem de arquivos JSON temporários (e não de `createDataFrame`) porque o caminho de RDD
local quebra no Python 3.14, como nos demais testes do projeto.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# São Paulo e Rio: ~360 km. Salvador: ~1.450 km de São Paulo.
SAO_PAULO = (-23.5505, -46.6333)
RIO = (-22.9068, -43.1729)
SALVADOR = (-12.9714, -38.5014)

EVENT_SCHEMA = StructType(
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
LABELLED_EVENT_SCHEMA = StructType(
    [
        *EVENT_SCHEMA.fields,
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),
    ]
)
HISTORY_SCHEMA = StructType([*EVENT_SCHEMA.fields, StructField("is_fraud", BooleanType(), True)])
CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("account_opening_date", DateType(), True),
    ]
)
PROFILE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("has_profile", BooleanType(), True),
        StructField("n_history", LongType(), True),
        StructField("mu_log", DoubleType(), True),
        StructField("sigma_log", DoubleType(), True),
        StructField("known_devices", ArrayType(StringType()), True),
        StructField("known_ip_prefixes", ArrayType(StringType()), True),
        StructField("known_destinations", ArrayType(StringType()), True),
        StructField("night_share", DoubleType(), True),
        StructField("home_lat", DoubleType(), True),
        StructField("home_lon", DoubleType(), True),
        StructField("account_opening_date", DateType(), True),
    ]
)


def df_from_rows(spark: SparkSession, rows: list[dict[str, Any]], schema: StructType) -> DataFrame:
    """DataFrame com `schema` a partir de dicts (campos ausentes ou None viram nulo)."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "rows.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(
                    json.dumps({k: v for k, v in row.items() if v is not None}, default=str) + "\n"
                )
        df = spark.read.schema(schema).json(path).cache()
        df.count()  # materializa antes de apagar o arquivo (a leitura é lazy)
        return df


def event(
    i: int | str,
    customer: str = "c1",
    ts: str = "2026-03-02T12:00:00",
    amount: float = 100.0,
    device: str | None = "dev-known",
    ip: str | None = "10.1.1.7",
    at: tuple[float, float] | None = SAO_PAULO,
    dest: str | None = "acc-known",
    **extra: Any,
) -> dict[str, Any]:
    """Um evento. `ts` é UTC; sem fuso na string, como o Spark lê nos testes."""
    return {
        "transaction_id": f"t{i}",
        "customer_id": customer,
        "timestamp": ts,
        "amount": amount,
        "device_id": device,
        "ip_address": ip,
        "latitude": at[0] if at else None,
        "longitude": at[1] if at else None,
        "destination_account": dest,
        **extra,
    }


def profile_row(customer: str = "c1", **overrides: Any) -> dict[str, Any]:
    """Um perfil conhecido: valor típico R$ 100 (μ = ln 100, σ = 0,5), casa em São Paulo."""
    import math

    base: dict[str, Any] = {
        "customer_id": customer,
        "has_profile": True,
        "n_history": 60,
        "mu_log": math.log(100.0),
        "sigma_log": 0.5,
        "known_devices": ["dev-known"],
        "known_ip_prefixes": ["10.1.1"],
        "known_destinations": ["acc-known"],
        "night_share": 0.01,
        "home_lat": SAO_PAULO[0],
        "home_lon": SAO_PAULO[1],
        "account_opening_date": date(2020, 1, 1).isoformat(),
    }
    base.update(overrides)
    return base


def profile_df(spark: SparkSession, *rows: dict[str, Any]) -> DataFrame:
    return df_from_rows(spark, list(rows) or [profile_row()], PROFILE_SCHEMA)


def events_df(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    return df_from_rows(spark, rows, EVENT_SCHEMA)
