"""Fixtures compartilhadas dos testes unitários que precisam de Spark local."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def fraud_spark() -> SparkSession:
    """SparkSession local por módulo (mesmo padrão dos demais testes: cada módulo a encerra)."""
    session = (
        SparkSession.builder.appName("test-fraud-engine")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
