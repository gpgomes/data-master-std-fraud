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


TEST_API_KEY = "chave-de-teste"


@pytest.fixture
def api_key(monkeypatch) -> str:
    """Configura a API para aceitar uma chave de teste (só o hash vai para settings, como em
    produção) e a devolve para os clientes mandarem no header `X-API-Key` (issue #58)."""
    from src.common.config import settings
    from src.serving.api.security import hash_key

    monkeypatch.setattr(settings.api, "api_key_hashes", hash_key(TEST_API_KEY))
    return TEST_API_KEY

