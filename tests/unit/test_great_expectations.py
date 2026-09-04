"""Testes para os quality gates do Great Expectations (issue #13).

Roda um FileDataContext real (não mockado) contra um diretório temporário —
mesmo espírito dos testes com PySpark local do resto do projeto: exercita o
comportamento real da biblioteca, não um substituto. Para cada dataset, uma
fixture válida deve passar e uma fixture inválida (violando exatamente UMA
expectativa conhecida) deve falhar de forma determinística, levantando
`QualityGateFailed` — critério de aceite literal da issue.

Nota de ambiente: great-expectations==0.18.13 não importa em Python 3.14 (a
camada de compatibilidade `pydantic.v1` interna quebra em runtimes >= 3.13 —
`pydantic.v1` é uma limitação do próprio Pydantic, não da GX). Este arquivo
não roda na suíte principal (`.venv`, Python 3.14 local) por esse motivo —
mesma classe de incompatibilidade já documentada para SQLAlchemy/schemas.py
em issues anteriores. CI usa Python 3.11 (compatível) e não é afetado.
"""

from __future__ import annotations

import pandas as pd
import pytest

try:
    from src.governance.great_expectations.context import get_context
    from src.governance.great_expectations.runner import QualityGateFailed, run_gate
except Exception as exc:  # great_expectations não importa em Python >= 3.13 (ver nota acima)
    pytest.skip(f"great_expectations indisponível neste ambiente: {exc}", allow_module_level=True)


@pytest.fixture
def context(tmp_path):
    return get_context(project_root_dir=tmp_path / "gx")


def _run_valid(context, dataset_key: str, df: pd.DataFrame) -> None:
    metrics = run_gate(dataset_key, df=df, context=context)
    assert metrics["success"] is True
    assert metrics["failed_expectations"] == 0


def _run_invalid(context, dataset_key: str, df: pd.DataFrame) -> None:
    with pytest.raises(QualityGateFailed):
        run_gate(dataset_key, df=df, context=context)


# ── Bronze ───────────────────────────────────────────────────────────────────


class TestBronzeTransactions:
    _BASE = {
        "transaction_id": ["tx-1", "tx-2"],
        "customer_id": ["c1", "c2"],
        "timestamp": pd.to_datetime(["2024-06-15T10:00:00Z", "2024-06-15T11:00:00Z"]),
        "amount": [150.0, 200.0],
        "currency": ["BRL", "USD"],
        "transaction_type": ["PIX", "TED"],
        "merchant_category": ["ALIMENTACAO", "TRANSPORTE"],
        "origin_account": ["a1", "a2"],
        "destination_account": ["b1", "b2"],
        "origin_bank": ["001", "002"],
        "destination_bank": ["003", "004"],
        "channel": ["APP_MOBILE", "INTERNET_BANKING"],
        "is_fraud": [False, True],
        "fraud_type": [None, "CARD_CLONING"],
    }

    def _fresh_ingestion(self, n: int) -> list:
        return [pd.Timestamp.now(tz="UTC")] * n

    def test_valid_passes(self, context):
        df = pd.DataFrame({**self._BASE, "ingestion_timestamp": self._fresh_ingestion(2)})
        _run_valid(context, "bronze_transactions", df)

    def test_invalid_fraud_type_missing_fails(self, context):
        bad = {**self._BASE, "fraud_type": [None, None]}  # is_fraud=True sem fraud_type
        df = pd.DataFrame({**bad, "ingestion_timestamp": self._fresh_ingestion(2)})
        _run_invalid(context, "bronze_transactions", df)


class TestBronzeMarketData:
    def _base(self, **overrides) -> dict:
        base = {
            "symbol": ["PETR4.SA", "VALE3.SA"],
            "date": pd.to_datetime(["2024-06-14", "2024-06-15"]),
            "open": [30.0, 60.0],
            "high": [31.0, 61.0],
            "low": [29.0, 59.0],
            "close": [30.5, 60.5],
            "volume": [1000, 2000],
            "adjusted_close": [30.5, 60.5],
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "bronze_market_data", pd.DataFrame(self._base()))

    def test_invalid_high_below_low_fails(self, context):
        df = pd.DataFrame(self._base(high=[28.0, 61.0]))  # high < low na 1a linha
        _run_invalid(context, "bronze_market_data", df)


# ── Silver ───────────────────────────────────────────────────────────────────


class TestSilverTransactions:
    def _base(self, **overrides) -> dict:
        base = {
            "transaction_id": ["tx-1", "tx-2"],
            "customer_id": ["c1", "c2"],
            "timestamp": pd.to_datetime(["2024-06-15T10:00:00Z", "2024-06-15T11:00:00Z"]),
            "amount": [150.0, 200.0],
            "currency": ["BRL", "BRL"],
            "transaction_type": ["PIX", "TED"],
            "channel": ["APP_MOBILE", "INTERNET_BANKING"],
            "is_fraud": [False, False],
            "fraud_type": [None, None],
            "processing_timestamp": [pd.Timestamp.now(tz="UTC")] * 2,
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "silver_transactions", pd.DataFrame(self._base()))

    def test_invalid_duplicate_transaction_id_fails(self, context):
        df = pd.DataFrame(self._base(transaction_id=["tx-1", "tx-1"]))
        _run_invalid(context, "silver_transactions", df)


class TestSilverMarketData:
    def _base(self, **overrides) -> dict:
        base = {
            "symbol": ["PETR4.SA", "VALE3.SA"],
            "date": pd.to_datetime(["2024-06-14", "2024-06-15"]),
            "open": [30.0, 60.0],
            "high": [31.0, 61.0],
            "low": [29.0, 59.0],
            "close": [30.5, 60.5],
            "volume": [1000, 2000],
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "silver_market_data", pd.DataFrame(self._base()))

    def test_invalid_high_below_low_fails(self, context):
        df = pd.DataFrame(self._base(high=[28.0, 61.0]))
        _run_invalid(context, "silver_market_data", df)


# ── Gold ─────────────────────────────────────────────────────────────────────


class TestGoldFactTransactions:
    def _base(self, **overrides) -> dict:
        base = {
            "transaction_id": ["tx-1", "tx-2"],
            "customer_key": ["c1", "c2"],
            "date_key": pd.to_datetime(["2024-06-14", "2024-06-15"]),
            "amount_brl": [150.0, 200.0],
            "currency": ["BRL", "BRL"],
            "transaction_type": ["PIX", "TED"],
            "channel": ["APP_MOBILE", "INTERNET_BANKING"],
            "merchant_category": ["ALIMENTACAO", "TRANSPORTE"],
            "is_fraud": [False, True],
            "fraud_type": [None, "CARD_CLONING"],
            "fraud_score": [None, 0.9],
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "gold_fact_transactions", pd.DataFrame(self._base()))

    def test_invalid_negative_amount_fails(self, context):
        df = pd.DataFrame(self._base(amount_brl=[-10.0, 200.0]))
        _run_invalid(context, "gold_fact_transactions", df)


class TestGoldDimCustomers:
    def _base(self, **overrides) -> dict:
        base = {
            "customer_key": ["c1", "c2", "c3"],
            "segment": ["VAREJO", "ALTA_RENDA", "PRIVATE"],
            "city": ["SP", "RJ", "BH"],
            "state": ["SP", "RJ", "MG"],
            "country": ["BR", "BR", "BR"],
            "risk_score": [10.0, 50.0, 90.0],
            "age": [30, 45, 60],
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "gold_dim_customers", pd.DataFrame(self._base()))

    def test_invalid_duplicate_pk_fails(self, context):
        df = pd.DataFrame(self._base(customer_key=["c1", "c1", "c3"]))
        _run_invalid(context, "gold_dim_customers", df)


class TestGoldDimDate:
    def _base(self, **overrides) -> dict:
        base = {
            "date_key": pd.to_datetime(["2024-06-14", "2024-06-15"]),
            "year": [2024, 2024],
            "month": [6, 6],
            "day": [14, 15],
            "quarter": [2, 2],
            "day_of_week": [6, 7],
            "day_name": ["Friday", "Saturday"],
            "week_of_year": [24, 24],
            "is_weekend": [False, True],
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "gold_dim_date", pd.DataFrame(self._base()))

    def test_invalid_month_out_of_range_fails(self, context):
        df = pd.DataFrame(self._base(month=[6, 13]))
        _run_invalid(context, "gold_dim_date", df)


class TestGoldAggDailyFraudMetrics:
    def _base(self, **overrides) -> dict:
        base = {
            "date_key": pd.to_datetime(["2024-06-14", "2024-06-15"]),
            "transaction_type": ["PIX", "TED"],
            "total_transactions": [100, 50],
            "total_amount_brl": [15000.0, 8000.0],
            "avg_amount_brl": [150.0, 160.0],
            "fraud_count": [3, 1],
            "fraud_rate": [0.03, 0.02],
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "gold_agg_daily_fraud_metrics", pd.DataFrame(self._base()))

    def test_invalid_fraud_count_exceeds_total_fails(self, context):
        df = pd.DataFrame(self._base(fraud_count=[3, 60]))  # 60 > total_transactions=50
        _run_invalid(context, "gold_agg_daily_fraud_metrics", df)

    def test_invalid_duplicate_compound_pk_fails(self, context):
        df = pd.DataFrame(self._base(transaction_type=["PIX", "PIX"], date_key=pd.to_datetime(["2024-06-14", "2024-06-14"])))
        _run_invalid(context, "gold_agg_daily_fraud_metrics", df)
