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

from unittest.mock import patch

import pandas as pd
import pytest

try:
    from src.governance.great_expectations import runner
    from src.governance.great_expectations.context import get_context
    from src.governance.great_expectations.runner import (
        OPTIONAL_DATASETS,
        QualityGateFailed,
        run_gate,
        run_gate_optional,
    )
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
        # Naive de propósito: dados reais lidos de Parquet chegam como
        # datetime64[ns] sem timezone (ver `_freshness()` em suites.py).
        return [pd.Timestamp.now()] * n

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
            # Naive de propósito: dados reais lidos de Parquet chegam como
            # datetime64[ns] sem timezone (ver `_freshness()` em suites.py).
            "processing_timestamp": [pd.Timestamp.now()] * 2,
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


class TestGoldCustomerBehaviorProfile:
    """Perfil que o detector do streaming lê por broadcast (issues #46 e #47)."""

    def _base(self, **overrides) -> dict:
        base = {
            "customer_id": ["c1", "c2", "new"],
            "has_profile": [True, True, False],
            "n_history": [48, 30, 0],
            "mu_log": [4.9, 5.4, 4.97],
            "sigma_log": [0.8, 1.1, 0.87],
            "known_devices": [["d1", "d2"], ["d3"], []],
            "known_ip_prefixes": [["177.10.20"], ["10.1.1"], []],
            "known_destinations": [["acc-1"], ["acc-2", "acc-3"], []],
            "night_share": [0.1, 0.02, 1.0],
            "home_lat": [-23.55, -12.97, None],  # cliente novo: sem histórico, sem coordenadas
            "home_lon": [-46.63, -38.50, None],
            "account_opening_date": pd.to_datetime(["2016-09-24", "2020-01-01", "2026-09-01"]).date,
        }
        base.update(overrides)
        return base

    def test_valid_passes(self, context):
        _run_valid(context, "gold_customer_behavior_profile", pd.DataFrame(self._base()))

    def test_duplicate_customer_fails(self, context):
        df = pd.DataFrame(self._base(customer_id=["c1", "c1", "new"]))
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_null_mu_log_fails(self, context):
        df = pd.DataFrame(self._base(mu_log=[4.9, None, 4.97]))
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_mu_log_holding_the_raw_amount_instead_of_its_log_fails(self, context):
        df = pd.DataFrame(self._base(mu_log=[4.9, 5400.0, 4.97]))
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_sigma_below_the_floor_fails(self, context):
        df = pd.DataFrame(self._base(sigma_log=[0.8, 0.0, 0.87]))  # o perfil aplica piso de 0,3
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_night_share_above_one_fails(self, context):
        df = pd.DataFrame(self._base(night_share=[0.1, 1.4, 1.0]))
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_swapped_latitude_and_longitude_fail(self, context):
        df = pd.DataFrame(self._base(home_lat=[-46.63, -38.50, None], home_lon=[-23.55, -12.97, None]))
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_customer_with_a_profile_but_no_coordinates_fails(self, context):
        df = pd.DataFrame(self._base(home_lat=[-23.55, None, None]))
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_has_profile_must_agree_with_n_history(self, context):
        df = pd.DataFrame(self._base(has_profile=[True, True, True]))  # o "new" tem n_history = 0
        _run_invalid(context, "gold_customer_behavior_profile", df)
        df = pd.DataFrame(self._base(has_profile=[False, True, False]))  # o "c1" tem 48 transações
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_missing_column_fails(self, context):
        df = pd.DataFrame(self._base()).drop(columns=["known_devices"])
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_empty_table_fails(self, context):
        df = pd.DataFrame(self._base()).iloc[0:0]
        _run_invalid(context, "gold_customer_behavior_profile", df)

    def test_thresholds_follow_the_profile_module(self):
        """A suite e o `build_profiles` compartilham o piso de σ e o mínimo de histórico."""
        from src.governance.great_expectations.suites import PROFILE_SIGMA_LOG_RANGE
        from src.transformation.fraud.profile import SIGMA_FLOOR

        assert PROFILE_SIGMA_LOG_RANGE[0] == SIGMA_FLOOR


# ── Gates opcionais (dados de mercado) ───────────────────────────────────────


class TestOptionalGates:
    _INVALID_MARKET = {
        "symbol": ["PETR4.SA", "VALE3.SA"],
        "date": pd.to_datetime(["2024-06-14", "2024-06-15"]),
        "open": [30.0, 60.0],
        "high": [28.0, 61.0],  # high < low na 1a linha
        "low": [29.0, 59.0],
        "close": [30.5, 60.5],
        "volume": [1000, 2000],
        "adjusted_close": [30.5, 60.5],
    }

    def test_only_market_datasets_are_optional(self):
        assert OPTIONAL_DATASETS == {"bronze_market_data", "silver_market_data"}

    def test_optional_gate_swallows_expectation_failure(self, context):
        result = run_gate_optional(
            "bronze_market_data", df=pd.DataFrame(self._INVALID_MARKET), context=context
        )
        assert result is None

    def test_optional_gate_swallows_missing_data(self, context):
        with patch.object(runner, "load_dataframe", side_effect=FileNotFoundError("sem parquet")):
            assert run_gate_optional("silver_market_data", context=context) is None

    def test_mandatory_gate_still_raises_on_missing_data(self, context):
        with patch.object(runner, "load_dataframe", side_effect=FileNotFoundError("sem parquet")):
            with pytest.raises(FileNotFoundError):
                run_gate("silver_transactions", context=context)

    def test_cli_all_exits_zero_when_only_optional_gates_fail(self):
        def fake_gate(key, **_):
            if key in OPTIONAL_DATASETS:
                raise FileNotFoundError("sem parquet")
            return {"success": True}

        with patch.object(runner, "run_gate", side_effect=fake_gate):
            runner.main(["--all"])  # não deve levantar SystemExit

    def test_cli_all_exits_one_when_mandatory_gate_fails(self):
        def fake_gate(key, **_):
            if key == "silver_transactions":
                raise QualityGateFailed("falhou")
            return {"success": True}

        with patch.object(runner, "run_gate", side_effect=fake_gate):
            with pytest.raises(SystemExit) as exc:
                runner.main(["--all"])
        assert exc.value.code == 1
