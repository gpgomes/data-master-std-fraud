"""Testes unitários para os módulos de ingestão batch."""

from __future__ import annotations

import io
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.batch.customer_loader import CustomerLoader
from src.ingestion.batch.market_data_collector import MarketDataCollector
from src.ingestion.batch.transaction_loader import TransactionLoader


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.check_exists.return_value = False
    storage.upload_parquet_bytes.return_value = None
    return storage


@pytest.fixture
def sample_ohlcv_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "Open": [37.50, 38.00],
            "High": [38.20, 38.50],
            "Low": [37.30, 37.80],
            "Close": [38.00, 38.40],
            "Volume": [1_000_000, 1_200_000],
        }
    )


@pytest.fixture
def sample_transactions_csv(tmp_path: Path) -> Path:
    """Cria estrutura de CSV de transações em um diretório temporário."""
    csv_dir = tmp_path / "2024" / "01" / "15"
    csv_dir.mkdir(parents=True)
    csv_file = csv_dir / "transactions.csv"
    df = pd.DataFrame(
        {
            "transaction_id": ["tx-001", "tx-002"],
            "customer_id": ["cust-001", "cust-002"],
            "amount": [100.0, 200.0],
            "timestamp": ["2024-01-15T10:00:00", "2024-01-15T11:00:00"],
        }
    )
    df.to_csv(csv_file, index=False)
    return tmp_path


@pytest.fixture
def sample_customers_csv(tmp_path: Path) -> Path:
    """Cria CSV de clientes em um diretório temporário."""
    customers_dir = tmp_path / "customers"
    customers_dir.mkdir(parents=True)
    csv_file = customers_dir / "customers.csv"
    df = pd.DataFrame(
        {
            "customer_id": ["cust-001", "cust-002"],
            "name": ["João Silva", "Maria Santos"],
            "risk_score": [25.0, 70.0],
            "segment": ["VAREJO", "ALTA_RENDA"],
        }
    )
    df.to_csv(csv_file, index=False)
    return csv_file


# ── MarketDataCollector ────────────────────────────────────────────────────────


class TestMarketDataCollector:
    def test_normalize_adds_required_columns(self, sample_ohlcv_df):
        df = MarketDataCollector._normalize(sample_ohlcv_df, "PETR4.SA")

        assert "symbol" in df.columns
        assert "date" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert "ingestion_timestamp" in df.columns
        assert "source_system" in df.columns
        assert (df["symbol"] == "PETR4.SA").all()
        assert (df["source_system"] == "yfinance").all()

    def test_normalize_date_format(self, sample_ohlcv_df):
        df = MarketDataCollector._normalize(sample_ohlcv_df, "VALE3.SA")
        assert df["date"].iloc[0] == "2024-01-02"

    def test_collect_daily_skips_existing_partitions(self, mock_storage):
        mock_storage.check_exists.return_value = True

        collector = MarketDataCollector(
            storage=mock_storage,
            tickers=["PETR4.SA"],
        )

        with patch.object(collector, "_fetch_ohlcv") as mock_fetch:
            mock_fetch.return_value = pd.DataFrame(
                {
                    "Date": pd.to_datetime(["2024-01-02"]),
                    "Open": [37.50],
                    "High": [38.20],
                    "Low": [37.30],
                    "Close": [38.00],
                    "Volume": [1_000_000],
                }
            )
            results = collector.collect_daily(n_days=1)

        # Partição já existe → não deve chamar upload
        mock_storage.upload_parquet_bytes.assert_not_called()
        assert results["PETR4.SA"] == 1

    def test_collect_daily_uploads_new_partitions(self, mock_storage, sample_ohlcv_df):
        mock_storage.check_exists.return_value = False

        collector = MarketDataCollector(
            storage=mock_storage,
            tickers=["PETR4.SA"],
        )

        with patch.object(collector, "_fetch_ohlcv", return_value=sample_ohlcv_df):
            results = collector.collect_daily(n_days=5)

        assert results["PETR4.SA"] == 2
        assert mock_storage.upload_parquet_bytes.call_count == 2

    def test_collect_daily_handles_empty_response(self, mock_storage):
        collector = MarketDataCollector(storage=mock_storage, tickers=["XXXX.SA"])

        with patch.object(collector, "_fetch_ohlcv", return_value=pd.DataFrame()):
            results = collector.collect_daily(n_days=5)

        assert results["XXXX.SA"] == 0
        mock_storage.upload_parquet_bytes.assert_not_called()

    def test_collect_daily_handles_exception(self, mock_storage):
        collector = MarketDataCollector(storage=mock_storage, tickers=["ERROR.SA"])

        with patch.object(collector, "_fetch_ohlcv", side_effect=RuntimeError("API down")):
            results = collector.collect_daily(n_days=1)

        assert results["ERROR.SA"] == 0


# ── TransactionLoader ──────────────────────────────────────────────────────────


class TestTransactionLoader:
    def test_load_to_bronze_ingests_csv(self, mock_storage, sample_transactions_csv):
        loader = TransactionLoader(storage=mock_storage, source_dir=sample_transactions_csv)
        results = loader.load_to_bronze()

        assert mock_storage.upload_parquet_bytes.call_count == 1
        total = sum(v for v in results.values() if v > 0)
        assert total == 2  # 2 linhas no CSV de exemplo

    def test_load_to_bronze_skips_existing(self, mock_storage, sample_transactions_csv):
        mock_storage.check_exists.return_value = True

        loader = TransactionLoader(storage=mock_storage, source_dir=sample_transactions_csv)
        results = loader.load_to_bronze()

        mock_storage.upload_parquet_bytes.assert_not_called()
        assert results[list(results.keys())[0]] == 0

    def test_load_to_bronze_empty_dir(self, mock_storage, tmp_path):
        empty_dir = tmp_path / "transactions"
        empty_dir.mkdir()

        loader = TransactionLoader(storage=mock_storage, source_dir=empty_dir)
        results = loader.load_to_bronze()

        assert results == {}
        mock_storage.upload_parquet_bytes.assert_not_called()

    def test_add_metadata_adds_columns(self, mock_storage):
        loader = TransactionLoader(storage=mock_storage)
        df = pd.DataFrame({"transaction_id": ["tx-001"], "amount": [100.0]})

        enriched = loader._add_metadata(df, source_file="test.csv")

        assert "ingestion_timestamp" in enriched.columns
        assert "source_file" in enriched.columns
        assert "batch_id" in enriched.columns
        assert enriched["source_file"].iloc[0] == "test.csv"

    def test_build_key_partitioned(self, mock_storage, tmp_path):
        source_dir = tmp_path / "transactions"
        loader = TransactionLoader(storage=mock_storage, source_dir=source_dir)

        csv_path = source_dir / "2024" / "01" / "15" / "transactions.csv"
        key = loader._build_key(csv_path)

        assert "year=2024" in key
        assert "month=01" in key
        assert "day=15" in key
        assert key.endswith(".parquet")

    def test_build_key_fallback(self, mock_storage, tmp_path):
        loader = TransactionLoader(storage=mock_storage, source_dir=tmp_path / "transactions")
        csv_path = tmp_path / "some_file.csv"
        key = loader._build_key(csv_path)

        assert "transactions/" in key
        assert key.endswith(".parquet")


# ── CustomerLoader ─────────────────────────────────────────────────────────────


class TestCustomerLoader:
    def test_load_to_bronze_ingests_customers(self, mock_storage, sample_customers_csv):
        loader = CustomerLoader(storage=mock_storage, source_file=sample_customers_csv)
        count = loader.load_to_bronze()

        assert count == 2
        mock_storage.upload_parquet_bytes.assert_called_once()

    def test_load_to_bronze_skips_existing(self, mock_storage, sample_customers_csv):
        mock_storage.check_exists.return_value = True

        loader = CustomerLoader(storage=mock_storage, source_file=sample_customers_csv)
        count = loader.load_to_bronze()

        assert count == 0
        mock_storage.upload_parquet_bytes.assert_not_called()

    def test_load_to_bronze_missing_file(self, mock_storage, tmp_path):
        loader = CustomerLoader(
            storage=mock_storage,
            source_file=tmp_path / "nonexistent.csv",
        )
        count = loader.load_to_bronze()

        assert count == 0
        mock_storage.upload_parquet_bytes.assert_not_called()

    def test_apply_scd2_adds_columns(self, mock_storage):
        loader = CustomerLoader(storage=mock_storage)
        df = pd.DataFrame({"customer_id": ["cust-001"], "name": ["João"]})

        result = loader._apply_scd2(df)

        assert "valid_from" in result.columns
        assert "valid_to" in result.columns
        assert "is_current" in result.columns
        assert "ingestion_timestamp" in result.columns
        assert "batch_id" in result.columns
        assert result["is_current"].iloc[0] == True  # noqa: E712 — numpy bool
        assert result["valid_to"].iloc[0] == "9999-12-31"

    def test_build_key_contains_today(self, mock_storage):
        loader = CustomerLoader(storage=mock_storage)
        key = loader._build_key()
        today = date.today().isoformat()

        assert today in key
        assert key.startswith("customers/")
        assert key.endswith(".parquet")
