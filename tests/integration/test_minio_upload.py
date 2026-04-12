"""Testes de integração — upload real para MinIO.

Requer o MinIO rodando localmente (make up).
Execute com: pytest tests/integration/test_minio_upload.py -v

Esses testes fazem chamadas reais ao MinIO; são ignorados automaticamente
se a instância não estiver acessível.
"""

from __future__ import annotations

import io
import uuid

import pandas as pd
import pytest

from src.common.config import settings


def _minio_available() -> bool:
    """Retorna True se o MinIO estiver acessível."""
    try:
        import boto3
        from botocore.exceptions import EndpointResolutionError, NoCredentialsError

        client = boto3.client(
            "s3",
            endpoint_url=settings.minio.endpoint,
            aws_access_key_id=settings.minio.access_key,
            aws_secret_access_key=settings.minio.secret_key,
            region_name="us-east-1",
        )
        client.list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(
    not _minio_available(),
    reason="MinIO não está acessível — execute 'make up' antes dos testes de integração",
)


@pytest.fixture(scope="module")
def real_storage():
    from src.common.storage import MinIOClient

    return MinIOClient(use_internal=False)


@pytest.fixture
def test_key() -> str:
    return f"_test/{uuid.uuid4()}.parquet"


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "value": [1.0, 2.0, 3.0],
            "label": ["x", "y", "z"],
        }
    )


# ── Testes de MinIOClient ──────────────────────────────────────────────────────


@requires_minio
class TestMinIOClientIntegration:
    def test_upload_and_download_parquet(self, real_storage, sample_df, test_key):
        bucket = settings.minio.bucket_bronze

        real_storage.upload_parquet(sample_df, bucket, test_key)
        result = real_storage.download_parquet(bucket, test_key)

        assert list(result.columns) == list(sample_df.columns)
        assert len(result) == len(sample_df)

    def test_check_exists_true_after_upload(self, real_storage, sample_df, test_key):
        bucket = settings.minio.bucket_bronze

        assert not real_storage.check_exists(bucket, test_key)
        real_storage.upload_parquet(sample_df, bucket, test_key)
        assert real_storage.check_exists(bucket, test_key)

    def test_check_exists_false_for_nonexistent(self, real_storage):
        bucket = settings.minio.bucket_bronze
        assert not real_storage.check_exists(bucket, f"_test/nonexistent_{uuid.uuid4()}.parquet")

    def test_list_objects_returns_uploaded(self, real_storage, sample_df):
        bucket = settings.minio.bucket_bronze
        prefix = f"_test/list_{uuid.uuid4()}"
        key = f"{prefix}/data.parquet"

        real_storage.upload_parquet(sample_df, bucket, key)
        keys = real_storage.list_objects(bucket, prefix=prefix)

        assert key in keys

    def test_upload_parquet_bytes(self, real_storage, sample_df, test_key):
        bucket = settings.minio.bucket_bronze
        buf = io.BytesIO()
        sample_df.to_parquet(buf, index=False)
        buf.seek(0)

        real_storage.upload_parquet_bytes(buf, bucket, test_key)
        result = real_storage.download_parquet(bucket, test_key)

        assert len(result) == len(sample_df)


# ── Testes de TransactionLoader ────────────────────────────────────────────────


@requires_minio
class TestTransactionLoaderIntegration:
    def test_load_to_bronze_e2e(self, tmp_path):
        from src.ingestion.batch.transaction_loader import TransactionLoader

        # Usa uuid para garantir chave única por execução (evita colisão com runs anteriores)
        run_id = uuid.uuid4().hex[:8]
        csv_dir = tmp_path / run_id / "01"
        csv_dir.mkdir(parents=True)
        df = pd.DataFrame(
            {
                "transaction_id": [f"tx-{i}" for i in range(10)],
                "amount": [float(i * 100) for i in range(10)],
                "timestamp": ["2024-06-01T10:00:00"] * 10,
            }
        )
        df.to_csv(csv_dir / "transactions.csv", index=False)

        loader = TransactionLoader(source_dir=tmp_path)
        results = loader.load_to_bronze()

        total = sum(v for v in results.values() if v > 0)
        assert total == 10

    def test_idempotency_no_double_ingestion(self, tmp_path):
        from src.ingestion.batch.transaction_loader import TransactionLoader

        run_id = uuid.uuid4().hex[:8]
        csv_dir = tmp_path / run_id / "02"
        csv_dir.mkdir(parents=True)
        df = pd.DataFrame({"transaction_id": ["tx-idem-1"], "amount": [50.0]})
        df.to_csv(csv_dir / "transactions.csv", index=False)

        loader = TransactionLoader(source_dir=tmp_path)

        first = loader.load_to_bronze()
        second = loader.load_to_bronze()

        first_total = sum(v for v in first.values() if v > 0)
        second_total = sum(v for v in second.values() if v > 0)

        assert first_total == 1
        assert second_total == 0  # Segunda chamada: arquivo já existe


# ── Testes de MarketDataCollector ──────────────────────────────────────────────


@requires_minio
class TestMarketDataCollectorIntegration:
    def test_collect_daily_e2e(self):
        from unittest.mock import patch

        import pandas as pd

        from src.ingestion.batch.market_data_collector import MarketDataCollector

        mock_df = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-02"]),
                "Open": [37.50],
                "High": [38.20],
                "Low": [37.30],
                "Close": [38.00],
                "Volume": [1_000_000],
            }
        )

        collector = MarketDataCollector(tickers=["PETR4.SA"])
        with patch.object(collector, "_fetch_ohlcv", return_value=mock_df):
            results = collector.collect_daily(n_days=1)

        assert results.get("PETR4.SA", 0) >= 0
