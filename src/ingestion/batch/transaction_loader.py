"""Carrega CSVs de transações do diretório local e faz upload para Bronze (MinIO)."""

from __future__ import annotations

import io
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from src.common.config import settings
from src.common.logger import get_logger
from src.common.storage import MinIOClient, get_storage_client

logger = get_logger("transaction_loader")

_DEFAULT_SAMPLE_DIR = Path("data/sample/transactions")
_BRONZE_PREFIX = "transactions"


class TransactionLoader:
    """Lê CSVs de transações do diretório local e persiste como Parquet no Bronze.

    Idempotência: verifica se a chave já existe no MinIO antes de re-ingerir.
    Particionamento: year=YYYY/month=MM/day=DD/
    """

    def __init__(
        self,
        storage: Optional[MinIOClient] = None,
        source_dir: Optional[Path] = None,
        bucket: Optional[str] = None,
    ) -> None:
        self._storage = storage or get_storage_client()
        self._source_dir = source_dir or _DEFAULT_SAMPLE_DIR
        self._bucket = bucket or settings.minio.bucket_bronze
        self._batch_id = str(uuid.uuid4())

    # ── API pública ────────────────────────────────────────────────────────────

    def load_to_bronze(self) -> dict[str, int]:
        """Lê todos os CSVs de transações e persiste no Bronze.

        Estrutura esperada em source_dir:
            transactions/YYYY/MM/DD/transactions.csv

        Returns:
            Dicionário {arquivo: linhas_ingeridas}.
        """
        csv_files = sorted(self._source_dir.rglob("*.csv"))

        if not csv_files:
            logger.warning("Nenhum CSV encontrado", source_dir=str(self._source_dir))
            return {}

        logger.info(
            "Iniciando carga de transações",
            batch_id=self._batch_id,
            csv_count=len(csv_files),
        )

        results: dict[str, int] = {}
        for csv_path in csv_files:
            key = self._build_key(csv_path)
            if self._storage.check_exists(self._bucket, key):
                logger.debug("Arquivo já ingerido, pulando", key=key)
                results[str(csv_path)] = 0
                continue

            try:
                count = self._ingest_file(csv_path, key)
                results[str(csv_path)] = count
            except Exception:
                logger.exception("Erro ao ingerir arquivo", file=str(csv_path))
                results[str(csv_path)] = -1

        total = sum(v for v in results.values() if v > 0)
        logger.info("Carga finalizada", batch_id=self._batch_id, total_records=total)
        return results

    # ── Implementação interna ──────────────────────────────────────────────────

    def _ingest_file(self, csv_path: Path, key: str) -> int:
        """Lê um CSV, adiciona metadados e envia como Parquet."""
        df = pd.read_csv(csv_path)
        df = self._add_metadata(df, source_file=str(csv_path))

        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        self._storage.upload_parquet_bytes(buf, self._bucket, key)

        logger.info("Arquivo ingerido", file=str(csv_path), key=key, rows=len(df))
        return len(df)

    def _add_metadata(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Adiciona colunas de auditoria ao DataFrame."""
        now = datetime.now(tz=timezone.utc).isoformat()
        df = df.copy()
        df["ingestion_timestamp"] = now
        df["source_file"] = source_file
        df["batch_id"] = self._batch_id
        return df

    def _build_key(self, csv_path: Path) -> str:
        """Constrói a chave MinIO a partir do caminho do CSV.

        Ex: data/sample/transactions/2024/01/15/transactions.csv
            → transactions/year=2024/month=01/day=15/transactions.parquet
        """
        # Extrai partes do caminho relativo ao source_dir
        try:
            rel = csv_path.relative_to(self._source_dir)
            parts = rel.parts  # ('2024', '01', '15', 'transactions.csv')
            if len(parts) >= 3:
                year, month, day = parts[0], parts[1], parts[2]
                return f"{_BRONZE_PREFIX}/year={year}/month={month}/day={day}/{csv_path.stem}.parquet"
        except ValueError:
            pass

        # Fallback: usa timestamp de ingestão
        now = datetime.now(tz=timezone.utc)
        return (
            f"{_BRONZE_PREFIX}/year={now.year}/month={now.month:02d}"
            f"/day={now.day:02d}/{csv_path.stem}.parquet"
        )
