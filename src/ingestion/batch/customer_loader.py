"""Carrega dados dimensionais de clientes para o Bronze com SCD Type 2 simplificado."""

from __future__ import annotations

import io
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from src.common.config import settings
from src.common.logger import get_logger
from src.common.storage import MinIOClient, get_storage_client

logger = get_logger("customer_loader")

_DEFAULT_CUSTOMERS_FILE = Path("data/sample/customers/customers.csv")
_BRONZE_PREFIX = "customers"
_SCD2_DATE_MAX = "9999-12-31"


class CustomerLoader:
    """Lê CSV de clientes e persiste no Bronze com controle SCD Type 2 simplificado.

    SCD Type 2 simplificado:
        - valid_from: data de ingestão do registro.
        - valid_to: '9999-12-31' para o registro atual.
        - is_current: True para a versão vigente.
    """

    def __init__(
        self,
        storage: Optional[MinIOClient] = None,
        source_file: Optional[Path] = None,
        bucket: Optional[str] = None,
    ) -> None:
        self._storage = storage or get_storage_client()
        self._source_file = source_file or _DEFAULT_CUSTOMERS_FILE
        self._bucket = bucket or settings.minio.bucket_bronze
        self._batch_id = str(uuid.uuid4())

    # ── API pública ────────────────────────────────────────────────────────────

    def load_to_bronze(self) -> int:
        """Lê o CSV de clientes, aplica SCD2 e persiste no Bronze.

        Returns:
            Número de registros ingeridos.
        """
        if not self._source_file.exists():
            logger.warning("Arquivo de clientes não encontrado", file=str(self._source_file))
            return 0

        key = self._build_key()

        if self._storage.check_exists(self._bucket, key):
            logger.info("Snapshot de clientes já existe, pulando", key=key)
            return 0

        df = pd.read_csv(self._source_file)
        df = self._apply_scd2(df)

        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        self._storage.upload_parquet_bytes(buf, self._bucket, key)

        logger.info(
            "Clientes ingeridos no Bronze",
            key=key,
            records=len(df),
            batch_id=self._batch_id,
        )
        return len(df)

    # ── Implementação interna ──────────────────────────────────────────────────

    def _apply_scd2(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas de controle SCD Type 2 e metadados de ingestão."""
        today = date.today().isoformat()
        now = datetime.now(tz=timezone.utc).isoformat()

        df = df.copy()
        df["valid_from"] = today
        df["valid_to"] = _SCD2_DATE_MAX
        df["is_current"] = True
        df["ingestion_timestamp"] = now
        df["batch_id"] = self._batch_id
        return df

    def _build_key(self) -> str:
        """Gera chave MinIO com snapshot diário para rastreabilidade."""
        today = date.today().isoformat()
        return f"{_BRONZE_PREFIX}/snapshot_date={today}/customers.parquet"
