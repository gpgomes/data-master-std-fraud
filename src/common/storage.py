"""Wrapper MinIO/S3 para operações de storage na plataforma."""

from __future__ import annotations

import io

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.common.config import settings
from src.common.logger import get_logger

logger = get_logger("storage")


class MinIOClient:
    """Wrapper boto3 para MinIO local ou S3 na AWS.

    Configurável via variáveis de ambiente — usa MinIOSettings de config.py.
    Em produção (AWS), basta apontar endpoint para o S3 ou remover o parâmetro.
    """

    def __init__(self, use_internal: bool = False) -> None:
        endpoint = settings.minio.internal_endpoint if use_internal else settings.minio.endpoint
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=settings.minio.access_key,
            aws_secret_access_key=settings.minio.secret_key,
            region_name="us-east-1",
        )
        logger.debug("MinIOClient inicializado", endpoint=endpoint)

    # ── Upload ──────────────────────────────────────────────────────────────────

    def upload_parquet(
        self,
        df: pd.DataFrame,
        bucket: str,
        key: str,
        partition_cols: list[str] | None = None,
    ) -> None:
        """Serializa DataFrame como Parquet e faz upload para o bucket.

        Args:
            df: DataFrame a ser enviado.
            bucket: Nome do bucket (ex: 'bronze').
            key: Caminho do objeto dentro do bucket (ex: 'market_data/date=2024-01-01/data.parquet').
            partition_cols: Se informado, usa pyarrow para escrever com particionamento
                            (neste caso `key` é tratado como prefixo de diretório).
        """
        if partition_cols:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.Table.from_pandas(df)
            buf = io.BytesIO()
            pq.write_to_dataset(
                table,
                root_path=f"s3://{bucket}/{key}",
                partition_cols=partition_cols,
                filesystem=self._get_pyarrow_fs(),
            )
            logger.info("Parquet particionado enviado", bucket=bucket, prefix=key, rows=len(df))
        else:
            buf = io.BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow")
            buf.seek(0)
            self._client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buf.getvalue(),
                ContentType="application/octet-stream",
            )
            logger.info("Parquet enviado", bucket=bucket, key=key, rows=len(df))

    def upload_parquet_bytes(self, buf: io.BytesIO, bucket: str, key: str) -> None:
        """Envia bytes Parquet já serializados diretamente."""
        buf.seek(0)
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.debug("Parquet bytes enviados", bucket=bucket, key=key)

    # ── Download ────────────────────────────────────────────────────────────────

    def download_parquet(self, bucket: str, key: str) -> pd.DataFrame:
        """Baixa objeto Parquet do bucket e retorna como DataFrame.

        Args:
            bucket: Nome do bucket.
            key: Caminho do objeto.

        Returns:
            DataFrame com os dados desserializados.
        """
        response = self._client.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(response["Body"].read())
        df = pd.read_parquet(buf)
        logger.debug("Parquet baixado", bucket=bucket, key=key, rows=len(df))
        return df

    # ── Listagem ────────────────────────────────────────────────────────────────

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        """Lista chaves de objetos no bucket com o prefixo dado.

        Args:
            bucket: Nome do bucket.
            prefix: Prefixo de filtro (ex: 'transactions/2024/').

        Returns:
            Lista de chaves encontradas.
        """
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    # ── Verificação ─────────────────────────────────────────────────────────────

    def check_exists(self, bucket: str, key: str) -> bool:
        """Verifica se um objeto existe no bucket.

        Args:
            bucket: Nome do bucket.
            key: Chave do objeto.

        Returns:
            True se o objeto existe, False caso contrário.
        """
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise

    # ── Helpers privados ────────────────────────────────────────────────────────

    def _get_pyarrow_fs(self):
        """Retorna filesystem pyarrow-s3 configurado para MinIO."""
        import pyarrow.fs as pafs

        return pafs.S3FileSystem(
            endpoint_override=settings.minio.endpoint.replace("http://", "").replace("https://", ""),
            access_key=settings.minio.access_key,
            secret_key=settings.minio.secret_key,
            scheme="http",
        )


def get_storage_client(use_internal: bool = False) -> MinIOClient:
    """Factory — retorna instância configurada de MinIOClient."""
    return MinIOClient(use_internal=use_internal)
