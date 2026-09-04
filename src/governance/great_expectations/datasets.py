"""Carrega os datasets Bronze/Silver/Gold do MinIO como DataFrames pandas
para validação pelo Great Expectations (issue #13).

Via pandas + s3fs, não PySpark — mesmo padrão já usado pelas tasks de
validação do Airflow (`dag_batch_ingestion.py`), que rodam como
PythonOperator simples, sem precisar de um spark-submit para uma checagem de
qualidade. `settings.minio.endpoint` (não `.internal_endpoint`) porque os
serviços Airflow já recebem `MINIO_ENDPOINT=http://minio:9000` via
`docker-compose.yml` — mesma convenção do resto do código que roda lá dentro.
"""

from __future__ import annotations

import re

import fsspec
import pandas as pd

from src.common.config import settings

_HIVE_PARTITION_RE = re.compile(r"([^/=]+)=([^/]+)")

# dataset_key -> caminho relativo dentro do bucket correspondente
_BRONZE_PATHS = {
    "bronze_transactions": "transactions/",
    "bronze_market_data": "market_data/",
}
_SILVER_PATHS = {
    "silver_transactions": "transactions/",
    "silver_market_data": "market_data/",
}
_GOLD_PATHS = {
    "gold_fact_transactions": "fact_transactions/",
    "gold_dim_customers": "dim_customers/",
    "gold_dim_date": "dim_date/",
    "gold_agg_daily_fraud_metrics": "agg_daily_fraud_metrics/",
}

DATASET_KEYS = tuple(
    key for mapping in (_BRONZE_PATHS, _SILVER_PATHS, _GOLD_PATHS) for key in mapping
)


def _storage_options() -> dict:
    return {
        "key": settings.minio.access_key,
        "secret": settings.minio.secret_key,
        "client_kwargs": {"endpoint_url": settings.minio.endpoint},
    }


def _resolve(dataset_key: str) -> str:
    if dataset_key in _BRONZE_PATHS:
        return f"s3://{settings.minio.bucket_bronze}/{_BRONZE_PATHS[dataset_key]}"
    if dataset_key in _SILVER_PATHS:
        return f"s3://{settings.minio.bucket_silver}/{_SILVER_PATHS[dataset_key]}"
    if dataset_key in _GOLD_PATHS:
        return f"s3://{settings.minio.bucket_gold}/{_GOLD_PATHS[dataset_key]}"
    raise ValueError(f"dataset_key desconhecido: {dataset_key!r} (válidos: {DATASET_KEYS})")


def _parse_hive_partitions(file_path: str) -> dict[str, str]:
    """Extrai pares chave=valor de segmentos Hive-style no caminho
    (`.../date_key=2024-06-15/part-000.parquet` -> {"date_key": "2024-06-15"})."""
    return dict(_HIVE_PARTITION_RE.findall(file_path))


def load_dataframe(dataset_key: str) -> pd.DataFrame:
    """Lê todos os arquivos Parquet do dataset (todas as partições) em um DataFrame.

    Lê cada arquivo individualmente pelo caminho exato e concatena, em vez de
    apontar `pd.read_parquet` para o diretório inteiro — dois motivos:

    1. Alguns datasets (`gold/fact_transactions`, `gold/agg_daily_fraud_metrics`,
       `silver/market_data`) são gravados com `.partitionBy(...)` do Spark, que
       por padrão NÃO duplica a coluna de partição dentro do arquivo (só fica
       codificada no nome do diretório, ex. `date_key=2024-06-15/`) — um
       `pd.read_parquet` no diretório reconstrói essa coluna automaticamente
       via descoberta de partição Hive, mas ler arquivo a arquivo (como fazemos
       aqui) não aciona essa descoberta, então precisamos reconstruir a coluna
       manualmente a partir do caminho (`_parse_hive_partitions`).
    2. Outros datasets (`bronze/market_data`) têm a mesma coluna gravada TANTO
       como partição QUANTO dentro do arquivo (`date`) — nesse caso o leitor
       de dataset particionado do pyarrow tenta unificar a versão
       dictionary-encoded (da partição) com a string do arquivo e falha
       (`ArrowTypeError: Unable to merge`). Ler arquivo a arquivo evita essa
       colisão; ao reconstruir a coluna de partição abaixo, só preenchemos
       quando ela ainda não existe no arquivo — a versão do arquivo (already
       validada/gerada pelo job) é sempre a autoritativa quando presente.

    Ler arquivo a arquivo e concatenar via pandas (em vez da unificação
    estrita do pyarrow) também tolera diferenças reais de schema entre
    arquivos de ingestões distintas.
    """
    path = _resolve(dataset_key)
    storage_options = _storage_options()
    fs = fsspec.filesystem("s3", **storage_options)

    bucket_and_prefix = path.removeprefix("s3://")
    files = [f for f in fs.find(bucket_and_prefix) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em {path}")

    frames = []
    for f in files:
        df = pd.read_parquet(f"s3://{f}", storage_options=storage_options)
        for key, value in _parse_hive_partitions(f).items():
            if key not in df.columns:
                df[key] = value
        frames.append(df)
    return pd.concat(frames, ignore_index=True)
