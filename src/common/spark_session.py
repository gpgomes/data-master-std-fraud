"""Factory para SparkSession configurada com MinIO/S3."""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.common.config import settings
from src.common.logger import get_logger

logger = get_logger("spark_session")


def create_spark_session(
    app_name: str = "data-master-std-fraud",
    local_mode: bool = False,
) -> SparkSession:
    """Cria e retorna uma SparkSession configurada para a plataforma.

    Configurações incluem:
    - S3/MinIO connectivity via hadoop-aws (fs.s3a)
    - Configurações de memória para dev local
    - Suporte ao Kafka connector (structured streaming)

    Formato de armazenamento: Parquet puro (não Delta Lake) — decisão da
    issue #9. As camadas Silver e Gold são reescritas por completo a cada
    execução (idempotente via overwrite dinâmico por partição), então o
    log de transação ACID do Delta não é necessário no cenário atual;
    time travel/versionamento fica para a fase de governança (roadmap
    item 3.6), quando justificar a complexidade extra.

    Args:
        app_name: Nome da aplicação Spark.
        local_mode: Se True, usa modo local (dev/teste); caso contrário, usa
                    o master configurado em SparkSettings.

    Returns:
        SparkSession pronta para uso.
    """
    master = "local[*]" if local_mode else settings.spark.master_url

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # ── S3 / MinIO ──────────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio.internal_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # ── Memória e paralelismo (ajustável para cluster) ──────────────────────
        .config("spark.driver.memory", settings.spark.driver_memory)
        .config("spark.executor.memory", settings.spark.executor_memory)
        .config("spark.executor.cores", str(settings.spark.executor_cores))
        # Menos partições em dev — evita overhead de shuffle em datasets pequenos
        .config("spark.sql.shuffle.partitions", "8")
        # Overwrite dinâmico: grava apenas as partições presentes no DataFrame,
        # em vez de apagar o diretório inteiro (essencial para reprocessamento
        # por intervalo de datas em datasets particionados)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # ── Misc ────────────────────────────────────────────────────────────────
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "SparkSession criada",
        app_name=app_name,
        master=master,
        spark_version=spark.version,
    )
    return spark


def get_local_spark(app_name: str = "data-master-local") -> SparkSession:
    """Atalho para SparkSession em modo local (dev/testes)."""
    return create_spark_session(app_name=app_name, local_mode=True)
