"""Job PySpark: carga da camada Gold para a serving layer PostgreSQL (issue #10).

Execução via CLI:
    python -m src.serving.loaders.gold_to_postgres
    python -m src.serving.loaders.gold_to_postgres --dataset fact_transactions
    python -m src.serving.loaders.gold_to_postgres --local
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession

from src.common.config import settings
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session

logger = get_logger("gold_to_postgres")

_SCHEMA_SQL_PATH = Path(__file__).parent / "schema.sql"

_DATASETS = ["dim_customers", "dim_date", "fact_transactions", "agg_daily_fraud_metrics"]


class GoldToPostgresLoader:
    """Carrega as tabelas Gold (star schema) no PostgreSQL da serving layer.

    Estratégia de carga: truncate + reload completo por tabela a cada
    execução (mode="overwrite", truncate=True via JDBC) — sempre idempotente
    (reexecuções não duplicam dados), mas recarrega o Gold inteiro a cada
    run. Não há reprocessamento incremental por intervalo de datas nesta
    versão (V1); os volumes atuais (maior tabela ~500k linhas) não
    justificam a complexidade extra de um merge/upsert real via JDBC.

    Args:
        spark: SparkSession ativa (injetada externamente para facilitar testes).
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._gold = f"s3a://{settings.minio.bucket_gold}"
        self._jdbc_url = (
            f"jdbc:postgresql://{settings.postgres.internal_host}:"
            f"{settings.postgres.port}/{settings.postgres.db}"
        )

    # ── Bootstrap de schema ────────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Garante que as tabelas da serving layer existem (DDL idempotente).

        Roda fora do Spark (psycopg2 direto) — é só DDL, não movimentação de
        dado, e precisa existir antes do primeiro `truncate=true` do Spark
        JDBC writer (que não cria tabela, só trunca uma já existente).
        """
        ddl = _SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        conn = psycopg2.connect(
            host=settings.postgres.internal_host,
            port=settings.postgres.port,
            dbname=settings.postgres.db,
            user=settings.postgres.user,
            password=settings.postgres.password,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
            logger.info("Schema da serving layer garantido (CREATE TABLE IF NOT EXISTS)")
        except Exception:
            conn.rollback()
            logger.exception("Falha ao aplicar DDL da serving layer — conexão ou SQL inválido")
            raise
        finally:
            conn.close()

    # ── Carga genérica ──────────────────────────────────────────────────────────

    def _jdbc_write_options(self, table: str) -> dict[str, str]:
        return {
            "url": self._jdbc_url,
            "dbtable": table,
            "user": settings.postgres.user,
            "password": settings.postgres.password,
            "driver": "org.postgresql.Driver",
            "truncate": "true",
        }

    def _load_table(self, dataset: str, table: str) -> dict[str, int]:
        """Lê uma tabela Gold do MinIO e recarrega no Postgres (truncate + reload)."""
        path = f"{self._gold}/{dataset}/"
        logger.info("Lendo Gold para carga na serving layer", dataset=dataset, path=path)

        try:
            df = self.spark.read.parquet(path)
            rows_read = df.count()
        except Exception:
            logger.exception("Falha ao ler Gold do MinIO", dataset=dataset, path=path)
            raise
        logger.info("Registros lidos", dataset=dataset, count=rows_read)

        try:
            (
                df.write.format("jdbc")
                .options(**self._jdbc_write_options(table))
                .mode("overwrite")
                .save()
            )
        except Exception:
            logger.exception(
                "Falha ao carregar tabela na serving layer — conexão ou dados inválidos",
                dataset=dataset,
                table=table,
            )
            raise

        metrics = {"rows_read": rows_read, "rows_written": rows_read}
        logger.info(f"load_{dataset} concluído", **metrics, table=table)
        return metrics

    def load_dim_customers(self) -> dict[str, int]:
        return self._load_table("dim_customers", "dim_customers")

    def load_dim_date(self) -> dict[str, int]:
        return self._load_table("dim_date", "dim_date")

    def load_fact_transactions(self) -> dict[str, int]:
        return self._load_table("fact_transactions", "fact_transactions")

    def load_agg_daily_fraud_metrics(self) -> dict[str, int]:
        return self._load_table("agg_daily_fraud_metrics", "agg_daily_fraud_metrics")

    # ── Execução completa ────────────────────────────────────────────────────────

    def run_all(self) -> dict[str, dict[str, int]]:
        """Garante o schema e carrega as 4 tabelas Gold, retornando métricas."""
        self.ensure_schema()
        return {
            "dim_customers": self.load_dim_customers(),
            "dim_date": self.load_dim_date(),
            "fact_transactions": self.load_fact_transactions(),
            "agg_daily_fraud_metrics": self.load_agg_daily_fraud_metrics(),
        }


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carrega dados Gold para a serving layer PostgreSQL (PySpark batch job)."
    )
    parser.add_argument(
        "--dataset",
        choices=["all", *_DATASETS],
        default="all",
        help="Dataset a carregar (padrão: all).",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        default=False,
        help="Usa SparkSession em modo local (dev).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    spark = create_spark_session(
        app_name="gold_to_postgres",
        local_mode=args.local,
    )

    loader = GoldToPostgresLoader(spark=spark)

    dataset = args.dataset
    if dataset == "all":
        metrics = loader.run_all()
    else:
        loader.ensure_schema()
        method = getattr(loader, f"load_{dataset}")
        metrics = {dataset: method()}

    logger.info("Job concluído", metrics=metrics)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
