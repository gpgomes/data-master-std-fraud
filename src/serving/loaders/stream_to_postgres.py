"""Job PySpark: carga da saída do streaming para a serving layer PostgreSQL (issue #38).

Lê `silver/transactions_stream/` (Parquet escrito pelo detector de fraude, issue #11) e
carrega duas tabelas: `stream_scored_transactions` (transações pontuadas, com `fraud_score`
e latência evento→processamento) e `fraud_alerts` (os alertas do detector).

Por que ler o Parquet e não consumir o Kafka: o streaming já ocupa todos os cores do cluster
Spark local, então um segundo job de streaming não teria recursos; o Parquet é o registro
durável e, como o `alert_id` é determinístico, os alertas são reconstruídos com a mesma
função do detector (`build_fraud_alerts`), dando exatamente o conteúdo do tópico `fraud-alerts`.

Sem dado de streaming (o job de streaming nunca rodou) o loader não falha: pula a carga com
um aviso, para poder fazer parte da DAG batch.

Execução via CLI:
    python -m src.serving.loaders.stream_to_postgres
    python -m src.serving.loaders.stream_to_postgres --dataset fraud_alerts
    python -m src.serving.loaders.stream_to_postgres --local
"""

from __future__ import annotations

import argparse
import sys

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.config import settings
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session
from src.serving.loaders.gold_to_postgres import GoldToPostgresLoader
from src.transformation.streaming.stream_processor import build_fraud_alerts

logger = get_logger("stream_to_postgres")

_DATASETS = ["stream_scored_transactions", "fraud_alerts"]
_EMPTY = {"rows_read": 0, "rows_written": 0}


class StreamToPostgresLoader(GoldToPostgresLoader):
    """Carrega a saída do streaming no PostgreSQL (truncate + reload por tabela).

    Herda `ensure_schema` e as opções JDBC de `GoldToPostgresLoader`: o DDL das duas tabelas
    novas está no mesmo `schema.sql`, aplicado de forma idempotente antes de cada carga.

    Args:
        spark: SparkSession ativa (injetada externamente para facilitar testes).
    """

    def __init__(self, spark: SparkSession) -> None:
        super().__init__(spark)
        self._silver = f"s3a://{settings.minio.bucket_silver}"

    # ── Leitura e transformação ─────────────────────────────────────────────────

    def _read_scored(self) -> DataFrame | None:
        """Lê o Parquet do streaming, com uma linha por `transaction_id`. Devolve None se o
        streaming nunca rodou (prefixo inexistente)."""
        path = f"{self._silver}/transactions_stream/"
        logger.info("Lendo saída do streaming para a serving layer", path=path)
        try:
            df = self.spark.read.parquet(path)
        except AnalysisException:
            logger.warning("Sem saída de streaming no Silver — carga pulada", path=path)
            return None
        return self._keep_latest_per_transaction(df)

    @staticmethod
    def _keep_latest_per_transaction(df: DataFrame) -> DataFrame:
        """Reprocessar o Kafka do início com outro checkpoint repete `transaction_id` em
        partições `query_id` diferentes; a linha mais recente vence."""
        window = Window.partitionBy("transaction_id").orderBy(F.col("processing_timestamp").desc())
        return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")

    @staticmethod
    def _to_scored_table(df: DataFrame) -> DataFrame:
        latency = F.col("processing_timestamp").cast("double") - F.col("produced_at").cast("double")
        return df.select(
            "transaction_id",
            "customer_id",
            F.col("timestamp").alias("event_time"),
            "amount",
            "currency",
            "transaction_type",
            "channel",
            "merchant_category",
            "is_fraud",
            "fraud_type",
            "z_score",
            "fraud_score",
            F.round("fraud_score", 1).alias("fraud_score_bucket"),
            "is_anomaly",
            "produced_at",
            "processing_timestamp",
            F.round(latency, 3).alias("latency_seconds"),
        )

    @staticmethod
    def _to_alerts_table(df: DataFrame) -> DataFrame:
        return build_fraud_alerts(df).withColumnRenamed("timestamp", "event_time")

    # ── Carga ───────────────────────────────────────────────────────────────────

    def _write_table(self, df: DataFrame, table: str) -> dict[str, int]:
        rows = df.count()
        try:
            (
                df.write.format("jdbc")
                .options(**self._jdbc_write_options(table))
                .mode("overwrite")
                .save()
            )
        except Exception:
            logger.exception("Falha ao carregar tabela na serving layer", table=table)
            raise
        metrics = {"rows_read": rows, "rows_written": rows}
        logger.info(f"load_{table} concluído", **metrics)
        return metrics

    def load_stream_scored_transactions(self) -> dict[str, int]:
        scored = self._read_scored()
        if scored is None:
            return dict(_EMPTY)
        return self._write_table(self._to_scored_table(scored), "stream_scored_transactions")

    def load_fraud_alerts(self) -> dict[str, int]:
        scored = self._read_scored()
        if scored is None:
            return dict(_EMPTY)
        return self._write_table(self._to_alerts_table(scored), "fraud_alerts")

    def run_all(self) -> dict[str, dict[str, int]]:
        """Garante o schema e carrega as duas tabelas, retornando métricas."""
        self.ensure_schema()
        return {
            "stream_scored_transactions": self.load_stream_scored_transactions(),
            "fraud_alerts": self.load_fraud_alerts(),
        }


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carrega a saída do streaming para a serving layer PostgreSQL."
    )
    parser.add_argument(
        "--dataset",
        choices=["all", *_DATASETS],
        default="all",
        help="Tabela a carregar (padrão: all).",
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

    spark = create_spark_session(app_name="stream_to_postgres", local_mode=args.local)
    loader = StreamToPostgresLoader(spark=spark)

    if args.dataset == "all":
        metrics = loader.run_all()
    else:
        loader.ensure_schema()
        metrics = {args.dataset: getattr(loader, f"load_{args.dataset}")()}

    logger.info("Job concluído", metrics=metrics)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
