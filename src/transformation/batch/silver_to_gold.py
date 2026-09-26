"""Job PySpark: transformação Silver → Gold para a plataforma de dados financeiros.

Constrói um star schema (dim_customers, dim_date, fact_transactions), uma tabela de
agregação diária de métricas de fraude e o perfil de comportamento por cliente
(customer_behavior_profile, lido pelo detector de fraude do streaming), a partir da camada Silver.

Execução via CLI:
    python -m src.transformation.batch.silver_to_gold
    python -m src.transformation.batch.silver_to_gold --start-date 2024-01-01 --end-date 2024-01-31
    python -m src.transformation.batch.silver_to_gold --local --dataset fact_transactions
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.config import settings
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session
from src.transformation.fraud.profile import build_profiles

logger = get_logger("silver_to_gold")


class SilverToGoldTransformer:
    """Transforma dados da camada Silver para Gold (star schema + agregações).

    A dimensão de data (`dim_date`) é sempre reconstruída por completo a partir
    de todas as datas distintas presentes em `silver/transactions`, independente
    de `start_date`/`end_date` — garante que a FK `date_key` da fato nunca fique
    quebrada mesmo em reprocessamentos parciais.

    Args:
        spark: SparkSession ativa (injetada externamente para facilitar testes).
        start_date: Filtro de data inicial (YYYY-MM-DD), opcional.
        end_date: Filtro de data final (YYYY-MM-DD), opcional.
    """

    def __init__(
        self,
        spark: SparkSession,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        self.spark = spark
        self.start_date = start_date
        self.end_date = end_date
        self._silver = f"s3a://{settings.minio.bucket_silver}"
        self._gold = f"s3a://{settings.minio.bucket_gold}"

    # ── dim_customers ───────────────────────────────────────────────────────────

    def transform_dim_customers(self) -> dict[str, int]:
        """Lê clientes Silver e persiste a dimensão de clientes no Gold.

        Mantém apenas o registro corrente (`is_current=True`) de cada cliente.

        Returns:
            Dicionário com contadores: rows_read, rows_discarded, rows_written.
        """
        path = f"{self._silver}/customers/"
        logger.info("Lendo clientes do Silver", path=path)

        df = self.spark.read.parquet(path)
        rows_read = df.count()
        logger.info("Registros lidos", count=rows_read)

        dim = self._build_dim_customers(df)
        rows_written = dim.count()
        gold_path = f"{self._gold}/dim_customers/"

        dim.write.format("parquet").mode("overwrite").save(gold_path)

        metrics = {
            "rows_read": rows_read,
            "rows_discarded": rows_read - rows_written,
            "rows_written": rows_written,
        }
        logger.info("transform_dim_customers concluído", **metrics, gold_path=gold_path)
        return metrics

    def _build_dim_customers(self, df: DataFrame) -> DataFrame:
        """Filtra clientes correntes e seleciona atributos da dimensão.

        O SCD2 simplificado do CustomerLoader marca is_current=True em cada novo
        snapshot diário sem fechar o snapshot anterior — reexecuções de
        `make seed-data` em dias diferentes podem deixar mais de um registro
        "corrente" para o mesmo customer_id. Deduplicamos aqui mantendo o
        snapshot mais recente (maior ingestion_timestamp), garantindo a
        unicidade que uma dimensão exige (contrato reforçado pela PK de
        `dim_customers` na serving layer Postgres).
        """
        df = df.filter(F.col("is_current"))
        latest = Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())
        df = df.withColumn("_rn", F.row_number().over(latest)).filter(F.col("_rn") == 1).drop("_rn")
        return df.select(
            F.col("customer_id").alias("customer_key"),
            "name",
            "cpf_masked",
            "gender",
            "birth_date",
            "age",
            "age_group",
            "segment",
            "city",
            "state",
            "country",
            "risk_score",
            "account_opening_date",
        ).withColumn("processing_timestamp", F.current_timestamp())

    # ── dim_date ────────────────────────────────────────────────────────────────

    def transform_dim_date(self) -> dict[str, int]:
        """Deriva a dimensão de data a partir das datas distintas em Silver.

        Returns:
            Dicionário com contadores: rows_read, rows_discarded, rows_written.
        """
        path = f"{self._silver}/transactions/"
        logger.info("Lendo datas distintas de transações do Silver", path=path)

        df = self.spark.read.parquet(path).select("transaction_date").distinct()
        rows_read = df.count()

        dim = self._build_dim_date(df)
        rows_written = dim.count()
        gold_path = f"{self._gold}/dim_date/"

        dim.write.format("parquet").mode("overwrite").save(gold_path)

        metrics = {
            "rows_read": rows_read,
            "rows_discarded": 0,
            "rows_written": rows_written,
        }
        logger.info("transform_dim_date concluído", **metrics, gold_path=gold_path)
        return metrics

    def _build_dim_date(self, df: DataFrame) -> DataFrame:
        """Constrói atributos de calendário a partir de `transaction_date`."""
        df = df.withColumnRenamed("transaction_date", "date_key")
        df = df.withColumn("year", F.year("date_key"))
        df = df.withColumn("month", F.month("date_key"))
        df = df.withColumn("day", F.dayofmonth("date_key"))
        df = df.withColumn("quarter", F.quarter("date_key"))
        df = df.withColumn("day_of_week", F.dayofweek("date_key"))
        df = df.withColumn("day_name", F.date_format("date_key", "EEEE"))
        df = df.withColumn("week_of_year", F.weekofyear("date_key"))
        df = df.withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
        df = df.withColumn("processing_timestamp", F.current_timestamp())
        return df

    # ── fact_transactions ───────────────────────────────────────────────────────

    def transform_fact_transactions(self) -> dict[str, int]:
        """Lê transações Silver e persiste a fato de transações no Gold.

        Returns:
            Dicionário com contadores: rows_read, rows_discarded, rows_written.
        """
        path = f"{self._silver}/transactions/"
        logger.info("Lendo transações do Silver", path=path)

        df = self.spark.read.parquet(path)
        rows_read = df.count()
        logger.info("Registros lidos", count=rows_read)

        if self.start_date or self.end_date:
            df = self._filter_by_date(df, "transaction_date")
        rows_after_filter = df.count()

        fact = self._build_fact_transactions(df)
        rows_written = fact.count()
        gold_path = f"{self._gold}/fact_transactions/"

        (
            fact.write.format("parquet")
            .mode("overwrite")
            .partitionBy("date_key")
            .save(gold_path)
        )

        metrics = {
            "rows_read": rows_read,
            "rows_discarded": rows_read - rows_after_filter,
            "rows_written": rows_written,
        }
        logger.info("transform_fact_transactions concluído", **metrics, gold_path=gold_path)
        return metrics

    def _build_fact_transactions(self, df: DataFrame) -> DataFrame:
        """Seleciona e renomeia colunas para o grão de fato (uma linha por transação).

        `fraud_score` só existe no Silver depois que o streaming (roadmap 2.3/2.4)
        gravar o score do detector de fraude; no batch puro a coluna não é
        produzida, então é tratada como opcional (nula quando ausente).
        """
        fraud_score = (
            F.col("fraud_score")
            if "fraud_score" in df.columns
            else F.lit(None).cast("double").alias("fraud_score")
        )
        return df.select(
            "transaction_id",
            F.col("customer_id").alias("customer_key"),
            F.col("transaction_date").alias("date_key"),
            "amount_brl",
            "currency",
            "transaction_type",
            "channel",
            "merchant_category",
            "is_fraud",
            "fraud_type",
            fraud_score,
        ).withColumn("processing_timestamp", F.current_timestamp())

    # ── agg_daily_fraud_metrics ─────────────────────────────────────────────────

    def transform_agg_daily_fraud_metrics(self) -> dict[str, int]:
        """Agrega métricas diárias de fraude por dia e tipo de transação.

        Returns:
            Dicionário com contadores: rows_read, rows_discarded, rows_written.
        """
        path = f"{self._silver}/transactions/"
        logger.info("Lendo transações do Silver para agregação", path=path)

        df = self.spark.read.parquet(path)
        rows_read = df.count()

        if self.start_date or self.end_date:
            df = self._filter_by_date(df, "transaction_date")

        agg = self._build_agg_daily_fraud_metrics(df)
        rows_written = agg.count()
        gold_path = f"{self._gold}/agg_daily_fraud_metrics/"

        (
            agg.write.format("parquet")
            .mode("overwrite")
            .partitionBy("date_key")
            .save(gold_path)
        )

        metrics = {
            "rows_read": rows_read,
            "rows_discarded": 0,
            "rows_written": rows_written,
        }
        logger.info(
            "transform_agg_daily_fraud_metrics concluído", **metrics, gold_path=gold_path
        )
        return metrics

    def _build_agg_daily_fraud_metrics(self, df: DataFrame) -> DataFrame:
        """Agrupa por data e tipo de transação, calculando volume e taxa de fraude."""
        agg = df.groupBy(
            F.col("transaction_date").alias("date_key"), "transaction_type"
        ).agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_brl").alias("total_amount_brl"),
            F.avg("amount_brl").alias("avg_amount_brl"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
        )
        agg = agg.withColumn(
            "fraud_rate", F.col("fraud_count") / F.col("total_transactions")
        )
        agg = agg.withColumn("processing_timestamp", F.current_timestamp())
        return agg

    # ── Filtro de datas ──────────────────────────────────────────────────────────

    def _filter_by_date(self, df: DataFrame, date_col: str) -> DataFrame:
        """Filtra DataFrame pelo intervalo [start_date, end_date] na coluna dada."""
        if self.start_date:
            df = df.filter(F.col(date_col) >= F.lit(self.start_date))
        if self.end_date:
            df = df.filter(F.col(date_col) <= F.lit(self.end_date))
        return df

    # ── customer_behavior_profile ───────────────────────────────────────────────

    def transform_customer_behavior_profile(self) -> dict[str, int]:
        """Perfil de comportamento por cliente, para o detector de fraude do streaming (issue #46).

        Aprende do histórico Silver o que é "normal" para cada cliente (valor típico, devices,
        redes, destinatários frequentes, horário, local) e grava uma linha por cliente em
        `gold/customer_behavior_profile/`, que o job de streaming lê por broadcast. É a camada
        "longa" da arquitetura Lambda: cara de calcular (varre meses de dados), estável, e só
        recalculada aqui no batch. Usa só as linhas legítimas do histórico (rótulos históricos
        existem após a confirmação da fraude); o streaming nunca lê o rótulo do evento.

        Sempre usa todo o Silver (ignora `start_date`/`end_date`): um perfil parcial, de um
        intervalo, esconderia devices e destinatários que o cliente já usa há meses. Lê o
        `dim_customers` do Gold, então roda depois de `transform_dim_customers`.

        Returns:
            Dicionário com contadores: rows_read (transações do histórico), rows_written (clientes)
            e customers_with_profile (clientes com histórico suficiente para o perfil valer).
        """
        silver_path = f"{self._silver}/transactions/"
        dim_path = f"{self._gold}/dim_customers/"
        logger.info("Construindo perfil de comportamento", silver=silver_path, dim_customers=dim_path)

        history = self.spark.read.parquet(silver_path)
        profile = self._build_customer_behavior_profile(history, self.spark.read.parquet(dim_path))

        gold_path = f"{self._gold}/customer_behavior_profile/"
        profile.write.format("parquet").mode("overwrite").save(gold_path)

        written = self.spark.read.parquet(gold_path)
        metrics = {
            "rows_read": history.count(),
            "rows_written": written.count(),
            "customers_with_profile": written.filter(F.col("has_profile")).count(),
        }
        logger.info(
            "transform_customer_behavior_profile concluído", **metrics, gold_path=gold_path
        )
        return metrics

    @staticmethod
    def _build_customer_behavior_profile(history: DataFrame, dim_customers: DataFrame) -> DataFrame:
        """Junta a dimensão de clientes ao histórico e delega o cálculo a `build_profiles`.

        `account_opening_date` chega como texto do Silver de clientes; o perfil precisa de data.
        """
        customers = dim_customers.select(
            F.col("customer_key").alias("customer_id"),
            "segment",
            F.to_date("account_opening_date").alias("account_opening_date"),
        )
        return build_profiles(history, customers)

    # ── Execução completa ────────────────────────────────────────────────────────

    def run_all(self) -> dict[str, dict[str, int]]:
        """Executa dimensões, fato, agregação e o perfil de comportamento, retornando métricas."""
        return {
            "dim_customers": self.transform_dim_customers(),
            "dim_date": self.transform_dim_date(),
            "fact_transactions": self.transform_fact_transactions(),
            "agg_daily_fraud_metrics": self.transform_agg_daily_fraud_metrics(),
            "customer_behavior_profile": self.transform_customer_behavior_profile(),
        }


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transforma dados Silver → Gold (PySpark batch job)."
    )
    parser.add_argument(
        "--dataset",
        choices=[
            "all",
            "dim_customers",
            "dim_date",
            "fact_transactions",
            "agg_daily_fraud_metrics",
            "customer_behavior_profile",
        ],
        default="all",
        help="Dataset a transformar (padrão: all).",
    )
    parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Data inicial para filtrar registros (fact_transactions e agregação).",
    )
    parser.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Data final para filtrar registros (fact_transactions e agregação).",
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
        app_name="silver_to_gold",
        local_mode=args.local,
    )

    transformer = SilverToGoldTransformer(
        spark=spark,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    dataset = args.dataset
    if dataset == "all":
        metrics = transformer.run_all()
    elif dataset == "dim_customers":
        metrics = {"dim_customers": transformer.transform_dim_customers()}
    elif dataset == "dim_date":
        metrics = {"dim_date": transformer.transform_dim_date()}
    elif dataset == "fact_transactions":
        metrics = {"fact_transactions": transformer.transform_fact_transactions()}
    elif dataset == "customer_behavior_profile":
        metrics = {"customer_behavior_profile": transformer.transform_customer_behavior_profile()}
    else:
        metrics = {"agg_daily_fraud_metrics": transformer.transform_agg_daily_fraud_metrics()}

    logger.info("Job concluído", metrics=metrics)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
