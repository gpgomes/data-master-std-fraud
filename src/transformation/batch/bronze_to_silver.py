"""Job PySpark: transformação Bronze → Silver para a plataforma de dados financeiros.

Execução via CLI:
    python -m src.transformation.batch.bronze_to_silver
    python -m src.transformation.batch.bronze_to_silver --start-date 2024-01-01 --end-date 2024-01-31
    python -m src.transformation.batch.bronze_to_silver --local --dataset transactions
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.common.config import settings
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session

logger = get_logger("bronze_to_silver")

# Taxas de câmbio fixas em BRL (configuráveis via argparse)
_DEFAULT_FX_RATES: dict[str, float] = {
    "BRL": 1.0,
    "USD": 5.0,
    "EUR": 5.4,
}

# Moedas aceitas
_VALID_CURRENCIES = {"BRL", "USD", "EUR"}


class BronzeToSilverTransformer:
    """Transforma dados da camada Bronze para Silver.

    Realiza limpeza, deduplicação, normalização e enriquecimento dos três
    domínios do lake: transações, dados de mercado e clientes.

    Args:
        spark: SparkSession ativa (injetada externamente para facilitar testes).
        fx_rates: Mapa moeda → taxa para BRL. Usa taxas padrão se omitido.
        start_date: Filtro de data inicial (YYYY-MM-DD), opcional.
        end_date: Filtro de data final (YYYY-MM-DD), opcional.
    """

    def __init__(
        self,
        spark: SparkSession,
        fx_rates: dict[str, float] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        self.spark = spark
        self.fx_rates = fx_rates or _DEFAULT_FX_RATES
        self.start_date = start_date
        self.end_date = end_date
        self._bronze = f"s3a://{settings.minio.bucket_bronze}"
        self._silver = f"s3a://{settings.minio.bucket_silver}"

    # ── Transactions ────────────────────────────────────────────────────────────

    def transform_transactions(self) -> dict[str, int]:
        """Lê transações Bronze, limpa, enriquece e persiste no Silver.

        Returns:
            Dicionário com contadores: rows_read, rows_discarded, rows_written.
        """
        path = f"{self._bronze}/transactions/"
        logger.info("Lendo transações do Bronze", path=path)

        df = self.spark.read.parquet(path)
        rows_read = df.count()
        logger.info("Registros lidos", count=rows_read)

        df = self._clean_transactions(df)
        rows_after_clean = df.count()

        df = self._enrich_transactions(df)

        if self.start_date or self.end_date:
            df = self._filter_by_date(df, "transaction_date")

        rows_written = df.count()
        silver_path = f"{self._silver}/transactions/"

        (
            df.write.format("parquet")
            .mode("overwrite")
            .partitionBy("transaction_date")
            .save(silver_path)
        )

        metrics = {
            "rows_read": rows_read,
            "rows_discarded": rows_read - rows_after_clean,
            "rows_written": rows_written,
        }
        logger.info("transform_transactions concluído", **metrics, silver_path=silver_path)
        return metrics

    def _clean_transactions(self, df: DataFrame) -> DataFrame:
        """Limpeza: nulos, dedup, cast de tipos, normalização de currency."""
        # Remove nulos no PK
        df = df.filter(F.col("transaction_id").isNotNull())

        # Cast de tipos
        df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

        # Normaliza currency e filtra inválidas
        df = df.withColumn("currency", F.upper(F.trim(F.col("currency"))))
        df = df.filter(F.col("currency").isin(list(_VALID_CURRENCIES)))

        # Preenche nulos em campos opcionais
        df = df.fillna({"channel": "UNKNOWN", "merchant_category": "OUTROS"})

        # Deduplicação: mantém o registro mais recente por transaction_id
        w = Window.partitionBy("transaction_id").orderBy(F.col("timestamp").desc())
        df = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        return df

    def _enrich_transactions(self, df: DataFrame) -> DataFrame:
        """Enriquecimento: colunas derivadas de tempo e conversão de moeda."""
        df = df.withColumn("transaction_date", F.to_date("timestamp"))
        df = df.withColumn("transaction_hour", F.hour("timestamp"))
        df = df.withColumn(
            "is_business_hours",
            (F.col("transaction_hour") >= 8) & (F.col("transaction_hour") < 18),
        )

        # Mapa de taxa de câmbio BRL usando create_map
        fx_pairs = []
        for currency, rate in self.fx_rates.items():
            fx_pairs.extend([F.lit(currency), F.lit(rate)])
        fx_map = F.create_map(*fx_pairs)

        df = df.withColumn(
            "amount_brl",
            F.col("amount").cast("double") * fx_map[F.col("currency")],
        )
        df = df.withColumn("processing_timestamp", F.current_timestamp())
        return df

    # ── Market Data ─────────────────────────────────────────────────────────────

    def transform_market_data(self) -> dict[str, int]:
        """Lê dados de mercado Bronze, limpa, calcula indicadores e persiste no Silver.

        Returns:
            Dicionário com contadores: rows_read, rows_discarded, rows_written.
        """
        path = f"{self._bronze}/market_data/"
        logger.info("Lendo dados de mercado do Bronze", path=path)

        df = self.spark.read.parquet(path)
        rows_read = df.count()
        logger.info("Registros lidos", count=rows_read)

        df = self._clean_market_data(df)
        rows_after_clean = df.count()

        df = self._calculate_market_indicators(df)

        if self.start_date or self.end_date:
            df = self._filter_by_date(df, "date")

        rows_written = df.count()
        silver_path = f"{self._silver}/market_data/"

        (
            df.write.format("parquet")
            .mode("overwrite")
            .partitionBy("date")
            .save(silver_path)
        )

        metrics = {
            "rows_read": rows_read,
            "rows_discarded": rows_read - rows_after_clean,
            "rows_written": rows_written,
        }
        logger.info("transform_market_data concluído", **metrics, silver_path=silver_path)
        return metrics

    def _clean_market_data(self, df: DataFrame) -> DataFrame:
        """Remove registros com OHLCV inválidos (negativos ou volume zero)."""
        df = df.filter(
            (F.col("open") > 0)
            & (F.col("high") > 0)
            & (F.col("low") > 0)
            & (F.col("close") > 0)
            & (F.col("volume") > 0)
        )
        df = df.dropna(subset=["symbol", "date", "open", "high", "low", "close", "volume"])
        return df

    def _calculate_market_indicators(self, df: DataFrame) -> DataFrame:
        """Calcula retorno diário, amplitude intradiária e médias móveis simples."""
        df = df.withColumn(
            "daily_return",
            (F.col("close") - F.col("open")) / F.col("open"),
        )
        df = df.withColumn(
            "intraday_range",
            (F.col("high") - F.col("low")) / F.col("low"),
        )

        for window_size in [5, 10, 20]:
            w = (
                Window.partitionBy("symbol")
                .orderBy(F.col("date").asc())
                .rowsBetween(-(window_size - 1), 0)
            )
            df = df.withColumn(f"sma_{window_size}", F.avg("close").over(w))

        df = df.withColumn("processing_timestamp", F.current_timestamp())
        return df

    # ── Customers ───────────────────────────────────────────────────────────────

    def transform_customers(self) -> dict[str, int]:
        """Lê clientes Bronze, calcula idade e faixa etária, persiste no Silver.

        Returns:
            Dicionário com contadores: rows_read, rows_discarded, rows_written.
        """
        path = f"{self._bronze}/customers/"
        logger.info("Lendo clientes do Bronze", path=path)

        df = self.spark.read.parquet(path)
        rows_read = df.count()
        logger.info("Registros lidos", count=rows_read)

        df = self._transform_customer_data(df)
        rows_written = df.count()

        silver_path = f"{self._silver}/customers/"
        df.write.format("parquet").mode("overwrite").save(silver_path)

        metrics = {
            "rows_read": rows_read,
            "rows_discarded": 0,
            "rows_written": rows_written,
        }
        logger.info("transform_customers concluído", **metrics, silver_path=silver_path)
        return metrics

    def _transform_customer_data(self, df: DataFrame) -> DataFrame:
        """Calcula idade e categoriza em faixa etária. CPF já mascarado no Bronze."""
        df = df.withColumn(
            "age",
            F.floor(
                F.datediff(F.current_date(), F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
                / F.lit(365.25)
            ),
        )
        df = df.withColumn(
            "age_group",
            F.when(F.col("age") < 25, "18-24")
            .when(F.col("age") < 35, "25-34")
            .when(F.col("age") < 45, "35-44")
            .when(F.col("age") < 55, "45-54")
            .when(F.col("age") < 65, "55-64")
            .otherwise("65+"),
        )
        df = df.withColumn("processing_timestamp", F.current_timestamp())
        return df

    # ── Filtro de datas ──────────────────────────────────────────────────────────

    def _filter_by_date(self, df: DataFrame, date_col: str) -> DataFrame:
        """Filtra DataFrame pelo intervalo [start_date, end_date] na coluna dada."""
        if self.start_date:
            df = df.filter(F.col(date_col) >= F.lit(self.start_date))
        if self.end_date:
            df = df.filter(F.col(date_col) <= F.lit(self.end_date))
        return df

    # ── Execução completa ────────────────────────────────────────────────────────

    def run_all(self) -> dict[str, dict[str, int]]:
        """Executa as três transformações e retorna métricas consolidadas."""
        return {
            "transactions": self.transform_transactions(),
            "market_data": self.transform_market_data(),
            "customers": self.transform_customers(),
        }


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transforma dados Bronze → Silver (PySpark batch job)."
    )
    parser.add_argument(
        "--dataset",
        choices=["all", "transactions", "market_data", "customers"],
        default="all",
        help="Dataset a transformar (padrão: all).",
    )
    parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Data inicial para filtrar registros.",
    )
    parser.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Data final para filtrar registros.",
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
        app_name="bronze_to_silver",
        local_mode=args.local,
    )

    transformer = BronzeToSilverTransformer(
        spark=spark,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    dataset = args.dataset
    if dataset == "all":
        metrics = transformer.run_all()
    elif dataset == "transactions":
        metrics = {"transactions": transformer.transform_transactions()}
    elif dataset == "market_data":
        metrics = {"market_data": transformer.transform_market_data()}
    else:
        metrics = {"customers": transformer.transform_customers()}

    logger.info("Job concluído", metrics=metrics)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
