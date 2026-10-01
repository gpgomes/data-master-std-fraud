"""Pré-carrega o estado curto do stream no tamanho de regime (issue #56).

O teste de carga roda cada nível por minutos, então o estado curto (`silver/_stream_state/recent_events/`)
teria só minutos de eventos. Em produção ele guarda 6 h, `TPS × 21.600` linhas, e é isso que pesa na
pontuação e na releitura/regravação a cada trigger. Este script grava um estado sintético desse tamanho
antes de subir o stream, para medir o antes/depois de uma otimização em regime.

Os eventos vêm do Silver do batch, só dos 1.000 clientes que o producer usa (o prefixo determinístico de
`generate_customers` com a seed 42), replicados até o tamanho pedido, com `transaction_id` novo e
`timestamp` espalhado uniformemente nas últimas 6 h (menos uma margem de 5 min).

Roda em dois passos, porque o Spark do container não tem o gerador de clientes à mão com as mesmas deps:

    python -m scripts.seed_stream_state ids            # no host: grava data/load_test/customer_ids.txt
    docker compose cp scripts/seed_stream_state.py spark-master:/opt/spark/work-dir/seed_stream_state.py
    docker compose exec -T -e PYTHONPATH=/opt/spark/work-dir spark-master \
        spark-submit --master 'local[2]' /opt/spark/work-dir/seed_stream_state.py seed --rows 1080000

Compatível com Python 3.10 no passo `seed` (roda no container do Spark).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

IDS_PATH = "data/load_test/customer_ids.txt"
HORIZON_S = 6 * 3600
MARGIN_S = 300


def write_ids(root: Path) -> Path:
    sys.path.insert(0, str(root))
    from src.common.data_generator import DataGenerator

    ids = [c["customer_id"] for c in DataGenerator(seed=42).generate_customers(n=1_000)]
    path = root / IDS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(ids) + "\n", encoding="utf-8")
    return path


def seed(rows: int) -> None:
    from pyspark.sql import functions as F

    from src.common.config import settings
    from src.common.spark_session import create_spark_session
    from src.transformation.fraud.signals import EVENT_COLUMNS

    spark = create_spark_session(app_name="seed_stream_state", local_mode=True)
    spark.sparkContext.setLogLevel("ERROR")
    ids = [line.strip() for line in open(IDS_PATH, encoding="utf-8") if line.strip()]

    base = (
        spark.read.parquet(f"s3a://{settings.minio.bucket_silver}/transactions/")
        .filter(F.col("customer_id").isin(ids) & ~F.coalesce(F.col("is_fraud"), F.lit(False)))
        .select(*EVENT_COLUMNS)
        .withColumn("amount", F.col("amount").cast("double"))
        .cache()
    )
    base_rows = base.count()
    copies = max(1, -(-rows // base_rows))  # teto
    state = (
        base.withColumn("_copy", F.explode(F.sequence(F.lit(1), F.lit(copies))))
        .drop("_copy")
        .limit(rows)
        .withColumn("transaction_id", F.expr("uuid()"))
        .withColumn(
            "timestamp",
            (
                F.unix_timestamp(F.current_timestamp())
                - F.lit(MARGIN_S)
                - F.rand(7) * F.lit(HORIZON_S - 2 * MARGIN_S)
            ).cast("timestamp"),
        )
        .select(*EVENT_COLUMNS)
    )
    path = f"s3a://{settings.minio.bucket_silver}/_stream_state/recent_events/"
    state.write.mode("overwrite").parquet(path)
    written = spark.read.parquet(path)
    r = written.agg(
        F.count("*"), F.countDistinct("customer_id"), F.min("timestamp"), F.max("timestamp")
    ).first()
    print(f"SEED|linhas={r[0]} clientes={r[1]} de={r[2]} até={r[3]} (base {base_rows} × {copies})")
    spark.stop()


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Pré-carrega o estado curto do stream (issue #56)."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("ids", help="No host: grava os ids dos 1.000 clientes do producer.")
    seed_cmd = sub.add_parser("seed", help="No container do Spark: grava o estado sintético.")
    seed_cmd.add_argument("--rows", type=int, default=1_080_000, help="6 h a 50 TPS = 1.080.000.")
    args = parser.parse_args(argv)
    if args.cmd == "ids":
        print(write_ids(Path(__file__).resolve().parents[1]))
    else:
        seed(args.rows)


if __name__ == "__main__":
    main(sys.argv[1:])
