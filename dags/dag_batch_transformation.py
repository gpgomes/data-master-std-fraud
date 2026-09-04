"""DAG de transformação batch — Bronze → Silver → Gold → PostgreSQL.

Pipeline diário que roda os jobs PySpark de transformação sobre o que a
DAG `batch_ingestion_pipeline` já carregou no Bronze, terminando com a carga
do Gold na serving layer PostgreSQL.

Schedule: diariamente às 07:00 UTC (uma hora depois da ingestão, às 06:00)
Tags: ['transformation', 'batch', 'silver', 'gold']
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── Default args ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-master",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

_SRC = "/opt/airflow/src/transformation/batch"
_SERVING = "/opt/airflow/src/serving/loaders"

# O driver roda em modo client dentro do próprio container do Airflow (pyspark
# instalado via pip, sem os jars extras da imagem customizada do Spark), então
# precisa baixar hadoop-aws/aws-java-sdk-bundle via Ivy para o s3a:// funcionar.
_S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# gold_to_postgres.py também precisa do driver JDBC do Postgres (baixado via
# Ivy pelo mesmo motivo acima — a imagem custom do Spark já tem esse jar
# embutido, mas o driver rodando aqui dentro do Airflow não).
_GOLD_POSTGRES_PACKAGES = f"{_S3A_PACKAGES},org.postgresql:postgresql:42.7.3"


def _notify_completion(**context) -> None:
    """Task: log de conclusão da pipeline."""
    run_id = context.get("run_id", "unknown")
    logical_date = context.get("logical_date", "unknown")
    print(
        f"[notify_completion] Pipeline de transformação batch concluída com sucesso.\n"
        f"  run_id:       {run_id}\n"
        f"  logical_date: {logical_date}"
    )


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="batch_transformation_pipeline",
    description="Pipeline diário Bronze → Silver → Gold → PostgreSQL (PySpark batch)",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["transformation", "batch", "silver", "gold"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # 1. Bronze → Silver (transactions, market_data, customers)
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{_SRC}/bronze_to_silver.py",
        conn_id="spark_default",
        name="bronze_to_silver",
        packages=_S3A_PACKAGES,
        verbose=False,
    )

    # 2. Silver → Gold (dim_customers, dim_date, fact_transactions, agg_daily_fraud_metrics)
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{_SRC}/silver_to_gold.py",
        conn_id="spark_default",
        name="silver_to_gold",
        packages=_S3A_PACKAGES,
        verbose=False,
    )

    # 3. Gold → PostgreSQL (serving layer)
    load_gold_postgres = SparkSubmitOperator(
        task_id="load_gold_postgres",
        application=f"{_SERVING}/gold_to_postgres.py",
        conn_id="spark_default",
        name="gold_to_postgres",
        packages=_GOLD_POSTGRES_PACKAGES,
        verbose=False,
    )

    # 4. Notificação de conclusão
    notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=_notify_completion,
        trigger_rule="all_success",
    )

    # ── Dependências ─────────────────────────────────────────────────────────
    # Silver depende do Bronze estar populado (batch_ingestion_pipeline, 06:00);
    # Gold depende da Silver que esta DAG acabou de gravar; a serving layer
    # depende do Gold.
    bronze_to_silver >> silver_to_gold >> load_gold_postgres >> notify_completion
