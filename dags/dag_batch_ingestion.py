"""DAG de ingestão batch — Bronze Layer.

Pipeline diário que coleta dados de mercado, transações e clientes,
salvando-os no MinIO (camada Bronze) com validação de qualidade.

Schedule: diariamente às 06:00 UTC
Tags: ['ingestion', 'batch', 'bronze']
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

# ── Default args ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-master",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── Callables das tasks ────────────────────────────────────────────────────────


def _ingest_market_data(**context) -> None:
    """Task: coleta dados OHLCV via yfinance e salva no Bronze."""
    import sys

    sys.path.insert(0, "/opt/airflow")

    from src.ingestion.batch.market_data_collector import MarketDataCollector

    collector = MarketDataCollector()
    results = collector.collect_daily(n_days=7)

    total = sum(results.values())
    context["ti"].xcom_push(key="market_records", value=total)
    print(f"[ingest_market_data] Registros coletados: {total} | {results}")


def _ingest_transactions(**context) -> None:
    """Task: carrega CSVs de transações do diretório local para o Bronze."""
    import sys

    sys.path.insert(0, "/opt/airflow")

    from src.ingestion.batch.transaction_loader import TransactionLoader

    loader = TransactionLoader()
    results = loader.load_to_bronze()

    total = sum(v for v in results.values() if v > 0)
    context["ti"].xcom_push(key="transaction_records", value=total)
    print(f"[ingest_transactions] Registros ingeridos: {total}")


def _ingest_customers(**context) -> None:
    """Task: carrega CSV de clientes com SCD Type 2 para o Bronze."""
    import sys

    sys.path.insert(0, "/opt/airflow")

    from src.ingestion.batch.customer_loader import CustomerLoader

    loader = CustomerLoader()
    count = loader.load_to_bronze()

    context["ti"].xcom_push(key="customer_records", value=count)
    print(f"[ingest_customers] Registros ingeridos: {count}")


def _validate_bronze_data(**context) -> None:
    """Task: placeholder para Great Expectations checkpoint (implementado na Fase 3)."""
    ti = context["ti"]
    market = ti.xcom_pull(task_ids="ingest_market_data", key="market_records") or 0
    txns = ti.xcom_pull(task_ids="ingest_transactions", key="transaction_records") or 0
    customers = ti.xcom_pull(task_ids="ingest_customers", key="customer_records") or 0

    print(
        "[validate_bronze_data] Validação placeholder — GX integrado na Fase 3\n"
        f"  Mercado:      {market} registros\n"
        f"  Transações:   {txns} registros\n"
        f"  Clientes:     {customers} registros"
    )

    if market == 0 and txns == 0 and customers == 0:
        raise ValueError(
            "Nenhum dado foi ingerido nesta execução — verifique as fontes."
        )


def _notify_completion(**context) -> None:
    """Task: log de conclusão da pipeline."""
    run_id = context.get("run_id", "unknown")
    logical_date = context.get("logical_date", "unknown")
    print(
        f"[notify_completion] Pipeline de ingestão batch concluída com sucesso.\n"
        f"  run_id:       {run_id}\n"
        f"  logical_date: {logical_date}"
    )


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="batch_ingestion_pipeline",
    description="Pipeline diário de ingestão batch — Bronze Layer (mercado + transações + clientes)",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "batch", "bronze"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # 1. Sensor: verifica se há dados locais disponíveis
    check_source_availability = FileSensor(
        task_id="check_source_availability",
        filepath="/opt/airflow/data/sample/transactions",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=300,
        mode="reschedule",
        soft_fail=True,  # Não falha o DAG se não houver dados — continua com o que tiver
    )

    # 2. Ingestão paralela de mercado, transações e clientes
    ingest_market_data = PythonOperator(
        task_id="ingest_market_data",
        python_callable=_ingest_market_data,
    )

    ingest_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=_ingest_transactions,
    )

    ingest_customers = PythonOperator(
        task_id="ingest_customers",
        python_callable=_ingest_customers,
    )

    # 3. Validação de qualidade (placeholder GX — Fase 3)
    validate_bronze_data = PythonOperator(
        task_id="validate_bronze_data",
        python_callable=_validate_bronze_data,
    )

    # 4. Notificação de conclusão
    notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=_notify_completion,
        trigger_rule="all_success",
    )

    # ── Dependências ─────────────────────────────────────────────────────────
    # check → [market, transactions, customers] → validate → notify
    check_source_availability >> [ingest_market_data, ingest_transactions, ingest_customers]
    [ingest_market_data, ingest_transactions, ingest_customers] >> validate_bronze_data
    validate_bronze_data >> notify_completion
