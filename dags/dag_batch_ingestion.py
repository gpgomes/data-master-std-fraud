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


def _check_source_availability(**context) -> None:
    """Task: verifica se há dados locais disponíveis para ingestão."""
    base = Path("/opt/airflow/data/sample")
    found = []
    for subdir in ["transactions", "market_data", "customers"]:
        path = base / subdir
        if path.exists():
            found.append(subdir)

    if not found:
        print(
            f"[check_source_availability] Diretório {base} vazio ou ausente. "
            "Execute a DAG 'seed_sample_data' primeiro para gerar os dados de exemplo."
        )
    else:
        print(f"[check_source_availability] Fontes disponíveis: {found}")


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
    """Task: quality gate real via Great Expectations (issue #13).

    bronze_transactions é o dado core do caso de fraude — falha bloqueia a
    task (e portanto a DAG). bronze_market_data é enriquecimento coletado de
    uma API gratuita de terceiros (yfinance/Yahoo Finance), sujeita a rate
    limiting fora do nosso controle (ex.: HTTP 429) mesmo com dados locais
    disponíveis — falha aqui não deve derrubar o pipeline principal, só fica
    registrada como warning. Nenhum dos dois depende de novos registros
    nesta execução especificamente (valida o estado atual do Bronze, que é
    cumulativo e idempotente).
    Diagnóstico e recuperação: docs/runbook.md, seção "Quality Gates".
    """
    import sys

    sys.path.insert(0, "/opt/airflow")

    from src.governance.great_expectations.runner import run_gate, run_gate_optional

    run_gate("bronze_transactions")
    run_gate_optional("bronze_market_data")


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

    # 1. Verifica se há dados locais disponíveis (não bloqueia o pipeline)
    check_source_availability = PythonOperator(
        task_id="check_source_availability",
        python_callable=_check_source_availability,
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

    # 3. Validação de qualidade (Great Expectations — issue #13)
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
