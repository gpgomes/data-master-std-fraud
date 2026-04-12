"""DAG manual para popular dados iniciais (seed) no diretório data/sample/.

Sem schedule — deve ser disparada manualmente via Airflow UI ou CLI:
    airflow dags trigger seed_sample_data

Útil para o primeiro setup do ambiente ou para regenerar os dados sintéticos.
Tags: ['setup', 'seed', 'manual']
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Default args ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-master",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── Callables ──────────────────────────────────────────────────────────────────


def _generate_sample_data(**context) -> None:
    """Task: executa o gerador de dados sintéticos."""
    import subprocess
    import sys

    params = context.get("params", {})
    transactions = params.get("transactions", 10_000)
    customers = params.get("customers", 500)
    market_days = params.get("market_days", 30)
    seed = params.get("seed", 42)

    cmd = [
        sys.executable,
        "-m", "scripts.generate_sample_data",
        "--transactions", str(transactions),
        "--customers", str(customers),
        "--market-days", str(market_days),
        "--seed", str(seed),
        "--output-dir", "data/sample",
    ]

    print(f"[seed_sample_data] Executando: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd="/opt/airflow")

    if result.returncode != 0:
        print(f"STDERR:\n{result.stderr}")
        raise RuntimeError(f"generate_sample_data falhou com código {result.returncode}")

    print(result.stdout)
    print("[seed_sample_data] Dados gerados com sucesso.")


def _verify_output(**context) -> None:
    """Task: verifica se os arquivos foram gerados corretamente."""
    from pathlib import Path

    base = Path("/opt/airflow/data/sample")
    checks = {
        "customers": base / "customers" / "customers.csv",
        "transactions": base / "transactions",
        "market_data": base / "market_data",
    }

    for name, path in checks.items():
        if not path.exists():
            raise FileNotFoundError(f"[verify_output] Caminho não encontrado: {path}")
        print(f"[verify_output] OK — {name}: {path}")

    print("[verify_output] Verificação concluída.")


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="seed_sample_data",
    description="DAG manual para gerar dados sintéticos iniciais (setup do ambiente)",
    schedule=None,  # Sem agendamento — disparada manualmente
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["setup", "seed", "manual"],
    params={
        "transactions": 10_000,
        "customers": 500,
        "market_days": 30,
        "seed": 42,
    },
    doc_md=__doc__,
) as dag:

    generate_data = PythonOperator(
        task_id="generate_sample_data",
        python_callable=_generate_sample_data,
    )

    verify_output = PythonOperator(
        task_id="verify_output",
        python_callable=_verify_output,
    )

    print_summary = BashOperator(
        task_id="print_summary",
        bash_command=(
            "echo '=== Arquivos gerados ===' && "
            "find /opt/airflow/data/sample -type f | head -20 && "
            "echo '=== Contagem ===' && "
            "find /opt/airflow/data/sample -type f | wc -l"
        ),
    )

    generate_data >> verify_output >> print_summary
