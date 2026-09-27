"""Guardas do benchmark online V1 x V2 (issue #47).

A query roda contra o Postgres (`make fraud-online-eval`) e não dá para executá-la em CI, que não
tem o banco: estes testes garantem o que dá para garantir sem ele. Que ela só lê, que só usa
colunas que o DDL da serving layer cria, e que o alvo do Makefile aponta para o arquivo certo.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQL_PATH = ROOT / "src" / "serving" / "queries" / "fraud_online_benchmark.sql"
SCHEMA_PATH = ROOT / "src" / "serving" / "loaders" / "schema.sql"

# Colunas de `stream_scored_transactions` de que a query depende: o rótulo, o V1 (shadow), o V2 e a
# latência. Se alguma sair do DDL, o benchmark quebraria só na hora de rodar contra o banco.
USED_COLUMNS = {
    "is_fraud",
    "fraud_type",
    "is_anomaly",
    "is_fraud_predicted",
    "fraud_type_predicted",
    "fraud_signals",
    "latency_seconds",
    "event_time",
    "detector_version",
    "shadow_detector_version",
}


def _sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _statements() -> str:
    """O SQL sem comentários de linha e sem meta-comandos do psql (`\\echo`, `\\pset`)."""
    lines = [
        line
        for line in _sql().splitlines()
        if not line.lstrip().startswith(("--", "\\"))
    ]
    return "\n".join(lines)


def _table_columns(table: str) -> set[str]:
    ddl = SCHEMA_PATH.read_text(encoding="utf-8")
    body = ddl.split(f"CREATE TABLE IF NOT EXISTS {table} (")[1].split(");")[0]
    columns = set()
    for line in body.strip().splitlines():
        parts = line.strip().rstrip(",").split()
        if parts and not parts[0].startswith("--"):
            columns.add(parts[0])
    return columns


class TestBenchmarkQuery:
    def test_only_reads_the_serving_table(self):
        statements = _statements().upper()
        for keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE"):
            assert not re.search(rf"\b{keyword}\b", statements), keyword
        assert "STREAM_SCORED_TRANSACTIONS" in statements

    def test_every_column_it_needs_exists_in_the_ddl(self):
        created = _table_columns("stream_scored_transactions")
        assert USED_COLUMNS <= created
        text = _statements()
        for column in USED_COLUMNS:
            assert re.search(rf"\b{column}\b", text), f"{column} não é usado na query"

    def test_compares_both_detectors_on_the_same_events(self):
        text = _statements()
        assert "is_anomaly" in text and "is_fraud_predicted" in text
        assert "zscore-v1" in text and "multisignal-v2" in text

    def test_reports_the_metrics_of_the_series(self):
        text = _statements().lower()
        for metric in ("precision_pct", "recall_pct", "f1_pct", "fpr_pct", "fnr_pct"):
            assert metric in text
        assert "alertas_por_1000" in text

    def test_reports_latency_percentiles(self):
        text = _statements().lower()
        for percentile in ("0.50", "0.95", "0.99"):
            assert f"percentile_cont({percentile})" in text

    def test_ends_every_statement_with_a_semicolon(self):
        statements = [s for s in _statements().split(";") if s.strip()]
        assert len(statements) >= 8
        assert _statements().rstrip().endswith(";")


class TestMakefileTarget:
    @pytest.fixture(scope="class")
    def makefile(self) -> str:
        return (ROOT / "Makefile").read_text(encoding="utf-8")

    def test_target_runs_the_versioned_query_through_psql(self, makefile):
        assert "fraud-online-eval:" in makefile
        assert "psql" in makefile
        assert "src/serving/queries/fraud_online_benchmark.sql" in makefile

    def test_target_is_phony(self, makefile):
        assert "fraud-online-eval" in makefile.split("clean clean-data")[0]
