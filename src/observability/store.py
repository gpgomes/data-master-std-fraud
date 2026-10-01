"""Gravação das métricas de plataforma no Postgres (issue #55).

Regra única: **métrica nunca derruba o pipeline.** Toda falha (Postgres fora, credencial errada,
tabela travada) vira um warning no log e a gravação é descartada. Com
`settings.observability.enabled = False` nada é gravado (os testes unitários desligam).

Roda em quatro processos diferentes, cada um com o seu host do Postgres: o driver do stream (no
container do Spark, `settings.postgres.internal_host`), o Airflow e a API em container
(`POSTGRES_HOST=postgres`) e a API/CLI no host (`localhost`). Por isso o host é um parâmetro.

Compatível com Python 3.10: é importado pelo stream, que roda no container do Spark.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.common.config import settings
from src.common.logger import get_logger

logger = get_logger("observability")

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

STREAM_BATCH_COLUMNS = (
    "query_id",
    "run_id",
    "batch_id",
    "batch_timestamp",
    "num_input_rows",
    "input_rows_per_second",
    "processed_rows_per_second",
    "batch_duration_ms",
    "add_batch_ms",
    "get_offset_ms",
    "kafka_lag",
    "rows_scored",
    "alerts",
    "state_rows",
    "latency_p50_s",
    "latency_p95_s",
    "latency_max_s",
    "state_load_ms",
    "score_ms",
    "parquet_ms",
    "kafka_ms",
    "state_write_ms",
)
GATE_COLUMNS = ("dataset", "success", "total_expectations", "failed_expectations", "optional")
PIPELINE_COLUMNS = (
    "dag_id",
    "run_id",
    "state",
    "start_date",
    "end_date",
    "duration_seconds",
    "failed_tasks",
)
API_COLUMNS = ("method", "route", "status_code", "duration_ms")
ACCESS_COLUMNS = ("api_key_id", "method", "route", "query", "status_code", "client_host")


def _default_connect(host: str) -> Any:
    import psycopg2

    return psycopg2.connect(
        host=host,
        port=settings.postgres.port,
        dbname=settings.postgres.db,
        user=settings.postgres.user,
        password=settings.postgres.password,
        connect_timeout=settings.observability.connect_timeout_s,
    )


def _insert_sql(table: str, columns: tuple, conflict: str | None = None) -> str:
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    if conflict:
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict)
        sql += f" ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    return sql


class MetricsStore:
    """Grava métricas; nunca levanta exceção para quem chama.

    Args:
        host: host do Postgres (padrão: `settings.postgres.host`).
        connect: fábrica de conexão (injetável nos testes).
    """

    def __init__(
        self, host: str | None = None, connect: Callable[[str], Any] | None = None
    ) -> None:
        self.host = host or settings.postgres.host
        self._connect = connect or _default_connect
        self._schema_ready = False

    @property
    def enabled(self) -> bool:
        return bool(settings.observability.enabled)

    def _execute(self, sql: str, params: tuple | None = None) -> bool:
        if not self.enabled:
            return False
        conn = None
        try:
            conn = self._connect(self.host)
            with conn.cursor() as cur:
                if not self._schema_ready:
                    cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
                cur.execute(sql, params)
            conn.commit()
            self._schema_ready = True
            return True
        except Exception as exc:  # noqa: BLE001 - métrica nunca derruba o pipeline
            logger.warning("Métrica de plataforma não gravada", host=self.host, erro=str(exc))
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass

    def ensure_schema(self) -> bool:
        """Cria as tabelas de métricas (idempotente)."""
        return self._execute("SELECT 1")

    def record_stream_batch(self, row: dict[str, Any]) -> bool:
        params = tuple(row.get(c) for c in STREAM_BATCH_COLUMNS)
        return self._execute(
            _insert_sql("stream_batch_metrics", STREAM_BATCH_COLUMNS, "query_id, batch_id"), params
        )

    def record_quality_gate(self, row: dict[str, Any]) -> bool:
        params = tuple(row.get(c) for c in GATE_COLUMNS)
        return self._execute(_insert_sql("quality_gate_runs", GATE_COLUMNS), params)

    def record_pipeline_run(self, row: dict[str, Any]) -> bool:
        params = tuple(row.get(c) for c in PIPELINE_COLUMNS)
        return self._execute(
            _insert_sql("pipeline_runs", PIPELINE_COLUMNS, "dag_id, run_id"), params
        )

    def record_api_access(self, row: dict[str, Any]) -> bool:
        """Log de auditoria de acesso à API (issue #58)."""
        params = tuple(row.get(c) for c in ACCESS_COLUMNS)
        return self._execute(_insert_sql("api_access_audit", ACCESS_COLUMNS), params)

    def record_api_request(self, row: dict[str, Any]) -> bool:
        params = tuple(row.get(c) for c in API_COLUMNS)
        return self._execute(_insert_sql("api_requests", API_COLUMNS), params)
