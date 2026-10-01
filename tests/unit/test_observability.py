"""Testes das métricas de plataforma (issue #55): gravação, listener do stream, execuções de DAG,
middleware da API, dashboard Platform Health e relatório de SLOs.

Nenhum teste toca o Postgres real: o `MetricsStore` recebe uma conexão falsa, e o `conftest.py`
desliga as métricas por padrão (os testes que precisam religam com `monkeypatch`).
"""

from __future__ import annotations

import ast
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.common.config import settings
from src.observability.pipeline import pipeline_run_row
from src.observability.store import (
    API_COLUMNS,
    SCHEMA_PATH,
    STREAM_BATCH_COLUMNS,
    MetricsStore,
)
from src.transformation.streaming.metrics_listener import (
    StreamMetricsListener,
    kafka_lag,
    progress_to_row,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(settings.observability, "enabled", True)


class FakeConnection:
    """Conexão psycopg2 mínima: guarda os comandos executados."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.executed: list[tuple[str, tuple | None]] = []
        self.committed = 0
        self.closed = False
        self._fail_on = fail_on

    def cursor(self):
        conn = self

        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, params=None):
                if conn._fail_on and conn._fail_on in sql:
                    raise RuntimeError("tabela travada")
                conn.executed.append((sql, params))

        return _Cursor()

    def commit(self):
        self.committed += 1

    def close(self):
        self.closed = True


def _store(conn: FakeConnection | Exception) -> tuple[MetricsStore, list[str]]:
    hosts: list[str] = []

    def connect(host: str):
        hosts.append(host)
        if isinstance(conn, Exception):
            raise conn
        return conn

    return MetricsStore(host="pg-test", connect=connect), hosts


# ── MetricsStore ─────────────────────────────────────────────────────────────────


class TestMetricsStore:
    def test_disabled_store_never_connects(self):
        store, hosts = _store(FakeConnection())
        assert store.enabled is False  # o conftest desliga
        assert store.record_api_request({"method": "GET"}) is False
        assert hosts == []

    def test_creates_the_schema_once_then_only_inserts(self, enabled):
        conn = FakeConnection()
        store, hosts = _store(conn)
        assert store.record_api_request({"method": "GET", "route": "/x", "status_code": 200})
        assert store.record_api_request({"method": "GET", "route": "/y", "status_code": 200})

        ddl = [sql for sql, _ in conn.executed if "CREATE TABLE" in sql]
        inserts = [sql for sql, _ in conn.executed if sql.startswith("INSERT INTO api_requests")]
        assert len(ddl) == 1 and len(inserts) == 2
        assert hosts == ["pg-test", "pg-test"]
        assert conn.closed

    def test_params_follow_the_column_order_and_missing_keys_are_null(self, enabled):
        conn = FakeConnection()
        store, _ = _store(conn)
        store.record_api_request({"route": "/alerts", "status_code": 503, "method": "GET"})
        _, params = conn.executed[-1]
        assert params == ("GET", "/alerts", 503, None)
        assert len(params) == len(API_COLUMNS)

    def test_stream_batches_are_upserted_by_query_and_batch(self, enabled):
        """Um replay do mesmo batch_id reescreve a linha em vez de duplicar."""
        conn = FakeConnection()
        store, _ = _store(conn)
        store.record_stream_batch({"query_id": "q", "batch_id": 7})
        sql, params = conn.executed[-1]
        assert "ON CONFLICT (query_id, batch_id) DO UPDATE" in sql
        assert "query_id = EXCLUDED.query_id" not in sql
        assert len(params) == len(STREAM_BATCH_COLUMNS)

    def test_pipeline_runs_are_upserted_by_dag_and_run(self, enabled):
        conn = FakeConnection()
        store, _ = _store(conn)
        store.record_pipeline_run({"dag_id": "d", "run_id": "r", "state": "success"})
        assert "ON CONFLICT (dag_id, run_id) DO UPDATE" in conn.executed[-1][0]

    def test_a_connection_failure_never_raises(self, enabled):
        store, _ = _store(ConnectionError("postgres fora"))
        assert store.record_quality_gate({"dataset": "x", "success": True}) is False

    def test_a_failed_insert_is_swallowed_and_the_connection_closed(self, enabled):
        conn = FakeConnection(fail_on="INSERT INTO quality_gate_runs")
        store, _ = _store(conn)
        assert store.record_quality_gate({"dataset": "x"}) is False
        assert conn.closed and conn.committed == 0

    def test_the_default_host_is_the_configured_one(self):
        assert MetricsStore().host == settings.postgres.host


class TestSchema:
    DDL = SCHEMA_PATH.read_text(encoding="utf-8")

    @pytest.mark.parametrize(
        "table, columns",
        [("stream_batch_metrics", STREAM_BATCH_COLUMNS), ("api_requests", API_COLUMNS)],
    )
    def test_every_recorded_column_exists_in_the_ddl(self, table, columns):
        body = self.DDL.split(f"CREATE TABLE IF NOT EXISTS {table} (")[1].split(");")[0]
        created = {line.split()[0] for line in body.strip().splitlines() if line.strip()}
        assert set(columns) <= created

    def test_ddl_is_idempotent(self):
        statements = [
            s.strip() for s in self.DDL.split(";") if s.strip() and not s.strip().startswith("--")
        ]
        for statement in statements:
            code = "\n".join(ln for ln in statement.splitlines() if not ln.strip().startswith("--"))
            assert "IF NOT EXISTS" in code, code[:60]


# ── Listener do stream ────────────────────────────────────────────────────────────


class TestKafkaLag:
    def test_sums_latest_minus_processed_per_partition(self):
        end = '{"raw-transactions": {"0": 100, "1": 50, "2": 10}}'
        latest = '{"raw-transactions": {"0": 130, "1": 50, "2": 15}}'
        assert kafka_lag(end, latest) == 35

    def test_never_negative(self):
        assert kafka_lag('{"t": {"0": 10}}', '{"t": {"0": 5}}') == 0

    def test_unknown_offsets_give_none(self):
        assert kafka_lag(None, '{"t": {"0": 5}}') is None
        assert kafka_lag("not json", '{"t": {"0": 5}}') is None

    def test_a_partition_not_yet_processed_is_ignored(self):
        assert kafka_lag('{"t": {"0": 1}}', '{"t": {"0": 3, "1": 9}}') == 2


def _progress(batch_id: int = 4, **overrides):
    source = SimpleNamespace(
        endOffset='{"raw-transactions": {"0": 100, "1": 100}}',
        latestOffset='{"raw-transactions": {"0": 110, "1": 105}}',
    )
    base = {
        "id": "query-1",
        "runId": "run-1",
        "batchId": batch_id,
        "timestamp": "2026-09-27T02:00:05.123Z",
        "numInputRows": 140,
        "inputRowsPerSecond": 14.0,
        "processedRowsPerSecond": 20.5,
        "durationMs": {"triggerExecution": 6800, "addBatch": 6100, "latestOffset": 12},
        "sources": [source],
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class TestProgressToRow:
    def test_maps_spark_progress_and_the_processor_stats(self):
        stats = {4: {"rows_scored": 140, "alerts": 3, "state_rows": 9000, "latency_p95_s": 9.8}}
        row = progress_to_row(_progress(), stats)
        assert row["query_id"] == "query-1" and row["batch_id"] == 4
        assert row["batch_timestamp"] == datetime(2026, 9, 27, 2, 0, 5)
        assert row["batch_duration_ms"] == 6800 and row["add_batch_ms"] == 6100
        assert row["kafka_lag"] == 15
        assert (row["rows_scored"], row["alerts"], row["state_rows"]) == (140, 3, 9000)
        assert row["latency_p95_s"] == 9.8
        assert set(row) == set(STREAM_BATCH_COLUMNS)

    def test_stats_are_consumed_so_they_do_not_accumulate(self):
        stats = {4: {"rows_scored": 1}, 5: {"rows_scored": 2}}
        progress_to_row(_progress(4), stats)
        assert list(stats) == [5]

    def test_a_replayed_batch_without_stats_still_records_spark_metrics(self):
        row = progress_to_row(_progress(9), {})
        assert row["rows_scored"] is None and row["num_input_rows"] == 140

    def test_no_kafka_offsets_means_unknown_lag(self):
        row = progress_to_row(_progress(sources=[]), {})
        assert row["kafka_lag"] is None


class TestStreamMetricsListener:
    def test_records_one_row_per_progress_event(self):
        store = MagicMock()
        listener = StreamMetricsListener(store, {})
        listener.onQueryProgress(SimpleNamespace(progress=_progress()))
        store.record_stream_batch.assert_called_once()
        assert store.record_stream_batch.call_args.args[0]["batch_id"] == 4

    def test_a_malformed_progress_never_raises(self):
        store = MagicMock()
        StreamMetricsListener(store, {}).onQueryProgress(SimpleNamespace(progress=None))
        store.record_stream_batch.assert_not_called()

    def test_creates_the_tables_when_the_query_starts(self):
        store = MagicMock()
        StreamMetricsListener(store, {}).onQueryStarted(SimpleNamespace())
        store.ensure_schema.assert_called_once()


# ── Execuções de DAG ──────────────────────────────────────────────────────────────


def _dag_run(states: dict[str, str], start: datetime | None):
    tis = [SimpleNamespace(task_id=t, state=s) for t, s in states.items()]
    return SimpleNamespace(
        dag_id="batch_transformation_pipeline",
        run_id="manual__1",
        start_date=start,
        get_task_instances=lambda: tis,
    )


class TestPipelineRunRow:
    START = datetime(2026, 9, 27, 7, 0, tzinfo=UTC)

    def test_success_when_no_task_failed(self):
        run = _dag_run(
            {"a": "success", "b": "success", "record_pipeline_run": "running"}, self.START
        )
        row = pipeline_run_row(run, "record_pipeline_run", now=self.START + timedelta(minutes=12))
        assert row["state"] == "success"
        assert row["duration_seconds"] == 720
        assert row["failed_tasks"] is None
        assert row["start_date"].tzinfo is None

    def test_failed_and_upstream_failed_tasks_mark_the_run_as_failed(self):
        run = _dag_run(
            {"a": "success", "validate_gold_data": "failed", "load": "upstream_failed"}, self.START
        )
        row = pipeline_run_row(run, "record_pipeline_run", now=self.START)
        assert row["state"] == "failed"
        assert row["failed_tasks"] == "load,validate_gold_data"

    def test_run_without_start_date(self):
        row = pipeline_run_row(_dag_run({}, None), "x", now=self.START)
        assert row["duration_seconds"] is None


class TestDagWiring:
    """As DAGs não importam sem Airflow no venv: a checagem é estática."""

    @pytest.mark.parametrize("dag_file", ["dag_batch_transformation.py", "dag_batch_ingestion.py"])
    def test_record_task_runs_even_after_a_failure(self, dag_file):
        source = (ROOT / "dags" / dag_file).read_text(encoding="utf-8")
        block = source.split('task_id="record_pipeline_run"')[1].split(")")[0]
        assert 'trigger_rule="all_done"' in block

    @pytest.mark.parametrize("dag_file", ["dag_batch_transformation.py", "dag_batch_ingestion.py"])
    def test_record_task_is_not_downstream_of_notify_completion(self, dag_file):
        """Se fosse, seria a única folha e a DAG com falha terminaria marcada como sucesso."""
        source = (ROOT / "dags" / dag_file).read_text(encoding="utf-8")
        wiring = source.split(">> record_pipeline_run")[0].rsplit("[", 1)[1]
        assert "notify_completion" not in wiring
        assert "notify_completion >> record_pipeline_run" not in source


# ── Middleware da API ─────────────────────────────────────────────────────────────


class TestApiMiddleware:
    @pytest.fixture
    def recorded(self, enabled, monkeypatch):
        from src.serving.api import main

        rows: list[dict] = []
        monkeypatch.setattr(main.metrics_store, "record_api_request", rows.append)
        return rows

    def test_records_route_template_status_and_duration(self, recorded):
        from fastapi.testclient import TestClient

        from src.serving.api.main import app

        TestClient(app).get("/health/live")
        assert len(recorded) == 1
        row = recorded[0]
        assert (row["method"], row["route"], row["status_code"]) == ("GET", "/health/live", 200)
        assert row["duration_ms"] >= 0

    def test_path_parameters_are_not_stored(self, recorded, api_key):
        from fastapi.testclient import TestClient

        from src.serving.api.db import get_db
        from src.serving.api.main import app

        app.dependency_overrides[get_db] = lambda: MagicMock(
            execute=MagicMock(
                return_value=MagicMock(mappings=lambda: MagicMock(first=lambda: None))
            )
        )
        try:
            TestClient(app, headers={"X-API-Key": api_key}).get("/transactions/tx-secreta-123")
        finally:
            app.dependency_overrides.clear()
        assert recorded[0]["route"] == "/transactions/{transaction_id}"
        assert recorded[0]["status_code"] == 404

    def test_unknown_url_is_grouped_as_unmatched(self, recorded):
        from fastapi.testclient import TestClient

        from src.serving.api.main import app

        TestClient(app).get("/nao-existe/123")
        assert recorded[0]["route"] == "unmatched"

    def test_nothing_is_recorded_when_disabled(self, monkeypatch):
        from fastapi.testclient import TestClient

        from src.serving.api import main

        spy = MagicMock()
        monkeypatch.setattr(main.metrics_store, "record_api_request", spy)
        TestClient(main.app).get("/health/live")
        spy.assert_not_called()


# ── Dashboard Platform Health ─────────────────────────────────────────────────────


class TestPlatformDashboard:
    def test_every_chart_reads_a_metrics_table(self):
        from src.serving.dashboards.platform_charts import PLATFORM_CHARTS, PLATFORM_DATASETS

        ddl = SCHEMA_PATH.read_text(encoding="utf-8")
        for table in PLATFORM_DATASETS:
            assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl
        assert {c.dataset_table for c in PLATFORM_CHARTS} == set(PLATFORM_DATASETS)

    def test_all_platform_charts_are_optional(self):
        from src.serving.dashboards.platform_charts import PLATFORM_CHARTS

        assert all(c.optional for c in PLATFORM_CHARTS)

    def test_row_sizes_cover_every_chart(self):
        from src.serving.dashboards.platform_charts import PLATFORM_CHARTS, PLATFORM_ROW_SIZES

        assert sum(PLATFORM_ROW_SIZES) == len(PLATFORM_CHARTS)

    def test_slice_names_are_unique_across_both_dashboards(self):
        """O provisionamento acha o chart pelo nome: nomes repetidos se sobrescreveriam."""
        from src.serving.dashboards.provision import DASHBOARDS

        names = [c.slice_name for spec in DASHBOARDS for c in spec.charts]
        assert len(names) == len(set(names))

    def test_two_dashboards_and_only_the_fraud_one_has_native_filters(self):
        from src.serving.dashboards.provision import DASHBOARDS

        assert [d.slug for d in DASHBOARDS] == ["fraude-transacoes-visao-geral", "platform-health"]
        assert [d.native_filters for d in DASHBOARDS] == [True, False]

    def test_platform_dashboard_is_published_without_native_filters(self):
        import json

        from src.serving.dashboards.provision import finalize_dashboard

        client = MagicMock()
        finalize_dashboard(client, 2, [1, 2, 3], {}, (3,), native_filters=False)
        payload = client.put.call_args.args[1]
        assert json.loads(payload["json_metadata"])["native_filter_configuration"] == []
        assert payload["published"] is True


# ── Relatório de SLOs ─────────────────────────────────────────────────────────────


class TestSloReport:
    SQL = (ROOT / "src" / "serving" / "queries" / "slo_report.sql").read_text(encoding="utf-8")

    def _statements(self) -> str:
        return "\n".join(
            ln for ln in self.SQL.splitlines() if not ln.lstrip().startswith(("--", "\\"))
        )

    def test_only_reads(self):
        code = self._statements().upper()
        for keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE"):
            assert not re.search(rf"\b{keyword}\b", code), keyword

    def test_reads_the_four_metrics_tables(self):
        code = self._statements()
        for table in ("stream_batch_metrics", "quality_gate_runs", "pipeline_runs", "api_requests"):
            assert table in code

    def test_the_targets_chosen_for_the_local_environment(self):
        code = self._statements()
        for target in (
            "latency_p95_s < 12",
            "max_lag < 10000",
            "duration_p95_ms < 10000",
            "p95_ms < 500",
            "err_pct < 1",
        ):
            assert target in code

    def test_no_data_is_reported_as_such_not_as_ok(self):
        assert "WHEN amostras = 0 THEN 'SEM DADOS'" in self._statements()

    def test_makefile_target(self):
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        assert "slo-report:" in makefile and "src/serving/queries/slo_report.sql" in makefile


# ── Stream processor e runner do GX ───────────────────────────────────────────────


class TestIntegrationPoints:
    def test_the_stream_registers_the_listener_with_the_internal_host(self):
        source = (ROOT / "src/transformation/streaming/stream_processor.py").read_text(
            encoding="utf-8"
        )
        run = source.split("def run(")[1]
        assert "addListener" in run and "internal_host" in run

    def test_the_gx_runner_records_every_gate(self, monkeypatch, enabled):
        from src.governance.great_expectations import runner

        tree = ast.parse(Path(runner.__file__).read_text(encoding="utf-8"))
        run_gate = next(
            n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_gate"
        )
        assert "record_quality_gate" in ast.unparse(run_gate)
