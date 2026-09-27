"""Testes das partes puras do harness de carga do stream (issue #56). A execução real precisa do
Docker, do stream e do Postgres, e é registrada em `docs/testes_issue_56.txt`."""

from __future__ import annotations

from datetime import datetime

import pytest

from scripts.stream_load_test import (
    LevelResult,
    _int,
    _parse_mem_mib,
    _to_spark_local,
    plan_level,
    saturation,
    summarize_stats,
)


def _result(**overrides) -> LevelResult:
    base = {
        "target_tps": 100,
        "instances": 4,
        "window_start": "2026-09-27T12:00:00+00:00",
        "window_end": "2026-09-27T12:04:00+00:00",
        "produced_tps": 60.0,
        "input_rps": 60.0,
        "processed_rps": 90.0,
        "duration_p95_s": 6.0,
        "lag_end": 0,
    }
    base.update(overrides)
    return LevelResult(**base)


class TestPlanLevel:
    @pytest.mark.parametrize(
        "target, cap, instances, per",
        [(25, 25, 1, 25), (50, 25, 2, 25), (100, 25, 4, 25), (300, 25, 12, 25), (10, 25, 1, 10)],
    )
    def test_splits_the_target_so_no_instance_exceeds_the_cap(self, target, cap, instances, per):
        plan = plan_level(target, cap)
        assert (plan.instances, plan.tps_per_instance) == (instances, per)

    def test_uneven_split(self):
        plan = plan_level(70, 25)
        assert plan.instances == 3 and plan.tps_per_instance == pytest.approx(23.333, abs=1e-3)


class TestSaturation:
    def test_a_stream_that_keeps_up_is_not_saturated(self):
        assert saturation(_result()) == []

    def test_micro_batch_longer_than_the_trigger(self):
        assert "trigger" in saturation(_result(duration_p95_s=12.5))[0]

    def test_lag_left_at_the_end_of_the_window(self):
        reasons = saturation(_result(lag_end=5000))
        assert any("lag no fim" in r for r in reasons)

    def test_small_lag_within_one_trigger_is_fine(self):
        assert saturation(_result(lag_end=500)) == []  # 60 TPS × 10 s = 600

    def test_reading_well_below_what_is_produced(self):
        reasons = saturation(_result(input_rps=40.0))
        assert any("90%" in r for r in reasons)

    def test_missing_metrics_do_not_flag(self):
        assert saturation(_result(duration_p95_s=None, lag_end=None, input_rps=None)) == []


class TestDockerStats:
    @pytest.mark.parametrize(
        "raw, mib",
        [("512MiB / 2GiB", 512.0), ("1.5GiB / 4GiB", 1536.0), ("2048KiB / 1GiB", 2.0)],
    )
    def test_parses_memory_usage(self, raw, mib):
        assert _parse_mem_mib(raw) == pytest.approx(mib)

    def test_average_cpu_and_peak_memory_per_container(self):
        samples = [
            {"name": "spark-worker-1", "cpu": 100.0, "mem": 800.0},
            {"name": "spark-worker-1", "cpu": 150.0, "mem": 900.0},
            {"name": "spark-master", "cpu": 20.0, "mem": 400.0},
        ]
        cpu, mem = summarize_stats(samples)
        assert cpu == {"spark-worker-1": 125.0, "spark-master": 20.0}
        assert mem == {"spark-worker-1": 900.0, "spark-master": 400.0}


class TestTimezone:
    def test_window_is_converted_to_the_spark_container_local_time(self):
        """O JDBC grava `processing_timestamp` na hora de São Paulo (TZ dos containers do Spark)."""
        assert _to_spark_local("2026-09-27T12:00:00+00:00") == datetime(2026, 9, 27, 9, 0, 0)


class TestJsonSafety:
    def test_postgres_decimals_become_ints(self):
        """`sum()`/`max()` de BIGINT chegam como Decimal; o JSON do resultado não aceita Decimal."""
        import json
        from dataclasses import asdict
        from decimal import Decimal

        result = _result(rows_scored=_int(Decimal("3253")), lag_max=_int(Decimal("0")))
        assert json.loads(json.dumps(asdict(result)))["rows_scored"] == 3253
        assert _int(None) is None
