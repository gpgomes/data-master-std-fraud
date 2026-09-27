"""Guardas do E2E (issue #57) que valem sem a stack: o namespace isolado não pode divergir entre o
Makefile e o harness, nem apontar para um bucket, tópico ou banco de desenvolvimento."""

from __future__ import annotations

import re
from pathlib import Path

from tests.e2e.harness import E2E_ENV, env_is_e2e

ROOT = Path(__file__).resolve().parents[2]


def _makefile_exports() -> dict[str, str]:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    block = makefile.split("E2E_EXPORTS =")[1].split("\n\n")[0]
    return dict(re.findall(r"([A-Z0-9_]+)=(\S+)", block))


def test_makefile_exports_the_same_namespace_as_the_harness():
    exports = _makefile_exports()
    assert exports.pop("E2E") == "1"
    assert exports == E2E_ENV


def test_the_namespace_never_points_at_development_resources():
    for key, value in E2E_ENV.items():
        if key.startswith(("MINIO_BUCKET_", "KAFKA_TOPIC_")):
            assert value.startswith("e2e-"), key
    assert E2E_ENV["POSTGRES_DB"].endswith("_e2e")


def test_without_the_namespace_the_suite_refuses_to_run(monkeypatch):
    for key in E2E_ENV:
        monkeypatch.delenv(key, raising=False)
    assert env_is_e2e() is False
    monkeypatch.setenv("MINIO_BUCKET_SILVER", "silver")
    assert env_is_e2e() is False


def test_the_workflow_runs_make_e2e_on_a_minimal_stack():
    workflow = (ROOT / ".github" / "workflows" / "e2e.yml").read_text(encoding="utf-8")
    assert "make e2e" in workflow
    assert "docker compose up -d --build zookeeper kafka minio postgres spark-master" in workflow
    assert "workflow_dispatch" in workflow
    assert "docker compose down -v" in workflow
