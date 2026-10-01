"""Exceções versionadas dos scans de segurança (issue #58)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from scripts.security_exceptions import (
    EXCEPTIONS,
    ignore_file,
    load,
    main,
    pip_audit_args,
    problems,
)

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "security.yml"
TODAY = date(2026, 9, 28)


def _entry(**overrides):
    base = {"id": "CVE-1", "package": "x", "reason": "motivo", "review_by": date(2026, 12, 31)}
    return {**base, **overrides}


def _data(**sections):
    return {"pip_audit": [], "gitleaks": [], "trivy": [], **sections}


class TestVersionedFile:
    def test_every_exception_has_a_reason_and_a_review_date(self):
        """Só a estrutura: a data vencida é o job `exceptions` do workflow que cobra, não o unitário
        (senão o teste quebraria sozinho num dia qualquer)."""
        found = problems(load(), TODAY)
        structural = [p for p in found if "vencida" not in p and "a mais de" not in p]
        assert structural == []

    def test_pyspark_is_the_only_dependency_exception(self):
        """A decisão da #58: atualizar tudo e manter só o pyspark, preso à versão do cluster."""
        assert {e["package"] for e in load()["pip_audit"]} == {"pyspark"}

    def test_gitleaks_fingerprints_have_the_git_scan_format(self):
        for entry in load()["gitleaks"]:
            assert re.fullmatch(r"[0-9a-f]{40}:[^:]+:[\w-]+:\d+", entry["fingerprint"])

    def test_generated_files_are_not_versioned(self):
        ignored = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        assert ".gitleaksignore" in ignored and ".trivyignore" in ignored


class TestProblems:
    def test_a_valid_exception_passes(self):
        assert problems(_data(trivy=[_entry()]), TODAY) == []

    def test_expired_review_blocks(self):
        found = problems(_data(trivy=[_entry(review_by=date(2026, 9, 27))]), TODAY)
        assert found == ["trivy:CVE-1: revisão vencida em 2026-09-27"]

    def test_review_too_far_in_the_future_blocks(self):
        found = problems(_data(trivy=[_entry(review_by=date(2028, 1, 1))]), TODAY)
        assert "a mais de" in found[0]

    @pytest.mark.parametrize(
        ("override", "message"),
        [
            ({"reason": "  "}, "sem `reason`"),
            ({"review_by": "2026-12-31"}, "não é uma data"),
            ({"id": ""}, "exceção sem `id`"),
        ],
    )
    def test_incomplete_exception_blocks(self, override, message):
        found = problems(_data(pip_audit=[_entry(**override)]), TODAY)
        assert len(found) == 1 and message in found[0]

    def test_duplicate_blocks(self):
        found = problems(_data(pip_audit=[_entry(), _entry()]), TODAY)
        assert found == ["pip_audit:CVE-1: duplicada"]

    def test_unknown_section_is_rejected(self, tmp_path):
        path = tmp_path / "e.toml"
        path.write_text('[[snyk]]\nid = "x"\n', encoding="utf-8")
        with pytest.raises(ValueError, match="snyk"):
            load(path)


class TestGeneratedOutputs:
    def test_pip_audit_args(self):
        data = _data(pip_audit=[_entry(id="A"), _entry(id="B")])
        assert pip_audit_args(data) == ["--ignore-vuln", "A", "--ignore-vuln", "B"]

    def test_ignore_files_list_one_identifier_per_line(self):
        data = _data(gitleaks=[{"fingerprint": "abc:f:rule:1"}], trivy=[_entry(id="CVE-9")])
        assert ignore_file(data, "gitleaks").splitlines()[1:] == ["abc:f:rule:1"]
        assert ignore_file(data, "trivy").splitlines()[1:] == ["CVE-9"]

    def test_cli_writes_the_ignore_file(self, tmp_path, capsys):
        out = tmp_path / ".gitleaksignore"
        assert main(["gitleaksignore", "--out", str(out)]) == 0
        assert len(out.read_text(encoding="utf-8").splitlines()) == 1 + len(load()["gitleaks"])

    def test_cli_check_fails_on_an_expired_file(self, tmp_path, capsys):
        path = tmp_path / "e.toml"
        path.write_text(
            '[[trivy]]\nid = "CVE-1"\nreason = "r"\nreview_by = 2000-01-01\n', encoding="utf-8"
        )
        assert main(["check", "--file", str(path)]) == 1
        assert "::error" in capsys.readouterr().out


class TestWorkflow:
    """O security.yml usa este arquivo e bloqueia de fato (issue #58)."""

    @pytest.fixture(scope="class")
    def text(self) -> str:
        return WORKFLOW.read_text(encoding="utf-8")

    def test_runs_on_prs_and_on_a_schedule(self, text):
        assert "pull_request:" in text and "schedule:" in text

    def test_every_blocking_scan_reads_the_exceptions(self, text):
        for command in ("check", "pip-audit-args", "gitleaksignore", "trivyignore"):
            assert f"scripts.security_exceptions {command}" in text

    def test_trivy_blocks_on_our_images(self, text):
        assert "--exit-code 1" in text
        assert "--severity HIGH,CRITICAL" in text

    def test_no_blocking_step_is_allowed_to_fail(self, text):
        """`continue-on-error` só no relatório das imagens base (Airflow/Spark), nunca num gate."""
        blocks = re.split(r"\n  (?=[\w-]+:\n)", text.split("\njobs:\n", 1)[1])
        for block in blocks:
            if "continue-on-error" in block:
                assert block.startswith("base-images-report:"), block.splitlines()[0]

    def test_exceptions_file_is_where_the_workflow_says(self):
        assert EXCEPTIONS == ROOT / "security" / "exceptions.toml"
