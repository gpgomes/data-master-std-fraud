"""Guardrails da infraestrutura Terraform (issue #17): custo, segurança e consistência.

Só parseiam os arquivos .tf (python-hcl2) — não exigem o binário do Terraform nem
credenciais AWS, então rodam em `make test-unit` e no CI. O que precisa do binário
(fmt, validate, tflint, checkov) roda via `make tf-check` e no job `terraform` do CI.

Protegem as decisões do projeto contra regressão silenciosa: teto de US$ 50/mês,
us-east-1, sem credencial estática, IAM sem curinga, um único ambiente (dev) e buckets
alinhados com o que o app espera (config.py).
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import hcl2
import pytest

from src.common.config import MinIOSettings

ROOT = Path(__file__).resolve().parents[2]
TF = ROOT / "terraform"
DEV = TF / "environments" / "dev"

# Ações somente-leitura de configuração: única categoria que pode usar resource "*".
READ_ONLY_ACTION = re.compile(r"^[a-z0-9-]+:(Get|List|Describe|View)[A-Za-z*]*$")


# ── Helpers ────────────────────────────────────────────────────────────────────


def _unquote(value: Any) -> Any:
    """python-hcl2 devolve literais string com as aspas embutidas ('"abc"')."""
    if isinstance(value, str) and len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1]
    return value


def _load(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return hcl2.load(f)


def _tf_files() -> list[Path]:
    return sorted(p for p in TF.rglob("*.tf") if ".terraform" not in p.parts)


def _named(path: Path, kind: str) -> dict[str, dict[str, Any]]:
    """Blocos de um rótulo (variable, output, module): nome -> corpo."""
    found: dict[str, dict[str, Any]] = {}
    for item in _load(path).get(kind, []):
        for label, body in item.items():
            found[_unquote(label)] = body
    return found


def _typed(path: Path, kind: str) -> Iterator[tuple[str, str, dict[str, Any]]]:
    """Blocos de dois rótulos (resource, data): (tipo, nome, corpo)."""
    for item in _load(path).get(kind, []):
        for type_label, named in item.items():
            for name_label, body in named.items():
                yield _unquote(type_label), _unquote(name_label), body


def _dir_named(directory: Path, kind: str) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob("*.tf")):
        found.update(_named(path, kind))
    return found


def _policy_statements() -> Iterator[tuple[str, str, dict[str, Any]]]:
    """Todo statement de todo aws_iam_policy_document: (arquivo, documento, statement)."""
    for path in _tf_files():
        for res_type, name, body in _typed(path, "data"):
            if res_type == "aws_iam_policy_document":
                for statement in body.get("statement", []):
                    yield path.relative_to(ROOT).as_posix(), name, statement


def _is_allow(statement: dict[str, Any]) -> bool:
    return _unquote(statement.get("effect", '"Allow"')) == "Allow"


def _actions(statement: dict[str, Any]) -> list[str]:
    return [_unquote(a) for a in statement.get("actions", [])]


def _resources(statement: dict[str, Any]) -> list[str]:
    return [_unquote(r) for r in statement.get("resources", [])]


# ── Guarda de custo (US$ 50/mês) ───────────────────────────────────────────────


class TestBudgetGuard:
    def test_bootstrap_budget_limit_is_50_usd(self):
        variables = _dir_named(TF / "bootstrap", "variable")
        assert variables["budget_limit_usd"]["default"] == 50

    def test_bootstrap_budget_module_uses_the_limit_variable(self):
        module = _dir_named(TF / "bootstrap", "module")["budget"]
        assert module["limit_usd"] == "${var.budget_limit_usd}"

    def test_alerts_at_50_80_100_percent_actual_and_100_forecast(self):
        variables = _named(TF / "modules/budget/variables.tf", "variable")
        assert variables["actual_thresholds"]["default"] == [50, 80, 100]
        assert variables["forecast_threshold"]["default"] == 100

        budget = next(
            body
            for res_type, _, body in _typed(TF / "modules/budget/main.tf", "resource")
            if res_type == "aws_budgets_budget"
        )
        assert _unquote(budget["budget_type"]) == "COST"
        assert _unquote(budget["time_unit"]) == "MONTHLY"
        assert _unquote(budget["limit_unit"]) == "USD"
        forecast = [n for n in budget["notification"] if "FORECASTED" in n["notification_type"]]
        assert len(forecast) == 1
        actual = budget["dynamic"][0]['"notification"']["content"][0]
        assert _unquote(actual["notification_type"]) == "ACTUAL"

    def test_credits_do_not_hide_real_spend(self):
        budget = next(
            body
            for res_type, _, body in _typed(TF / "modules/budget/main.tf", "resource")
            if res_type == "aws_budgets_budget"
        )
        assert budget["cost_types"][0]["include_credit"] is False

    def test_alert_emails_have_no_default_so_none_are_versioned(self):
        module_vars = _named(TF / "modules/budget/variables.tf", "variable")
        bootstrap_vars = _dir_named(TF / "bootstrap", "variable")
        assert "default" not in module_vars["subscriber_emails"]
        assert "default" not in bootstrap_vars["budget_alert_emails"]


# ── Sem credencial/segredo versionado ──────────────────────────────────────────

SECRET_PATTERNS = {
    "AWS access key id": re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),
    "chave estática atribuída": re.compile(
        r"(?i)\b(aws_secret_access_key|secret_access_key|secret_key|access_key)\s*="
    ),
    "senha literal": re.compile(r'(?i)\bpassword\s*=\s*"[^"$]'),
    "chave privada": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
}


def _scannable_files() -> list[Path]:
    return sorted(
        p
        for p in TF.rglob("*")
        if p.is_file()
        and ".terraform" not in p.parts
        and p.name != ".terraform.lock.hcl"
        and (p.suffix in {".tf", ".hcl"} or p.name.endswith(".example"))
    )


class TestNoVersionedSecrets:
    @pytest.mark.parametrize("label", sorted(SECRET_PATTERNS))
    def test_no_secret_in_terraform_files(self, label: str):
        offenders = [
            p.relative_to(ROOT).as_posix()
            for p in _scannable_files()
            if SECRET_PATTERNS[label].search(p.read_text(encoding="utf-8"))
        ]
        assert not offenders, f"{label} em: {offenders}"

    def test_scan_actually_covers_the_terraform_tree(self):
        # Protege o teste acima de passar "no vazio" se a árvore mudar de lugar.
        names = {p.name for p in _scannable_files()}
        assert {"main.tf", "backend.hcl.example", "terraform.tfvars.example"} <= names

    def test_gitignore_keeps_state_plans_and_local_vars_out_of_git(self):
        lines = {
            line.strip()
            for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        }
        assert {"*.tfstate", "*.tfplan", "*.tfvars", "backend.hcl", "**/.terraform/"} <= lines

    def test_lock_files_are_versioned_not_ignored(self):
        lines = [
            line
            for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
            if not line.startswith("#")
        ]
        assert not any(".terraform.lock.hcl" in line for line in lines)
        for root in ("bootstrap", "environments/dev"):
            assert (TF / root / ".terraform.lock.hcl").is_file(), f"lock ausente em {root}"


# ── Módulo s3 ──────────────────────────────────────────────────────────────────


class TestS3Module:
    @pytest.fixture(scope="class")
    def resources(self) -> dict[str, dict[str, Any]]:
        return {
            res_type: body for res_type, _, body in _typed(TF / "modules/s3/main.tf", "resource")
        }

    def test_bucket_is_hardened(self, resources):
        required = {
            "aws_s3_bucket_public_access_block",
            "aws_s3_bucket_versioning",
            "aws_s3_bucket_server_side_encryption_configuration",
            "aws_s3_bucket_lifecycle_configuration",
            "aws_s3_bucket_ownership_controls",
            "aws_s3_bucket_policy",
        }
        assert required <= set(resources)

    def test_public_access_is_fully_blocked(self, resources):
        block = resources["aws_s3_bucket_public_access_block"]
        for flag in (
            "block_public_acls",
            "block_public_policy",
            "ignore_public_acls",
            "restrict_public_buckets",
        ):
            assert block[flag] is True, flag

    def test_versioning_is_enabled(self, resources):
        config = resources["aws_s3_bucket_versioning"]["versioning_configuration"][0]
        assert _unquote(config["status"]) == "Enabled"

    def test_insecure_transport_is_denied(self):
        tls = [
            st
            for path, name, st in _policy_statements()
            if path.endswith("modules/s3/main.tf") and name == "tls_only"
        ]
        assert len(tls) == 1
        condition = tls[0]["condition"][0]
        assert not _is_allow(tls[0])
        assert _unquote(condition["variable"]) == "aws:SecureTransport"
        assert [_unquote(v) for v in condition["values"]] == ["false"]

    def test_buckets_cannot_be_force_destroyed_by_default(self):
        variables = _named(TF / "modules/s3/variables.tf", "variable")
        assert variables["force_destroy"]["default"] is False
        env_vars = _dir_named(DEV, "variable")
        assert env_vars["force_destroy_buckets"]["default"] is False

    def test_encryption_defaults_to_sse_s3_without_paid_kms_key(self):
        variables = _named(TF / "modules/s3/variables.tf", "variable")
        assert variables["kms_key_arn"]["default"] is None


# ── IAM: least privilege ───────────────────────────────────────────────────────


class TestIamLeastPrivilege:
    def test_policy_documents_were_found(self):
        # Sem isto os testes abaixo passariam "no vazio" se o parser não achasse nada.
        documents = {(path, name) for path, name, _ in _policy_statements()}
        assert len(documents) >= 4

    def test_no_allow_statement_grants_wildcard_actions(self):
        for path, name, statement in _policy_statements():
            if not _is_allow(statement):
                continue
            for action in _actions(statement):
                assert action != "*" and not action.endswith(":*"), f"{path}:{name}: {action}"

    def test_wildcard_resource_only_on_read_only_actions(self):
        for path, name, statement in _policy_statements():
            if _is_allow(statement) and "*" in _resources(statement):
                for action in _actions(statement):
                    assert READ_ONLY_ACTION.match(action), f"{path}:{name}: {action} com resource *"

    def test_github_plan_role_is_read_only(self):
        statements = [st for _, name, st in _policy_statements() if name == "github_plan"]
        assert statements, "documento github_plan não encontrado"
        for statement in statements:
            assert _is_allow(statement)
            for action in _actions(statement):
                assert READ_ONLY_ACTION.match(action), f"ação de escrita na role de plan: {action}"

    def test_github_trust_is_pinned_to_this_repository(self):
        trust = [st for _, name, st in _policy_statements() if name == "github_plan_trust"]
        assert len(trust) == 1
        subs = [c for c in trust[0]["condition"] if _unquote(c["variable"]).endswith(":sub")]
        assert len(subs) == 1
        values = [_unquote(v) for v in subs[0]["values"]]
        assert values
        for value in values:
            assert "*" not in value
            assert value.startswith("repo:${var.github_repository}:")

    def test_no_admin_managed_policy_is_attached(self):
        for path in _tf_files():
            text = path.read_text(encoding="utf-8")
            assert "AdministratorAccess" not in text, path
            assert "PowerUserAccess" not in text, path


# ── Único ambiente: dev ────────────────────────────────────────────────────────


class TestDevIsTheOnlyEnvironment:
    def test_no_other_environment_directory_exists(self):
        environments = sorted(p.name for p in (TF / "environments").iterdir() if p.is_dir())
        assert environments == ["dev"], "decisão do projeto: só existe o ambiente dev"

    def test_terraform_tree_has_no_prod_reference(self):
        offenders = [
            p.relative_to(ROOT).as_posix()
            for p in _scannable_files()
            if re.search(r"(?i)\bprod\b", p.read_text(encoding="utf-8"))
        ]
        assert not offenders, f"referência a prod em: {offenders}"

    def test_environment_is_the_dev_constant(self):
        locals_ = _load(DEV / "main.tf")["locals"][0]
        assert _unquote(locals_["environment"]) == "dev"
        variables = _dir_named(DEV, "variable")
        assert "environment" not in variables

    def test_backend_is_partial_so_no_bucket_is_versioned(self):
        versions = (DEV / "versions.tf").read_text(encoding="utf-8")
        assert re.search(r'backend\s+"s3"\s*\{\s*\}', versions)
        example = (DEV / "backend.hcl.example").read_text(encoding="utf-8")
        assert 'key          = "dev/terraform.tfstate"' in example
        assert "use_lockfile = true" in example

    @pytest.mark.parametrize("root", ["bootstrap", "environments/dev"])
    def test_region_is_us_east_1(self, root: str):
        variables = _dir_named(TF / root, "variable")
        assert _unquote(variables["region"]["default"]) == "us-east-1"

    def test_env_example_region_matches_terraform(self):
        text = (ROOT / ".env.example").read_text(encoding="utf-8")
        match = re.search(r"^AWS_REGION=(.+)$", text, flags=re.MULTILINE)
        assert match
        dev = _dir_named(DEV, "variable")
        assert match.group(1).strip() == _unquote(dev["region"]["default"])

    def test_bootstrap_backend_example_points_at_the_dev_state_key(self):
        outputs = _dir_named(TF / "bootstrap", "output")
        assert (
            'key          = "dev/terraform.tfstate"' in outputs["backend_config_example"]["value"]
        )


# ── Consistência com o app (config.py) ─────────────────────────────────────────


class TestAppConsistency:
    LAYERS = sorted(
        name.removeprefix("bucket_")
        for name in MinIOSettings.model_fields
        if name.startswith("bucket_")
    )

    def test_app_layers_were_discovered(self):
        assert self.LAYERS == ["bronze", "checkpoints", "gold", "silver"]

    def test_each_app_bucket_setting_has_a_terraform_output(self):
        outputs = _dir_named(DEV, "output")
        for layer in self.LAYERS:
            assert f"bucket_{layer}" in outputs, f"falta output bucket_{layer} no dev"

    def test_data_lake_buckets_match_app_layers(self):
        module = _dir_named(DEV, "module")["s3"]
        assert sorted(module["buckets"]) == self.LAYERS

    def test_iam_receives_every_data_lake_bucket(self):
        module = _dir_named(DEV, "module")["iam"]
        assert module["datalake_bucket_arns"] == "${module.s3.bucket_arns}"
