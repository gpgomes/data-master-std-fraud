"""Segredos fora do repositório e gerados no setup (`make env`, issue #58)."""

from __future__ import annotations

import base64
import re
import stat
from pathlib import Path

import pytest

from scripts.ensure_env import GENERATED, ensure_env, parse
from src.serving.api.security import hash_key

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / ".env.example"
COMPOSE = ROOT / "docker-compose.yml"
# Variáveis que o docker-compose.yml exige do .env (`${VAR:?...}`) em vez de fixar um valor.
COMPOSE_SECRETS = (
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__WEBSERVER__SECRET_KEY",
    "SUPERSET_SECRET_KEY",
)


@pytest.fixture
def env_path(tmp_path: Path) -> Path:
    return tmp_path / ".env"


def _values(path: Path) -> dict[str, str]:
    return parse(path.read_text(encoding="utf-8").splitlines())


class TestVersionedFilesHaveNoSecrets:
    def test_env_example_leaves_every_generated_secret_empty(self):
        values = parse(EXAMPLE.read_text(encoding="utf-8").splitlines())
        for key in (*GENERATED, "API_KEY_HASHES"):
            assert key in values, f"{key} some do .env.example"
            assert values[key] == "", f"{key} tem valor no .env.example: {values[key]!r}"

    @pytest.mark.parametrize("key", COMPOSE_SECRETS)
    def test_compose_requires_the_secret_from_the_env(self, key):
        """Nada de valor fixo: sem o .env, o compose falha com a dica em vez de usar uma chave pública."""
        lines = [ln.strip() for ln in COMPOSE.read_text(encoding="utf-8").splitlines()]
        uses = [ln for ln in lines if ln.startswith(f"{key}:")]
        assert uses, f"{key} não aparece no docker-compose.yml"
        for line in uses:
            assert re.fullmatch(rf"{key}: \$\{{{key}:\?rode make env\}}", line), line

    def test_the_old_public_keys_are_gone(self):
        text = COMPOSE.read_text(encoding="utf-8")
        for leaked in ("46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLt0nfd4e0=", "data-master-local-secret-key"):
            assert leaked not in text


class TestEnsureEnv:
    def test_creates_the_file_from_the_example_and_fills_every_secret(self, env_path):
        generated = ensure_env(env_path, EXAMPLE)
        values = _values(env_path)

        assert set(generated) == {*GENERATED, "API_KEY_HASHES"}
        assert all(values[k] for k in GENERATED)
        # o resto do exemplo chega intacto
        assert values["POSTGRES_DB"] == "fraud_analytics"
        assert stat.S_IMODE(env_path.stat().st_mode) == 0o600

    def test_fernet_key_has_the_format_airflow_expects(self, env_path):
        ensure_env(env_path, EXAMPLE)
        key = _values(env_path)["AIRFLOW__CORE__FERNET_KEY"]
        assert len(base64.urlsafe_b64decode(key)) == 32

    def test_the_stored_hash_authenticates_the_generated_dev_key(self, env_path):
        ensure_env(env_path, EXAMPLE)
        values = _values(env_path)
        assert values["API_KEY_HASHES"] == hash_key(values["API_DEV_KEY"])

    def test_each_run_generates_different_secrets(self, tmp_path):
        a, b = tmp_path / "a.env", tmp_path / "b.env"
        ensure_env(a, EXAMPLE)
        ensure_env(b, EXAMPLE)
        assert all(_values(a)[k] != _values(b)[k] for k in GENERATED)

    def test_is_idempotent_and_never_rotates_an_existing_secret(self, env_path):
        ensure_env(env_path, EXAMPLE)
        before = env_path.read_text(encoding="utf-8")

        assert ensure_env(env_path, EXAMPLE) == []
        assert env_path.read_text(encoding="utf-8") == before

    def test_old_placeholders_are_replaced_and_real_values_kept(self, env_path):
        """Um .env copiado do exemplo antigo (`your-...`) é corrigido; o que o dev pôs, preservado."""
        env_path.write_text(
            "AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here\n"
            "SUPERSET_SECRET_KEY=minha-chave\n"
            f"API_KEY_HASHES={hash_key('outra')}\n",
            encoding="utf-8",
        )
        ensure_env(env_path, EXAMPLE)
        values = _values(env_path)

        assert values["AIRFLOW__CORE__FERNET_KEY"] != "your-fernet-key-here"
        assert values["SUPERSET_SECRET_KEY"] == "minha-chave"
        # a chave que já existia continua aceita, ao lado da de dev
        assert values["API_KEY_HASHES"].split(",") == [
            hash_key("outra"),
            hash_key(values["API_DEV_KEY"]),
        ]
