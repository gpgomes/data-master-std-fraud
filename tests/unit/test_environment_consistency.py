"""Smoke tests de consistência entre Makefile, docker-compose.yml, .env.example e config.py.

Protege contra regressões como portas divergentes entre os arquivos de setup local
(issue #7) — cada um pode ser editado isoladamente sem lembrar dos outros.
"""

import re
from pathlib import Path

from src.common.config import MinIOSettings, PostgresSettings

ROOT = Path(__file__).resolve().parents[2]


def _read(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _env_example_value(key: str) -> str:
    match = re.search(rf"^{key}=(.+)$", _read(".env.example"), flags=re.MULTILINE)
    assert match, f"{key} não encontrado em .env.example"
    return match.group(1).strip()


class TestDockerComposePorts:
    def test_no_obsolete_version_attribute(self):
        content = _read("docker-compose.yml")
        assert not re.search(r"(?m)^version:\s*", content), (
            "Atributo 'version' é obsoleto no Docker Compose v2+ e deve ser removido"
        )

    def test_minio_external_port_default_matches_env_example(self):
        compose = _read("docker-compose.yml")
        match = re.search(r'"\$\{MINIO_EXTERNAL_PORT:-(\d+)\}:9000"', compose)
        assert match, "Porta externa do MinIO deve ser parametrizável via MINIO_EXTERNAL_PORT"

        expected_port = re.search(r":(\d+)$", _env_example_value("MINIO_ENDPOINT")).group(1)
        assert match.group(1) == expected_port == _env_example_value("MINIO_EXTERNAL_PORT")

    def test_postgres_external_port_default_matches_env_example(self):
        compose = _read("docker-compose.yml")
        match = re.search(r'"\$\{POSTGRES_EXTERNAL_PORT:-(\d+)\}:5432"', compose)
        assert match, (
            "Porta externa do Postgres (serving) deve ser parametrizável via "
            "POSTGRES_EXTERNAL_PORT"
        )
        assert match.group(1) == _env_example_value("POSTGRES_PORT")
        assert match.group(1) == _env_example_value("POSTGRES_EXTERNAL_PORT")


class TestConfigMatchesEnvExample:
    def test_minio_endpoint_default_matches_env_example(self):
        assert MinIOSettings().endpoint == _env_example_value("MINIO_ENDPOINT")

    def test_postgres_port_default_matches_env_example(self):
        assert str(PostgresSettings().port) == _env_example_value("POSTGRES_PORT")


class TestMakefilePython:
    def test_python_variable_is_not_windows_path(self):
        makefile = _read("Makefile")
        match = re.search(r"^PYTHON\s*\??=\s*(.+)$", makefile, flags=re.MULTILINE)
        assert match, "Variável PYTHON não encontrada no Makefile"
        assert "\\" not in match.group(1), (
            "PYTHON não pode apontar para um caminho Windows (.venv\\Scripts\\python) "
            "— o setup precisa ser reproduzível em macOS/Linux"
        )


class TestProducerSeeds:
    """O producer só enriquece com dim_customers se usar os mesmos clientes do seed-data (#43)."""

    def test_customer_seed_matches_seed_data_everywhere(self):
        from src.ingestion.streaming.kafka_producer_transactions import CUSTOMER_SEED

        assert CUSTOMER_SEED == int(_env_example_value("CUSTOMER_SEED")) == 42
        assert re.search(r'CUSTOMER_SEED:\s*"42"', _read("docker-compose.yml"))
        assert re.search(r'"--seed",\s*type=int,\s*default=42', _read("scripts/generate_sample_data.py"))
        assert '"seed": 42' in _read("dags/dag_seed_data.py")

    def test_event_seed_stays_time_based_by_default(self):
        """Fixar a seed dos eventos repetiria os mesmos transaction_id a cada reinício."""
        assert _env_example_value("GENERATOR_SEED") == "0"
        assert re.search(r'GENERATOR_SEED:\s*"0"', _read("docker-compose.yml"))
