"""O container do Spark roda Python 3.10 e não tem numpy nem pandas; o CI e a máquina de dev, sim.

O job de streaming e o `silver_to_gold` executam **dentro** desse container, e o CI (Python 3.11) não
tem como pegar sintaxe mais nova que a do container nem um `import numpy` no topo de um módulo. Isso
já quebrou o Fraud Engine da #45 ao chegar no streaming (#46), quando o container era Python 3.8.
Com a base `apache/spark:3.5.8` (issue #66) o piso subiu para 3.10: `dict[str, float]`, `X | Y` e
`zip(strict=)` passaram a valer lá dentro, e os testes que barravam isso saíram.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Versão do Python da imagem base do Spark. Ao trocar a base, confira com
# `docker run --rm --entrypoint python3 <imagem> --version` e atualize as duas constantes.
SPARK_BASE_IMAGE = "apache/spark:3.5.8"
CONTAINER_PYTHON = (3, 10)

# Módulos importados pelos jobs que rodam no container do Spark (Python 3.10, sem numpy/pandas).
CONTAINER_MODULES = [
    "src/common/schemas.py",
    "src/transformation/fraud/__init__.py",
    "src/transformation/fraud/profile.py",
    "src/transformation/fraud/signals.py",
    "src/transformation/fraud/scoring.py",
    "src/transformation/fraud/fraud_type.py",
    "src/transformation/fraud/detector.py",
    "src/transformation/fraud/weights.py",
    "src/transformation/streaming/stream_processor.py",
    "src/transformation/streaming/metrics_listener.py",
    "src/observability/__init__.py",
    "src/observability/store.py",
    "src/common/config.py",
    "src/serving/loaders/gold_to_postgres.py",
    "src/transformation/batch/silver_to_gold.py",
]
HEAVY_PACKAGES = {"numpy", "pandas", "scipy", "sklearn", "matplotlib"}


def _tree(relative: str) -> ast.Module:
    source = (ROOT / relative).read_text(encoding="utf-8")
    return ast.parse(source, filename=relative, feature_version=CONTAINER_PYTHON)


def _is_type_checking_block(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "TYPE_CHECKING"
    )


@pytest.mark.parametrize("relative", CONTAINER_MODULES)
class TestContainerModule:
    def test_parses_with_the_container_python(self, relative: str) -> None:
        _tree(relative)  # `feature_version` recusa sintaxe mais nova que a do container

    def test_does_not_import_numpy_or_pandas_at_module_level(self, relative: str) -> None:
        tree = _tree(relative)
        for node in tree.body:
            if _is_type_checking_block(node):
                continue
            if isinstance(node, ast.Import):
                modules = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module.split(".")[0]]
            else:
                continue
            assert not (set(modules) & HEAVY_PACKAGES), f"{relative}: import de {modules} no topo"


class TestTheGuardsAreNotVacuous:
    def test_rejects_syntax_newer_than_the_container(self) -> None:
        """`except*` é do 3.11: o parse com a versão do container tem de recusar."""
        source = "try:\n    pass\nexcept* ValueError:\n    pass\n"
        with pytest.raises(SyntaxError):
            ast.parse(source, feature_version=CONTAINER_PYTHON)

    def test_recognises_a_type_checking_guard(self) -> None:
        guarded = ast.parse("if TYPE_CHECKING:\n    import numpy\n")
        assert _is_type_checking_block(guarded.body[0])


def test_the_constants_match_the_spark_dockerfile() -> None:
    """Sentinela: trocar a base do Spark sem rever CONTAINER_PYTHON faz este teste falhar."""
    dockerfile = (ROOT / "docker" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^FROM (\S+)", dockerfile, re.MULTILINE).group(1) == SPARK_BASE_IMAGE
    # O harness, o Makefile e as DAGs chamam `spark-submit` pelo nome: a imagem base não o põe no
    # PATH, e o que existia vinha do pyspark instalado pelo delta-spark, removido na #66.
    assert 'ENV PATH="/opt/spark/bin:${PATH}"' in dockerfile
    # HOME do usuário spark é /nonexistent na base 3.5.8: o Ivy do `--packages` precisa de um HOME real.
    assert "ENV HOME=/home/spark" in dockerfile


def test_the_streaming_path_imports_without_numpy_or_pandas() -> None:
    """Importa os módulos do container num processo onde numpy e pandas não existem."""
    code = (
        "import sys\n"
        "sys.modules['numpy'] = None\n"
        "sys.modules['pandas'] = None\n"
        "import src.transformation.fraud.detector\n"
        "import src.transformation.fraud.profile\n"
        "import src.transformation.fraud.weights\n"
        "import src.transformation.streaming.stream_processor\n"
        "import src.transformation.batch.silver_to_gold\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=180
    )
    assert result.returncode == 0, result.stderr[-1500:]
    assert "ok" in result.stdout
