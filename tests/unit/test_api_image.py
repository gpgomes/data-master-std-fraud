"""A imagem da API tem todo o código de `src/` que ela importa (issue #58).

`docker/api/Dockerfile` copia só alguns pacotes de `src/` (e o `docker-compose.yml` monta os mesmos
por volume), para a imagem não levar o projeto inteiro. A issue #55 fez o `main.py` importar
`src.observability` sem copiá-lo: o container passou a morrer no import, e nada no CI percebeu
(os testes unitários rodam no host, onde `src/` inteiro está no path). Este teste segue os imports
a partir do `main.py` e exige que cada pacote interno alcançado esteja no Dockerfile e no compose.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "docker" / "api" / "Dockerfile"
COMPOSE = ROOT / "docker-compose.yml"
ENTRY = "src.serving.api.main"


def _source(module: str) -> Path | None:
    base = ROOT.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _internal_modules(entry: str) -> set[str]:
    """Módulos de `src/` alcançados a partir de `entry`, só pelos imports de topo de módulo
    (os de dentro de funções, como o pyspark de `schemas.get_*_schema`, a API nunca executa)."""
    seen: set[str] = set()
    pending = [entry]
    while pending:
        module = pending.pop()
        path = _source(module)
        if module in seen or path is None:
            continue
        seen.add(module)
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module] + [f"{node.module}.{a.name}" for a in node.names]
            pending += [n for n in names if n.split(".")[0] == "src"]
    return seen


def _copied(paths: list[str], module: str) -> bool:
    relative = module.replace(".", "/")
    return any(relative == p or relative.startswith(p + "/") for p in paths)


def _dockerfile_paths() -> list[str]:
    text = DOCKERFILE.read_text(encoding="utf-8")
    return [m.rstrip("/") for m in re.findall(r"^COPY\s+(src/\S*)\s", text, re.MULTILINE)]


def _compose_api_volumes() -> list[str]:
    block = COMPOSE.read_text(encoding="utf-8").split("\n  api:\n", 1)[1]
    block = re.split(r"\n  [a-z][\w-]*:\n", block, maxsplit=1)[0]
    return re.findall(r"-\s+\./(src/[^:]+):/app/src/", block)


def _packages_needed() -> list[str]:
    # só pacotes/módulos com código (o __init__ vazio de src.serving o Dockerfile cria à parte)
    return sorted(
        m for m in _internal_modules(ENTRY) if m not in {"src", "src.serving"}
    )


class TestApiImage:
    def test_every_internal_module_the_api_imports_is_copied_into_the_image(self):
        paths = _dockerfile_paths()
        missing = [m for m in _packages_needed() if not _copied(paths, m)]
        assert not missing, f"{DOCKERFILE.relative_to(ROOT)} não copia: {missing}"

    def test_the_compose_service_mounts_the_same_code(self):
        volumes = _compose_api_volumes()
        missing = [m for m in _packages_needed() if not _copied(volumes, m)]
        assert not missing, f"o serviço `api` do docker-compose.yml não monta: {missing}"

    def test_the_trace_reaches_observability(self):
        """Não vacuoso: o defeito original (#55) era justamente `src.observability`."""
        assert "src.observability.store" in _internal_modules(ENTRY)
        assert not _copied(["src/common", "src/serving/api"], "src.observability.store")

    def test_the_container_receives_the_api_key_hashes(self):
        """Sem a variável no container, a API sobe negando tudo (issue #58)."""
        block = COMPOSE.read_text(encoding="utf-8").split("\n  api:\n", 1)[1]
        assert "API_KEY_HASHES:" in re.split(r"\n  [a-z][\w-]*:\n", block, maxsplit=1)[0]
