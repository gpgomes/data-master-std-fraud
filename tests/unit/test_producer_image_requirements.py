"""A imagem dos producers tem de ter tudo que eles usam (issue #48).

A imagem (`docker/producer/Dockerfile`) instala só `docker/producer/requirements-producer.txt`, e
nada no CI a constrói ou a executa (os testes unitários mockam o Kafka). Por isso ela ficou meses
sem conseguir iniciar: o `ProducerConfig` pede compressão `lz4`, a lib não estava na imagem, e o
`KafkaProducer` abortava na criação (`Libraries for lz4 compression codec not found`), com o
container em crash loop. No host funcionava porque o `pyproject.toml` declara `lz4`.

Estes testes pegam a mesma classe de defeito sem subir Docker:

  * a lib do codec de compressão configurado está nos requirements da imagem;
  * todo pacote de terceiros que os dois producers importam (seguindo os imports dentro de `src/`)
    está nos requirements da imagem;
  * o que a imagem fixa é compatível com o que o `pyproject.toml` declara para o mesmo pacote.
"""

from __future__ import annotations

import ast
import sys
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from src.ingestion.streaming.producer_config import (
    HIGH_THROUGHPUT_CONFIG,
    LOW_LATENCY_CONFIG,
    ProducerConfig,
)

ROOT = Path(__file__).resolve().parents[2]
REQUIREMENTS = ROOT / "docker" / "producer" / "requirements-producer.txt"
PYPROJECT = ROOT / "pyproject.toml"
PRODUCER_MODULES = (
    "src.ingestion.streaming.kafka_producer_transactions",
    "src.ingestion.streaming.kafka_producer_market",
)

# Lib de que o kafka-python precisa para cada codec de compressão (None = nada a instalar).
CODEC_LIBRARIES: dict[str | None, str | None] = {
    None: None,
    "none": None,
    "gzip": None,  # biblioteca padrão
    "snappy": "python-snappy",
    "lz4": "lz4",
    "zstd": "zstandard",
}

# Nome do módulo importado -> pacote no PyPI, para os pacotes de terceiros que o código dos
# producers pode importar. Um import de terceiros fora desta tabela falha o teste: é preciso
# decidir se o pacote entra na imagem.
IMPORT_TO_DISTRIBUTION = {
    "kafka": "kafka-python",
    "faker": "faker",
    "numpy": "numpy",
    "loguru": "loguru",
    "pydantic": "pydantic",
    "pydantic_settings": "pydantic-settings",
    "dotenv": "python-dotenv",
}

# Imports de terceiros que existem no código alcançado pelos producers mas que eles nunca executam,
# com o motivo. Sem isto o rastreador (que enxerga imports dentro de funções, como o
# `from kafka import KafkaProducer` do `run()`) exigiria o pyspark na imagem.
LAZY_IMPORTS_NEVER_RUN_BY_PRODUCERS = {
    ("src.common.schemas", "pyspark"): (
        "as funções get_*_schema() importam o pyspark dentro delas de propósito, para os "
        "producers (e a API) não precisarem dele; só os jobs Spark as chamam"
    ),
}

# Pacotes que a imagem e o host resolvem com nomes diferentes de propósito. A imagem fixa o
# `kafka-python==2.0.2`; o `pyproject.toml` usa o fork `kafka-python-ng` (o mesmo módulo `kafka`).
KNOWN_NAME_DIVERGENCES = {"kafka-python"}


def _image_requirements() -> dict[str, Requirement]:
    requirements = {}
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            requirement = Requirement(line)
            requirements[canonicalize_name(requirement.name)] = requirement
    return requirements


def _pyproject_requirements() -> dict[str, Requirement]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    return {
        canonicalize_name(r.name): r for r in map(Requirement, data["project"]["dependencies"])
    }


def _resolve_source(module: str) -> Path | None:
    """Arquivo de um módulo interno (`src.x.y`), ou None se não é um módulo do repositório."""
    base = ROOT.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _imports(path: Path) -> set[str]:
    """Todo módulo importado no arquivo, em qualquer profundidade (inclusive dentro de funções,
    como o `from kafka import KafkaProducer` do `run()`), menos o que só vale para tipagem."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    type_checking_only = {
        id(node)
        for block in ast.walk(tree)
        if isinstance(block, ast.If)
        and isinstance(block.test, ast.Name)
        and block.test.id == "TYPE_CHECKING"
        for node in ast.walk(block)
    }
    found: set[str] = set()
    for node in ast.walk(tree):
        if id(node) in type_checking_only:
            continue
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.add(node.module)
            # `from src.common import schemas` importa o submódulo `src.common.schemas`
            found.update(f"{node.module}.{alias.name}" for alias in node.names)
    return found


def _third_party_imports(entry_modules: tuple[str, ...]) -> set[tuple[str, str]]:
    """Pacotes de terceiros usados pelos módulos de entrada e por tudo que eles importam de
    `src/`. Devolve pares (módulo interno que importa, pacote de topo)."""
    third_party: set[tuple[str, str]] = set()
    pending = list(entry_modules)
    seen: set[str] = set()
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        seen.add(module)
        source = _resolve_source(module)
        if source is None:
            continue
        for imported in _imports(source):
            top = imported.split(".")[0]
            if top == "src":
                pending.append(imported)
            elif top not in sys.stdlib_module_names and top != "__future__":
                third_party.add((module, top))
    return third_party


def _needed_by_producers() -> dict[str, str]:
    """{pacote de topo: um módulo que o importa}, sem os imports preguiçosos que nunca rodam."""
    needed: dict[str, str] = {}
    for module, top in sorted(_third_party_imports(PRODUCER_MODULES)):
        if (module, top) not in LAZY_IMPORTS_NEVER_RUN_BY_PRODUCERS:
            needed.setdefault(top, module)
    return needed


def _assert_codec_library_installed(config: ProducerConfig) -> None:
    codec = config.compression_type
    assert codec in CODEC_LIBRARIES, (
        f"codec {codec!r} desconhecido: diga em CODEC_LIBRARIES qual lib ele exige"
    )
    library = CODEC_LIBRARIES[codec]
    if library is None:
        return
    assert canonicalize_name(library) in _image_requirements(), (
        f"o ProducerConfig usa compression_type={codec!r}, que exige `{library}`, e ela não está "
        f"em {REQUIREMENTS.relative_to(ROOT)}: o container cairia em crash loop"
    )


class TestCompressionCodec:
    @pytest.mark.parametrize(
        "config",
        [ProducerConfig(), LOW_LATENCY_CONFIG, HIGH_THROUGHPUT_CONFIG],
        ids=["default", "low-latency", "high-throughput"],
    )
    def test_the_codec_library_is_installed_in_the_image(self, config):
        _assert_codec_library_installed(config)

    def test_a_missing_codec_library_would_be_caught(self, monkeypatch):
        """O teste acima não é vacuoso: sem `lz4` na imagem (o estado antes da issue #48) ele
        reprovaria."""
        installed = _image_requirements()
        installed.pop("lz4")
        monkeypatch.setattr(sys.modules[__name__], "_image_requirements", lambda: installed)
        with pytest.raises(AssertionError, match="compression_type='lz4'"):
            _assert_codec_library_installed(ProducerConfig())

    def test_an_unknown_codec_is_flagged_instead_of_silently_passing(self):
        with pytest.raises(AssertionError, match="desconhecido"):
            _assert_codec_library_installed(ProducerConfig(compression_type="brotli"))

    def test_the_default_codec_is_the_one_the_issue_is_about(self):
        """Sentinela: se o padrão mudar, este teste lembra de rever a tabela e o Dockerfile."""
        assert ProducerConfig().compression_type == "lz4"

    def test_the_lz4_requirement_is_not_optional_or_unpinned(self):
        requirement = _image_requirements()["lz4"]
        assert requirement.specifier, "a imagem fixa a versão dos demais pacotes; fixe a do lz4 também"
        assert requirement.marker is None

    @pytest.mark.parametrize("module", PRODUCER_MODULES)
    def test_the_producers_do_not_override_the_codec(self, module):
        """Os testes acima olham o padrão do `ProducerConfig`; um `compression_type=` explícito
        no producer os contornaria."""
        source = _resolve_source(module)
        assert source is not None
        tree = ast.parse(source.read_text(encoding="utf-8"))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "ProducerConfig"
        ]
        assert calls, f"{module} deveria instanciar ProducerConfig"
        assert all(kw.arg != "compression_type" for call in calls for kw in call.keywords)


class TestImportsAreInstalledInTheImage:
    def test_every_third_party_package_the_producers_import_is_in_the_requirements(self):
        imported = _needed_by_producers()
        assert imported, "o rastreador de imports não achou nada: o teste estaria vazio"

        unmapped = sorted(top for top in imported if top not in IMPORT_TO_DISTRIBUTION)
        assert not unmapped, (
            f"pacotes de terceiros sem mapeamento em IMPORT_TO_DISTRIBUTION: {unmapped}. "
            "Decida se entram em docker/producer/requirements-producer.txt e mapeie."
        )
        installed = _image_requirements()
        missing = {
            top: imported[top]
            for top, dist in IMPORT_TO_DISTRIBUTION.items()
            if top in imported and canonicalize_name(dist) not in installed
        }
        assert not missing, f"faltam em {REQUIREMENTS.relative_to(ROOT)}: {missing}"

    def test_the_tracer_sees_the_import_inside_run(self):
        """`from kafka import KafkaProducer` fica dentro de `run()`: o rastreador tem de ver."""
        assert "kafka" in _needed_by_producers()

    def test_the_tracer_follows_imports_into_src(self):
        # `settings` vem de src.common.config, que importa pydantic_settings
        assert "pydantic_settings" in _needed_by_producers()

    def test_a_third_party_package_missing_from_the_image_would_be_caught(self, monkeypatch):
        installed = _image_requirements()
        installed.pop("faker")
        monkeypatch.setattr(sys.modules[__name__], "_image_requirements", lambda: installed)
        with pytest.raises(AssertionError, match="faltam em"):
            self.test_every_third_party_package_the_producers_import_is_in_the_requirements()

    def test_the_lazy_pyspark_allowlist_is_real(self):
        """A exceção existe porque o import está de fato lá, dentro de função; se sumir do código,
        a entrada da lista vira lixo e deve sair."""
        for module, top in LAZY_IMPORTS_NEVER_RUN_BY_PRODUCERS:
            assert (module, top) in _third_party_imports(PRODUCER_MODULES)

    def test_the_image_does_not_install_the_dev_stack(self):
        """A imagem é enxuta de propósito: nada de pyspark/pandas/great-expectations."""
        installed = _image_requirements()
        assert not {"pyspark", "pandas", "great-expectations"} & set(installed)


class TestVersionsAgreeWithPyproject:
    def test_pins_in_the_image_satisfy_what_pyproject_declares(self):
        host = _pyproject_requirements()
        for name, requirement in _image_requirements().items():
            if name in KNOWN_NAME_DIVERGENCES or name not in host:
                continue
            pinned = next(iter(requirement.specifier)).version
            assert host[name].specifier.contains(pinned, prereleases=True), (
                f"{name}: a imagem fixa {pinned}, mas o pyproject.toml declara {host[name].specifier}"
            )

    def test_lz4_is_declared_on_both_sides(self):
        assert "lz4" in _pyproject_requirements()
        assert "lz4" in _image_requirements()
