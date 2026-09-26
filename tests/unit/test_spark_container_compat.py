"""O container do Spark roda Python 3.8 e não tem numpy nem pandas; o CI e a máquina de dev, não.

O job de streaming e o `silver_to_gold` executam **dentro** desse container, e o CI (Python 3.11) não
tem como pegar um `zip(strict=True)`, um `dict[str, float]` avaliado em tempo de execução ou um
`import numpy` no topo de um módulo. Isso já quebrou uma vez o Fraud Engine da #45 ao chegar no
streaming (#46). Estes testes olham o código dos módulos que rodam lá dentro.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Módulos importados pelos jobs que rodam no container do Spark (Python 3.8, sem numpy/pandas).
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
    "src/transformation/batch/silver_to_gold.py",
]
HEAVY_PACKAGES = {"numpy", "pandas", "scipy", "sklearn", "matplotlib"}
BUILTIN_GENERICS = {"dict", "list", "tuple", "set", "frozenset", "type"}
PY39_STRING_METHODS = {"removeprefix", "removesuffix"}


def _tree(relative: str) -> ast.Module:
    source = (ROOT / relative).read_text(encoding="utf-8")
    return ast.parse(source, filename=relative, feature_version=(3, 8))


def _has_future_annotations(tree: ast.Module) -> bool:
    return any(
        isinstance(n, ast.ImportFrom)
        and n.module == "__future__"
        and any(a.name == "annotations" for a in n.names)
        for n in tree.body
    )


def _is_type_checking_block(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "TYPE_CHECKING"
    )


def _uses_py39_annotation_syntax(annotation: ast.AST) -> bool:
    """`dict[str, float]` ou `X | Y`: só valem em tempo de execução no Python 3.9/3.10+."""
    for node in ast.walk(annotation):
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id in BUILTIN_GENERICS
        ):
            return True
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return True
    return False


def _evaluated_annotations(tree: ast.Module) -> list[ast.AST]:
    """Anotações que o Python 3.8 avalia ao carregar o módulo (sem `from __future__`)."""
    found: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and _is_module_or_class_level(tree, node):
            found.append(node.annotation)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            args = node.args
            for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs, args.vararg, args.kwarg]:
                if arg is not None and arg.annotation is not None:
                    found.append(arg.annotation)
            if node.returns is not None:
                found.append(node.returns)
    return found


def _is_module_or_class_level(tree: ast.Module, target: ast.AnnAssign) -> bool:
    """Anotações de variável local não são avaliadas; as de módulo e de classe são."""
    for parent in ast.walk(tree):
        if isinstance(parent, ast.FunctionDef | ast.AsyncFunctionDef):
            if any(child is target for child in ast.walk(parent)):
                return False
    return True


@pytest.mark.parametrize("relative", CONTAINER_MODULES)
class TestContainerModule:
    def test_parses_as_python_3_8(self, relative: str) -> None:
        _tree(relative)  # `feature_version=(3, 8)` recusa sintaxe mais nova (match, etc.)

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

    def test_does_not_use_zip_strict_or_py39_string_methods(self, relative: str) -> None:
        for node in ast.walk(_tree(relative)):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "zip"
            ):
                assert all(k.arg != "strict" for k in node.keywords), f"{relative}: zip(strict=)"
            if isinstance(node, ast.Attribute):
                assert node.attr not in PY39_STRING_METHODS, f"{relative}: .{node.attr}()"

    def test_builtin_generic_annotations_need_the_future_import(self, relative: str) -> None:
        tree = _tree(relative)
        if _has_future_annotations(tree):
            return
        offenders = [a for a in _evaluated_annotations(tree) if _uses_py39_annotation_syntax(a)]
        assert not offenders, f"{relative}: anotação avaliada em tempo de execução no Python 3.8"


class TestTheGuardsAreNotVacuous:
    """Os detectores acima precisam de fato reconhecer os três defeitos que já aconteceram."""

    def _check(self, source: str) -> ast.Module:
        return ast.parse(source, feature_version=(3, 8))

    def test_flags_a_module_level_builtin_generic_without_the_future_import(self) -> None:
        tree = self._check("WEIGHTS: dict[str, float] = {}\n")
        assert not _has_future_annotations(tree)
        assert any(_uses_py39_annotation_syntax(a) for a in _evaluated_annotations(tree))

    def test_accepts_the_same_annotation_with_the_future_import(self) -> None:
        tree = self._check("from __future__ import annotations\nWEIGHTS: dict[str, float] = {}\n")
        assert _has_future_annotations(tree)

    def test_ignores_local_variable_annotations(self) -> None:
        tree = self._check("def f():\n    x: list[int] = []\n    return x\n")
        assert not any(_uses_py39_annotation_syntax(a) for a in _evaluated_annotations(tree))

    def test_flags_a_union_in_a_signature(self) -> None:
        tree = self._check("def f(x: int | None) -> None: ...\n")
        assert any(_uses_py39_annotation_syntax(a) for a in _evaluated_annotations(tree))

    def test_recognises_zip_strict_and_a_type_checking_guard(self) -> None:
        tree = self._check("import numpy\nzip(a, b, strict=True)\n")
        assert any(isinstance(n, ast.Import) for n in tree.body)
        guarded = self._check("if TYPE_CHECKING:\n    import numpy\n")
        assert _is_type_checking_block(guarded.body[0])


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
