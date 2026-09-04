"""Data Context do Great Expectations (issue #13).

FileDataContext: suites e checkpoints ficam versionados como JSON/YAML em
`src/governance/great_expectations/gx/{expectations,checkpoints}/` — a pasta
`gx/uncommitted/` (data docs renderizados, resultados de validação,
`config_variables.yml`) é gitignored pelo próprio GX (não é config, é saída
de execução).
"""

from __future__ import annotations

from pathlib import Path

import great_expectations as gx
from great_expectations.data_context import FileDataContext

PROJECT_ROOT_DIR = Path(__file__).parent


def get_context(project_root_dir: Path | str = PROJECT_ROOT_DIR) -> FileDataContext:
    """Cria ou carrega o FileDataContext (idempotente — chamado a cada run).

    Desabilita a telemetria anônima do GX (habilitada por padrão, envia dados
    de uso para os servidores da própria Great Expectations) — consistente
    com a postura deste projeto sobre dados (ex.: issue #8/#9, sem envio de
    PII ou dados de execução para terceiros sem necessidade).
    """
    Path(project_root_dir).mkdir(parents=True, exist_ok=True)
    # mypy não resolve gx.get_context estaticamente (grande API pública
    # exposta dinamicamente pelo pacote) — verificado empiricamente que existe.
    context: FileDataContext = gx.get_context(  # type: ignore[attr-defined]
        mode="file", project_root_dir=str(project_root_dir)
    )
    if context.anonymous_usage_statistics.enabled:
        context.anonymous_usage_statistics.enabled = False
        context._save_project_config()
    return context
