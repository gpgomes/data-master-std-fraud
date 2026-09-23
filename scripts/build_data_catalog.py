"""CLI para gerar o catálogo de dados leve (issue #14).

Substitui o `seed-openmetadata`/OpenMetadata completo — ver
`src/governance/data_catalog/registry.py` para a decisão e o motivo.

Uso:
    python -m scripts.build_data_catalog                # gera docs/data_catalog.md
    python -m scripts.build_data_catalog --skip-validation  # sem checar infra real
    python -m scripts.build_data_catalog --strict        # falha (exit 1) se algo não bater
                                                         # (assets `optional` ausentes só avisam)

`--strict` requer a infra local rodando (`make up && make setup`) e dados
já processados até a camada Gold (`make spark-submit-batch`) — funciona como
smoke test do catálogo, mesmo espírito do `run_gate --all` do Great
Expectations (issue #13).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Garante que o pacote raiz está no sys.path ao rodar como script direto
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.logger import get_logger
from src.governance.data_catalog.registry import CATALOG
from src.governance.data_catalog.render import render_markdown
from src.governance.data_catalog.validator import validate_live, validate_references

logger = get_logger("data_catalog")

DEFAULT_OUTPUT = Path(__file__).parent.parent / "docs" / "data_catalog.md"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera o catálogo de dados leve.")
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT, help="Arquivo Markdown de saída."
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Não checa a infra real (MinIO/Postgres/Kafka), só renderiza o registro.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Sai com código 1 se alguma checagem (referência ou infra real) falhar.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    reference_results = validate_references(CATALOG)
    failed_references = [r for r in reference_results if not r.ok]
    for r in failed_references:
        logger.error("Referência de linhagem inválida", entry=r.entry_key, detail=r.detail)

    live_results = [] if args.skip_validation else validate_live(CATALOG)
    failed_live = [r for r in live_results if not r.ok]
    optional_keys = {e.key for e in CATALOG if e.optional}
    for r in failed_live:
        logger.warning(
            "Asset do catálogo não encontrado na infra real",
            entry=r.entry_key,
            detail=r.detail,
            optional=r.entry_key in optional_keys,
        )
    blocking_live = [r for r in failed_live if r.entry_key not in optional_keys]

    # Só `live_results` alimenta a coluna "Status" do doc renderizado — ela
    # significa "existe na infra real", não "linhagem bem formada". Falhas de
    # referência já são logadas acima e derrubam `--strict` por conta própria.
    content = render_markdown(CATALOG, validation_results=live_results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")
    logger.info(
        "Catálogo gerado",
        output=str(args.output),
        datasets=len(CATALOG),
        referencias_invalidas=len(failed_references),
        assets_nao_encontrados=len(failed_live),
    )

    if args.strict and (failed_references or blocking_live):
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
