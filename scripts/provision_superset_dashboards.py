"""CLI para provisionar o dashboard de KPIs de fraude/transacoes no Superset
(issue #16).

Uso:
    python -m scripts.provision_superset_dashboards            # provisiona
    python -m scripts.provision_superset_dashboards --verify    # + smoke test
    python -m scripts.provision_superset_dashboards --verify --strict  # falha (exit 1) se algum chart nao bater

Requer o Superset rodando (`make up`) e os dados carregados no Postgres
(`make spark-submit-batch` + `make spark-submit-gold-postgres`, issue #10).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.logger import get_logger
from src.serving.dashboards.provision import provision_all

logger = get_logger("superset_dashboards")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provisiona o dashboard Superset de fraude/transacoes.")
    parser.add_argument("--verify", action="store_true", help="Roda o smoke test contra cada chart apos provisionar.")
    parser.add_argument("--strict", action="store_true", help="Sai com codigo 1 se algum chart falhar na verificacao.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    summary = provision_all(verify=args.verify)

    failed = [r for r in summary["verification"] if not r.ok]
    logger.info(
        "Dashboard provisionado",
        dashboard_url=summary["dashboard_url"],
        datasets=len(summary["dataset_ids"]),
        charts=len(summary["chart_ids"]),
        verificacoes_falhadas=len(failed),
    )

    if args.strict and failed:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
