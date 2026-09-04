"""Executa um quality gate: carrega o dataset, roda o checkpoint, publica data
docs e levanta exceção se falhar (issue #13) — gate simples: qualquer
expectativa falhando bloqueia o chamador (Airflow ou CLI).

Execução via CLI:
    python -m src.governance.great_expectations.runner bronze_transactions
    python -m src.governance.great_expectations.runner --all
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.data_context import FileDataContext

from src.common.logger import get_logger
from src.governance.great_expectations.checkpoints import (
    ensure_asset,
    ensure_checkpoints,
    get_checkpoint_name,
)
from src.governance.great_expectations.context import get_context
from src.governance.great_expectations.datasets import DATASET_KEYS, load_dataframe
from src.governance.great_expectations.suites import ensure_suites

logger = get_logger("great_expectations")


class QualityGateFailed(Exception):
    """Levantada quando um checkpoint falha — o chamador decide o que fazer
    (Airflow: task falha, bloqueia a DAG — gate simples, ver docs/runbook.md)."""


def _log_result(dataset_key: str, result: CheckpointResult) -> dict:
    vr = result.list_validation_results()[0]
    per_expectation = [
        {
            "expectation_type": r.expectation_config.expectation_type,
            "success": r.success,
            "kwargs": {
                k: v
                for k, v in r.expectation_config.kwargs.items()
                if k not in ("row_condition", "condition_parser")
            },
        }
        for r in vr.results
    ]
    failed = [e for e in per_expectation if not e["success"]]
    metrics = {
        "dataset": dataset_key,
        "success": result.success,
        "total_expectations": len(per_expectation),
        "failed_expectations": len(failed),
    }
    if failed:
        logger.error("Quality gate falhou", **metrics, failures=failed)
    else:
        logger.info("Quality gate passou", **metrics)
    return metrics


def run_gate(
    dataset_key: str,
    df: pd.DataFrame | None = None,
    context: FileDataContext | None = None,
) -> dict:
    """Roda o quality gate de um dataset. Levanta QualityGateFailed se falhar.

    Args:
        dataset_key: uma das chaves em DATASET_KEYS (== nome da suite).
        df: DataFrame a validar; se None, carrega do MinIO via `datasets.py`
            (usado pelo Airflow). Passar explicitamente é o que os testes
            fazem, com fixtures válidas/inválidas.
        context: FileDataContext a usar; se None, usa o contexto real do
            projeto (`get_context()`). Injetável para apontar a um diretório
            temporário nos testes, mesmo padrão de `spark=spark` usado no
            resto do projeto.
    """
    if context is None:
        context = get_context()
    ensure_suites(context)
    ensure_checkpoints(context)
    asset = ensure_asset(context)

    if df is None:
        df = load_dataframe(dataset_key)

    batch_request = asset.build_batch_request(dataframe=df)
    checkpoint = context.get_checkpoint(get_checkpoint_name(dataset_key))
    result = checkpoint.run(batch_request=batch_request)

    metrics = _log_result(dataset_key, result)

    try:
        context.build_data_docs()
    except Exception:
        logger.exception("Falha ao publicar data docs (não bloqueia o gate)", dataset=dataset_key)

    if not result.success:
        raise QualityGateFailed(
            f"Quality gate falhou para '{dataset_key}': "
            f"{metrics['failed_expectations']}/{metrics['total_expectations']} expectativas falharam. "
            f"Diagnóstico: docs/runbook.md (seção Quality Gates)."
        )
    return metrics


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Roda quality gates do Great Expectations.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("dataset", nargs="?", choices=DATASET_KEYS, help="Dataset a validar.")
    group.add_argument("--all", action="store_true", help="Roda todos os datasets.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    targets = list(DATASET_KEYS) if args.all else [args.dataset]

    failures = []
    for dataset_key in targets:
        try:
            run_gate(dataset_key)
        except QualityGateFailed as exc:
            failures.append(str(exc))

    if failures:
        for msg in failures:
            logger.error(msg)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
