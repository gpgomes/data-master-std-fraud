"""Checkpoints (issue #13) — um por dataset, sem `batch_request` fixo: o
DataFrame real é passado em `checkpoint.run(batch_request=...)` a cada
execução (`runner.py`), já que os dados mudam a cada run do Airflow.

Datasource/asset pandas únicos, compartilhados por todos os datasets — o
`batch_request` é reconstruído a cada chamada com o DataFrame do momento
(`asset.build_batch_request(dataframe=df)`), então não há necessidade de um
asset por dataset.
"""

from __future__ import annotations

from great_expectations.data_context import FileDataContext
from great_expectations.datasource.fluent import PandasDatasource
from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset

from src.governance.great_expectations.suites import SUITE_NAMES

DATASOURCE_NAME = "gx_pandas_datasource"
ASSET_NAME = "gx_dataframe_asset"


def ensure_asset(context: FileDataContext) -> DataFrameAsset:
    """Cria/atualiza o datasource+asset pandas compartilhado (idempotente)."""
    datasource: PandasDatasource = context.sources.add_or_update_pandas(DATASOURCE_NAME)
    return datasource.add_dataframe_asset(name=ASSET_NAME)


def _checkpoint_name(suite_name: str) -> str:
    return f"{suite_name}_checkpoint"


def ensure_checkpoints(context: FileDataContext) -> None:
    """Cria/atualiza um checkpoint por suite (idempotente)."""
    for suite_name in SUITE_NAMES:
        context.add_or_update_checkpoint(
            name=_checkpoint_name(suite_name),
            validations=[{"expectation_suite_name": suite_name}],
        )


def get_checkpoint_name(dataset_key: str) -> str:
    return _checkpoint_name(dataset_key)
