"""Testes para `src.governance.great_expectations.datasets` (issue #13).

Roda na suíte principal (não precisa de `great_expectations`, só
`fsspec`/`pandas`) — diferente de `test_great_expectations.py`.

`_parse_hive_partitions` é regressão direta de um bug real encontrado no
teste de integração: `gold/fact_transactions` e `silver/market_data` são
gravados com `.partitionBy(...)` do Spark, que não duplica a coluna de
partição dentro do arquivo — ler arquivo a arquivo (para evitar o
`ArrowTypeError: Unable to merge` do pyarrow em `bronze/market_data`, que
tem a mesma coluna nos dois lugares) descartava silenciosamente `date_key`/
`date` nesses datasets até essa função reconstruir a coluna a partir do
caminho.
"""

from __future__ import annotations

from src.governance.great_expectations.datasets import (
    DATASET_KEYS,
    _parse_hive_partitions,
    _resolve,
)


class TestParseHivePartitions:
    def test_single_partition_segment(self):
        result = _parse_hive_partitions("bronze/market_data/date=2024-01-02/PETR4.SA.parquet")
        assert result == {"date": "2024-01-02"}

    def test_multiple_partition_segments(self):
        result = _parse_hive_partitions(
            "bronze/transactions/year=2024/month=06/day=15/transactions.parquet"
        )
        assert result == {"year": "2024", "month": "06", "day": "15"}

    def test_gold_date_key_partition(self):
        result = _parse_hive_partitions(
            "gold/fact_transactions/date_key=2025-10-14/part-00052-abc.c000.snappy.parquet"
        )
        assert result == {"date_key": "2025-10-14"}

    def test_no_partition_segments_returns_empty(self):
        result = _parse_hive_partitions("gold/dim_customers/part-00000-abc.snappy.parquet")
        assert result == {}


class TestResolve:
    def test_all_dataset_keys_resolve_without_error(self):
        for key in DATASET_KEYS:
            path = _resolve(key)
            assert path.startswith("s3://")

    def test_bronze_resolves_to_bronze_bucket(self):
        assert "bronze" in _resolve("bronze_transactions")

    def test_gold_resolves_to_gold_bucket(self):
        assert "gold" in _resolve("gold_dim_customers")

    def test_unknown_dataset_key_raises(self):
        import pytest

        with pytest.raises(ValueError, match="desconhecido"):
            _resolve("does_not_exist")
