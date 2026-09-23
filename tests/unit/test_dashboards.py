"""Testes para o provisionamento de dashboards Superset (issue #16).

Só lógica pura (registro de charts, construção de position_json/native
filters, paginação do client) — sem depender de um Superset real rodando.
A verificação contra a infra real (`provision_all(verify=True)`, que
efetivamente consulta o Postgres via Superset) é documentada em
`docs/testes_issue_16.txt`, mesmo padrão de `test_data_catalog.py` (issue
#14) e `test_gx_datasets.py` (issue #13).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.serving.dashboards.charts import CHARTS, DATASETS
from src.serving.dashboards.client import SupersetClient
from src.serving.dashboards.provision import (
    _build_native_filters,
    _build_position_json,
    finalize_dashboard,
)


class TestChartsRegistry:
    def test_no_duplicate_slice_names(self):
        names = [c.slice_name for c in CHARTS]
        assert len(names) == len(set(names))

    def test_all_dataset_tables_are_known(self):
        for chart in CHARTS:
            assert chart.dataset_table in DATASETS

    def test_every_chart_has_query_extra_metrics_or_metric(self):
        for chart in CHARTS:
            assert "metric" in chart.query_extra or "metrics" in chart.query_extra

    def test_datasets_tuple_matches_schema_tables(self):
        assert DATASETS == ("fact_transactions", "agg_daily_fraud_metrics")


class TestBuildPositionJson:
    def test_single_row_for_few_charts(self):
        position = _build_position_json([1, 2, 3])
        assert position["GRID_ID"]["children"] == ["ROW-0"]
        assert position["ROW-0"]["children"] == ["CHART-1", "CHART-2", "CHART-3"]

    def test_seven_charts_produce_three_rows(self):
        position = _build_position_json([1, 2, 3, 4, 5, 6, 7])
        assert position["GRID_ID"]["children"] == ["ROW-0", "ROW-1", "ROW-2"]
        assert position["ROW-0"]["children"] == ["CHART-1", "CHART-2", "CHART-3", "CHART-4"]
        assert position["ROW-1"]["children"] == ["CHART-5", "CHART-6"]
        assert position["ROW-2"]["children"] == ["CHART-7"]

    def test_every_chart_node_references_correct_chart_id(self):
        position = _build_position_json([10, 20])
        assert position["CHART-10"]["meta"]["chartId"] == 10
        assert position["CHART-20"]["meta"]["chartId"] == 20

    def test_root_and_grid_present(self):
        position = _build_position_json([1])
        assert position["ROOT_ID"]["children"] == ["GRID_ID"]
        assert position["DASHBOARD_VERSION_KEY"] == "v2"


class TestBuildNativeFilters:
    def test_two_filters_period_and_transaction_type(self):
        filters = _build_native_filters({"fact_transactions": 1, "agg_daily_fraud_metrics": 2})
        names = [f["name"] for f in filters]
        assert names == ["Periodo", "Tipo de Transacao"]

    def test_period_filter_targets_date_key_on_both_datasets(self):
        filters = _build_native_filters({"fact_transactions": 1, "agg_daily_fraud_metrics": 2})
        period = filters[0]
        assert period["filterType"] == "filter_time"
        target_dataset_ids = {t["datasetId"] for t in period["targets"]}
        assert target_dataset_ids == {1, 2}
        assert all(t["column"]["name"] == "date_key" for t in period["targets"])

    def test_transaction_type_filter_targets_correct_column(self):
        filters = _build_native_filters({"fact_transactions": 1, "agg_daily_fraud_metrics": 2})
        tx_type = filters[1]
        assert tx_type["filterType"] == "filter_select"
        assert all(t["column"]["name"] == "transaction_type" for t in tx_type["targets"])


class TestSupersetClientFindOne:
    def _client_with_mocked_session(self) -> SupersetClient:
        client = SupersetClient.__new__(SupersetClient)
        client.base_url = "http://superset.test"
        client._session = MagicMock()
        return client

    def test_finds_match_on_first_page(self):
        client = self._client_with_mocked_session()
        response = MagicMock()
        response.json.return_value = {"count": 1, "result": [{"id": 1, "table_name": "fact_transactions"}]}
        client._session.get.return_value = response

        result = client.find_one("/api/v1/dataset/", {"table_name": "fact_transactions"})
        assert result == {"id": 1, "table_name": "fact_transactions"}

    def test_returns_none_when_no_match_and_no_more_pages(self):
        client = self._client_with_mocked_session()
        response = MagicMock()
        response.json.return_value = {"count": 1, "result": [{"id": 1, "table_name": "other_table"}]}
        client._session.get.return_value = response

        result = client.find_one("/api/v1/dataset/", {"table_name": "fact_transactions"})
        assert result is None

    def test_paginates_until_match_found(self):
        client = self._client_with_mocked_session()
        page0 = MagicMock()
        page0.json.return_value = {"count": 2, "result": [{"id": 1, "table_name": "other_table"}]}
        page1 = MagicMock()
        page1.json.return_value = {"count": 2, "result": [{"id": 2, "table_name": "fact_transactions"}]}
        client._session.get.side_effect = [page0, page1]

        result = client.find_one("/api/v1/dataset/", {"table_name": "fact_transactions"}, page_size=1)
        assert result == {"id": 2, "table_name": "fact_transactions"}
        assert client._session.get.call_count == 2


class TestFinalizeDashboard:
    def test_publishes_dashboard_so_superset_does_not_show_draft_badge(self):
        client = MagicMock()
        finalize_dashboard(client, 1, [1, 2, 3], {"fact_transactions": 1, "agg_daily_fraud_metrics": 2})
        path, payload = client.put.call_args.args
        assert path == "/api/v1/dashboard/1"
        assert payload["published"] is True
