"""Testes para o catálogo de dados leve (issue #14).

Só lógica pura (registro, integridade de linhagem, renderização) — roda na
suíte principal, sem depender de MinIO/Postgres/Kafka reais. As checagens
de infra real de `validator.py` (`validate_live`) são verificadas
manualmente contra o Docker Compose real (ver `docs/testes_issue_14.txt`),
mesmo padrão dos testes de integração do resto do projeto.
"""

from __future__ import annotations

from src.governance.data_catalog.registry import CATALOG, CATALOG_BY_KEY, DatasetKind, DatasetLayer
from src.governance.data_catalog.render import render_markdown
from src.governance.data_catalog.validator import ValidationResult, validate_references


class TestRegistry:
    def test_no_duplicate_keys(self):
        keys = [entry.key for entry in CATALOG]
        assert len(keys) == len(set(keys))

    def test_catalog_by_key_matches_catalog(self):
        assert len(CATALOG_BY_KEY) == len(CATALOG)
        for entry in CATALOG:
            assert CATALOG_BY_KEY[entry.key] is entry

    def test_all_upstream_references_exist(self):
        keys = {entry.key for entry in CATALOG}
        for entry in CATALOG:
            for upstream_key in entry.upstream:
                assert upstream_key in keys, f"{entry.key} referencia {upstream_key} inexistente"

    def test_dashboard_entries_have_dashboard_kind(self):
        for entry in CATALOG:
            if entry.layer is DatasetLayer.DASHBOARD:
                assert entry.kind is DatasetKind.DASHBOARD

    def test_all_entries_have_location(self):
        for entry in CATALOG:
            assert entry.location

    def test_minio_locations_use_configured_buckets(self):
        minio_entries = [e for e in CATALOG if e.kind is DatasetKind.MINIO_PREFIX]
        assert minio_entries
        for entry in minio_entries:
            assert entry.location.startswith("s3://")


class TestValidateReferences:
    def test_real_catalog_has_no_broken_references(self):
        results = validate_references(CATALOG)
        assert all(r.ok for r in results)
        assert len(results) == len(CATALOG)

    def test_detects_broken_upstream_reference(self):
        from dataclasses import replace

        broken = (replace(CATALOG[0], upstream=("does_not_exist",)),) + CATALOG[1:]
        results = validate_references(broken)
        broken_result = next(r for r in results if r.entry_key == broken[0].key)
        assert broken_result.ok is False
        assert "does_not_exist" in broken_result.detail


class TestRenderMarkdown:
    def test_contains_all_dataset_names(self):
        content = render_markdown(CATALOG)
        for entry in CATALOG:
            assert entry.name in content

    def test_contains_layer_headers(self):
        content = render_markdown(CATALOG)
        assert "## Bronze" in content
        assert "## Silver" in content
        assert "## Gold" in content
        assert "## Serving (PostgreSQL)" in content
        assert "## Streaming (Kafka)" in content
        assert "## Dashboards" in content

    def test_contains_mermaid_lineage_block(self):
        content = render_markdown(CATALOG)
        assert "```mermaid" in content
        assert "graph LR" in content
        assert "bronze_transactions --> silver_transactions" in content

    def test_planned_entry_marked_as_planned(self):
        # Nenhuma entry real está "planejado" hoje (issue #16 entregou o
        # dashboard) — testa a via de renderização diretamente com uma
        # entry sintética, mesmo padrão de test_detects_broken_upstream_reference.
        from dataclasses import replace

        planned = (replace(CATALOG[0], status="planejado"),) + CATALOG[1:]
        content = render_markdown(planned)
        assert "planejado" in content

    def test_without_validation_results_shows_not_validated(self):
        content = render_markdown(CATALOG)
        assert "não validado" in content

    def test_with_passing_validation_shows_ok(self):
        results = [
            ValidationResult(entry.key, True, "ok")
            for entry in CATALOG
            if entry.kind is not DatasetKind.DASHBOARD
        ]
        content = render_markdown(CATALOG, validation_results=results)
        # DatasetKind.DASHBOARD não tem checagem de infra implementada (ver
        # registry.py) — mesmo com todo o resto validado, essas entries
        # continuam "não validado", por design, não é um bug.
        non_dashboard_count = sum(1 for e in CATALOG if e.kind is not DatasetKind.DASHBOARD)
        assert content.count("✅ ok") == non_dashboard_count

    def test_with_failing_validation_shows_detail(self):
        entry = next(e for e in CATALOG if e.kind is DatasetKind.MINIO_PREFIX)
        results = [ValidationResult(entry.key, False, "nenhum objeto encontrado")]
        content = render_markdown(CATALOG, validation_results=results)
        assert "❌" in content
        assert "nenhum objeto encontrado" in content

    def test_glossary_terms_reference_known_business_glossary(self):
        # Termos usados no catálogo devem bater com o Glossário de Negócio
        # já documentado em docs/data_dictionary.md — evita duplicar/divergir.
        known_terms = {
            "VWAP",
            "Volatilidade",
            "Fraud Score",
            "Z-Score",
            "Velocity Check",
            "Account Takeover",
            "Smurfing",
        }
        used_terms = {term for entry in CATALOG for term in entry.glossary_terms}
        assert used_terms.issubset(known_terms)


class TestOptionalAssets:
    def test_only_market_and_streaming_output_are_optional(self):
        optional = {e.key for e in CATALOG if e.optional}
        assert optional == {"bronze_market_data", "silver_market_data", "silver_transactions_stream"}

    def test_missing_optional_asset_rendered_as_warning_not_error(self):
        results = [ValidationResult("bronze_market_data", False, "nenhum objeto encontrado")]
        content = render_markdown(CATALOG, validation_results=results)
        assert "⚠️ sem dados (opcional): nenhum objeto encontrado" in content
        assert "❌" not in content

    def _run_strict(self, tmp_path, failing_keys):
        from unittest.mock import patch

        from scripts import build_data_catalog

        live = [
            ValidationResult(e.key, e.key not in failing_keys, "detalhe")
            for e in CATALOG
            if e.kind is not DatasetKind.DASHBOARD
        ]
        with patch.object(build_data_catalog, "validate_live", return_value=live):
            build_data_catalog.main(
                [
                    "--strict",
                    "--output",
                    str(tmp_path / "catalog.md"),
                    "--lineage-image",
                    str(tmp_path / "lineage.svg"),
                ]
            )

    def test_strict_passes_when_only_optional_assets_missing(self, tmp_path):
        self._run_strict(tmp_path, {"bronze_market_data", "silver_market_data"})  # sem SystemExit

    def test_strict_fails_when_mandatory_asset_missing(self, tmp_path):
        import pytest

        with pytest.raises(SystemExit) as exc:
            self._run_strict(tmp_path, {"bronze_market_data", "gold_fact_transactions"})
        assert exc.value.code == 1
