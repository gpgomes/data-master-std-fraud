"""Testes para a imagem SVG da linhagem do catálogo (issue #37)."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import replace
from pathlib import Path

from src.governance.data_catalog.lineage_image import _label, _layout, render_lineage_svg
from src.governance.data_catalog.registry import CATALOG, DatasetLayer
from src.governance.data_catalog.render import render_markdown

_SVG_NS = "{http://www.w3.org/2000/svg}"
_COMMITTED_IMAGE = Path(__file__).parents[2] / "docs" / "images" / "data_lineage.svg"


def _root(svg: str) -> ET.Element:
    return ET.fromstring(svg)


class TestRenderLineageSvg:
    def test_is_valid_xml_with_accessible_title_and_description(self):
        root = _root(render_lineage_svg(CATALOG))
        assert root.tag == f"{_SVG_NS}svg"
        assert root.find(f"{_SVG_NS}title") is not None
        assert root.find(f"{_SVG_NS}desc") is not None

    def test_every_catalog_entry_is_drawn_with_its_label(self):
        svg = render_lineage_svg(CATALOG)
        drawn = {g.get("data-key") for g in _root(svg).iter(f"{_SVG_NS}g") if g.get("data-key")}
        assert drawn == {e.key for e in CATALOG}
        for entry in CATALOG:
            assert _label(entry).replace("&", "&amp;") in svg

    def test_every_upstream_dependency_is_an_arrow(self):
        keys = {e.key for e in CATALOG}
        expected = {(u, e.key) for e in CATALOG for u in e.upstream if u in keys}
        drawn = {
            (p.get("data-from"), p.get("data-to"))
            for p in _root(render_lineage_svg(CATALOG)).iter(f"{_SVG_NS}path")
            if p.get("data-from")
        }
        assert drawn == expected

    def test_optional_nodes_are_dashed_and_mandatory_ones_are_not(self):
        root = _root(render_lineage_svg(CATALOG))
        by_key = {e.key: e for e in CATALOG}
        for g in root.iter(f"{_SVG_NS}g"):
            key = g.get("data-key")
            if not key:
                continue
            rect = g.find(f"{_SVG_NS}rect")
            assert (rect.get("stroke-dasharray") is not None) == by_key[key].optional, key

    def test_is_deterministic_without_timestamps(self):
        first, second = render_lineage_svg(CATALOG), render_lineage_svg(CATALOG)
        assert first == second
        assert not re.search(r"20\d\d-\d\d-\d\d", first)

    def test_node_without_layout_hint_is_still_drawn(self):
        extra = replace(CATALOG[0], key="gold_novo_dataset", name="Gold — Novo", layer=DatasetLayer.GOLD)
        svg = render_lineage_svg(CATALOG + (extra,))
        assert 'data-key="gold_novo_dataset"' in svg
        _root(svg)

    def test_unknown_upstream_is_ignored_instead_of_crashing(self):
        broken = replace(CATALOG[0], key="gold_quebrado", name="Gold — Quebrado", layer=DatasetLayer.GOLD, upstream=("nao_existe",))
        svg = render_lineage_svg(CATALOG + (broken,))
        assert 'data-to="gold_quebrado"' not in svg

    def test_special_characters_are_escaped(self):
        tricky = replace(CATALOG[0], key="bronze_x", name="Bronze — A & <B>", description='diz "oi" & <tchau>')
        svg = render_lineage_svg((tricky,))
        _root(svg)  # continua XML válido
        assert "A &amp; &lt;B&gt;" in svg

    def test_only_unconnected_nodes_get_the_isolated_note(self):
        svg = render_lineage_svg(CATALOG)
        used = {u for e in CATALOG for u in e.upstream}
        isolated = [e.key for e in CATALOG if not e.upstream and e.key not in used]
        assert isolated == ["kafka_raw_market_data"]
        assert svg.count("sem consumidor no catálogo") == len(isolated)


class TestLayout:
    def test_streaming_band_is_below_the_batch_band_and_dashboard_sits_between(self):
        positions, width, height = _layout(CATALOG)
        batch = [positions[e.key][1] for e in CATALOG if e.key.startswith(("bronze_", "silver_market", "gold_"))]
        stream = [
            positions[e.key][1]
            for e in CATALOG
            if e.layer is DatasetLayer.STREAMING or e.key in ("silver_transactions_stream", "serving_fraud_alerts", "serving_stream_scored_transactions")
        ]
        assert max(batch) < min(stream)
        dashboard_y = positions["dashboard_fraud_overview"][1]
        assert min(batch) < dashboard_y < max(stream)
        assert all(0 < x < width and 0 < y < height for x, y in positions.values())

    def test_no_two_nodes_overlap(self):
        positions, _, _ = _layout(CATALOG)
        points = list(positions.values())
        for i, (x1, y1) in enumerate(points):
            for x2, y2 in points[i + 1 :]:
                assert abs(x1 - x2) > 1 or abs(y1 - y2) > 50


class TestCommittedImage:
    def test_versioned_image_matches_the_catalog(self):
        assert _COMMITTED_IMAGE.exists(), "rode `make catalog` para gerar docs/images/data_lineage.svg"
        assert _COMMITTED_IMAGE.read_text(encoding="utf-8") == render_lineage_svg(CATALOG), (
            "docs/images/data_lineage.svg está desatualizado em relação ao registro: "
            "rode `make catalog` e commite o resultado"
        )


class TestMarkdownEmbedding:
    def test_image_is_embedded_above_the_mermaid_block_when_given(self):
        content = render_markdown(CATALOG, lineage_image="images/data_lineage.svg")
        assert "![Linhagem de dados do catálogo](images/data_lineage.svg)" in content
        assert content.index("data_lineage.svg") < content.index("```mermaid")

    def test_no_image_reference_by_default(self):
        assert "data_lineage.svg" not in render_markdown(CATALOG)

    def test_build_script_writes_the_image(self, tmp_path):
        from unittest.mock import patch

        from scripts import build_data_catalog

        image = tmp_path / "img" / "lineage.svg"
        with patch.object(build_data_catalog, "validate_live", return_value=[]):
            build_data_catalog.main(
                ["--output", str(tmp_path / "catalog.md"), "--lineage-image", str(image)]
            )
        assert image.read_text(encoding="utf-8") == render_lineage_svg(CATALOG)
        assert "(img/lineage.svg)" in (tmp_path / "catalog.md").read_text(encoding="utf-8")
