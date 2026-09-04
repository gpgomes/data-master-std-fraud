"""Renderiza o catálogo em Markdown (`docs/data_catalog.md`), issue #14.

Uma tabela por camada (dataset, localização, owner, classificação, termos
de glossário — ver `docs/data_dictionary.md`, status de validação) mais um
diagrama de linhagem Mermaid (`graph LR`) construído a partir de
`CatalogEntry.upstream`. Sem servidor: o arquivo gerado é o próprio
artefato, aberto localmente (editor ou GitHub, que renderiza Mermaid
nativamente) — não depende de nenhum container novo.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.governance.data_catalog.registry import CatalogEntry, DatasetLayer
from src.governance.data_catalog.validator import ValidationResult

_LAYER_ORDER = (
    DatasetLayer.BRONZE,
    DatasetLayer.SILVER,
    DatasetLayer.GOLD,
    DatasetLayer.SERVING,
    DatasetLayer.STREAMING,
    DatasetLayer.DASHBOARD,
)

_LAYER_TITLES = {
    DatasetLayer.BRONZE: "Bronze",
    DatasetLayer.SILVER: "Silver",
    DatasetLayer.GOLD: "Gold",
    DatasetLayer.SERVING: "Serving (PostgreSQL)",
    DatasetLayer.STREAMING: "Streaming (Kafka)",
    DatasetLayer.DASHBOARD: "Dashboards",
}


def _status_marker(entry: CatalogEntry, status_by_key: dict[str, ValidationResult]) -> str:
    if entry.status == "planejado":
        return "🗓️ planejado"
    result = status_by_key.get(entry.key)
    if result is None:
        return "— não validado"
    return "✅ ok" if result.ok else f"❌ {result.detail}"


def _render_layer_table(
    entries: tuple[CatalogEntry, ...],
    status_by_key: dict[str, ValidationResult],
) -> str:
    lines = [
        "| Dataset | Localização | Owner | Classificação | Glossário | Status |",
        "|---------|-------------|-------|---------------|-----------|--------|",
    ]
    for entry in entries:
        classification = ", ".join(entry.classification) or "—"
        glossary = ", ".join(entry.glossary_terms) or "—"
        location = entry.location or "—"
        lines.append(
            f"| **{entry.name}**<br>{entry.description} | `{location}` | {entry.owner} "
            f"| {classification} | {glossary} | {_status_marker(entry, status_by_key)} |"
        )
    return "\n".join(lines)


def _render_lineage(entries: tuple[CatalogEntry, ...]) -> str:
    lines = ["```mermaid", "graph LR"]
    for entry in entries:
        lines.append(f'    {entry.key}["{entry.name}"]')
    for entry in entries:
        for upstream_key in entry.upstream:
            lines.append(f"    {upstream_key} --> {entry.key}")
    lines.append("```")
    return "\n".join(lines)


def render_markdown(
    entries: tuple[CatalogEntry, ...],
    validation_results: list[ValidationResult] | None = None,
) -> str:
    status_by_key = {r.entry_key: r for r in (validation_results or [])}
    generated_at = datetime.now(tz=UTC).isoformat()

    parts = [
        "# Catálogo de Dados",
        "",
        f"_Gerado em {generated_at} por `python -m scripts.build_data_catalog`._",
        "",
        "Substitui o OpenMetadata completo na V1 local (decisão documentada em "
        "`docs/architecture.md`) — ver definições de campo e o glossário de negócio "
        "completo em [`docs/data_dictionary.md`](data_dictionary.md).",
        "",
    ]

    for layer in _LAYER_ORDER:
        layer_entries = tuple(e for e in entries if e.layer is layer)
        if not layer_entries:
            continue
        parts.append(f"## {_LAYER_TITLES[layer]}")
        parts.append("")
        parts.append(_render_layer_table(layer_entries, status_by_key))
        parts.append("")

    parts.append("## Linhagem")
    parts.append("")
    parts.append(_render_lineage(entries))
    parts.append("")

    return "\n".join(parts)
