"""Imagem SVG da linhagem do catálogo (issue #37).

Gerada a partir do registro (`CATALOG`), sem desenho manual, para nunca divergir do catálogo:
cada `CatalogEntry` vira um nó e cada `upstream` vira uma seta. Python puro, sem dependência
externa (nem Graphviz nem mermaid-cli), então roda igual num clone novo e no CI.

Layout: colunas por camada (Bronze, Silver, Gold, Serving, Dashboard) e duas faixas, Batch em
cima e Streaming embaixo (um nó cai em Streaming se for um tópico Kafka ou descender de um).
`_ROW_HINTS` só ordena as linhas dos nós conhecidos para reduzir cruzamentos: um nó novo sem
dica entra no fim da coluna e continua sendo desenhado.

A saída é determinística (sem data/hora): só muda quando o catálogo muda, e um teste compara a
imagem versionada com a gerada agora, para não esquecer de regenerar.
"""

from __future__ import annotations

from html import escape

from src.governance.data_catalog.registry import CatalogEntry, DatasetLayer

_COLUMN_BY_LAYER = {
    DatasetLayer.BRONZE: 0,
    DatasetLayer.SILVER: 1,
    DatasetLayer.GOLD: 2,
    DatasetLayer.SERVING: 3,
    DatasetLayer.DASHBOARD: 4,
}
_COLUMN_TITLES = ("Bronze", "Silver", "Gold", "Serving (Postgres)", "Dashboard")

# fill, stroke, rótulo curto (o rótulo evita depender só de cor para distinguir a camada)
_STYLE = {
    DatasetLayer.BRONZE: ("#F6E6D6", "#B0703C", "BRONZE"),
    DatasetLayer.SILVER: ("#E8EBF0", "#6F7C8B", "SILVER"),
    DatasetLayer.GOLD: ("#FBF1CC", "#B8901F", "GOLD"),
    DatasetLayer.SERVING: ("#DDEBFA", "#2F78BD", "SERVING"),
    DatasetLayer.STREAMING: ("#E6E2F8", "#6250C2", "KAFKA"),
    DatasetLayer.DASHBOARD: ("#DDF3E4", "#2A9455", "DASHBOARD"),
}

# Linha de cada nó dentro da sua (faixa, coluna). Só apresentação: ordena para evitar
# cruzamentos e manter setas curtas.
_ROW_HINTS: dict[str, float] = {
    # Batch
    "bronze_transactions": 1,
    "silver_transactions": 1,
    "gold_fact_transactions": 0,
    "gold_agg_daily_fraud_metrics": 1,
    "gold_dim_customers": 2,
    "gold_dim_date": 3,
    "serving_fact_transactions": 0,
    "serving_agg_daily_fraud_metrics": 1,
    "serving_dim_customers": 2,
    "serving_dim_date": 3,
    "bronze_market_data": 4.4,
    "silver_market_data": 4.4,
    # Streaming
    "kafka_raw_transactions": 0.5,
    "silver_transactions_stream": 0,
    "kafka_enriched_transactions": 1,
    "kafka_fraud_alerts": 2,
    "serving_stream_scored_transactions": 0,
    "serving_fraud_alerts": 1,
    "kafka_raw_market_data": 3,
}

_NODE_W, _NODE_H = 216, 48
_COL_PITCH, _ROW_PITCH = 276, 68
_MARGIN_LEFT, _MARGIN_RIGHT = 118, 40
_TITLE_H = 64
_BAND_GAP = 54
_FONT = "Inter, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
_INK, _MUTED, _EDGE = "#1F2933", "#5B6673", "#7A8794"


def _label(entry: CatalogEntry) -> str:
    """Nome sem o prefixo de camada (a camada já aparece na etiqueta do nó)."""
    return entry.name.split(" — ", 1)[1] if " — " in entry.name else entry.name


def _wrap(text: str, limit: int = 28) -> list[str]:
    if len(text) <= limit:
        return [text]
    words, lines, current = text.split(" "), [], ""
    for word in words:
        if current and len(current) + 1 + len(word) > limit:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    lines.append(current)
    return lines[:2]


def _descends_from_streaming(entry: CatalogEntry, by_key: dict[str, CatalogEntry]) -> bool:
    seen: set[str] = set()
    stack = list(entry.upstream)
    while stack:
        key = stack.pop()
        if key in seen or key not in by_key:
            continue
        seen.add(key)
        parent = by_key[key]
        if parent.layer is DatasetLayer.STREAMING:
            return True
        stack.extend(parent.upstream)
    return False


def _kafka_depth(entry: CatalogEntry, by_key: dict[str, CatalogEntry]) -> int:
    parents = [
        by_key[u] for u in entry.upstream if u in by_key and by_key[u].layer is DatasetLayer.STREAMING
    ]
    return 1 + max(_kafka_depth(p, by_key) for p in parents) if parents else 0


def _layout(entries: tuple[CatalogEntry, ...]) -> tuple[dict[str, tuple[float, float]], float, float]:
    """Centro (x, y) de cada nó, mais largura e altura do desenho."""
    by_key = {e.key: e for e in entries}
    band: dict[str, str] = {}
    column: dict[str, int] = {}
    for entry in entries:
        if entry.layer is DatasetLayer.STREAMING:
            band[entry.key], column[entry.key] = "stream", _kafka_depth(entry, by_key)
        else:
            column[entry.key] = _COLUMN_BY_LAYER[entry.layer]
            if entry.layer is DatasetLayer.DASHBOARD:
                band[entry.key] = "both"
            else:
                band[entry.key] = "stream" if _descends_from_streaming(entry, by_key) else "batch"

    rows: dict[str, float] = {}
    next_free: dict[tuple[str, int], float] = {}
    for entry in entries:
        if band[entry.key] == "both":
            continue
        slot = (band[entry.key], column[entry.key])
        if entry.key in _ROW_HINTS:
            rows[entry.key] = _ROW_HINTS[entry.key]
        else:
            rows[entry.key] = next_free.get(slot, 0)
        next_free[slot] = max(next_free.get(slot, 0), rows[entry.key] + 1)

    def band_rows(name: str) -> float:
        values = [rows[k] for k in rows if band[k] == name]
        return (max(values) + 1) if values else 0

    batch_h = band_rows("batch") * _ROW_PITCH
    stream_y0 = _TITLE_H + batch_h + _BAND_GAP
    positions: dict[str, tuple[float, float]] = {}
    for key, row in rows.items():
        x = _MARGIN_LEFT + column[key] * _COL_PITCH + _NODE_W / 2
        y0 = _TITLE_H if band[key] == "batch" else stream_y0
        positions[key] = (x, y0 + row * _ROW_PITCH + _ROW_PITCH / 2)
    for entry in entries:  # dashboard: no meio dos pais (que ficam em faixas diferentes)
        if band[entry.key] == "both":
            ys = [positions[u][1] for u in entry.upstream if u in positions]
            y = sum(ys) / len(ys) if ys else _TITLE_H + batch_h / 2
            positions[entry.key] = (_MARGIN_LEFT + column[entry.key] * _COL_PITCH + _NODE_W / 2, y)

    width = _MARGIN_LEFT + 5 * _COL_PITCH - (_COL_PITCH - _NODE_W) + _MARGIN_RIGHT
    height = stream_y0 + band_rows("stream") * _ROW_PITCH + 92  # 92 = legenda
    return positions, width, height


def _fmt(value: float) -> str:
    return f"{value:.1f}".rstrip("0").rstrip(".")


def _edge(entry: CatalogEntry, parent: CatalogEntry, positions: dict[str, tuple[float, float]]) -> str:
    sx, sy = positions[parent.key]
    tx, ty = positions[entry.key]
    dashed = entry.optional or parent.optional
    style = f'stroke="{_EDGE}" stroke-width="1.6" fill="none" marker-end="url(#arrow)"'
    if dashed:
        style += ' stroke-dasharray="6 4"'
    if abs(sx - tx) < 1:  # mesma coluna: seta vertical entre a base e o topo
        down = ty > sy
        y1, y2 = (sy + _NODE_H / 2, ty - _NODE_H / 2) if down else (sy - _NODE_H / 2, ty + _NODE_H / 2)
        d = f"M {_fmt(sx)} {_fmt(y1)} L {_fmt(tx)} {_fmt(y2)}"
    else:
        x1, x2 = sx + _NODE_W / 2, tx - _NODE_W / 2
        c = (x2 - x1) * 0.5
        d = f"M {_fmt(x1)} {_fmt(sy)} C {_fmt(x1 + c)} {_fmt(sy)}, {_fmt(x2 - c)} {_fmt(ty)}, {_fmt(x2)} {_fmt(ty)}"
    return f'<path d="{d}" {style} data-from="{escape(parent.key)}" data-to="{escape(entry.key)}"/>'


def _node(entry: CatalogEntry, x: float, y: float, isolated: bool = False) -> str:
    fill, stroke, tag = _STYLE[entry.layer]
    left, top = x - _NODE_W / 2, y - _NODE_H / 2
    dash = ' stroke-dasharray="6 4"' if entry.optional else ""
    lines = _wrap(_label(entry))
    text_y = y + 3 if len(lines) == 1 else y - 1
    tspans = "".join(
        f'<tspan x="{_fmt(x)}" dy="{0 if i == 0 else 15}">{escape(line)}</tspan>'
        for i, line in enumerate(lines)
    )
    optional_tag = ""
    if entry.optional:
        optional_tag = (
            f'<text x="{_fmt(left + _NODE_W - 8)}" y="{_fmt(top + 12)}" text-anchor="end" '
            f'font-size="9" fill="{_MUTED}">opcional</text>'
        )
    isolated_note = ""
    if isolated:
        isolated_note = (
            f'<text x="{_fmt(x)}" y="{_fmt(y + _NODE_H / 2 + 14)}" text-anchor="middle" '
            f'font-size="10" font-style="italic" fill="{_MUTED}">sem consumidor no catálogo</text>'
        )
    return (
        f'<g data-key="{escape(entry.key)}">'
        f'<title>{escape(entry.name)}: {escape(entry.description)}</title>'
        f'<rect x="{_fmt(left)}" y="{_fmt(top)}" width="{_NODE_W}" height="{_NODE_H}" rx="8" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="1.6"{dash}/>'
        f'<text x="{_fmt(left + 8)}" y="{_fmt(top + 12)}" font-size="9" font-weight="700" '
        f'letter-spacing="0.6" fill="{stroke}">{tag}</text>'
        f'{optional_tag}'
        f'<text x="{_fmt(x)}" y="{_fmt(text_y + 6)}" text-anchor="middle" font-size="13" '
        f'font-weight="600" fill="{_INK}">{tspans}</text>'
        f"{isolated_note}"
        f"</g>"
    )


def _legend(width: float, y: float) -> str:
    parts = [f'<text x="{_MARGIN_LEFT - 100}" y="{_fmt(y)}" font-size="11" font-weight="700" fill="{_MUTED}">LEGENDA</text>']
    x = _MARGIN_LEFT
    for layer in (
        DatasetLayer.BRONZE,
        DatasetLayer.SILVER,
        DatasetLayer.GOLD,
        DatasetLayer.SERVING,
        DatasetLayer.STREAMING,
        DatasetLayer.DASHBOARD,
    ):
        fill, stroke, tag = _STYLE[layer]
        parts.append(
            f'<rect x="{_fmt(x)}" y="{_fmt(y - 11)}" width="16" height="14" rx="3" fill="{fill}" stroke="{stroke}" stroke-width="1.4"/>'
            f'<text x="{_fmt(x + 22)}" y="{_fmt(y)}" font-size="11" fill="{_INK}">{tag.title()}</text>'
        )
        x += 112
    parts.append(
        f'<rect x="{_fmt(x)}" y="{_fmt(y - 11)}" width="16" height="14" rx="3" fill="#FFFFFF" stroke="{_EDGE}" stroke-width="1.4" stroke-dasharray="4 3"/>'
        f'<text x="{_fmt(x + 22)}" y="{_fmt(y)}" font-size="11" fill="{_INK}">Opcional: pode não ter dados (API de terceiros ou streaming ainda não executado)</text>'
    )
    parts.append(
        f'<text x="{_MARGIN_LEFT - 100}" y="{_fmt(y + 24)}" font-size="11" fill="{_MUTED}">'
        f"Gerado de src/governance/data_catalog/registry.py por scripts/build_data_catalog.py; não edite à mão.</text>"
    )
    return "".join(parts)


def render_lineage_svg(entries: tuple[CatalogEntry, ...]) -> str:
    """SVG completo da linhagem dos `entries` (uma seta por `upstream` existente)."""
    positions, width, height = _layout(entries)
    by_key = {e.key: e for e in entries}

    edges = [
        _edge(entry, by_key[u], positions)
        for entry in entries
        for u in entry.upstream
        if u in by_key
    ]
    used_as_upstream = {u for entry in entries for u in entry.upstream}
    nodes = [
        _node(
            entry,
            *positions[entry.key],
            isolated=not entry.upstream and entry.key not in used_as_upstream,
        )
        for entry in entries
    ]

    batch_ys = [y for k, (x, y) in positions.items() if by_key[k].layer is not DatasetLayer.STREAMING]
    headers = "".join(
        f'<text x="{_fmt(_MARGIN_LEFT + i * _COL_PITCH + _NODE_W / 2)}" y="{_TITLE_H - 18}" text-anchor="middle" '
        f'font-size="12" font-weight="700" fill="{_MUTED}">{escape(title)}</text>'
        for i, title in enumerate(_COLUMN_TITLES)
    )
    # o dashboard descende do Kafka, mas fica entre as faixas: não conta para a faixa Streaming
    stream_ys = [
        y
        for k, (x, y) in positions.items()
        if by_key[k].layer is not DatasetLayer.DASHBOARD
        and (by_key[k].layer is DatasetLayer.STREAMING or _descends_from_streaming(by_key[k], by_key))
    ]
    bands = ""
    if stream_ys:
        top = min(stream_ys) - _ROW_PITCH / 2 - 8
        bands = (
            f'<rect x="8" y="{_fmt(top)}" width="{_fmt(width - 16)}" height="{_fmt(max(stream_ys) - min(stream_ys) + _ROW_PITCH + 16)}" '
            f'rx="12" fill="#F4F2FC" opacity="0.7"/>'
            f'<text x="18" y="{_fmt(top + 22)}" font-size="12" font-weight="700" fill="#6250C2">STREAMING</text>'
        )
    batch_label = (
        f'<text x="18" y="{_TITLE_H + 6}" font-size="12" font-weight="700" fill="{_MUTED}">BATCH</text>'
        if batch_ys
        else ""
    )
    legend_y = height - 60

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {_fmt(width)} {_fmt(height)}" '
        f'width="{_fmt(width)}" height="{_fmt(height)}" font-family="{_FONT}" role="img" '
        f'aria-labelledby="lineage-title lineage-desc">\n'
        f'<title id="lineage-title">Linhagem de dados do catálogo</title>\n'
        f'<desc id="lineage-desc">{len(entries)} datasets e {len(edges)} dependências, '
        f"de Bronze e Kafka até o dashboard.</desc>\n"
        f'<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="{_EDGE}"/></marker></defs>\n'
        f'<rect width="100%" height="100%" fill="#FFFFFF"/>\n'
        f'<text x="18" y="26" font-size="17" font-weight="700" fill="{_INK}">Linhagem de dados — V1 local</text>\n'
        f"{bands}\n{batch_label}{headers}\n"
        f'<g fill="none">{"".join(edges)}</g>\n'
        f'{"".join(nodes)}\n'
        f"{_legend(width, legend_y)}\n"
        f"</svg>\n"
    )
