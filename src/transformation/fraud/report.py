"""Renderiza `docs/fraud_evaluation.md` a partir dos resultados da avaliação (issue #44).

Determinístico: nenhum timestamp nem caminho absoluto no corpo, então a mesma configuração e as
mesmas seeds geram exatamente o mesmo documento. Todos os números vêm de `results`; o texto estático
só afirma o que vale por construção do protocolo.
"""

from __future__ import annotations

import math
from typing import Any

from src.transformation.fraud.evaluate import (
    ANALYTIC_FPR_RANGE,
    DEPLOYED,
    FRAUD_TYPES,
    HARD_NEGATIVES,
    HYPOTHESIS_PRECISION,
    HYPOTHESIS_TOLERANCE,
    NO_HARD_NEGATIVE,
    OPERATING_FPRS,
    PRIMARY_FPR,
    condition_key,
)

_STEALTH_LABEL = {"normal": "normal", "stealth": "stealth"}


# ── Formatação (pt-BR: vírgula decimal, ponto de milhar) ───────────────────────


def _nan(x: float) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


def _num(x: float, digits: int = 1) -> str:
    return "n/d" if _nan(x) else f"{x:.{digits}f}".replace(".", ",")


def _int(x: float) -> str:
    return "n/d" if _nan(x) else f"{x:,.0f}".replace(",", ".")


def _pct(x: float, digits: int = 1) -> str:
    return "n/d" if _nan(x) else _num(x * 100, digits) + "%"


def _pm_pct(mean_sd: tuple[float, float], digits: int = 1) -> str:
    mean, sd = mean_sd
    if _nan(mean):
        return "n/d"
    return f"{_num(mean * 100, digits)}% ± {_num(sd * 100, digits)}"


def _pm_num(mean_sd: tuple[float, float], digits: int = 1) -> str:
    mean, sd = mean_sd
    if _nan(mean):
        return "n/d"
    return f"{_num(mean, digits)} ± {_num(sd, digits)}"


def _seconds(x: float) -> str:
    """Duração em segundos, em s ou min conforme a ordem de grandeza."""
    if _nan(x):
        return "n/d"
    return f"{_num(x, 0)} s" if x < 120 else f"{_num(x / 60, 1)} min"


def _table(headers: list[str], rows: list[list[str]], text_cols: int = 1) -> str:
    """Tabela Markdown; as `text_cols` primeiras colunas alinham à esquerda, o resto à direita."""
    align = ["---"] * text_cols + ["---:"] * (len(headers) - text_cols)
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(align) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def _fpr_label(fpr: float) -> str:
    return _pct(fpr, 1)


# ── Seções ─────────────────────────────────────────────────────────────────────


def _hypothesis_verdict(precision: float) -> tuple[bool, str]:
    """Regra fixada antes de medir: confirmada se a Precision cai em 50% ± 10 p.p."""
    diff = precision - HYPOTHESIS_PRECISION
    confirmed = abs(diff) <= HYPOTHESIS_TOLERANCE
    low, high = (
        HYPOTHESIS_PRECISION - HYPOTHESIS_TOLERANCE,
        HYPOTHESIS_PRECISION + HYPOTHESIS_TOLERANCE,
    )
    band = f"{_pct(low, 0)}–{_pct(high, 0)}"
    side = "abaixo" if diff < 0 else "acima"
    distance = f"{_num(abs(diff) * 100, 1)} p.p. {side} de {_pct(HYPOTHESIS_PRECISION, 0)}"
    if confirmed:
        edge = (
            f"; perto do limite da faixa de {band}"
            if abs(diff) > HYPOTHESIS_TOLERANCE / 2
            else f", dentro da faixa de {band}"
        )
        return True, f"**confirmada** (Precision de {_pct(precision)}, {distance}{edge})"
    return False, (
        f"**refutada** (Precision de {_pct(precision)}, {distance}, fora da faixa de {band})"
    )


def _summary(results: dict[str, Any]) -> str:
    cond = results["conditions"]
    deployed = cond[DEPLOYED]
    primary = cond[condition_key(PRIMARY_FPR)]
    z = results["config"]["z_threshold"]
    threshold = results["thresholds"][PRIMARY_FPR]
    _, verdict = _hypothesis_verdict(deployed["precision"][0])

    by_type = {t: v["value"][0] for t, v in deployed["by_type"].items() if not _nan(v["value"][0])}
    lines = [
        f"- **Como implantado (|z| > {_num(z, 0)}):** Precision {_pm_pct(deployed['precision'])}, "
        f"Recall {_pm_pct(deployed['recall'])}, FPR {_pm_pct(deployed['fpr'], 2)}, "
        f"{_pm_num(deployed['alerts_per_1000'], 1)} alertas por 1.000 transações.",
        f"- **Hipótese de Precision ≈ 50%:** {verdict}.",
        f"- **Na regra de operação (recall máximo com FPR ≤ {_fpr_label(PRIMARY_FPR)}):** "
        f"limiar |z| > {_num(threshold, 2)}, Recall {_pm_pct(primary['recall'])} e Precision "
        f"{_pm_pct(primary['precision'])}.",
        f"- **Ranking (independe de limiar):** PR-AUC {_pm_pct(results['ranking']['pr_auc'])} e "
        f"recall a FPR de {_fpr_label(PRIMARY_FPR)} de {_pm_pct(results['ranking']['recall_at_fpr'])}.",
    ]
    if by_type:
        worst = min(by_type, key=lambda t: (by_type[t], t))
        best = max(by_type, key=lambda t: (by_type[t], t))
        lines.append(
            f"- **Por tipo (como implantado):** maior Recall em `{best}` ({_pct(by_type[best])}), "
            f"menor em `{worst}` ({_pct(by_type[worst])})."
        )
    batch = results.get("batch_density")
    lines.append(
        f"- **Cobertura do baseline de 1 h:** {_pm_pct(results['coverage'])} dos eventos no replay "
        "de stream"
        + (
            f", contra {_pct(batch['coverage'], 2)} na densidade do `make seed-data` "
            f"({_num(batch['events_per_customer'], 0)} eventos por cliente em 180 dias): "
            "**o V1 só enxerga fraude na densidade do streaming**."
            if batch
            else "."
        )
    )
    return "\n".join(lines)


def _protocol(results: dict[str, Any]) -> str:
    c = results["config"]
    tests = results["tests"]
    val = results["validation"]
    mean_events = sum(t["events"] for t in tests) / len(tests)
    mean_fraud = sum(t["fraud"] for t in tests) / len(tests)
    mean_ep = sum(t["episodes"] for t in tests) / len(tests)
    rows = [
        [
            "Regime principal",
            "replay de stream: o `TransactionStream` do producer em tempo simulado",
        ],
        ["Clientes por dataset", _int(c["n_customers"])],
        ["Taxa", f"{_num(c['rate_tps'], 0)} eventos/s (ritmo constante)"],
        [
            "Duração simulada",
            f"{_int(c['duration_minutes'])} min; follow-ups de episódios entram inteiros",
        ],
        [
            "Corte (`--profile-until`)",
            f"{c['cutoff']}: antes dele os eventos só alimentam a janela do detector",
        ],
        ["Seed de validação", str(c["validation_seed"])],
        ["Seeds de teste", ", ".join(str(s) for s in c["test_seeds"])],
        ["Eventos avaliados por seed de teste (média)", _int(mean_events)],
        ["Eventos fraudulentos por seed de teste (média)", _int(mean_fraud)],
        ["Episódios de fraude por seed de teste (média)", _int(mean_ep)],
        ["Eventos avaliados na validação", _int(val["events"])],
    ]
    detector = (
        f"`{results['detector']}`: Z-Score do `amount` cru por cliente, janela deslizante de "
        f"{_int(c['z_window_seconds'] / 60)} min anteriores ao evento, mínimo de "
        f"{c['z_min_transactions']} transações na janela e alerta quando |z| > "
        f"{_num(c['z_threshold'], 0)}. O score de ranking é |z|; evento sem baseline tem score 0."
    )
    return (
        _table(["Parâmetro", "Valor"], rows, text_cols=2)
        + "\n\n"
        + f"**Detector:** {detector}\n\n"
        + "**Regra de operação:** *recall máximo com FPR ≤ alvo*. O limiar é o menor |z| cujo FPR "
        + "na seed de validação não passa do alvo, e é aplicado sem ajuste nas seeds de teste "
        + "(por isso o FPR medido nos testes varia em torno do alvo). Resultados são média ± "
        + "desvio padrão amostral sobre as seeds de teste; nas taxas, o desvio é em pontos "
        + "percentuais."
    )


_METRIC_HEADERS = ["Precision", "Recall", "F1", "FPR", "FNR", "Alertas/1.000 tx"]


def _metric_cells(agg: dict[str, Any]) -> list[str]:
    return [
        _pm_pct(agg["precision"]),
        _pm_pct(agg["recall"]),
        _pm_pct(agg["f1"]),
        _pm_pct(agg["fpr"], 2),
        _pm_pct(agg["fnr"]),
        _pm_num(agg["alerts_per_1000"], 1),
    ]


def _results(results: dict[str, Any]) -> str:
    cond = results["conditions"]
    z = results["config"]["z_threshold"]
    deployed_table = _table(_METRIC_HEADERS, [_metric_cells(cond[DEPLOYED])])

    rows = []
    for fpr in OPERATING_FPRS:
        key = condition_key(fpr)
        rows.append(
            [_fpr_label(fpr), _num(results["thresholds"][fpr], 2)] + _metric_cells(cond[key])
        )
    operating_table = _table(["FPR alvo", "Limiar |z|"] + _METRIC_HEADERS, rows)

    tp = cond[DEPLOYED]
    counts = (
        f"Soma nas seeds de teste (como implantado): {_int(tp['tp'][0] * len(results['tests']))} "
        f"verdadeiros positivos, {_int(tp['fp'][0] * len(results['tests']))} falsos positivos, "
        f"{_int(tp['fn'][0] * len(results['tests']))} falsos negativos."
    )
    return (
        f"### Como implantado (|z| > {_num(z, 0)})\n\n{deployed_table}\n\n{counts}\n\n"
        f"### Na regra de operação e sensibilidade ao FPR alvo\n\n{operating_table}\n\n"
        f"A linha de {_fpr_label(PRIMARY_FPR)} é a regra de operação da série. As outras mostram "
        "o custo em Recall de exigir menos falso alerta (e o ganho de tolerar mais).\n\n"
        "### Qualidade do ranking (não depende de limiar)\n\n"
        + _table(
            ["PR-AUC", f"Recall a FPR de {_fpr_label(PRIMARY_FPR)} (no próprio conjunto)"],
            [
                [
                    _pm_pct(results["ranking"]["pr_auc"]),
                    _pm_pct(results["ranking"]["recall_at_fpr"]),
                ]
            ],
        )
    )


def _where_it_errs(results: dict[str, Any]) -> str:
    cond = results["conditions"]
    deployed, primary = cond[DEPLOYED], cond[condition_key(PRIMARY_FPR)]
    label = _fpr_label(PRIMARY_FPR)

    type_rows = []
    for t in FRAUD_TYPES:
        d, p = deployed["by_type"].get(t), primary["by_type"].get(t)
        if d is None:
            continue
        type_rows.append([f"`{t}`", _int(d["n"]), _pm_pct(d["value"]), _pm_pct(p["value"])])

    scenario_rows = []
    for t in FRAUD_TYPES:
        for mode in ("normal", "stealth"):
            key = f"{t}|{mode}"
            d, p = deployed["by_scenario"].get(key), primary["by_scenario"].get(key)
            if d is None:
                continue
            scenario_rows.append(
                [
                    f"`{t}`",
                    _STEALTH_LABEL[mode],
                    _int(d["n"]),
                    _pm_pct(d["value"]),
                    _pm_pct(p["value"]),
                ]
            )

    hn_rows = []
    for kind in (*HARD_NEGATIVES, NO_HARD_NEGATIVE):
        d, p = deployed["by_hard_negative"].get(kind), primary["by_hard_negative"].get(kind)
        if d is None:
            continue
        name = "legítimo sem nenhum" if kind == NO_HARD_NEGATIVE else f"`{kind}`"
        hn_rows.append([name, _int(d["n"]), _pm_pct(d["value"], 2), _pm_pct(p["value"], 2)])

    ep = {name: cond[name]["episodes"] for name in (DEPLOYED, condition_key(PRIMARY_FPR))}
    ep_rows = [
        [
            "Como implantado",
            _pm_pct(ep[DEPLOYED]["recall"]),
            _seconds(ep[DEPLOYED]["ttd_median"][0]),
            _seconds(ep[DEPLOYED]["ttd_p90"][0]),
        ],
        [
            f"FPR ≤ {label}",
            _pm_pct(ep[condition_key(PRIMARY_FPR)]["recall"]),
            _seconds(ep[condition_key(PRIMARY_FPR)]["ttd_median"][0]),
            _seconds(ep[condition_key(PRIMARY_FPR)]["ttd_p90"][0]),
        ],
    ]
    return (
        "### Recall por tipo de fraude\n\n"
        + _table(["Tipo", "Eventos (soma)", "Como implantado", f"FPR ≤ {label}"], type_rows)
        + "\n\n### Recall por cenário e variante stealth\n\n"
        + "A variante *stealth* mascara um sinal (por exemplo, o account takeover que reusa a rede "
        + "da vítima); o Recall dela mostra o quanto o detector depende desse sinal.\n\n"
        + _table(
            ["Tipo", "Variante", "Eventos (soma)", "Como implantado", f"FPR ≤ {label}"],
            scenario_rows,
            text_cols=2,
        )
        + "\n\n### Falsos positivos por tipo de hard negative\n\n"
        + "Eventos legítimos que imitam fraude (`new_device`: troca de celular; `new_ip`: rede "
        + "nova; `travel`: viagem legítima; `big_purchase`: compra grande; `off_hours`: fora do "
        + "horário habitual do cliente). O valor é a fração alertada, isto é, o FPR do grupo.\n\n"
        + _table(["Grupo", "Eventos (soma)", "Como implantado", f"FPR ≤ {label}"], hn_rows)
        + "\n\n### Detecção por episódio e time-to-detect\n\n"
        + "Um episódio conta como detectado se algum dos seus eventos alertou. O *time-to-detect* é "
        + "o intervalo entre o primeiro evento fraudulento e o primeiro evento alertado, nos "
        + "episódios detectados (média das medianas e dos p90 entre as seeds).\n\n"
        + _table(
            ["Decisão", "Episódios detectados", "TTD mediano", "TTD p90"],
            ep_rows,
        )
    )


def _density(results: dict[str, Any]) -> str:
    batch = results.get("batch_density")
    rows = [
        [
            "Replay de stream (regime principal)",
            _int(sum(t["events"] for t in results["tests"]) / len(results["tests"])),
            _pm_pct(results["coverage"]),
            _pm_pct(results["conditions"][DEPLOYED]["recall"]),
            _pm_pct(results["conditions"][DEPLOYED]["precision"]),
        ]
    ]
    if batch:
        rows.append(
            [
                f"Densidade do `make seed-data` ({_num(batch['events_per_customer'], 0)} eventos/cliente em 180 dias)",
                _int(batch["events"]),
                _pct(batch["coverage"], 2),
                _pct(batch["recall"], 2),
                _pct(batch["precision"], 1),
            ]
        )
    text = (
        "Cobertura é a fração de eventos com baseline (pelo menos "
        f"{results['config']['z_min_transactions']} transações do mesmo cliente na janela anterior "
        "de 1 h). Sem baseline o Z-Score não é calculado e o evento nunca alerta.\n\n"
        + _table(["Regime", "Eventos avaliados", "Cobertura", "Recall", "Precision"], rows)
    )
    if batch:
        text += (
            "\n\nO V1 foi desenhado para o streaming: com poucos eventos por cliente ao longo de "
            "meses, quase nenhum evento tem 2 transações na hora anterior, e o detector fica cego. "
            "Por isso a avaliação usa o replay de stream, que é o regime em que ele roda; a linha "
            "de densidade de batch fica como evidência. A avaliação do V2 (#45) precisa considerar "
            "os dois regimes, já que o perfil de comportamento vem do batch."
        )
    return text


def _hypothesis(results: dict[str, Any]) -> str:
    deployed = results["conditions"][DEPLOYED]
    precision, fpr = deployed["precision"][0], deployed["fpr"][0]
    _, verdict = _hypothesis_verdict(precision)
    in_range = ANALYTIC_FPR_RANGE[0] <= fpr <= ANALYTIC_FPR_RANGE[1]
    fpr_note = (
        f"O FPR medido ({_pct(fpr, 2)}) ficou {'dentro' if in_range else 'fora'} da faixa analítica "
        f"de {_pct(ANALYTIC_FPR_RANGE[0])}–{_pct(ANALYTIC_FPR_RANGE[1])}"
        + (
            ": a estimativa do falso alerta se sustenta."
            if in_range
            else ": a estimativa analítica do falso alerta não se sustentou."
        )
    )
    return (
        "A issue #44 estimou, com um cálculo analítico, que a Precision do V1 ficaria perto de 50%: "
        "o `amount` do gerador é lognormal (μ = 5, σ = 1,2), então cerca de 1,6% dos legítimos passam "
        "de média + 3·desvio (até ~4,6% com baseline amostral), contra ~2,5% de fraude.\n\n"
        f"**Resultado:** {verdict}. Precision {_pm_pct(deployed['precision'])}, Recall "
        f"{_pm_pct(deployed['recall'])}. {fpr_note}\n\n"
        "A regra de leitura foi fixada antes de medir: confirmada se a Precision média cair em "
        f"{_pct(HYPOTHESIS_PRECISION, 0)} ± {_num(HYPOTHESIS_TOLERANCE * 100, 0)} pontos percentuais."
    )


def _limitations() -> str:
    return "\n".join(
        [
            "- **Circularidade.** Os dados são sintéticos e vêm do mesmo projeto que o detector. As "
            "métricas medem a concordância entre o gerador e o detector, não o desempenho em produção. "
            "O ganho que importa é **relativo** (V1 × V2, sobre o mesmo dado); nenhum valor absoluto "
            "deste documento deve ser citado como desempenho real.",
            "- **Mitigações embutidas.** Hard negatives no tráfego legítimo, ~20% dos episódios na "
            "variante stealth, limiar calibrado em uma seed diferente das de teste, e seeds de teste "
            "com clientes diferentes entre si e da validação.",
            "- **Regime.** Replay com ritmo constante e qualquer cliente pode transacionar a qualquer "
            "hora (`diurnal=False`); o detector V1 não usa hora, então isso não o afeta, mas afeta "
            "sinais como `UNUSUAL_HOUR` no V2.",
            "- **Viagens legítimas.** Cerca de 1% dos clientes já começa o replay viajando (estado "
            "estacionário). Quem viaja fica em trânsito, sem emitir eventos, durante o "
            "deslocamento, e transaciona na cidade destino por 6 a 48 h; a fração de eventos "
            "`travel` fica por isso abaixo de 1%.",
            "- **Um detector.** Só o V1 é medido aqui. O V2 entra na #45 pela mesma interface "
            "(`Detector`), no mesmo protocolo.",
            "- **Reprodutibilidade.** Mesma configuração e mesmas seeds geram o mesmo documento. "
            "Mudar clientes, taxa, duração, corte ou seeds muda os números.",
            "- **Latência de detecção online** (p50/p95 de `latency_seconds`) não é medida aqui: "
            "vem do Postgres, da execução do streaming, na #47.",
        ]
    )


def render_report(results: dict[str, Any]) -> str:
    """Documento Markdown completo (determinístico)."""
    parts = [
        f"# Avaliação do detector de fraude: `{results['detector']}`",
        "> Documento gerado por `make fraud-eval` (`src/transformation/fraud/evaluate.py`). Não "
        "edite à mão: rode o comando de novo. Mesma configuração e mesmas seeds geram o mesmo "
        "documento.",
        "## Resumo\n\n" + _summary(results),
        "## Protocolo\n\n" + _protocol(results),
        "## Resultados\n\n" + _results(results),
        "## Onde o detector acerta e erra\n\n" + _where_it_errs(results),
        "## Cobertura por densidade de eventos\n\n" + _density(results),
        "## Hipótese: Precision perto de 50%\n\n" + _hypothesis(results),
        "## Limitações e circularidade\n\n" + _limitations(),
    ]
    return "\n\n".join(parts) + "\n"
