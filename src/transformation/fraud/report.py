"""Renderiza `docs/fraud_evaluation.md` a partir dos resultados da avaliação (issues #44 e #45).

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
    NO_TYPE,
    OPERATING_FPRS,
    PRIMARY_FPR,
    SENSITIVITY_SIGNAL,
    condition_key,
)
from src.transformation.fraud.profile import MIN_HISTORY
from src.transformation.fraud.signals import SIGNALS

_STEALTH_LABEL = {"normal": "normal", "stealth": "stealth"}
V1 = "zscore-v1"
V2 = "multisignal-v2"

# ── Formatação (pt-BR: vírgula decimal, ponto de milhar) ───────────────────────


def _nan(x: float | None) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


def _num(x: float | None, digits: int = 1) -> str:
    return "n/d" if x is None or _nan(x) else f"{x:.{digits}f}".replace(".", ",")


def _int(x: float | None) -> str:
    return "n/d" if x is None or _nan(x) else f"{x:,.0f}".replace(",", ".")


def _pct(x: float | None, digits: int = 1) -> str:
    return "n/d" if x is None or _nan(x) else _num(x * 100, digits) + "%"


def _pm_pct(mean_sd: tuple[float, float], digits: int = 1) -> str:
    mean, sd = mean_sd
    if _nan(mean):
        return "n/d"
    return f"{_num(mean * 100, digits)}% ± {_num(sd * 100, digits)}"


def _pm_pp(mean_sd: tuple[float, float], digits: int = 1) -> str:
    """Diferença em pontos percentuais, com sinal."""
    mean, sd = mean_sd
    if _nan(mean):
        return "n/d"
    sign = "+" if mean >= 0 else "−"
    return f"{sign}{_num(abs(mean) * 100, digits)} p.p. ± {_num(sd * 100, digits)}"


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


def _by_name(results: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {d["name"]: d for d in results["detectors"]}


# ── Hipótese do V1 ─────────────────────────────────────────────────────────────


def _hypothesis_verdict(precision: float) -> tuple[bool, str]:
    """Regra fixada antes de medir: confirmada se a Precision cai em 50% ± 10 p.p."""
    diff = precision - HYPOTHESIS_PRECISION
    confirmed = abs(diff) <= HYPOTHESIS_TOLERANCE
    low = HYPOTHESIS_PRECISION - HYPOTHESIS_TOLERANCE
    high = HYPOTHESIS_PRECISION + HYPOTHESIS_TOLERANCE
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


def _hypothesis(v1: dict[str, Any]) -> str:
    deployed = v1["conditions"][DEPLOYED]
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


# ── Resumo ─────────────────────────────────────────────────────────────────────


def _summary(results: dict[str, Any]) -> str:
    detectors = _by_name(results)
    z = results["config"]["z_threshold"]
    primary = condition_key(PRIMARY_FPR)
    lines: list[str] = []

    v1 = detectors.get(V1)
    if v1:
        d = v1["conditions"][DEPLOYED]
        lines.append(
            f"- **V1 como implantado (|z| > {_num(z, 0)}):** Precision {_pm_pct(d['precision'])}, "
            f"Recall {_pm_pct(d['recall'])}, FPR {_pm_pct(d['fpr'], 2)}, "
            f"{_pm_num(d['alerts_per_1000'], 1)} alertas por 1.000 transações."
        )
        verdict = _hypothesis_verdict(d["precision"][0])[1]
        lines.append(f"- **Hipótese de Precision ≈ 50% (V1):** {verdict}.")
    v2 = detectors.get(V2)
    if v2:
        d = v2["conditions"][DEPLOYED]
        lines.append(
            f"- **V2 como implantado (score > {_num(v2['alert_threshold'], 3)}):** Precision "
            f"{_pm_pct(d['precision'])}, Recall {_pm_pct(d['recall'])}, FPR {_pm_pct(d['fpr'], 2)}, "
            f"{_pm_num(d['alerts_per_1000'], 1)} alertas por 1.000 transações."
        )
    for comparison in results.get("comparison", []):
        c = comparison["conditions"]
        lines.append(
            f"- **{comparison['challenger']} × {comparison['baseline']} (pareado por seed):** "
            f"ΔF1 {_pm_pp(c[DEPLOYED]['f1'])} como implantado (vence em {c[DEPLOYED]['wins']}/"
            f"{c[DEPLOYED]['seeds']} seeds) e {_pm_pp(c[primary]['f1'])} na regra de operação "
            f"(vence em {c[primary]['wins']}/{c[primary]['seeds']})."
        )
    if v2:
        conf = v2.get("type_confusion")
        if conf:
            total = sum(sum(row.values()) for row in conf.values())
            right = sum(conf[t][t] for t in FRAUD_TYPES)
            none = sum(conf[t][NO_TYPE] for t in FRAUD_TYPES)
            lines.append(
                f"- **Tipo inferido (V2), entre as fraudes alertadas:** {_pct(right / total)} "
                f"corretos e {_pct(none / total)} sem tipo ({_int(total)} alertas verdadeiros)."
            )
        sens = v2.get("sensitivity")
        if sens:
            lines.append(
                f"- **Circularidade dimensionada:** sem o sinal `{sens['signal']}` (que o gerador "
                "injeta em toda a fraude), recalibrado na validação, o V2 fica com Recall "
                f"{_pm_pct(sens['recall'])} e F1 {_pm_pct(sens['f1'])} a FPR de "
                f"{_pm_pct(sens['fpr'], 2)} (seção de sensibilidade)."
            )
    batch = results.get("batch_density")
    if v1 and batch:
        lines.append(
            f"- **Cobertura do baseline de 1 h do V1:** {_pm_pct(v1['coverage'])} dos eventos no "
            f"replay de stream, contra {_pct(batch['coverage'], 2)} na densidade do `make seed-data` "
            f"({_num(batch['events_per_customer'], 0)} eventos por cliente em 180 dias): **o V1 só "
            "enxerga fraude na densidade do streaming**."
        )
    return "\n".join(lines)


# ── Protocolo ──────────────────────────────────────────────────────────────────


def _protocol(results: dict[str, Any]) -> str:
    c = results["config"]
    tests = results["tests"]
    val = results["validation"]
    mean_events = sum(t["events"] for t in tests) / len(tests)
    mean_fraud = sum(t["fraud"] for t in tests) / len(tests)
    mean_ep = sum(t["episodes"] for t in tests) / len(tests)
    rhythm = "ritmo diurno" if c["diurnal"] else "ritmo constante"
    rows = [
        [
            "Regime principal",
            "replay de stream: o `TransactionStream` do producer em tempo simulado",
        ],
        ["Clientes por dataset", _int(c["n_customers"])],
        ["Taxa", f"{_num(c['rate_tps'], 0)} eventos/s ({rhythm})"],
        [
            "Duração simulada",
            f"{_int(c['duration_minutes'])} min; follow-ups de episódios entram inteiros",
        ],
        [
            "Corte (`--profile-until`)",
            f"{c['cutoff']}: antes dele os eventos só alimentam o estado do detector",
        ],
    ]
    if c.get("history_transactions"):
        rows.append(
            [
                "Histórico de batch (perfil do V2)",
                f"{_int(c['history_transactions'])} transações em {_int(c['history_days'])} dias, "
                "dos mesmos clientes",
            ]
        )
    rows += [
        ["Seed de validação", str(c["validation_seed"])],
        ["Seeds de teste", ", ".join(str(s) for s in c["test_seeds"])],
        ["Eventos avaliados por seed de teste (média)", _int(mean_events)],
        ["Eventos fraudulentos por seed de teste (média)", _int(mean_fraud)],
        ["Episódios de fraude por seed de teste (média)", _int(mean_ep)],
        ["Eventos avaliados na validação", _int(val["events"])],
    ]
    descriptions = []
    detectors = _by_name(results)
    if V1 in detectors:
        descriptions.append(
            f"- **`{V1}`**: Z-Score do `amount` cru por cliente, janela deslizante de "
            f"{_int(c['z_window_seconds'] / 60)} min anteriores ao evento, mínimo de "
            f"{c['z_min_transactions']} transações na janela e alerta quando |z| > "
            f"{_num(c['z_threshold'], 0)}. O score de ranking é |z|; evento sem baseline tem score 0."
        )
    if V2 in detectors:
        v2 = detectors[V2]
        descriptions.append(
            f"- **`{V2}`**: {len(SIGNALS)} sinais (perfil do batch: valor, device, rede, "
            "destinatário, hora, local e idade da conta; janela curta: velocidade, viagem "
            "impossível e concentração de destinatários) combinados por noisy-OR "
            "(`score = 1 − Π(1 − wᵢ·sᵢ)`). Pesos e limiar calibrados só na seed de validação e "
            f"versionados em `weights.py`; alerta quando score > {_num(v2['alert_threshold'], 3)}. "
            "O detector nunca lê o rótulo: só enxerga id, cliente, instante, valor, device, IP, "
            "coordenadas e destinatário."
        )
    return (
        _table(["Parâmetro", "Valor"], rows, text_cols=2)
        + "\n\n**Detectores:**\n\n"
        + "\n".join(descriptions)
        + "\n\n**Regra de operação:** *recall máximo com FPR ≤ alvo*. O limiar é o menor score cujo "
        "FPR na seed de validação não passa do alvo, e é aplicado sem ajuste nas seeds de teste "
        "(por isso o FPR medido nos testes varia em torno do alvo). Resultados são média ± "
        "desvio padrão amostral sobre as seeds de teste; nas taxas, o desvio é em pontos "
        "percentuais. As comparações entre detectores são **pareadas por seed** (mesmos eventos)."
    )


# ── Comparação ─────────────────────────────────────────────────────────────────

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


def _metric_cells_short(agg: dict[str, Any]) -> list[str]:
    return [
        _pm_pct(agg["precision"]),
        _pm_pct(agg["recall"]),
        _pm_pct(agg["f1"]),
        _pm_pct(agg["fpr"], 2),
    ]


def _comparison(results: dict[str, Any]) -> str:
    primary = condition_key(PRIMARY_FPR)
    label = _fpr_label(PRIMARY_FPR)
    rows = []
    for cond, name in (
        (DEPLOYED, "Como implantado"),
        (primary, f"Regra de operação (FPR ≤ {label})"),
    ):
        for det in results["detectors"]:
            rows.append([name, f"`{det['name']}`", *_metric_cells(det["conditions"][cond])])
    table = _table(["Decisão", "Detector", *_METRIC_HEADERS], rows, text_cols=2)

    diffs = []
    for comp in results.get("comparison", []):
        for cond, name in ((DEPLOYED, "Como implantado"), (primary, f"FPR ≤ {label}")):
            c = comp["conditions"][cond]
            diffs.append(
                [
                    f"`{comp['challenger']}` − `{comp['baseline']}`",
                    name,
                    _pm_pp(c["precision"]),
                    _pm_pp(c["recall"]),
                    _pm_pp(c["f1"]),
                    f"{c['wins']}/{c['seeds']}",
                ]
            )
    paired = ""
    if diffs:
        paired = "\n\n### Diferença pareada por seed\n\n" + _table(
            ["Comparação", "Decisão", "ΔPrecision", "ΔRecall", "ΔF1", "Seeds em que vence (F1)"],
            diffs,
            text_cols=2,
        )
    return table + paired


# ── Resultados por detector ────────────────────────────────────────────────────


def _detector_results(det: dict[str, Any], config: dict[str, Any]) -> str:
    cond = det["conditions"]
    rows = []
    for fpr in OPERATING_FPRS:
        rows.append(
            [_fpr_label(fpr), _num(det["thresholds"][fpr], 3)]
            + _metric_cells(cond[condition_key(fpr)])
        )
    operating = _table(["FPR alvo", "Limiar do score", *_METRIC_HEADERS], rows, text_cols=1)
    counts = cond[DEPLOYED]
    seeds = len(config["test_seeds"])
    return (
        "**Como implantado**\n\n"
        + _table(_METRIC_HEADERS, [_metric_cells(cond[DEPLOYED])], text_cols=0)
        + f"\n\nSoma nas seeds de teste: {_int(counts['tp'][0] * seeds)} verdadeiros positivos, "
        f"{_int(counts['fp'][0] * seeds)} falsos positivos, {_int(counts['fn'][0] * seeds)} falsos "
        "negativos.\n\n**Na regra de operação e sensibilidade ao FPR alvo**\n\n"
        + operating
        + f"\n\nA linha de {_fpr_label(PRIMARY_FPR)} é a regra de operação da série.\n\n"
        "**Qualidade do ranking (não depende de limiar)**\n\n"
        + _table(
            ["PR-AUC", f"Recall a FPR de {_fpr_label(PRIMARY_FPR)} (no próprio conjunto)"],
            [[_pm_pct(det["ranking"]["pr_auc"]), _pm_pct(det["ranking"]["recall_at_fpr"])]],
            text_cols=0,
        )
    )


def _where_it_errs(det: dict[str, Any]) -> str:
    cond = det["conditions"]
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
        "**Recall por tipo de fraude**\n\n"
        + _table(["Tipo", "Eventos (soma)", "Como implantado", f"FPR ≤ {label}"], type_rows)
        + "\n\n**Recall por cenário e variante stealth** (a variante *stealth* mascara um sinal; o "
        "Recall dela mostra o quanto o detector depende dele)\n\n"
        + _table(
            ["Tipo", "Variante", "Eventos (soma)", "Como implantado", f"FPR ≤ {label}"],
            scenario_rows,
            text_cols=2,
        )
        + "\n\n**Falsos positivos por tipo de hard negative** (eventos legítimos que imitam fraude; "
        "o valor é a fração alertada, isto é, o FPR do grupo)\n\n"
        + _table(["Grupo", "Eventos (soma)", "Como implantado", f"FPR ≤ {label}"], hn_rows)
        + "\n\n**Detecção por episódio e time-to-detect** (um episódio conta como detectado se algum "
        "dos seus eventos alertou; o *time-to-detect* é o intervalo entre o primeiro evento "
        "fraudulento e o primeiro alertado, nos episódios detectados)\n\n"
        + _table(["Decisão", "Episódios detectados", "TTD mediano", "TTD p90"], ep_rows)
    )


# ── Extras do V2 ───────────────────────────────────────────────────────────────

_SHORT_TYPE = {
    "ACCOUNT_TAKEOVER": "Account takeover",
    "CARD_CLONING": "Clone de cartão",
    "IDENTITY_THEFT": "Roubo de identidade",
    "MONEY_LAUNDERING": "Lavagem",
    "SOCIAL_ENGINEERING": "Engenharia social",
}


def _type_confusion(det: dict[str, Any]) -> str:
    conf = det["type_confusion"]
    columns = [*FRAUD_TYPES, NO_TYPE]
    rows = []
    for true in FRAUD_TYPES:
        row = conf[true]
        total = sum(row.values())
        cells = [f"{_int(row[c])} ({_pct(row[c] / total, 0)})" if total else "n/d" for c in columns]
        rows.append([f"`{true}`", _int(total), *cells])
    headers = [_SHORT_TYPE.get(c, c) for c in columns]
    return (
        "Entre as fraudes que o V2 alertou (como implantado, soma das seeds de teste), o tipo que "
        "as regras inferiram. Linhas: tipo verdadeiro; colunas: tipo previsto. `nenhuma` = nenhuma "
        "regra de tipo casou. O tipo nunca é copiado do rótulo.\n\n"
        + _table(["Tipo verdadeiro", "Alertas", *headers], rows, text_cols=1)
    )


def _signal_table(det: dict[str, Any]) -> str:
    rows = []
    for name in SIGNALS:
        s = det["signals"][name]
        rows.append(
            [
                f"`{name}`",
                _num(s["weight"], 2),
                _pct(s["legit"], 2),
                _pct(s["fraud"], 1),
                *[_pct(s["by_type"][t], 0) for t in FRAUD_TYPES],
            ]
        )
    headers = [_SHORT_TYPE[t] for t in FRAUD_TYPES]
    return (
        "Com que frequência cada sinal está ativo (≥ 0,5): no tráfego legítimo, na fraude e em cada "
        "tipo. Um sinal útil acende muito mais na fraude do que no legítimo; um que acende igual nos "
        "dois não discrimina, e a calibração o deixa com peso zero.\n\n"
        + _table(["Sinal", "Peso", "Legítimo", "Fraude", *headers], rows, text_cols=1)
    )


def _sensitivity(v2: dict[str, Any]) -> str:
    sens = v2["sensitivity"]
    primary = v2["conditions"][condition_key(PRIMARY_FPR)]
    label = _fpr_label(PRIMARY_FPR)
    rows = [
        ["V2 completo", *_metric_cells_short(primary)],
        [f"V2 sem `{sens['signal']}` (recalibrado)", *_metric_cells_short(sens)],
    ]
    type_rows = []
    for t in FRAUD_TYPES:
        full = primary["by_type"].get(t)
        without = sens["by_type"].get(t)
        if full is None or without is None:
            continue
        type_rows.append([f"`{t}`", _pm_pct(full["value"]), _pm_pct(without["value"])])
    return (
        f"O gerador manda **toda** a fraude para uma conta-destino nova (`{sens['signal']}` ativo em "
        "100% dela), e em produção parte da fraude usa destinatários conhecidos. Para dimensionar "
        "quanto o V2 depende desse sinal, ele é retirado, os pesos e o limiar são recalibrados "
        f"**só na validação** (mesma regra: recall máximo com FPR ≤ {label}) e o resultado é medido "
        "nas seeds de teste.\n\n"
        + _table(["Variante", "Precision", "Recall", "F1", "FPR"], rows, text_cols=1)
        + "\n\nRecall por tipo:\n\n"
        + _table(["Tipo", "V2 completo", f"Sem `{sens['signal']}`"], type_rows, text_cols=1)
    )


def _detector_section(det: dict[str, Any], results: dict[str, Any]) -> str:
    parts = [
        f"### Resultados\n\n{_detector_results(det, results['config'])}",
        f"### Onde o `{det['name']}` acerta e erra\n\n{_where_it_errs(det)}",
    ]
    if det.get("type_confusion"):
        parts.append(f"### Tipo inferido: matriz de confusão\n\n{_type_confusion(det)}")
    if det.get("signals"):
        parts.append(f"### Os sinais\n\n{_signal_table(det)}")
    if det.get("sensitivity"):
        parts.append(f"### Sensibilidade: o V2 sem `{SENSITIVITY_SIGNAL}`\n\n{_sensitivity(det)}")
    return "\n\n".join(parts)


# ── Cobertura e limitações ─────────────────────────────────────────────────────


def _density(results: dict[str, Any]) -> str:
    detectors = _by_name(results)
    batch = results.get("batch_density")
    tests = results["tests"]
    n_events = sum(t["events"] for t in tests) / len(tests)
    rows = []
    for det in results["detectors"]:
        what = "baseline de 1 h" if det["name"] == V1 else "perfil do cliente"
        rows.append(
            [
                f"`{det['name']}`: replay de stream",
                _int(n_events),
                _pm_pct(det["coverage"]),
                _pm_pct(det["conditions"][DEPLOYED]["recall"]),
                _pm_pct(det["conditions"][DEPLOYED]["precision"]),
                what,
            ]
        )
    if batch and V1 in detectors:
        per_customer = _num(batch["events_per_customer"], 0)
        rows.append(
            [
                f"`{V1}`: densidade do `make seed-data` ({per_customer} eventos/cliente em 180 dias)",
                _int(batch["events"]),
                _pct(batch["coverage"], 2),
                _pct(batch["recall"], 2),
                _pct(batch["precision"], 1),
                "baseline de 1 h",
            ]
        )
    text = (
        "Cobertura é a fração de eventos com o contexto de que o detector precisa: para o V1, "
        f"pelo menos {results['config']['z_min_transactions']} transações do mesmo cliente na janela "
        f"anterior de 1 h; para o V2, pelo menos {MIN_HISTORY} transações legítimas do cliente no "
        "histórico de batch (o perfil). Sem contexto o detector não pontua e o evento nunca alerta pelo sinal "
        "que depende dele.\n\n"
        + _table(
            ["Regime", "Eventos avaliados", "Cobertura", "Recall", "Precision", "Contexto"],
            rows,
            text_cols=1,
        )
    )
    if batch and V1 in detectors:
        text += (
            "\n\nO V1 foi desenhado para o streaming: com poucos eventos por cliente ao longo de "
            "meses, quase nenhum evento tem 2 transações na hora anterior, e o detector fica cego. "
            "Por isso a avaliação usa o replay de stream, que é o regime em que ele roda. O V2 "
            "não sofre disso porque o perfil longo vem do batch e só os sinais de janela curta "
            "dependem da densidade do stream."
        )
    return text


def _limitations(results: dict[str, Any]) -> str:
    has_v2 = V2 in _by_name(results)
    items = [
        "- **Circularidade.** Os dados são sintéticos e vêm do mesmo projeto que os detectores. As "
        "métricas medem a concordância entre o gerador e o detector, não o desempenho em produção. "
        "O ganho que importa é **relativo** (V1 × V2, sobre o mesmo dado); nenhum valor absoluto "
        "deste documento deve ser citado como desempenho real.",
    ]
    if has_v2:
        items.append(
            "- **O destinatário é a assinatura mais óbvia do gerador.** Ele envia toda a fraude para "
            f"uma conta nova (`{SENSITIVITY_SIGNAL}` ativo em 100% dela), o que faz esse sinal, "
            "combinado com qualquer outro, separar fraude de legítimo com facilidade. A seção de "
            "sensibilidade mostra o V2 sem ele, e ele segue muito acima do V1. Isso **não** dá uma "
            "estimativa do Recall real: os demais sinais também vêm de assinaturas que o gerador "
            "injeta (valores altos, device novo, viagem impossível, vários remetentes para a mesma "
            "conta), então o Recall em produção pode ser menor que os dois valores."
        )
    items += [
        "- **Mitigações embutidas.** Hard negatives no tráfego legítimo, ~20% dos episódios na "
        "variante stealth, pesos e limiar calibrados em uma seed diferente das de teste, e seeds de "
        "teste com clientes diferentes entre si e da validação.",
        "- **Regime.** Replay com ritmo constante e qualquer cliente pode transacionar a qualquer "
        "hora (`diurnal=False`), e a janela padrão (4 h ao meio-dia) não passa pela madrugada: o "
        "sinal `UNUSUAL_HOUR` **não é exercitado** aqui (0% de ativação). Para exercitá-lo, use "
        "`--diurnal` com um `--start` noturno.",
    ]
    if has_v2:
        items.append(
            "- **`TX_VELOCITY` não discrimina neste regime.** Cada cliente transaciona a cada ~100 s "
            "(36 eventos/h), ritmo irreal; com isso a maioria dos legítimos passa de 10 transações "
            "em 10 min, e a calibração deixa o peso do sinal em zero. Em dados esparsos, o sinal "
            "faria sentido."
        )
    items.append(
        "- **Viagens legítimas.** Cerca de 1,2% dos clientes já começa o replay viajando (estado "
        "estacionário). Quem viaja fica em trânsito, sem emitir eventos, durante o deslocamento, e "
        "transaciona na cidade destino por 6 a 48 h; a quantidade de eventos `travel` está na "
        "tabela de hard negatives."
    )
    if has_v2:
        items.append(
            "- **Perfil do V2.** O histórico é gerado pelo mesmo gerador, dos mesmos clientes, e o "
            "perfil usa só as linhas legítimas dele (rótulos históricos existem após a confirmação). "
            "O detector nunca lê o rótulo do evento que pontua."
        )
    items += [
        "- **Reprodutibilidade.** Mesma configuração e mesmas seeds geram o mesmo documento. Mudar "
        "clientes, taxa, duração, corte, seeds ou o gerador muda os números; mudar o gerador ou os "
        "sinais exige rodar `make fraud-calibrate` de novo.",
        "- **Latência de detecção online** (p50/p95 de `latency_seconds`) não é medida aqui: vem do "
        "Postgres, da execução do streaming, na #47.",
    ]
    return "\n".join(items)


# ── Documento ──────────────────────────────────────────────────────────────────


def render_report(results: dict[str, Any]) -> str:
    """Documento Markdown completo (determinístico)."""
    detectors = _by_name(results)
    parts = [
        "# Avaliação dos detectores de fraude",
        "> Documento gerado por `make fraud-eval` (`src/transformation/fraud/evaluate.py`). Não "
        "edite à mão: rode o comando de novo. Mesma configuração e mesmas seeds geram o mesmo "
        "documento.",
        "## Resumo\n\n" + _summary(results),
        "## Protocolo\n\n" + _protocol(results),
        "## Comparação entre detectores\n\n" + _comparison(results),
    ]
    for det in results["detectors"]:
        parts.append(f"## Detector `{det['name']}`\n\n" + _detector_section(det, results))
    parts.append("## Cobertura por densidade de eventos\n\n" + _density(results))
    if V1 in detectors:
        parts.append("## Hipótese do V1: Precision perto de 50%\n\n" + _hypothesis(detectors[V1]))
    parts.append("## Limitações e circularidade\n\n" + _limitations(results))
    return "\n\n".join(parts) + "\n"
