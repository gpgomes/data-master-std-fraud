# Avaliação do detector de fraude: `zscore-v1`

> Documento gerado por `make fraud-eval` (`src/transformation/fraud/evaluate.py`). Não edite à mão: rode o comando de novo. Mesma configuração e mesmas seeds geram o mesmo documento.

## Resumo

- **Como implantado (|z| > 3):** Precision 40,7% ± 1,1, Recall 56,4% ± 1,5, FPR 2,21% ± 0,03, 36,3 ± 0,3 alertas por 1.000 transações.
- **Hipótese de Precision ≈ 50%:** **confirmada** (Precision de 40,7%, 9,3 p.p. abaixo de 50%; perto do limite da faixa de 40%–60%).
- **Na regra de operação (recall máximo com FPR ≤ 1,0%):** limiar |z| > 4,53, Recall 43,5% ± 1,9 e Precision 54,1% ± 1,8.
- **Ranking (independe de limiar):** PR-AUC 49,5% ± 2,2 e recall a FPR de 1,0% de 43,6% ± 1,9.
- **Por tipo (como implantado):** maior Recall em `MONEY_LAUNDERING` (90,3%), menor em `CARD_CLONING` (13,5%).
- **Cobertura do baseline de 1 h:** 100,0% ± 0,0 dos eventos no replay de stream, contra 0,48% na densidade do `make seed-data` (50 eventos por cliente em 180 dias): **o V1 só enxerga fraude na densidade do streaming**.

## Protocolo

| Parâmetro | Valor |
|---|---|
| Regime principal | replay de stream: o `TransactionStream` do producer em tempo simulado |
| Clientes por dataset | 1.000 |
| Taxa | 10 eventos/s (ritmo constante) |
| Duração simulada | 240 min; follow-ups de episódios entram inteiros |
| Corte (`--profile-until`) | 2026-03-02T13:00:00+00:00: antes dele os eventos só alimentam a janela do detector |
| Seed de validação | 1000 |
| Seeds de teste | 1, 2, 3, 4, 5 |
| Eventos avaliados por seed de teste (média) | 109.695 |
| Eventos fraudulentos por seed de teste (média) | 2.874 |
| Episódios de fraude por seed de teste (média) | 1.665 |
| Eventos avaliados na validação | 109.613 |

**Detector:** `zscore-v1`: Z-Score do `amount` cru por cliente, janela deslizante de 60 min anteriores ao evento, mínimo de 2 transações na janela e alerta quando |z| > 3. O score de ranking é |z|; evento sem baseline tem score 0.

**Regra de operação:** *recall máximo com FPR ≤ alvo*. O limiar é o menor |z| cujo FPR na seed de validação não passa do alvo, e é aplicado sem ajuste nas seeds de teste (por isso o FPR medido nos testes varia em torno do alvo). Resultados são média ± desvio padrão amostral sobre as seeds de teste; nas taxas, o desvio é em pontos percentuais.

## Resultados

### Como implantado (|z| > 3)

| Precision | Recall | F1 | FPR | FNR | Alertas/1.000 tx |
|---|---:|---:|---:|---:|---:|
| 40,7% ± 1,1 | 56,4% ± 1,5 | 47,3% ± 1,2 | 2,21% ± 0,03 | 43,6% ± 1,5 | 36,3 ± 0,3 |

Soma nas seeds de teste (como implantado): 8.105 verdadeiros positivos, 11.819 falsos positivos, 6.266 falsos negativos.

### Na regra de operação e sensibilidade ao FPR alvo

| FPR alvo | Limiar |z| | Precision | Recall | F1 | FPR | FNR | Alertas/1.000 tx |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0,5% | 5,84 | 64,4% ± 1,9 | 35,7% ± 1,8 | 45,9% ± 1,9 | 0,53% ± 0,02 | 64,3% ± 1,8 | 14,5 ± 0,5 |
| 1,0% | 4,53 | 54,1% ± 1,8 | 43,5% ± 1,9 | 48,2% ± 1,7 | 0,99% ± 0,03 | 56,5% ± 1,9 | 21,1 ± 0,5 |
| 2,0% | 3,17 | 42,2% ± 1,2 | 54,8% ± 1,5 | 47,7% ± 1,3 | 2,02% ± 0,03 | 45,2% ± 1,5 | 34,0 ± 0,3 |

A linha de 1,0% é a regra de operação da série. As outras mostram o custo em Recall de exigir menos falso alerta (e o ganho de tolerar mais).

### Qualidade do ranking (não depende de limiar)

| PR-AUC | Recall a FPR de 1,0% (no próprio conjunto) |
|---|---:|
| 49,5% ± 2,2 | 43,6% ± 1,9 |

## Onde o detector acerta e erra

### Recall por tipo de fraude

| Tipo | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---:|---:|---:|
| `ACCOUNT_TAKEOVER` | 3.906 | 66,8% ± 1,3 | 51,6% ± 1,2 |
| `CARD_CLONING` | 2.222 | 13,5% ± 1,6 | 3,5% ± 1,2 |
| `IDENTITY_THEFT` | 2.471 | 48,1% ± 6,2 | 28,7% ± 7,5 |
| `MONEY_LAUNDERING` | 3.433 | 90,3% ± 1,3 | 84,5% ± 1,7 |
| `SOCIAL_ENGINEERING` | 2.339 | 38,9% ± 0,5 | 23,5% ± 0,6 |

### Recall por cenário e variante stealth

A variante *stealth* mascara um sinal (por exemplo, o account takeover que reusa a rede da vítima); o Recall dela mostra o quanto o detector depende desse sinal.

| Tipo | Variante | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---|---:|---:|---:|
| `ACCOUNT_TAKEOVER` | normal | 3.087 | 66,4% ± 2,0 | 51,6% ± 2,0 |
| `ACCOUNT_TAKEOVER` | stealth | 819 | 67,9% ± 3,2 | 51,6% ± 3,0 |
| `CARD_CLONING` | normal | 1.725 | 13,7% ± 1,4 | 3,7% ± 1,2 |
| `CARD_CLONING` | stealth | 497 | 12,6% ± 2,6 | 2,9% ± 2,1 |
| `IDENTITY_THEFT` | normal | 1.959 | 58,9% ± 5,8 | 35,0% ± 8,1 |
| `IDENTITY_THEFT` | stealth | 512 | 6,9% ± 3,6 | 4,1% ± 2,3 |
| `MONEY_LAUNDERING` | normal | 3.098 | 90,6% ± 1,0 | 84,9% ± 1,3 |
| `MONEY_LAUNDERING` | stealth | 335 | 87,1% ± 5,8 | 80,5% ± 7,1 |
| `SOCIAL_ENGINEERING` | normal | 1.891 | 48,1% ± 0,9 | 29,1% ± 0,9 |
| `SOCIAL_ENGINEERING` | stealth | 448 | 0,0% ± 0,0 | 0,0% ± 0,0 |

### Falsos positivos por tipo de hard negative

Eventos legítimos que imitam fraude (`new_device`: troca de celular; `new_ip`: rede nova; `travel`: viagem legítima; `big_purchase`: compra grande; `off_hours`: fora do horário habitual do cliente). O valor é a fração alertada, isto é, o FPR do grupo.

| Grupo | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---:|---:|---:|
| `new_device` | 12.046 | 2,33% ± 0,22 | 0,97% ± 0,08 |
| `new_ip` | 21.424 | 2,33% ± 0,12 | 1,00% ± 0,03 |
| `travel` | 6.168 | 2,43% ± 0,38 | 1,09% ± 0,37 |
| `big_purchase` | 5.268 | 51,31% ± 1,65 | 28,87% ± 1,65 |
| `off_hours` | 5.913 | 2,48% ± 0,41 | 1,02% ± 0,19 |
| legítimo sem nenhum | 485.062 | 1,71% ± 0,04 | 0,71% ± 0,03 |

### Detecção por episódio e time-to-detect

Um episódio conta como detectado se algum dos seus eventos alertou. O *time-to-detect* é o intervalo entre o primeiro evento fraudulento e o primeiro evento alertado, nos episódios detectados (média das medianas e dos p90 entre as seeds).

| Decisão | Episódios detectados | TTD mediano | TTD p90 |
|---|---:|---:|---:|
| Como implantado | 50,3% ± 1,3 | 0 s | 20 s |
| FPR ≤ 1,0% | 38,5% ± 2,1 | 0 s | 3,5 min |

## Cobertura por densidade de eventos

Cobertura é a fração de eventos com baseline (pelo menos 2 transações do mesmo cliente na janela anterior de 1 h). Sem baseline o Z-Score não é calculado e o evento nunca alerta.

| Regime | Eventos avaliados | Cobertura | Recall | Precision |
|---|---:|---:|---:|---:|
| Replay de stream (regime principal) | 109.695 | 100,0% ± 0,0 | 56,4% ± 1,5 | 40,7% ± 1,1 |
| Densidade do `make seed-data` (50 eventos/cliente em 180 dias) | 200.000 | 0,48% | 3,04% | 50,7% |

O V1 foi desenhado para o streaming: com poucos eventos por cliente ao longo de meses, quase nenhum evento tem 2 transações na hora anterior, e o detector fica cego. Por isso a avaliação usa o replay de stream, que é o regime em que ele roda; a linha de densidade de batch fica como evidência. A avaliação do V2 (#45) precisa considerar os dois regimes, já que o perfil de comportamento vem do batch.

## Hipótese: Precision perto de 50%

A issue #44 estimou, com um cálculo analítico, que a Precision do V1 ficaria perto de 50%: o `amount` do gerador é lognormal (μ = 5, σ = 1,2), então cerca de 1,6% dos legítimos passam de média + 3·desvio (até ~4,6% com baseline amostral), contra ~2,5% de fraude.

**Resultado:** **confirmada** (Precision de 40,7%, 9,3 p.p. abaixo de 50%; perto do limite da faixa de 40%–60%). Precision 40,7% ± 1,1, Recall 56,4% ± 1,5. O FPR medido (2,21%) ficou dentro da faixa analítica de 1,6%–4,6%: a estimativa do falso alerta se sustenta.

A regra de leitura foi fixada antes de medir: confirmada se a Precision média cair em 50% ± 10 pontos percentuais.

## Limitações e circularidade

- **Circularidade.** Os dados são sintéticos e vêm do mesmo projeto que o detector. As métricas medem a concordância entre o gerador e o detector, não o desempenho em produção. O ganho que importa é **relativo** (V1 × V2, sobre o mesmo dado); nenhum valor absoluto deste documento deve ser citado como desempenho real.
- **Mitigações embutidas.** Hard negatives no tráfego legítimo, ~20% dos episódios na variante stealth, limiar calibrado em uma seed diferente das de teste, e seeds de teste com clientes diferentes entre si e da validação.
- **Regime.** Replay com ritmo constante e qualquer cliente pode transacionar a qualquer hora (`diurnal=False`); o detector V1 não usa hora, então isso não o afeta, mas afeta sinais como `UNUSUAL_HOUR` no V2.
- **Viagens legítimas.** Cerca de 1,2% dos clientes já começa o replay viajando (estado estacionário). Quem viaja fica em trânsito, sem emitir eventos, durante o deslocamento, e transaciona na cidade destino por 6 a 48 h; a quantidade de eventos `travel` está na tabela de hard negatives.
- **Um detector.** Só o V1 é medido aqui. O V2 entra na #45 pela mesma interface (`Detector`), no mesmo protocolo.
- **Reprodutibilidade.** Mesma configuração e mesmas seeds geram o mesmo documento. Mudar clientes, taxa, duração, corte ou seeds muda os números.
- **Latência de detecção online** (p50/p95 de `latency_seconds`) não é medida aqui: vem do Postgres, da execução do streaming, na #47.
