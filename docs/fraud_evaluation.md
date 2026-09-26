# Avaliação dos detectores de fraude

> Documento gerado por `make fraud-eval` (`src/transformation/fraud/evaluate.py`). Não edite à mão: rode o comando de novo. Mesma configuração e mesmas seeds geram o mesmo documento.

## Resumo

- **V1 como implantado (|z| > 3):** Precision 40,7% ± 1,1, Recall 56,4% ± 1,5, FPR 2,21% ± 0,03, 36,3 ± 0,3 alertas por 1.000 transações.
- **Hipótese de Precision ≈ 50% (V1):** **confirmada** (Precision de 40,7%, 9,3 p.p. abaixo de 50%; perto do limite da faixa de 40%–60%).
- **V2 como implantado (score > 0,912):** Precision 72,7% ± 0,4, Recall 94,1% ± 0,3, FPR 0,95% ± 0,02, 33,9 ± 0,6 alertas por 1.000 transações.
- **multisignal-v2 × zscore-v1 (pareado por seed):** ΔF1 +34,8 p.p. ± 1,1 como implantado (vence em 5/5 seeds) e +33,9 p.p. ± 1,6 na regra de operação (vence em 5/5).
- **Tipo inferido (V2), entre as fraudes alertadas:** 76,3% corretos e 9,0% sem tipo (13.520 alertas verdadeiros).
- **Circularidade dimensionada:** sem o sinal `NEW_DESTINATION` (que o gerador injeta em toda a fraude), recalibrado na validação, o V2 fica com Recall 88,0% ± 0,6 e F1 77,7% ± 0,3 a FPR de 1,04% ± 0,04 (seção de sensibilidade).
- **Cobertura do baseline de 1 h do V1:** 100,0% ± 0,0 dos eventos no replay de stream, contra 0,48% na densidade do `make seed-data` (50 eventos por cliente em 180 dias): **o V1 só enxerga fraude na densidade do streaming**.

## Protocolo

| Parâmetro | Valor |
|---|---|
| Regime principal | replay de stream: o `TransactionStream` do producer em tempo simulado |
| Clientes por dataset | 1.000 |
| Taxa | 10 eventos/s (ritmo constante) |
| Duração simulada | 240 min; follow-ups de episódios entram inteiros |
| Corte (`--profile-until`) | 2026-03-02T13:00:00+00:00: antes dele os eventos só alimentam o estado do detector |
| Histórico de batch (perfil do V2) | 60.000 transações em 180 dias, dos mesmos clientes |
| Seed de validação | 1000 |
| Seeds de teste | 1, 2, 3, 4, 5 |
| Eventos avaliados por seed de teste (média) | 109.695 |
| Eventos fraudulentos por seed de teste (média) | 2.874 |
| Episódios de fraude por seed de teste (média) | 1.665 |
| Eventos avaliados na validação | 109.613 |

**Detectores:**

- **`zscore-v1`**: Z-Score do `amount` cru por cliente, janela deslizante de 60 min anteriores ao evento, mínimo de 2 transações na janela e alerta quando |z| > 3. O score de ranking é |z|; evento sem baseline tem score 0.
- **`multisignal-v2`**: 10 sinais (perfil do batch: valor, device, rede, destinatário, hora, local e idade da conta; janela curta: velocidade, viagem impossível e concentração de destinatários) combinados por noisy-OR (`score = 1 − Π(1 − wᵢ·sᵢ)`). Pesos e limiar calibrados só na seed de validação e versionados em `weights.py`; alerta quando score > 0,912. O detector nunca lê o rótulo: só enxerga id, cliente, instante, valor, device, IP, coordenadas e destinatário.

**Regra de operação:** *recall máximo com FPR ≤ alvo*. O limiar é o menor score cujo FPR na seed de validação não passa do alvo, e é aplicado sem ajuste nas seeds de teste (por isso o FPR medido nos testes varia em torno do alvo). Resultados são média ± desvio padrão amostral sobre as seeds de teste; nas taxas, o desvio é em pontos percentuais. As comparações entre detectores são **pareadas por seed** (mesmos eventos).

## Comparação entre detectores

| Decisão | Detector | Precision | Recall | F1 | FPR | FNR | Alertas/1.000 tx |
|---|---|---:|---:|---:|---:|---:|---:|
| Como implantado | `zscore-v1` | 40,7% ± 1,1 | 56,4% ± 1,5 | 47,3% ± 1,2 | 2,21% ± 0,03 | 43,6% ± 1,5 | 36,3 ± 0,3 |
| Como implantado | `multisignal-v2` | 72,7% ± 0,4 | 94,1% ± 0,3 | 82,0% ± 0,4 | 0,95% ± 0,02 | 5,9% ± 0,3 | 33,9 ± 0,6 |
| Regra de operação (FPR ≤ 1,0%) | `zscore-v1` | 54,1% ± 1,8 | 43,5% ± 1,9 | 48,2% ± 1,7 | 0,99% ± 0,03 | 56,5% ± 1,9 | 21,1 ± 0,5 |
| Regra de operação (FPR ≤ 1,0%) | `multisignal-v2` | 72,8% ± 0,4 | 94,1% ± 0,3 | 82,1% ± 0,4 | 0,95% ± 0,03 | 5,9% ± 0,3 | 33,9 ± 0,6 |

### Diferença pareada por seed

| Comparação | Decisão | ΔPrecision | ΔRecall | ΔF1 | Seeds em que vence (F1) |
|---|---|---:|---:|---:|---:|
| `multisignal-v2` − `zscore-v1` | Como implantado | +32,1 p.p. ± 1,1 | +37,7 p.p. ± 1,2 | +34,8 p.p. ± 1,1 | 5/5 |
| `multisignal-v2` − `zscore-v1` | FPR ≤ 1,0% | +18,7 p.p. ± 1,7 | +50,6 p.p. ± 1,6 | +33,9 p.p. ± 1,6 | 5/5 |

## Detector `zscore-v1`

### Resultados

**Como implantado**

| Precision | Recall | F1 | FPR | FNR | Alertas/1.000 tx |
|---:|---:|---:|---:|---:|---:|
| 40,7% ± 1,1 | 56,4% ± 1,5 | 47,3% ± 1,2 | 2,21% ± 0,03 | 43,6% ± 1,5 | 36,3 ± 0,3 |

Soma nas seeds de teste: 8.105 verdadeiros positivos, 11.819 falsos positivos, 6.266 falsos negativos.

**Na regra de operação e sensibilidade ao FPR alvo**

| FPR alvo | Limiar do score | Precision | Recall | F1 | FPR | FNR | Alertas/1.000 tx |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0,5% | 5,838 | 64,4% ± 1,9 | 35,7% ± 1,8 | 45,9% ± 1,9 | 0,53% ± 0,02 | 64,3% ± 1,8 | 14,5 ± 0,5 |
| 1,0% | 4,533 | 54,1% ± 1,8 | 43,5% ± 1,9 | 48,2% ± 1,7 | 0,99% ± 0,03 | 56,5% ± 1,9 | 21,1 ± 0,5 |
| 2,0% | 3,171 | 42,2% ± 1,2 | 54,8% ± 1,5 | 47,7% ± 1,3 | 2,02% ± 0,03 | 45,2% ± 1,5 | 34,0 ± 0,3 |

A linha de 1,0% é a regra de operação da série.

**Qualidade do ranking (não depende de limiar)**

| PR-AUC | Recall a FPR de 1,0% (no próprio conjunto) |
|---:|---:|
| 49,5% ± 2,2 | 43,6% ± 1,9 |

### Onde o `zscore-v1` acerta e erra

**Recall por tipo de fraude**

| Tipo | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---:|---:|---:|
| `ACCOUNT_TAKEOVER` | 3.906 | 66,8% ± 1,3 | 51,6% ± 1,2 |
| `CARD_CLONING` | 2.222 | 13,5% ± 1,6 | 3,5% ± 1,2 |
| `IDENTITY_THEFT` | 2.471 | 48,1% ± 6,2 | 28,7% ± 7,5 |
| `MONEY_LAUNDERING` | 3.433 | 90,3% ± 1,3 | 84,5% ± 1,7 |
| `SOCIAL_ENGINEERING` | 2.339 | 38,9% ± 0,5 | 23,5% ± 0,6 |

**Recall por cenário e variante stealth** (a variante *stealth* mascara um sinal; o Recall dela mostra o quanto o detector depende dele)

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

**Falsos positivos por tipo de hard negative** (eventos legítimos que imitam fraude; o valor é a fração alertada, isto é, o FPR do grupo)

| Grupo | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---:|---:|---:|
| `new_device` | 12.046 | 2,33% ± 0,22 | 0,97% ± 0,08 |
| `new_ip` | 21.424 | 2,33% ± 0,12 | 1,00% ± 0,03 |
| `travel` | 6.168 | 2,43% ± 0,38 | 1,09% ± 0,37 |
| `big_purchase` | 5.268 | 51,31% ± 1,65 | 28,87% ± 1,65 |
| `off_hours` | 5.913 | 2,48% ± 0,41 | 1,02% ± 0,19 |
| legítimo sem nenhum | 485.062 | 1,71% ± 0,04 | 0,71% ± 0,03 |

**Detecção por episódio e time-to-detect** (um episódio conta como detectado se algum dos seus eventos alertou; o *time-to-detect* é o intervalo entre o primeiro evento fraudulento e o primeiro alertado, nos episódios detectados)

| Decisão | Episódios detectados | TTD mediano | TTD p90 |
|---|---:|---:|---:|
| Como implantado | 50,3% ± 1,3 | 0 s | 20 s |
| FPR ≤ 1,0% | 38,5% ± 2,1 | 0 s | 3,5 min |

## Detector `multisignal-v2`

### Resultados

**Como implantado**

| Precision | Recall | F1 | FPR | FNR | Alertas/1.000 tx |
|---:|---:|---:|---:|---:|---:|
| 72,7% ± 0,4 | 94,1% ± 0,3 | 82,0% ± 0,4 | 0,95% ± 0,02 | 5,9% ± 0,3 | 33,9 ± 0,6 |

Soma nas seeds de teste: 13.520 verdadeiros positivos, 5.068 falsos positivos, 851 falsos negativos.

**Na regra de operação e sensibilidade ao FPR alvo**

| FPR alvo | Limiar do score | Precision | Recall | F1 | FPR | FNR | Alertas/1.000 tx |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0,5% | 0,922 | 83,5% ± 0,3 | 90,8% ± 0,6 | 87,0% ± 0,4 | 0,48% ± 0,02 | 9,2% ± 0,6 | 28,5 ± 0,5 |
| 1,0% | 0,912 | 72,8% ± 0,4 | 94,1% ± 0,3 | 82,1% ± 0,4 | 0,95% ± 0,03 | 5,9% ± 0,3 | 33,9 ± 0,6 |
| 2,0% | 0,907 | 55,7% ± 0,9 | 95,5% ± 0,3 | 70,3% ± 0,7 | 2,04% ± 0,08 | 4,5% ± 0,3 | 44,9 ± 1,0 |

A linha de 1,0% é a regra de operação da série.

**Qualidade do ranking (não depende de limiar)**

| PR-AUC | Recall a FPR de 1,0% (no próprio conjunto) |
|---:|---:|
| 94,1% ± 0,3 | 94,3% ± 0,4 |

### Onde o `multisignal-v2` acerta e erra

**Recall por tipo de fraude**

| Tipo | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---:|---:|---:|
| `ACCOUNT_TAKEOVER` | 3.906 | 100,0% ± 0,1 | 100,0% ± 0,1 |
| `CARD_CLONING` | 2.222 | 96,7% ± 1,0 | 96,7% ± 1,0 |
| `IDENTITY_THEFT` | 2.471 | 99,9% ± 0,1 | 99,9% ± 0,1 |
| `MONEY_LAUNDERING` | 3.433 | 99,1% ± 0,5 | 99,1% ± 0,5 |
| `SOCIAL_ENGINEERING` | 2.339 | 68,2% ± 1,4 | 68,2% ± 1,4 |

**Recall por cenário e variante stealth** (a variante *stealth* mascara um sinal; o Recall dela mostra o quanto o detector depende dele)

| Tipo | Variante | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---|---:|---:|---:|
| `ACCOUNT_TAKEOVER` | normal | 3.087 | 100,0% ± 0,1 | 100,0% ± 0,1 |
| `ACCOUNT_TAKEOVER` | stealth | 819 | 100,0% ± 0,0 | 100,0% ± 0,0 |
| `CARD_CLONING` | normal | 1.725 | 99,6% ± 0,5 | 99,6% ± 0,5 |
| `CARD_CLONING` | stealth | 497 | 86,5% ± 2,9 | 86,5% ± 2,9 |
| `IDENTITY_THEFT` | normal | 1.959 | 100,0% ± 0,0 | 100,0% ± 0,0 |
| `IDENTITY_THEFT` | stealth | 512 | 99,6% ± 0,5 | 99,6% ± 0,5 |
| `MONEY_LAUNDERING` | normal | 3.098 | 99,3% ± 0,4 | 99,3% ± 0,4 |
| `MONEY_LAUNDERING` | stealth | 335 | 97,7% ± 2,6 | 97,7% ± 2,6 |
| `SOCIAL_ENGINEERING` | normal | 1.891 | 84,4% ± 1,1 | 84,4% ± 1,1 |
| `SOCIAL_ENGINEERING` | stealth | 448 | 0,0% ± 0,0 | 0,0% ± 0,0 |

**Falsos positivos por tipo de hard negative** (eventos legítimos que imitam fraude; o valor é a fração alertada, isto é, o FPR do grupo)

| Grupo | Eventos (soma) | Como implantado | FPR ≤ 1,0% |
|---|---:|---:|---:|
| `new_device` | 12.046 | 2,88% ± 0,32 | 2,88% ± 0,32 |
| `new_ip` | 21.424 | 1,04% ± 0,14 | 1,04% ± 0,14 |
| `travel` | 6.168 | 1,90% ± 0,61 | 1,90% ± 0,61 |
| `big_purchase` | 5.268 | 21,80% ± 1,47 | 21,78% ± 1,47 |
| `off_hours` | 5.913 | 0,93% ± 0,28 | 0,93% ± 0,28 |
| legítimo sem nenhum | 485.062 | 0,69% ± 0,01 | 0,69% ± 0,01 |

**Detecção por episódio e time-to-detect** (um episódio conta como detectado se algum dos seus eventos alertou; o *time-to-detect* é o intervalo entre o primeiro evento fraudulento e o primeiro alertado, nos episódios detectados)

| Decisão | Episódios detectados | TTD mediano | TTD p90 |
|---|---:|---:|---:|
| Como implantado | 94,1% ± 0,4 | 0 s | 0 s |
| FPR ≤ 1,0% | 94,1% ± 0,4 | 0 s | 0 s |

### Tipo inferido: matriz de confusão

Entre as fraudes que o V2 alertou (como implantado, soma das seeds de teste), o tipo que as regras inferiram. Linhas: tipo verdadeiro; colunas: tipo previsto. `nenhuma` = nenhuma regra de tipo casou. O tipo nunca é copiado do rótulo.

| Tipo verdadeiro | Alertas | Account takeover | Clone de cartão | Roubo de identidade | Lavagem | Engenharia social | nenhuma |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ACCOUNT_TAKEOVER` | 3.905 | 3.504 (90%) | 190 (5%) | 5 (0%) | 30 (1%) | 26 (1%) | 150 (4%) |
| `CARD_CLONING` | 2.148 | 0 (0%) | 2.079 (97%) | 0 (0%) | 1 (0%) | 1 (0%) | 67 (3%) |
| `IDENTITY_THEFT` | 2.469 | 0 (0%) | 2 (0%) | 2.448 (99%) | 3 (0%) | 0 (0%) | 16 (1%) |
| `MONEY_LAUNDERING` | 3.403 | 0 (0%) | 27 (1%) | 144 (4%) | 1.812 (53%) | 1.330 (39%) | 90 (3%) |
| `SOCIAL_ENGINEERING` | 1.595 | 0 (0%) | 0 (0%) | 20 (1%) | 210 (13%) | 469 (29%) | 896 (56%) |

### Os sinais

Com que frequência cada sinal está ativo (≥ 0,5): no tráfego legítimo, na fraude e em cada tipo. Um sinal útil acende muito mais na fraude do que no legítimo; um que acende igual nos dois não discrimina, e a calibração o deixa com peso zero.

| Sinal | Peso | Legítimo | Fraude | Account takeover | Clone de cartão | Roubo de identidade | Lavagem | Engenharia social |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `AMOUNT_ANOMALY` | 0,75 | 0,45% | 55,7% | 53% | 0% | 90% | 92% | 24% |
| `TX_VELOCITY` | 0,00 | 9,21% | 12,4% | 15% | 9% | 16% | 9% | 14% |
| `NEW_DEVICE` | 0,10 | 2,26% | 40,0% | 90% | 0% | 91% | 0% | 0% |
| `NEW_IP` | 0,00 | 4,01% | 20,4% | 75% | 0% | 0% | 0% | 0% |
| `GEO_VELOCITY` | 0,75 | 0,00% | 29,0% | 52% | 94% | 1% | 1% | 0% |
| `GEO_FAR_FROM_HOME` | 0,10 | 1,00% | 41,2% | 100% | 91% | 0% | 0% | 0% |
| `UNUSUAL_HOUR` | 0,00 | 0,00% | 0,0% | 0% | 0% | 0% | 0% | 0% |
| `NEW_DESTINATION` | 0,90 | 25,99% | 100,0% | 100% | 100% | 100% | 100% | 100% |
| `RECIPIENT_CONCENTRATION` | 0,75 | 0,00% | 12,3% | 0% | 0% | 0% | 51% | 0% |
| `ACCOUNT_AGE_LOW` | 0,05 | 4,52% | 21,1% | 5% | 5% | 100% | 4% | 5% |

### Sensibilidade: o V2 sem `NEW_DESTINATION`

O gerador manda **toda** a fraude para uma conta-destino nova (`NEW_DESTINATION` ativo em 100% dela), e em produção parte da fraude usa destinatários conhecidos. Para dimensionar quanto o V2 depende desse sinal, ele é retirado, os pesos e o limiar são recalibrados **só na validação** (mesma regra: recall máximo com FPR ≤ 1,0%) e o resultado é medido nas seeds de teste.

| Variante | Precision | Recall | F1 | FPR |
|---|---:|---:|---:|---:|
| V2 completo | 72,8% ± 0,4 | 94,1% ± 0,3 | 82,1% ± 0,4 | 0,95% ± 0,03 |
| V2 sem `NEW_DESTINATION` (recalibrado) | 69,5% ± 0,7 | 88,0% ± 0,6 | 77,7% ± 0,3 | 1,04% ± 0,04 |

Recall por tipo:

| Tipo | V2 completo | Sem `NEW_DESTINATION` |
|---|---:|---:|
| `ACCOUNT_TAKEOVER` | 100,0% ± 0,1 | 99,9% ± 0,1 |
| `CARD_CLONING` | 96,7% ± 1,0 | 94,8% ± 1,2 |
| `IDENTITY_THEFT` | 99,9% ± 0,1 | 97,4% ± 0,5 |
| `MONEY_LAUNDERING` | 99,1% ± 0,5 | 97,5% ± 0,7 |
| `SOCIAL_ENGINEERING` | 68,2% ± 1,4 | 38,0% ± 1,1 |

## Cobertura por densidade de eventos

Cobertura é a fração de eventos com o contexto de que o detector precisa: para o V1, pelo menos 2 transações do mesmo cliente na janela anterior de 1 h; para o V2, pelo menos 3 transações legítimas do cliente no histórico de batch (o perfil). Sem contexto o detector não pontua e o evento nunca alerta pelo sinal que depende dele.

| Regime | Eventos avaliados | Cobertura | Recall | Precision | Contexto |
|---|---:|---:|---:|---:|---:|
| `zscore-v1`: replay de stream | 109.695 | 100,0% ± 0,0 | 56,4% ± 1,5 | 40,7% ± 1,1 | baseline de 1 h |
| `multisignal-v2`: replay de stream | 109.695 | 100,0% ± 0,0 | 94,1% ± 0,3 | 72,7% ± 0,4 | perfil do cliente |
| `zscore-v1`: densidade do `make seed-data` (50 eventos/cliente em 180 dias) | 200.000 | 0,48% | 3,04% | 50,7% | baseline de 1 h |

O V1 foi desenhado para o streaming: com poucos eventos por cliente ao longo de meses, quase nenhum evento tem 2 transações na hora anterior, e o detector fica cego. Por isso a avaliação usa o replay de stream, que é o regime em que ele roda. O V2 não sofre disso porque o perfil longo vem do batch e só os sinais de janela curta dependem da densidade do stream.

## Hipótese do V1: Precision perto de 50%

A issue #44 estimou, com um cálculo analítico, que a Precision do V1 ficaria perto de 50%: o `amount` do gerador é lognormal (μ = 5, σ = 1,2), então cerca de 1,6% dos legítimos passam de média + 3·desvio (até ~4,6% com baseline amostral), contra ~2,5% de fraude.

**Resultado:** **confirmada** (Precision de 40,7%, 9,3 p.p. abaixo de 50%; perto do limite da faixa de 40%–60%). Precision 40,7% ± 1,1, Recall 56,4% ± 1,5. O FPR medido (2,21%) ficou dentro da faixa analítica de 1,6%–4,6%: a estimativa do falso alerta se sustenta.

A regra de leitura foi fixada antes de medir: confirmada se a Precision média cair em 50% ± 10 pontos percentuais.

## Limitações e circularidade

- **Circularidade.** Os dados são sintéticos e vêm do mesmo projeto que os detectores. As métricas medem a concordância entre o gerador e o detector, não o desempenho em produção. O ganho que importa é **relativo** (V1 × V2, sobre o mesmo dado); nenhum valor absoluto deste documento deve ser citado como desempenho real.
- **O destinatário é a assinatura mais óbvia do gerador.** Ele envia toda a fraude para uma conta nova (`NEW_DESTINATION` ativo em 100% dela), o que faz esse sinal, combinado com qualquer outro, separar fraude de legítimo com facilidade. A seção de sensibilidade mostra o V2 sem ele, e ele segue muito acima do V1. Isso **não** dá uma estimativa do Recall real: os demais sinais também vêm de assinaturas que o gerador injeta (valores altos, device novo, viagem impossível, vários remetentes para a mesma conta), então o Recall em produção pode ser menor que os dois valores.
- **Mitigações embutidas.** Hard negatives no tráfego legítimo, ~20% dos episódios na variante stealth, pesos e limiar calibrados em uma seed diferente das de teste, e seeds de teste com clientes diferentes entre si e da validação.
- **Regime.** Replay com ritmo constante e qualquer cliente pode transacionar a qualquer hora (`diurnal=False`), e a janela padrão (4 h ao meio-dia) não passa pela madrugada: o sinal `UNUSUAL_HOUR` **não é exercitado** aqui (0% de ativação). Para exercitá-lo, use `--diurnal` com um `--start` noturno.
- **`TX_VELOCITY` não discrimina neste regime.** Cada cliente transaciona a cada ~100 s (36 eventos/h), ritmo irreal; com isso a maioria dos legítimos passa de 10 transações em 10 min, e a calibração deixa o peso do sinal em zero. Em dados esparsos, o sinal faria sentido.
- **Viagens legítimas.** Cerca de 1,2% dos clientes já começa o replay viajando (estado estacionário). Quem viaja fica em trânsito, sem emitir eventos, durante o deslocamento, e transaciona na cidade destino por 6 a 48 h; a quantidade de eventos `travel` está na tabela de hard negatives.
- **Perfil do V2.** O histórico é gerado pelo mesmo gerador, dos mesmos clientes, e o perfil usa só as linhas legítimas dele (rótulos históricos existem após a confirmação). O detector nunca lê o rótulo do evento que pontua.
- **Reprodutibilidade.** Mesma configuração e mesmas seeds geram o mesmo documento. Mudar clientes, taxa, duração, corte, seeds ou o gerador muda os números; mudar o gerador ou os sinais exige rodar `make fraud-calibrate` de novo.
- **Latência de detecção online** (p50/p95 de `latency_seconds`) não é medida aqui: vem do Postgres, da execução do streaming, na #47.
