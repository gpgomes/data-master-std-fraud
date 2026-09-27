# Perguntas da banca sobre o detector de fraude

Respostas curtas, cada uma com o número e onde conferi-lo. Os números vêm de dois lugares que se reproduzem com um comando: o **offline** (`make fraud-eval` → [`fraud_evaluation.md`](fraud_evaluation.md), replay de stream, 5 seeds de teste, média ± desvio) e o **online** (`make fraud-online-eval`, SQL sobre `stream_scored_transactions`, uma execução de 21 min: 18.170 eventos, 323 fraudes). O detalhamento está em [`architecture.md`](architecture.md#detecção-de-fraude-fraud-engine).

A ressalva que vale para todas as respostas: **o dado é sintético**, e o gerador injeta as assinaturas que o detector procura. O número que importa é o relativo (V2 contra V1) e a sensibilidade, não o valor absoluto. Ver a pergunta 10.

## O que o detector faz

**1. Onde o detector encontra clonagem de cartão (`CARD_CLONING`)?**
No sinal `GEO_VELOCITY` (`src/transformation/fraud/signals.py`): dispara quando **nenhum** dos últimos 5 eventos do cliente, em 6 h, é uma origem plausível (≤ 900 km/h ou ≤ 100 km). Comparar só com o evento anterior geraria falso positivo no evento legítimo que vem logo depois de uma fraude em outra cidade. Recall: 96,7% offline (86,5% na variante *stealth*, cidade próxima a 250–600 km/h) e 27 de 27 online. No offline o sinal fica ativo em 94% dos eventos de clonagem e em 0,00% do tráfego legítimo (tabela "Os sinais" do relatório).

**2. Como detectar tomada de conta (`ACCOUNT_TAKEOVER`)?**
`NEW_DEVICE` ∧ (`NEW_IP` ∨ `GEO_FAR_FROM_HOME`), regra de tipo em `fraud_type.py`, sobre o perfil do cliente (devices, redes /24 e centro geográfico conhecidos). Recall 100% offline, inclusive na variante *stealth*, e 100% online. É o tipo mais fácil: a fraude troca o device e a rede ao mesmo tempo.

**3. Como o alerta explica o porquê?**
Cada alerta traz `fraud_signals`, a lista dos sinais ativos (≥ 0,5), o `alert_reason` (`Sinais: A, B | score 0.975 | multisignal-v2`) e o `fraud_type` **inferido** pelos sinais, nulo se nenhuma regra casa. Está no tópico `fraud-alerts`, na tabela `fraud_alerts` e em `GET /alerts`. O `fraud_type` nunca é copiado do rótulo (o V1 copiava, com um tipo fixo de fallback: por isso não respondia à pergunta 1).

**4. Por que não um modelo de ML?**
Por escolha: o score é um noisy-OR de sinais ponderados (`1 − Π(1 − wᵢ·sᵢ)`), fica em [0, 1], é monotônico e o motivo do alerta sai de graça. Um modelo treinado pediria infra de treino, versionamento e uma discussão sobre rótulos que, em produção, chegam tarde (chargeback). É a evolução natural: o `Detector` já é um contrato (`detector_api.py`), e uma regressão logística sobre os mesmos sinais seria um V3 medido no mesmo harness.

## Como foi medido

**5. Como o limiar foi definido?**
Ponto de operação declarado: *recall máximo com FPR ≤ 1%*. Pesos e limiar são calibrados **só na seed de validação** (1000) por `make fraud-calibrate` e congelados em `weights.py` antes de olhar as 5 seeds de teste, que têm clientes diferentes. Sensibilidade no relatório:

| FPR alvo | Precision | Recall |
|---|---:|---:|
| 0,5% | 83,5% | 90,8% |
| 1,0% (a da série) | 72,8% | 94,1% |
| 2,0% | 55,7% | 95,5% |

**6. Qual a Precision, o Recall e o FPR?**

| | Detector | Precision | Recall | F1 | FPR |
|---|---|---:|---:|---:|---:|
| Offline (5 seeds) | `zscore-v1` | 40,7% ± 1,1 | 56,4% ± 1,5 | 47,3% ± 1,2 | 2,21% |
| Offline (5 seeds) | `multisignal-v2` | 72,7% ± 0,4 | 94,1% ± 0,3 | 82,0% ± 0,4 | 0,95% |
| Online (1 execução) | `zscore-v1` | 14,2% | 57,6% | 22,8% | 6,30% |
| Online (1 execução) | `multisignal-v2` | 60,0% | 91,3% | 72,4% | 1,10% |

A Precision online é menor porque a prevalência é menor (1,8% contra ~2,6%): com o mesmo Recall e FPR, é o que a conta dá. O intervalo do Recall online é de ±3 p.p. (323 fraudes), então 91,3% e 94,1% não divergem.

**7. O V2 é mesmo melhor que o Z-Score?**
Sim, nos dois regimes e nos mesmos eventos: offline, ΔF1 de +34,8 p.p. ± 1,1 (o V2 vence em 5 de 5 seeds); online (shadow scoring, os dois detectores sobre cada evento), F1 de 72,4% contra 22,8%. O V2 pegou 115 fraudes que o Z-Score não via; o Z-Score pegou 6 que o V2 perdeu.

**8. O detector vê o rótulo (`is_fraud`)?**
Não, e isso é verificado de três formas: (a) `_score_batch` tira `is_fraud`/`fraud_type` do DataFrame **antes** de qualquer detector e só os junta de volta por `transaction_id` no fim; (b) o núcleo seleciona uma lista explícita de colunas e uma guarda estática garante que nenhum módulo dele cita o rótulo; (c) um teste roda o mesmo micro-batch com o rótulo verdadeiro, invertido e nulo e exige score, sinais, tipo e alerta idênticos. O perfil de comportamento usa só as linhas `is_fraud = false` do **histórico** do batch (rótulos históricos existem depois da confirmação da fraude); o evento que está sendo pontuado nunca é lido pelo rótulo.

**9. Como se sabe que batch e stream calculam a mesma coisa?**
Um teste de paridade: o dataset inteiro de uma vez dá o mesmo score, sinais, tipo e Z-Score que o mesmo dataset em micro-batches, com o estado curto persistido entre eles, incluindo viagem impossível e concentração de destinatários atravessando micro-batches. Se o horizonte do estado for encolhido, o teste falha (conferido por mutação).

## Onde ele erra e o que muda com dado real

**10. Por que confiar em 94% de Recall?**
Não confie no valor absoluto. O gerador manda **toda** a fraude para uma conta-destino nova, então `NEW_DESTINATION` fica ativo em 100% das fraudes. O relatório dimensiona: sem esse sinal, recalibrado só na validação, o V2 fica com Recall de 88,0% e F1 de 77,7%, e engenharia social cai de 68% para 38%. Os outros sinais também vêm de assinaturas injetadas (valor alto, device novo, viagem impossível, vários remetentes para a mesma conta), então com dado real o Recall seria menor que os dois. O que se defende é o ganho relativo, o protocolo (seeds separadas, limiar calibrado fora do teste, hard negatives e variantes *stealth* no gerador) e o fato de os erros estarem documentados.

**11. Onde o detector erra?**
- **Engenharia social *stealth*: 0% de Recall** (vítima no próprio device e rede, valor normal; só o destinatário novo a separa de um pagamento legítimo, e ele sozinho não passa do limiar). No total, 68% de Recall no tipo.
- **Compra grande legítima** é alertada em 21,8% dos casos.
- **Lavagem** é classificada como engenharia social em 39% dos alertas offline, porque os primeiros eventos de cada conta-mula têm device conhecido, destinatário novo e valor alto.
- **Todos os falsos positivos online (197 de 197) tinham `NEW_DESTINATION`**, que está ativo em 26% do tráfego legítimo e é o sinal de maior peso.
- Entre as fraudes alertadas, 9% (offline) a 14% (online) ficam **sem tipo** (nenhuma regra casou), e o tipo inferido está certo em 76% (offline) e 69% (online) delas. Somando todos os alertas online, 34% (169 de 492) ficam sem tipo, e 128 desses são falsos positivos: um alerta sem tipo é um sinal de que a evidência é fraca.

**12. Por que o Z-Score erra mais online (6,3% de FPR) que offline (2,2%)?**
Não sei. A hipótese é a janela de 1 h com poucas amostras por cliente numa execução curta (~18 eventos por cliente na execução inteira), mas o FPR por terço de uma execução anterior não a confirmou de forma limpa. Está registrado como hipótese. Não afeta o V2, que depende do perfil longo do batch, e não muda a comparação (os dois detectores viram os mesmos eventos).

**13. O que muda com dado de produção?**
Os rótulos chegam tarde (chargeback), então a calibração passa a ser periódica, contra rótulos confirmados; o perfil precisa ser recalculado no ritmo em que o comportamento muda (hoje, a cada execução do batch, e o stream o lê só na partida: reiniciar o job depois de recalcular); clientes novos entram com `has_profile = false`, e os sinais de "conhecido" ficam neutros em vez de disparar; e é preciso monitorar deriva (o `detector_version` e o `fraud_signals` já vão em cada evento). O tópico `ground-truth` separado, com o rótulo chegando depois do evento, é o desenho mais realista e ficou como evolução.

## Por que esta arquitetura

**14. Por que a Lambda se justifica aqui?**
Porque o problema tem duas escalas de tempo. O que é normal para um cliente (valor, devices, redes, destinatários, horário, cidade) só se aprende com meses de histórico: é **batch** (`gold/customer_behavior_profile/`, ~30 s para 500 mil transações). O que precisa de segundos (rajada, viagem impossível, vários remetentes para a mesma conta) só existe na janela curta: é **stream**, com 6 h de estado. Kappa obrigaria a reprocessar o histórico inteiro no stream ou a reter meses no Kafka.

**15. Qual o custo da lógica duplicada?**
Um núcleo puro (`src/transformation/fraud/`, `DataFrame → DataFrame`, sem UDF Python) usado pelo avaliador offline e pelo stream, mais o teste de paridade da pergunta 9. O batch só calcula o perfil; ele não pontua eventos.

**16. Qual a latência, e o que ela custou?**
Evento → processamento, online: p50 5,97 s, p95 10,46 s, p99 10,92 s, contra p50 5,46 s e p95 9,90 s do V1: ~0,5 s de custo, e o teto é o trigger de 10 s. Medida também com o estado curto em regime (346 mil linhas, ~6 h de eventos): p95 10,38 s, sem acumular atraso entre micro-batches. O plano B (guardar só o último evento por cliente) não foi necessário.

**17. E se o job cair no meio de um micro-batch?**
A idempotência da issue #36 continua valendo: cada etapa (Parquet, `enriched-transactions`, `fraud-alerts`, estado curto) grava um marcador, e o replay pula o que já foi feito. Testado matando o driver (`kill -9`) com o micro-batch parcialmente gravado e reiniciando com o mesmo checkpoint: 0 duplicatas em 15.433 linhas, e o estado não contou nenhum evento em dobro. Os tópicos Kafka são at-least-once (o sink do Spark não é transacional); consumidores deduplicam por `transaction_id`.

**18. Isso roda igual no container do Spark e no CI?**
Não roda igual por padrão, e já quebrou uma vez: o Spark do Docker usa Python 3.8 e não tem numpy/pandas, e o CI usa 3.11. O código da #45 tinha `import numpy` no topo e `zip(strict=True)` e só quebrou ao chegar no stream (#46). Hoje `tests/unit/test_spark_container_compat.py` lê os módulos que rodam no container com `ast.parse(feature_version=(3, 8))` e importa o caminho do stream com numpy/pandas bloqueados.
