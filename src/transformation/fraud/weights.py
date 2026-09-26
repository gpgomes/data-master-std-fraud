"""Pesos dos sinais e limiar de alerta do detector multi-signal (issue #45).

Arquivo **gerado** por `make fraud-calibrate` (`python -m src.transformation.fraud.calibrate`), que
busca os pesos na seed de validação, nunca nas seeds de teste. Não edite à mão: mude o código dos
sinais ou o gerador, rode a calibração de novo e commite o resultado.

Os pesos entram no score como `1 − Π(1 − wᵢ·sᵢ)` (noisy-OR): cada sinal ativo é uma evidência
independente, o score fica em [0, 1] e nunca diminui quando aparece um sinal a mais.

Calibração: seed 1000, 109,613 eventos avaliados (2,751 fraudes). Objetivo
(recall máximo com FPR ≤ 1%): recall 0.9440, PR-AUC 0.9396
(pesos iniciais uniformes de 0.3: recall 0.7605, PR-AUC 0.7632).
"""

WEIGHTS_VERSION = "seed-1000"

SIGNAL_WEIGHTS: dict[str, float] = {
    "AMOUNT_ANOMALY": 0.75,
    "TX_VELOCITY": 0.00,
    "NEW_DEVICE": 0.10,
    "NEW_IP": 0.00,
    "GEO_VELOCITY": 0.75,
    "GEO_FAR_FROM_HOME": 0.10,
    "UNUSUAL_HOUR": 0.00,
    "NEW_DESTINATION": 0.90,
    "RECIPIENT_CONCENTRATION": 0.75,
    "ACCOUNT_AGE_LOW": 0.05,
}

# Alerta quando `fraud_score > ALERT_THRESHOLD`: o menor limiar com FPR ≤ 1% na validação.
ALERT_THRESHOLD = 0.9122
