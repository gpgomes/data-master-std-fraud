"""Sinais de fraude do Fraud Engine multi-signal (issue #45).

`compute_signals` é uma função pura `DataFrame → DataFrame`: recebe os eventos a pontuar e o perfil
dos clientes (`profile.py`) e devolve os mesmos eventos com uma coluna `sig_*` por sinal, em [0, 1]
(binária, exceto `AMOUNT_ANOMALY`, que é graduada). Nada de UDF Python: haversine e o resto saem
como `Column`, o que evita o problema de cloudpickle com Python 3.14.

Dois tipos de estado, como na arquitetura Lambda:
  - **perfil (longo)**, do batch: o que é conhecido/normal para o cliente (device, rede, destinatário,
    valor, hora, local, idade da conta);
  - **janela curta**, calculada aqui sobre os próprios eventos: velocidade, viagem impossível e
    concentração de destinatários.

O avaliador passa o dataset inteiro como `events` e nenhum `recent`; o streaming (#46) passa o
micro-batch como `events` e o estado persistido dos últimos minutos/horas como `recent`. As linhas de
`recent` só dão contexto às janelas e não saem no resultado.

**Sem rótulo:** a função seleciona só `EVENT_COLUMNS`. `is_fraud` e `fraud_type` não passam por aqui.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.transformation.fraud.profile import NIGHT_END_HOUR, ip_prefix, local_hour

# Colunas que o detector enxerga. É uma lista explícita de propósito: nenhum campo de rótulo
# consegue entrar por engano.
EVENT_COLUMNS = (
    "transaction_id",
    "customer_id",
    "timestamp",
    "amount",
    "device_id",
    "ip_address",
    "latitude",
    "longitude",
    "destination_account",
)

AMOUNT_ANOMALY = "AMOUNT_ANOMALY"
TX_VELOCITY = "TX_VELOCITY"
NEW_DEVICE = "NEW_DEVICE"
NEW_IP = "NEW_IP"
GEO_VELOCITY = "GEO_VELOCITY"
GEO_FAR_FROM_HOME = "GEO_FAR_FROM_HOME"
UNUSUAL_HOUR = "UNUSUAL_HOUR"
NEW_DESTINATION = "NEW_DESTINATION"
RECIPIENT_CONCENTRATION = "RECIPIENT_CONCENTRATION"
ACCOUNT_AGE_LOW = "ACCOUNT_AGE_LOW"

SIGNALS = (
    AMOUNT_ANOMALY,
    TX_VELOCITY,
    NEW_DEVICE,
    NEW_IP,
    GEO_VELOCITY,
    GEO_FAR_FROM_HOME,
    UNUSUAL_HOUR,
    NEW_DESTINATION,
    RECIPIENT_CONCENTRATION,
    ACCOUNT_AGE_LOW,
)

# ── Parâmetros dos sinais ──────────────────────────────────────────────────────
# AMOUNT_ANOMALY: 0 até z = Z_LOW, 1 a partir de z = Z_HIGH, linear no meio.
Z_LOW = 1.5
Z_HIGH = 4.0
VELOCITY_WINDOW_S = 600  # TX_VELOCITY: 10 min
VELOCITY_MIN_TX = 10
GEO_LOOKBACK = 5  # eventos anteriores comparados na viagem impossível
GEO_WINDOW_S = 6 * 3600
GEO_MIN_KM = 100.0
GEO_MAX_KMH = 900.0
GEO_FAR_KM = 500.0
NIGHT_SHARE_MAX = 0.05  # UNUSUAL_HOUR só vale para quem quase não transaciona de madrugada
CONCENTRATION_WINDOW_S = 3600
CONCENTRATION_MIN_SENDERS = 3
ACCOUNT_AGE_DAYS = 30

_CURRENT = "_current"
_EARTH_RADIUS_KM = 6371.0088


def signal_column(name: str) -> str:
    """Nome da coluna do sinal (`AMOUNT_ANOMALY` → `sig_amount_anomaly`)."""
    return f"sig_{name.lower()}"


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Distância em km entre dois pontos (graus), como `Column`. Igual à versão em Python."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    a = F.pow(F.sin((p2 - p1) / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(
        F.sin(F.radians(lon2 - lon1) / 2), 2
    )
    return 2 * _EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def _geo_velocity() -> Column:
    """Viagem impossível: nenhum dos últimos `GEO_LOOKBACK` eventos do cliente é um ponto de origem
    plausível.

    Comparar só com o evento imediatamente anterior geraria falso positivo no evento legítimo que
    vem logo depois de uma fraude em outra cidade. Exigir que **todos** os anteriores (dentro de 6 h)
    sejam inalcançáveis a 900 km/h evita isso: basta um vizinho plausível para o evento passar.
    """
    impossible: list[Column] = []
    available: list[Column] = []
    for i in range(1, GEO_LOOKBACK + 1):
        dt_s = F.col("_ts") - F.col(f"_ts{i}")
        km = haversine_km(
            F.col("latitude"), F.col("longitude"), F.col(f"_lat{i}"), F.col(f"_lon{i}")
        )
        has = (
            F.col(f"_ts{i}").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col(f"_lat{i}").isNotNull()
            & (dt_s <= GEO_WINDOW_S)
        )
        unreachable = (km > GEO_MIN_KM) & (
            F.when(dt_s > 0, km / (dt_s / 3600.0) > GEO_MAX_KMH).otherwise(F.lit(True))
        )
        available.append(has)
        impossible.append(has & unreachable)
    any_previous = reduce(lambda a, b: a | b, available)
    all_unreachable = reduce(
        lambda a, b: a & b, [~has | imp for has, imp in zip(available, impossible, strict=True)]
    )
    return (any_previous & all_unreachable).cast("double")


def compute_signals(
    events: DataFrame, profile: DataFrame, recent: DataFrame | None = None
) -> DataFrame:
    """Eventos com uma coluna `sig_*` por sinal (mais `has_profile`).

    Args:
        events: eventos a pontuar (`EVENT_COLUMNS`; outras colunas, como o rótulo, são ignoradas).
        profile: saída de `build_profiles` (uma linha por cliente).
        recent: eventos anteriores, só como contexto das janelas (streaming). Opcional.
    """
    current = events.select(*EVENT_COLUMNS).withColumn(_CURRENT, F.lit(True))
    if recent is None:
        base = current
    else:
        context = recent.select(*EVENT_COLUMNS).withColumn(_CURRENT, F.lit(False))
        base = current.unionByName(context)
    base = base.withColumn("_ts", F.col("timestamp").cast("long"))

    # Vizinhos anteriores do cliente, em ordem de tempo (desempate por id: determinístico).
    by_customer = Window.partitionBy("customer_id").orderBy("_ts", "transaction_id")
    for i in range(1, GEO_LOOKBACK + 1):
        base = (
            base.withColumn(f"_ts{i}", F.lag("_ts", i).over(by_customer))
            .withColumn(f"_lat{i}", F.lag("latitude", i).over(by_customer))
            .withColumn(f"_lon{i}", F.lag("longitude", i).over(by_customer))
        )

    velocity_window = (
        Window.partitionBy("customer_id").orderBy("_ts").rangeBetween(-VELOCITY_WINDOW_S, -1)
    )
    concentration_window = (
        Window.partitionBy("destination_account")
        .orderBy("_ts")
        .rangeBetween(-CONCENTRATION_WINDOW_S, 0)
    )
    base = base.withColumn(
        "_count_10m", F.count("transaction_id").over(velocity_window)
    ).withColumn("_senders", F.size(F.collect_set("customer_id").over(concentration_window)))

    df = base.join(F.broadcast(profile), "customer_id", "left")
    has_profile = F.coalesce(F.col("has_profile"), F.lit(False))

    z = (F.log("amount") - F.col("mu_log")) / F.col("sigma_log")
    amount_anomaly = F.when(
        F.col("mu_log").isNotNull(),
        F.least(F.greatest((z - Z_LOW) / (Z_HIGH - Z_LOW), F.lit(0.0)), F.lit(1.0)),
    ).otherwise(F.lit(0.0))

    def flag(condition: Column) -> Column:
        # Condição nula (dado ausente) é ausência de evidência: 0.
        return F.coalesce(condition, F.lit(False)).cast("double")

    age_days = F.datediff(F.to_date("timestamp"), F.col("account_opening_date"))
    sensors: dict[str, Column] = {
        AMOUNT_ANOMALY: amount_anomaly,
        TX_VELOCITY: flag(F.col("_count_10m") >= VELOCITY_MIN_TX),
        NEW_DEVICE: flag(
            has_profile
            & F.col("device_id").isNotNull()
            & ~F.array_contains(F.col("known_devices"), F.col("device_id"))
        ),
        NEW_IP: flag(
            has_profile
            & F.col("ip_address").isNotNull()
            & ~F.array_contains(F.col("known_ip_prefixes"), ip_prefix(F.col("ip_address")))
        ),
        GEO_VELOCITY: _geo_velocity(),
        GEO_FAR_FROM_HOME: flag(
            has_profile
            & (
                haversine_km(
                    F.col("latitude"), F.col("longitude"), F.col("home_lat"), F.col("home_lon")
                )
                > GEO_FAR_KM
            )
        ),
        UNUSUAL_HOUR: flag(
            has_profile
            & (local_hour(F.col("timestamp")) < NIGHT_END_HOUR)
            & (F.col("night_share") < NIGHT_SHARE_MAX)
        ),
        NEW_DESTINATION: flag(
            has_profile
            & F.col("destination_account").isNotNull()
            & ~F.array_contains(F.col("known_destinations"), F.col("destination_account"))
        ),
        RECIPIENT_CONCENTRATION: flag(
            F.col("destination_account").isNotNull()
            & (F.col("_senders") >= CONCENTRATION_MIN_SENDERS)
        ),
        ACCOUNT_AGE_LOW: flag((age_days >= 0) & (age_days < ACCOUNT_AGE_DAYS)),
    }
    for name in SIGNALS:
        df = df.withColumn(signal_column(name), sensors[name])

    keep: list[Column | str] = [
        *EVENT_COLUMNS,
        has_profile.alias("has_profile"),
        *[signal_column(s) for s in SIGNALS],
    ]
    return df.filter(F.col(_CURRENT)).select(*keep)
