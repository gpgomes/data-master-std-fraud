"""Perfil de comportamento por cliente, calculado do histórico (issue #45).

Função pura `DataFrame → DataFrame`: recebe o histórico rotulado (o batch, meses de dados) e os
clientes, e devolve **uma linha por cliente** com o que é "normal" para ele: μ/σ de `ln(amount)`,
devices, redes /24 e destinatários conhecidos, share noturno e centro geográfico. É o estado
"longo" da arquitetura Lambda: caro de calcular, estável, e lido por broadcast pelo detector (no
streaming, na #46). O estado "curto" (janelas de minutos) fica nos sinais.

O perfil usa só linhas `is_fraud = false` do histórico. Isso usa o rótulo, e é legítimo: rótulos
históricos existem depois da confirmação da fraude (chargeback). O que o detector nunca faz é ler o
rótulo do evento que está pontuando.

Cliente sem histórico suficiente (`n_history < MIN_HISTORY`) sai com `has_profile = false`: o valor
cai para o prior do segmento, e os sinais que dependem de "conhecido" ficam neutros (ausência de
evidência, não sinal).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

LOCAL_TZ = "America/Sao_Paulo"
NIGHT_END_HOUR = 6  # madrugada = horas locais 0–5
MIN_HISTORY = 3  # transações legítimas mínimas para o perfil valer
MIN_DESTINATION_SUPPORT = 2  # um destinatário só é "frequente" se apareceu pelo menos 2 vezes
MAX_KNOWN_DESTINATIONS = 50
SHRINKAGE_K = 5  # peso do prior do segmento, em transações
SIGMA_FLOOR = 0.3  # σ mínimo de ln(amount): evita z explosivo em cliente de valor quase fixo

HISTORY_COLUMNS = (
    "customer_id",
    "timestamp",
    "amount",
    "device_id",
    "ip_address",
    "latitude",
    "longitude",
    "destination_account",
    "is_fraud",
)
CUSTOMER_COLUMNS = ("customer_id", "segment", "account_opening_date")
PROFILE_COLUMNS = (
    "customer_id",
    "has_profile",
    "n_history",
    "mu_log",
    "sigma_log",
    "known_devices",
    "known_ip_prefixes",
    "known_destinations",
    "night_share",
    "home_lat",
    "home_lon",
    "account_opening_date",
)


def ip_prefix(ip: Column) -> Column:
    """Prefixo /24 ("a.b.c") de um IPv4; nulo se o IP é nulo."""
    return F.when(ip.isNotNull(), F.substring_index(ip, ".", 3))


def local_hour(timestamp: Column) -> Column:
    """Hora local (0–23) de um instante, no fuso de Brasília."""
    return F.hour(F.from_utc_timestamp(timestamp, LOCAL_TZ))


def build_profiles(history: DataFrame, customers: DataFrame) -> DataFrame:
    """Uma linha por cliente de `customers`, com o perfil aprendido de `history`.

    Args:
        history: colunas de `HISTORY_COLUMNS`. Só as linhas `is_fraud = false` entram.
        customers: colunas de `CUSTOMER_COLUMNS` (`account_opening_date` como data).
    """
    legit = history.select(*HISTORY_COLUMNS).filter(~F.coalesce(F.col("is_fraud"), F.lit(False)))
    events = (
        legit.withColumn("ln_amount", F.log("amount"))
        .withColumn("ip_prefix", ip_prefix(F.col("ip_address")))
        .withColumn("hour", local_hour(F.col("timestamp")))
    )

    per_customer = events.groupBy("customer_id").agg(
        F.count("*").alias("n_history"),
        F.avg("ln_amount").alias("mu_hat"),
        F.coalesce(F.stddev_samp("ln_amount"), F.lit(0.0)).alias("sd_hat"),
        F.collect_set("device_id").alias("known_devices"),
        F.collect_set("ip_prefix").alias("known_ip_prefixes"),
        F.avg((F.col("hour") < NIGHT_END_HOUR).cast("double")).alias("night_share"),
        F.percentile_approx("latitude", 0.5).alias("home_lat"),
        F.percentile_approx("longitude", 0.5).alias("home_lon"),
    )

    frequent = (
        events.filter(F.col("destination_account").isNotNull())
        .groupBy("customer_id", "destination_account")
        .count()
        .filter(F.col("count") >= MIN_DESTINATION_SUPPORT)
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.desc("count"), "destination_account")
            ),
        )
        .filter(F.col("rank") <= MAX_KNOWN_DESTINATIONS)
        .groupBy("customer_id")
        .agg(F.collect_set("destination_account").alias("known_destinations"))
    )

    # Prior por segmento (e global, para segmento desconhecido): μ e σ de ln(amount).
    seg = customers.select("customer_id", "segment")
    prior = (
        events.join(seg, "customer_id")
        .groupBy("segment")
        .agg(
            F.avg("ln_amount").alias("mu_seg"),
            F.coalesce(F.stddev_samp("ln_amount"), F.lit(0.0)).alias("sd_seg"),
        )
    )
    overall = events.agg(
        F.avg("ln_amount").alias("mu_all"),
        F.coalesce(F.stddev_samp("ln_amount"), F.lit(1.0)).alias("sd_all"),
    )

    empty_strings = F.array().cast("array<string>")
    n = F.coalesce(F.col("n_history"), F.lit(0)).cast("double")
    mu_prior = F.coalesce(F.col("mu_seg"), F.col("mu_all"))
    sd_prior = F.coalesce(F.col("sd_seg"), F.col("sd_all"))
    k = F.lit(float(SHRINKAGE_K))
    # Encolhimento: com poucas transações o cliente herda o prior do segmento.
    mu = (n * F.coalesce(F.col("mu_hat"), F.lit(0.0)) + k * mu_prior) / (n + k)
    var = (n * F.pow(F.coalesce(F.col("sd_hat"), F.lit(0.0)), 2) + k * F.pow(sd_prior, 2)) / (n + k)

    return (
        customers.select(*CUSTOMER_COLUMNS)
        .join(per_customer, "customer_id", "left")
        .join(frequent, "customer_id", "left")
        .join(F.broadcast(prior), "segment", "left")
        .crossJoin(F.broadcast(overall))
        .select(
            "customer_id",
            (n >= MIN_HISTORY).alias("has_profile"),
            n.cast("long").alias("n_history"),
            mu.alias("mu_log"),
            F.greatest(F.sqrt(var), F.lit(SIGMA_FLOOR)).alias("sigma_log"),
            F.coalesce(F.col("known_devices"), empty_strings).alias("known_devices"),
            F.coalesce(F.col("known_ip_prefixes"), empty_strings).alias("known_ip_prefixes"),
            F.coalesce(F.col("known_destinations"), empty_strings).alias("known_destinations"),
            F.coalesce(F.col("night_share"), F.lit(1.0)).alias("night_share"),
            F.col("home_lat"),
            F.col("home_lon"),
            F.col("account_opening_date"),
        )
    )
