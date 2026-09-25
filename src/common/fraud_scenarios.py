"""Cenários de fraude do gerador sintético: um episódio coerente por tipo de fraude (issue #43).

Cada builder devolve um `Episode`: 1-6 eventos com a assinatura comportamental do tipo
(device/IP novos, cidade distante, viagem impossível, mesma conta-destino recebendo de vários
clientes, ...), mais uma variante *stealth* (~20%) que mascara um dos sinais, para o recall de
qualquer detector não chegar a 1,0.

Os builders só decidem a **assinatura** (valor, canal, device, IP, coordenadas, destinatário,
atraso relativo). O `DataGenerator` completa o resto do evento (ids, timestamp, moeda, conta de
origem) e aplica o mesmo ruído de campos ausentes que aplica ao tráfego legítimo.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass, field

from src.common.customer_profile import (
    BRAZILIAN_BANKS,
    CustomerProfile,
    cities_in_range,
    city_distance_km,
    jitter_location,
    random_account,
    random_device,
    random_ip,
    random_ip_prefix,
)
from src.common.schemas import Channel, FraudType, MerchantCategory, TransactionType

# Peso de cada tipo na escolha do episódio (por episódio, não por evento).
SCENARIO_WEIGHTS: dict[str, float] = {
    FraudType.ACCOUNT_TAKEOVER.value: 0.25,
    FraudType.CARD_CLONING.value: 0.25,
    FraudType.IDENTITY_THEFT.value: 0.20,
    FraudType.MONEY_LAUNDERING.value: 0.10,
    FraudType.SOCIAL_ENGINEERING.value: 0.20,
}
STEALTH_PROBABILITY = 0.20

# Média de eventos fraudulentos por episódio (usada para calibrar a taxa de fraude no streaming).
MEAN_FRAUD_EVENTS: dict[str, float] = {
    FraudType.ACCOUNT_TAKEOVER.value: 2.0,  # 1-3
    FraudType.CARD_CLONING.value: 1.0,
    FraudType.IDENTITY_THEFT.value: 1.5,  # 1-2
    FraudType.MONEY_LAUNDERING.value: 0.8 * 4.5 + 0.2 * 2.0,  # 3-6 remetentes; stealth: 2
    FraudType.SOCIAL_ENGINEERING.value: 1.5,  # 1-2
}
# Eventos legítimos que o episódio injeta (o cartão do cliente, usado em casa antes do clone).
PRECURSOR_EVENTS: dict[str, float] = {FraudType.CARD_CLONING.value: 1.0}

_PIX = TransactionType.PIX.value
_TED = TransactionType.TED.value
_TRANSFER = MerchantCategory.TRANSFERENCIA.value
_APP = Channel.APP_MOBILE.value
_WEB = Channel.INTERNET_BANKING.value


@dataclass
class EpisodeEvent:
    """Assinatura de um evento do episódio. `delay_s` é relativo ao início do episódio."""

    delay_s: float
    customer_id: str
    amount: float
    transaction_type: str
    merchant_category: str
    channel: str
    destination_account: str
    destination_bank: str
    device_id: str | None
    ip_address: str | None
    latitude: float
    longitude: float
    is_fraud: bool = True
    fraud_type: str | None = None


@dataclass
class Episode:
    scenario: str
    stealth: bool
    events: list[EpisodeEvent] = field(default_factory=list)
    # Hora local em que o episódio começa (só o gerador batch respeita; o streaming acontece "agora").
    start_hour_local: int | None = None

    @property
    def span_s(self) -> float:
        return max((e.delay_s for e in self.events), default=0.0)


def _mule(rng: random.Random) -> tuple[str, str]:
    """Conta-destino nova, controlada pelo fraudador."""
    return random_account(rng), rng.choice(BRAZILIAN_BANKS)


def _account_takeover(rng: random.Random, victim: CustomerProfile, stealth: bool) -> Episode:
    device = random_device(rng)
    prefix = (
        rng.choice(victim.ip_prefixes)  # stealth: usa a rede da vítima
        if stealth
        else random_ip_prefix(rng, avoid=victim.ip_prefixes)
    )
    ip = random_ip(rng, prefix)
    city = rng.choice(cities_in_range(victim.city, 800))
    mule_account, mule_bank = _mule(rng)
    tx_type = rng.choice((_PIX, _PIX, _TED))
    channel = rng.choice((_APP, _APP, _WEB))

    amount = max(victim.typical_amount * rng.uniform(3, 8), rng.uniform(500, 1500))
    delay = 0.0
    events = []
    for _ in range(rng.randint(1, 3)):
        lat, lon = jitter_location(rng, city)
        events.append(
            EpisodeEvent(
                delay_s=delay,
                customer_id=victim.customer_id,
                amount=round(min(amount, 100_000.0), 2),
                transaction_type=tx_type,
                merchant_category=_TRANSFER,
                channel=channel,
                destination_account=mule_account,
                destination_bank=mule_bank,
                device_id=device,
                ip_address=ip,
                latitude=lat,
                longitude=lon,
                fraud_type=FraudType.ACCOUNT_TAKEOVER.value,
            )
        )
        amount *= rng.uniform(1.3, 2.0)  # valor crescente
        delay += rng.uniform(60, 300)
    return Episode(
        FraudType.ACCOUNT_TAKEOVER.value, stealth, events, start_hour_local=rng.randint(0, 4)
    )


def _card_cloning(rng: random.Random, victim: CustomerProfile, stealth: bool) -> Episode:
    if stealth:
        # Cidade próxima e intervalo derivado da distância: velocidade implícita plausível
        # (250–600 km/h) mesmo quando a cidade-base não tem vizinha a 150–450 km.
        clone_city = rng.choice(cities_in_range(victim.city, 150, 450))
        km = city_distance_km(victim.city, clone_city)
        gap = max(3600.0, km / rng.uniform(250, 600) * 3600)
    else:
        clone_city = rng.choice(cities_in_range(victim.city, 700))
        gap = rng.uniform(300, 1200)

    home_lat, home_lon = jitter_location(rng, victim.city)
    dest_account, dest_bank = _mule(rng)
    # Cartão presente: sem device nem IP (o gerador força isso em canais físicos).
    legit_use = EpisodeEvent(
        delay_s=0.0,
        customer_id=victim.customer_id,
        amount=round(max(20.0, victim.typical_amount * rng.uniform(0.5, 1.5)), 2),
        transaction_type=TransactionType.CARTAO_DEBITO.value,
        merchant_category=MerchantCategory.SAQUE.value,
        channel=Channel.ATM.value,
        destination_account=dest_account,
        destination_bank=dest_bank,
        device_id=None,
        ip_address=None,
        latitude=home_lat,
        longitude=home_lon,
        is_fraud=False,
    )

    clone_lat, clone_lon = jitter_location(rng, clone_city)
    dest_account, dest_bank = _mule(rng)
    clone = EpisodeEvent(
        delay_s=gap,
        customer_id=victim.customer_id,
        amount=round(min(max(victim.typical_amount * rng.uniform(2, 6), 200.0), 4000.0), 2),
        transaction_type=TransactionType.CARTAO_DEBITO.value,
        merchant_category=MerchantCategory.SAQUE.value,
        channel=Channel.ATM.value,
        destination_account=dest_account,
        destination_bank=dest_bank,
        device_id=None,
        ip_address=None,
        latitude=clone_lat,
        longitude=clone_lon,
        fraud_type=FraudType.CARD_CLONING.value,
    )
    return Episode(FraudType.CARD_CLONING.value, stealth, [legit_use, clone])


def _identity_theft(rng: random.Random, victim: CustomerProfile, stealth: bool) -> Episode:
    # Conta recém-aberta com device novo; o cadastro é feito na rede já conhecida da conta.
    device = random_device(rng)
    ip = random_ip(rng, rng.choice(victim.ip_prefixes))
    mule_account, mule_bank = _mule(rng)
    tx_type = rng.choice((_PIX, _TED))
    amount = rng.uniform(800, 2500) if stealth else rng.uniform(4000, 25000)

    events = []
    delay = 0.0
    for _ in range(rng.randint(1, 2)):
        lat, lon = jitter_location(rng, victim.city)
        events.append(
            EpisodeEvent(
                delay_s=delay,
                customer_id=victim.customer_id,
                amount=round(amount, 2),
                transaction_type=tx_type,
                merchant_category=_TRANSFER,
                channel=_APP,
                destination_account=mule_account,
                destination_bank=mule_bank,
                device_id=device,
                ip_address=ip,
                latitude=lat,
                longitude=lon,
                fraud_type=FraudType.IDENTITY_THEFT.value,
            )
        )
        amount *= rng.uniform(0.8, 1.2)
        delay += rng.uniform(120, 900)
    return Episode(FraudType.IDENTITY_THEFT.value, stealth, events)


def _money_laundering(
    rng: random.Random,
    victim: CustomerProfile,
    stealth: bool,
    accomplices: Sequence[CustomerProfile],
) -> Episode:
    """Vários clientes enviam valores fracionados para a mesma conta-destino (mula)."""
    senders = [victim, *accomplices][: 2 if stealth else rng.randint(3, 6)]
    mule_account, mule_bank = _mule(rng)
    window = 5400.0 if stealth else 1800.0
    delays = sorted([0.0] + [rng.uniform(0, window) for _ in senders[1:]])
    tx_type = rng.choice((_PIX, _TED))

    events = []
    for sender, delay in zip(senders, delays, strict=True):
        lat, lon = jitter_location(rng, sender.city)
        events.append(
            EpisodeEvent(
                delay_s=delay,
                customer_id=sender.customer_id,
                amount=float(rng.randrange(20, 96) * 100),  # R$ 2.000-9.500, abaixo de 10 mil
                transaction_type=tx_type,
                merchant_category=_TRANSFER,
                channel=_APP,
                destination_account=mule_account,
                destination_bank=mule_bank,
                device_id=rng.choice(sender.devices),
                ip_address=random_ip(rng, rng.choice(sender.ip_prefixes)),
                latitude=lat,
                longitude=lon,
                fraud_type=FraudType.MONEY_LAUNDERING.value,
            )
        )
    return Episode(FraudType.MONEY_LAUNDERING.value, stealth, events)


def _social_engineering(rng: random.Random, victim: CustomerProfile, stealth: bool) -> Episode:
    """A vítima, autenticada no próprio device e rede, é induzida a pagar um destinatário novo."""
    device = rng.choice(victim.devices)
    ip = random_ip(rng, rng.choice(victim.ip_prefixes))
    mule_account, mule_bank = _mule(rng)
    amount = victim.typical_amount * (rng.uniform(2, 3) if stealth else rng.uniform(4, 10))
    if not stealth:
        amount = max(amount, 1000.0)

    events = []
    delay = 0.0
    for _ in range(rng.randint(1, 2)):
        lat, lon = jitter_location(rng, victim.city)
        events.append(
            EpisodeEvent(
                delay_s=delay,
                customer_id=victim.customer_id,
                amount=round(amount, 2),
                transaction_type=_PIX,
                merchant_category=_TRANSFER,
                channel=_APP,
                destination_account=mule_account,
                destination_bank=mule_bank,
                device_id=device,
                ip_address=ip,
                latitude=lat,
                longitude=lon,
                fraud_type=FraudType.SOCIAL_ENGINEERING.value,
            )
        )
        amount *= rng.uniform(0.5, 1.0)
        delay += rng.uniform(120, 600)
    return Episode(FraudType.SOCIAL_ENGINEERING.value, stealth, events)


def build_episode(
    scenario: str,
    rng: random.Random,
    victim: CustomerProfile,
    *,
    stealth: bool,
    accomplices: Sequence[CustomerProfile] = (),
) -> Episode:
    """Monta o episódio de `scenario` (um valor de `FraudType`) contra `victim`.

    `accomplices` só é usado na lavagem de dinheiro (remetentes adicionais da mesma mula).
    """
    if scenario == FraudType.ACCOUNT_TAKEOVER.value:
        return _account_takeover(rng, victim, stealth)
    if scenario == FraudType.CARD_CLONING.value:
        return _card_cloning(rng, victim, stealth)
    if scenario == FraudType.IDENTITY_THEFT.value:
        return _identity_theft(rng, victim, stealth)
    if scenario == FraudType.MONEY_LAUNDERING.value:
        return _money_laundering(rng, victim, stealth, accomplices)
    if scenario == FraudType.SOCIAL_ENGINEERING.value:
        return _social_engineering(rng, victim, stealth)
    raise ValueError(f"cenário de fraude desconhecido: {scenario!r}")
