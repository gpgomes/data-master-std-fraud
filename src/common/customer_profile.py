"""Perfil de comportamento determinístico por cliente e utilitários do gerador sintético.

O perfil descreve o que é "normal" para cada cliente (devices, redes, destinatários frequentes,
faixa de valor, horário ativo, cidade-base). Ele é derivado só de `seed` + `customer_id`
(`random.Random(f"{seed}:{customer_id}")`), então não depende da ordem de chamada nem do estado do
RNG de eventos: o batch (`make seed-data`) e o producer de streaming, ambos com a mesma seed de
clientes, enxergam exatamente o mesmo cliente (issue #43).

O perfil **não** vira coluna de `customers.csv`: é estado interno do gerador.
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

# ── Constantes geográficas e bancárias ─────────────────────────────────────────

BRAZILIAN_BANKS = [
    "Banco do Brasil",
    "Itaú",
    "Bradesco",
    "Caixa Econômica",
    "Santander",
    "Nubank",
    "Inter",
    "C6 Bank",
    "BTG Pactual",
    "Sicredi",
]

BRAZILIAN_CITIES: list[dict[str, Any]] = [
    {"city": "São Paulo", "state": "SP", "lat": -23.5505, "lon": -46.6333},
    {"city": "Rio de Janeiro", "state": "RJ", "lat": -22.9068, "lon": -43.1729},
    {"city": "Brasília", "state": "DF", "lat": -15.7801, "lon": -47.9292},
    {"city": "Salvador", "state": "BA", "lat": -12.9714, "lon": -38.5014},
    {"city": "Fortaleza", "state": "CE", "lat": -3.7319, "lon": -38.5267},
    {"city": "Curitiba", "state": "PR", "lat": -25.4290, "lon": -49.2671},
    {"city": "Manaus", "state": "AM", "lat": -3.1190, "lon": -60.0217},
    {"city": "Recife", "state": "PE", "lat": -8.0476, "lon": -34.8770},
    {"city": "Porto Alegre", "state": "RS", "lat": -30.0346, "lon": -51.2177},
    {"city": "Belo Horizonte", "state": "MG", "lat": -19.9167, "lon": -43.9345},
]

# ── Parâmetros de comportamento ────────────────────────────────────────────────

LOCAL_UTC_OFFSET_HOURS = -3  # Brasília (sem horário de verão)
PHYSICAL_CHANNELS = frozenset({"ATM", "AGENCIA"})  # cartão presente: sem device nem IP
NEW_ACCOUNT_DAYS = 30
# Jitter em torno do centro da cidade. ±0,1° ≈ 11 km: dois eventos consecutivos do mesmo cliente
# na mesma cidade ficam a < 25 km um do outro, então nunca parecem "viagem impossível".
CITY_JITTER_DEG = 0.1

# μ do ln(valor) por segmento (mediana = e^μ: ~R$100, ~R$270, ~R$665).
SEGMENT_AMOUNT_MU = {"VAREJO": 4.6, "ALTA_RENDA": 5.6, "PRIVATE": 6.5}
DEFAULT_AMOUNT_MU = 4.6

_PUBLIC_FIRST_OCTETS = tuple(o for o in range(11, 224) if o not in (127, 169, 172, 192))


# ── Utilitários de identificadores, tempo e geografia ──────────────────────────


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância em km entre dois pontos (fórmula de haversine)."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def city_distance_km(a: dict[str, Any], b: dict[str, Any]) -> float:
    return haversine_km(a["lat"], a["lon"], b["lat"], b["lon"])


def random_account(rng: random.Random) -> str:
    """Número de conta bancária fictício (mesmo formato usado desde a V1)."""
    agency = f"{rng.randint(1, 9999):04d}"
    account = f"{rng.randint(10000, 999999):06d}"
    digit = rng.randint(0, 9)
    return f"{agency}-{account}-{digit}"


def random_device(rng: random.Random) -> str:
    return f"dev-{rng.getrandbits(48):012x}"


def random_ip_prefix(rng: random.Random, avoid: Iterable[str] = ()) -> str:
    """Prefixo /24 ("a.b.c") de uma rede pública fictícia, diferente dos de `avoid`."""
    avoid_set = set(avoid)
    while True:
        prefix = f"{rng.choice(_PUBLIC_FIRST_OCTETS)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}"
        if prefix not in avoid_set:
            return prefix


def random_ip(rng: random.Random, prefix: str) -> str:
    return f"{prefix}.{rng.randint(1, 254)}"


def ip_prefix(ip: str) -> str:
    """Prefixo /24 de um IPv4 ("a.b.c.d" → "a.b.c")."""
    return ".".join(ip.split(".")[:3])


def jitter_location(rng: random.Random, city: dict[str, Any]) -> tuple[float, float]:
    return (
        round(city["lat"] + rng.uniform(-CITY_JITTER_DEG, CITY_JITTER_DEG), 6),
        round(city["lon"] + rng.uniform(-CITY_JITTER_DEG, CITY_JITTER_DEG), 6),
    )


def local_hour(epoch: float) -> int:
    """Hora local (0-23) de um instante em epoch (segundos, UTC)."""
    return int(((epoch + LOCAL_UTC_OFFSET_HOURS * 3600) % 86400) // 3600)


def set_local_hour(epoch: int, hour: int) -> int:
    """Move `epoch` para `hour` (hora local) no mesmo dia local, preservando min/seg."""
    offset = LOCAL_UTC_OFFSET_HOURS * 3600
    local = epoch + offset
    day_start = local - local % 86400
    return day_start + hour * 3600 + local % 3600 - offset


def city_by_name(name: str | None) -> dict[str, Any]:
    return next((c for c in BRAZILIAN_CITIES if c["city"] == name), BRAZILIAN_CITIES[0])


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


# ── Perfil ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CustomerProfile:
    """Comportamento habitual de um cliente (imutável e derivado de seed + customer_id)."""

    customer_id: str
    city: dict[str, Any]
    own_account: str
    own_bank: str
    devices: tuple[str, ...]
    ip_prefixes: tuple[str, ...]
    contacts: tuple[tuple[str, str], ...]  # (conta, banco) de destinatários frequentes
    amount_mu: float
    amount_sigma: float
    active_hours: tuple[int, ...]  # horas locais em que o cliente costuma transacionar
    opening_epoch: int | None  # abertura da conta (00:00 UTC), None se desconhecida

    @property
    def typical_amount(self) -> float:
        """Valor típico (mediana) das transações do cliente."""
        return math.exp(self.amount_mu)

    def account_age_days(self, epoch: float) -> float | None:
        if self.opening_epoch is None:
            return None
        return (epoch - self.opening_epoch) / 86400


def build_profile(seed: int, customer: dict[str, Any]) -> CustomerProfile:
    """Deriva o perfil de um cliente. Determinístico: só depende de `seed` e do cliente."""
    customer_id = customer["customer_id"]
    r = random.Random(f"{seed}:{customer_id}")

    devices = tuple(random_device(r) for _ in range(r.choice((1, 1, 2, 3))))

    prefixes: list[str] = []
    for _ in range(r.choice((1, 2, 2, 3))):
        prefixes.append(random_ip_prefix(r, avoid=prefixes))

    contacts = tuple(
        (random_account(r), r.choice(BRAZILIAN_BANKS)) for _ in range(r.randint(5, 15))
    )

    mu = SEGMENT_AMOUNT_MU.get(str(customer.get("segment")), DEFAULT_AMOUNT_MU) + r.gauss(0, 0.35)
    sigma = r.uniform(0.7, 1.0)

    if r.random() < 0.10:  # "coruja": ativo até de madrugada
        start, end = r.choice((9, 10, 11)), r.choice((25, 26))
    else:
        start, end = r.choice((6, 7, 8)), r.choice((21, 22, 23))
    active_hours = tuple(sorted({h % 24 for h in range(start, end)}))

    opening = _parse_date(customer.get("account_opening_date"))
    opening_epoch = (
        int(datetime(opening.year, opening.month, opening.day, tzinfo=UTC).timestamp())
        if opening
        else None
    )

    return CustomerProfile(
        customer_id=customer_id,
        city=city_by_name(customer.get("city")),
        own_account=random_account(r),
        own_bank=r.choice(BRAZILIAN_BANKS),
        devices=devices,
        ip_prefixes=tuple(prefixes),
        contacts=contacts,
        amount_mu=mu,
        amount_sigma=sigma,
        active_hours=active_hours,
        opening_epoch=opening_epoch,
    )


def cities_in_range(
    home: dict[str, Any], min_km: float, max_km: float | None = None
) -> Sequence[dict[str, Any]]:
    """Cidades (≠ `home`) cuja distância à cidade-base está em [min_km, max_km].

    Se nenhuma satisfaz, devolve a mais próxima que passa de `min_km` (ou, faltando essa, a mais
    distante), para o cenário sempre ter um destino.
    """
    others = [c for c in BRAZILIAN_CITIES if c["city"] != home["city"]]
    in_range = [
        c
        for c in others
        if city_distance_km(home, c) >= min_km
        and (max_km is None or city_distance_km(home, c) <= max_km)
    ]
    if in_range:
        return in_range
    beyond = [c for c in others if city_distance_km(home, c) >= min_km]
    if beyond:
        return [min(beyond, key=lambda c: city_distance_km(home, c))]
    return [max(others, key=lambda c: city_distance_km(home, c))]
