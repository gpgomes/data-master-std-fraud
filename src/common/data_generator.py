"""Gerador de dados sintéticos para transações financeiras e dados de mercado.

Transações (issue #43): cada cliente tem um perfil de comportamento determinístico
(`customer_profile.py`) e a fraude é gerada como **episódio** coerente com o tipo
(`fraud_scenarios.py`), em vez de linhas independentes. O tráfego legítimo carrega ruído
deliberado (troca de celular, rede nova, viagem, compra grande) para que nenhum sinal isolado
separe perfeitamente fraude de legítimo — os *hard negatives*.

Dois modos:
  - batch (`generate_transactions`): dataset com timestamps espalhados em [start, end] e um sidecar
    de ground truth em `last_ground_truth` (episódio, cenário, stealth, hard negatives);
  - streaming (`TransactionStream`): eventos "agora", com os follow-ups de cada episódio devolvidos
    com um atraso, para o producer emitir na hora certa.
"""

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import numpy as np
from faker import Faker

from src.common.customer_profile import (
    BRAZILIAN_BANKS,
    BRAZILIAN_CITIES,
    NEW_ACCOUNT_DAYS,
    PHYSICAL_CHANNELS,
    CustomerProfile,
    build_profile,
    city_distance_km,
    jitter_location,
    local_hour,
    random_account,
    random_device,
    random_ip,
    random_ip_prefix,
    set_local_hour,
)
from src.common.fraud_scenarios import (
    MEAN_FRAUD_EVENTS,
    PRECURSOR_EVENTS,
    SCENARIO_WEIGHTS,
    STEALTH_PROBABILITY,
    EpisodeEvent,
    build_episode,
)
from src.common.schemas import (
    Channel,
    Currency,
    CustomerSegment,
    FraudType,
    MerchantCategory,
    TransactionType,
)

# ── Constantes ─────────────────────────────────────────────────────────────────

MARKET_SYMBOLS = [
    "PETR4.SA",
    "VALE3.SA",
    "ITUB4.SA",
    "BBDC4.SA",
    "ABEV3.SA",
    "WEGE3.SA",
    "RENT3.SA",
    "BBAS3.SA",
    "MGLU3.SA",
    "LREN3.SA",
]

# Pesos para distribuição de transaction_type
TRANSACTION_TYPE_WEIGHTS = [0.45, 0.20, 0.10, 0.12, 0.08, 0.05]  # PIX dominante

# Pesos para merchant_category
MERCHANT_CATEGORY_WEIGHTS = [0.25, 0.20, 0.10, 0.08, 0.12, 0.10, 0.05, 0.10]

# Pesos para channel
CHANNEL_WEIGHTS = [0.50, 0.25, 0.05, 0.10, 0.10]

# Pesos para currency (80% BRL)
CURRENCY_WEIGHTS = [0.80, 0.12, 0.08]

_TRANSACTION_TYPES = [t.value for t in TransactionType]
_MERCHANT_CATEGORIES = [m.value for m in MerchantCategory]
_CHANNELS = [c.value for c in Channel]
_CURRENCIES = [c.value for c in Currency]

FRAUD_RATE = 0.025  # fração de eventos fraudulentos

# Comportamento legítimo com ruído (hard negatives). As taxas de device/IP valem entre os eventos
# em que o campo está presente.
RECENT_ACCOUNT_RATE = 0.04  # clientes com conta aberta nos últimos NEW_ACCOUNT_DAYS dias
ACCOUNT_HISTORY_DAYS = 3653  # "-10y" do Faker: mantém as datas idênticas às da versão relativa
NEW_DEVICE_RATE = 0.03  # troca de celular
NEW_IP_RATE = 0.05  # rede nova (viagem curta, Wi-Fi público)
NEW_DESTINATION_RATE = 0.20  # destinatário fora dos frequentes
BIG_PURCHASE_RATE = 0.01  # compra grande legítima (×5–10 do valor típico)
OFF_HOURS_RATE = 0.03  # evento legítimo fora do horário habitual do cliente
MISSING_DEVICE_RATE = 0.10
MISSING_IP_RATE = 0.05

# Viagens legítimas: o cliente fica numa outra cidade por 6–48h. Início por tempo (1 viagem a cada
# ~90 dias), não por evento, para valer igual no batch (eventos esparsos) e no streaming (densos).
TRIP_MEAN_INTERVAL_H = 24 * 90
TRIP_MEAN_DURATION_H = 27  # duração média (6–48h)
TRAVEL_SPEED_KMH = 700  # abaixo do limiar de "viagem impossível" (900 km/h) usado na detecção


# ── Estruturas internas ────────────────────────────────────────────────────────


@dataclass(slots=True)
class _Row:
    """Transação em construção: campos ainda sem id/timestamp e metadados de ground truth."""

    fields: dict[str, Any]
    epoch: int
    is_fraud: bool = False
    fraud_type: str | None = None
    hard_negatives: list[str] = field(default_factory=list)
    fixed_location: bool = False  # coordenadas definidas pelo cenário (não passam pelo tracker)
    episode_id: str = ""
    stealth: bool = False


class _TravelTracker:
    """Localização de cada cliente ao longo do tempo, incluindo viagens legítimas.

    Consumido em ordem de tempo por cliente. Garante que o intervalo entre a última transação em
    casa e a primeira na cidade da viagem (e o inverso) seja compatível com `TRAVEL_SPEED_KMH`,
    para uma viagem legítima nunca parecer "viagem impossível".
    """

    def __init__(self) -> None:
        self._trip: dict[str, tuple[dict[str, Any], int]] = {}  # cliente → (cidade, até quando)
        self._last: dict[str, int] = {}

    def locate(
        self, profile: CustomerProfile, epoch: int, rng: random.Random
    ) -> tuple[float, float, bool]:
        """(latitude, longitude, em_viagem) do cliente no instante `epoch`."""
        cid, home = profile.customer_id, profile.city
        last = self._last.get(cid)
        city, traveling = home, False

        trip = self._trip.get(cid)
        if trip is not None:
            trip_city, until = trip
            needed = city_distance_km(trip_city, home) / TRAVEL_SPEED_KMH * 3600
            if epoch < until or (last is not None and epoch - last < needed):
                city, traveling = trip_city, True
            else:
                del self._trip[cid]
        elif last is not None:
            gap = epoch - last
            gap_h = gap / 3600
            # P(uma viagem começou no intervalo) × P(o evento cai dentro dela). Com eventos
            # esparsos (batch) a maior parte das viagens termina antes do próximo evento e não
            # aparece; sem o segundo fator, a fração de eventos em viagem sairia inflada.
            p_start = 1 - math.exp(-gap_h / TRIP_MEAN_INTERVAL_H)
            p_observed = min(1.0, TRIP_MEAN_DURATION_H / gap_h) if gap_h > 0 else 1.0
            if rng.random() < p_start * p_observed:
                trip_city = rng.choice([c for c in BRAZILIAN_CITIES if c["city"] != home["city"]])
                needed = city_distance_km(home, trip_city) / TRAVEL_SPEED_KMH * 3600
                if gap >= needed:
                    self._trip[cid] = (trip_city, epoch + int(rng.uniform(6, 48) * 3600))
                    city, traveling = trip_city, True

        self._last[cid] = epoch
        lat, lon = jitter_location(rng, city)
        return lat, lon, traveling


class _StreamTravel:
    """Viagens legítimas no streaming, onde cada cliente transaciona a cada poucos minutos.

    O `_TravelTracker` do batch só inicia uma viagem se o intervalo desde o último evento do cliente
    cobre o deslocamento. No streaming esse intervalo quase nunca existe (~100 s entre eventos, contra
    pelo menos ~30 min de viagem), então nenhuma viagem legítima aconteceria. Aqui a viagem tem fases
    explícitas: o cliente parte (o último evento é em casa), fica **em trânsito sem emitir nada** até
    chegar, transaciona na cidade da viagem, e volta (de novo em trânsito). Chegada e retorno
    respeitam `TRAVEL_SPEED_KMH`, então uma viagem legítima nunca parece "viagem impossível".
    """

    def __init__(self) -> None:
        # cliente → (cidade, chegada, saída, volta), em epoch: em trânsito antes da chegada e entre
        # a saída e a volta; na cidade da viagem entre a chegada e a saída.
        self._trips: dict[str, tuple[dict[str, Any], int, int, int]] = {}
        self._last: dict[str, int] = {}
        self.seeded = False

    def seed_steady_state(
        self, profiles: list[CustomerProfile], epoch: int, rng: random.Random
    ) -> None:
        """Estado inicial estacionário: a fração de clientes que já estaria fora num stream longo."""
        self.seeded = True
        p_away = TRIP_MEAN_DURATION_H / (TRIP_MEAN_INTERVAL_H + TRIP_MEAN_DURATION_H)
        for profile in profiles:
            if rng.random() < p_away:
                city = rng.choice(
                    [c for c in BRAZILIAN_CITIES if c["city"] != profile.city["city"]]
                )
                needed = int(city_distance_km(profile.city, city) / TRAVEL_SPEED_KMH * 3600)
                leave = epoch + int(rng.uniform(0, 48 * 3600))
                self._trips[profile.customer_id] = (city, epoch - 1, leave, leave + needed)

    def _trip(self, customer_id: str, epoch: int) -> tuple[dict[str, Any], int, int, int] | None:
        trip = self._trips.get(customer_id)
        if trip is not None and epoch >= trip[3]:  # já voltou para casa
            del self._trips[customer_id]
            return None
        return trip

    def away(self, customer_id: str, epoch: int) -> bool:
        """Cliente em viagem (na cidade destino ou nos deslocamentos)."""
        return self._trip(customer_id, epoch) is not None

    def in_transit(self, customer_id: str, epoch: int) -> bool:
        """Cliente se deslocando: não pode emitir evento (não está em nenhuma das duas cidades)."""
        trip = self._trip(customer_id, epoch)
        return trip is not None and (epoch < trip[1] or epoch >= trip[2])

    def locate(
        self, profile: CustomerProfile, epoch: int, rng: random.Random
    ) -> tuple[float, float, bool]:
        """(latitude, longitude, em_viagem). Só chamar para cliente fora de trânsito."""
        cid = profile.customer_id
        trip = self._trip(cid, epoch)
        if trip is not None:
            lat, lon = jitter_location(rng, trip[0])
            self._last[cid] = epoch
            return lat, lon, True

        last = self._last.get(cid)
        if last is not None:
            gap_h = (epoch - last) / 3600
            # Início por tempo (1 viagem a cada ~90 dias): a soma dos intervalos é o tempo total.
            if gap_h > 0 and rng.random() < 1 - math.exp(-gap_h / TRIP_MEAN_INTERVAL_H):
                city = rng.choice(
                    [c for c in BRAZILIAN_CITIES if c["city"] != profile.city["city"]]
                )
                needed = int(city_distance_km(profile.city, city) / TRAVEL_SPEED_KMH * 3600)
                arrive = epoch + needed
                leave = arrive + int(rng.uniform(6, 48) * 3600)
                self._trips[cid] = (city, arrive, leave, leave + needed)
        self._last[cid] = epoch
        lat, lon = jitter_location(rng, profile.city)  # o evento da partida ainda é em casa
        return lat, lon, False


def episode_probability(fraud_rate: float = FRAUD_RATE) -> float:
    """Probabilidade de um sorteio do streaming abrir um episódio de fraude.

    Calibrada para que a fração de *eventos* fraudulentos seja ~`fraud_rate`, considerando que um
    episódio traz vários eventos (e, no clone de cartão, um evento legítimo de contexto).
    """
    fraud_per_episode = sum(w * MEAN_FRAUD_EVENTS[s] for s, w in SCENARIO_WEIGHTS.items())
    total_per_episode = fraud_per_episode + sum(
        w * PRECURSOR_EVENTS.get(s, 0.0) for s, w in SCENARIO_WEIGHTS.items()
    )
    return fraud_rate / (fraud_per_episode - fraud_rate * (total_per_episode - 1))


# ── DataGenerator ──────────────────────────────────────────────────────────────


class DataGenerator:
    """Gera datasets sintéticos realistas de transações financeiras."""

    def __init__(self, seed: int = 42) -> None:
        # `seed` define clientes e perfis (estáveis entre batch e streaming); `rng`/`np_rng`
        # geram os eventos e podem ser reiniciados à parte com `reseed_events`.
        self.seed = seed
        # Instâncias isoladas de RNG para garantir reprodutibilidade
        self.rng = random.Random(seed)
        self.np_rng = np.random.RandomState(seed)
        self.fake = Faker("pt_BR")
        # Por instância: `Faker.seed` é global à classe, e dois geradores na mesma execução
        # (ex.: testes, ou batch + producer) se atrapalhariam nos nomes e datas dos clientes.
        self.fake.seed_instance(seed)
        self._profiles: dict[str, CustomerProfile] = {}
        # Sidecar de ground truth da última chamada de `generate_transactions`.
        self.last_ground_truth: list[dict[str, Any]] = []

    def _uuid(self) -> str:
        """Gera UUID determinístico usando a instância de RNG."""
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def reseed_events(self, seed: int) -> None:
        """Reinicia só o RNG dos eventos, sem mexer em `self.seed` (clientes e perfis).

        O producer de streaming precisa dos mesmos clientes do `make seed-data` (seed 42) mas de
        eventos diferentes a cada execução: com a seed toda fixa, um reinício reproduziria os
        mesmos `transaction_id`.
        """
        self.rng = random.Random(seed)
        self.np_rng = np.random.RandomState(seed)

    def profile_for(self, customer: dict[str, Any]) -> CustomerProfile:
        """Perfil de comportamento do cliente (determinístico por seed + customer_id)."""
        customer_id = customer["customer_id"]
        profile = self._profiles.get(customer_id)
        if profile is None:
            profile = build_profile(self.seed, customer)
            self._profiles[customer_id] = profile
        return profile

    # ── Customers ──────────────────────────────────────────────────────────────

    def generate_customers(
        self, n: int = 10_000, reference_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Gera n registros de clientes (tabela dimensional).

        `reference_date` é o "hoje" das datas relativas (abertura da conta: até 10 anos atrás, ou
        nos últimos `NEW_ACCOUNT_DAYS` dias nas contas recentes). O padrão é a data de hoje; fixe-o
        para gerar clientes reproduzíveis independentemente do dia da execução (avaliação do
        detector, issue #44). Com o padrão o resultado é idêntico ao de antes.
        """
        ref = reference_date or date.today()
        customers = []
        genders = ["M", "F"]
        segments = [s.value for s in CustomerSegment]
        segment_weights = [0.70, 0.25, 0.05]

        for _ in range(n):
            city_info = self.rng.choice(BRAZILIAN_CITIES)
            gender = self.rng.choice(genders)
            birth_date = self.fake.date_of_birth(minimum_age=18, maximum_age=75)
            opening_date = self.fake.date_between_dates(
                date_start=ref - timedelta(days=ACCOUNT_HISTORY_DAYS), date_end=ref
            )
            if self.rng.random() < RECENT_ACCOUNT_RATE:
                # Contas recentes: alvo do cenário de roubo de identidade (ACCOUNT_AGE_LOW).
                opening_date = self.fake.date_between_dates(
                    date_start=ref - timedelta(days=NEW_ACCOUNT_DAYS), date_end=ref
                )

            # CPF mascarado: formato ***.***.***-XX (só os 2 últimos dígitos visíveis)
            cpf_digits = "".join([str(self.rng.randint(0, 9)) for _ in range(11)])
            cpf_masked = f"***.***.***-{cpf_digits[9:11]}"

            customers.append(
                {
                    "customer_id": self._uuid(),
                    "name": self.fake.name_male() if gender == "M" else self.fake.name_female(),
                    "cpf_masked": cpf_masked,
                    "birth_date": birth_date.strftime("%Y-%m-%d"),
                    "gender": gender,
                    "account_opening_date": opening_date.strftime("%Y-%m-%d"),
                    "risk_score": round(self.rng.uniform(0, 100), 2),
                    "segment": self.rng.choices(segments, weights=segment_weights, k=1)[0],
                    "city": city_info["city"],
                    "state": city_info["state"],
                    "country": "BR",
                }
            )

        return customers

    # ── Transactions (batch) ───────────────────────────────────────────────────

    def generate_transactions(
        self,
        customers: list[dict[str, Any]],
        n: int = 500_000,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Gera n transações realistas com ~2,5% de eventos fraudulentos.

        A fraude vem em episódios (1–6 eventos coerentes com o tipo) e o tráfego legítimo segue o
        perfil de cada cliente, com ruído. O resultado sai ordenado por timestamp. O ground truth
        por evento (episódio, cenário, stealth, hard negatives) fica em `self.last_ground_truth`,
        fora do `TransactionEvent` de propósito: nenhum contrato de dados muda.
        """
        if start_date is None:
            end_date = datetime.now(tz=UTC)
            start_date = end_date - timedelta(days=180)
        elif end_date is None:
            end_date = datetime.now(tz=UTC)

        self.last_ground_truth = []
        if n <= 0 or not customers:
            return []

        start_epoch, end_epoch = int(start_date.timestamp()), int(end_date.timestamp())
        profiles = [self.profile_for(c) for c in customers]
        by_id = {p.customer_id: p for p in profiles}

        rows = self._build_episode_rows(profiles, n, start_epoch, end_epoch)
        rows.extend(self._build_legit_rows(profiles, n - len(rows), start_epoch, end_epoch))
        self._apply_trips(rows, by_id)
        rows.sort(key=lambda r: r.epoch)

        transactions: list[dict[str, Any]] = []
        truth: list[dict[str, Any]] = []
        for row in rows:
            tx = self._to_transaction(row)
            transactions.append(tx)
            if row.is_fraud:
                truth.append(
                    {
                        "transaction_id": tx["transaction_id"],
                        "episode_id": row.episode_id,
                        "scenario": row.fraud_type,
                        "stealth": row.stealth,
                        "hard_negative": "",
                    }
                )
            elif row.hard_negatives:
                truth.append(
                    {
                        "transaction_id": tx["transaction_id"],
                        "episode_id": "",
                        "scenario": "",
                        "stealth": False,
                        "hard_negative": ";".join(row.hard_negatives),
                    }
                )
        self.last_ground_truth = truth
        return transactions

    def _build_legit_rows(
        self, profiles: list[CustomerProfile], count: int, start_epoch: int, end_epoch: int
    ) -> list[_Row]:
        rows: list[_Row] = []
        rng = self.rng
        for _ in range(max(count, 0)):
            profile = rng.choice(profiles)
            lo = start_epoch
            if (
                profile.opening_epoch is not None
                and start_epoch < profile.opening_epoch < end_epoch
            ):
                lo = profile.opening_epoch  # não há transação antes de a conta existir
            epoch = rng.randint(lo, end_epoch) if end_epoch > lo else lo
            if rng.random() >= OFF_HOURS_RATE:
                moved = set_local_hour(epoch, rng.choice(profile.active_hours))
                if lo <= moved <= end_epoch:
                    epoch = moved
            fields, hard_negatives = self._draft_legit(profile)
            if local_hour(epoch) not in profile.active_hours:
                hard_negatives.append("off_hours")
            rows.append(_Row(fields=fields, epoch=epoch, hard_negatives=hard_negatives))
        return rows

    def _build_episode_rows(
        self, profiles: list[CustomerProfile], n: int, start_epoch: int, end_epoch: int
    ) -> list[_Row]:
        rng = self.rng
        target = int(n * FRAUD_RATE + rng.random())  # arredondamento probabilístico
        recent = [
            p
            for p in profiles
            if p.opening_epoch is not None and start_epoch <= p.opening_epoch <= end_epoch
        ]
        rows: list[_Row] = []
        fraud_rows = 0
        episode_seq = 0
        while fraud_rows < target and len(rows) < n:
            scenario = self._pick_scenario(has_new_accounts=bool(recent))
            stealth = rng.random() < STEALTH_PROBABILITY
            is_identity = scenario == FraudType.IDENTITY_THEFT.value
            victim = rng.choice(recent if is_identity else profiles)
            episode = build_episode(
                scenario,
                rng,
                victim,
                stealth=stealth,
                accomplices=self._accomplices_for(scenario, profiles, victim, start_epoch),
            )

            lo = start_epoch
            if victim.opening_epoch is not None and start_epoch < victim.opening_epoch < end_epoch:
                lo = victim.opening_epoch
            hi = end_epoch - int(episode.span_s)
            if is_identity and victim.opening_epoch is not None:
                hi = min(hi, victim.opening_epoch + NEW_ACCOUNT_DAYS * 86400)
            hi = max(hi, lo)
            t0 = rng.randint(lo, hi)
            if episode.start_hour_local is not None:
                moved = set_local_hour(t0, episode.start_hour_local)
                if lo <= moved <= hi:
                    t0 = moved

            episode_seq += 1
            episode_id = f"ep-{episode_seq:06d}"
            for event in episode.events[: n - len(rows)]:
                rows.append(
                    _Row(
                        fields=self._episode_event_fields(event),
                        epoch=min(t0 + int(event.delay_s), end_epoch),
                        is_fraud=event.is_fraud,
                        fraud_type=event.fraud_type,
                        fixed_location=True,
                        episode_id=episode_id if event.is_fraud else "",
                        stealth=episode.stealth,
                    )
                )
                fraud_rows += event.is_fraud
        return rows

    def _apply_trips(self, rows: list[_Row], by_id: dict[str, CustomerProfile]) -> None:
        """Define a localização dos eventos legítimos, com as viagens de cada cliente."""
        tracker = _TravelTracker()
        legit = sorted(
            (r for r in rows if not r.fixed_location),
            key=lambda r: (r.fields["customer_id"], r.epoch),
        )
        for row in legit:
            lat, lon, traveling = tracker.locate(
                by_id[row.fields["customer_id"]], row.epoch, self.rng
            )
            row.fields["latitude"], row.fields["longitude"] = lat, lon
            if traveling:
                row.hard_negatives.append("travel")

    # ── Composição de eventos ──────────────────────────────────────────────────

    def _pick_scenario(self, has_new_accounts: bool) -> str:
        scenarios = [
            s for s in SCENARIO_WEIGHTS if has_new_accounts or s != FraudType.IDENTITY_THEFT.value
        ]
        weights = [SCENARIO_WEIGHTS[s] for s in scenarios]
        return self.rng.choices(scenarios, weights=weights, k=1)[0]

    def _accomplices_for(
        self,
        scenario: str,
        profiles: list[CustomerProfile],
        victim: CustomerProfile,
        not_after: int,
    ) -> list[CustomerProfile]:
        """Remetentes adicionais da mesma conta-mula (só na lavagem de dinheiro).

        Só entram contas abertas até `not_after`: o episódio pode cair em qualquer instante a
        partir daí, e uma conta não transaciona antes de existir.
        """
        if scenario != FraudType.MONEY_LAUNDERING.value:
            return []
        others = [
            p
            for p in profiles
            if p is not victim and (p.opening_epoch is None or p.opening_epoch <= not_after)
        ]
        return self.rng.sample(others, min(5, len(others)))

    def _draft_legit(self, profile: CustomerProfile) -> tuple[dict[str, Any], list[str]]:
        """Campos de uma transação legítima do cliente (sem id, timestamp nem coordenadas).

        Devolve também os hard negatives aplicados (`new_device`, `new_ip`, `big_purchase`).
        """
        rng = self.rng
        tx_type = rng.choices(_TRANSACTION_TYPES, weights=TRANSACTION_TYPE_WEIGHTS, k=1)[0]
        merchant = rng.choices(_MERCHANT_CATEGORIES, weights=MERCHANT_CATEGORY_WEIGHTS, k=1)[0]
        channel = rng.choices(_CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
        currency = rng.choices(_CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0]
        hard_negatives: list[str] = []

        amount = rng.lognormvariate(profile.amount_mu, profile.amount_sigma)
        if rng.random() < BIG_PURCHASE_RATE:
            amount = profile.typical_amount * rng.uniform(5, 10)
            hard_negatives.append("big_purchase")
        amount = round(min(max(amount, 1.0), 500_000.0), 2)

        if rng.random() < NEW_DESTINATION_RATE:
            dest_account, dest_bank = random_account(rng), rng.choice(BRAZILIAN_BANKS)
        else:
            dest_account, dest_bank = rng.choice(profile.contacts)

        device, ip = None, None
        if channel not in PHYSICAL_CHANNELS:  # cartão presente não traz device nem IP
            if rng.random() >= MISSING_DEVICE_RATE:
                if rng.random() < NEW_DEVICE_RATE:
                    device = random_device(rng)
                    hard_negatives.append("new_device")
                else:
                    device = rng.choice(profile.devices)
            if rng.random() >= MISSING_IP_RATE:
                if rng.random() < NEW_IP_RATE:
                    ip = random_ip(rng, random_ip_prefix(rng, avoid=profile.ip_prefixes))
                    hard_negatives.append("new_ip")
                else:
                    ip = random_ip(rng, rng.choice(profile.ip_prefixes))

        fields = {
            "customer_id": profile.customer_id,
            "amount": amount,
            "currency": currency,
            "transaction_type": tx_type,
            "merchant_category": merchant,
            "channel": channel,
            "destination_account": dest_account,
            "destination_bank": dest_bank,
            "device_id": device,
            "ip_address": ip,
        }
        return fields, hard_negatives

    def _episode_event_fields(self, event: EpisodeEvent) -> dict[str, Any]:
        """Campos de um evento de episódio, com o mesmo ruído de campos ausentes do legítimo."""
        rng = self.rng
        device, ip = event.device_id, event.ip_address
        if event.channel in PHYSICAL_CHANNELS:
            device = ip = None
        else:
            if rng.random() < MISSING_DEVICE_RATE:
                device = None
            if rng.random() < MISSING_IP_RATE:
                ip = None
        return {
            "customer_id": event.customer_id,
            "amount": event.amount,
            "currency": rng.choices(_CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0],
            "transaction_type": event.transaction_type,
            "merchant_category": event.merchant_category,
            "channel": event.channel,
            "destination_account": event.destination_account,
            "destination_bank": event.destination_bank,
            "device_id": device,
            "ip_address": ip,
            "latitude": event.latitude,
            "longitude": event.longitude,
        }

    def _to_transaction(self, row: _Row) -> dict[str, Any]:
        f = row.fields
        profile = self._profiles[f["customer_id"]]
        return {
            "transaction_id": self._uuid(),
            "customer_id": f["customer_id"],
            "timestamp": datetime.fromtimestamp(row.epoch, tz=UTC).isoformat(),
            "amount": f["amount"],
            "currency": f["currency"],
            "transaction_type": f["transaction_type"],
            "merchant_category": f["merchant_category"],
            "origin_account": profile.own_account,
            "destination_account": f["destination_account"],
            "origin_bank": profile.own_bank,
            "destination_bank": f["destination_bank"],
            "channel": f["channel"],
            "device_id": f["device_id"],
            "ip_address": f["ip_address"],
            "latitude": f["latitude"],
            "longitude": f["longitude"],
            "is_fraud": row.is_fraud,
            "fraud_type": row.fraud_type,
        }

    # ── Market Data ────────────────────────────────────────────────────────────

    def generate_market_data(
        self,
        symbols: list[str] | None = None,
        n_days: int = 100,
        end_date: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Gera n_days de dados OHLCV para cada símbolo (total = len(symbols) * n_days)."""
        if symbols is None:
            symbols = MARKET_SYMBOLS
        if end_date is None:
            end_date = datetime.now(tz=UTC)

        records = []

        # Preços iniciais realistas (BRL)
        initial_prices: dict[str, float] = {
            "PETR4.SA": 38.50,
            "VALE3.SA": 68.00,
            "ITUB4.SA": 32.00,
            "BBDC4.SA": 14.50,
            "ABEV3.SA": 12.80,
            "WEGE3.SA": 42.00,
            "RENT3.SA": 58.00,
            "BBAS3.SA": 27.00,
            "MGLU3.SA": 5.50,
            "LREN3.SA": 18.00,
        }

        for symbol in symbols:
            base_price = initial_prices.get(symbol, 30.0)
            price = base_price

            trading_days = self._get_trading_days(end_date, n_days)

            for trade_date in trading_days:
                # Random walk com drift leve
                daily_return = self.np_rng.normal(0.0002, 0.018)
                price = max(0.01, price * (1 + daily_return))

                intraday_vol = price * self.rng.uniform(0.005, 0.03)
                open_p = round(price * (1 + self.np_rng.normal(0, 0.005)), 2)
                close_p = round(price, 2)
                high_p = round(
                    max(open_p, close_p) + abs(self.np_rng.normal(0, intraday_vol)), 2
                )
                low_p = round(
                    min(open_p, close_p) - abs(self.np_rng.normal(0, intraday_vol)), 2
                )
                low_p = max(0.01, low_p)
                volume = int(self.np_rng.lognormal(14, 1))
                adj_close = round(close_p * self.rng.uniform(0.97, 1.0), 2)

                records.append(
                    {
                        "symbol": symbol,
                        "date": trade_date.strftime("%Y-%m-%d"),
                        "open": open_p,
                        "high": high_p,
                        "low": low_p,
                        "close": close_p,
                        "volume": volume,
                        "adjusted_close": adj_close,
                    }
                )

        return records

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _get_trading_days(end_date: datetime, n_days: int) -> list[datetime]:
        """Retorna lista de dias úteis (seg–sex) terminando em end_date."""
        days: list[datetime] = []
        current = end_date
        while len(days) < n_days:
            if current.weekday() < 5:  # 0=seg … 4=sex
                days.append(current)
            current -= timedelta(days=1)
        return list(reversed(days))


# ── Transactions (streaming) ───────────────────────────────────────────────────


class TransactionStream:
    """Fonte de eventos em tempo real para o producer de streaming.

    A cada `next_events(now)` devolve uma lista de `(atraso_em_segundos, transação)`: um evento
    legítimo com atraso 0, ou um episódio de fraude cujos follow-ups saem com atraso > 0 (o
    producer os mantém num heap e emite na hora certa). O evento legítimo vem de um cliente ativo
    naquela hora local. As viagens legítimas têm fases (`_StreamTravel`): quem viaja fica em trânsito,
    sem emitir nada, entre a partida e a chegada.

    Diferenças conscientes em relação ao batch: o streaming acontece "agora", então a hora de
    início do episódio de account takeover (madrugada no batch) não é imposta; e, por padrão, o
    ritmo é constante (o do producer) e qualquer cliente pode transacionar a qualquer hora. Com
    `diurnal=True` o ritmo segue o horário ativo dos clientes: um sorteio de cliente inativo naquela
    hora (fora dos `OFF_HOURS_RATE`) não emite nada, e o volume cai de madrugada como na vida real.
    """

    def __init__(
        self,
        generator: DataGenerator,
        customers: list[dict[str, Any]],
        fraud_rate: float = FRAUD_RATE,
        diurnal: bool = False,
        record_ground_truth: bool = False,
    ) -> None:
        self._gen = generator
        self._profiles = [generator.profile_for(c) for c in customers]
        self._travel = _StreamTravel()
        self._episode_prob = episode_probability(fraud_rate)
        self._diurnal = diurnal
        # Ground truth por evento (mesmo formato de `DataGenerator.last_ground_truth`). Opt-in: o
        # producer roda por dias e a lista cresceria sem limite; só a avaliação (issue #44) usa.
        self._record = record_ground_truth
        self.ground_truth: list[dict[str, Any]] = []
        self._episode_seq = 0

    def next_events(self, now: datetime) -> list[tuple[float, dict[str, Any]]]:
        """Eventos deste instante como `(atraso_s, transação)`; vazio no modo diurno fora de hora."""
        epoch = int(now.timestamp())
        if not self._travel.seeded:
            self._travel.seed_steady_state(self._profiles, epoch, self._gen.rng)
        if self._gen.rng.random() < self._episode_prob:
            return self._episode_events(epoch)
        event = self._legit_event(epoch)
        return [(0.0, event)] if event is not None else []

    def _pick_active(self, epoch: int) -> CustomerProfile:
        """Cliente ativo nesta hora local e em casa (fraude não mira quem está viajando)."""
        rng = self._gen.rng
        hour = local_hour(epoch)
        profile = rng.choice(self._profiles)
        for _ in range(20):
            if hour in profile.active_hours and not self._travel.away(profile.customer_id, epoch):
                break
            profile = rng.choice(self._profiles)
        return profile

    def _pick_victim(self, epoch: int) -> CustomerProfile:
        if self._diurnal:
            return self._pick_active(epoch)
        # Vítima em casa: o evento legítimo de contexto do clone de cartão é em casa.
        rng = self._gen.rng
        profile = rng.choice(self._profiles)
        for _ in range(20):
            if not self._travel.away(profile.customer_id, epoch):
                break
            profile = rng.choice(self._profiles)
        return profile

    def _legit_event(self, epoch: int) -> dict[str, Any] | None:
        gen = self._gen
        # Quem está em trânsito não emite nada: sorteia outro cliente.
        profile = None
        for _ in range(20):
            candidate = gen.rng.choice(self._profiles)
            if not self._travel.in_transit(candidate.customer_id, epoch):
                profile = candidate
                break
        if profile is None:
            return None
        if (
            self._diurnal
            and local_hour(epoch) not in profile.active_hours
            and gen.rng.random() >= OFF_HOURS_RATE
        ):
            return None
        fields, hard_negatives = gen._draft_legit(profile)
        lat, lon, traveling = self._travel.locate(profile, epoch, gen.rng)
        fields["latitude"], fields["longitude"] = lat, lon
        tx = gen._to_transaction(_Row(fields=fields, epoch=epoch))
        if self._record:
            if traveling:
                hard_negatives.append("travel")
            if local_hour(epoch) not in profile.active_hours:
                hard_negatives.append("off_hours")
            if hard_negatives:
                self.ground_truth.append(
                    {
                        "transaction_id": tx["transaction_id"],
                        "episode_id": "",
                        "scenario": "",
                        "stealth": False,
                        "hard_negative": ";".join(hard_negatives),
                    }
                )
        return tx

    def _episode_events(self, epoch: int) -> list[tuple[float, dict[str, Any]]]:
        gen = self._gen
        rng = gen.rng
        recent = [
            p
            for p in self._profiles
            if p.opening_epoch is not None
            and 0 <= epoch - p.opening_epoch <= NEW_ACCOUNT_DAYS * 86400
        ]
        scenario = gen._pick_scenario(has_new_accounts=bool(recent))
        is_identity = scenario == FraudType.IDENTITY_THEFT.value
        victim = rng.choice(recent) if is_identity else self._pick_victim(epoch)
        episode = build_episode(
            scenario,
            rng,
            victim,
            stealth=rng.random() < STEALTH_PROBABILITY,
            accomplices=gen._accomplices_for(scenario, self._profiles, victim, epoch),
        )
        self._episode_seq += 1
        episode_id = f"ep-{self._episode_seq:06d}"
        events = []
        for event in sorted(episode.events, key=lambda e: e.delay_s):
            row = _Row(
                fields=gen._episode_event_fields(event),
                epoch=epoch + int(event.delay_s),
                is_fraud=event.is_fraud,
                fraud_type=event.fraud_type,
                fixed_location=True,
            )
            tx = gen._to_transaction(row)
            events.append((event.delay_s, tx))
            if self._record and event.is_fraud:
                self.ground_truth.append(
                    {
                        "transaction_id": tx["transaction_id"],
                        "episode_id": episode_id,
                        "scenario": event.fraud_type,
                        "stealth": episode.stealth,
                        "hard_negative": "",
                    }
                )
        return events
