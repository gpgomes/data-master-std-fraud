"""Testes do gerador v2: perfis, episódios de fraude, hard negatives e streaming (issue #43).

A maior parte dos testes usa um dataset de 30 mil transações gerado uma única vez por módulo
(seed 42), cruzado com o sidecar de ground truth (`gen.last_ground_truth`).
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from src.common.customer_profile import (
    NEW_ACCOUNT_DAYS,
    PHYSICAL_CHANNELS,
    haversine_km,
    ip_prefix,
    local_hour,
)
from src.common.data_generator import (
    FRAUD_RATE,
    DataGenerator,
    TransactionStream,
    episode_probability,
)
from src.common.schemas import FraudType, TransactionEvent

N_CUSTOMERS = 500
N_TX = 30_000


def _epoch(tx: dict) -> float:
    return datetime.fromisoformat(tx["timestamp"]).timestamp()


@pytest.fixture(scope="module")
def dataset():
    gen = DataGenerator(seed=42)
    customers = gen.generate_customers(n=N_CUSTOMERS)
    txs = gen.generate_transactions(customers, n=N_TX)
    return gen, customers, txs, gen.last_ground_truth


@pytest.fixture(scope="module")
def profiles(dataset):
    gen, customers, _, _ = dataset
    return {c["customer_id"]: gen.profile_for(c) for c in customers}


@pytest.fixture(scope="module")
def episodes(dataset):
    """episode_id → (scenario, stealth, [transações fraudulentas do episódio])."""
    _, _, txs, truth = dataset
    by_id = {t["transaction_id"]: t for t in txs}
    grouped: dict[str, list[dict]] = defaultdict(list)
    meta: dict[str, tuple[str, bool]] = {}
    for row in truth:
        if row["episode_id"]:
            grouped[row["episode_id"]].append(by_id[row["transaction_id"]])
            meta[row["episode_id"]] = (row["scenario"], row["stealth"])
    return {ep: (*meta[ep], sorted(txs_, key=_epoch)) for ep, txs_ in grouped.items()}


def _of_type(episodes: dict, scenario: FraudType, *, stealth: bool | None = None) -> list:
    return [
        (evs, s)
        for _, (sc, s, evs) in episodes.items()
        if sc == scenario.value and (stealth is None or s == stealth)
    ]


def _share(flags: list[bool]) -> float:
    return sum(flags) / len(flags)


# ── Perfil e determinismo ──────────────────────────────────────────────────────


class TestProfilesAndDeterminism:
    def test_customers_do_not_expose_profile_columns(self) -> None:
        customers = DataGenerator(seed=42).generate_customers(n=5)
        assert set(customers[0]) == {
            "customer_id", "name", "cpf_masked", "birth_date", "gender",
            "account_opening_date", "risk_score", "segment", "city", "state", "country",
        }  # fmt: skip

    def test_profile_is_independent_of_call_order_and_event_rng(self) -> None:
        gen = DataGenerator(seed=42)
        customers = gen.generate_customers(n=20)
        first = {c["customer_id"]: gen.profile_for(c) for c in customers}

        other = DataGenerator(seed=42)
        other.generate_customers(n=20)
        other.generate_transactions(customers, n=300)  # consome bastante do RNG de eventos
        for customer in reversed(customers):
            assert other.profile_for(customer) == first[customer["customer_id"]]

    def test_same_seed_generates_the_same_transactions_and_ground_truth(self) -> None:
        def run() -> tuple[list[dict], list[dict]]:
            gen = DataGenerator(seed=7)
            customers = gen.generate_customers(n=50)
            return gen.generate_transactions(customers, n=1_000), gen.last_ground_truth

        (txs1, truth1), (txs2, truth2) = run(), run()
        assert [t["transaction_id"] for t in txs1] == [t["transaction_id"] for t in txs2]
        assert truth1 == truth2

    def test_reseed_events_changes_events_but_not_customers_or_profiles(self) -> None:
        a, b = DataGenerator(seed=42), DataGenerator(seed=42)
        customers_a, customers_b = a.generate_customers(n=30), b.generate_customers(n=30)
        assert customers_a == customers_b

        b.reseed_events(999)
        ids_a = [t["transaction_id"] for t in a.generate_transactions(customers_a, n=100)]
        ids_b = [t["transaction_id"] for t in b.generate_transactions(customers_b, n=100)]
        assert ids_a != ids_b
        assert all(a.profile_for(c) == b.profile_for(c) for c in customers_a)

    def test_producer_customers_are_a_prefix_of_the_seed_data_customers(self) -> None:
        """Regressão do achado da #43: o stream (1.000 clientes) precisa existir no batch (10.000)."""
        stream_side = DataGenerator(seed=42).generate_customers(n=200)
        batch_side = DataGenerator(seed=42).generate_customers(n=2_000)[:200]
        assert [c["customer_id"] for c in stream_side] == [c["customer_id"] for c in batch_side]

    def test_some_customers_have_recent_accounts(self) -> None:
        customers = DataGenerator(seed=42).generate_customers(n=2_000)
        today = datetime.now(tz=UTC).date()
        recent = [
            c
            for c in customers
            if (today - datetime.fromisoformat(c["account_opening_date"]).date()).days
            <= NEW_ACCOUNT_DAYS
        ]
        assert 0.02 <= len(recent) / len(customers) <= 0.07


# ── Contrato e formato ─────────────────────────────────────────────────────────


class TestOutputContract:
    def test_transactions_validate_against_the_pydantic_contract(self, dataset) -> None:
        _, _, txs, _ = dataset
        for tx in txs[:1_500] + [t for t in txs if t["is_fraud"]][:300]:
            TransactionEvent(**tx)

    def test_exact_count_and_sorted_by_timestamp(self, dataset) -> None:
        _, _, txs, _ = dataset
        assert len(txs) == N_TX
        stamps = [t["timestamp"] for t in txs]
        assert stamps == sorted(stamps)

    def test_transaction_ids_are_unique(self, dataset) -> None:
        _, _, txs, _ = dataset
        assert len({t["transaction_id"] for t in txs}) == N_TX

    def test_fraud_rate_is_close_to_target(self, dataset) -> None:
        _, _, txs, _ = dataset
        rate = sum(t["is_fraud"] for t in txs) / len(txs)
        assert FRAUD_RATE * 0.85 <= rate <= FRAUD_RATE * 1.25

    def test_every_fraud_type_is_generated(self, dataset) -> None:
        _, _, txs, _ = dataset
        assert {t["fraud_type"] for t in txs if t["is_fraud"]} == {f.value for f in FraudType}

    def test_physical_channels_carry_no_device_or_ip(self, dataset) -> None:
        _, _, txs, _ = dataset
        physical = [t for t in txs if t["channel"] in PHYSICAL_CHANNELS]
        assert physical
        assert all(t["device_id"] is None and t["ip_address"] is None for t in physical)

    def test_device_and_ip_are_sometimes_missing_on_app_channels(self, dataset) -> None:
        _, _, txs, _ = dataset
        app = [t for t in txs if t["channel"] not in PHYSICAL_CHANNELS]
        assert 0.07 <= _share([t["device_id"] is None for t in app]) <= 0.13
        assert 0.03 <= _share([t["ip_address"] is None for t in app]) <= 0.08

    def test_origin_account_and_bank_are_stable_per_customer(self, dataset) -> None:
        _, _, txs, _ = dataset
        seen: dict[str, tuple[str, str]] = {}
        for tx in txs:
            pair = (tx["origin_account"], tx["origin_bank"])
            assert seen.setdefault(tx["customer_id"], pair) == pair

    def test_no_transaction_before_the_account_exists(self, dataset, profiles) -> None:
        _, _, txs, _ = dataset
        checked = 0
        for tx in txs:
            opening = profiles[tx["customer_id"]].opening_epoch
            if opening is not None and opening > _epoch(txs[0]):
                checked += 1
                assert _epoch(tx) >= opening
        assert checked > 0

    def test_timestamps_stay_inside_the_requested_range(self) -> None:
        gen = DataGenerator(seed=3)
        customers = gen.generate_customers(n=100)
        start, end = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 10, tzinfo=UTC)
        for tx in gen.generate_transactions(customers, n=3_000, start_date=start, end_date=end):
            assert start <= datetime.fromisoformat(tx["timestamp"]) <= end

    def test_empty_and_tiny_requests(self) -> None:
        gen = DataGenerator(seed=1)
        customers = gen.generate_customers(n=10)
        assert gen.generate_transactions(customers, n=0) == []
        assert gen.last_ground_truth == []
        assert gen.generate_transactions([], n=10) == []
        assert len(gen.generate_transactions(customers, n=1)) == 1


# ── Assinaturas dos episódios de fraude ────────────────────────────────────────


class TestFraudSignatures:
    """Cada tipo emite sua assinatura em ≥ (1 − stealth) dos episódios."""

    def test_ground_truth_covers_every_fraud_event_exactly_once(self, dataset) -> None:
        _, _, txs, truth = dataset
        fraud_ids = {t["transaction_id"] for t in txs if t["is_fraud"]}
        fraud_rows = [r for r in truth if r["scenario"]]
        assert {r["transaction_id"] for r in fraud_rows} == fraud_ids
        assert len(fraud_rows) == len(fraud_ids)
        assert all(r["episode_id"] for r in fraud_rows)

    def test_scenario_in_ground_truth_matches_fraud_type(self, dataset) -> None:
        _, _, txs, truth = dataset
        by_id = {t["transaction_id"]: t for t in txs}
        for row in truth:
            if row["scenario"]:
                assert by_id[row["transaction_id"]]["fraud_type"] == row["scenario"]

    def test_stealth_share_is_about_20_percent(self, episodes) -> None:
        flags = [stealth for _, stealth, _ in episodes.values()]
        assert 0.12 <= _share(flags) <= 0.30

    def test_account_takeover(self, episodes, profiles) -> None:
        eps = _of_type(episodes, FraudType.ACCOUNT_TAKEOVER)
        assert len(eps) >= 30

        def new_device(evs) -> bool:
            p = profiles[evs[0]["customer_id"]]
            return any(e["device_id"] and e["device_id"] not in p.devices for e in evs)

        def new_ip(evs) -> bool:
            p = profiles[evs[0]["customer_id"]]
            return any(e["ip_address"] and ip_prefix(e["ip_address"]) not in p.ip_prefixes
                       for e in evs)  # fmt: skip

        def far_from_home(evs) -> bool:
            home = profiles[evs[0]["customer_id"]].city
            return all(
                haversine_km(home["lat"], home["lon"], e["latitude"], e["longitude"]) > 700
                for e in evs
            )

        def at_night(evs) -> bool:
            return local_hour(_epoch(evs[0])) in range(0, 5)

        assert _share([new_device(evs) for evs, _ in eps]) >= 0.85
        assert _share([new_ip(evs) for evs, _ in eps]) >= 0.65  # stealth reusa a rede da vítima
        assert _share([far_from_home(evs) for evs, _ in eps]) == 1.0
        assert _share([at_night(evs) for evs, _ in eps]) >= 0.9
        # o stealth mascara só o IP: o device continua novo
        stealth = _of_type(episodes, FraudType.ACCOUNT_TAKEOVER, stealth=True)
        assert stealth
        assert not any(new_ip(evs) for evs, _ in stealth)

    def test_account_takeover_amounts_grow_within_the_episode(self, episodes) -> None:
        multi = [evs for evs, _ in _of_type(episodes, FraudType.ACCOUNT_TAKEOVER) if len(evs) > 1]
        assert multi
        for evs in multi:
            amounts = [e["amount"] for e in evs]
            assert amounts == sorted(amounts)

    def test_card_cloning_produces_impossible_travel(self, dataset, episodes) -> None:
        _, _, txs, _ = dataset
        by_customer: dict[str, list[tuple[float, dict]]] = defaultdict(list)
        for tx in txs:
            by_customer[tx["customer_id"]].append((_epoch(tx), tx))

        def has_impossible_pair(clone: dict) -> bool:
            events = by_customer[clone["customer_id"]]
            t = _epoch(clone)
            idx = bisect.bisect_left([e[0] for e in events], t)
            for ts, other in events[max(0, idx - 3) : idx]:
                km = haversine_km(
                    other["latitude"], other["longitude"], clone["latitude"], clone["longitude"]
                )
                if t - ts <= 7200 and km > 100 and km / ((t - ts) / 3600) > 900:
                    return True
            return False

        for stealth, expected in ((False, 0.85), (True, None)):
            clones = [
                evs[-1] for evs, _ in _of_type(episodes, FraudType.CARD_CLONING, stealth=stealth)
            ]
            assert clones
            assert all(c["channel"] == "ATM" and c["device_id"] is None for c in clones)
            hits = _share([has_impossible_pair(c) for c in clones])
            if expected is not None:
                assert hits >= expected
            else:  # stealth: cidade próxima e intervalo longo, velocidade implícita plausível
                assert hits <= 0.10

    def test_identity_theft_hits_recent_accounts_with_new_device(self, episodes, profiles) -> None:
        eps = _of_type(episodes, FraudType.IDENTITY_THEFT)
        assert len(eps) >= 20
        for evs, _ in eps:
            age = profiles[evs[0]["customer_id"]].account_age_days(_epoch(evs[0]))
            assert age is not None and 0 <= age <= NEW_ACCOUNT_DAYS

        def new_device(evs) -> bool:
            p = profiles[evs[0]["customer_id"]]
            return any(e["device_id"] and e["device_id"] not in p.devices for e in evs)

        assert _share([new_device(evs) for evs, _ in eps]) >= 0.85
        high = [evs[0]["amount"] >= 4_000 for evs, stealth in eps if not stealth]
        moderate = [evs[0]["amount"] < 2_600 for evs, stealth in eps if stealth]
        assert all(high) and all(moderate)

    def test_money_laundering_concentrates_recipients(self, episodes) -> None:
        for stealth, min_senders in ((False, 3), (True, 2)):
            eps = _of_type(episodes, FraudType.MONEY_LAUNDERING, stealth=stealth)
            assert eps
            for evs, _ in eps:
                assert len({e["destination_account"] for e in evs}) == 1
                assert all(e["amount"] < 10_000 for e in evs)
            enough = [len({e["customer_id"] for e in evs}) >= min_senders for evs, _ in eps]
            assert _share(enough) >= 0.9
        # stealth: só 2 remetentes, abaixo do limiar de concentração
        assert all(
            len({e["customer_id"] for e in evs}) <= 2
            for evs, _ in _of_type(episodes, FraudType.MONEY_LAUNDERING, stealth=True)
        )

    def test_social_engineering_uses_victims_device_and_a_new_recipient(
        self, episodes, profiles
    ) -> None:
        eps = _of_type(episodes, FraudType.SOCIAL_ENGINEERING)
        assert len(eps) >= 30
        for evs, _ in eps:
            p = profiles[evs[0]["customer_id"]]
            for e in evs:
                assert e["device_id"] is None or e["device_id"] in p.devices
                assert e["ip_address"] is None or ip_prefix(e["ip_address"]) in p.ip_prefixes
                assert e["destination_account"] not in {a for a, _ in p.contacts}
                assert e["transaction_type"] == "PIX"
        big = [evs[0]["amount"] >= 1.5 * profiles[evs[0]["customer_id"]].typical_amount
               for evs, _ in eps]  # fmt: skip
        assert _share(big) >= 0.95


# ── Hard negatives ─────────────────────────────────────────────────────────────


class TestHardNegatives:
    @pytest.fixture(scope="class")
    def rates(self, dataset) -> dict[str, float]:
        _, _, txs, truth = dataset
        counts: dict[str, int] = defaultdict(int)
        for row in truth:
            for kind in filter(None, row["hard_negative"].split(";")):
                counts[kind] += 1
        return {k: v / len(txs) for k, v in counts.items()}

    def test_hard_negatives_are_only_attached_to_legitimate_events(self, dataset) -> None:
        _, _, txs, truth = dataset
        by_id = {t["transaction_id"]: t for t in txs}
        assert all(not by_id[r["transaction_id"]]["is_fraud"] for r in truth if r["hard_negative"])

    def test_new_device_rate(self, rates) -> None:
        assert 0.015 <= rates["new_device"] <= 0.035

    def test_new_ip_rate(self, rates) -> None:
        assert 0.025 <= rates["new_ip"] <= 0.055

    def test_big_purchase_rate(self, rates) -> None:
        assert 0.006 <= rates["big_purchase"] <= 0.016

    def test_travel_rate(self, rates) -> None:
        assert 0.004 <= rates["travel"] <= 0.03

    def test_off_hours_rate(self, rates) -> None:
        assert 0.004 <= rates["off_hours"] <= 0.02

    def test_off_hours_events_are_outside_the_customers_active_hours(
        self, dataset, profiles
    ) -> None:
        _, _, txs, truth = dataset
        by_id = {t["transaction_id"]: t for t in txs}
        off = [by_id[r["transaction_id"]] for r in truth if "off_hours" in r["hard_negative"]]
        assert off
        for tx in off:
            assert local_hour(_epoch(tx)) not in profiles[tx["customer_id"]].active_hours

    def test_big_purchases_are_large_for_the_customer(self, dataset, profiles) -> None:
        _, _, txs, truth = dataset
        by_id = {t["transaction_id"]: t for t in txs}
        big = [by_id[r["transaction_id"]] for r in truth if "big_purchase" in r["hard_negative"]]
        assert big
        ratio = [t["amount"] / profiles[t["customer_id"]].typical_amount for t in big]
        assert _share([r >= 4 for r in ratio]) >= 0.9

    def test_legit_traffic_never_implies_impossible_travel(self, dataset) -> None:
        """Viagem legítima respeita a velocidade: nunca > 900 km/h entre eventos legítimos."""
        _, _, txs, _ = dataset
        legit: dict[str, list[dict]] = defaultdict(list)
        for tx in txs:
            if not tx["is_fraud"]:
                legit[tx["customer_id"]].append(tx)
        pairs = impossible = 0
        for events in legit.values():
            for prev, cur in zip(events, events[1:], strict=False):
                hours = (_epoch(cur) - _epoch(prev)) / 3600
                km = haversine_km(
                    prev["latitude"], prev["longitude"], cur["latitude"], cur["longitude"]
                )
                pairs += 1
                if km > 100 and (hours <= 0 or km / hours > 900):
                    impossible += 1
        assert pairs > 10_000
        # o único resíduo são os clones de cartão, cujo evento legítimo de contexto é fixo em casa
        assert impossible / pairs <= 0.002


# ── Streaming ──────────────────────────────────────────────────────────────────


class TestTransactionStream:
    @pytest.fixture(scope="class")
    def stream_setup(self):
        gen = DataGenerator(seed=42)
        customers = gen.generate_customers(n=300)
        gen.reseed_events(2026)
        return gen, customers

    def test_episode_probability_is_calibrated_and_monotonic(self) -> None:
        assert episode_probability(0.025) == pytest.approx(0.0145, abs=0.0005)
        assert episode_probability(0.01) < episode_probability(0.025) < episode_probability(0.1)

    def test_legit_event_comes_now_with_zero_delay(self, stream_setup) -> None:
        gen, customers = stream_setup
        stream = TransactionStream(gen, customers, fraud_rate=0.0)
        now = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)
        events = stream.next_events(now)
        assert len(events) == 1
        delay, tx = events[0]
        assert delay == 0.0
        assert datetime.fromisoformat(tx["timestamp"]) == now
        assert tx["is_fraud"] is False

    def test_constant_rate_by_default_even_at_night(self, stream_setup) -> None:
        gen, customers = stream_setup
        stream = TransactionStream(gen, customers, fraud_rate=0.0)
        night = datetime(2026, 5, 4, 7, 0, tzinfo=UTC)  # 04h em Brasília
        assert all(len(stream.next_events(night)) == 1 for _ in range(200))

    def test_diurnal_mode_follows_the_customers_active_hours(self, stream_setup) -> None:
        gen, customers = stream_setup
        stream = TransactionStream(gen, customers, fraud_rate=0.0, diurnal=True)
        midday = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)  # 12h em Brasília
        night = datetime(2026, 5, 4, 7, 0, tzinfo=UTC)  # 04h em Brasília: ninguém ativo

        def emitted(now: datetime) -> list[dict]:
            return [tx for _ in range(600) for _, tx in stream.next_events(now)]

        assert len(emitted(midday)) >= 0.85 * 600
        night_events = emitted(night)
        assert 0 < len(night_events) <= 0.15 * 600  # só o tráfego fora de horário (OFF_HOURS_RATE)

    def test_episodes_schedule_follow_ups_in_order(self, stream_setup) -> None:
        gen, customers = stream_setup
        stream = TransactionStream(gen, customers, fraud_rate=0.5)
        now = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)
        multi = []
        for _ in range(300):
            events = stream.next_events(now)
            delays = [d for d, _ in events]
            assert delays == sorted(delays)
            assert delays[0] == 0.0
            if len(events) > 1:
                multi.append(events)
        assert multi
        for events in multi:
            assert any(d > 0 for d, _ in events[1:])
            for delay, tx in events:
                assert datetime.fromisoformat(tx["timestamp"]) == now + timedelta(
                    seconds=int(delay)
                )

    def test_fraud_share_of_emitted_events_is_close_to_target(self, stream_setup) -> None:
        gen, customers = stream_setup
        stream = TransactionStream(gen, customers)
        now = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)
        emitted = [tx for _ in range(15_000) for _, tx in stream.next_events(now)]
        share = sum(tx["is_fraud"] for tx in emitted) / len(emitted)
        assert 0.015 <= share <= 0.04

    def test_every_fraud_event_validates_against_the_contract(self, stream_setup) -> None:
        gen, customers = stream_setup
        stream = TransactionStream(gen, customers, fraud_rate=0.5)
        now = datetime(2026, 5, 4, 15, 0, tzinfo=UTC)
        kinds = set()
        for _ in range(400):
            for _, tx in stream.next_events(now):
                TransactionEvent(**tx)
                if tx["is_fraud"]:
                    kinds.add(tx["fraud_type"])
        assert FraudType.ACCOUNT_TAKEOVER.value in kinds
        assert FraudType.MONEY_LAUNDERING.value in kinds

    def test_identity_theft_only_targets_recent_accounts(self, stream_setup) -> None:
        gen, customers = stream_setup
        stream = TransactionStream(gen, customers, fraud_rate=0.6)
        now = datetime.now(tz=UTC)
        checked = 0
        for _ in range(2_000):
            for _, tx in stream.next_events(now):
                if tx["fraud_type"] == FraudType.IDENTITY_THEFT.value:
                    profile = gen.profile_for(_customer_of(customers, tx))
                    age = profile.account_age_days(now.timestamp())
                    assert age is not None and 0 <= age <= NEW_ACCOUNT_DAYS
                    checked += 1
        assert checked > 0


def _customer_of(customers: list[dict], tx: dict) -> dict:
    return next(c for c in customers if c["customer_id"] == tx["customer_id"])


# ── Clientes reproduzíveis (reference_date) ────────────────────────────────────


class TestCustomersReferenceDate:
    def test_default_is_identical_to_using_today(self) -> None:
        from datetime import date

        a = DataGenerator(seed=42).generate_customers(n=200)
        b = DataGenerator(seed=42).generate_customers(n=200, reference_date=date.today())
        assert a == b

    def test_fixed_reference_date_anchors_opening_dates_and_recent_accounts(self) -> None:
        from datetime import date, timedelta

        ref = date(2026, 3, 2)
        customers = DataGenerator(seed=42).generate_customers(n=2_000, reference_date=ref)
        openings = [datetime.fromisoformat(c["account_opening_date"]).date() for c in customers]
        assert max(openings) <= ref
        assert min(openings) >= ref - timedelta(days=3653)
        recent = [o for o in openings if (ref - o).days <= NEW_ACCOUNT_DAYS]
        assert 0.02 <= len(recent) / len(customers) <= 0.07

    def test_same_fixed_date_gives_the_same_customers_any_day(self) -> None:
        from datetime import date

        ref = date(2026, 3, 2)
        a = DataGenerator(seed=9).generate_customers(n=100, reference_date=ref)
        b = DataGenerator(seed=9).generate_customers(n=100, reference_date=ref)
        assert a == b

    def test_history_constant_matches_fakers_relative_ten_years(self) -> None:
        """Trava `ACCOUNT_HISTORY_DAYS`: mantém as datas iguais às do `date_between('-10y')`."""
        from datetime import date, timedelta

        from faker import Faker

        from src.common.data_generator import ACCOUNT_HISTORY_DAYS

        relative, explicit = Faker("pt_BR"), Faker("pt_BR")
        relative.seed_instance(3)
        explicit.seed_instance(3)
        today = date.today()
        for _ in range(100):
            assert relative.date_between(start_date="-10y", end_date="today") == (
                explicit.date_between_dates(
                    date_start=today - timedelta(days=ACCOUNT_HISTORY_DAYS), date_end=today
                )
            )


# ── Ground truth do streaming (opt-in) ─────────────────────────────────────────


class TestStreamGroundTruth:
    @pytest.fixture(scope="class")
    def setup(self):
        gen = DataGenerator(seed=5)
        customers = gen.generate_customers(n=200, reference_date=datetime(2026, 3, 2).date())
        return gen, customers

    def test_off_by_default(self, setup) -> None:
        gen, customers = setup
        stream = TransactionStream(gen, customers, fraud_rate=0.5)
        now = datetime(2026, 3, 2, 15, tzinfo=UTC)
        for _ in range(100):
            stream.next_events(now)
        assert stream.ground_truth == []

    def test_every_fraud_event_is_recorded_once_with_its_episode(self, setup) -> None:
        gen, customers = setup
        stream = TransactionStream(gen, customers, fraud_rate=0.4, record_ground_truth=True)
        now = datetime(2026, 3, 2, 15, tzinfo=UTC)
        emitted = [tx for _ in range(600) for _, tx in stream.next_events(now)]
        fraud_ids = {t["transaction_id"] for t in emitted if t["is_fraud"]}
        rows = [r for r in stream.ground_truth if r["scenario"]]
        assert {r["transaction_id"] for r in rows} == fraud_ids
        assert len(rows) == len(fraud_ids)
        assert all(r["episode_id"].startswith("ep-") for r in rows)
        by_id = {t["transaction_id"]: t for t in emitted}
        assert all(by_id[r["transaction_id"]]["fraud_type"] == r["scenario"] for r in rows)

    def test_events_of_one_episode_share_the_episode_id(self, setup) -> None:
        gen, customers = setup
        stream = TransactionStream(gen, customers, fraud_rate=0.4, record_ground_truth=True)
        now = datetime(2026, 3, 2, 15, tzinfo=UTC)
        episodes: dict[str, set[str]] = {}
        for _ in range(600):
            stream.next_events(now)
        for row in stream.ground_truth:
            if row["episode_id"]:
                episodes.setdefault(row["episode_id"], set()).add(row["scenario"])
        assert episodes
        assert all(len(scenarios) == 1 for scenarios in episodes.values())  # um tipo por episódio

    def test_hard_negatives_only_on_legitimate_events(self, setup) -> None:
        gen, customers = setup
        stream = TransactionStream(gen, customers, fraud_rate=0.2, record_ground_truth=True)
        noon = datetime(2026, 3, 2, 15, tzinfo=UTC)  # 12h local: todos os clientes ativos
        night = datetime(2026, 3, 2, 7, tzinfo=UTC)  # 04h local: ninguém ativo
        emitted = [
            tx for now in (noon, night) for _ in range(1_500) for _, tx in stream.next_events(now)
        ]
        by_id = {t["transaction_id"]: t for t in emitted}
        rows = [r for r in stream.ground_truth if r["hard_negative"]]
        assert rows
        assert all(not by_id[r["transaction_id"]]["is_fraud"] for r in rows)
        kinds = {k for r in rows for k in r["hard_negative"].split(";")}
        assert {"new_device", "new_ip", "off_hours"} <= kinds

    def test_recording_does_not_change_the_events(self) -> None:
        def run(record: bool) -> list[str]:
            gen = DataGenerator(seed=5)
            customers = gen.generate_customers(n=100, reference_date=datetime(2026, 3, 2).date())
            stream = TransactionStream(gen, customers, record_ground_truth=record)
            now = datetime(2026, 3, 2, 15, tzinfo=UTC)
            return [tx["transaction_id"] for _ in range(300) for _, tx in stream.next_events(now)]

        assert run(True) == run(False)


# ── Viagens legítimas no streaming ─────────────────────────────────────────────


class TestStreamTravel:
    """No stream cada cliente transaciona a cada ~100 s; sem fases de trânsito nenhuma viagem
    aconteceria (o intervalo nunca cobre o deslocamento)."""

    @pytest.fixture(scope="class")
    def run(self):
        gen = DataGenerator(seed=1)
        customers = gen.generate_customers(n=500, reference_date=datetime(2026, 3, 2).date())
        stream = TransactionStream(gen, customers, record_ground_truth=True)
        t0 = datetime(2026, 3, 2, 12, tzinfo=UTC)
        events = [
            tx
            for i in range(30_000)
            for _, tx in stream.next_events(t0 + timedelta(seconds=i / 10))
        ]
        return stream, events

    def test_legit_travel_exists_in_the_stream(self, run) -> None:
        stream, events = run
        travel = [r for r in stream.ground_truth if "travel" in r["hard_negative"]]
        assert travel
        assert 0.001 <= len(travel) / len(events) <= 0.03

    def test_travel_events_are_legitimate_and_in_another_city(self, run) -> None:
        stream, events = run
        by_id = {t["transaction_id"]: t for t in events}
        for row in stream.ground_truth:
            if "travel" in row["hard_negative"]:
                assert not by_id[row["transaction_id"]]["is_fraud"]

    def test_legit_traffic_never_implies_impossible_travel(self, run) -> None:
        _, events = run
        legit: dict[str, list[dict]] = defaultdict(list)
        for tx in events:
            if not tx["is_fraud"]:
                legit[tx["customer_id"]].append(tx)
        pairs = impossible = 0
        for evs in legit.values():
            for prev, cur in zip(evs, evs[1:], strict=False):
                hours = (_epoch(cur) - _epoch(prev)) / 3600
                km = haversine_km(
                    prev["latitude"], prev["longitude"], cur["latitude"], cur["longitude"]
                )
                pairs += 1
                impossible += km > 100 and (hours <= 0 or km / hours > 900)
        assert pairs > 5_000
        assert impossible / pairs <= 0.001

    def test_trip_phases(self) -> None:
        from src.common.data_generator import _StreamTravel

        gen = DataGenerator(seed=2)
        profile = gen.profile_for(gen.generate_customers(n=1)[0])
        travel = _StreamTravel()
        city = {"city": "Elsewhere", "lat": 0.0, "lon": 0.0}
        travel._trips[profile.customer_id] = (city, 1_000, 5_000, 6_000)  # chega, sai, volta
        cid = profile.customer_id
        assert travel.in_transit(cid, 500) and travel.away(cid, 500)  # a caminho
        assert not travel.in_transit(cid, 2_000) and travel.away(cid, 2_000)  # lá
        assert travel.in_transit(cid, 5_500) and travel.away(cid, 5_500)  # voltando
        assert not travel.in_transit(cid, 6_000) and not travel.away(cid, 6_000)  # em casa
        assert cid not in travel._trips  # limpo

    def test_departure_event_is_at_home_and_arrival_respects_the_speed(self, monkeypatch) -> None:
        from src.common import data_generator as dg
        from src.common.customer_profile import city_distance_km

        monkeypatch.setattr(dg, "TRIP_MEAN_INTERVAL_H", 1e-9)  # começa viagem em qualquer intervalo
        gen = DataGenerator(seed=3)
        profile = gen.profile_for(gen.generate_customers(n=1)[0])
        travel = dg._StreamTravel()
        rng = gen.rng
        travel.locate(profile, 1_000, rng)  # 1º evento só registra o último instante
        lat, lon, traveling = travel.locate(profile, 1_100, rng)  # partida
        assert traveling is False
        assert haversine_km(lat, lon, profile.city["lat"], profile.city["lon"]) < 30  # em casa
        city, arrive, _, _ = travel._trips[profile.customer_id]
        needed = city_distance_km(profile.city, city) / dg.TRAVEL_SPEED_KMH * 3600
        assert arrive - 1_100 == pytest.approx(needed, abs=1)
        assert travel.in_transit(profile.customer_id, 1_101)
        assert not travel.in_transit(profile.customer_id, arrive)
        lat, lon, traveling = travel.locate(profile, arrive, rng)
        assert traveling is True
        assert haversine_km(lat, lon, city["lat"], city["lon"]) < 30

    def test_steady_state_puts_a_small_fraction_of_customers_away(self) -> None:
        from src.common.data_generator import _StreamTravel

        gen = DataGenerator(seed=4)
        profiles = [gen.profile_for(c) for c in gen.generate_customers(n=2_000)]
        travel = _StreamTravel()
        travel.seed_steady_state(profiles, 1_000_000, gen.rng)
        away = sum(travel.away(p.customer_id, 1_000_000) for p in profiles)
        assert 0.004 <= away / len(profiles) <= 0.025

    def test_customers_in_transit_emit_nothing_and_are_not_fraud_victims(self) -> None:
        gen = DataGenerator(seed=5)
        customers = gen.generate_customers(n=3, reference_date=datetime(2026, 3, 2).date())
        stream = TransactionStream(gen, customers, fraud_rate=0.3)
        now = datetime(2026, 3, 2, 15, tzinfo=UTC)
        epoch = int(now.timestamp())
        stream._travel.seeded = True  # sem o estado inicial aleatório
        city = {"city": "Elsewhere", "lat": 0.0, "lon": 0.0}
        away_ids = [customers[0]["customer_id"], customers[1]["customer_id"]]
        for cid in away_ids:  # em trânsito durante toda a janela
            stream._travel._trips[cid] = (city, epoch + 10**6, epoch + 2 * 10**6, epoch + 3 * 10**6)
        free = customers[2]["customer_id"]

        emitted = [tx for _ in range(300) for _, tx in stream.next_events(now)]
        legit = [tx for tx in emitted if not tx["is_fraud"]]
        assert legit and all(tx["customer_id"] == free for tx in legit)
        # o mesmo vale para a vítima do episódio (os cúmplices da lavagem podem ser outros clientes)
        victims = {tx["customer_id"] for tx in emitted if tx["fraud_type"] != "MONEY_LAUNDERING"}
        assert victims <= {free}
