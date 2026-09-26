"""Testes dos perfis de cliente e utilitários do gerador sintético (issue #43)."""

import random
from datetime import UTC, datetime

import pytest

from src.common.customer_profile import (
    BRAZILIAN_CITIES,
    CITY_JITTER_DEG,
    NEW_ACCOUNT_DAYS,
    build_profile,
    cities_in_range,
    city_by_name,
    city_distance_km,
    haversine_km,
    ip_prefix,
    jitter_location,
    local_hour,
    random_ip,
    random_ip_prefix,
    set_local_hour,
)


def _customer(customer_id: str = "cust-1", **overrides) -> dict:
    base = {
        "customer_id": customer_id,
        "segment": "VAREJO",
        "city": "São Paulo",
        "account_opening_date": "2020-01-15",
    }
    base.update(overrides)
    return base


class TestGeo:
    def test_haversine_sao_paulo_rio_is_about_360_km(self) -> None:
        sp, rio = city_by_name("São Paulo"), city_by_name("Rio de Janeiro")
        assert 340 <= city_distance_km(sp, rio) <= 380

    def test_haversine_of_same_point_is_zero(self) -> None:
        assert haversine_km(-23.55, -46.63, -23.55, -46.63) == pytest.approx(0.0)

    def test_unknown_city_falls_back_to_first(self) -> None:
        assert city_by_name("Atlântida") == BRAZILIAN_CITIES[0]

    def test_jitter_stays_close_to_the_city_center(self) -> None:
        rng = random.Random(1)
        city = city_by_name("Curitiba")
        for _ in range(200):
            lat, lon = jitter_location(rng, city)
            assert abs(lat - city["lat"]) <= CITY_JITTER_DEG + 1e-6
            assert abs(lon - city["lon"]) <= CITY_JITTER_DEG + 1e-6

    def test_two_events_in_the_same_city_are_never_far_apart(self) -> None:
        """Garante que legítimos consecutivos na mesma cidade nunca parecem viagem impossível."""
        rng = random.Random(2)
        city = city_by_name("Manaus")
        points = [jitter_location(rng, city) for _ in range(300)]
        worst = max(haversine_km(*points[i], *points[i + 1]) for i in range(len(points) - 1))
        assert worst < 30

    def test_cities_in_range_respects_bounds(self) -> None:
        home = city_by_name("São Paulo")
        for city in cities_in_range(home, 150, 450):
            assert 150 <= city_distance_km(home, city) <= 450
        for city in cities_in_range(home, 800):
            assert city_distance_km(home, city) >= 800

    def test_cities_in_range_falls_back_to_a_destination(self) -> None:
        home = city_by_name("Manaus")  # nenhuma cidade da lista a 150–450 km
        result = cities_in_range(home, 150, 450)
        assert len(result) == 1
        assert result[0]["city"] != home["city"]


class TestTime:
    def test_local_hour_uses_brasilia_offset(self) -> None:
        epoch = int(datetime(2026, 3, 1, 15, 0, tzinfo=UTC).timestamp())  # 12h em Brasília
        assert local_hour(epoch) == 12

    def test_set_local_hour_keeps_local_day_and_minutes(self) -> None:
        epoch = int(datetime(2026, 3, 1, 15, 42, 7, tzinfo=UTC).timestamp())  # 12:42:07 local
        moved = set_local_hour(epoch, 3)
        assert local_hour(moved) == 3
        assert (moved - epoch) == (3 - 12) * 3600  # mesmo dia local, min/seg preservados


class TestIp:
    def test_ip_prefix_is_the_first_three_octets(self) -> None:
        assert ip_prefix("21.80.46.197") == "21.80.46"

    def test_random_ip_uses_the_given_prefix(self) -> None:
        rng = random.Random(3)
        assert ip_prefix(random_ip(rng, "21.80.46")) == "21.80.46"

    def test_random_ip_prefix_avoids_the_given_ones(self) -> None:
        rng = random.Random(4)
        avoid = {random_ip_prefix(rng) for _ in range(5)}
        for _ in range(200):
            assert random_ip_prefix(rng, avoid=avoid) not in avoid


class TestBuildProfile:
    def test_profile_depends_only_on_seed_and_customer(self) -> None:
        assert build_profile(42, _customer()) == build_profile(42, _customer())

    def test_different_seed_or_customer_changes_the_profile(self) -> None:
        base = build_profile(42, _customer())
        assert build_profile(43, _customer()) != base
        assert build_profile(42, _customer("cust-2")) != base

    def test_home_city_and_opening_come_from_the_customer(self) -> None:
        profile = build_profile(42, _customer(city="Recife", account_opening_date="2026-01-10"))
        assert profile.city["city"] == "Recife"
        assert profile.account_age_days(profile.opening_epoch + 86400 * 5) == pytest.approx(5)

    def test_missing_opening_date_is_tolerated(self) -> None:
        profile = build_profile(42, _customer(account_opening_date=None))
        assert profile.opening_epoch is None
        assert profile.account_age_days(0) is None

    def test_profile_shape(self) -> None:
        profile = build_profile(42, _customer())
        assert 1 <= len(profile.devices) <= 3
        assert 1 <= len(profile.ip_prefixes) <= 3
        assert 5 <= len(profile.contacts) <= 15
        assert 0.7 <= profile.amount_sigma <= 1.0
        assert profile.typical_amount > 0
        assert set(profile.active_hours) <= set(range(24))
        assert len(profile.active_hours) >= 13

    def test_amount_scales_with_segment(self) -> None:
        def median_typical(segment: str) -> float:
            values = sorted(
                build_profile(42, _customer(f"c{i}", segment=segment)).typical_amount
                for i in range(200)
            )
            return values[len(values) // 2]

        assert median_typical("VAREJO") < median_typical("ALTA_RENDA") < median_typical("PRIVATE")

    def test_new_account_window_constant(self) -> None:
        assert NEW_ACCOUNT_DAYS == 30
