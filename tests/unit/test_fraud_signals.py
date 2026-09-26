"""Testes dos sinais do Fraud Engine: um micro-dataset montado à mão por sinal (issue #45)."""

from __future__ import annotations

import math
from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from src.common.customer_profile import haversine_km as python_haversine
from src.transformation.fraud import signals as sg
from src.transformation.fraud.signals import SIGNALS, compute_signals, signal_column
from tests.unit.fraud_helpers import (
    LABELLED_EVENT_SCHEMA,
    RIO,
    SALVADOR,
    SAO_PAULO,
    df_from_rows,
    event,
    events_df,
    profile_df,
    profile_row,
)


def _signal(spark, name, rows, *profiles, recent=None):
    """Valor do sinal `name` por transaction_id."""
    result = compute_signals(events_df(spark, rows), profile_df(spark, *profiles), recent)
    col = signal_column(name)
    return {r["transaction_id"]: r[col] for r in result.select("transaction_id", col).collect()}


class TestAmountAnomaly:
    """z = (ln(valor) − μ) / σ; 0 até Z_LOW, 1 a partir de Z_HIGH, linear no meio."""

    def _at_z(self, z: float) -> float:
        return 100.0 * math.exp(0.5 * z)  # μ = ln 100 e σ = 0,5 no perfil de teste

    def test_typical_amount_is_zero(self, fraud_spark) -> None:
        out = _signal(fraud_spark, sg.AMOUNT_ANOMALY, [event(1, amount=100.0)])
        assert out["t1"] == pytest.approx(0.0)

    def test_grows_linearly_between_the_two_thresholds(self, fraud_spark) -> None:
        rows = [event(i, amount=self._at_z(z)) for i, z in enumerate([1.5, 2.75, 4.0], start=1)]
        out = _signal(fraud_spark, sg.AMOUNT_ANOMALY, rows)
        assert out["t1"] == pytest.approx(0.0, abs=1e-6)
        assert out["t2"] == pytest.approx(0.5, abs=1e-6)  # meio do caminho
        assert out["t3"] == pytest.approx(1.0, abs=1e-6)

    def test_saturates_at_one_and_ignores_a_low_amount(self, fraud_spark) -> None:
        rows = [event(1, amount=1_000_000.0), event(2, amount=1.0)]
        out = _signal(fraud_spark, sg.AMOUNT_ANOMALY, rows)
        assert out["t1"] == 1.0
        assert out["t2"] == 0.0  # valor baixo não é anomalia

    def test_customer_without_profile_row_gives_no_evidence(self, fraud_spark) -> None:
        out = _signal(fraud_spark, sg.AMOUNT_ANOMALY, [event(1, "ghost", amount=1e9)])
        assert out["t1"] == 0.0

    def test_uses_the_prior_when_the_customer_has_no_history(self, fraud_spark) -> None:
        """`has_profile` falso ainda traz μ/σ do prior do segmento: o valor alto vale como sinal."""
        weak = profile_row(has_profile=False, n_history=0)
        out = _signal(fraud_spark, sg.AMOUNT_ANOMALY, [event(1, amount=self._at_z(5.0))], weak)
        assert out["t1"] == 1.0


class TestTxVelocity:
    def _burst(self, n_before: int, gap_s: int = 30):
        base = "2026-03-02T12:{m:02d}:{s:02d}"
        rows = [
            event(i, ts=base.format(m=(i * gap_s) // 60, s=(i * gap_s) % 60))
            for i in range(n_before)
        ]
        last_s = n_before * gap_s
        rows.append(event("last", ts=base.format(m=last_s // 60, s=last_s % 60)))
        return rows

    def test_fires_at_the_minimum_transactions_in_ten_minutes(self, fraud_spark) -> None:
        out = _signal(fraud_spark, sg.TX_VELOCITY, self._burst(sg.VELOCITY_MIN_TX))
        assert out["tlast"] == 1.0

    def test_does_not_fire_one_short_of_the_minimum(self, fraud_spark) -> None:
        out = _signal(fraud_spark, sg.TX_VELOCITY, self._burst(sg.VELOCITY_MIN_TX - 1))
        assert out["tlast"] == 0.0

    def test_older_transactions_leave_the_window(self, fraud_spark) -> None:
        # 10 eventos espaçados de 2 min: só ~5 cabem em qualquer janela de 10 min
        rows = [event(i, ts=f"2026-03-02T12:{2 * i:02d}:00") for i in range(10)]
        rows.append(event("last", ts="2026-03-02T12:20:00"))
        assert _signal(fraud_spark, sg.TX_VELOCITY, rows)["tlast"] == 0.0

    def test_counts_only_the_same_customer(self, fraud_spark) -> None:
        rows = [event(i, "other", ts=f"2026-03-02T12:00:{i:02d}") for i in range(20)]
        rows.append(event("mine", "c1", ts="2026-03-02T12:01:00"))
        out = _signal(fraud_spark, sg.TX_VELOCITY, rows, profile_row("c1"), profile_row("other"))
        assert out["tmine"] == 0.0


class TestNewDevice:
    def test_known_new_and_missing_device(self, fraud_spark) -> None:
        rows = [
            event(1, device="dev-known"),
            event(2, device="dev-thief"),
            event(3, device=None),  # ATM: sem device é ausência de evidência
        ]
        out = _signal(fraud_spark, sg.NEW_DEVICE, rows)
        assert (out["t1"], out["t2"], out["t3"]) == (0.0, 1.0, 0.0)

    def test_needs_a_profile(self, fraud_spark) -> None:
        weak = profile_row(has_profile=False, known_devices=[])
        assert _signal(fraud_spark, sg.NEW_DEVICE, [event(1, device="dev-x")], weak)["t1"] == 0.0

    def test_unknown_customer_gives_no_evidence(self, fraud_spark) -> None:
        assert _signal(fraud_spark, sg.NEW_DEVICE, [event(1, "ghost", device="dev-x")])["t1"] == 0.0


class TestNewIp:
    def test_uses_the_24_prefix(self, fraud_spark) -> None:
        rows = [
            event(1, ip="10.1.1.7"),  # rede conhecida
            event(2, ip="10.1.1.200"),  # mesma /24, outro host
            event(3, ip="10.1.2.7"),  # /24 diferente
            event(4, ip=None),
        ]
        out = _signal(fraud_spark, sg.NEW_IP, rows)
        assert (out["t1"], out["t2"], out["t3"], out["t4"]) == (0.0, 0.0, 1.0, 0.0)

    def test_needs_a_profile(self, fraud_spark) -> None:
        weak = profile_row(has_profile=False, known_ip_prefixes=[])
        assert _signal(fraud_spark, sg.NEW_IP, [event(1, ip="1.2.3.4")], weak)["t1"] == 0.0


class TestGeoVelocity:
    """Viagem impossível: nenhum dos últimos 5 eventos é origem plausível (≤ 900 km/h, > 100 km)."""

    def test_far_from_every_recent_event_is_impossible(self, fraud_spark) -> None:
        home = [event(i, ts=f"2026-03-02T12:0{i}:00", at=SAO_PAULO) for i in range(3)]
        clone = event("far", ts="2026-03-02T12:05:00", at=SALVADOR)  # 1.450 km em 3 min
        out = _signal(fraud_spark, sg.GEO_VELOCITY, home + [clone])
        assert out["tfar"] == 1.0
        assert out["t2"] == 0.0

    def test_one_plausible_neighbour_is_enough_to_pass(self, fraud_spark) -> None:
        """O evento legítimo logo depois de uma fraude em outra cidade não vira falso positivo."""
        rows = [
            event(1, ts="2026-03-02T12:00:00", at=SAO_PAULO),
            event(2, ts="2026-03-02T12:01:00", at=SAO_PAULO),
            event(3, ts="2026-03-02T12:02:00", at=SALVADOR),  # a fraude em outra cidade
            event(4, ts="2026-03-02T12:03:00", at=SAO_PAULO),  # o cliente, de volta ao normal
        ]
        out = _signal(fraud_spark, sg.GEO_VELOCITY, rows)
        assert out["t3"] == 1.0
        assert out["t4"] == 0.0

    def test_enough_time_to_travel_is_plausible(self, fraud_spark) -> None:
        # São Paulo → Rio (~360 km) em 2 h: 180 km/h
        rows = [
            event(1, ts="2026-03-02T10:00:00", at=SAO_PAULO),
            event(2, ts="2026-03-02T12:00:00", at=RIO),
        ]
        assert _signal(fraud_spark, sg.GEO_VELOCITY, rows)["t2"] == 0.0

    def test_short_distances_are_never_impossible(self, fraud_spark) -> None:
        near = (SAO_PAULO[0] + 0.05, SAO_PAULO[1])  # ~5 km
        rows = [
            event(1, ts="2026-03-02T12:00:00", at=SAO_PAULO),
            event(2, ts="2026-03-02T12:00:01", at=near),
        ]
        assert _signal(fraud_spark, sg.GEO_VELOCITY, rows)["t2"] == 0.0

    def test_no_previous_event_is_no_evidence(self, fraud_spark) -> None:
        assert _signal(fraud_spark, sg.GEO_VELOCITY, [event(1, at=SALVADOR)])["t1"] == 0.0

    def test_events_older_than_the_window_do_not_count(self, fraud_spark) -> None:
        rows = [
            event(1, ts="2026-03-02T00:00:00", at=SAO_PAULO),
            event(2, ts="2026-03-02T12:00:00", at=SALVADOR),  # 12 h depois: fora da janela de 6 h
        ]
        assert _signal(fraud_spark, sg.GEO_VELOCITY, rows)["t2"] == 0.0

    def test_same_instant_in_two_cities_is_impossible(self, fraud_spark) -> None:
        rows = [
            event(1, ts="2026-03-02T12:00:00", at=SAO_PAULO),
            event(2, ts="2026-03-02T12:00:00", at=SALVADOR),
        ]
        assert _signal(fraud_spark, sg.GEO_VELOCITY, rows)["t2"] == 1.0

    def test_missing_coordinates_give_no_evidence(self, fraud_spark) -> None:
        rows = [
            event(1, ts="2026-03-02T12:00:00", at=SAO_PAULO),
            event(2, ts="2026-03-02T12:01:00", at=None),
        ]
        assert _signal(fraud_spark, sg.GEO_VELOCITY, rows)["t2"] == 0.0


class TestGeoFarFromHome:
    def test_distance_to_the_profile_home(self, fraud_spark) -> None:
        rows = [
            event(1, at=SAO_PAULO),
            event(2, at=RIO),  # ~360 km: dentro
            event(3, at=SALVADOR),  # ~1.450 km: longe
            event(4, at=None),
        ]
        out = _signal(fraud_spark, sg.GEO_FAR_FROM_HOME, rows)
        assert (out["t1"], out["t2"], out["t3"], out["t4"]) == (0.0, 0.0, 1.0, 0.0)

    def test_needs_a_profile(self, fraud_spark) -> None:
        weak = profile_row(has_profile=False)
        assert (
            _signal(fraud_spark, sg.GEO_FAR_FROM_HOME, [event(1, at=SALVADOR)], weak)["t1"] == 0.0
        )


class TestUnusualHour:
    def test_night_for_a_customer_who_never_transacts_at_night(self, fraud_spark) -> None:
        rows = [
            event(1, ts="2026-03-02T06:00:00"),  # 03:00 local
            event(2, ts="2026-03-02T15:00:00"),  # 12:00 local
        ]
        out = _signal(fraud_spark, sg.UNUSUAL_HOUR, rows)
        assert (out["t1"], out["t2"]) == (1.0, 0.0)

    def test_a_night_owl_is_not_flagged(self, fraud_spark) -> None:
        owl = profile_row(night_share=0.20)
        out = _signal(fraud_spark, sg.UNUSUAL_HOUR, [event(1, ts="2026-03-02T06:00:00")], owl)
        assert out["t1"] == 0.0

    def test_the_boundary_hours(self, fraud_spark) -> None:
        rows = [
            event(1, ts="2026-03-02T08:59:00"),  # 05:59 local: ainda madrugada
            event(2, ts="2026-03-02T09:00:00"),  # 06:00 local: já não
        ]
        out = _signal(fraud_spark, sg.UNUSUAL_HOUR, rows)
        assert (out["t1"], out["t2"]) == (1.0, 0.0)

    def test_needs_a_profile(self, fraud_spark) -> None:
        weak = profile_row(has_profile=False)
        assert (
            _signal(fraud_spark, sg.UNUSUAL_HOUR, [event(1, ts="2026-03-02T06:00:00")], weak)["t1"]
            == 0.0
        )


class TestNewDestination:
    def test_known_new_and_missing_destination(self, fraud_spark) -> None:
        rows = [event(1, dest="acc-known"), event(2, dest="acc-mule"), event(3, dest=None)]
        out = _signal(fraud_spark, sg.NEW_DESTINATION, rows)
        assert (out["t1"], out["t2"], out["t3"]) == (0.0, 1.0, 0.0)

    def test_needs_a_profile(self, fraud_spark) -> None:
        weak = profile_row(has_profile=False, known_destinations=[])
        assert _signal(fraud_spark, sg.NEW_DESTINATION, [event(1, dest="acc-x")], weak)["t1"] == 0.0


class TestRecipientConcentration:
    """Remetentes distintos para a mesma conta-destino na última hora (entre clientes)."""

    def _senders(self, n: int, spacing_s: int = 60):
        return [
            event(
                f"s{i}",
                f"sender{i}",
                ts=f"2026-03-02T12:{(i * spacing_s) // 60:02d}:{(i * spacing_s) % 60:02d}",
                dest="acc-mule",
            )
            for i in range(n)
        ]

    def _profiles(self, n: int):
        return [profile_row(f"sender{i}") for i in range(n)]

    def test_third_distinct_sender_triggers_it_but_not_the_first_two(self, fraud_spark) -> None:
        out = _signal(fraud_spark, sg.RECIPIENT_CONCENTRATION, self._senders(4), *self._profiles(4))
        assert [out[f"ts{i}"] for i in range(4)] == [0.0, 0.0, 1.0, 1.0]

    def test_the_same_customer_paying_repeatedly_does_not_count(self, fraud_spark) -> None:
        rows = [event(i, "c1", ts=f"2026-03-02T12:0{i}:00", dest="acc-mule") for i in range(5)]
        assert set(_signal(fraud_spark, sg.RECIPIENT_CONCENTRATION, rows).values()) == {0.0}

    def test_senders_outside_the_hour_do_not_count(self, fraud_spark) -> None:
        rows = [
            event("a", "s0", ts="2026-03-02T10:00:00", dest="acc-mule"),
            event("b", "s1", ts="2026-03-02T10:10:00", dest="acc-mule"),
            event("c", "s2", ts="2026-03-02T12:00:00", dest="acc-mule"),  # 2 h depois dos outros
        ]
        out = _signal(fraud_spark, sg.RECIPIENT_CONCENTRATION, rows, *self._profiles(3))
        assert out["tc"] == 0.0

    def test_different_destinations_do_not_add_up(self, fraud_spark) -> None:
        rows = [event(i, f"s{i}", ts=f"2026-03-02T12:0{i}:00", dest=f"acc-{i}") for i in range(5)]
        out = _signal(fraud_spark, sg.RECIPIENT_CONCENTRATION, rows, *self._profiles(5))
        assert set(out.values()) == {0.0}

    def test_missing_destination_is_never_a_signal(self, fraud_spark) -> None:
        rows = [event(i, f"s{i}", ts=f"2026-03-02T12:0{i}:00", dest=None) for i in range(5)]
        out = _signal(fraud_spark, sg.RECIPIENT_CONCENTRATION, rows, *self._profiles(5))
        assert set(out.values()) == {0.0}


class TestAccountAgeLow:
    def test_new_account_versus_old_account(self, fraud_spark) -> None:
        rows = [
            event(1, "new", ts="2026-03-02T12:00:00"),
            event(2, "old", ts="2026-03-02T12:00:00"),
        ]
        new = profile_row("new", account_opening_date=date(2026, 2, 20).isoformat())  # 10 dias
        old = profile_row("old", account_opening_date=date(2026, 1, 1).isoformat())  # 60 dias
        out = _signal(fraud_spark, sg.ACCOUNT_AGE_LOW, rows, new, old)
        assert (out["t1"], out["t2"]) == (1.0, 0.0)

    def test_the_30_day_boundary(self, fraud_spark) -> None:
        opened = date(2026, 2, 1)
        rows = [
            event(1, "a", ts="2026-03-02T12:00:00"),  # 29 dias: conta nova
            event(2, "b", ts="2026-03-03T12:00:00"),  # 30 dias: já não
        ]
        p = [profile_row(c, account_opening_date=opened.isoformat()) for c in ("a", "b")]
        out = _signal(fraud_spark, sg.ACCOUNT_AGE_LOW, rows, *p)
        assert (out["t1"], out["t2"]) == (1.0, 0.0)

    def test_event_before_the_account_exists_is_not_a_signal(self, fraud_spark) -> None:
        future = profile_row(account_opening_date=date(2026, 6, 1).isoformat())
        assert _signal(fraud_spark, sg.ACCOUNT_AGE_LOW, [event(1)], future)["t1"] == 0.0

    def test_unknown_opening_date_gives_no_evidence(self, fraud_spark) -> None:
        p = profile_row(account_opening_date=None)
        assert _signal(fraud_spark, sg.ACCOUNT_AGE_LOW, [event(1)], p)["t1"] == 0.0


class TestHaversineParity:
    """O haversine em `Column` tem de dar o mesmo número que o do gerador, em Python."""

    def test_matches_the_python_implementation(self, fraud_spark) -> None:
        pairs = [(SAO_PAULO, RIO), (SAO_PAULO, SALVADOR), (RIO, SALVADOR), (SAO_PAULO, SAO_PAULO)]
        schema = StructType(
            [StructField(n, DoubleType(), True) for n in ("lat1", "lon1", "lat2", "lon2")]
        )
        df = df_from_rows(
            fraud_spark,
            [{"lat1": a[0], "lon1": a[1], "lat2": b[0], "lon2": b[1]} for a, b in pairs],
            schema,
        )
        rows = df.select(
            sg.haversine_km(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")).alias("km")
        ).collect()
        for row, (a, b) in zip(rows, pairs, strict=True):
            assert row["km"] == pytest.approx(python_haversine(a[0], a[1], b[0], b[1]), rel=1e-9)


class TestContractOfTheFunction:
    def test_one_column_per_signal_in_the_documented_order(self, fraud_spark) -> None:
        result = compute_signals(events_df(fraud_spark, [event(1)]), profile_df(fraud_spark))
        assert result.columns == [
            *sg.EVENT_COLUMNS,
            "has_profile",
            *[signal_column(s) for s in SIGNALS],
        ]

    def test_all_signals_are_between_zero_and_one(self, fraud_spark) -> None:
        rows = [
            event(i, amount=10.0**i, device=f"d{i}", ip=f"9.9.{i}.1", dest=f"a{i}")
            for i in range(1, 7)
        ]
        result = compute_signals(events_df(fraud_spark, rows), profile_df(fraud_spark))
        for name in SIGNALS:
            values = [r[0] for r in result.select(signal_column(name)).collect()]
            assert all(0.0 <= v <= 1.0 for v in values), name

    def test_label_columns_are_ignored_and_never_reach_the_output(self, fraud_spark) -> None:
        rows = [event(1, device="dev-thief"), event(2)]
        plain = compute_signals(events_df(fraud_spark, rows), profile_df(fraud_spark))
        labelled_rows = [
            {**r, "is_fraud": r["device_id"] == "dev-thief", "fraud_type": "ACCOUNT_TAKEOVER"}
            for r in rows
        ]
        labelled_df = df_from_rows(fraud_spark, labelled_rows, LABELLED_EVENT_SCHEMA)
        labelled = compute_signals(labelled_df, profile_df(fraud_spark))
        assert "is_fraud" not in labelled.columns and "fraud_type" not in labelled.columns
        key = lambda r: r["transaction_id"]  # noqa: E731
        assert sorted(plain.collect(), key=key) == sorted(labelled.collect(), key=key)

    def test_recent_events_give_context_but_are_not_returned(self, fraud_spark) -> None:
        """O streaming passa o estado curto em `recent`: ele alimenta as janelas e não sai."""
        recent = events_df(
            fraud_spark,
            [event(i, ts=f"2026-03-02T12:0{i}:00", at=SAO_PAULO) for i in range(3)],
        )
        current = [event("far", ts="2026-03-02T12:05:00", at=SALVADOR)]
        with_context = compute_signals(
            events_df(fraud_spark, current), profile_df(fraud_spark), recent
        )
        rows = with_context.collect()
        assert [r["transaction_id"] for r in rows] == ["tfar"]
        assert rows[0][signal_column(sg.GEO_VELOCITY)] == 1.0
        alone = compute_signals(events_df(fraud_spark, current), profile_df(fraud_spark)).collect()
        assert alone[0][signal_column(sg.GEO_VELOCITY)] == 0.0  # sem contexto não há de onde vir
