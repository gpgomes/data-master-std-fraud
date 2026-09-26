"""Testes do perfil de comportamento (`build_profiles`), com histórico montado à mão (issue #45)."""

from __future__ import annotations

import math
from datetime import date

import pytest

from src.transformation.fraud import profile as prof
from tests.unit.fraud_helpers import (
    CUSTOMER_SCHEMA,
    HISTORY_SCHEMA,
    RIO,
    SAO_PAULO,
    df_from_rows,
    event,
)


def _history(spark, rows):
    return df_from_rows(spark, rows, HISTORY_SCHEMA)


def _customers(spark, *rows):
    return df_from_rows(
        spark,
        [
            {"customer_id": cid, "segment": seg, "account_opening_date": opening}
            for cid, seg, opening in rows
        ],
        CUSTOMER_SCHEMA,
    )


def _profile(spark, history_rows, customers):
    result = prof.build_profiles(_history(spark, history_rows), customers)
    return {r["customer_id"]: r.asDict() for r in result.collect()}


def _legit(i, customer="c1", **kw):
    return {**event(i, customer, **kw), "is_fraud": False}


def _fraud(i, customer="c1", **kw):
    return {**event(i, customer, **kw), "is_fraud": True}


class TestBuildProfiles:
    def test_one_row_per_customer_including_those_without_history(self, fraud_spark) -> None:
        customers = _customers(
            fraud_spark, ("c1", "VAREJO", "2020-01-01"), ("c2", "VAREJO", "2020-01-01")
        )
        result = _profile(fraud_spark, [_legit(i) for i in range(5)], customers)
        assert set(result) == {"c1", "c2"}
        assert result["c1"]["n_history"] == 5 and result["c1"]["has_profile"] is True
        assert result["c2"]["n_history"] == 0 and result["c2"]["has_profile"] is False

    def test_has_profile_needs_the_minimum_history(self, fraud_spark) -> None:
        customers = _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01"))
        few = _profile(fraud_spark, [_legit(i) for i in range(prof.MIN_HISTORY - 1)], customers)
        enough = _profile(fraud_spark, [_legit(i) for i in range(prof.MIN_HISTORY)], customers)
        assert few["c1"]["has_profile"] is False
        assert enough["c1"]["has_profile"] is True

    def test_fraud_rows_never_shape_the_profile(self, fraud_spark) -> None:
        """Só o histórico legítimo entra: o device e a rede da fraude não viram "conhecidos"."""
        rows = [_legit(i) for i in range(4)] + [
            _fraud(90, device="dev-thief", ip="99.9.9.9", amount=50_000.0, dest="acc-mule")
            for _ in range(3)
        ]
        p = _profile(fraud_spark, rows, _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01")))[
            "c1"
        ]
        assert p["n_history"] == 4
        assert "dev-thief" not in p["known_devices"]
        assert "99.9.9" not in p["known_ip_prefixes"]
        assert p["mu_log"] == pytest.approx(
            math.log(100.0), abs=0.05
        )  # o valor de R$ 50 mil ficou de fora

    def test_known_devices_and_ip_prefixes(self, fraud_spark) -> None:
        rows = [
            _legit(1, device="dev-a", ip="10.1.1.5"),
            _legit(2, device="dev-a", ip="10.1.1.99"),  # mesma rede /24
            _legit(3, device="dev-b", ip="200.5.6.7"),
            _legit(4, device=None, ip=None),  # nulos não entram
        ]
        p = _profile(fraud_spark, rows, _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01")))[
            "c1"
        ]
        assert set(p["known_devices"]) == {"dev-a", "dev-b"}
        assert set(p["known_ip_prefixes"]) == {"10.1.1", "200.5.6"}

    def test_frequent_destinations_need_repeated_use(self, fraud_spark) -> None:
        rows = (
            [_legit(i, dest="acc-friend") for i in range(3)]
            + [_legit(10, dest="acc-once")]  # uma vez só: não é destinatário frequente
            + [_legit(20 + i, dest="acc-twice") for i in range(prof.MIN_DESTINATION_SUPPORT)]
        )
        p = _profile(fraud_spark, rows, _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01")))[
            "c1"
        ]
        assert set(p["known_destinations"]) == {"acc-friend", "acc-twice"}

    def test_known_destinations_are_capped_at_the_most_frequent(
        self, fraud_spark, monkeypatch
    ) -> None:
        monkeypatch.setattr(prof, "MAX_KNOWN_DESTINATIONS", 2)
        rows = (
            [_legit(i, dest="acc-a") for i in range(5)]
            + [_legit(10 + i, dest="acc-b") for i in range(4)]
            + [_legit(20 + i, dest="acc-c") for i in range(3)]
        )
        p = _profile(fraud_spark, rows, _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01")))[
            "c1"
        ]
        assert set(p["known_destinations"]) == {"acc-a", "acc-b"}

    def test_night_share_uses_the_local_hour(self, fraud_spark) -> None:
        # 05:00 UTC = 02:00 em Brasília (madrugada); 15:00 UTC = 12:00 local
        rows = [
            _legit(1, ts="2026-03-02T05:00:00"),
            _legit(2, ts="2026-03-02T05:30:00"),
            _legit(3, ts="2026-03-02T15:00:00"),
            _legit(4, ts="2026-03-02T16:00:00"),
        ]
        p = _profile(fraud_spark, rows, _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01")))[
            "c1"
        ]
        assert p["night_share"] == pytest.approx(0.5)

    def test_home_is_the_median_location_and_ignores_a_trip(self, fraud_spark) -> None:
        rows = [_legit(i, at=SAO_PAULO) for i in range(5)] + [_legit(9, at=RIO)]
        p = _profile(fraud_spark, rows, _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01")))[
            "c1"
        ]
        assert p["home_lat"] == pytest.approx(SAO_PAULO[0], abs=0.01)
        assert p["home_lon"] == pytest.approx(SAO_PAULO[1], abs=0.01)

    def test_amount_distribution_in_log_space_with_shrinkage(self, fraud_spark) -> None:
        # c1: 200 transações de ~R$ 100 (baseline próprio dominante). c2: nenhuma (herda o prior).
        # c3: outro segmento com valores altos, só para o prior de VAREJO ser o de R$ 100.
        rows = [_legit(i, "c1", amount=100.0 * (1.1 if i % 2 else 0.9)) for i in range(200)]
        rows += [_legit(1000 + i, "c3", amount=5_000.0) for i in range(50)]
        customers = _customers(
            fraud_spark,
            ("c1", "VAREJO", "2020-01-01"),
            ("c2", "VAREJO", "2020-01-01"),
            ("c3", "PRIVATE", "2020-01-01"),
        )
        result = _profile(fraud_spark, rows, customers)
        assert result["c1"]["mu_log"] == pytest.approx(math.log(100.0), abs=0.05)
        # sem histórico: μ é o do segmento VAREJO (só o c1 tem linhas), não o de PRIVATE
        assert result["c2"]["mu_log"] == pytest.approx(result["c1"]["mu_log"], abs=0.1)
        assert result["c2"]["mu_log"] < math.log(1_000.0)
        assert result["c3"]["mu_log"] == pytest.approx(math.log(5_000.0), abs=0.05)

    def test_few_transactions_shrink_toward_the_segment_prior(self, fraud_spark) -> None:
        prior = [_legit(i, "c1", amount=100.0) for i in range(200)]  # define o prior de VAREJO
        rows = prior + [_legit(500 + i, "c2", amount=10_000.0) for i in range(prof.MIN_HISTORY)]
        customers = _customers(
            fraud_spark, ("c1", "VAREJO", "2020-01-01"), ("c2", "VAREJO", "2020-01-01")
        )
        c2 = _profile(fraud_spark, rows, customers)["c2"]
        raw = math.log(10_000.0)
        assert c2["mu_log"] < raw  # puxado para o prior
        assert c2["mu_log"] > math.log(100.0)  # mas não descartou o que viu

    def test_sigma_has_a_floor(self, fraud_spark) -> None:
        rows = [_legit(i, amount=100.0) for i in range(20)]  # valor idêntico: σ observado = 0
        p = _profile(fraud_spark, rows, _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01")))[
            "c1"
        ]
        assert p["sigma_log"] >= prof.SIGMA_FLOOR

    def test_account_opening_date_comes_from_the_customer(self, fraud_spark) -> None:
        customers = _customers(fraud_spark, ("c1", "VAREJO", "2026-02-20"))
        p = _profile(fraud_spark, [_legit(1)], customers)["c1"]
        assert p["account_opening_date"] == date(2026, 2, 20)

    def test_empty_history_still_returns_every_customer(self, fraud_spark) -> None:
        customers = _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01"))
        result = prof.build_profiles(
            _history(fraud_spark, [_legit(1, "other")]), customers
        ).collect()
        assert [r["customer_id"] for r in result] == ["c1"]
        assert result[0]["has_profile"] is False
        assert result[0]["known_devices"] == []

    def test_output_columns_are_the_documented_ones(self, fraud_spark) -> None:
        customers = _customers(fraud_spark, ("c1", "VAREJO", "2020-01-01"))
        df = prof.build_profiles(_history(fraud_spark, [_legit(1)]), customers)
        assert tuple(df.columns) == prof.PROFILE_COLUMNS
