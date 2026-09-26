"""Testes do score noisy-OR e das regras de tipo de fraude (issue #45)."""

from __future__ import annotations

import numpy as np
import pytest
from pyspark.sql.types import DoubleType, StructField, StructType

from src.common.schemas import FraudType
from src.transformation.fraud import scoring
from src.transformation.fraud import signals as sg
from src.transformation.fraud.fraud_type import predict_fraud_type
from src.transformation.fraud.signals import SIGNALS, signal_column
from tests.unit.fraud_helpers import df_from_rows

_SIGNAL_SCHEMA = StructType([StructField(signal_column(s), DoubleType(), True) for s in SIGNALS])


def _signals_df(spark, rows: list[dict[str, float]]):
    """Um DataFrame de colunas `sig_*` a partir de {nome do sinal: valor}; o resto é zero."""
    full = [{signal_column(s): r.get(s, 0.0) for s in SIGNALS} for r in rows]
    return df_from_rows(spark, full, _SIGNAL_SCHEMA)


def _zero_weights(**weights: float) -> dict[str, float]:
    return {**{s: 0.0 for s in SIGNALS}, **weights}


class TestNoisyOr:
    def test_no_active_signal_scores_zero(self, fraud_spark) -> None:
        df = _signals_df(fraud_spark, [{}])
        row = df.select(scoring.noisy_or(_zero_weights(NEW_DEVICE=0.9)).alias("s")).first()
        assert row["s"] == pytest.approx(0.0)

    def test_a_single_active_signal_scores_its_weight(self, fraud_spark) -> None:
        df = _signals_df(fraud_spark, [{sg.NEW_DEVICE: 1.0}])
        row = df.select(scoring.noisy_or(_zero_weights(NEW_DEVICE=0.6)).alias("s")).first()
        assert row["s"] == pytest.approx(0.6)

    def test_two_signals_combine_as_independent_evidence(self, fraud_spark) -> None:
        df = _signals_df(fraud_spark, [{sg.NEW_DEVICE: 1.0, sg.NEW_IP: 1.0}])
        w = _zero_weights(NEW_DEVICE=0.6, NEW_IP=0.5)
        row = df.select(scoring.noisy_or(w).alias("s")).first()
        assert row["s"] == pytest.approx(1 - (1 - 0.6) * (1 - 0.5))  # 0,8

    def test_a_graded_signal_scales_the_weight(self, fraud_spark) -> None:
        df = _signals_df(fraud_spark, [{sg.AMOUNT_ANOMALY: 0.5}])
        row = df.select(scoring.noisy_or(_zero_weights(AMOUNT_ANOMALY=0.8)).alias("s")).first()
        assert row["s"] == pytest.approx(0.4)

    def test_zero_weight_signals_are_ignored(self, fraud_spark) -> None:
        df = _signals_df(fraud_spark, [{s: 1.0 for s in SIGNALS}])
        row = df.select(scoring.noisy_or(_zero_weights(NEW_DEVICE=0.3)).alias("s")).first()
        assert row["s"] == pytest.approx(0.3)

    def test_missing_weight_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="pesos faltando"):
            scoring.noisy_or({"NEW_DEVICE": 0.5})

    def test_score_stays_in_the_unit_interval(self, fraud_spark) -> None:
        rng = np.random.default_rng(0)
        rows = [
            dict(zip(SIGNALS, map(float, rng.random(len(SIGNALS))), strict=True)) for _ in range(50)
        ]
        w = {s: float(rng.random()) for s in SIGNALS}
        scores = [
            r["s"]
            for r in _signals_df(fraud_spark, rows).select(scoring.noisy_or(w).alias("s")).collect()
        ]
        assert all(0.0 <= s <= 1.0 for s in scores)

    def test_an_extra_signal_never_lowers_the_score(self) -> None:
        rng = np.random.default_rng(1)
        w = {s: float(rng.random()) for s in SIGNALS}
        base = rng.integers(0, 2, size=(200, len(SIGNALS))).astype(float)
        for j in range(len(SIGNALS)):
            more = base.copy()
            more[:, j] = 1.0
            assert (scoring.noisy_or_np(more, w) >= scoring.noisy_or_np(base, w) - 1e-12).all()

    def test_spark_and_numpy_give_the_same_score(self, fraud_spark) -> None:
        """A calibração usa a versão em numpy: as duas contas têm de coincidir."""
        rng = np.random.default_rng(2)
        matrix = rng.random((300, len(SIGNALS)))
        weights = {s: float(round(rng.random(), 2)) for s in SIGNALS}
        rows = [dict(zip(SIGNALS, map(float, m), strict=True)) for m in matrix]
        spark_scores = np.array(
            [
                r["s"]
                for r in _signals_df(fraud_spark, rows)
                .select(scoring.noisy_or(weights).alias("s"))
                .collect()
            ]
        )
        # a ordem das linhas do DataFrame lido do arquivo é a do arquivo
        np.testing.assert_allclose(spark_scores, scoring.noisy_or_np(matrix, weights), atol=1e-12)

    def test_numpy_accepts_a_dict_or_a_vector(self) -> None:
        matrix = np.eye(len(SIGNALS))
        w = {s: 0.1 * (i + 1) for i, s in enumerate(SIGNALS)}
        np.testing.assert_allclose(
            scoring.noisy_or_np(matrix, w), scoring.noisy_or_np(matrix, [w[s] for s in SIGNALS])
        )


class TestActiveSignals:
    def test_lists_the_active_signals_in_the_canonical_order(self, fraud_spark) -> None:
        df = _signals_df(
            fraud_spark, [{sg.NEW_IP: 1.0, sg.AMOUNT_ANOMALY: 0.9, sg.NEW_DEVICE: 1.0}]
        )
        row = df.select(scoring.active_signals().alias("a")).first()
        assert row["a"] == [sg.AMOUNT_ANOMALY, sg.NEW_DEVICE, sg.NEW_IP]  # ordem de SIGNALS

    def test_graded_signal_is_active_from_one_half(self, fraud_spark) -> None:
        df = _signals_df(fraud_spark, [{sg.AMOUNT_ANOMALY: 0.49}, {sg.AMOUNT_ANOMALY: 0.5}])
        rows = df.select(scoring.active_signals().alias("a")).collect()
        assert rows[0]["a"] == []
        assert rows[1]["a"] == [sg.AMOUNT_ANOMALY]


def _type(spark, *active: str, graded: dict[str, float] | None = None) -> str | None:
    row = {s: 1.0 for s in active} | (graded or {})
    return _signals_df(spark, [row]).select(predict_fraud_type().alias("t")).first()["t"]


class TestFraudTypeRules:
    """A tabela de regras da issue, uma linha de teste por regra e por prioridade."""

    def test_account_takeover_needs_a_new_device_plus_a_new_network_or_a_far_location(
        self, fraud_spark
    ) -> None:
        assert _type(fraud_spark, sg.NEW_DEVICE, sg.NEW_IP) == "ACCOUNT_TAKEOVER"
        assert _type(fraud_spark, sg.NEW_DEVICE, sg.GEO_FAR_FROM_HOME) == "ACCOUNT_TAKEOVER"
        assert _type(fraud_spark, sg.NEW_DEVICE) is None
        assert _type(fraud_spark, sg.NEW_IP) is None

    def test_card_cloning_is_impossible_travel_without_a_new_device(self, fraud_spark) -> None:
        assert _type(fraud_spark, sg.GEO_VELOCITY) == "CARD_CLONING"
        assert _type(fraud_spark, sg.GEO_VELOCITY, sg.NEW_DEVICE) is None  # nem clone, nem ATO

    def test_identity_theft_needs_a_young_account_and_a_new_device_or_a_big_amount(
        self, fraud_spark
    ) -> None:
        assert _type(fraud_spark, sg.ACCOUNT_AGE_LOW, sg.NEW_DEVICE) == "IDENTITY_THEFT"
        assert _type(fraud_spark, sg.ACCOUNT_AGE_LOW, sg.AMOUNT_ANOMALY) == "IDENTITY_THEFT"
        assert _type(fraud_spark, sg.ACCOUNT_AGE_LOW) is None
        assert _type(fraud_spark, sg.NEW_DEVICE) is None  # conta velha, só device novo

    def test_money_laundering_by_concentration_or_by_new_recipient_with_velocity(
        self, fraud_spark
    ) -> None:
        assert _type(fraud_spark, sg.RECIPIENT_CONCENTRATION) == "MONEY_LAUNDERING"
        assert _type(fraud_spark, sg.NEW_DESTINATION, sg.TX_VELOCITY) == "MONEY_LAUNDERING"
        assert _type(fraud_spark, sg.NEW_DESTINATION) is None

    def test_social_engineering_is_a_new_recipient_and_a_big_amount_on_known_device_and_network(
        self, fraud_spark
    ) -> None:
        assert _type(fraud_spark, sg.NEW_DESTINATION, sg.AMOUNT_ANOMALY) == "SOCIAL_ENGINEERING"
        assert _type(fraud_spark, sg.NEW_DESTINATION, sg.AMOUNT_ANOMALY, sg.NEW_DEVICE) is None
        assert _type(fraud_spark, sg.NEW_DESTINATION, sg.AMOUNT_ANOMALY, sg.NEW_IP) is None

    def test_no_signal_means_no_type(self, fraud_spark) -> None:
        assert _type(fraud_spark) is None

    def test_priority_follows_the_table(self, fraud_spark) -> None:
        # ATO vence clone, identidade e lavagem
        assert (
            _type(
                fraud_spark,
                sg.NEW_DEVICE,
                sg.NEW_IP,
                sg.GEO_VELOCITY,
                sg.RECIPIENT_CONCENTRATION,
                sg.ACCOUNT_AGE_LOW,
            )
            == "ACCOUNT_TAKEOVER"
        )
        # clone vence identidade e lavagem
        assert (
            _type(
                fraud_spark,
                sg.GEO_VELOCITY,
                sg.ACCOUNT_AGE_LOW,
                sg.AMOUNT_ANOMALY,
                sg.RECIPIENT_CONCENTRATION,
            )
            == "CARD_CLONING"
        )
        # identidade vence lavagem
        assert (
            _type(fraud_spark, sg.ACCOUNT_AGE_LOW, sg.NEW_DEVICE, sg.RECIPIENT_CONCENTRATION)
            == "IDENTITY_THEFT"
        )
        # lavagem vence engenharia social
        assert (
            _type(fraud_spark, sg.RECIPIENT_CONCENTRATION, sg.NEW_DESTINATION, sg.AMOUNT_ANOMALY)
            == "MONEY_LAUNDERING"
        )

    def test_a_weak_graded_signal_does_not_count(self, fraud_spark) -> None:
        assert _type(fraud_spark, sg.NEW_DESTINATION, graded={sg.AMOUNT_ANOMALY: 0.49}) is None
        assert (
            _type(fraud_spark, sg.NEW_DESTINATION, graded={sg.AMOUNT_ANOMALY: 0.5})
            == "SOCIAL_ENGINEERING"
        )

    def test_only_documented_fraud_types_are_ever_returned(self, fraud_spark) -> None:
        rng = np.random.default_rng(3)
        rows = [
            dict(zip(SIGNALS, map(float, r), strict=True))
            for r in rng.integers(0, 2, size=(500, len(SIGNALS)))
        ]
        types = {
            r["t"]
            for r in _signals_df(fraud_spark, rows)
            .select(predict_fraud_type().alias("t"))
            .collect()
        }
        assert types <= {*(t.value for t in FraudType), None}

    def test_type_is_computed_from_signals_only(self, fraud_spark) -> None:
        """Não há coluna de rótulo na entrada: a regra só consegue olhar os sinais."""
        df = _signals_df(fraud_spark, [{sg.GEO_VELOCITY: 1.0}])
        assert all(c.startswith("sig_") for c in df.columns)
        assert df.select(predict_fraud_type().alias("t")).first()["t"] == "CARD_CLONING"
