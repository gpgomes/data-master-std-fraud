"""Expectation suites para Bronze, Silver e Gold (issue #13).

Convenções adotadas em todas as suites, deliberadas (não default do GX):

- `expect_column_to_exist` em vez de `expect_table_columns_to_match_set` para
  schema: um `exact_match` quebraria a cada coluna de auditoria nova (já
  aconteceu neste projeto — `produced_at`/`source_system` na issue #8,
  `valid_from`/`batch_id` no SCD2 de clientes). Checar existência das colunas
  que a suite realmente usa é suficiente para pegar remoção/renomeação sem
  ser frágil a adições aditivas.
- Unicidade de `transaction_id` só é exigida em Silver/Gold, não em Bronze —
  Bronze pode legitimamente ter duplicatas entre arquivos de ingestão
  sobrepostos; é exatamente por isso que o dedup existe em
  `bronze_to_silver.py`. Uma expectativa de unicidade em Bronze estaria
  testando um invariante que o pipeline nunca prometeu.
- A regra `fraud_type` não-nulo quando `is_fraud=true` espelha o
  `field_validator` já existente em `TransactionEvent` (schemas.py) — mesma
  regra de negócio, agora também verificada nos dados já persistidos.
- "Freshness" é checada em `ingestion_timestamp`/`processing_timestamp`
  (quando o pipeline rodou), não no `timestamp` do evento em si — os dados
  deste projeto são sintéticos e podem ter `timestamp` de negócio em
  qualquer ponto da janela gerada (até 6 meses), então isso não indicaria
  nada sobre o pipeline estar "vivo". Janela generosa (90 dias) — o objetivo
  é pegar "ninguém roda a ingestão há muito tempo", não simular SLA de
  streaming.
"""

from __future__ import annotations

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import FileDataContext

FRESHNESS_WINDOW_DAYS = 90

CURRENCIES = ["BRL", "USD", "EUR"]
TRANSACTION_TYPES = ["PIX", "TED", "DOC", "CARTAO_CREDITO", "CARTAO_DEBITO", "BOLETO"]
CUSTOMER_SEGMENTS = ["VAREJO", "ALTA_RENDA", "PRIVATE"]

SUITE_NAMES = (
    "bronze_transactions",
    "bronze_market_data",
    "silver_transactions",
    "silver_market_data",
    "gold_fact_transactions",
    "gold_dim_customers",
    "gold_dim_date",
    "gold_agg_daily_fraud_metrics",
)


def _exists(*columns: str) -> list[ExpectationConfiguration]:
    return [
        ExpectationConfiguration(expectation_type="expect_column_to_exist", kwargs={"column": c})
        for c in columns
    ]


def _not_null(*columns: str) -> list[ExpectationConfiguration]:
    return [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null", kwargs={"column": c}
        )
        for c in columns
    ]


def _unique(column: str) -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique", kwargs={"column": column}
    )


def _compound_unique(columns: list[str]) -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_compound_columns_to_be_unique",
        kwargs={"column_list": columns},
    )


def _in_set(column: str, values: list[str]) -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": column, "value_set": values},
    )


def _between(column: str, min_value: float | None = None, max_value: float | None = None):
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": column, "min_value": min_value, "max_value": max_value},
    )


def _not_null_when(column: str, condition: str) -> ExpectationConfiguration:
    """fraud_type não-nulo quando is_fraud=true — mesma regra do validator
    Pydantic em TransactionEvent (schemas.py)."""
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": column, "row_condition": condition, "condition_parser": "pandas"},
    )


def _freshness(column: str) -> ExpectationConfiguration:
    import datetime

    # Naive (sem tzinfo) de propósito: ingestion_timestamp/processing_timestamp
    # chegam do Spark/pandas como datetime64[ns] sem timezone — comparar contra
    # bounds timezone-aware faz o GX falhar silenciosamente (não levanta erro,
    # só reporta a expectativa como não atendida) em vez de comparar de fato.
    now = datetime.datetime.now(tz=datetime.UTC).replace(tzinfo=None)
    return ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={
            "column": column,
            "min_value": (now - datetime.timedelta(days=FRESHNESS_WINDOW_DAYS)).isoformat(),
            "max_value": (now + datetime.timedelta(days=1)).isoformat(),
        },
    )


def _save(context: FileDataContext, name: str, expectations: list[ExpectationConfiguration]) -> None:
    suite = context.add_or_update_expectation_suite(name)
    suite.expectations = []  # idempotente: reconstrói do zero a cada chamada
    suite.add_expectation_configurations(expectations)
    context.update_expectation_suite(suite)


# ── Bronze ───────────────────────────────────────────────────────────────────


def _build_bronze_transactions(context: FileDataContext) -> None:
    cols = [
        "transaction_id",
        "customer_id",
        "timestamp",
        "amount",
        "currency",
        "transaction_type",
        "merchant_category",
        "origin_account",
        "destination_account",
        "origin_bank",
        "destination_bank",
        "channel",
        "is_fraud",
        "ingestion_timestamp",
    ]
    expectations = [
        *_exists(*cols),
        *_not_null(
            "transaction_id",
            "customer_id",
            "timestamp",
            "amount",
            "currency",
            "transaction_type",
            "is_fraud",
        ),
        _in_set("currency", CURRENCIES),
        _in_set("transaction_type", TRANSACTION_TYPES),
        _between("amount", min_value=0.01),
        _not_null_when("fraud_type", "is_fraud == True"),
        _freshness("ingestion_timestamp"),
    ]
    _save(context, "bronze_transactions", expectations)


def _build_bronze_market_data(context: FileDataContext) -> None:
    cols = ["symbol", "date", "open", "high", "low", "close", "volume", "adjusted_close"]
    expectations = [
        *_exists(*cols),
        *_not_null("symbol", "date", "close"),
        _between("open", min_value=0.01),
        _between("high", min_value=0.01),
        _between("low", min_value=0.01),
        _between("close", min_value=0.01),
        _between("adjusted_close", min_value=0.01),
        _between("volume", min_value=0),
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_a_to_be_greater_than_b",
            kwargs={"column_A": "high", "column_B": "low", "or_equal": True},
        ),
    ]
    _save(context, "bronze_market_data", expectations)


# ── Silver ───────────────────────────────────────────────────────────────────


def _build_silver_transactions(context: FileDataContext) -> None:
    cols = [
        "transaction_id",
        "customer_id",
        "timestamp",
        "amount",
        "currency",
        "transaction_type",
        "channel",
        "is_fraud",
        "processing_timestamp",
    ]
    expectations = [
        *_exists(*cols),
        *_not_null(
            "transaction_id",
            "customer_id",
            "timestamp",
            "amount",
            "currency",
            "transaction_type",
            "is_fraud",
        ),
        _unique("transaction_id"),  # dedup já aconteceu — regressão aqui é grave
        _in_set("currency", CURRENCIES),
        _between("amount", min_value=0.01),
        _not_null_when("fraud_type", "is_fraud == True"),
        _freshness("processing_timestamp"),
    ]
    _save(context, "silver_transactions", expectations)


def _build_silver_market_data(context: FileDataContext) -> None:
    cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    expectations = [
        *_exists(*cols),
        *_not_null("symbol", "date", "close"),
        _between("open", min_value=0.01),
        _between("high", min_value=0.01),
        _between("low", min_value=0.01),
        _between("close", min_value=0.01),
        _between("volume", min_value=0),
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_a_to_be_greater_than_b",
            kwargs={"column_A": "high", "column_B": "low", "or_equal": True},
        ),
    ]
    _save(context, "silver_market_data", expectations)


# ── Gold ─────────────────────────────────────────────────────────────────────


def _build_gold_fact_transactions(context: FileDataContext) -> None:
    cols = [
        "transaction_id",
        "customer_key",
        "date_key",
        "amount_brl",
        "currency",
        "transaction_type",
        "channel",
        "merchant_category",
        "is_fraud",
    ]
    expectations = [
        *_exists(*cols),
        *_not_null(
            "transaction_id",
            "customer_key",
            "date_key",
            "amount_brl",
            "currency",
            "transaction_type",
            "is_fraud",
        ),
        _unique("transaction_id"),  # PK
        _in_set("currency", CURRENCIES),
        _in_set("transaction_type", TRANSACTION_TYPES),
        _between("amount_brl", min_value=0.01),
        _between("fraud_score", min_value=0.0, max_value=1.0),
        _not_null_when("fraud_type", "is_fraud == True"),
    ]
    _save(context, "gold_fact_transactions", expectations)


def _build_gold_dim_customers(context: FileDataContext) -> None:
    cols = ["customer_key", "segment", "city", "state", "country", "risk_score", "age"]
    expectations = [
        *_exists(*cols),
        *_not_null("customer_key", "segment", "city", "state", "country", "risk_score"),
        _unique("customer_key"),  # PK — teria pego o bug real da issue #10
        _in_set("segment", CUSTOMER_SEGMENTS),
        _between("risk_score", min_value=0.0, max_value=100.0),
        _between("age", min_value=0, max_value=120),
    ]
    _save(context, "gold_dim_customers", expectations)


def _build_gold_dim_date(context: FileDataContext) -> None:
    cols = [
        "date_key",
        "year",
        "month",
        "day",
        "quarter",
        "day_of_week",
        "day_name",
        "week_of_year",
        "is_weekend",
    ]
    expectations = [
        *_exists(*cols),
        *_not_null(*cols),
        _unique("date_key"),  # PK
        _between("month", min_value=1, max_value=12),
        _between("day", min_value=1, max_value=31),
        _between("quarter", min_value=1, max_value=4),
        _between("day_of_week", min_value=1, max_value=7),
        _between("week_of_year", min_value=1, max_value=53),
    ]
    _save(context, "gold_dim_date", expectations)


def _build_gold_agg_daily_fraud_metrics(context: FileDataContext) -> None:
    cols = [
        "date_key",
        "transaction_type",
        "total_transactions",
        "total_amount_brl",
        "avg_amount_brl",
        "fraud_count",
        "fraud_rate",
    ]
    expectations = [
        *_exists(*cols),
        *_not_null(*cols),
        _compound_unique(["date_key", "transaction_type"]),  # PK composta
        _in_set("transaction_type", TRANSACTION_TYPES),
        _between("total_transactions", min_value=0),
        _between("fraud_count", min_value=0),
        _between("fraud_rate", min_value=0.0, max_value=1.0),
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_a_to_be_greater_than_b",
            kwargs={
                "column_A": "total_transactions",
                "column_B": "fraud_count",
                "or_equal": True,
            },
        ),
    ]
    _save(context, "gold_agg_daily_fraud_metrics", expectations)


_BUILDERS = {
    "bronze_transactions": _build_bronze_transactions,
    "bronze_market_data": _build_bronze_market_data,
    "silver_transactions": _build_silver_transactions,
    "silver_market_data": _build_silver_market_data,
    "gold_fact_transactions": _build_gold_fact_transactions,
    "gold_dim_customers": _build_gold_dim_customers,
    "gold_dim_date": _build_gold_dim_date,
    "gold_agg_daily_fraud_metrics": _build_gold_agg_daily_fraud_metrics,
}


def ensure_suites(context: FileDataContext) -> None:
    """Cria/atualiza todas as expectation suites (idempotente)."""
    for builder in _BUILDERS.values():
        builder(context)


def ensure_suite(context: FileDataContext, name: str) -> None:
    """Cria/atualiza uma única suite pelo nome (usado pelos testes)."""
    if name not in _BUILDERS:
        raise ValueError(f"suite desconhecida: {name!r} (válidas: {SUITE_NAMES})")
    _BUILDERS[name](context)
