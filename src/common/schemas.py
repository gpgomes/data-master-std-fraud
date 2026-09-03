"""Schemas Pydantic e PySpark para toda a plataforma."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator

# ── Enums ──────────────────────────────────────────────────────────────────────

class Currency(StrEnum):
    BRL = "BRL"
    USD = "USD"
    EUR = "EUR"


class TransactionType(StrEnum):
    PIX = "PIX"
    TED = "TED"
    DOC = "DOC"
    CARTAO_CREDITO = "CARTAO_CREDITO"
    CARTAO_DEBITO = "CARTAO_DEBITO"
    BOLETO = "BOLETO"


class MerchantCategory(StrEnum):
    ALIMENTACAO = "ALIMENTACAO"
    TRANSPORTE = "TRANSPORTE"
    SAUDE = "SAUDE"
    EDUCACAO = "EDUCACAO"
    LAZER = "LAZER"
    TRANSFERENCIA = "TRANSFERENCIA"
    SAQUE = "SAQUE"
    SERVICOS = "SERVICOS"


class Channel(StrEnum):
    APP_MOBILE = "APP_MOBILE"
    INTERNET_BANKING = "INTERNET_BANKING"
    AGENCIA = "AGENCIA"
    ATM = "ATM"
    API = "API"


class FraudType(StrEnum):
    ACCOUNT_TAKEOVER = "ACCOUNT_TAKEOVER"
    CARD_CLONING = "CARD_CLONING"
    IDENTITY_THEFT = "IDENTITY_THEFT"
    MONEY_LAUNDERING = "MONEY_LAUNDERING"
    SOCIAL_ENGINEERING = "SOCIAL_ENGINEERING"


class CustomerSegment(StrEnum):
    VAREJO = "VAREJO"
    ALTA_RENDA = "ALTA_RENDA"
    PRIVATE = "PRIVATE"


# ── Pydantic Models ────────────────────────────────────────────────────────────

class TransactionEvent(BaseModel):
    """Evento de transação financeira — produzido no Kafka e ingerido no Bronze."""

    transaction_id: str = Field(description="UUID da transação")
    customer_id: str = Field(description="UUID do cliente")
    timestamp: datetime = Field(description="Data/hora da transação com timezone")
    amount: float = Field(gt=0, description="Valor da transação em moeda local")
    currency: Currency = Field(default=Currency.BRL)
    transaction_type: TransactionType
    merchant_category: MerchantCategory
    origin_account: str
    destination_account: str
    origin_bank: str
    destination_bank: str
    channel: Channel
    device_id: str | None = None
    ip_address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    is_fraud: bool = Field(default=False)
    fraud_type: FraudType | None = None
    fraud_score: float | None = Field(default=None, ge=0.0, le=1.0)

    @field_validator("fraud_type")
    @classmethod
    def validate_fraud_type(cls, v: FraudType | None, info) -> FraudType | None:
        if info.data.get("is_fraud") and v is None:
            raise ValueError("fraud_type deve ser informado quando is_fraud=True")
        return v

    model_config = {"use_enum_values": True}


class MarketTradeEvent(BaseModel):
    """Evento de trade de mercado — cotação tick-by-tick."""

    event_id: str = Field(description="UUID do evento")
    symbol: str = Field(description="Ticker do ativo (ex: PETR4.SA)")
    timestamp: datetime
    price: float = Field(gt=0)
    volume: int = Field(ge=0)
    bid: float = Field(gt=0, description="Melhor oferta de compra")
    ask: float = Field(gt=0, description="Melhor oferta de venda")
    spread: float = Field(ge=0, description="Diferença entre ask e bid")
    source: str = Field(default="simulator", description="Fonte dos dados")

    model_config = {"use_enum_values": True}


class CustomerRecord(BaseModel):
    """Registro de cliente — tabela dimensional dim_clientes."""

    customer_id: str
    name: str
    cpf_masked: str = Field(description="CPF mascarado (***.***.***-**)")
    birth_date: str = Field(description="Data de nascimento (YYYY-MM-DD)")
    gender: str
    account_opening_date: str
    risk_score: float = Field(ge=0.0, le=100.0)
    segment: CustomerSegment
    city: str
    state: str
    country: str = Field(default="BR")

    model_config = {"use_enum_values": True}


class MarketOHLCV(BaseModel):
    """Registro OHLCV de ativo — ingestão batch via yfinance."""

    symbol: str
    date: str = Field(description="Data (YYYY-MM-DD)")
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: float


class FraudAlert(BaseModel):
    """Alerta de fraude detectado no streaming."""

    alert_id: str
    transaction_id: str
    customer_id: str
    timestamp: datetime
    amount: float
    fraud_type: FraudType
    fraud_score: float = Field(ge=0.0, le=1.0)
    z_score: float | None = None
    alert_reason: str
    processed_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"use_enum_values": True}


# ── PySpark Schema Strings ────────────────────────────────────────────────────
# Usados nos jobs PySpark para inferência de schema

TRANSACTION_SPARK_SCHEMA = """
    transaction_id STRING,
    customer_id STRING,
    timestamp TIMESTAMP,
    amount DOUBLE,
    currency STRING,
    transaction_type STRING,
    merchant_category STRING,
    origin_account STRING,
    destination_account STRING,
    origin_bank STRING,
    destination_bank STRING,
    channel STRING,
    device_id STRING,
    ip_address STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    is_fraud BOOLEAN,
    fraud_type STRING,
    fraud_score DOUBLE
"""

MARKET_TRADE_SPARK_SCHEMA = """
    event_id STRING,
    symbol STRING,
    timestamp TIMESTAMP,
    price DOUBLE,
    volume LONG,
    bid DOUBLE,
    ask DOUBLE,
    spread DOUBLE,
    source STRING
"""

MARKET_OHLCV_SPARK_SCHEMA = """
    symbol STRING,
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    adjusted_close DOUBLE
"""

# ── PySpark StructType Schemas ─────────────────────────────────────────────────
# Importação lazy para não forçar dependência do PySpark em contextos leves.
# Use estas funções apenas dentro de jobs PySpark.


def get_bronze_transaction_schema():
    """StructType para transações na camada Bronze (dados brutos do CSV/Parquet)."""
    from pyspark.sql.types import (
        BooleanType,
        DecimalType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("amount", DecimalType(18, 2), True),
            StructField("currency", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("origin_account", StringType(), True),
            StructField("destination_account", StringType(), True),
            StructField("origin_bank", StringType(), True),
            StructField("destination_bank", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("is_fraud", BooleanType(), True),
            StructField("fraud_type", StringType(), True),
            StructField("fraud_score", DoubleType(), True),
            # metadados de ingestão
            StructField("ingestion_timestamp", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("batch_id", StringType(), True),
        ]
    )


def get_silver_transaction_schema():
    """StructType para transações na camada Silver (limpa e enriquecida)."""
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("amount", DecimalType(18, 2), True),
            StructField("currency", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("origin_account", StringType(), True),
            StructField("destination_account", StringType(), True),
            StructField("origin_bank", StringType(), True),
            StructField("destination_bank", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("is_fraud", BooleanType(), True),
            StructField("fraud_type", StringType(), True),
            StructField("fraud_score", DoubleType(), True),
            # colunas enriquecidas
            StructField("transaction_date", DateType(), True),
            StructField("transaction_hour", IntegerType(), True),
            StructField("is_business_hours", BooleanType(), True),
            StructField("amount_brl", DoubleType(), True),
            StructField("processing_timestamp", TimestampType(), True),
        ]
    )


def get_bronze_market_data_schema():
    """StructType para dados de mercado OHLCV na camada Bronze."""
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("symbol", StringType(), True),
            StructField("date", DateType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("adjusted_close", DoubleType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("source_system", StringType(), True),
        ]
    )


def get_silver_market_data_schema():
    """StructType para dados de mercado na camada Silver (com indicadores calculados)."""
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("date", DateType(), False),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("adjusted_close", DoubleType(), True),
            # indicadores calculados
            StructField("daily_return", DoubleType(), True),
            StructField("intraday_range", DoubleType(), True),
            StructField("sma_5", DoubleType(), True),
            StructField("sma_10", DoubleType(), True),
            StructField("sma_20", DoubleType(), True),
            StructField("processing_timestamp", TimestampType(), True),
        ]
    )


def get_bronze_customer_schema():
    """StructType para clientes na camada Bronze (snapshot diário com SCD2)."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("cpf_masked", StringType(), True),
            StructField("birth_date", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("account_opening_date", StringType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("segment", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            # SCD2
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
            StructField("is_current", BooleanType(), True),
            StructField("ingestion_timestamp", StringType(), True),
            StructField("batch_id", StringType(), True),
        ]
    )


def get_silver_customer_schema():
    """StructType para clientes na camada Silver (com idade e faixa etária)."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("cpf_masked", StringType(), True),
            StructField("birth_date", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("account_opening_date", StringType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("segment", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
            StructField("is_current", BooleanType(), True),
            # enriquecimento
            StructField("age", IntegerType(), True),
            StructField("age_group", StringType(), True),
            StructField("processing_timestamp", TimestampType(), True),
        ]
    )


def get_gold_dim_customers_schema():
    """StructType para a dimensão de clientes na camada Gold."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("customer_key", StringType(), False),
            StructField("name", StringType(), True),
            StructField("cpf_masked", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("birth_date", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("age_group", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("account_opening_date", StringType(), True),
            StructField("processing_timestamp", TimestampType(), True),
        ]
    )


def get_gold_dim_date_schema():
    """StructType para a dimensão de data (calendário) na camada Gold."""
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("date_key", DateType(), False),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("day_name", StringType(), True),
            StructField("week_of_year", IntegerType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("processing_timestamp", TimestampType(), True),
        ]
    )


def get_gold_fact_transactions_schema():
    """StructType para a tabela fato de transações na camada Gold."""
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("customer_key", StringType(), True),
            StructField("date_key", DateType(), True),
            StructField("amount_brl", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("is_fraud", BooleanType(), True),
            StructField("fraud_type", StringType(), True),
            StructField("fraud_score", DoubleType(), True),
            StructField("processing_timestamp", TimestampType(), True),
        ]
    )


def get_gold_agg_daily_fraud_metrics_schema():
    """StructType para a agregação diária de métricas de fraude na camada Gold."""
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("date_key", DateType(), False),
            StructField("transaction_type", StringType(), False),
            StructField("total_transactions", LongType(), True),
            StructField("total_amount_brl", DoubleType(), True),
            StructField("avg_amount_brl", DoubleType(), True),
            StructField("fraud_count", LongType(), True),
            StructField("fraud_rate", DoubleType(), True),
            StructField("processing_timestamp", TimestampType(), True),
        ]
    )
