"""Configurações centralizadas da plataforma via variáveis de ambiente."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = Field(default="localhost:29092", alias="KAFKA_BOOTSTRAP_SERVERS")
    internal_servers: str = Field(default="kafka:9092", alias="KAFKA_INTERNAL_SERVERS")
    topic_transactions: str = Field(default="raw-transactions", alias="KAFKA_TOPIC_TRANSACTIONS")
    topic_market_data: str = Field(default="raw-market-data", alias="KAFKA_TOPIC_MARKET_DATA")
    topic_enriched: str = Field(default="enriched-transactions", alias="KAFKA_TOPIC_ENRICHED")
    topic_fraud_alerts: str = Field(default="fraud-alerts", alias="KAFKA_TOPIC_FRAUD_ALERTS")
    consumer_group: str = Field(default="fraud-detection-group", alias="KAFKA_CONSUMER_GROUP")
    auto_offset_reset: str = Field(default="earliest", alias="KAFKA_AUTO_OFFSET_RESET")
    replication_factor: int = Field(default=1, alias="KAFKA_REPLICATION_FACTOR")
    num_partitions: int = Field(default=3, alias="KAFKA_NUM_PARTITIONS")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class MinIOSettings(BaseSettings):
    endpoint: str = Field(default="http://localhost:9000", alias="MINIO_ENDPOINT")
    internal_endpoint: str = Field(default="http://minio:9000", alias="MINIO_INTERNAL_ENDPOINT")
    access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    bucket_bronze: str = Field(default="bronze", alias="MINIO_BUCKET_BRONZE")
    bucket_silver: str = Field(default="silver", alias="MINIO_BUCKET_SILVER")
    bucket_gold: str = Field(default="gold", alias="MINIO_BUCKET_GOLD")
    bucket_checkpoints: str = Field(default="checkpoints", alias="MINIO_BUCKET_CHECKPOINTS")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class PostgresSettings(BaseSettings):
    host: str = Field(default="localhost", alias="POSTGRES_HOST")
    internal_host: str = Field(default="postgres", alias="POSTGRES_INTERNAL_HOST")
    port: int = Field(default=5432, alias="POSTGRES_PORT")
    db: str = Field(default="fraud_analytics", alias="POSTGRES_DB")
    user: str = Field(default="datamaster", alias="POSTGRES_USER")
    password: str = Field(default="datamaster123", alias="POSTGRES_PASSWORD")

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    @property
    def internal_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.internal_host}:{self.port}/{self.db}"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class SparkSettings(BaseSettings):
    master_url: str = Field(default="spark://spark-master:7077", alias="SPARK_MASTER_URL")
    driver_memory: str = Field(default="2g", alias="SPARK_DRIVER_MEMORY")
    executor_memory: str = Field(default="2g", alias="SPARK_EXECUTOR_MEMORY")
    executor_cores: int = Field(default=2, alias="SPARK_EXECUTOR_CORES")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class APISettings(BaseSettings):
    host: str = Field(default="0.0.0.0", alias="API_HOST")
    port: int = Field(default=8000, alias="API_PORT")
    reload: bool = Field(default=True, alias="API_RELOAD")
    workers: int = Field(default=1, alias="API_WORKERS")
    secret_key: str = Field(default="change-me-in-production", alias="API_SECRET_KEY")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class MarketDataSettings(BaseSettings):
    finnhub_api_key: str = Field(default="", alias="FINNHUB_API_KEY")
    alpha_vantage_api_key: str = Field(default="", alias="ALPHA_VANTAGE_API_KEY")
    tickers: str = Field(
        default="PETR4.SA,VALE3.SA,ITUB4.SA,BBDC4.SA,ABEV3.SA,WEGE3.SA,RENT3.SA,BBAS3.SA,MGLU3.SA,LREN3.SA",
        alias="YFINANCE_TICKERS",
    )

    @property
    def ticker_list(self) -> list[str]:
        return [t.strip() for t in self.tickers.split(",")]

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class Settings(BaseSettings):
    project_name: str = Field(default="data-master-std-fraud", alias="PROJECT_NAME")
    environment: str = Field(default="local", alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="json", alias="LOG_FORMAT")

    kafka: KafkaSettings = KafkaSettings()
    minio: MinIOSettings = MinIOSettings()
    postgres: PostgresSettings = PostgresSettings()
    spark: SparkSettings = SparkSettings()
    api: APISettings = APISettings()
    market: MarketDataSettings = MarketDataSettings()

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
