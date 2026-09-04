"""Registro declarativo do catálogo de dados (issue #14).

Substitui o OpenMetadata completo (server + MySQL/Postgres próprio +
Elasticsearch + ingestion-Airflow) na V1 local: o stack oficial dele soma
mais 3-4 serviços pesados aos 19 que já rodam em `docker-compose.yml`
(Kafka, Zookeeper, Spark x3, Airflow x3, Superset, Postgres x2, MinIO x2,
producers, API), que já disputam os ~8GB alocados ao Docker neste ambiente.
Decisão documentada em `docs/architecture.md`/`README.md` — OpenMetadata
real (ou um catálogo gerenciado equivalente, ex. AWS Glue Data Catalog)
fica para V2/cloud.

Cada `CatalogEntry` é validada contra a infra real por `validator.py` (não
é só um documento estático) e renderizada em `docs/data_catalog.md` por
`render.py`. `glossary_terms` referencia definições que já existem em
`docs/data_dictionary.md` — não duplicadas aqui.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from src.common.config import settings


class DatasetLayer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    SERVING = "serving"
    STREAMING = "streaming"
    DASHBOARD = "dashboard"


class DatasetKind(str, Enum):
    MINIO_PREFIX = "minio_prefix"
    POSTGRES_TABLE = "postgres_table"
    KAFKA_TOPIC = "kafka_topic"
    DASHBOARD = "dashboard"  # sem checagem de infra — issue #16 ainda não existe


@dataclass(frozen=True)
class CatalogEntry:
    key: str
    name: str
    layer: DatasetLayer
    kind: DatasetKind
    location: str
    owner: str
    classification: tuple[str, ...]
    glossary_terms: tuple[str, ...]
    description: str
    upstream: tuple[str, ...] = field(default_factory=tuple)
    status: str = "ativo"


CATALOG: tuple[CatalogEntry, ...] = (
    # ── Bronze ───────────────────────────────────────────────────────────
    CatalogEntry(
        key="bronze_transactions",
        name="Bronze — Transações",
        layer=DatasetLayer.BRONZE,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_bronze}/transactions/",
        owner="Data Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=(),
        description="Transações financeiras raw, conforme recebidas do Kafka.",
    ),
    CatalogEntry(
        key="bronze_market_data",
        name="Bronze — Market Data",
        layer=DatasetLayer.BRONZE,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_bronze}/market_data/",
        owner="Data Engineering",
        classification=("Público",),
        glossary_terms=("VWAP", "Volatilidade"),
        description="Cotações OHLCV coletadas via yfinance.",
    ),
    # ── Silver ───────────────────────────────────────────────────────────
    CatalogEntry(
        key="silver_transactions",
        name="Silver — Transações",
        layer=DatasetLayer.SILVER,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_silver}/transactions/",
        owner="Data Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=(),
        description="Transações limpas, deduplicadas, timestamps normalizados para UTC.",
        upstream=("bronze_transactions",),
    ),
    CatalogEntry(
        key="silver_market_data",
        name="Silver — Market Data",
        layer=DatasetLayer.SILVER,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_silver}/market_data/",
        owner="Data Engineering",
        classification=("Público",),
        glossary_terms=("VWAP", "Volatilidade"),
        description="Cotações enriquecidas com retorno diário e price range.",
        upstream=("bronze_market_data",),
    ),
    # ── Gold (MinIO) ─────────────────────────────────────────────────────
    CatalogEntry(
        key="gold_fact_transactions",
        name="Gold — Fato Transações",
        layer=DatasetLayer.GOLD,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_gold}/fact_transactions/",
        owner="Analytics Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=("Fraud Score",),
        description="Tabela fato de transações — grão: uma linha por transação.",
        upstream=("silver_transactions",),
    ),
    CatalogEntry(
        key="gold_dim_customers",
        name="Gold — Dimensão Clientes",
        layer=DatasetLayer.GOLD,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_gold}/dim_customers/",
        owner="Analytics Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=(),
        description="Dimensão de clientes (SCD2, apenas registro corrente).",
        upstream=("silver_transactions",),
    ),
    CatalogEntry(
        key="gold_dim_date",
        name="Gold — Dimensão Data",
        layer=DatasetLayer.GOLD,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_gold}/dim_date/",
        owner="Analytics Engineering",
        classification=("Público",),
        glossary_terms=(),
        description="Dimensão de calendário derivada das datas distintas do Silver.",
        upstream=("silver_transactions",),
    ),
    CatalogEntry(
        key="gold_agg_daily_fraud_metrics",
        name="Gold — Métricas Diárias de Fraude",
        layer=DatasetLayer.GOLD,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_gold}/agg_daily_fraud_metrics/",
        owner="Analytics Engineering",
        classification=("Confidencial",),
        glossary_terms=("Fraud Score",),
        description="Agregação diária de volume e taxa de fraude por tipo de transação.",
        upstream=("gold_fact_transactions",),
    ),
    # ── Serving (Postgres) ───────────────────────────────────────────────
    CatalogEntry(
        key="serving_fact_transactions",
        name="Postgres — fact_transactions",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="fact_transactions",
        owner="Analytics Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=("Fraud Score",),
        description="Espelho da tabela fato Gold, carregado via truncate+reload (issue #10).",
        upstream=("gold_fact_transactions",),
    ),
    CatalogEntry(
        key="serving_dim_customers",
        name="Postgres — dim_customers",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="dim_customers",
        owner="Analytics Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=(),
        description="Espelho da dimensão de clientes Gold.",
        upstream=("gold_dim_customers",),
    ),
    CatalogEntry(
        key="serving_dim_date",
        name="Postgres — dim_date",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="dim_date",
        owner="Analytics Engineering",
        classification=("Público",),
        glossary_terms=(),
        description="Espelho da dimensão de calendário Gold.",
        upstream=("gold_dim_date",),
    ),
    CatalogEntry(
        key="serving_agg_daily_fraud_metrics",
        name="Postgres — agg_daily_fraud_metrics",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="agg_daily_fraud_metrics",
        owner="Analytics Engineering",
        classification=("Confidencial",),
        glossary_terms=("Fraud Score",),
        description="Espelho da agregação diária de fraude Gold, consumido pela API (issue #15).",
        upstream=("gold_agg_daily_fraud_metrics",),
    ),
    # ── Streaming (Kafka) ────────────────────────────────────────────────
    CatalogEntry(
        key="kafka_raw_transactions",
        name="Kafka — raw-transactions",
        layer=DatasetLayer.STREAMING,
        kind=DatasetKind.KAFKA_TOPIC,
        location=settings.kafka.topic_transactions,
        owner="Data Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=(),
        description="Transações publicadas em tempo real pelo producer.",
    ),
    CatalogEntry(
        key="kafka_raw_market_data",
        name="Kafka — raw-market-data",
        layer=DatasetLayer.STREAMING,
        kind=DatasetKind.KAFKA_TOPIC,
        location=settings.kafka.topic_market_data,
        owner="Data Engineering",
        classification=("Público",),
        glossary_terms=("VWAP",),
        description="Cotações publicadas em tempo real pelo producer.",
    ),
    CatalogEntry(
        key="kafka_enriched_transactions",
        name="Kafka — enriched-transactions",
        layer=DatasetLayer.STREAMING,
        kind=DatasetKind.KAFKA_TOPIC,
        location=settings.kafka.topic_enriched,
        owner="Data Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=("Z-Score", "Fraud Score"),
        description="Transações com fraud_score anexado pelo StreamProcessor (issue #11).",
        upstream=("kafka_raw_transactions",),
    ),
    CatalogEntry(
        key="kafka_fraud_alerts",
        name="Kafka — fraud-alerts",
        layer=DatasetLayer.STREAMING,
        kind=DatasetKind.KAFKA_TOPIC,
        location=settings.kafka.topic_fraud_alerts,
        owner="Fraud Analytics",
        classification=("PII", "Confidencial"),
        glossary_terms=("Z-Score", "Fraud Score", "Velocity Check"),
        description="Alertas de fraude confirmados pelo detector Z-Score (issue #11).",
        upstream=("kafka_enriched_transactions",),
    ),
    # ── Dashboard (planejado — issue #16 ainda não implementada) ─────────
    CatalogEntry(
        key="dashboard_fraud_overview",
        name="Dashboard — Visão Geral de Fraude",
        layer=DatasetLayer.DASHBOARD,
        kind=DatasetKind.DASHBOARD,
        location="",
        owner="Fraud Analytics",
        classification=("Confidencial",),
        glossary_terms=("Fraud Score",),
        description="KPIs de volume, valor, taxa de fraude e alertas (Superset/Grafana).",
        upstream=("serving_fact_transactions", "serving_agg_daily_fraud_metrics"),
        status="planejado",
    ),
)

CATALOG_BY_KEY: dict[str, CatalogEntry] = {entry.key: entry for entry in CATALOG}
