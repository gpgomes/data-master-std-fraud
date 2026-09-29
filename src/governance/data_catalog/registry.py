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
    DASHBOARD = "dashboard"  # sem checagem de infra — validar contra a API do Superset ainda não implementado


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
    # Asset de enriquecimento que pode legitimamente não existir (ex.: mercado, vindo do
    # yfinance com rate limit): ausência é warning no catálogo, não derruba `--strict`.
    optional: bool = False


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
        optional=True,
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
        optional=True,
    ),
    CatalogEntry(
        key="silver_transactions_stream",
        name="Silver — Transações (Streaming)",
        layer=DatasetLayer.SILVER,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_silver}/transactions_stream/",
        owner="Data Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=("Fraud Engine", "Shadow Scoring", "Z-Score", "Fraud Score"),
        description=(
            "Saída do detector de streaming, particionada por query_id/batch_id: transações com "
            "o veredito do Fraud Engine (fraud_score, is_fraud_predicted, fraud_signals, "
            "fraud_type_predicted, detector_version), o Z-Score antigo em paralelo (z_score, "
            "is_anomaly, fraud_score_v1) e o rótulo do gerador (is_fraud, fraud_type). Só existe "
            "depois que o job de streaming rodou (issues #11 e #46)."
        ),
        upstream=(
            "kafka_raw_transactions",
            "gold_dim_customers",
            "gold_customer_behavior_profile",
        ),
        optional=True,
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
    CatalogEntry(
        key="gold_customer_behavior_profile",
        name="Gold — Perfil de Comportamento do Cliente",
        layer=DatasetLayer.GOLD,
        kind=DatasetKind.MINIO_PREFIX,
        location=f"s3://{settings.minio.bucket_gold}/customer_behavior_profile/",
        owner="Fraud Analytics",
        classification=("PII", "Confidencial"),
        glossary_terms=("Perfil de Comportamento", "Fraud Engine"),
        description=(
            "Perfil de comportamento por cliente, aprendido do histórico legítimo do Silver: "
            "valor típico (μ/σ de ln(amount)), devices, redes /24 e destinatários conhecidos, "
            "share noturno, centro geográfico e idade da conta. Grão: uma linha por cliente. É "
            "a camada longa da arquitetura Lambda, lida por broadcast pelo detector do "
            "streaming (issue #46). Validado pelo gate gold_customer_behavior_profile do Great "
            "Expectations (issue #47)."
        ),
        upstream=("silver_transactions", "gold_dim_customers"),
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
    CatalogEntry(
        key="serving_stream_scored_transactions",
        name="Postgres — stream_scored_transactions",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="stream_scored_transactions",
        owner="Analytics Engineering",
        classification=("Confidencial",),
        glossary_terms=("Fraud Engine", "Shadow Scoring", "Rótulo (ground truth)", "Fraud Score"),
        description=(
            "Transações pontuadas pelo Fraud Engine (fraud_score, is_fraud_predicted, fraud_signals, "
            "fraud_type_predicted, detector_version), com o Z-Score antigo em paralelo (z_score, "
            "is_anomaly, fraud_score_v1) e a latência evento→processamento. is_fraud/fraud_type são "
            "o **rótulo** do gerador (ground truth, só para medir); as colunas *_predicted e "
            "fraud_score são a predição. Carregada por `make spark-submit-stream-postgres` "
            "(issues #38 e #47)."
        ),
        upstream=("silver_transactions_stream",),
    ),
    CatalogEntry(
        key="serving_fraud_alerts",
        name="Postgres — fraud_alerts",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="fraud_alerts",
        owner="Fraud Analytics",
        classification=("Confidencial",),
        glossary_terms=("Fraud Engine", "Fraud Score"),
        description=(
            "Alertas do Fraud Engine (V2), os mesmos do tópico fraud-alerts, com os sinais que "
            "dispararam cada alerta (signals) e o tipo inferido pelos sinais (fraud_type, nulo se "
            "nenhuma regra casou; nunca o rótulo). Consumido por GET /alerts (issues #38 e #47)."
        ),
        upstream=("silver_transactions_stream",),
    ),
    # ── Observabilidade (Postgres, issue #55) ───────────────────────────
    # Tabelas append-only criadas na primeira gravação de métrica: num ambiente onde nada
    # rodou ainda elas não existem, por isso são opcionais no catálogo.
    CatalogEntry(
        key="serving_stream_batch_metrics",
        name="Postgres — stream_batch_metrics",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="stream_batch_metrics",
        owner="Data Engineering",
        classification=("Interno",),
        glossary_terms=("SLO",),
        description=(
            "Uma linha por micro-batch do stream, gravada pelo StreamingQueryListener: linhas/s de "
            "entrada e processadas, duração por fase, lag do Kafka, linhas pontuadas, alertas, "
            "tamanho do estado curto, latência p50/p95/máx evento→processamento (issue #55) e o "
            "tempo de cada etapa do foreachBatch (issue #56)."
        ),
        upstream=("kafka_raw_transactions",),
        optional=True,
    ),
    CatalogEntry(
        key="serving_pipeline_runs",
        name="Postgres — pipeline_runs",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="pipeline_runs",
        owner="Data Engineering",
        classification=("Interno",),
        glossary_terms=("SLO",),
        description=(
            "Uma linha por execução de DAG do Airflow (estado, duração, tasks que falharam), "
            "gravada pela task record_pipeline_run, que roda mesmo com falha upstream (issue #55)."
        ),
        optional=True,
    ),
    CatalogEntry(
        key="serving_quality_gate_runs",
        name="Postgres — quality_gate_runs",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="quality_gate_runs",
        owner="Data Engineering",
        classification=("Interno",),
        glossary_terms=("SLO",),
        description=(
            "Uma linha por execução de quality gate do Great Expectations: dataset, sucesso, "
            "expectativas avaliadas e com falha, e se o gate é opcional (issue #55)."
        ),
        optional=True,
    ),
    CatalogEntry(
        key="serving_api_requests",
        name="Postgres — api_requests",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="api_requests",
        owner="Analytics Engineering",
        classification=("Interno",),
        glossary_terms=("SLO",),
        description=(
            "Uma linha por request da API: método, rota (o template, sem ids), status e duração. "
            "Gravada depois da resposta, fora da latência medida (issue #55)."
        ),
        optional=True,
    ),
    CatalogEntry(
        key="serving_api_access_audit",
        name="Postgres — api_access_audit",
        layer=DatasetLayer.SERVING,
        kind=DatasetKind.POSTGRES_TABLE,
        location="api_access_audit",
        owner="Segurança da Informação",
        classification=("PII", "Confidencial"),
        glossary_terms=("PII",),
        description=(
            "Log de auditoria de acesso à API: quem (id da chave, nunca a chave), o quê (rota e "
            "parâmetros de consulta), o resultado (inclusive 401) e o IP de origem. Append-only; "
            "health checks ficam de fora (issue #58)."
        ),
        optional=True,
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
        description=(
            "Cotações publicadas em tempo real pelo producer. Sem consumidor na V1: o Spark "
            "Streaming só lê raw-transactions e o Bronze de mercado vem do yfinance."
        ),
    ),
    CatalogEntry(
        key="kafka_enriched_transactions",
        name="Kafka — enriched-transactions",
        layer=DatasetLayer.STREAMING,
        kind=DatasetKind.KAFKA_TOPIC,
        location=settings.kafka.topic_enriched,
        owner="Data Engineering",
        classification=("PII", "Confidencial"),
        glossary_terms=("Fraud Engine", "Shadow Scoring", "Z-Score", "Fraud Score"),
        description=(
            "Transações com o veredito do Fraud Engine anexado pelo StreamProcessor: fraud_score, "
            "fraud_signals (o motivo), fraud_type_predicted; e o Z-Score antigo em paralelo "
            "(issues #11 e #46)."
        ),
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
        glossary_terms=("Fraud Engine", "Fraud Score", "Velocity Check"),
        description=(
            "Alertas do Fraud Engine multi-signal: tipo inferido pelos sinais (não o rótulo), "
            "lista de sinais ativos e versão do detector (issues #11 e #46)."
        ),
        upstream=("kafka_enriched_transactions",),
    ),
    # ── Dashboard (issue #16) ─────────────────────────────────────────────
    CatalogEntry(
        key="dashboard_fraud_overview",
        name="Dashboard — Visão Geral de Fraude",
        layer=DatasetLayer.DASHBOARD,
        kind=DatasetKind.DASHBOARD,
        location="http://localhost:8088/superset/dashboard/fraude-transacoes-visao-geral/",
        owner="Fraud Analytics",
        classification=("Confidencial",),
        glossary_terms=("Fraud Score",),
        description=(
            "KPIs de volume, valor, taxa de fraude e alertas, mais latência, distribuição de "
            "fraud_score e alertas por hora do streaming (Superset — Grafana descoped, issue #16)."
        ),
        upstream=(
            "serving_fact_transactions",
            "serving_agg_daily_fraud_metrics",
            "serving_stream_scored_transactions",
            "serving_fraud_alerts",
        ),
    ),
    CatalogEntry(
        key="dashboard_platform_health",
        name="Dashboard — Platform Health",
        layer=DatasetLayer.DASHBOARD,
        kind=DatasetKind.DASHBOARD,
        location="http://localhost:8088/superset/dashboard/platform-health/",
        owner="Data Engineering",
        classification=("Interno",),
        glossary_terms=("SLO",),
        description=(
            "Saúde da plataforma, separada dos KPIs de negócio: latência, duração, throughput e lag "
            "do stream; execuções das DAGs e gates com falha; latência e erros da API (issue #55). "
            "As metas ficam no `make slo-report`."
        ),
        upstream=(
            "serving_stream_batch_metrics",
            "serving_pipeline_runs",
            "serving_quality_gate_runs",
            "serving_api_requests",
        ),
    ),
)

CATALOG_BY_KEY: dict[str, CatalogEntry] = {entry.key: entry for entry in CATALOG}

# ── Colunas PII por dataset (issue #58) ────────────────────────────────────────
# Dados pessoais diretos ou sensíveis. `customer_id`/`customer_key` ficam de fora de propósito: são
# pseudônimos (UUID sem significado fora da plataforma), necessários para a API e para os alertas.
# Um teste garante que nenhuma coluna daqui aparece nos modelos de resposta da API.
_TRANSACTION_PII = (
    "origin_account",
    "destination_account",
    "device_id",
    "ip_address",
    "latitude",
    "longitude",
)
_CUSTOMER_PII = ("name", "cpf_masked", "birth_date")
_PROFILE_PII = ("known_devices", "known_ip_prefixes", "known_destinations", "home_lat", "home_lon")

PII_COLUMNS: dict[str, tuple[str, ...]] = {
    "bronze_transactions": _TRANSACTION_PII,
    "silver_transactions": _TRANSACTION_PII,
    "silver_transactions_stream": _TRANSACTION_PII,
    "kafka_raw_transactions": _TRANSACTION_PII,
    "kafka_enriched_transactions": _TRANSACTION_PII,
    "gold_dim_customers": _CUSTOMER_PII,
    # no Postgres o nome vai só com iniciais e a data de nascimento só com o ano (gold_to_postgres)
    "serving_dim_customers": _CUSTOMER_PII,
    "gold_customer_behavior_profile": _PROFILE_PII,
    "serving_api_access_audit": ("client_host",),
}
