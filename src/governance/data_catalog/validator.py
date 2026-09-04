"""Validação do catálogo contra a infra real (issue #14).

Duas categorias de checagem:

1. Integridade referencial (sem infra) — toda `upstream` deve apontar para
   uma key existente no catálogo. Roda sempre, inclusive em CI.
2. Existência real (com infra) — MinIO (prefixo tem objetos), Postgres
   (tabela existe em `information_schema`) e Kafka (tópico existe no
   broker). Requer `make up` rodando; não roda em CI, mesmo motivo dos
   testes de integração do resto do projeto (ver Makefile `test-integration`).
   `DatasetKind.DASHBOARD` não tem checagem de infra: issue #16 ainda não
   existe, então esses assets ficam com `status="planejado"`.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.common.config import settings
from src.common.logger import get_logger
from src.common.storage import get_storage_client
from src.governance.data_catalog.registry import CatalogEntry, DatasetKind

logger = get_logger("data_catalog")


@dataclass
class ValidationResult:
    entry_key: str
    ok: bool
    detail: str


def validate_references(entries: tuple[CatalogEntry, ...]) -> list[ValidationResult]:
    """Checa que todo `upstream` referencia uma key existente no catálogo."""
    keys = {e.key for e in entries}
    results = []
    for entry in entries:
        missing = [u for u in entry.upstream if u not in keys]
        if missing:
            results.append(ValidationResult(entry.key, False, f"upstream desconhecido: {missing}"))
        else:
            results.append(ValidationResult(entry.key, True, "linhagem ok"))
    return results


def _validate_minio(entry: CatalogEntry) -> ValidationResult:
    bucket, _, prefix = entry.location.removeprefix("s3://").partition("/")
    objects = get_storage_client().list_objects(bucket, prefix)
    if objects:
        return ValidationResult(entry.key, True, f"{len(objects)} objeto(s) em {entry.location}")
    return ValidationResult(entry.key, False, f"nenhum objeto encontrado em {entry.location}")


def _validate_postgres(entry: CatalogEntry) -> ValidationResult:
    from sqlalchemy import create_engine, text

    engine = create_engine(settings.postgres.url)
    try:
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = :name"
                ),
                {"name": entry.location},
            ).scalar()
    finally:
        engine.dispose()
    if count:
        return ValidationResult(entry.key, True, f"tabela '{entry.location}' existe")
    return ValidationResult(entry.key, False, f"tabela '{entry.location}' não encontrada")


def _validate_kafka(entry: CatalogEntry) -> ValidationResult:
    from kafka.admin import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        topics = admin.list_topics()
    finally:
        admin.close()
    if entry.location in topics:
        return ValidationResult(entry.key, True, f"tópico '{entry.location}' existe")
    return ValidationResult(entry.key, False, f"tópico '{entry.location}' não encontrado")


_LIVE_VALIDATORS = {
    DatasetKind.MINIO_PREFIX: _validate_minio,
    DatasetKind.POSTGRES_TABLE: _validate_postgres,
    DatasetKind.KAFKA_TOPIC: _validate_kafka,
}


def validate_live(entries: tuple[CatalogEntry, ...]) -> list[ValidationResult]:
    """Checa existência real de cada asset contra MinIO/Postgres/Kafka.

    Requer a infra local rodando (`make up`). Entradas `DASHBOARD` são
    puladas (sem serviço para checar ainda — issue #16 pendente).
    """
    results = []
    for entry in entries:
        validator = _LIVE_VALIDATORS.get(entry.kind)
        if validator is None:
            continue
        try:
            result = validator(entry)
        except Exception as exc:
            result = ValidationResult(entry.key, False, f"erro ao validar: {exc}")
            logger.warning("Falha ao validar asset do catálogo", entry=entry.key, error=str(exc))
        results.append(result)
    return results
