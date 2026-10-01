"""Harness do teste ponta a ponta (issue #57).

Roda o pipeline de verdade contra a stack Docker, num **namespace isolado** (`e2e-*` nos buckets e
tópicos, banco `fraud_e2e` no Postgres): rodar local não toca nos dados de desenvolvimento. Os jobs
Spark rodam dentro do container `spark-master` em modo local (`--master local[2]`), o que funciona
tanto com a stack completa quanto com a stack mínima do CI (sem workers, Airflow nem Superset).

O próprio processo do pytest precisa das mesmas variáveis (os loaders, o GX e as consultas usam
`settings`): o `make e2e` as exporta antes de chamar o pytest. `E2E_ENV` é a fonte única delas.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Iterable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

E2E_ENV = {
    "MINIO_BUCKET_BRONZE": "e2e-bronze",
    "MINIO_BUCKET_SILVER": "e2e-silver",
    "MINIO_BUCKET_GOLD": "e2e-gold",
    "MINIO_BUCKET_CHECKPOINTS": "e2e-checkpoints",
    "KAFKA_TOPIC_TRANSACTIONS": "e2e-raw-transactions",
    "KAFKA_TOPIC_MARKET_DATA": "e2e-raw-market-data",
    "KAFKA_TOPIC_ENRICHED": "e2e-enriched-transactions",
    "KAFKA_TOPIC_FRAUD_ALERTS": "e2e-fraud-alerts",
    "POSTGRES_DB": "fraud_e2e",
}
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
STREAM_APP_NAME = "e2e-stream"
STREAM_LOG = "/tmp/e2e_stream.log"
DATA_DIR = ROOT / "data" / "e2e"


class StepFailed(RuntimeError):
    """Um passo do pipeline (job Spark, loader) falhou; a mensagem traz o fim do log."""


def env_is_e2e() -> bool:
    """O processo foi iniciado com o namespace do E2E (proteção contra apagar dado de dev)."""
    return all(os.environ.get(k) == v for k, v in E2E_ENV.items())


# ── Docker ──────────────────────────────────────────────────────────────────────


def compose(*args: str, check: bool = True, timeout: float = 900) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout
    )
    if check and result.returncode != 0:
        tail = (result.stdout + result.stderr)[-3000:]
        raise StepFailed(
            f"docker compose {' '.join(args[:3])}... saiu com {result.returncode}\n{tail}"
        )
    return result


def _env_flags() -> list[str]:
    flags: list[str] = []
    for key, value in E2E_ENV.items():
        flags += ["-e", f"{key}={value}"]
    return flags


def spark_submit(app: str, *app_args: str, kafka: bool = False, timeout: float = 900) -> str:
    """Roda um job Spark no container, em modo local, com o namespace do E2E. Devolve o log."""
    packages = ["--packages", KAFKA_PACKAGE] if kafka else []
    result = compose(
        "exec",
        "-T",
        *_env_flags(),
        "spark-master",
        "spark-submit",
        "--master",
        "local[2]",
        *packages,
        app,
        "--local",
        *app_args,
        timeout=timeout,
    )
    return result.stdout + result.stderr


def start_stream(trigger_seconds: int = 5) -> None:
    """Sobe o stream em segundo plano no container, lendo o tópico do E2E desde o início."""
    command = (
        f"spark-submit --master 'local[2]' --name {STREAM_APP_NAME} --packages {KAFKA_PACKAGE} "
        "src/transformation/streaming/stream_processor.py --local --starting-offsets earliest "
        f"--trigger-seconds {trigger_seconds} >> {STREAM_LOG} 2>&1"
    )
    compose("exec", "-d", *_env_flags(), "spark-master", "sh", "-c", command)
    wait_for(lambda: "StreamProcessor iniciado" in stream_log(), timeout=240, what="stream subir")


def stream_log() -> str:
    return compose(
        "exec", "-T", "spark-master", "sh", "-c", f"cat {STREAM_LOG} 2>/dev/null", check=False
    ).stdout


def stream_alive() -> bool:
    out = compose("exec", "-T", "spark-master", "pgrep", "-f", STREAM_APP_NAME, check=False)
    return out.returncode == 0


def kill_stream() -> None:
    """`kill -9` no stream do E2E (e só nele: o padrão é o `--name` da aplicação)."""
    compose("exec", "-T", "spark-master", "pkill", "-9", "-f", STREAM_APP_NAME, check=False)
    wait_for(lambda: not stream_alive(), timeout=60, what="stream morrer")


def reset_stream_log() -> None:
    compose("exec", "-T", "spark-master", "sh", "-c", f"rm -f {STREAM_LOG}", check=False)


def service_stop(name: str) -> None:
    compose("stop", name)


def service_start_healthy(name: str, timeout: float = 180) -> None:
    compose("start", name)

    def healthy() -> bool:
        out = compose("ps", name, "--format", "{{.Status}}", check=False).stdout
        return "healthy" in out and "unhealthy" not in out

    wait_for(healthy, timeout=timeout, what=f"{name} ficar healthy")


# ── Espera ──────────────────────────────────────────────────────────────────────


def wait_for(condition, timeout: float, what: str, every: float = 3.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = condition()
        except Exception as exc:  # noqa: BLE001 - a condição pode falhar enquanto o recurso sobe
            last = exc
        if last and not isinstance(last, Exception):
            return last
        time.sleep(every)
    raise TimeoutError(f"tempo esgotado esperando: {what} (último valor: {last!r})")


# ── MinIO ───────────────────────────────────────────────────────────────────────


def _s3():
    import boto3

    from src.common.config import settings

    return boto3.client(
        "s3",
        endpoint_url=settings.minio.endpoint,
        aws_access_key_id=settings.minio.access_key,
        aws_secret_access_key=settings.minio.secret_key,
        region_name="us-east-1",
    )


def reset_buckets() -> None:
    """Recria vazios os buckets do E2E (nunca os de dev: a lista vem de `E2E_ENV`)."""
    s3 = _s3()
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for key in (
        "MINIO_BUCKET_BRONZE",
        "MINIO_BUCKET_SILVER",
        "MINIO_BUCKET_GOLD",
        "MINIO_BUCKET_CHECKPOINTS",
    ):
        bucket = E2E_ENV[key]
        assert bucket.startswith("e2e-"), bucket
        if bucket in existing:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket):
                objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        else:
            s3.create_bucket(Bucket=bucket)


def list_keys(bucket: str, prefix: str) -> list[str]:
    s3 = _s3()
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def read_parquet(bucket: str, prefix: str, columns: list[str] | None = None):
    """Todos os arquivos Parquet sob o prefixo, num DataFrame pandas (arquivo a arquivo, como o GX)."""
    import pandas as pd

    from src.common.config import settings

    options = {
        "key": settings.minio.access_key,
        "secret": settings.minio.secret_key,
        "client_kwargs": {"endpoint_url": settings.minio.endpoint},
    }
    files = [k for k in list_keys(bucket, prefix) if k.endswith(".parquet")]
    if not files:
        return pd.DataFrame(columns=columns or [])
    frames = [
        pd.read_parquet(f"s3://{bucket}/{k}", columns=columns, storage_options=options)
        for k in files
    ]
    return pd.concat(frames, ignore_index=True)


def put_parquet(bucket: str, key: str, frame) -> None:
    import io

    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    _s3().put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def delete_key(bucket: str, key: str) -> None:
    _s3().delete_object(Bucket=bucket, Key=key)


# ── Kafka ───────────────────────────────────────────────────────────────────────


def reset_topics() -> None:
    from kafka.admin import KafkaAdminClient, NewTopic

    from src.common.config import settings

    topics = [v for k, v in E2E_ENV.items() if k.startswith("KAFKA_TOPIC_")]
    assert all(t.startswith("e2e-") for t in topics)
    admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        present = [t for t in topics if t in admin.list_topics()]
        if present:
            admin.delete_topics(present)
            wait_for(lambda: not set(present) & set(admin.list_topics()), 60, "tópicos sumirem")
        partitions = {E2E_ENV["KAFKA_TOPIC_FRAUD_ALERTS"]: 1}

        def create() -> bool:
            # A exclusão é assíncrona: o tópico some da listagem antes de o broker terminar de
            # apagá-lo, e recriar nesse intervalo dá TopicAlreadyExistsError ("marked for deletion").
            from kafka.errors import TopicAlreadyExistsError

            missing = [t for t in topics if t not in admin.list_topics()]
            try:
                admin.create_topics(
                    [
                        NewTopic(t, num_partitions=partitions.get(t, 3), replication_factor=1)
                        for t in missing
                    ]
                )
            except TopicAlreadyExistsError:
                return False
            return True

        wait_for(create, 120, "tópicos do E2E serem recriados")
    finally:
        admin.close()


def producer():
    from kafka import KafkaProducer

    from src.common.config import settings

    # Idempotente como os producers de verdade (#68). A duplicata de propósito do E2E continua
    # chegando: são dois `send` do mesmo payload (números de sequência diferentes), não um retry.
    return KafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers, acks="all", enable_idempotence=True
    )


def consume_all(topic: str, idle_ms: int = 10_000) -> list[bytes]:
    from kafka import KafkaConsumer

    from src.common.config import settings

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=idle_ms,
        group_id=None,
    )
    try:
        return [m.value for m in consumer]
    finally:
        consumer.close()


def json_field(messages: Iterable[bytes], field: str) -> list:
    return [json.loads(m)[field] for m in messages]


# ── Postgres ────────────────────────────────────────────────────────────────────


def _pg(dbname: str):
    import psycopg2

    from src.common.config import settings

    return psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=dbname,
        user=settings.postgres.user,
        password=settings.postgres.password,
        connect_timeout=5,
    )


def reset_database() -> None:
    """Recria o banco `fraud_e2e` (conectando no `postgres`, o banco padrão do servidor)."""
    name = E2E_ENV["POSTGRES_DB"]
    assert name.endswith("_e2e")
    conn = _pg("postgres")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (name,)
            )
            cur.execute(f"DROP DATABASE IF EXISTS {name}")
            cur.execute(f"CREATE DATABASE {name}")
    finally:
        conn.close()


def query(sql: str, params: tuple | None = None) -> list[tuple]:
    conn = _pg(E2E_ENV["POSTGRES_DB"])
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()
