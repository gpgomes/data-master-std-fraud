"""Teste ponta a ponta do pipeline (issue #57): invariantes de dados e cenários de falha.

Roda **uma vez** o batch (CSV → Bronze → Silver → Gold → Postgres, com os quality gates) e o stream
(Kafka → Fraud Engine → Parquet/Kafka → Postgres), com os cenários de falha no meio, e depois cada
teste verifica uma invariante sobre o que ficou gravado. Invariantes, não "HTTP 200": o mesmo
conjunto de `transaction_id` e a mesma soma de valor atravessam as camadas, os `alert_id` do Kafka
são os do Postgres, reiniciar o stream não duplica nada, dado inválido não é promovido.

Uso: `make e2e` (stack de pé; ~15–20 min). Ver `docs/runbook.md`, seção "Teste ponta a ponta".
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time

import pandas as pd
import pytest

from src.common.config import settings
from tests.e2e import harness as h
from tests.e2e.publisher import publish

BATCH_TRANSACTIONS = 3_000
BATCH_CUSTOMERS = 1_000
GOLD_GATES = (
    "gold_fact_transactions",
    "gold_dim_customers",
    "gold_dim_date",
    "gold_agg_daily_fraud_metrics",
    "gold_customer_behavior_profile",
)


# ── Execução do pipeline (uma vez por sessão) ────────────────────────────────────


@pytest.fixture(scope="session")
def namespace():
    h.kill_stream()
    h.reset_stream_log()
    h.reset_buckets()
    h.reset_topics()
    h.reset_database()
    yield


def _gate(dataset: str) -> None:
    from src.governance.great_expectations.runner import run_gate

    run_gate(dataset)


@pytest.fixture(scope="session")
def batch(namespace):
    from src.ingestion.batch.customer_loader import CustomerLoader
    from src.ingestion.batch.transaction_loader import TransactionLoader

    subprocess.run(
        [
            sys.executable,
            "scripts/generate_sample_data.py",
            "--transactions",
            str(BATCH_TRANSACTIONS),
            "--customers",
            str(BATCH_CUSTOMERS),
            "--market-days",
            "1",
            "--months",
            "1",
            "--seed",
            "42",
            "--output-dir",
            str(h.DATA_DIR),
        ],
        cwd=h.ROOT,
        check=True,
        capture_output=True,
    )
    generated = pd.concat(
        pd.read_csv(p) for p in sorted((h.DATA_DIR / "transactions").rglob("*.csv"))
    )

    TransactionLoader(source_dir=h.DATA_DIR / "transactions").load_to_bronze()
    CustomerLoader(source_file=h.DATA_DIR / "customers" / "customers.csv").load_to_bronze()
    _gate("bronze_transactions")
    h.spark_submit("src/transformation/batch/bronze_to_silver.py")
    _gate("silver_transactions")
    h.spark_submit("src/transformation/batch/silver_to_gold.py")
    for dataset in GOLD_GATES:
        _gate(dataset)
    h.spark_submit("src/serving/loaders/gold_to_postgres.py")
    return {"generated": generated}


class _StreamRun:
    """O que aconteceu na execução do stream, para os testes verificarem."""

    def __init__(self) -> None:
        self.published: list[str] = []
        self.duplicated_id: str | None = None
        self.malformed = 0
        self.waves: dict[str, list[str]] = {}
        self.caught_mid_batch = False
        self.replay_logged = False
        self.kafka_outage: dict = {}
        self.postgres_outage: dict = {}


def _stream_ids() -> pd.Series:
    frame = h.read_parquet(settings.minio.bucket_silver, "transactions_stream/", ["transaction_id"])
    return frame["transaction_id"] if len(frame) else pd.Series(dtype=str)


def _wait_ids(ids: list[str], timeout: float, what: str) -> None:
    wanted = set(ids)
    h.wait_for(lambda: wanted <= set(_stream_ids()), timeout=timeout, what=what, every=5)


def _batches_with(stage: str) -> set[int]:
    prefix = "stream_processor_progress/"
    keys = h.list_keys(settings.minio.bucket_checkpoints, prefix)
    return {
        int(k.split("batch_")[1].split(".")[0])
        for k in keys
        if k.endswith(f".{stage}") and "batch_" in k
    }


def _kill_mid_batch(timeout: float = 90) -> bool:
    """Mata o stream quando algum micro-batch gravou o Parquet mas ainda não o estado (o fim do
    `foreachBatch`). Devolve se pegou o meio do batch; se não pegar no tempo, mata mesmo assim."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        partial = _batches_with("parquet") - _batches_with("history")
        if partial:
            h.kill_stream()
            return True
        time.sleep(0.2)
    h.kill_stream()
    return False


@pytest.fixture(scope="session")
def stream_run(batch):
    run = _StreamRun()
    h.start_stream()

    # Onda 1: eventos normais + uma duplicata idêntica + um JSON inválido.
    wave1 = publish(400, events_seed=101, duplicate=True, malformed=True)
    run.waves["normal"] = wave1.ids
    run.duplicated_id, run.malformed = wave1.duplicated_id, wave1.malformed
    _wait_ids(wave1.ids, 300, "onda 1 no Parquet")

    # Onda 2: kill -9 no meio de um micro-batch e reinício com o mesmo checkpoint.
    result: dict = {}
    publisher = threading.Thread(
        target=lambda: result.update(p=publish(400, events_seed=202, rate_tps=40))
    )
    publisher.start()
    run.caught_mid_batch = _kill_mid_batch()
    publisher.join()
    run.waves["restart"] = result["p"].ids
    h.start_stream()
    _wait_ids(run.waves["restart"], 300, "onda 2 depois do reinício")
    run.replay_logged = "ignorada no replay" in h.stream_log()

    # Onda 3: Kafka fora do ar por 45 s com o stream rodando.
    h.service_stop("kafka")
    time.sleep(45)
    h.service_start_healthy("kafka")
    run.kafka_outage["stream_alive_after"] = h.stream_alive()
    if not run.kafka_outage["stream_alive_after"]:
        run.kafka_outage["restarted"] = True
        h.start_stream()
    wave3 = publish(200, events_seed=303)
    run.waves["kafka_outage"] = wave3.ids
    _wait_ids(wave3.ids, 300, "onda 3 depois da volta do Kafka")

    # Onda 4: Postgres fora do ar enquanto o stream processa (só as métricas dependem dele).
    h.service_stop("postgres")
    wave4 = publish(200, events_seed=404)
    run.waves["postgres_outage"] = wave4.ids
    _wait_ids(wave4.ids, 300, "onda 4 com o Postgres fora")
    run.postgres_outage["stream_alive_during"] = h.stream_alive()
    h.service_start_healthy("postgres")

    run.published = [i for ids in run.waves.values() for i in ids]
    time.sleep(15)  # último micro-batch termina as etapas Kafka e estado
    h.kill_stream()
    h.spark_submit("src/serving/loaders/stream_to_postgres.py")
    _write_report(run)
    return run


def _write_report(run: _StreamRun) -> None:
    """O que aconteceu nos cenários, para o registro do teste e o artefato do CI."""
    import json

    stored = _stream_ids()
    report = {
        "events_published": len(run.published),
        "waves": {k: len(v) for k, v in run.waves.items()},
        "duplicated_id_resent": run.duplicated_id,
        "malformed_messages": run.malformed,
        "parquet_rows": int(len(stored)),
        "parquet_distinct": int(stored.nunique()),
        "restart": {"caught_mid_batch": run.caught_mid_batch, "replay_logged": run.replay_logged},
        "kafka_outage": run.kafka_outage,
        "postgres_outage": run.postgres_outage,
        "stream_batches_recorded": h.query("SELECT COUNT(*) FROM stream_batch_metrics")[0][0],
        "alerts": h.query("SELECT COUNT(*) FROM fraud_alerts")[0][0],
    }
    (h.DATA_DIR / "report.json").write_text(json.dumps(report, indent=2, default=str))


# ── Batch ───────────────────────────────────────────────────────────────────────


class TestBatchInvariants:
    def test_transaction_ids_are_the_same_in_every_layer(self, batch):
        generated = set(batch["generated"]["transaction_id"])
        bronze = h.read_parquet(settings.minio.bucket_bronze, "transactions/", ["transaction_id"])
        silver = h.read_parquet(settings.minio.bucket_silver, "transactions/", ["transaction_id"])
        gold = h.read_parquet(settings.minio.bucket_gold, "fact_transactions/", ["transaction_id"])
        postgres = {r[0] for r in h.query("SELECT transaction_id FROM fact_transactions")}

        assert len(generated) == BATCH_TRANSACTIONS
        assert set(bronze["transaction_id"]) == generated
        assert set(silver["transaction_id"]) == generated and silver["transaction_id"].is_unique
        assert set(gold["transaction_id"]) == generated and gold["transaction_id"].is_unique
        assert postgres == generated

    def test_amount_sum_is_the_same_in_silver_gold_and_postgres(self, batch):
        silver = h.read_parquet(settings.minio.bucket_silver, "transactions/", ["amount_brl"])
        gold = h.read_parquet(settings.minio.bucket_gold, "fact_transactions/", ["amount_brl"])
        postgres = h.query("SELECT SUM(amount_brl) FROM fact_transactions")[0][0]
        total = round(float(silver["amount_brl"].sum()), 2)
        assert round(float(gold["amount_brl"].sum()), 2) == total
        assert round(float(postgres), 2) == total

    def test_fraud_labels_are_preserved(self, batch):
        expected = int(batch["generated"]["is_fraud"].astype(str).str.lower().eq("true").sum())
        gold = h.read_parquet(settings.minio.bucket_gold, "fact_transactions/", ["is_fraud"])
        postgres = h.query("SELECT COUNT(*) FROM fact_transactions WHERE is_fraud")[0][0]
        assert expected > 0
        assert int(gold["is_fraud"].sum()) == expected == postgres

    def test_daily_aggregate_adds_up_to_the_fact(self, batch):
        total, frauds = h.query(
            "SELECT SUM(total_transactions), SUM(fraud_count) FROM agg_daily_fraud_metrics"
        )[0]
        fact = h.query("SELECT COUNT(*), COUNT(*) FILTER (WHERE is_fraud) FROM fact_transactions")[
            0
        ]
        assert (total, frauds) == fact

    def test_profile_has_one_row_per_customer(self, batch):
        profile = h.read_parquet(
            settings.minio.bucket_gold, "customer_behavior_profile/", ["customer_id"]
        )
        dim = h.query("SELECT customer_key FROM dim_customers")
        assert profile["customer_id"].is_unique
        assert set(profile["customer_id"]) == {r[0] for r in dim}


class TestInvalidDataBlocksPromotion:
    """Um arquivo inválido no Bronze faz o gate falhar, e o Silver não é tocado."""

    BAD_KEY = "transactions/year=2099/month=01/day=01/e2e_invalid.parquet"

    def test_invalid_bronze_row_stops_the_pipeline_before_silver(self, batch):
        from src.governance.great_expectations.runner import QualityGateFailed

        silver_before = set(h.list_keys(settings.minio.bucket_silver, "transactions/"))
        good = h.read_parquet(settings.minio.bucket_bronze, "transactions/").head(1).copy()
        good["transaction_id"] = "e2e-invalido"
        good["amount"] = -10.0  # viola amount >= 0,01
        good["is_fraud"] = True
        good["fraud_type"] = None  # viola fraud_type obrigatório quando is_fraud
        h.put_parquet(settings.minio.bucket_bronze, self.BAD_KEY, good)
        try:
            with pytest.raises(QualityGateFailed):
                _gate("bronze_transactions")  # o gate vem antes do job, como na DAG
                h.spark_submit("src/transformation/batch/bronze_to_silver.py")
            assert set(h.list_keys(settings.minio.bucket_silver, "transactions/")) == silver_before
        finally:
            h.delete_key(settings.minio.bucket_bronze, self.BAD_KEY)
        _gate("bronze_transactions")  # sem o arquivo inválido, volta a passar


# ── Stream ──────────────────────────────────────────────────────────────────────


class TestStreamInvariants:
    def test_every_published_event_reaches_the_parquet(self, stream_run):
        stored = _stream_ids()
        assert set(stream_run.published) <= set(stored)
        assert set(stored) <= set(stream_run.published)  # nada de fora (o JSON inválido some)

    def test_no_duplicates_in_the_parquet_except_the_resent_event(self, stream_run):
        """O Parquet deduplica dentro do micro-batch; uma duplicata que cai em outro micro-batch
        fica (at-least-once), e é o loader que entrega uma linha só. Qualquer outra duplicata,
        inclusive do reinício no meio do batch, é defeito."""
        counts = _stream_ids().value_counts()
        duplicated = set(counts[counts > 1].index)
        assert duplicated <= {stream_run.duplicated_id}

    def test_serving_layer_has_each_event_exactly_once(self, stream_run):
        rows = h.query("SELECT transaction_id FROM stream_scored_transactions")
        ids = [r[0] for r in rows]
        assert len(ids) == len(set(ids))
        assert set(ids) == set(stream_run.published)

    def test_enriched_topic_carries_every_event(self, stream_run):
        enriched = set(h.json_field(h.consume_all(settings.kafka.topic_enriched), "transaction_id"))
        assert enriched == set(stream_run.published)

    def test_alert_ids_in_kafka_match_the_postgres_alerts(self, stream_run):
        kafka = set(h.json_field(h.consume_all(settings.kafka.topic_fraud_alerts), "alert_id"))
        postgres = {r[0] for r in h.query("SELECT alert_id FROM fraud_alerts")}
        assert kafka, "nenhum alerta: o teste não verificaria nada"
        assert kafka == postgres

    def test_malformed_message_was_dropped_and_the_stream_kept_going(self, stream_run):
        assert stream_run.malformed == 1
        # eventos publicados DEPOIS da mensagem inválida foram processados
        assert set(stream_run.waves["restart"]) <= set(_stream_ids())

    def test_platform_metrics_were_recorded_for_the_stream(self, stream_run):
        batches, scored = h.query(
            "SELECT COUNT(*), COALESCE(SUM(rows_scored), 0) FROM stream_batch_metrics"
        )[0]
        assert batches > 0
        assert scored >= len(stream_run.waves["normal"])


class TestFailureScenarios:
    def test_restart_mid_batch_loses_and_duplicates_nothing(self, stream_run):
        stored = _stream_ids()
        restart_ids = set(stream_run.waves["restart"])
        assert restart_ids <= set(stored)
        assert not (stored[stored.isin(restart_ids)].duplicated().any())
        if stream_run.caught_mid_batch:
            assert stream_run.replay_logged, "reinício no meio do batch deveria pular etapas feitas"

    def test_stream_processes_events_after_a_kafka_outage(self, stream_run):
        assert set(stream_run.waves["kafka_outage"]) <= set(_stream_ids())

    def test_stream_keeps_processing_while_postgres_is_down(self, stream_run):
        assert stream_run.postgres_outage["stream_alive_during"]
        assert set(stream_run.waves["postgres_outage"]) <= set(_stream_ids())
