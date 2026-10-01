"""Custo do producer idempotente no throughput (issue #68).

Mede o producer com idempotência ligada × desligada, em dois modos de envio, contra o Kafka local:

  * síncrono: `send().get()` a cada mensagem, como os producers do projeto fazem. Só há um request
    em voo de qualquer jeito, então o `max_in_flight_requests_per_connection=1` que o kafka-python
    exige com idempotência não deveria pesar;
  * assíncrono: `send()` em sequência e um `flush()` no fim. É onde o limite de 1 request em voo
    aparece, porque sem idempotência o cliente manda até 5 lotes sem esperar ack.

Usa um tópico próprio (`bench-producer-idempotence`, criado e apagado aqui), o payload real de uma
transação e o `ProducerConfig` do projeto (compressão lz4, batch, linger). Mostra também o
producer id que o broker atribuiu, que é o que permite descartar um retry duplicado.

    python -m scripts.producer_idempotence_benchmark                 # 3 rodadas por cenário
    python -m scripts.producer_idempotence_benchmark --rounds 5 --sync-messages 2000
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import UTC, datetime

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from src.common.config import settings
from src.common.data_generator import DataGenerator, TransactionStream
from src.ingestion.streaming.kafka_producer_transactions import _build_message, _serialize
from src.ingestion.streaming.producer_config import ProducerConfig

TOPIC = "bench-producer-idempotence"


def _payloads(n: int) -> list[tuple[bytes, bytes]]:
    gen = DataGenerator(seed=42)
    stream = TransactionStream(gen, gen.generate_customers(n=200))
    out: list[tuple[bytes, bytes]] = []
    while len(out) < n:
        now = datetime.now(tz=UTC)
        for _delay, tx in stream.next_events(now):
            tx["timestamp"] = now.isoformat()
            out.append((tx["customer_id"].encode(), _serialize(_build_message(tx)).encode()))
    return out[:n]


def _producer(idempotent: bool) -> KafkaProducer:
    if idempotent:
        cfg = ProducerConfig()
    else:
        # O que o projeto usava antes da #68: acks="all", sem idempotência, 5 em voo (padrão).
        cfg = ProducerConfig(enable_idempotence=False, max_in_flight_requests_per_connection=5)
    options = cfg.to_kafka_python_dict()
    options.pop("key_serializer")
    options.pop("value_serializer")
    return KafkaProducer(**options)


def _run(idempotent: bool, sync: bool, payloads: list[tuple[bytes, bytes]]) -> dict:
    producer = _producer(idempotent)
    # Aquecimento: conexão, metadata e (com idempotência) o pedido de producer id ao broker.
    producer.send(TOPIC, key=b"warmup", value=b"{}").get(timeout=30)
    latencies: list[float] = []
    start = time.perf_counter()
    if sync:
        for key, value in payloads:
            t0 = time.perf_counter()
            producer.send(TOPIC, key=key, value=value).get(timeout=30)
            latencies.append((time.perf_counter() - t0) * 1000)
    else:
        futures = [producer.send(TOPIC, key=key, value=value) for key, value in payloads]
        producer.flush(timeout=120)
        for future in futures:
            future.get(timeout=0)  # erro de envio vira exceção aqui
    elapsed = time.perf_counter() - start
    manager = producer._transaction_manager  # noqa: SLF001 - só para mostrar o producer id
    producer_id = manager.producer_id_and_epoch.producer_id if manager else None
    producer.close()
    result = {"msgs_per_s": len(payloads) / elapsed, "producer_id": producer_id}
    if latencies:
        quantiles = statistics.quantiles(latencies, n=100)
        result |= {"p50_ms": statistics.median(latencies), "p95_ms": quantiles[94]}
    return result


def _reset_topic(admin: KafkaAdminClient, create: bool) -> None:
    try:
        admin.delete_topics([TOPIC])
    except UnknownTopicOrPartitionError:
        pass
    if not create:
        return
    deadline = time.time() + 60
    while True:
        try:
            admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
            return
        except TopicAlreadyExistsError:  # a exclusão é assíncrona
            if time.time() > deadline:
                raise
            time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--sync-messages", type=int, default=2_000)
    parser.add_argument("--async-messages", type=int, default=20_000)
    parser.add_argument("--json", help="grava o resultado neste arquivo")
    args = parser.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
    _reset_topic(admin, create=True)
    results: dict[str, list[dict]] = {}
    try:
        for sync, n in ((True, args.sync_messages), (False, args.async_messages)):
            payloads = _payloads(n)
            for _ in range(args.rounds):
                # Alterna a ordem a cada rodada para não favorecer um lado (cache, aquecimento).
                for idempotent in (False, True) if _ % 2 == 0 else (True, False):
                    name = f"{'sync' if sync else 'async'}-{'idempotente' if idempotent else 'sem'}"
                    results.setdefault(name, []).append(_run(idempotent, sync, payloads))
    finally:
        _reset_topic(admin, create=False)
        admin.close()

    summary = {}
    for name, runs in results.items():
        row = {"msgs_per_s": statistics.median(r["msgs_per_s"] for r in runs)}
        if "p50_ms" in runs[0]:
            row["p50_ms"] = statistics.median(r["p50_ms"] for r in runs)
            row["p95_ms"] = statistics.median(r["p95_ms"] for r in runs)
        row["producer_ids"] = [r["producer_id"] for r in runs]
        summary[name] = row

    print(f"{'cenário':<20} {'msgs/s':>10} {'p50 ms':>8} {'p95 ms':>8}  producer id (broker)")
    for name, row in summary.items():
        p50 = f"{row['p50_ms']:.2f}" if "p50_ms" in row else "-"
        p95 = f"{row['p95_ms']:.2f}" if "p95_ms" in row else "-"
        print(f"{name:<20} {row['msgs_per_s']:>10.0f} {p50:>8} {p95:>8}  {row['producer_ids']}")
    for mode in ("sync", "async"):
        base, idem = summary[f"{mode}-sem"], summary[f"{mode}-idempotente"]
        change = (idem["msgs_per_s"] / base["msgs_per_s"] - 1) * 100
        print(f"{mode}: throughput com idempotência {change:+.1f}% (mediana de {args.rounds} rodadas)")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
