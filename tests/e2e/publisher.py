"""Publicador determinístico de transações para o E2E (issue #57).

Usa o mesmo gerador e a mesma validação do producer (`TransactionStream`, `_build_message`), com os
mesmos 1.000 clientes (seed 42) que o batch do E2E gera, para o perfil do Gold encontrar cada
cliente. Diferente do producer, publica um número exato de eventos e devolve os `transaction_id`,
que são a base das invariantes. Opcionalmente reenvia um evento idêntico (duplicata, como num retry
do Kafka) e uma mensagem com JSON inválido.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from src.common.config import settings
from src.common.data_generator import DataGenerator, TransactionStream
from src.ingestion.streaming.kafka_producer_transactions import _build_message, _serialize
from tests.e2e.harness import producer

MALFORMED = b'{"transaction_id": "quebrado", "amount": '


@dataclass
class Published:
    ids: list[str] = field(default_factory=list)
    duplicated_id: str | None = None
    malformed: int = 0

    @property
    def messages_sent(self) -> int:
        return len(self.ids) + (1 if self.duplicated_id else 0) + self.malformed


def publish(
    count: int,
    events_seed: int,
    rate_tps: float = 20.0,
    duplicate: bool = False,
    malformed: bool = False,
) -> Published:
    gen = DataGenerator(seed=42)
    customers = gen.generate_customers(n=1_000)
    gen.reseed_events(events_seed)
    stream = TransactionStream(gen, customers)
    kafka = producer()
    topic = settings.kafka.topic_transactions
    out = Published()
    first_payload: tuple[str, str] | None = None
    try:
        while len(out.ids) < count:
            now = datetime.now(tz=UTC)
            for _delay, tx in stream.next_events(now):
                if len(out.ids) >= count:
                    break
                tx["timestamp"] = now.isoformat()
                payload = _serialize(_build_message(tx))
                kafka.send(topic, value=payload.encode(), key=tx["customer_id"].encode()).get(10)
                out.ids.append(tx["transaction_id"])
                if first_payload is None:
                    first_payload = (tx["customer_id"], payload)
            time.sleep(1.0 / rate_tps)
        if duplicate and first_payload is not None:
            key, payload = first_payload
            kafka.send(topic, value=payload.encode(), key=key.encode()).get(10)
            out.duplicated_id = out.ids[0]
        if malformed:
            kafka.send(topic, value=MALFORMED, key=b"e2e").get(10)
            out.malformed = 1
    finally:
        kafka.flush()
        kafka.close()
    return out
