"""Kafka producer de transações financeiras — simula stream em tempo real."""

import heapq
import itertools
import json
import os
import signal
import threading
import time
from datetime import UTC, datetime
from threading import Event
from typing import Any

from loguru import logger

from src.common.config import settings
from src.common.data_generator import DataGenerator, TransactionStream
from src.common.schemas import TransactionEvent
from src.ingestion.streaming.producer_config import ProducerConfig

# ── Configuração ───────────────────────────────────────────────────────────────

TOPIC = settings.kafka.topic_transactions
RATE = float(os.getenv("PRODUCER_RATE_TPS", "10"))          # transações por segundo
SOURCE_SYSTEM = os.getenv("SOURCE_SYSTEM", "transaction-simulator")
# Ritmo diurno: o volume segue o horário ativo dos clientes (cai de madrugada). Por padrão o ritmo
# é constante (PRODUCER_RATE_TPS), como sempre foi.
DIURNAL = os.getenv("PRODUCER_DIURNAL", "false").strip().lower() == "true"
# Seed dos clientes: tem de bater com a do `make seed-data` (--seed 42). Os 1.000 clientes daqui
# são então o prefixo dos 10.000 do batch, e o enriquecimento (dim_customers) e o perfil de
# comportamento (Gold) encontram cada cliente do stream.
CUSTOMER_SEED = int(os.getenv("CUSTOMER_SEED", "42"))
# Seed dos eventos: 0 = por horário. Fixa-la faria cada reinício repetir os mesmos transaction_id.
SEED = int(os.getenv("GENERATOR_SEED", "0")) or None

# ── Métricas em memória ────────────────────────────────────────────────────────

_metrics: dict[str, Any] = {
    "sent": 0,
    "errors": 0,
    "latency_sum_ms": 0.0,
}


def _avg_latency() -> float:
    if _metrics["sent"] == 0:
        return 0.0
    return float(_metrics["latency_sum_ms"]) / float(_metrics["sent"])


# ── Serialização ───────────────────────────────────────────────────────────────

def _build_message(tx: dict[str, Any]) -> dict[str, Any]:
    """Valida a transação contra TransactionEvent e adiciona metadados de proveniência.

    A validação garante que o payload publicado no Kafka sempre respeita o
    contrato de dados (issue #8) — falha rápido se o gerador produzir algo
    incompatível com o schema, em vez de propagar dado inválido no stream.
    """
    event = TransactionEvent(
        **tx,
        produced_at=datetime.now(tz=UTC),
        source_system=SOURCE_SYSTEM,
    )
    return event.model_dump(mode="json")


def _serialize(msg: dict[str, Any]) -> str:
    return json.dumps(msg, default=str)


# ── Producer loop ──────────────────────────────────────────────────────────────

def run(stop_event: Event | None = None) -> None:
    """Inicia o producer. Bloqueia até stop_event ser sinalizado ou SIGINT."""
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    if stop_event is None:
        stop_event = Event()

    # Graceful shutdown via SIGINT / SIGTERM (apenas na thread principal)
    if threading.current_thread() is threading.main_thread():
        def _handle_signal(signum, frame):  # noqa: ANN001
            logger.info("Sinal recebido, encerrando producer...")
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    config = ProducerConfig()
    producer = KafkaProducer(**config.to_kafka_python_dict())

    gen = DataGenerator(seed=CUSTOMER_SEED)
    # Pré-gera clientes para reutilizar nos eventos
    customers = gen.generate_customers(n=1_000)
    gen.reseed_events(SEED or int(time.time()))
    stream = TransactionStream(gen, customers, diurnal=DIURNAL)

    # Follow-ups de episódios de fraude saem com atraso: heap por instante de emissão.
    pending: list[tuple[float, int, dict[str, Any]]] = []
    tiebreak = itertools.count()

    def send(tx: dict[str, Any]) -> None:
        # O timestamp é o instante de emissão: o detector usa janela em tempo de evento e o
        # watermark do streaming depende de o evento chegar "agora".
        tx["timestamp"] = datetime.now(tz=UTC).isoformat()
        msg = _build_message(tx)
        payload = _serialize(msg)
        key = tx["customer_id"]

        t0 = time.monotonic()
        try:
            future = producer.send(TOPIC, value=payload, key=key, headers=[
                ("produced_at", msg["produced_at"].encode()),
                ("source_system", SOURCE_SYSTEM.encode()),
            ])
            future.get(timeout=10)
            latency_ms = (time.monotonic() - t0) * 1000
            _metrics["sent"] += 1
            _metrics["latency_sum_ms"] += latency_ms

            if _metrics["sent"] % 100 == 0:
                logger.info(
                    f"Enviadas {_metrics['sent']} msgs | "
                    f"erros={_metrics['errors']} | "
                    f"lat_media={_avg_latency():.1f}ms"
                )

        except KafkaError as exc:
            _metrics["errors"] += 1
            logger.error(f"Erro ao enviar mensagem: {exc}")

    interval = 1.0 / RATE
    logger.info(f"Producer iniciado | tópico={TOPIC} | rate={RATE} tps")

    try:
        while not stop_event.is_set():
            now = datetime.now(tz=UTC)
            for delay_s, tx in stream.next_events(now):
                heapq.heappush(pending, (now.timestamp() + delay_s, next(tiebreak), tx))
            while pending and pending[0][0] <= now.timestamp():
                send(heapq.heappop(pending)[2])

            stop_event.wait(timeout=interval)

    finally:
        producer.flush()
        producer.close()
        logger.info(
            f"Producer encerrado | enviadas={_metrics['sent']} | "
            f"erros={_metrics['errors']} | lat_media={_avg_latency():.1f}ms"
        )


if __name__ == "__main__":
    run()
