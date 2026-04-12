"""Kafka producer de transações financeiras — simula stream em tempo real."""

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
from src.common.data_generator import DataGenerator
from src.ingestion.streaming.producer_config import ProducerConfig

# ── Configuração ───────────────────────────────────────────────────────────────

TOPIC = settings.kafka.topic_transactions
RATE = float(os.getenv("PRODUCER_RATE_TPS", "10"))          # transações por segundo
SOURCE_SYSTEM = os.getenv("SOURCE_SYSTEM", "transaction-simulator")
SEED = int(os.getenv("GENERATOR_SEED", "0")) or None        # 0 = não-determinístico

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
    """Adiciona campos de metadados ao evento de transação."""
    return {
        **tx,
        "produced_at": datetime.now(tz=UTC).isoformat(),
        "source_system": SOURCE_SYSTEM,
    }


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

    gen = DataGenerator(seed=SEED or int(time.time()))
    # Pré-gera clientes para reutilizar nos generates
    customers = gen.generate_customers(n=1_000)

    interval = 1.0 / RATE
    logger.info(f"Producer iniciado | tópico={TOPIC} | rate={RATE} tps")

    try:
        while not stop_event.is_set():
            # Gera uma transação por vez reaproveitando o gerador
            tx_list = gen.generate_transactions(customers, n=1)
            tx = tx_list[0]
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
