"""Kafka producer de dados de mercado — simula tick-by-tick durante pregão."""

import json
import os
import random
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from threading import Event
from typing import Any

import numpy as np
from loguru import logger

from src.common.config import settings
from src.common.data_generator import MARKET_SYMBOLS
from src.ingestion.streaming.producer_config import ProducerConfig

# ── Configuração ───────────────────────────────────────────────────────────────

TOPIC = settings.kafka.topic_market_data
RATE = float(os.getenv("MARKET_PRODUCER_RATE_TPS", "50"))   # eventos por segundo
SOURCE_SYSTEM = os.getenv("SOURCE_SYSTEM", "market-simulator")

# Horário de pregão B3 (BRT = UTC-3)
PREGAO_START_HOUR_UTC = 13   # 10h BRT
PREGAO_END_HOUR_UTC = 20     # 17h BRT

# Preços base realistas (BRL)
_BASE_PRICES: dict[str, float] = {
    "PETR4.SA": 38.50,
    "VALE3.SA": 68.00,
    "ITUB4.SA": 32.00,
    "BBDC4.SA": 14.50,
    "ABEV3.SA": 12.80,
    "WEGE3.SA": 42.00,
    "RENT3.SA": 58.00,
    "BBAS3.SA": 27.00,
    "MGLU3.SA": 5.50,
    "LREN3.SA": 18.00,
}

# Métricas em memória
_metrics: dict[str, Any] = {
    "sent": 0,
    "errors": 0,
    "latency_sum_ms": 0.0,
}


def _avg_latency() -> float:
    if _metrics["sent"] == 0:
        return 0.0
    return _metrics["latency_sum_ms"] / _metrics["sent"]


# ── Simulação de preços ────────────────────────────────────────────────────────

class TickSimulator:
    """Mantém estado de preço por símbolo e gera ticks tick-by-tick."""

    def __init__(self) -> None:
        self.prices = {sym: price for sym, price in _BASE_PRICES.items()}
        self._rng = random.Random()
        self._np_rng = np.random.RandomState()

    def _volume_multiplier(self, hour_utc: int) -> float:
        """Volume maior na abertura (13-14 UTC) e fechamento (19-20 UTC)."""
        if hour_utc in (PREGAO_START_HOUR_UTC, PREGAO_START_HOUR_UTC + 1):
            return 3.0
        if hour_utc in (PREGAO_END_HOUR_UTC - 1, PREGAO_END_HOUR_UTC):
            return 2.5
        return 1.0

    def next_tick(self, symbol: str) -> dict[str, Any]:
        """Gera o próximo tick para um símbolo."""
        price = self.prices[symbol]

        # Random walk micro
        tick_return = self._np_rng.normal(0, 0.0003)
        price = max(0.01, price * (1 + tick_return))
        self.prices[symbol] = price

        spread_pct = self._rng.uniform(0.0005, 0.003)
        half_spread = price * spread_pct / 2
        bid = round(price - half_spread, 2)
        ask = round(price + half_spread, 2)
        spread = round(ask - bid, 4)

        now_utc = datetime.now(tz=timezone.utc)
        vol_mult = self._volume_multiplier(now_utc.hour)
        volume = max(1, int(self._np_rng.lognormal(7, 1) * vol_mult))

        return {
            "event_id": str(uuid.uuid4()),
            "symbol": symbol,
            "timestamp": now_utc.isoformat(),
            "price": round(price, 2),
            "volume": volume,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "produced_at": now_utc.isoformat(),
            "source_system": SOURCE_SYSTEM,
        }


def _is_pregao() -> bool:
    """Retorna True se estiver dentro do horário de pregão B3."""
    hour = datetime.now(tz=timezone.utc).hour
    return PREGAO_START_HOUR_UTC <= hour < PREGAO_END_HOUR_UTC


def _serialize(msg: dict[str, Any]) -> str:
    return json.dumps(msg, default=str)


# ── Producer loop ──────────────────────────────────────────────────────────────

def run(stop_event: Event | None = None) -> None:
    """Inicia o producer de mercado. Bloqueia até stop_event ou SIGINT."""
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    if stop_event is None:
        stop_event = Event()

    # Graceful shutdown via SIGINT / SIGTERM (apenas na thread principal)
    if threading.current_thread() is threading.main_thread():
        def _handle_signal(signum, frame):  # noqa: ANN001
            logger.info("Sinal recebido, encerrando market producer...")
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    config = ProducerConfig()
    producer = KafkaProducer(**config.to_kafka_python_dict())
    simulator = TickSimulator()
    symbols = list(_BASE_PRICES.keys())
    interval = 1.0 / RATE

    logger.info(f"Market producer iniciado | tópico={TOPIC} | rate={RATE} tps")

    try:
        while not stop_event.is_set():
            if not _is_pregao():
                if _metrics["sent"] % 100 == 0:
                    logger.debug("Fora do horário de pregão, aguardando...")
                stop_event.wait(timeout=60.0)
                continue

            symbol = random.choice(symbols)
            tick = simulator.next_tick(symbol)
            payload = _serialize(tick)

            t0 = time.monotonic()
            try:
                future = producer.send(TOPIC, value=payload, key=symbol, headers=[
                    ("produced_at", tick["produced_at"].encode()),
                    ("source_system", SOURCE_SYSTEM.encode()),
                ])
                future.get(timeout=10)
                latency_ms = (time.monotonic() - t0) * 1000
                _metrics["sent"] += 1
                _metrics["latency_sum_ms"] += latency_ms

                if _metrics["sent"] % 500 == 0:
                    logger.info(
                        f"Enviados {_metrics['sent']} ticks | "
                        f"erros={_metrics['errors']} | "
                        f"lat_media={_avg_latency():.1f}ms"
                    )

            except KafkaError as exc:
                _metrics["errors"] += 1
                logger.error(f"Erro ao enviar tick: {exc}")

            stop_event.wait(timeout=interval)

    finally:
        producer.flush()
        producer.close()
        logger.info(
            f"Market producer encerrado | enviados={_metrics['sent']} | "
            f"erros={_metrics['errors']} | lat_media={_avg_latency():.1f}ms"
        )


if __name__ == "__main__":
    run()
