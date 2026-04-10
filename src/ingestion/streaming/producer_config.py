"""Configuração centralizada do Kafka producer."""

from dataclasses import dataclass, field

from src.common.config import settings


@dataclass
class ProducerConfig:
    """Parâmetros do Kafka producer — balanceia throughput vs latência."""

    bootstrap_servers: str = field(default_factory=lambda: settings.kafka.bootstrap_servers)

    # Confiabilidade
    acks: str = "1"            # líder confirma (throughput > durabilidade máxima)
    retries: int = 3
    retry_backoff_ms: int = 300

    # Throughput — mensagens são agrupadas antes de enviar
    batch_size: int = 16_384   # 16 KB
    linger_ms: int = 10        # aguarda até 10 ms para encher o batch
    buffer_memory: int = 33_554_432  # 32 MB de buffer total

    # Compressão (reduz banda, aumenta CPU)
    compression_type: str = "lz4"

    # Serialização
    key_serializer: str = "utf-8"
    value_serializer: str = "utf-8"

    # Timeouts
    request_timeout_ms: int = 30_000
    max_block_ms: int = 60_000

    def to_kafka_python_dict(self) -> dict:
        """Retorna dict compatível com kafka-python KafkaProducer."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "acks": self.acks,
            "retries": self.retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "batch_size": self.batch_size,
            "linger_ms": self.linger_ms,
            "buffer_memory": self.buffer_memory,
            "compression_type": self.compression_type,
            "request_timeout_ms": self.request_timeout_ms,
            "max_block_ms": self.max_block_ms,
            "value_serializer": lambda v: v.encode(self.value_serializer),
            "key_serializer": lambda k: k.encode(self.key_serializer) if k else None,
        }


# Perfil de baixa latência (streaming crítico)
LOW_LATENCY_CONFIG = ProducerConfig(linger_ms=0, batch_size=1, acks="1")

# Perfil de alto throughput (ingestão em lote)
HIGH_THROUGHPUT_CONFIG = ProducerConfig(linger_ms=50, batch_size=65_536, acks="1")
