"""Configuração centralizada do Kafka producer."""

from dataclasses import dataclass, field

from src.common.config import settings


@dataclass
class ProducerConfig:
    """Parâmetros do Kafka producer — balanceia throughput vs latência."""

    bootstrap_servers: str = field(default_factory=lambda: settings.kafka.bootstrap_servers)

    # Confiabilidade
    # Todas as réplicas em sincronia confirmam antes do ack (#59). Com `acks=1` o líder confirmava
    # sozinho, e uma queda dele antes de replicar perdia a mensagem na ENTRADA do pipeline. Com o
    # broker único do ambiente local o custo é nulo; num cluster, é alguns ms de latência.
    acks: int | str = "all"
    retries: int = 3
    retry_backoff_ms: int = 300

    # Producer idempotente (#68): o broker dá um id ao producer e numera cada lote por partição;
    # um retry de um lote já gravado (o ack se perdeu, não a mensagem) é descartado em vez de
    # duplicar no tópico. Vale só dentro da sessão do producer: reiniciar o processo gera outro
    # id, e um reenvio depois disso ainda duplica (exigiria transações). Por isso o stream continua
    # deduplicando por `transaction_id` e o loader do Postgres fica com uma linha.
    # O kafka-python exige `acks="all"`, `retries > 0` e UM request em voo por conexão (o cliente
    # Java aceita até 5). Os producers já enviam de forma síncrona (`future.get()` a cada
    # mensagem), então o limite de 1 em voo não muda o throughput deles (medido na #68).
    enable_idempotence: bool = True
    max_in_flight_requests_per_connection: int = 1

    # Throughput — mensagens são agrupadas antes de enviar
    batch_size: int = 16_384   # 16 KB
    # Sem espera para encher o lote (#72): os producers enviam de forma síncrona (`future.get()` a
    # cada mensagem), então nunca há um segundo envio esperando e os 10 ms de antes eram só
    # latência. Medido: envio de ~13 ms para ~3 ms. Envio assíncrono em lote usa o
    # HIGH_THROUGHPUT_CONFIG (linger_ms=50).
    linger_ms: int = 0
    buffer_memory: int = 33_554_432  # 32 MB de buffer total

    # Compressão (reduz banda, aumenta CPU)
    compression_type: str = "lz4"

    # Serialização
    key_serializer: str = "utf-8"
    value_serializer: str = "utf-8"

    # Timeouts
    request_timeout_ms: int = 30_000
    max_block_ms: int = 60_000

    def __post_init__(self) -> None:
        """Recusa na criação a combinação que o KafkaProducer só recusaria ao conectar."""
        if not self.enable_idempotence:
            return
        problems = []
        if self.acks not in ("all", -1):
            problems.append(f'acks={self.acks!r} (precisa ser "all")')
        if self.retries <= 0:
            problems.append(f"retries={self.retries} (precisa ser > 0)")
        if self.max_in_flight_requests_per_connection != 1:
            problems.append(
                f"max_in_flight_requests_per_connection={self.max_in_flight_requests_per_connection}"
                " (o kafka-python exige 1)"
            )
        if problems:
            raise ValueError("producer idempotente com " + ", ".join(problems))

    def to_kafka_python_dict(self) -> dict:
        """Retorna dict compatível com kafka-python KafkaProducer."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "acks": self.acks,
            "retries": self.retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "enable_idempotence": self.enable_idempotence,
            "max_in_flight_requests_per_connection": self.max_in_flight_requests_per_connection,
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
LOW_LATENCY_CONFIG = ProducerConfig(linger_ms=0, batch_size=1)

# Perfil de alto throughput (ingestão em lote)
HIGH_THROUGHPUT_CONFIG = ProducerConfig(linger_ms=50, batch_size=65_536)
