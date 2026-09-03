"""Testes do contrato de dados entre producers, Pydantic, Avro e Spark (issue #8).

Cobre os 4 critérios de aceite da issue:
  1. Payloads de transação e mercado são validados pelos modelos correspondentes.
  2. Schemas Pydantic, Avro e Spark têm campos e tipos compatíveis.
  3. Variáveis de ambiente possuem nomes canônicos.
  4. Testes cobrem serialização, validação e compatibilidade dos contratos.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from src.common.schemas import (
    MARKET_TRADE_SPARK_SCHEMA,
    TRANSACTION_SPARK_SCHEMA,
    MarketTradeEvent,
    TransactionEvent,
)
from src.ingestion.streaming.kafka_producer_market import (
    SOURCE_SYSTEM as MARKET_SOURCE_SYSTEM,
)
from src.ingestion.streaming.kafka_producer_market import (
    TickSimulator,
)
from src.ingestion.streaming.kafka_producer_market import (
    _build_message as market_build_message,
)
from src.ingestion.streaming.kafka_producer_transactions import (
    SOURCE_SYSTEM as TRANSACTION_SOURCE_SYSTEM,
)
from src.ingestion.streaming.kafka_producer_transactions import (
    _build_message as transaction_build_message,
)

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = ROOT / "src" / "ingestion" / "streaming" / "schemas"


def _avro_field_names(avsc_path: Path) -> set[str]:
    schema = json.loads(avsc_path.read_text(encoding="utf-8"))
    return {f["name"] for f in schema["fields"]}


def _spark_ddl_field_names(ddl: str) -> set[str]:
    """Extrai os nomes de coluna de uma string DDL "col TYPE, col TYPE, ..."."""
    return {line.strip().split()[0] for line in ddl.strip().rstrip(",").split(",") if line.strip()}


def _sample_tx_dict(**overrides) -> dict:
    base = {
        "transaction_id": "tx-001",
        "customer_id": "cust-001",
        "timestamp": datetime(2024, 6, 15, 14, 30, tzinfo=UTC),
        "amount": 150.0,
        "currency": "BRL",
        "transaction_type": "PIX",
        "merchant_category": "ALIMENTACAO",
        "origin_account": "0001-1",
        "destination_account": "0002-2",
        "origin_bank": "BancoA",
        "destination_bank": "BancoB",
        "channel": "APP_MOBILE",
        "is_fraud": False,
        "fraud_type": None,
    }
    base.update(overrides)
    return base


# ── 1. Validação pelos modelos correspondentes ─────────────────────────────────


class TestProducerValidatesPayload:
    def test_transaction_message_validated_and_carries_metadata(self):
        tx = _sample_tx_dict()
        msg = transaction_build_message(tx)

        # A validação passou (não levantou ValidationError) e os metadados de
        # proveniência não foram perdidos/silenciosamente sobrescritos.
        assert msg["transaction_id"] == "tx-001"
        assert msg["source_system"] == TRANSACTION_SOURCE_SYSTEM
        assert msg["produced_at"] is not None

    def test_market_message_validated_and_carries_metadata(self):
        sim = TickSimulator()
        tick = sim.next_tick("PETR4.SA")
        msg = market_build_message(tick)

        assert msg["symbol"] == "PETR4.SA"
        assert msg["source_system"] == MARKET_SOURCE_SYSTEM
        assert msg["produced_at"] is not None

    def test_transaction_message_is_valid_json_after_validation(self):
        from src.ingestion.streaming.kafka_producer_transactions import _serialize

        msg = transaction_build_message(_sample_tx_dict())
        parsed = json.loads(_serialize(msg))
        assert parsed["transaction_id"] == "tx-001"

    def test_market_message_is_valid_json_after_validation(self):
        from src.ingestion.streaming.kafka_producer_market import _serialize

        sim = TickSimulator()
        msg = market_build_message(sim.next_tick("VALE3.SA"))
        parsed = json.loads(_serialize(msg))
        assert parsed["symbol"] == "VALE3.SA"


# ── 2. Compatibilidade Avro <-> Pydantic <-> Spark ─────────────────────────────


class TestTransactionEventContract:
    def test_avro_fields_covered_by_pydantic_model(self):
        """Todo campo publicado no Kafka (Avro) deve existir no TransactionEvent —
        senão o dado é descartado silenciosamente na validação (era o bug original:
        produced_at/source_system existiam no payload real mas não no modelo)."""
        avro_fields = _avro_field_names(SCHEMAS_DIR / "transaction_event.avsc")
        pydantic_fields = set(TransactionEvent.model_fields.keys())
        missing = avro_fields - pydantic_fields
        assert not missing, f"Campos do Avro ausentes em TransactionEvent: {missing}"

    def test_spark_ddl_covers_produced_at_and_source_system(self):
        """O DDL Spark usado pra ler raw-transactions precisa ter as mesmas
        colunas de proveniência que o producer/Avro realmente enviam."""
        ddl_fields = _spark_ddl_field_names(TRANSACTION_SPARK_SCHEMA)
        assert "produced_at" in ddl_fields
        assert "source_system" in ddl_fields

    def test_producer_payload_validates_against_pydantic_model(self):
        """Round-trip real: o dict que o producer manda pro Kafka (já validado
        por _build_message) precisa validar de novo sem perdas quando lido de
        volta — simula o que um consumer/streaming job faria."""
        msg = transaction_build_message(_sample_tx_dict())
        revalidated = TransactionEvent(**msg)
        assert revalidated.source_system == TRANSACTION_SOURCE_SYSTEM
        assert revalidated.produced_at is not None

    def test_real_data_generator_output_passes_producer_validation(self):
        """DataGenerator não é mockado aqui — garante que o gerador batch e o
        contrato do producer streaming continuam compatíveis de verdade,
        não só com o dict de exemplo feito à mão."""
        from src.common.data_generator import DataGenerator

        gen = DataGenerator(seed=7)
        customers = gen.generate_customers(n=20)
        txs = gen.generate_transactions(customers, n=200)

        for tx in txs:
            msg = transaction_build_message(tx)
            assert msg["source_system"] == TRANSACTION_SOURCE_SYSTEM


class TestMarketTradeEventContract:
    def test_avro_fields_covered_by_pydantic_model(self):
        avro_fields = _avro_field_names(SCHEMAS_DIR / "market_event.avsc")
        pydantic_fields = set(MarketTradeEvent.model_fields.keys())
        missing = avro_fields - pydantic_fields
        assert not missing, f"Campos do Avro ausentes em MarketTradeEvent: {missing}"

    def test_pydantic_model_has_no_stale_source_field(self):
        """Bug original da issue #8: o modelo tinha `source`, mas o producer e o
        Avro sempre mandaram `source_system` — o valor real era descartado."""
        fields = set(MarketTradeEvent.model_fields.keys())
        assert "source" not in fields
        assert "source_system" in fields

    def test_spark_ddl_uses_source_system_not_source(self):
        ddl_fields = _spark_ddl_field_names(MARKET_TRADE_SPARK_SCHEMA)
        assert "source" not in ddl_fields
        assert "source_system" in ddl_fields
        assert "produced_at" in ddl_fields

    def test_producer_payload_validates_against_pydantic_model(self):
        sim = TickSimulator()
        msg = market_build_message(sim.next_tick("ITUB4.SA"))
        revalidated = MarketTradeEvent(**msg)
        assert revalidated.source_system == MARKET_SOURCE_SYSTEM
        assert revalidated.produced_at is not None


# ── 3. Nomes canônicos de variáveis de ambiente ────────────────────────────────


class TestCanonicalEnvVarNames:
    """Verificação estática (mesmo padrão de test_environment_consistency.py):
    os nomes lidos pelos producers precisam bater com o que .env.example
    documenta — hoje o .env.example documentava nomes que ninguém lia."""

    def _source(self, path: str) -> str:
        return (ROOT / path).read_text(encoding="utf-8")

    def test_transaction_producer_reads_canonical_rate_var(self):
        source = self._source("src/ingestion/streaming/kafka_producer_transactions.py")
        assert 'os.getenv("PRODUCER_RATE_TPS"' in source

    def test_market_producer_reads_canonical_rate_var(self):
        source = self._source("src/ingestion/streaming/kafka_producer_market.py")
        assert 'os.getenv("MARKET_PRODUCER_RATE_TPS"' in source

    def test_env_example_documents_names_producers_actually_read(self):
        content = self._source(".env.example")
        assert "PRODUCER_RATE_TPS=" in content
        assert "MARKET_PRODUCER_RATE_TPS=" in content
        assert "GENERATOR_SEED=" in content
        # nomes antigos, que nada lia, não devem voltar
        assert "TRANSACTION_PRODUCER_RATE=" not in content
        assert "MARKET_PRODUCER_RATE=" not in content

    def test_docker_compose_producer_services_match_env_example_names(self):
        """docker-compose.yml (serviços producer-transactions/producer-market)
        já usava os nomes certos — .env.example que estava desalinhado."""
        compose = self._source("docker-compose.yml")
        env_example = self._source(".env.example")
        for name in ("PRODUCER_RATE_TPS", "MARKET_PRODUCER_RATE_TPS", "GENERATOR_SEED"):
            assert f"{name}=" in compose or f"{name}:" in compose
            assert f"{name}=" in env_example
