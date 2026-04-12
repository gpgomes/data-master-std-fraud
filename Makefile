.PHONY: up down setup test lint format spark-submit-batch spark-submit-stream seed-data clean logs ps help

# ── Variáveis ──────────────────────────────────────────────────────────────────
COMPOSE          := docker compose
SPARK_MASTER     := spark://localhost:7077
BATCH_JOB        := src/transformation/batch/bronze_to_silver.py
STREAM_JOB       := src/transformation/streaming/stream_processor.py
PYTHON           := .venv/Scripts/python
PYTEST_ARGS      ?= -v

# ── Infra ──────────────────────────────────────────────────────────────────────
up: ## Subir toda a infraestrutura local
	$(COMPOSE) up -d
	@echo "Infraestrutura online. Execute 'make setup' para inicializar."

down: ## Derrubar todos os containers
	$(COMPOSE) down

setup: ## Inicializar buckets MinIO e tópicos Kafka
	@echo "=== Verificando MinIO ==="
	$(COMPOSE) exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin --quiet 2>nul || true
	$(COMPOSE) exec -T minio mc mb --ignore-existing local/bronze local/silver local/gold local/checkpoints
	$(COMPOSE) exec -T minio mc anonymous set download local/bronze >nul 2>&1 || true
	@echo "=== Verificando Kafka ==="
	$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic raw-transactions --partitions 3 --replication-factor 1
	$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic raw-market-data --partitions 3 --replication-factor 1
	$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic enriched-transactions --partitions 3 --replication-factor 1
	$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic fraud-alerts --partitions 1 --replication-factor 1
	$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list
	@echo "=== Setup concluido! ==="

ps: ## Listar containers em execução
	$(COMPOSE) ps

logs: ## Exibir logs de todos os serviços (últimas 100 linhas)
	$(COMPOSE) logs --tail=100 -f

logs-%: ## Exibir logs de um serviço específico: make logs-kafka
	$(COMPOSE) logs --tail=100 -f $*

# ── Desenvolvimento ────────────────────────────────────────────────────────────
install: ## Instalar dependências de desenvolvimento
	pip install -e ".[dev]"

lint: ## Verificar código com ruff e mypy
	$(PYTHON) -m ruff check src/ tests/ scripts/
	$(PYTHON) -m mypy src/ --ignore-missing-imports

format: ## Formatar código com black e ruff
	$(PYTHON) -m black src/ tests/ scripts/
	$(PYTHON) -m ruff check --fix src/ tests/ scripts/

# ── Testes ─────────────────────────────────────────────────────────────────────
test: ## Executar todos os testes
	$(PYTHON) -m pytest $(PYTEST_ARGS)

test-unit: ## Executar somente testes unitários
	$(PYTHON) -m pytest tests/unit/ $(PYTEST_ARGS)

test-integration: ## Executar somente testes de integração
	$(PYTHON) -m pytest tests/integration/ $(PYTEST_ARGS)

test-cov: ## Executar testes com relatório de cobertura HTML
	$(PYTHON) -m pytest --cov=src --cov-report=html:htmlcov --cov-report=term-missing
	@echo "Relatorio de cobertura gerado em htmlcov/index.html"

# ── Pipeline ───────────────────────────────────────────────────────────────────
seed-data: ## Gerar dados sintéticos de transações e mercado
	$(PYTHON) -m scripts.generate_sample_data --transactions 500000 --customers 10000 --months 6

spark-submit-batch: ## Submeter job PySpark batch (Bronze → Silver → Gold)
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		$(BATCH_JOB)

spark-submit-stream: ## Submeter job PySpark streaming (Kafka → Silver + detecção de fraude)
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		$(STREAM_JOB)

spark-submit-silver-gold: ## Submeter job Silver → Gold
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages io.delta:delta-core_2.12:2.4.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		src/transformation/batch/silver_to_gold.py

# ── Producers ──────────────────────────────────────────────────────────────────
producer-transactions: ## Iniciar producer de transações financeiras
	$(PYTHON) -m src.ingestion.streaming.kafka_producer_transactions

producer-market: ## Iniciar producer de dados de mercado
	$(PYTHON) -m src.ingestion.streaming.kafka_producer_market

# ── API ────────────────────────────────────────────────────────────────────────
api: ## Iniciar API FastAPI em modo desenvolvimento
	$(PYTHON) -m uvicorn src.serving.api.main:app --reload --host 0.0.0.0 --port 8000

# ── OpenMetadata ───────────────────────────────────────────────────────────────
seed-openmetadata: ## Popular catálogo OpenMetadata com metadados dos datasets
	$(PYTHON) -m scripts.seed_openmetadata

# ── Limpeza ────────────────────────────────────────────────────────────────────
clean: ## Limpar volumes Docker, dados temporários e artefatos de build
	$(COMPOSE) down -v --remove-orphans
	$(PYTHON) -c "import shutil, pathlib; [shutil.rmtree(p, ignore_errors=True) for p in ['data/sample','htmlcov','.pytest_cache']]; [p.unlink() for p in pathlib.Path('.').rglob('*.pyc')]"
	@echo "Ambiente limpo."

clean-data: ## Limpar apenas dados gerados (mantém containers)
	$(PYTHON) -c "import shutil; shutil.rmtree('data/sample', ignore_errors=True)"
	@echo "Dados de exemplo removidos."

# ── Help ───────────────────────────────────────────────────────────────────────
help: ## Exibir esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-28s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
