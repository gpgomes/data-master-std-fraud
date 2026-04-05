.PHONY: up down setup test lint format spark-submit-batch spark-submit-stream seed-data clean logs ps help

# ── Variáveis ──────────────────────────────────────────────────────────────────
COMPOSE          := docker compose
SPARK_MASTER     := spark://localhost:7077
BATCH_JOB        := src/transformation/batch/bronze_to_silver.py
STREAM_JOB       := src/transformation/streaming/stream_processor.py
PYTHON           := python
PYTEST_ARGS      ?= -v

# ── Infra ──────────────────────────────────────────────────────────────────────
up: ## Subir toda a infraestrutura local
	$(COMPOSE) up -d
	@echo "Aguardando serviços ficarem prontos..."
	@sleep 10
	@echo "Infraestrutura online. Execute 'make setup' para inicializar."

down: ## Derrubar todos os containers
	$(COMPOSE) down

setup: ## Inicializar buckets MinIO e tópicos Kafka
	@echo "Inicializando infraestrutura..."
	bash scripts/setup_local.sh

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
	ruff check src/ tests/ scripts/
	mypy src/ --ignore-missing-imports

format: ## Formatar código com black e ruff
	black src/ tests/ scripts/
	ruff check --fix src/ tests/ scripts/

# ── Testes ─────────────────────────────────────────────────────────────────────
test: ## Executar todos os testes
	pytest $(PYTEST_ARGS)

test-unit: ## Executar somente testes unitários
	pytest tests/unit/ $(PYTEST_ARGS)

test-integration: ## Executar somente testes de integração
	pytest tests/integration/ $(PYTEST_ARGS)

test-cov: ## Executar testes com relatório de cobertura HTML
	pytest --cov=src --cov-report=html:htmlcov --cov-report=term-missing
	@echo "Relatório de cobertura gerado em htmlcov/index.html"

# ── Pipeline ───────────────────────────────────────────────────────────────────
seed-data: ## Gerar dados sintéticos de transações e mercado
	$(PYTHON) scripts/generate_sample_data.py --transactions 500000 --customers 10000 --days 180

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
	$(PYTHON) src/ingestion/streaming/kafka_producer_transactions.py

producer-market: ## Iniciar producer de dados de mercado
	$(PYTHON) src/ingestion/streaming/kafka_producer_market.py

# ── API ────────────────────────────────────────────────────────────────────────
api: ## Iniciar API FastAPI em modo desenvolvimento
	uvicorn src.serving.api.main:app --reload --host 0.0.0.0 --port 8000

# ── OpenMetadata ───────────────────────────────────────────────────────────────
seed-openmetadata: ## Popular catálogo OpenMetadata com metadados dos datasets
	$(PYTHON) scripts/seed_openmetadata.py

# ── Limpeza ────────────────────────────────────────────────────────────────────
clean: ## Limpar volumes Docker, dados temporários e artefatos de build
	$(COMPOSE) down -v --remove-orphans
	rm -rf data/sample/ htmlcov/ .coverage .pytest_cache __pycache__
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Ambiente limpo."

clean-data: ## Limpar apenas dados gerados (mantém containers)
	rm -rf data/sample/
	@echo "Dados de exemplo removidos."

# ── Help ───────────────────────────────────────────────────────────────────────
help: ## Exibir esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-28s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
