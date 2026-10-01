.PHONY: env up down setup test test-unit test-integration test-cov lint format install \
        spark-submit-batch spark-submit-stream spark-submit-silver-gold spark-submit-gold-postgres \
        spark-submit-stream-postgres \
        seed-data producer-transactions producer-market api catalog dashboards dashboards-export \
        fraud-eval fraud-calibrate fraud-online-eval slo-report load-test e2e \
        clean clean-data logs ps help

# ── Variáveis ──────────────────────────────────────────────────────────────────
COMPOSE          := docker compose
SPARK_MASTER     := spark://localhost:7077
BATCH_JOB        := src/transformation/batch/bronze_to_silver.py
STREAM_JOB       := src/transformation/streaming/stream_processor.py
# Usa o interpretador do .venv se existir (criado via `python3.11 -m venv .venv`),
# senão cai para python3 do PATH. Sobrescrevível: `make test PYTHON=python3.11`.
PYTHON           ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
PYTEST_ARGS      ?= -v
# Padrões de .env.example; sobrescrevíveis: `make fraud-online-eval POSTGRES_USER=... POSTGRES_DB=...`.
POSTGRES_USER    ?= datamaster
POSTGRES_DB      ?= fraud_analytics

# ── Infra ──────────────────────────────────────────────────────────────────────
env: ## Cria/completa o .env e gera os segredos locais (Airflow, Superset, chave da API); idempotente
	$(PYTHON) -m scripts.ensure_env

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

# Namespace isolado do E2E (issue #57): buckets e tópicos e2e-*, banco fraud_e2e. Fonte única em
# tests/e2e/harness.py (E2E_ENV); os valores aqui têm de bater com ela (um teste confere).
E2E_EXPORTS = E2E=1 MINIO_BUCKET_BRONZE=e2e-bronze MINIO_BUCKET_SILVER=e2e-silver MINIO_BUCKET_GOLD=e2e-gold \
	MINIO_BUCKET_CHECKPOINTS=e2e-checkpoints KAFKA_TOPIC_TRANSACTIONS=e2e-raw-transactions \
	KAFKA_TOPIC_MARKET_DATA=e2e-raw-market-data KAFKA_TOPIC_ENRICHED=e2e-enriched-transactions \
	KAFKA_TOPIC_FRAUD_ALERTS=e2e-fraud-alerts POSTGRES_DB=fraud_e2e

e2e: ## Teste ponta a ponta com invariantes e cenários de falha (issue #57): requer kafka, minio, postgres e spark-master de pé; ~20 min
	$(E2E_EXPORTS) $(PYTHON) -m pytest tests/e2e/ -v --no-cov -p no:cacheprovider --junitxml=data/e2e/junit.xml

test-cov: ## Executar testes com relatório de cobertura HTML
	$(PYTHON) -m pytest --cov=src --cov-report=html:htmlcov --cov-report=term-missing
	@echo "Relatorio de cobertura gerado em htmlcov/index.html"

# ── Pipeline ───────────────────────────────────────────────────────────────────
seed-data: ## Gerar dados sintéticos de transações e mercado
	$(PYTHON) -m scripts.generate_sample_data --transactions 500000 --customers 10000 --months 6

spark-submit-batch: ## Submeter job PySpark batch (Bronze → Silver) — Gold é um job separado, ver spark-submit-silver-gold
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
		$(BATCH_JOB)

spark-submit-stream: ## Submeter job PySpark streaming (Kafka → Silver + detecção de fraude)
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
		$(STREAM_JOB)

spark-submit-silver-gold: ## Submeter job Silver → Gold
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		src/transformation/batch/silver_to_gold.py

spark-submit-gold-postgres: ## Submeter job Gold → PostgreSQL (serving layer)
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		src/serving/loaders/gold_to_postgres.py

spark-submit-stream-postgres: ## Submeter job saída do streaming → PostgreSQL (fraud_score e alertas do detector)
	$(COMPOSE) exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		src/serving/loaders/stream_to_postgres.py

# ── Producers ──────────────────────────────────────────────────────────────────
producer-transactions: ## Iniciar producer de transações financeiras
	$(PYTHON) -m src.ingestion.streaming.kafka_producer_transactions

producer-market: ## Iniciar producer de dados de mercado
	$(PYTHON) -m src.ingestion.streaming.kafka_producer_market

# ── API ────────────────────────────────────────────────────────────────────────
api: ## Iniciar API FastAPI em modo desenvolvimento
	$(PYTHON) -m uvicorn src.serving.api.main:app --reload --host 0.0.0.0 --port 8000

# ── Catálogo de Dados ────────────────────────────────────────────────────────────
catalog: ## Gera docs/data_catalog.md e a imagem da linhagem, e valida os datasets contra a infra real (requer make up)
	$(PYTHON) -m scripts.build_data_catalog --strict

# ── Dashboards ─────────────────────────────────────────────────────────────────
fraud-eval: ## Avalia o detector de fraude (Precision/Recall/FPR) e gera docs/fraud_evaluation.md (Spark local, sem Docker)
	$(PYTHON) -m src.transformation.fraud.evaluate

fraud-calibrate: ## Calibra os pesos e o limiar do detector multi-signal na seed de validação e grava src/transformation/fraud/weights.py
	$(PYTHON) -m src.transformation.fraud.calibrate

fraud-online-eval: ## Benchmark online V1 x V2 (SQL sobre stream_scored_transactions; requer o streaming rodado e make spark-submit-stream-postgres)
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 < src/serving/queries/fraud_online_benchmark.sql

slo-report: ## Relatório de SLOs da plataforma nas últimas 24 h (stream, pipeline, gates e API; issue #55)
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 < src/serving/queries/slo_report.sql

LOAD_LEVELS ?= 25,50,100,200
load-test: ## Teste de carga do stream (issue #56): sobe producers no host por nível de TPS e mede via stream_batch_metrics; requer o stream rodando sozinho
	$(PYTHON) -u -m scripts.stream_load_test --levels $(LOAD_LEVELS) --per-instance-tps 25 --duration-s 240 --warmup-s 60 --output data/load_test/results.json

dashboards: ## Provisiona o dashboard Superset de KPIs de fraude/transações (requer make up + dados no Postgres)
	$(PYTHON) -m scripts.provision_superset_dashboards --verify --strict

dashboards-export: ## Exporta o dashboard Superset provisionado para dashboards/superset/dashboard_configs/
	rm -rf .tmp_dashboard_export
	mkdir -p .tmp_dashboard_export
	MSYS_NO_PATHCONV=1 $(COMPOSE) exec -T superset superset export-dashboards -f /tmp/dashboard_export.zip
	$(COMPOSE) cp superset:/tmp/dashboard_export.zip .tmp_dashboard_export/export.zip
	cd .tmp_dashboard_export && unzip -o -q export.zip
	rm -rf dashboards/superset/dashboard_configs/charts dashboards/superset/dashboard_configs/dashboards dashboards/superset/dashboard_configs/databases dashboards/superset/dashboard_configs/datasets dashboards/superset/dashboard_configs/metadata.yaml
	cp -r .tmp_dashboard_export/dashboard_export_*/* dashboards/superset/dashboard_configs/
	rm -rf .tmp_dashboard_export
	@echo "Export atualizado em dashboards/superset/dashboard_configs/"

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
