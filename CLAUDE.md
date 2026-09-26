# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Financial Fraud Detection Platform — a Data Master case study. End-to-end data platform using **Medallion Architecture** (Bronze → Silver → Gold) with simultaneous batch and streaming processing. Domain: Brazilian financial transactions (PIX/TED/DOC) with real-time fraud detection.

## Commands

### Development Setup
```bash
pip install -e ".[dev]"      # Install all dependencies including dev tools
cp .env.example .env          # Configure environment variables
```

### Testing
```bash
make test                     # All tests (requires 70% coverage minimum)
make test-unit                # Unit tests only: pytest tests/unit/
make test-integration         # Integration tests only: pytest tests/integration/
pytest tests/unit/test_schemas.py  # Single test file
pytest tests/unit/test_schemas.py::TestClass::test_method  # Single test
make test-cov                 # Generate HTML coverage report in htmlcov/
```

### Linting & Formatting
```bash
make lint      # ruff check + mypy
make format    # black + ruff --fix
```

### CI (GitHub Actions)
`.github/workflows/ci.yml` runs on every push/PR to `main` (plus manual `workflow_dispatch`), two parallel jobs mirroring the Makefile targets above so there's no drift between CI and local dev:
- `lint` — `make lint` (ruff + mypy)
- `test` — `make test-unit`, with Java 17 set up first (PySpark needs a JVM even in local mode); enforces the `--cov-fail-under=70` gate already defined in `pyproject.toml`; uploads `htmlcov/` as an artifact

Integration tests (`tests/integration/`) are intentionally **not** run in CI — they need the full Docker Compose stack (Kafka, Zookeeper, Airflow, Superset, Postgres, MinIO, Spark cluster), which is too slow/heavy for a per-PR gate. Run them locally via `make up && make setup && make test-integration`.

### Infrastructure (Docker)
```bash
make up        # Start all containers (Kafka, MinIO, Spark, Airflow, Postgres, etc.)
make down      # Stop all containers
make setup     # Initialize MinIO buckets and Kafka topics (run after make up)
make seed-data # Generate 500k synthetic transactions + market data
make logs      # Tail all container logs
make logs-kafka  # Tail specific service logs
```

### Pipeline Execution
```bash
make spark-submit-batch        # Run Bronze→Silver PySpark batch job (Gold is a separate step below)
make spark-submit-stream       # Start Spark Structured Streaming (Kafka consumer)
make spark-submit-silver-gold  # Run Silver→Gold job
make spark-submit-gold-postgres # Load Gold (MinIO) into the Postgres serving layer
make spark-submit-stream-postgres # Load streaming output (fraud_score + alerts) into Postgres (stop the stream first)
make producer-transactions     # Start Kafka transaction producer
make producer-market           # Start Kafka market data producer
make api                       # Start FastAPI dev server at :8000
make catalog                   # Generate docs/data_catalog.md + docs/images/data_lineage.svg, validate datasets against live infra
make fraud-eval                # Evaluate the fraud detectors (V1 Z-Score vs V2 multi-signal; Precision/Recall/FPR, local Spark, no Docker) and generate docs/fraud_evaluation.md
make fraud-calibrate           # Recalibrate the V2 weights/threshold on the validation seed and write src/transformation/fraud/weights.py
make dashboards                # Provision Superset dashboard (KPIs), smoke-test against live infra
make dashboards-export         # Snapshot the provisioned dashboard to dashboards/superset/dashboard_configs/
```

## Architecture

### Data Flow

**Batch:**
```
yfinance / CSV → Python Collector → MinIO bronze/ (JSON/CSV)
                                         ↓ PySpark (bronze_to_silver.py)
                                    MinIO silver/ (Parquet)
                                         ↓ PySpark (silver_to_gold.py)
                                    MinIO gold/ (Parquet) → PostgreSQL
```

**Streaming:**
```
Python Simulator → Kafka raw-transactions → Spark Structured Streaming
                                                    ↓ Z-Score anomaly detection
                          MinIO silver/transactions_stream/ (Parquet, distinct
                          from batch's silver/transactions/) + Kafka
                          enriched-transactions (all scored rows) + Kafka
                          fraud-alerts (rows flagged anomalous)
```

### Key Source Locations

| Component | Path |
|-----------|------|
| Centralized config (pydantic-settings) | `src/common/config.py` |
| All Pydantic + PySpark schemas | `src/common/schemas.py` |
| Batch PySpark jobs | `src/transformation/batch/` |
| Streaming processor + anomaly detector | `src/transformation/streaming/` |
| Kafka producers | `src/ingestion/streaming/` |
| Batch data collectors | `src/ingestion/batch/` |
| FastAPI app | `src/serving/api/main.py` |
| Gold→Postgres loader | `src/serving/loaders/` |
| Superset dashboard provisioning | `src/serving/dashboards/` |
| Great Expectations suites | `src/governance/great_expectations/` |
| Data catalog (registry, validation, render) | `src/governance/data_catalog/` |
| Airflow DAGs | `dags/` |
| Shared test fixtures | `tests/conftest.py` |

### Configuration Pattern

All settings are loaded via `src/common/config.py` using pydantic-settings. Access via:
```python
from src.common.config import settings

settings.kafka.bootstrap_servers
settings.minio.bucket_bronze
settings.postgres.url  # property that builds the connection string
```

Settings are grouped: `KafkaSettings`, `MinIOSettings`, `PostgresSettings`, `SparkSettings`, `APISettings`, `MarketDataSettings`. Each reads from environment variables or `.env`.

### Schema Pattern

`src/common/schemas.py` defines both Pydantic models (for Kafka/API) and PySpark DDL strings (for Spark jobs):
- `TransactionEvent` — financial transaction (with fraud fields)
- `MarketTradeEvent` — tick-by-tick market trade
- `FraudAlert` — output of streaming fraud detection
- `TRANSACTION_SPARK_SCHEMA`, `MARKET_TRADE_SPARK_SCHEMA` — DDL strings used in `spark.createDataFrame()` / `schema_of_json()`

### Kafka Topics

| Topic | Purpose |
|-------|---------|
| `raw-transactions` | Raw financial transactions (input) |
| `raw-market-data` | Market quotes (input) |
| `enriched-transactions` | Transactions with fraud score (output) |
| `fraud-alerts` | Confirmed fraud alerts (output) |

### Local Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow | http://localhost:8082 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| FastAPI docs | http://localhost:8000/docs | — |
| Spark UI | http://localhost:8081 | — |

> **Local dev only:** these credentials (and the fixed keys in `docker-compose.yml`, e.g. Airflow's Fernet key) must never be used in production/AWS — generate your own.

Data catalog is not a web service — it's a generated, versioned document (`docs/data_catalog.md`, via `make catalog`); see issue #14 / `docs/architecture.md`'s "Decisões Arquiteturais" table for why OpenMetadata was descoped from local V1.

## Implementation Phases

The project is being built in phases (see `CaseFinancialDataLakeHouse.md` — the original, frozen brief; it does not reflect current status). Status below reflects what's actually implemented and merged into `main`, not the original weekly schedule:

- **Phase 1 — Local infra with Docker Compose:** ✅ Done
- **Phase 2 — PySpark batch + streaming transformations:** ✅ Done (batch: `bronze_to_silver.py`/`silver_to_gold.py`; streaming: `stream_processor.py`, Z-Score anomaly detection, issue #11)
- **Phase 3 — Data governance:** ✅ Done, with two scope changes from the original plan: Great Expectations quality gates (issue #13); a lightweight, code-based data catalog (issue #14) instead of OpenMetadata — see `docs/architecture.md`'s "Decisões Arquiteturais" table; Delta Lake was evaluated and explicitly **not** adopted (issue #9) — the platform uses Parquet only, no time-travel/versioning layer exists
- **Phase 4 — Serving layer:** ✅ Done — PostgreSQL loader (issue #10), FastAPI (issue #15), Superset dashboards (issue #16, Grafana descoped — see `docs/architecture.md`)
- **Phase 5 — AWS migration via Terraform:** ⬜ Not started (issue #17, open)
- **Phase 6 — CI/CD, QuickSight, documentation:** 🚧 Partial — the lint + unit-test slice of CI/CD (`.github/workflows/ci.yml`) is done; deploy automation and QuickSight are not started; this documentation pass is issue #18

## Code Style

- Line length: 100 characters (black + ruff)
- Python 3.11+, ruff rules: E, W, F, I (isort), B (bugbear), C4, UP
- `known-first-party = ["src"]` for isort
- mypy with `strict = false`, `ignore_missing_imports = true`
