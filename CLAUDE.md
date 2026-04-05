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
make spark-submit-batch        # Run Bronze→Silver→Gold PySpark batch job
make spark-submit-stream       # Start Spark Structured Streaming (Kafka consumer)
make spark-submit-silver-gold  # Run only Silver→Gold job
make producer-transactions     # Start Kafka transaction producer
make producer-market           # Start Kafka market data producer
make api                       # Start FastAPI dev server at :8000
make seed-openmetadata         # Populate OpenMetadata catalog
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
                                           MinIO silver/ + Kafka fraud-alerts
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
| Great Expectations suites | `src/governance/great_expectations/` |
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
| OpenMetadata | http://localhost:8585 | admin / admin |

## Implementation Phases

The project is being built in phases (see `CaseFinancialDataLakeHouse.md`):
- **Phase 1 (Weeks 1–2):** Local infra with Docker Compose (current: `feature/step_1.2`)
- **Phase 2 (Weeks 3–4):** PySpark batch + streaming transformations
- **Phase 3 (Weeks 5–6):** Data governance (Great Expectations, OpenMetadata, Delta Lake)
- **Phase 4 (Weeks 7–8):** Serving layer (Postgres, FastAPI, dashboards)
- **Phase 5 (Weeks 9–10):** AWS migration via Terraform
- **Phase 6 (Weeks 11–12):** CI/CD, QuickSight, documentation

## Code Style

- Line length: 100 characters (black + ruff)
- Python 3.11+, ruff rules: E, W, F, I (isort), B (bugbear), C4, UP
- `known-first-party = ["src"]` for isort
- mypy with `strict = false`, `ignore_missing_imports = true`
