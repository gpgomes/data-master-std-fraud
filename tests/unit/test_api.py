"""Testes para a API FastAPI da serving layer (issue #15).

`repository.py` é testado contra um Postgres real via SQLite in-memory (SQL
real, não mocks — mesmo espírito dos testes com PySpark local do resto do
projeto). As rotas são testadas via `TestClient` + `dependency_overrides`,
trocando `get_db` pela sessão SQLite; a falha de banco é simulada com uma
dependency que levanta `SQLAlchemyError` diretamente.
"""

from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.serving.api import repository
from src.serving.api.db import get_db
from src.serving.api.main import app

# ── Fixtures: banco SQLite in-memory com o schema mínimo usado pela API ────────

_SCHEMA = """
CREATE TABLE fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_key TEXT,
    date_key DATE,
    amount_brl REAL,
    currency TEXT,
    transaction_type TEXT,
    channel TEXT,
    merchant_category TEXT,
    is_fraud BOOLEAN,
    fraud_type TEXT,
    fraud_score REAL,
    processing_timestamp TIMESTAMP
);

CREATE TABLE agg_daily_fraud_metrics (
    date_key DATE,
    transaction_type TEXT,
    total_transactions INTEGER,
    total_amount_brl REAL,
    avg_amount_brl REAL,
    fraud_count INTEGER,
    fraud_rate REAL,
    processing_timestamp TIMESTAMP,
    PRIMARY KEY (date_key, transaction_type)
);
"""


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with eng.begin() as conn:
        for stmt in _SCHEMA.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    return eng


@pytest.fixture
def db(engine) -> Session:
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = session_local()
    yield session
    session.close()


def _insert_transaction(db: Session, **overrides) -> None:
    base = {
        "transaction_id": "tx-001",
        "customer_key": "cust-001",
        "date_key": "2024-06-15",
        "amount_brl": 150.0,
        "currency": "BRL",
        "transaction_type": "PIX",
        "channel": "APP_MOBILE",
        "merchant_category": "ALIMENTACAO",
        "is_fraud": False,
        "fraud_type": None,
        "fraud_score": None,
        "processing_timestamp": "2024-06-15T10:00:00",
    }
    base.update(overrides)
    db.execute(
        text(
            """
            INSERT INTO fact_transactions
            (transaction_id, customer_key, date_key, amount_brl, currency,
             transaction_type, channel, merchant_category, is_fraud,
             fraud_type, fraud_score, processing_timestamp)
            VALUES
            (:transaction_id, :customer_key, :date_key, :amount_brl, :currency,
             :transaction_type, :channel, :merchant_category, :is_fraud,
             :fraud_type, :fraud_score, :processing_timestamp)
            """
        ),
        base,
    )
    db.commit()


def _insert_daily_metric(db: Session, **overrides) -> None:
    base = {
        "date_key": "2024-06-15",
        "transaction_type": "PIX",
        "total_transactions": 100,
        "total_amount_brl": 15000.0,
        "avg_amount_brl": 150.0,
        "fraud_count": 3,
        "fraud_rate": 0.03,
        "processing_timestamp": "2024-06-15T10:00:00",
    }
    base.update(overrides)
    db.execute(
        text(
            """
            INSERT INTO agg_daily_fraud_metrics
            (date_key, transaction_type, total_transactions, total_amount_brl,
             avg_amount_brl, fraud_count, fraud_rate, processing_timestamp)
            VALUES
            (:date_key, :transaction_type, :total_transactions, :total_amount_brl,
             :avg_amount_brl, :fraud_count, :fraud_rate, :processing_timestamp)
            """
        ),
        base,
    )
    db.commit()


# ── TestRepository ──────────────────────────────────────────────────────────────


class TestRepositoryTransactions:
    def test_list_transactions_returns_inserted_rows(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1")
        _insert_transaction(db, transaction_id="tx-2")
        rows = repository.list_transactions(db)
        assert len(rows) == 2

    def test_count_transactions_matches_list(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1")
        _insert_transaction(db, transaction_id="tx-2")
        assert repository.count_transactions(db) == 2

    def test_filter_by_customer_id(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1", customer_key="cust-a")
        _insert_transaction(db, transaction_id="tx-2", customer_key="cust-b")
        rows = repository.list_transactions(db, customer_id="cust-a")
        assert len(rows) == 1
        assert rows[0]["customer_key"] == "cust-a"

    def test_filter_by_date_range(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1", date_key="2024-01-01")
        _insert_transaction(db, transaction_id="tx-2", date_key="2024-06-15")
        _insert_transaction(db, transaction_id="tx-3", date_key="2024-12-31")
        rows = repository.list_transactions(
            db, start_date=date(2024, 3, 1), end_date=date(2024, 9, 1)
        )
        assert len(rows) == 1
        assert rows[0]["transaction_id"] == "tx-2"

    def test_filter_by_is_fraud(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1", is_fraud=True)
        _insert_transaction(db, transaction_id="tx-2", is_fraud=False)
        rows = repository.list_transactions(db, is_fraud=True)
        assert len(rows) == 1
        assert rows[0]["transaction_id"] == "tx-1"

    def test_pagination_limit_offset(self, db: Session):
        for i in range(5):
            _insert_transaction(db, transaction_id=f"tx-{i}", date_key=f"2024-01-0{i + 1}")
        page1 = repository.list_transactions(db, limit=2, offset=0)
        page2 = repository.list_transactions(db, limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        assert {r["transaction_id"] for r in page1}.isdisjoint(
            {r["transaction_id"] for r in page2}
        )

    def test_get_transaction_found(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1")
        row = repository.get_transaction(db, "tx-1")
        assert row is not None
        assert row["transaction_id"] == "tx-1"

    def test_get_transaction_not_found(self, db: Session):
        assert repository.get_transaction(db, "does-not-exist") is None


class TestRepositoryAlerts:
    def test_list_alerts_only_fraudulent(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1", is_fraud=True)
        _insert_transaction(db, transaction_id="tx-2", is_fraud=False)
        rows = repository.list_alerts(db)
        assert len(rows) == 1
        assert rows[0]["transaction_id"] == "tx-1"

    def test_count_alerts_matches_list(self, db: Session):
        _insert_transaction(db, transaction_id="tx-1", is_fraud=True)
        _insert_transaction(db, transaction_id="tx-2", is_fraud=True)
        _insert_transaction(db, transaction_id="tx-3", is_fraud=False)
        assert repository.count_alerts(db) == 2


class TestRepositoryDailyFraudMetrics:
    def test_list_and_count(self, db: Session):
        _insert_daily_metric(db, transaction_type="PIX")
        _insert_daily_metric(db, transaction_type="TED")
        assert repository.count_daily_fraud_metrics(db) == 2
        rows = repository.list_daily_fraud_metrics(db)
        assert len(rows) == 2

    def test_filter_by_transaction_type(self, db: Session):
        _insert_daily_metric(db, transaction_type="PIX")
        _insert_daily_metric(db, transaction_type="TED")
        rows = repository.list_daily_fraud_metrics(db, transaction_type="PIX")
        assert len(rows) == 1
        assert rows[0]["transaction_type"] == "PIX"


# ── TestRoutes ───────────────────────────────────────────────────────────────────


@pytest.fixture
def client(db: Session) -> TestClient:
    app.dependency_overrides[get_db] = lambda: db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


class _FailingSession:
    """Simula uma sessão que resolve normalmente (get_db não falha — uma
    Session do SQLAlchemy não abre conexão de fato até a primeira query) mas
    cuja execução de query falha, como uma conexão recusada/timeout real."""

    def execute(self, *args, **kwargs):
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))


@pytest.fixture
def failing_client() -> TestClient:
    app.dependency_overrides[get_db] = lambda: _FailingSession()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


class TestHealthRoutes:
    def test_live_always_ok(self):
        with TestClient(app) as client:
            response = client.get("/health/live")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_ready_ok_when_db_responds(self, client: TestClient):
        response = client.get("/health/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}

    def test_ready_503_when_db_unavailable(self, failing_client: TestClient):
        response = failing_client.get("/health/ready")
        assert response.status_code == 503


class TestTransactionRoutes:
    def test_list_returns_paginated_envelope(self, client: TestClient, db: Session):
        _insert_transaction(db, transaction_id="tx-1")
        _insert_transaction(db, transaction_id="tx-2")
        response = client.get("/transactions")
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 2
        assert body["page"] == 1
        assert body["page_size"] == 20
        assert len(body["items"]) == 2

    def test_list_respects_page_size(self, client: TestClient, db: Session):
        for i in range(5):
            _insert_transaction(db, transaction_id=f"tx-{i}", date_key=f"2024-01-0{i + 1}")
        response = client.get("/transactions", params={"page": 1, "page_size": 2})
        body = response.json()
        assert len(body["items"]) == 2
        assert body["total"] == 5

    def test_list_page_beyond_data_is_empty(self, client: TestClient, db: Session):
        _insert_transaction(db, transaction_id="tx-1")
        response = client.get("/transactions", params={"page": 5, "page_size": 20})
        body = response.json()
        assert body["items"] == []
        assert body["total"] == 1

    def test_page_size_over_max_is_rejected(self, client: TestClient):
        response = client.get("/transactions", params={"page_size": 1000})
        assert response.status_code == 422

    def test_filter_by_customer_id(self, client: TestClient, db: Session):
        _insert_transaction(db, transaction_id="tx-1", customer_key="cust-a")
        _insert_transaction(db, transaction_id="tx-2", customer_key="cust-b")
        response = client.get("/transactions", params={"customer_id": "cust-a"})
        body = response.json()
        assert body["total"] == 1
        assert body["items"][0]["customer_key"] == "cust-a"

    def test_get_by_id_found(self, client: TestClient, db: Session):
        _insert_transaction(db, transaction_id="tx-1", amount_brl=999.0)
        response = client.get("/transactions/tx-1")
        assert response.status_code == 200
        assert response.json()["amount_brl"] == 999.0

    def test_get_by_id_not_found(self, client: TestClient):
        response = client.get("/transactions/does-not-exist")
        assert response.status_code == 404

    def test_list_returns_503_on_db_failure(self, failing_client: TestClient):
        response = failing_client.get("/transactions")
        assert response.status_code == 503

    def test_get_by_id_returns_503_on_db_failure(self, failing_client: TestClient):
        response = failing_client.get("/transactions/tx-1")
        assert response.status_code == 503


class TestAlertRoutes:
    def test_list_only_fraudulent_transactions(self, client: TestClient, db: Session):
        _insert_transaction(db, transaction_id="tx-1", is_fraud=True)
        _insert_transaction(db, transaction_id="tx-2", is_fraud=False)
        response = client.get("/alerts")
        body = response.json()
        assert body["total"] == 1
        assert body["items"][0]["transaction_id"] == "tx-1"

    def test_returns_503_on_db_failure(self, failing_client: TestClient):
        response = failing_client.get("/alerts")
        assert response.status_code == 503


class TestKpiRoutes:
    def test_list_daily_fraud_metrics(self, client: TestClient, db: Session):
        _insert_daily_metric(db, transaction_type="PIX")
        _insert_daily_metric(db, transaction_type="TED")
        response = client.get("/kpis/fraud-daily")
        body = response.json()
        assert body["total"] == 2
        assert len(body["items"]) == 2

    def test_filter_by_transaction_type(self, client: TestClient, db: Session):
        _insert_daily_metric(db, transaction_type="PIX")
        _insert_daily_metric(db, transaction_type="TED")
        response = client.get("/kpis/fraud-daily", params={"transaction_type": "PIX"})
        body = response.json()
        assert body["total"] == 1
        assert body["items"][0]["transaction_type"] == "PIX"

    def test_returns_503_on_db_failure(self, failing_client: TestClient):
        response = failing_client.get("/kpis/fraud-daily")
        assert response.status_code == 503
