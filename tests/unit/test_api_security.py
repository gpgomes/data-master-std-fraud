"""Autenticação da API e log de auditoria de acesso (issue #58)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.common.config import settings
from src.serving.api import main
from src.serving.api.db import get_db
from src.serving.api.main import app
from src.serving.api.security import _matches, hash_key, key_id

PROTECTED = ["/transactions", "/transactions/tx-1", "/alerts", "/kpis/fraud-daily"]


def _empty_db():
    result = MagicMock()
    result.mappings.return_value.all.return_value = []
    result.mappings.return_value.first.return_value = None
    result.first.return_value = (0,)
    return MagicMock(execute=MagicMock(return_value=result))


@pytest.fixture
def client():
    app.dependency_overrides[get_db] = _empty_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


class TestAuthentication:
    @pytest.mark.parametrize("path", PROTECTED)
    def test_protected_routes_reject_a_request_without_key(self, client, api_key, path):
        response = client.get(path)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "ApiKey"

    @pytest.mark.parametrize("path", PROTECTED)
    def test_protected_routes_reject_a_wrong_key(self, client, api_key, path):
        assert client.get(path, headers={"X-API-Key": "chave-errada"}).status_code == 401

    def test_the_right_key_is_accepted(self, client, api_key):
        assert client.get("/alerts", headers={"X-API-Key": api_key}).status_code == 200

    def test_health_checks_stay_open(self, client, api_key):
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 200

    def test_without_configured_keys_everything_but_health_is_denied(self, client, monkeypatch):
        """Padrão seguro: sem `API_KEY_HASHES` nenhuma chave vale, nem uma vazia."""
        monkeypatch.setattr(settings.api, "api_key_hashes", "")
        assert client.get("/alerts", headers={"X-API-Key": ""}).status_code == 401
        assert client.get("/alerts", headers={"X-API-Key": "qualquer"}).status_code == 401
        assert client.get("/health/live").status_code == 200

    def test_several_keys_can_be_active(self, client, monkeypatch):
        """Rotação: a chave nova e a antiga valem ao mesmo tempo até a antiga ser retirada."""
        monkeypatch.setattr(
            settings.api, "api_key_hashes", f"{hash_key('antiga')}, {hash_key('nova').upper()}"
        )
        for key in ("antiga", "nova"):
            assert client.get("/alerts", headers={"X-API-Key": key}).status_code == 200

    def test_the_openapi_schema_documents_the_key(self, client):
        schema = client.get("/openapi.json").json()
        assert schema["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"


class TestKeyHandling:
    def test_key_id_is_a_short_prefix_of_the_hash(self):
        assert key_id(hash_key("k")) == hash_key("k")[:8]

    def test_matching_checks_every_accepted_hash(self):
        accepted = [hash_key("a"), hash_key("b"), hash_key("c")]
        assert _matches(hash_key("c"), accepted)
        assert not _matches(hash_key("d"), accepted)

    def test_cli_prints_the_hash(self, capsys, monkeypatch):
        import runpy
        import sys

        monkeypatch.setattr(sys, "argv", ["security", "minha-chave"])
        runpy.run_module("src.serving.api.security", run_name="__main__")
        assert capsys.readouterr().out.strip() == hash_key("minha-chave")


class TestAccessAudit:
    @pytest.fixture
    def audited(self, monkeypatch):
        monkeypatch.setattr(settings.observability, "enabled", True)
        rows: list[dict] = []
        monkeypatch.setattr(main.metrics_store, "record_api_access", rows.append)
        monkeypatch.setattr(main.metrics_store, "record_api_request", lambda row: None)
        return rows

    def test_an_authorized_request_is_audited_with_the_key_id_and_query(
        self, client, api_key, audited
    ):
        client.get("/alerts?customer_id=c-42&page=2", headers={"X-API-Key": api_key})
        assert len(audited) == 1
        row = audited[0]
        assert row["api_key_id"] == key_id(hash_key(api_key))
        assert (row["method"], row["route"], row["status_code"]) == ("GET", "/alerts", 200)
        assert row["query"] == "customer_id=c-42&page=2"

    def test_the_key_itself_is_never_written(self, client, api_key, audited):
        client.get("/alerts", headers={"X-API-Key": api_key})
        assert api_key not in repr(audited)

    def test_a_denied_request_is_audited_too(self, client, api_key, audited):
        client.get("/transactions", headers={"X-API-Key": "errada"})
        assert audited[0]["status_code"] == 401
        assert audited[0]["api_key_id"] is None

    def test_health_checks_are_not_audited(self, client, api_key, audited):
        client.get("/health/live")
        assert audited == []

    def test_long_queries_are_truncated(self, client, api_key, audited):
        client.get("/alerts?customer_id=" + "x" * 2000, headers={"X-API-Key": api_key})
        assert len(audited[0]["query"]) == 500
