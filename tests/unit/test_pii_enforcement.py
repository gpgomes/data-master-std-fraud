"""Governança de PII aplicada, não só documentada (issue #58).

O catálogo declara as colunas PII de cada dataset (`PII_COLUMNS`). Estes testes garantem que:
- a declaração é coerente com a classificação do catálogo;
- nenhuma coluna PII aparece em modelo de resposta da API, nem nas consultas que a API faz;
- toda rota que devolve dado passa por um desses modelos (uma rota que devolvesse um dict cru
  escaparia do teste acima).
"""

from __future__ import annotations

import inspect

from pydantic import BaseModel

from src.governance.data_catalog.registry import CATALOG_BY_KEY, PII_COLUMNS
from src.serving.api import repository
from src.serving.api.main import app
from src.serving.api.models import schemas

ALL_PII = {column for columns in PII_COLUMNS.values() for column in columns}


def _response_models() -> list[type[BaseModel]]:
    return [
        obj
        for _, obj in inspect.getmembers(schemas, inspect.isclass)
        if issubclass(obj, BaseModel) and obj.__module__ == schemas.__name__
    ]


def test_every_pii_declaration_points_at_a_pii_classified_dataset():
    for key in PII_COLUMNS:
        assert key in CATALOG_BY_KEY, key
        assert "PII" in CATALOG_BY_KEY[key].classification, key


def test_no_api_response_model_exposes_a_pii_column():
    models = _response_models()
    assert models, "nenhum modelo encontrado: o teste não verificaria nada"
    for model in models:
        leaked = set(model.model_fields) & ALL_PII
        assert not leaked, f"{model.__name__} expõe PII: {sorted(leaked)}"


def test_the_api_queries_never_select_a_pii_column():
    for name in ("_TRANSACTION_COLUMNS", "_ALERT_COLUMNS"):
        selected = {c.strip().split()[0] for c in getattr(repository, name).split(",")}
        assert not selected & ALL_PII, name


def test_every_data_route_declares_a_response_model():
    data_routes = [
        route
        for route in app.routes
        if getattr(route, "path", "").split("/")[1:2] in (["transactions"], ["alerts"], ["kpis"])
    ]
    assert data_routes
    for route in data_routes:
        assert route.response_model is not None, route.path


def test_the_check_would_catch_a_leak():
    """O teste principal não é vacuoso: um modelo com `ip_address` reprovaria."""

    class Leaky(BaseModel):
        transaction_id: str
        ip_address: str

    assert set(Leaky.model_fields) & ALL_PII == {"ip_address"}
