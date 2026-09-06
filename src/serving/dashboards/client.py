"""Cliente REST mínimo do Superset (issue #16).

Autenticação JWT + CSRF (obrigatório em toda escrita da API do Superset
3.x) via `requests.Session` — a sessão guarda o cookie e o header
`X-CSRFToken` automaticamente para as chamadas seguintes. Sem SDK externo:
a API pública do Superset já é suficiente e evita mais uma dependência.
"""

from __future__ import annotations

from typing import Any, cast

import requests

from src.common.config import settings
from src.common.logger import get_logger

logger = get_logger("superset_dashboards")


class SupersetClient:
    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.base_url = (base_url or settings.superset.url).rstrip("/")
        self._session = requests.Session()
        self._login(username or settings.superset.admin_user, password or settings.superset.admin_password)

    def _login(self, username: str, password: str) -> None:
        resp = self._session.post(
            f"{self.base_url}/api/v1/security/login",
            json={"username": username, "password": password, "provider": "db", "refresh": True},
        )
        resp.raise_for_status()
        access_token = resp.json()["access_token"]
        self._session.headers["Authorization"] = f"Bearer {access_token}"

        csrf_resp = self._session.get(f"{self.base_url}/api/v1/security/csrf_token/")
        csrf_resp.raise_for_status()
        self._session.headers["X-CSRFToken"] = csrf_resp.json()["result"]
        self._session.headers["Referer"] = f"{self.base_url}/"
        logger.info("Autenticado no Superset", base_url=self.base_url, username=username)

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self._session.get(f"{self.base_url}{path}", **kwargs)
        resp.raise_for_status()
        return resp

    def post(self, path: str, json: dict) -> requests.Response:
        resp = self._session.post(f"{self.base_url}{path}", json=json)
        resp.raise_for_status()
        return resp

    def put(self, path: str, json: dict) -> requests.Response:
        resp = self._session.put(f"{self.base_url}{path}", json=json)
        resp.raise_for_status()
        return resp

    def delete(self, path: str) -> requests.Response:
        resp = self._session.delete(f"{self.base_url}{path}")
        resp.raise_for_status()
        return resp

    def find_one(self, list_path: str, match: dict, page_size: int = 100) -> dict | None:
        """Lista `list_path` (paginado) e retorna o primeiro item cujos campos
        batem com `match` — usado para tornar create/update idempotente
        (a API de listagem do Superset aceita filtros Rison, mas filtrar em
        Python é mais simples e robusto o suficiente no volume deste projeto).
        """
        page = 0
        while True:
            resp = self.get(f"{list_path}?q=(page:{page},page_size:{page_size})")
            body = resp.json()
            for item in body["result"]:
                if all(item.get(k) == v for k, v in match.items()):
                    return cast(dict, item)
            if (page + 1) * page_size >= body["count"]:
                return None
            page += 1
