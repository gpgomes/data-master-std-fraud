"""Autenticação da API por API key (issue #58).

O cliente manda a chave no header `X-API-Key`. A configuração guarda só o **hash SHA-256** de cada
chave aceita (`API_KEY_HASHES`), nunca a chave: vazar o `.env` não vaza credencial utilizável. A
comparação é em tempo constante (`hmac.compare_digest`) e percorre todos os hashes, para o tempo de
resposta não indicar quanto da chave acertou.

Rotas de health ficam abertas (orquestradores as consultam sem credencial); todas as outras exigem a
chave. Sem nenhum hash configurado a API recusa tudo: o padrão é negar.

A dependência grava em `request.state.api_key_id` um identificador curto da chave (os 8 primeiros
caracteres do hash, que não permitem recuperá-la), usado pelo log de auditoria de acesso.

Gerar o hash de uma chave nova:

    python -m src.serving.api.security <chave>
"""

from __future__ import annotations

import hashlib
import hmac
import sys

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from src.common.config import settings

API_KEY_HEADER = "X-API-Key"
_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False, description="Chave de acesso à API")


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def key_id(key_hash: str) -> str:
    """Identificador da chave para auditoria: não reversível e curto o bastante para não ser o hash."""
    return key_hash[:8]


def _matches(candidate_hash: str, accepted: list[str]) -> bool:
    found = False
    for accepted_hash in accepted:  # sem break: o tempo não depende de qual (ou se alguma) casou
        found |= hmac.compare_digest(candidate_hash, accepted_hash)
    return found


def require_api_key(request: Request, api_key: str | None = Security(_header)) -> str:
    """Dependência das rotas protegidas. Devolve o id da chave; 401 sem chave ou com chave errada."""
    accepted = settings.api.key_hash_list
    if api_key and accepted:
        candidate = hash_key(api_key)
        if _matches(candidate, accepted):
            request.state.api_key_id = key_id(candidate)
            return request.state.api_key_id
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="API key ausente ou inválida",
        headers={"WWW-Authenticate": "ApiKey"},
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("uso: python -m src.serving.api.security <chave>", file=sys.stderr)
        sys.exit(2)
    print(hash_key(sys.argv[1]))
