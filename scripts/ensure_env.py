"""Cria/completa o `.env` local com segredos gerados na máquina (issue #58).

`make env` roda este script. Ele copia o `.env.example` quando não há `.env` e gera o que é segredo
e nunca deve ser versionado: a Fernet key, a secret key e o segredo JWT do Airflow 3 (#69), a secret key do Superset e a
chave da API (só o hash SHA-256 vai para `API_KEY_HASHES`; a chave em texto fica em `API_DEV_KEY`,
no `.env` local, para o desenvolvedor usar no header `X-API-Key`).

É idempotente: um valor que já existe e não é placeholder nunca é sobrescrito. Trocar a Fernet key
depois invalida o que o Airflow cifrou com a anterior, e trocar a do Superset invalida a senha da
conexão com o Postgres guardada nele; por isso a regeração é sempre manual (apague a linha).

    python -m scripts.ensure_env            # .env na raiz do repositório
    python -m scripts.ensure_env --path x   # outro arquivo (testes)
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import secrets
import shutil
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / ".env.example"

# Valor vazio ou começando por um destes prefixos = ainda não configurado.
PLACEHOLDER_PREFIXES = ("your-", "change-me")


def fernet_key() -> str:
    """Mesmo formato de `cryptography.fernet.Fernet.generate_key()`, sem depender da lib."""
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()


def token() -> str:
    return secrets.token_urlsafe(32)


GENERATED: dict[str, Callable[[], str]] = {
    "AIRFLOW__CORE__FERNET_KEY": fernet_key,
    "AIRFLOW__API__SECRET_KEY": token,
    "AIRFLOW__API_AUTH__JWT_SECRET": token,
    "SUPERSET_SECRET_KEY": token,
    "API_DEV_KEY": token,
}


def is_placeholder(value: str | None) -> bool:
    return value is None or not value.strip() or value.strip().startswith(PLACEHOLDER_PREFIXES)


def parse(lines: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, value = stripped.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def _set(lines: list[str], key: str, value: str) -> None:
    for i, line in enumerate(lines):
        if line.strip().startswith(f"{key}="):
            lines[i] = f"{key}={value}"
            return
    lines.append(f"{key}={value}")


def ensure_env(path: Path, example: Path = EXAMPLE) -> list[str]:
    """Garante o arquivo e os segredos. Devolve as variáveis que foram geradas agora."""
    if not path.exists():
        shutil.copyfile(example, path)
    lines = path.read_text(encoding="utf-8").splitlines()
    values = parse(lines)
    generated: list[str] = []
    for key, make in GENERATED.items():
        if is_placeholder(values.get(key)):
            values[key] = make()
            _set(lines, key, values[key])
            generated.append(key)
    # O hash acompanha a chave de dev: se a chave foi gerada agora ou não há hash, ele é recalculado.
    dev_hash = hashlib.sha256(values["API_DEV_KEY"].encode()).hexdigest()
    hashes = [h for h in values.get("API_KEY_HASHES", "").split(",") if h.strip()]
    if dev_hash not in hashes:
        _set(lines, "API_KEY_HASHES", ",".join([*hashes, dev_hash]))
        generated.append("API_KEY_HASHES")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(0o600)
    return generated


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--path", type=Path, default=ROOT / ".env")
    args = parser.parse_args()
    generated = ensure_env(args.path)
    if generated:
        print(f"{args.path.name}: gerado {', '.join(generated)}")
        print("Chave da API (header X-API-Key): veja API_DEV_KEY no .env")
    else:
        print(f"{args.path.name}: nada a gerar, segredos já configurados")


if __name__ == "__main__":
    main()
