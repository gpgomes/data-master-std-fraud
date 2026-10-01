"""Exceções dos scans de segurança: validação e geração dos arquivos de cada ferramenta (issue #58).

A fonte única é `security/exceptions.toml`. O workflow `security.yml` usa este script para:

    python -m scripts.security_exceptions check             # motivo + data de revisão não vencida
    python -m scripts.security_exceptions pip-audit-args    # imprime os --ignore-vuln
    python -m scripts.security_exceptions gitleaksignore    # escreve .gitleaksignore
    python -m scripts.security_exceptions trivyignore       # escreve .trivyignore
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
EXCEPTIONS = ROOT / "security" / "exceptions.toml"
# Campo que identifica o achado em cada ferramenta.
TOOLS = {"pip_audit": "id", "gitleaks": "fingerprint", "trivy": "id"}
MAX_HORIZON_DAYS = 366


def load(path: Path = EXCEPTIONS) -> dict[str, list[dict[str, Any]]]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    unknown = set(data) - set(TOOLS)
    if unknown:
        raise ValueError(f"seção desconhecida em {path.name}: {sorted(unknown)}")
    return {tool: data.get(tool, []) for tool in TOOLS}


def problems(data: dict[str, list[dict[str, Any]]], today: date) -> list[str]:
    """Tudo que impede o arquivo de valer: campo faltando, data vencida ou longe demais, duplicata."""
    found: list[str] = []
    for tool, key in TOOLS.items():
        seen: set[str] = set()
        for entry in data[tool]:
            ident = entry.get(key)
            label = f"{tool}:{ident}"
            if not ident:
                found.append(f"{tool}: exceção sem `{key}`")
                continue
            if ident in seen:
                found.append(f"{label}: duplicada")
            seen.add(ident)
            if not str(entry.get("reason", "")).strip():
                found.append(f"{label}: sem `reason`")
            review_by = entry.get("review_by")
            if not isinstance(review_by, date):
                found.append(f"{label}: `review_by` ausente ou não é uma data (AAAA-MM-DD)")
            elif review_by < today:
                found.append(f"{label}: revisão vencida em {review_by.isoformat()}")
            elif (review_by - today).days > MAX_HORIZON_DAYS:
                found.append(f"{label}: `review_by` a mais de {MAX_HORIZON_DAYS} dias")
    return found


def pip_audit_args(data: dict[str, list[dict[str, Any]]]) -> list[str]:
    args: list[str] = []
    for entry in data["pip_audit"]:
        args += ["--ignore-vuln", entry["id"]]
    return args


def ignore_file(data: dict[str, list[dict[str, Any]]], tool: str) -> str:
    header = f"# Gerado por scripts/security_exceptions.py a partir de {EXCEPTIONS.relative_to(ROOT)}\n"
    return header + "".join(f"{entry[TOOLS[tool]]}\n" for entry in data[tool])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["check", "pip-audit-args", "gitleaksignore", "trivyignore"])
    parser.add_argument("--file", type=Path, default=EXCEPTIONS)
    parser.add_argument("--out", type=Path, help="destino do arquivo gerado (padrão: raiz do repo)")
    args = parser.parse_args(argv)
    data = load(args.file)

    if args.command == "check":
        found = problems(data, date.today())
        for problem in found:
            print(f"::error file={args.file.name}::{problem}")
        total = sum(len(v) for v in data.values())
        print(f"{total} exceções, {len(found)} problema(s)")
        return 1 if found else 0
    if args.command == "pip-audit-args":
        print(" ".join(pip_audit_args(data)))
        return 0
    tool = "gitleaks" if args.command == "gitleaksignore" else "trivy"
    out = args.out or ROOT / f".{args.command}"
    out.write_text(ignore_file(data, tool), encoding="utf-8")
    print(f"{out.name}: {len(data[tool])} exceção(ões)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
