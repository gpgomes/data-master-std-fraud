"""Portão do E2E (issue #57): só roda quando pedido explicitamente, e só no namespace isolado.

`make e2e` exporta `E2E=1` e as variáveis de `E2E_ENV`. Sem isso a suíte inteira é pulada, então
`make test` (que coleta `tests/`) nunca dispara o E2E sem querer nem apaga dado de desenvolvimento.
"""

from __future__ import annotations

import os
import subprocess

import pytest

from tests.e2e.harness import ROOT, env_is_e2e


def pytest_collection_modifyitems(config, items):
    reason = None
    if os.environ.get("E2E") != "1":
        reason = "E2E desligado: rode `make e2e` (exporta E2E=1 e o namespace e2e-*)"
    elif not env_is_e2e():
        reason = "E2E=1 sem o namespace isolado (E2E_ENV): recusado para não tocar em dado de dev"
    else:
        ps = subprocess.run(
            ["docker", "compose", "ps", "--services", "--status", "running"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        needed = {"kafka", "minio", "postgres", "spark-master"}
        missing = needed - set(ps.stdout.split())
        if ps.returncode != 0 or missing:
            reason = f"stack do E2E fora do ar (faltam: {sorted(missing) or 'docker'})"
    if reason:
        skip = pytest.mark.skip(reason=reason)
        for item in items:
            if "tests/e2e/" in str(item.fspath).replace(os.sep, "/"):
                item.add_marker(skip)
