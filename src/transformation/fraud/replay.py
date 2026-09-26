"""Datasets sintéticos com ground truth para avaliar detectores (issue #44).

Dois regimes, porque a densidade de eventos por cliente decide o que um detector consegue ver:

  - **replay de stream** (`simulate_stream`): o `TransactionStream` do producer rodando em tempo
    simulado (sem `sleep`), com vários eventos por cliente por hora. É a densidade em que o
    detector de streaming de fato roda, e o regime principal da avaliação;
  - **densidade de batch** (`simulate_batch`): o `generate_transactions` do `make seed-data`
    (~50 eventos por cliente em 180 dias). Serve para mostrar que um baseline de janela curta não
    existe nesse regime (o Z-Score de 1 h por cliente fica cego).

Tudo é determinístico: a mesma seed e a mesma configuração geram exatamente os mesmos eventos
(clientes com `reference_date` fixa; nada depende de `now`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from src.common.data_generator import DataGenerator, TransactionStream


@dataclass(frozen=True)
class ReplayConfig:
    """Parâmetros do replay de stream."""

    n_customers: int = 1_000
    rate_tps: float = 10.0
    duration_minutes: int = 240
    # Até o corte (start + warmup) os eventos só alimentam o estado do detector (a janela de 1 h do
    # Z-Score; o perfil, na #45). A avaliação usa só o que vem depois.
    warmup_minutes: int = 60
    start: datetime = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)

    @property
    def cutoff(self) -> datetime:
        return self.start + timedelta(minutes=self.warmup_minutes)


@dataclass
class ReplayDataset:
    """Eventos ordenados por timestamp e o ground truth por `transaction_id`."""

    seed: int
    events: list[dict[str, Any]]
    truth: dict[str, dict[str, Any]]
    cutoff: datetime
    label: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def simulate_stream(seed: int, cfg: ReplayConfig) -> ReplayDataset:
    """Roda o `TransactionStream` em tempo simulado e recolhe eventos e ground truth.

    Os follow-ups de um episódio (clone de cartão, saques seguintes do account takeover) já vêm com
    o timestamp certo, então nada fica retido: episódios que terminam depois da janela entram
    inteiros, sem viés de truncamento na taxa de fraude.
    """
    gen = DataGenerator(seed=seed)
    customers = gen.generate_customers(cfg.n_customers, reference_date=cfg.start.date())
    stream = TransactionStream(gen, customers, record_ground_truth=True)

    step = 1.0 / cfg.rate_tps
    events: list[dict[str, Any]] = []
    for tick in range(int(cfg.duration_minutes * 60 * cfg.rate_tps)):
        now = cfg.start + timedelta(seconds=tick * step)
        events.extend(tx for _, tx in stream.next_events(now))

    # Ordem estável por timestamp ISO (formato uniforme "+00:00": lexicográfica = cronológica).
    events.sort(key=lambda e: e["timestamp"])
    truth = {row["transaction_id"]: row for row in stream.ground_truth}
    return ReplayDataset(seed, events, truth, cfg.cutoff, label="stream")


def simulate_batch(
    seed: int, n_customers: int, n_transactions: int, end: datetime, days: int = 180
) -> ReplayDataset:
    """Dataset com a densidade do `make seed-data`: `n_transactions` espalhadas em `days` dias."""
    gen = DataGenerator(seed=seed)
    customers = gen.generate_customers(n_customers, reference_date=end.date())
    start = end - timedelta(days=days)
    events = gen.generate_transactions(customers, n=n_transactions, start_date=start, end_date=end)
    truth = {row["transaction_id"]: row for row in gen.last_ground_truth}
    return ReplayDataset(seed, events, truth, start, label="batch")
