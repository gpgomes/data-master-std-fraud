"""Teste de carga do stream (issue #56): throughput × latência por nível de carga.

Para cada nível (TPS alvo), sobe N instâncias do producer de transações no host, cada uma com uma
`GENERATOR_SEED` própria (com a mesma seed, instâncias iniciadas no mesmo segundo repetiriam
`transaction_id`), espera um aquecimento, mede uma janela e para os producers. A medição vem de três
fontes:

  * Kafka: offsets no início e no fim da janela → eventos/s **produzidos** de fato;
  * `stream_batch_metrics` (issue #55): linhas/s processadas, duração do micro-batch, lag, tamanho do
    estado curto e a latência p95 de cada micro-batch;
  * `docker stats`: CPU e memória dos containers do Spark durante a janela.

Uma instância do producer não passa de algumas dezenas de TPS (envio síncrono: cada mensagem espera
o ack do Kafka), por isso a carga é dividida entre instâncias (`--per-instance-tps`).

Pré-requisitos: stack de pé, `make spark-submit-stream` rodando (sozinho, sem outras cargas no Docker)
e o perfil no Gold. Uso:

    python -m scripts.stream_load_test --levels 20,50,100,200 --duration-s 240 --output <arquivo.json>

A latência por evento (p50/p95/p99 exatos) é um passo separado, depois de parar o stream e rodar
`make spark-submit-stream-postgres`: `--latency-from <arquivo.json>` lê as janelas gravadas e calcula os
percentis sobre `stream_scored_transactions.latency_seconds`, completando o mesmo JSON.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.config import settings  # noqa: E402

SPARK_CONTAINERS = ("spark-master", "spark-worker-1", "spark-worker-2")
# Os containers do Spark rodam com TZ=America/Sao_Paulo e o JDBC grava a hora local em
# `stream_scored_transactions`; as janelas (UTC) são convertidas antes de filtrar.
SPARK_TZ = ZoneInfo("America/Sao_Paulo")
TRIGGER_S = 10.0


@dataclass
class LevelPlan:
    target_tps: float
    instances: int
    tps_per_instance: float


@dataclass
class LevelResult:
    target_tps: float
    instances: int
    window_start: str
    window_end: str
    produced_tps: float
    batches: int = 0
    input_rps: float | None = None
    processed_rps: float | None = None
    duration_p50_s: float | None = None
    duration_p95_s: float | None = None
    batch_latency_p50_s: float | None = None
    batch_latency_p95_s: float | None = None
    lag_max: int | None = None
    lag_end: int | None = None
    state_rows_max: int | None = None
    rows_scored: int | None = None
    cpu_pct: dict[str, float] = field(default_factory=dict)
    mem_mib: dict[str, float] = field(default_factory=dict)
    event_latency_p50_s: float | None = None
    event_latency_p95_s: float | None = None
    event_latency_p99_s: float | None = None
    events_measured: int | None = None
    saturated: bool = False
    saturation_reasons: list[str] = field(default_factory=list)


def plan_level(target_tps: float, per_instance_tps: float) -> LevelPlan:
    """Divide o TPS alvo entre instâncias do producer, nenhuma acima de `per_instance_tps`."""
    instances = max(1, math.ceil(target_tps / per_instance_tps))
    return LevelPlan(target_tps, instances, round(target_tps / instances, 3))


def saturation(result: LevelResult) -> list[str]:
    """Motivos para considerar o nível saturado (vazio = o stream acompanha a carga).

    - micro-batch mais longo que o trigger: o próximo batch começa atrasado e o atraso acumula;
    - lag no fim da janela maior que um trigger inteiro de eventos: o stream não drena o que chega;
    - processadas/s claramente abaixo de produzidas/s.
    """
    reasons = []
    if result.duration_p95_s is not None and result.duration_p95_s > TRIGGER_S:
        reasons.append(f"duração p95 {result.duration_p95_s:.1f} s > trigger {TRIGGER_S:.0f} s")
    if result.lag_end is not None and result.lag_end > result.produced_tps * TRIGGER_S:
        reasons.append(f"lag no fim {result.lag_end} > {result.produced_tps * TRIGGER_S:.0f}")
    if result.input_rps and result.produced_tps and result.input_rps < 0.9 * result.produced_tps:
        reasons.append(
            f"stream lê {result.input_rps:.1f}/s < 90% do produzido ({result.produced_tps:.1f}/s)"
        )
    return reasons


# ── Kafka ───────────────────────────────────────────────────────────────────────


def topic_end_offset(topic: str) -> int:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or []]
        return int(sum(consumer.end_offsets(partitions).values()))
    finally:
        consumer.close()


# ── Producers ───────────────────────────────────────────────────────────────────


def start_producers(plan: LevelPlan, log_dir: Path, seed_base: int) -> list[subprocess.Popen]:
    procs = []
    for i in range(plan.instances):
        env = {
            **os.environ,
            "PRODUCER_RATE_TPS": str(plan.tps_per_instance),
            "GENERATOR_SEED": str(seed_base + i),
            "SOURCE_SYSTEM": "load-test",
        }
        log = open(log_dir / f"producer_{int(plan.target_tps)}tps_{i}.log", "w")  # noqa: SIM115
        procs.append(
            subprocess.Popen(
                [sys.executable, "-m", "src.ingestion.streaming.kafka_producer_transactions"],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        )
    return procs


def stop_producers(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        p.send_signal(signal.SIGINT)
    for p in procs:
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()


# ── docker stats ────────────────────────────────────────────────────────────────


def _parse_mem_mib(value: str) -> float:
    used = value.split("/")[0].strip()
    units = {"KiB": 1 / 1024, "MiB": 1.0, "GiB": 1024.0, "B": 1 / (1024 * 1024)}
    for unit, factor in units.items():
        if used.endswith(unit):
            return float(used[: -len(unit)]) * factor
    return float("nan")


def sample_docker_stats(stop: threading.Event, samples: list[dict], every_s: float = 10.0) -> None:
    while not stop.is_set():
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *SPARK_CONTAINERS],
            capture_output=True,
            text=True,
        ).stdout
        for line in out.splitlines():
            row = json.loads(line)
            samples.append(
                {
                    "name": row["Name"],
                    "cpu": float(row["CPUPerc"].rstrip("%")),
                    "mem": _parse_mem_mib(row["MemUsage"]),
                }
            )
        stop.wait(every_s)


def summarize_stats(samples: list[dict]) -> tuple[dict[str, float], dict[str, float]]:
    cpu: dict[str, list[float]] = {}
    mem: dict[str, list[float]] = {}
    for s in samples:
        cpu.setdefault(s["name"], []).append(s["cpu"])
        mem.setdefault(s["name"], []).append(s["mem"])
    return (
        {k: round(sum(v) / len(v), 1) for k, v in cpu.items()},
        {k: round(max(v), 0) for k, v in mem.items()},
    )


# ── Postgres ────────────────────────────────────────────────────────────────────

_WINDOW_SQL = """
SELECT count(*),
       avg(input_rows_per_second), avg(processed_rows_per_second),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY batch_duration_ms) / 1000.0,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY batch_duration_ms) / 1000.0,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_p95_s),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_p95_s),
       max(kafka_lag), max(state_rows), sum(rows_scored)
FROM stream_batch_metrics
WHERE batch_timestamp >= %s AND batch_timestamp < %s
"""
_LAG_END_SQL = """
SELECT kafka_lag FROM stream_batch_metrics
WHERE batch_timestamp >= %s AND batch_timestamp < %s
ORDER BY batch_timestamp DESC LIMIT 1
"""


def stream_window(start: datetime, end: datetime) -> tuple:
    import psycopg2

    conn = psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.db,
        user=settings.postgres.user,
        password=settings.postgres.password,
    )
    try:
        with conn.cursor() as cur:
            params = (start.replace(tzinfo=None), end.replace(tzinfo=None))
            cur.execute(_WINDOW_SQL, params)
            row = cur.fetchone()
            cur.execute(_LAG_END_SQL, params)
            lag_end = cur.fetchone()
        return row, (lag_end[0] if lag_end else None)
    finally:
        conn.close()


_EVENT_LATENCY_SQL = """
SELECT count(*),
       percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_seconds),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_seconds),
       percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_seconds)
FROM stream_scored_transactions
WHERE processing_timestamp >= %s AND processing_timestamp < %s AND latency_seconds IS NOT NULL
"""


def _to_spark_local(iso: str) -> datetime:
    return datetime.fromisoformat(iso).astimezone(SPARK_TZ).replace(tzinfo=None)


def event_latency(window_start: str, window_end: str) -> tuple:
    import psycopg2

    conn = psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.db,
        user=settings.postgres.user,
        password=settings.postgres.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                _EVENT_LATENCY_SQL, (_to_spark_local(window_start), _to_spark_local(window_end))
            )
            return cur.fetchone()
    finally:
        conn.close()


def fill_event_latency(path: Path) -> list[dict]:
    """Completa o JSON de um teste com a latência por evento de cada janela."""
    results = json.loads(path.read_text(encoding="utf-8"))
    for r in results:
        n, p50, p95, p99 = event_latency(r["window_start"], r["window_end"])
        r.update(
            events_measured=int(n or 0),
            event_latency_p50_s=_round(p50),
            event_latency_p95_s=_round(p95),
            event_latency_p99_s=_round(p99),
        )
    path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    return results


def _int(value) -> int | None:
    """O psycopg2 devolve `Decimal` para `sum()`/`max()` de BIGINT; o JSON não serializa."""
    return None if value is None else int(value)


def _round(value, digits=2):
    return None if value is None else round(float(value), digits)


# ── Execução ────────────────────────────────────────────────────────────────────


def run_level(
    plan: LevelPlan, warmup_s: float, duration_s: float, log_dir: Path, seed_base: int
) -> LevelResult:
    procs = start_producers(plan, log_dir, seed_base)
    try:
        time.sleep(warmup_s)
        start, offset_start = datetime.now(tz=UTC), topic_end_offset(
            settings.kafka.topic_transactions
        )
        stop, samples = threading.Event(), []
        sampler = threading.Thread(target=sample_docker_stats, args=(stop, samples), daemon=True)
        sampler.start()
        time.sleep(duration_s)
        end, offset_end = datetime.now(tz=UTC), topic_end_offset(settings.kafka.topic_transactions)
        stop.set()
        sampler.join(timeout=30)
    finally:
        stop_producers(procs)

    # Espera o listener gravar os micro-batches da janela.
    time.sleep(2 * TRIGGER_S)
    row, lag_end = stream_window(start, end)
    cpu, mem = summarize_stats(samples)
    result = LevelResult(
        target_tps=plan.target_tps,
        instances=plan.instances,
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        produced_tps=round((offset_end - offset_start) / (end - start).total_seconds(), 2),
        batches=int(row[0] or 0),
        input_rps=_round(row[1]),
        processed_rps=_round(row[2]),
        duration_p50_s=_round(row[3]),
        duration_p95_s=_round(row[4]),
        batch_latency_p50_s=_round(row[5]),
        batch_latency_p95_s=_round(row[6]),
        lag_max=_int(row[7]),
        lag_end=_int(lag_end),
        state_rows_max=_int(row[8]),
        rows_scored=_int(row[9]),
        cpu_pct=cpu,
        mem_mib=mem,
    )
    result.saturation_reasons = saturation(result)
    result.saturated = bool(result.saturation_reasons)
    return result


def wait_for_drain(max_wait_s: float) -> None:
    """Entre níveis, espera o stream alcançar o fim do tópico (lag 0) para um nível não contaminar
    o próximo. Se não alcançar no tempo, segue (e o próximo nível começa com backlog)."""
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        _, lag = stream_window(datetime(1970, 1, 1), datetime(2100, 1, 1))
        if lag == 0:
            return
        time.sleep(TRIGGER_S)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Teste de carga do stream (issue #56).")
    parser.add_argument(
        "--levels", default="20,50,100,200", help="TPS alvo, separados por vírgula."
    )
    parser.add_argument("--duration-s", type=float, default=240)
    parser.add_argument("--warmup-s", type=float, default=60)
    parser.add_argument("--per-instance-tps", type=float, default=25)
    parser.add_argument("--drain-max-s", type=float, default=180)
    parser.add_argument("--output", help="JSON de saída do teste de carga.")
    parser.add_argument("--log-dir", default=None)
    parser.add_argument(
        "--latency-from",
        help="Só completa este JSON com a latência por evento (stream parado e carregado no Postgres).",
    )
    args = parser.parse_args(argv)

    if args.latency_from:
        for r in fill_event_latency(Path(args.latency_from)):
            print(
                json.dumps(
                    {
                        k: r[k]
                        for k in (
                            "target_tps",
                            "events_measured",
                            "event_latency_p50_s",
                            "event_latency_p95_s",
                            "event_latency_p99_s",
                        )
                    }
                )
            )
        return
    if not args.output:
        parser.error("--output é obrigatório (ou use --latency-from)")

    output = Path(args.output)
    log_dir = Path(args.log_dir) if args.log_dir else output.parent
    log_dir.mkdir(parents=True, exist_ok=True)
    seed_base = int(time.time())

    results = []
    for i, level in enumerate(float(v) for v in args.levels.split(",")):
        plan = plan_level(level, args.per_instance_tps)
        print(
            f"[load-test] nível {level:g} TPS: {plan.instances} producer(s) de {plan.tps_per_instance:g}"
        )
        result = run_level(plan, args.warmup_s, args.duration_s, log_dir, seed_base + 1000 * i)
        results.append(asdict(result))
        output.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[load-test] {json.dumps(asdict(result), ensure_ascii=False)}")
        wait_for_drain(args.drain_max_s)


if __name__ == "__main__":
    main(sys.argv[1:])
