"""Métricas por micro-batch do stream, via `StreamingQueryListener` (issue #55).

O Spark chama `onQueryProgress` no driver ao fim de cada micro-batch com um
`StreamingQueryProgress`: linhas de entrada, linhas/s, duração por fase e, por fonte, o offset
processado (`endOffset`) e o último offset disponível no Kafka (`latestOffset`). A diferença entre
os dois, somada por partição, é o **lag** do consumidor.

O que só o `_process_batch` sabe (linhas pontuadas, alertas, tamanho do estado curto e a latência
evento → processamento do micro-batch) chega por um dicionário compartilhado, `batch_stats`,
preenchido pelo processor e consumido aqui pelo `batch_id`. As duas coisas rodam no mesmo
processo (o driver).

Compatível com Python 3.8: roda no container do Spark.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

from src.common.logger import get_logger
from src.observability.store import MetricsStore

logger = get_logger("stream_metrics")


def _offsets(raw: str | None) -> dict[str, dict[str, int]]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def kafka_lag(end_offset: str | None, latest_offset: str | None) -> int | None:
    """Soma, por tópico e partição, de (último offset disponível − offset processado).

    Devolve None quando o Spark não informou os offsets (a fonte não é Kafka, ou primeiro batch).
    """
    end, latest = _offsets(end_offset), _offsets(latest_offset)
    if not end or not latest:
        return None
    lag = 0
    for topic, partitions in latest.items():
        for partition, last in partitions.items():
            done = end.get(topic, {}).get(partition)
            if done is not None:
                lag += max(int(last) - int(done), 0)
    return lag


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def progress_to_row(progress: Any, batch_stats: dict[int, dict[str, Any]]) -> dict[str, Any]:
    """Uma linha de `stream_batch_metrics` a partir do progresso do Spark e das estatísticas do
    processor. Função pura, testável sem Spark."""
    durations = progress.durationMs or {}
    sources = list(progress.sources or [])
    lags = [kafka_lag(s.endOffset, s.latestOffset) for s in sources]
    known = [lag for lag in lags if lag is not None]
    stats = batch_stats.pop(progress.batchId, {})
    return {
        "query_id": str(progress.id),
        "run_id": str(progress.runId),
        "batch_id": progress.batchId,
        "batch_timestamp": _parse_timestamp(progress.timestamp),
        "num_input_rows": progress.numInputRows,
        "input_rows_per_second": progress.inputRowsPerSecond,
        "processed_rows_per_second": progress.processedRowsPerSecond,
        "batch_duration_ms": durations.get("triggerExecution"),
        "add_batch_ms": durations.get("addBatch"),
        "get_offset_ms": durations.get("latestOffset"),
        "kafka_lag": sum(known) if known else None,
        "rows_scored": stats.get("rows_scored"),
        "alerts": stats.get("alerts"),
        "state_rows": stats.get("state_rows"),
        "latency_p50_s": stats.get("latency_p50_s"),
        "latency_p95_s": stats.get("latency_p95_s"),
        "latency_max_s": stats.get("latency_max_s"),
    }


class StreamMetricsListener(StreamingQueryListener):
    """Grava uma linha em `stream_batch_metrics` por micro-batch. Nunca levanta exceção."""

    def __init__(self, store: MetricsStore, batch_stats: dict[int, dict[str, Any]]) -> None:
        self._store = store
        self._batch_stats = batch_stats

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802 - API do Spark
        self._store.ensure_schema()

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        try:
            row = progress_to_row(event.progress, self._batch_stats)
        except Exception as exc:  # noqa: BLE001 - métrica nunca derruba o stream
            logger.warning("Progresso do micro-batch não pôde ser lido", erro=str(exc))
            return
        self._store.record_stream_batch(row)

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        pass
