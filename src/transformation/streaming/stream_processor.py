"""Job PySpark Structured Streaming: speed layer da arquitetura Lambda (issues #11 e #46).

Consome `raw-transactions` do Kafka, enriquece com atributos de `dim_customers` (Gold) e detecta
fraude com o **Fraud Engine multi-signal** (`src/transformation/fraud/`): 10 sinais (perfil de
comportamento do cliente, calculado no batch e lido por broadcast, mais janelas curtas de
velocidade, viagem impossível e concentração de destinatários) combinados por noisy-OR. O tipo de
fraude do alerta é **inferido pelos sinais**, nunca copiado do rótulo, e o motivo do alerta é a lista
de sinais ativos. Publica o resultado em `enriched-transactions`, os alertas em `fraud-alerts`, e
persiste em `s3a://silver/transactions_stream/` (caminho separado de `silver/transactions/`,
escrito pelo batch — evita conflito entre os overwrites por partição do batch e os appends
contínuos do streaming).

**Shadow scoring:** o Z-Score do V1 (`z_score`, `is_anomaly`, `fraud_score_v1`) continua calculado
nas mesmas linhas, com `shadow_detector_version = "zscore-v1"`. Só o V2 alerta. Isso permite comparar
os dois detectores no mesmo tráfego.

**Sem rótulo no detector (separação estrutural):** o payload do Kafka continua trazendo `is_fraud` e
`fraud_type` (o rótulo do gerador sintético), mas o `_process_batch` os separa do DataFrame antes
do scoring e os devolve por `transaction_id` no fim. Nenhum detector recebe uma coluna de rótulo.

**Estado curto** (`_load_history`/`_persist_history`): os últimos `STATE_HORIZON_SECONDS` (6 h) de
eventos, com as colunas que os sinais de janela precisam. Vai para um caminho novo
(`_stream_state/recent_events/`) porque o Parquet antigo, de 3 colunas, é incompatível.

**Python 3.8:** este módulo e os do Fraud Engine rodam no container do Spark (Python 3.8, sem
numpy); `tests/unit/test_spark_container_compat.py` vigia isso.

Execução via CLI:
    python -m src.transformation.streaming.stream_processor
    python -m src.transformation.streaming.stream_processor --local
    python -m src.transformation.streaming.stream_processor --starting-offsets earliest
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections.abc import Mapping

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark.sql.utils import AnalysisException

from src.common.config import settings
from src.common.logger import get_logger
from src.common.schemas import TRANSACTION_SPARK_SCHEMA
from src.common.spark_session import create_spark_session
from src.transformation.fraud.detector import detect
from src.transformation.fraud.profile import PROFILE_SCHEMA
from src.transformation.fraud.signals import EVENT_COLUMNS

logger = get_logger("stream_processor")

# ── Constantes de detecção ──────────────────────────────────────────────────────

Z_SCORE_WINDOW_SECONDS = 3600  # histórico considerado: 1h anterior à transação
Z_SCORE_MIN_TRANSACTIONS = 2  # mínimo de transações no histórico p/ calcular score
Z_SCORE_THRESHOLD = 3.0  # |z_score| > limiar => anomalia
Z_SCORE_SCALE = 6.0  # mapeia |z_score| para [0,1]: |z|=6 => fraud_score=1.0
WATERMARK_DELAY = "1 hour"
# Versão do detector antigo (Z-Score), que segue em paralelo como shadow (issue #46). O detector
# que alerta é o `multisignal-v2` (`src.transformation.fraud.detector.DETECTOR_VERSION`).
DETECTOR_VERSION = "zscore-v1"
# Horizonte do estado curto: cobre a maior janela dos sinais (6 h da viagem impossível). O V1 usa 1 h.
STATE_HORIZON_SECONDS = 6 * 3600
# Rótulo do gerador sintético: viaja no payload, mas nunca entra no caminho de scoring.
LABEL_COLUMNS = ("is_fraud", "fraud_type")

# Etapas de um micro-batch, na ordem em que rodam. Cada uma grava um marcador ao concluir
# (ver `_run_stage`), para que o replay de um batch interrompido pule o que já foi feito.
_STAGES = ("parquet", "enriched", "alerts", "history")
PROGRESS_RETENTION_BATCHES = 100  # marcadores de batches mais antigos que isto são apagados

# Schema do estado curto persistido entre micro-batches: as colunas que o detector enxerga
# (`EVENT_COLUMNS`), dos últimos `STATE_HORIZON_SECONDS`. O Z-Score do V1 só usa customer_id,
# timestamp e amount; os sinais de janela do V2 usam o resto.
_STATE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("device_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("destination_account", StringType(), True),
    ]
)


def deterministic_alert_id(transaction_id: Column) -> Column:
    """UUID derivado só do `transaction_id`: o mesmo alerta reprocessado mantém o
    `alert_id` (com `uuid()` aleatório, um replay gerava um alerta "novo")."""
    digest = F.md5(F.concat(F.lit("fraud-alert:"), transaction_id))
    return F.concat_ws(
        "-",
        digest.substr(1, 8),
        digest.substr(9, 4),
        digest.substr(13, 4),
        digest.substr(17, 4),
        digest.substr(21, 12),
    )


def build_fraud_alerts(scored_df: DataFrame) -> DataFrame:
    """Constrói o payload de FraudAlert a partir das linhas alertadas de um DataFrame já
    pontuado. Função pura: usada pelo streaming (`fraud-alerts`) e pelo loader da serving
    layer (`fraud_alerts` no Postgres), que reconstrói os alertas a partir do Parquet.

    Alerta é o que o Fraud Engine (V2) marcou (`is_fraud_predicted`). O `fraud_type` é o **inferido
    pelos sinais** (`fraud_type_predicted`, nulo se nenhuma regra casou): nunca o rótulo do
    evento, e não há mais tipo de fallback. O motivo do alerta é a lista dos sinais ativos.
    O `z_score` é o do detector antigo (shadow) e pode ser nulo.

    `processed_at` é o `processing_timestamp` da própria linha (quando existe), então o
    valor é o mesmo no Kafka e no Postgres e não muda com o momento da carga."""
    alerted = scored_df.filter(F.col("is_fraud_predicted"))
    processed_at = (
        F.col("processing_timestamp")
        if "processing_timestamp" in scored_df.columns
        else F.current_timestamp()
    )
    signals = F.when(
        F.size("fraud_signals") > 0, F.array_join("fraud_signals", ", ")
    ).otherwise(F.lit("nenhum acima de 0,5 (combinação de sinais fracos)"))
    reason = F.concat(
        F.lit("Sinais: "),
        signals,
        F.lit(" | score "),
        F.round("fraud_score", 3).cast("string"),
        F.lit(" | "),
        F.col("detector_version"),
    )
    return alerted.select(
        deterministic_alert_id(F.col("transaction_id")).alias("alert_id"),
        F.col("transaction_id"),
        F.col("customer_id"),
        F.col("timestamp"),
        F.col("amount"),
        F.col("fraud_type_predicted").alias("fraud_type"),
        F.col("fraud_score"),
        F.col("z_score"),
        reason.alias("alert_reason"),
        F.col("fraud_signals").alias("signals"),
        F.col("detector_version"),
        processed_at.alias("processed_at"),
    )


class StreamProcessor:
    """Enriquece `raw-transactions` e detecta fraude com o Fraud Engine multi-signal.

    Args:
        spark: SparkSession ativa (injetada externamente para facilitar testes).
        dim_customers: DataFrame estático de `gold/dim_customers` para o join de
            enriquecimento (injetado nos testes; carregado lazy em `run()`).
        profile: DataFrame estático de `gold/customer_behavior_profile` (o perfil de
            comportamento do batch), lido por broadcast (injetado nos testes; carregado em `run()`).
    """

    def __init__(
        self,
        spark: SparkSession,
        dim_customers: DataFrame | None = None,
        profile: DataFrame | None = None,
        weights: Mapping[str, float] | None = None,
        threshold: float | None = None,
    ) -> None:
        self.spark = spark
        self._silver = f"s3a://{settings.minio.bucket_silver}"
        self._gold = f"s3a://{settings.minio.bucket_gold}"
        self._checkpoints = f"s3a://{settings.minio.bucket_checkpoints}"
        self._history_path = f"{self._silver}/_stream_state/recent_events/"
        # Marcadores de progresso ficam no bucket de checkpoints de propósito: apagar o
        # checkpoint (que zera os batch_id) apaga os marcadores junto.
        self._progress_path = f"{self._checkpoints}/stream_processor_progress"
        self._query_id: str | None = None
        self._dim_customers = dim_customers
        # Perfil de comportamento dos clientes (Gold `customer_behavior_profile`, do batch), lido
        # por broadcast como o `dim_customers`. `run()` o carrega; sem ele, o V2 fica cego para
        # tudo que depende de "conhecido" (device, rede, destinatário, valor típico).
        self._profile = profile
        # Pesos e limiar do Fraud Engine: por padrão os versionados em `weights.py` (calibrados).
        self._weights = weights
        self._threshold = threshold

    # ── Leitura e parsing ────────────────────────────────────────────────────────

    def _parse_raw_kafka_batch(self, raw: DataFrame) -> DataFrame:
        """Desserializa `value` (JSON) contra o schema de TransactionEvent.

        Mensagens com JSON malformado ou sem `transaction_id` (falha de parse)
        são descartadas — uma falha de dado isolada não derruba o micro-batch.
        """
        parsed = raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), TRANSACTION_SPARK_SCHEMA).alias("data"),
        ).select("kafka_key", "data.*")
        return parsed.filter(F.col("transaction_id").isNotNull())

    def read_transactions_stream(self, starting_offsets: str = "latest") -> DataFrame:
        """Lê `raw-transactions` como stream.

        `starting_offsets` só é respeitado numa query nova (sem checkpoint
        existente) — reinício com checkpoint válido sempre retoma do offset
        salvo, independente desta opção. Para reprocessar do início, apague o
        checkpoint (`s3a://checkpoints/stream_processor/`) e rode com
        `--starting-offsets earliest`.
        """
        raw = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.internal_servers)
            .option("subscribe", settings.kafka.topic_transactions)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )
        parsed = self._parse_raw_kafka_batch(raw)
        return parsed.withWatermark("timestamp", WATERMARK_DELAY)

    # ── Estado curto entre micro-batches (eventos das últimas 6 h) ──────────────

    def _load_history(self) -> DataFrame:
        """Carrega o estado curto persistido pelo micro-batch anterior. Ausência do path
        (primeira execução, ou pós replay do zero) é normal — retorna estado vazio."""
        try:
            return self.spark.read.parquet(self._history_path)
        except AnalysisException:
            logger.info("Nenhum estado anterior encontrado — estado vazio", path=self._history_path)
            return self._empty_history_df()

    def _empty_df(self, schema: StructType) -> DataFrame:
        # Via arquivo (não `spark.createDataFrame([], schema)`) — o caminho de
        # RDD local aciona uma serialização por cloudpickle da função de
        # conversão de linha que sofre de recursão infinita em Python 3.14+.
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            tmp_path = f.name
        try:
            df = self.spark.read.schema(schema).json(tmp_path).cache()
            df.count()  # força materialização antes de apagar o arquivo (leitura é lazy)
            return df
        finally:
            os.unlink(tmp_path)

    def _empty_history_df(self) -> DataFrame:
        return self._empty_df(_STATE_SCHEMA)

    def _compute_pruned_history(self, current_batch: DataFrame, history: DataFrame) -> DataFrame:
        """Funde o estado anterior com o micro-batch atual, podando tudo mais antigo que
        `STATE_HORIZON_SECONDS` em relação ao evento mais recente já visto (tempo de evento, não
        wall-clock — segue correto tanto em tempo real quanto em replay de dados antigos).
        Guarda só as colunas que o detector enxerga (`EVENT_COLUMNS`): o rótulo nunca entra no
        estado. Função pura: o resultado é o que `_persist_history` grava para o próximo
        micro-batch."""
        combined = history.select(*EVENT_COLUMNS).unionByName(current_batch.select(*EVENT_COLUMNS))
        max_row = combined.agg(F.max("timestamp")).first()
        max_ts = max_row[0] if max_row is not None else None
        if max_ts is None:
            return combined
        cutoff = max_ts - F.expr(f"INTERVAL {STATE_HORIZON_SECONDS} SECONDS")
        return combined.filter(F.col("timestamp") >= cutoff)

    def _persist_history(self, current_batch: DataFrame, history: DataFrame) -> None:
        pruned = self._compute_pruned_history(current_batch, history)
        pruned.write.format("parquet").mode("overwrite").save(self._history_path)

    # ── Enriquecimento e Z-Score ─────────────────────────────────────────────────

    def _enrich_and_score(self, current_batch: DataFrame, history: DataFrame) -> DataFrame:
        """Calcula o Z-Score do `amount` por cliente numa janela deslizante.

        `history` fornece o baseline acumulado de micro-batches anteriores
        (persistido via `_persist_history`) — sem isso, cada micro-batch veria
        só suas próprias poucas linhas, quase nunca o suficiente por cliente
        para formar uma baseline. A janela cobre os `Z_SCORE_WINDOW_SECONDS`
        segundos ANTERIORES a cada transação (exclui a própria linha — evita
        viés de auto-comparação). Retorna só as linhas de `current_batch`,
        enriquecidas — `history` entra apenas como contexto para o cálculo.

        Função pura sobre DataFrames estáticos: testável diretamente, e é
        exatamente o que roda dentro de `foreachBatch` em produção.
        """
        current = current_batch.withColumn("_source", F.lit("current"))
        hist = history.select("customer_id", "timestamp", "amount").withColumn(
            "_source", F.lit("history")
        )
        combined = current.unionByName(hist, allowMissingColumns=True)

        ts_seconds = F.col("timestamp").cast("long")
        window_spec = (
            Window.partitionBy("customer_id")
            .orderBy(ts_seconds)
            .rangeBetween(-Z_SCORE_WINDOW_SECONDS, -1)
        )

        combined = combined.withColumn("_hist_count", F.count("amount").over(window_spec))
        combined = combined.withColumn("_hist_avg", F.avg("amount").over(window_spec))
        combined = combined.withColumn("_hist_stddev", F.stddev_samp("amount").over(window_spec))

        has_baseline = (F.col("_hist_count") >= Z_SCORE_MIN_TRANSACTIONS) & (
            F.col("_hist_stddev") > 0
        )
        combined = combined.withColumn(
            "z_score",
            F.when(has_baseline, (F.col("amount") - F.col("_hist_avg")) / F.col("_hist_stddev")),
        )
        combined = combined.withColumn(
            "fraud_score",
            F.when(
                F.col("z_score").isNotNull(),
                F.least(F.abs(F.col("z_score")) / F.lit(Z_SCORE_SCALE), F.lit(1.0)),
            ),
        )
        combined = combined.withColumn(
            "is_anomaly",
            F.coalesce(F.abs(F.col("z_score")) > F.lit(Z_SCORE_THRESHOLD), F.lit(False)),
        )

        df = combined.filter(F.col("_source") == "current").drop(
            "_source", "_hist_count", "_hist_avg", "_hist_stddev"
        )

        if self._dim_customers is not None:
            enrichment = self._dim_customers.select(
                F.col("customer_key").alias("customer_id"),
                F.col("segment").alias("customer_segment"),
                F.col("risk_score").alias("customer_risk_score"),
                F.col("city").alias("customer_city"),
            )
            df = df.join(F.broadcast(enrichment), on="customer_id", how="left")

        return df.withColumn("processing_timestamp", F.current_timestamp())

    def _empty_profile(self) -> DataFrame:
        return self._empty_df(PROFILE_SCHEMA)

    def _score_batch(self, batch_df: DataFrame, history: DataFrame) -> DataFrame:
        """Pontua o micro-batch: o Fraud Engine (V2) decide; o Z-Score (V1) segue em paralelo.

        **Separação estrutural do rótulo:** `is_fraud`/`fraud_type` (o rótulo do gerador sintético)
        saem do DataFrame antes de qualquer detector e só voltam, por `transaction_id`, no fim. Os
        dois detectores trabalham sobre `features`, que não tem coluna de rótulo nenhuma.

        `batch_df` deve ter `transaction_id` único (o `_process_batch` deduplica), para os joins
        de volta serem 1:1.
        """
        labels_present = [c for c in LABEL_COLUMNS if c in batch_df.columns]
        labels = batch_df.select("transaction_id", *labels_present)
        # `fraud_score` do payload é sempre nulo (campo derivado); cada detector calcula o seu.
        features = batch_df.drop(*labels_present, "fraud_score")

        shadow = self._enrich_and_score(
            features, history.select("customer_id", "timestamp", "amount")
        ).withColumnRenamed("fraud_score", "fraud_score_v1")

        profile = self._profile if self._profile is not None else self._empty_profile()
        engine = detect(
            features, profile, recent=history, weights=self._weights, threshold=self._threshold
        ).select(
            "transaction_id",
            "fraud_score",
            "is_fraud_predicted",
            "fraud_signals",
            "fraud_type_predicted",
            "detector_version",
        )
        return (
            shadow.join(F.broadcast(engine), "transaction_id", "left")
            .join(F.broadcast(labels), "transaction_id", "left")
            .withColumn("shadow_detector_version", F.lit(DETECTOR_VERSION))
        )

    def _build_fraud_alerts(self, scored_df: DataFrame) -> DataFrame:
        return build_fraud_alerts(scored_df)

    # ── Escrita ──────────────────────────────────────────────────────────────────

    def _write_to_kafka(self, df: DataFrame, topic: str, key_col: str) -> None:
        (
            df.select(
                F.col(key_col).cast("string").alias("key"),
                F.to_json(F.struct(*df.columns)).alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka.internal_servers)
            .option("topic", topic)
            .save()
        )

    # ── Idempotência do micro-batch (issue #36) ──────────────────────────────────
    #
    # O Spark reexecuta o mesmo `batch_id` (com os mesmos dados) se o job cair antes de
    # gravar o commit do checkpoint. O foreachBatch escreve em vários destinos sem
    # transação, então cada etapa é idempotente (Parquet, sobrescrevendo a própria
    # partição) ou pulada no replay (marcador de etapa concluída).
    # Janela residual: cair entre o fim de uma etapa Kafka e a escrita do marcador dela
    # ainda pode duplicar aquela mensagem — o Kafka sink do Spark não é transacional;
    # consumidores devem deduplicar por `transaction_id` (e `alert_id`, agora estável).

    def _hadoop_path(self, path: str):
        hadoop_path = self.spark._jvm.org.apache.hadoop.fs.Path(path)  # type: ignore[union-attr]
        return hadoop_path, hadoop_path.getFileSystem(self.spark._jsc.hadoopConfiguration())

    def _marker_path(self, batch_id: int, stage: str) -> str:
        return f"{self._progress_path}/batch_{batch_id}.{stage}"

    def _stage_done(self, batch_id: int, stage: str) -> bool:
        path, fs = self._hadoop_path(self._marker_path(batch_id, stage))
        return bool(fs.exists(path))

    def _mark_stage_done(self, batch_id: int, stage: str) -> None:
        path, fs = self._hadoop_path(self._marker_path(batch_id, stage))
        fs.create(path, True).close()

    def _prune_markers(self, batch_id: int) -> None:
        old_batch = batch_id - PROGRESS_RETENTION_BATCHES
        if old_batch < 0:
            return
        for stage in _STAGES:
            path, fs = self._hadoop_path(self._marker_path(old_batch, stage))
            fs.delete(path, False)

    def _run_stage(self, batch_id: int, stage: str, action) -> None:
        if self._stage_done(batch_id, stage):
            logger.info(
                "Etapa do micro-batch já concluída — ignorada no replay",
                batch_id=batch_id,
                stage=stage,
            )
            return
        action()
        self._mark_stage_done(batch_id, stage)

    def _resolve_query_id(self) -> str:
        """Id da query, lido do `metadata` do checkpoint. Muda quando o checkpoint é
        recriado (o `batch_id` volta a 0), então separa a saída de cada "vida" do stream."""
        if self._query_id is None:
            path = f"{self._checkpoints}/stream_processor/metadata"
            try:
                row = self.spark.read.text(path).first()
                if row is None:
                    raise ValueError("metadata vazio")
                self._query_id = json.loads(row["value"])["id"]
            except (AnalysisException, ValueError, KeyError, TypeError):
                logger.warning("Checkpoint sem metadata legível — usando query_id 'unknown'", path=path)
                self._query_id = "unknown"
        return self._query_id

    def _write_silver_stream(self, scored: DataFrame, batch_id: int) -> None:
        """Grava em `query_id=<id>/batch_id=<n>` com overwrite dinâmico: reexecutar o
        mesmo batch substitui a própria partição em vez de anexar linhas duplicadas."""
        (
            scored.withColumn("query_id", F.lit(self._resolve_query_id()))
            .withColumn("batch_id", F.lit(batch_id))
            .write.format("parquet")
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("query_id", "batch_id")
            .save(f"{self._silver}/transactions_stream/")
        )

    def _process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        self._prune_markers(batch_id)
        if batch_df.isEmpty():
            return
        if self._stage_done(batch_id, _STAGES[-1]):
            logger.info("Micro-batch já concluído (replay após restart) — ignorado", batch_id=batch_id)
            return

        # `transaction_id` único no micro-batch: o at-least-once do Kafka pode repetir um evento, e
        # os joins de volta do scoring (V1, V2 e rótulo) precisam ser 1:1.
        batch_df = batch_df.dropDuplicates(["transaction_id"]).cache()
        cached = [batch_df]
        try:
            history = self._load_history().cache()
            cached.append(history)

            scored = self._score_batch(batch_df, history).cache()
            cached.append(scored)
            rows = scored.count()
            alerts = self._build_fraud_alerts(scored).cache()
            cached.append(alerts)
            alert_count = alerts.count()

            def write_alerts() -> None:
                if alert_count:
                    self._write_to_kafka(
                        alerts, settings.kafka.topic_fraud_alerts, key_col="customer_id"
                    )

            self._run_stage(batch_id, "parquet", lambda: self._write_silver_stream(scored, batch_id))
            self._run_stage(
                batch_id,
                "enriched",
                lambda: self._write_to_kafka(
                    scored, settings.kafka.topic_enriched, key_col="customer_id"
                ),
            )
            self._run_stage(batch_id, "alerts", write_alerts)
            self._run_stage(batch_id, "history", lambda: self._persist_history(batch_df, history))

            logger.info(
                "Micro-batch processado", batch_id=batch_id, rows=rows, alerts=alert_count
            )
        except Exception:
            logger.exception("Falha ao processar micro-batch", batch_id=batch_id)
            raise
        finally:
            # O estado curto é 6 h de eventos: sem liberar o cache a cada trigger a memória cresceria.
            for df in cached:
                df.unpersist()

    # ── Execução ─────────────────────────────────────────────────────────────────

    def _load_static_inputs(self) -> None:
        """Carrega as duas tabelas Gold que o detector lê por broadcast, se não foram injetadas."""
        if self._dim_customers is None:
            self._dim_customers = self.spark.read.parquet(f"{self._gold}/dim_customers/")
        if self._profile is None:
            path = f"{self._gold}/customer_behavior_profile/"
            try:
                self._profile = self.spark.read.parquet(path)
            except AnalysisException as exc:
                raise RuntimeError(
                    f"Perfil de comportamento não encontrado em {path}. Rode o batch antes do "
                    "streaming (`make spark-submit-silver-gold`): sem ele o detector não conhece "
                    "device, rede, destinatário nem valor típico de nenhum cliente."
                ) from exc

    def run(self, starting_offsets: str = "latest", trigger_seconds: int = 10):
        """Inicia a query de streaming. Bloqueia até a query terminar."""
        self._load_static_inputs()

        stream = self.read_transactions_stream(starting_offsets=starting_offsets)
        query = (
            stream.writeStream.foreachBatch(self._process_batch)
            .option("checkpointLocation", f"{self._checkpoints}/stream_processor/")
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )
        logger.info(
            "StreamProcessor iniciado",
            topic=settings.kafka.topic_transactions,
            starting_offsets=starting_offsets,
            trigger_seconds=trigger_seconds,
        )
        query.awaitTermination()
        return query


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Speed layer: enriquecimento + detecção de fraude via Z-Score (streaming)."
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="latest",
        help="Offset inicial (só respeitado sem checkpoint existente — modo de replay).",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=10,
        help="Intervalo do trigger de micro-batch, em segundos (padrão: 10).",
    )
    parser.add_argument(
        "--local", action="store_true", default=False, help="Usa SparkSession em modo local (dev)."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    spark = create_spark_session(app_name="stream_processor", local_mode=args.local)
    processor = StreamProcessor(spark=spark)
    processor.run(starting_offsets=args.starting_offsets, trigger_seconds=args.trigger_seconds)


if __name__ == "__main__":
    main(sys.argv[1:])
