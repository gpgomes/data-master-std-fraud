"""Coleta de dados de mercado via yfinance e upload para MinIO (camada Bronze)."""

from __future__ import annotations

import io
import uuid
from datetime import UTC, datetime, timedelta

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import settings
from src.common.logger import get_logger
from src.common.storage import MinIOClient, get_storage_client

logger = get_logger("market_data_collector")

# Sentinel gravado junto ao arquivo para rastreabilidade
_METADATA_COLS = {
    "ingestion_timestamp": None,
    "source_system": "yfinance",
    "batch_id": None,
}


class MarketDataCollector:
    """Coleta OHLCV histórico de ativos brasileiros via yfinance e persiste no Bronze.

    Fluxo:
        yfinance API → DataFrame → Parquet → MinIO bronze/market_data/date=YYYY-MM-DD/
    """

    def __init__(
        self,
        storage: MinIOClient | None = None,
        tickers: list[str] | None = None,
        bucket: str | None = None,
    ) -> None:
        self._storage = storage or get_storage_client()
        self._tickers = tickers or settings.market.ticker_list
        self._bucket = bucket or settings.minio.bucket_bronze
        self._batch_id = str(uuid.uuid4())

    # ── API pública ────────────────────────────────────────────────────────────

    def collect_daily(self, n_days: int = 30) -> dict[str, int]:
        """Coleta dados OHLCV dos últimos `n_days` dias e salva no Bronze.

        Args:
            n_days: Número de dias históricos a buscar.

        Returns:
            Dicionário {ticker: registros_salvos} para rastreabilidade.
        """
        end_date = datetime.now(tz=UTC).date()
        start_date = end_date - timedelta(days=n_days)

        logger.info(
            "Iniciando coleta de mercado",
            batch_id=self._batch_id,
            tickers=self._tickers,
            start=str(start_date),
            end=str(end_date),
        )

        results: dict[str, int] = {}
        for ticker in self._tickers:
            try:
                count = self._collect_ticker(ticker, str(start_date), str(end_date))
                results[ticker] = count
            except Exception:
                logger.exception("Erro ao coletar ticker", ticker=ticker, batch_id=self._batch_id)
                results[ticker] = 0

        total = sum(results.values())
        logger.info("Coleta finalizada", batch_id=self._batch_id, total_records=total, results=results)
        return results

    # ── Implementação interna ──────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_ohlcv(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """Busca dados via yfinance com retry automático."""
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            logger.warning("Sem dados retornados pelo yfinance", ticker=ticker, start=start, end=end)
            return pd.DataFrame()
        return df

    def _collect_ticker(self, ticker: str, start: str, end: str) -> int:
        """Coleta e persiste dados de um único ticker."""
        raw = self._fetch_ohlcv(ticker, start, end)
        if raw.empty:
            return 0

        df = self._normalize(raw, ticker)

        # Persiste por data para particionamento eficiente
        dates = df["date"].unique()
        for date_str in dates:
            partition_df = df[df["date"] == date_str].copy()
            key = f"market_data/date={date_str}/{ticker}.parquet"

            if self._storage.check_exists(self._bucket, key):
                logger.debug("Partição já existe, pulando", key=key)
                continue

            buf = io.BytesIO()
            partition_df.to_parquet(buf, index=False, engine="pyarrow")
            self._storage.upload_parquet_bytes(buf, self._bucket, key)

        logger.info("Ticker coletado", ticker=ticker, records=len(df), dates=len(dates))
        return len(df)

    @staticmethod
    def _normalize(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Normaliza o DataFrame bruto do yfinance para o schema Bronze."""
        # yfinance pode retornar MultiIndex quando baixa um único ticker
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        raw = raw.reset_index()

        col_map = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
        raw = raw.rename(columns={k: v for k, v in col_map.items() if k in raw.columns})

        df = pd.DataFrame(index=raw.index)
        df["symbol"] = ticker
        df["date"] = raw["date"].astype(str).str[:10]
        df["open"] = raw["open"].astype(float)
        df["high"] = raw["high"].astype(float)
        df["low"] = raw["low"].astype(float)
        df["close"] = raw["close"].astype(float)
        df["volume"] = raw["volume"].astype("int64")
        df["adjusted_close"] = raw.get("close", raw["close"]).astype(float)
        df = df.reset_index(drop=True)

        # Metadados de ingestão
        now = datetime.now(tz=UTC).isoformat()
        df["ingestion_timestamp"] = now
        df["source_system"] = "yfinance"

        return df
