"""CLI-facing ingestion orchestration."""

from __future__ import annotations

from pathlib import Path

import httpx

from pmbot.config import Settings
from pmbot.ingestion.clob import (
    DEFAULT_CLOB_HTTP_URL,
    DEFAULT_CLOB_WS_URL,
    ClobHttpClient,
    MarketDataIngestor,
)
from pmbot.ingestion.gamma import DEFAULT_GAMMA_URL, GammaClient
from pmbot.ingestion.recorder import ParquetRecorder


async def run_ingestion(*, markets: str, duration_seconds: int, settings: Settings) -> None:
    """Run read-only public market-data ingestion from CLI arguments."""
    data_dir = Path(settings.DATA_DIR)
    recorder = ParquetRecorder(
        data_dir=data_dir,
        buffer_size=settings.RAW_RECORDER_BUFFER_SIZE,
        flush_interval_seconds=settings.RECORDER_FLUSH_INTERVAL_SECONDS,
        max_recorded_depth=settings.MAX_RECORDED_DEPTH,
    )
    gamma_url = _url_or_default(settings.GAMMA_URL, DEFAULT_GAMMA_URL)
    clob_http_url = _url_or_default(settings.CLOB_HTTP_URL, DEFAULT_CLOB_HTTP_URL)
    clob_ws_url = _url_or_default(settings.CLOB_WS_URL, DEFAULT_CLOB_WS_URL)

    async with httpx.AsyncClient(timeout=20.0) as http:
        gamma = GammaClient(base_url=gamma_url, http=http)
        token_ids = await _resolve_markets(markets, gamma=gamma, recorder=recorder)
        if not token_ids:
            raise ValueError("no token IDs resolved for ingestion")
        ingestor = MarketDataIngestor(
            token_ids=token_ids,
            recorder=recorder,
            clob_http=ClobHttpClient(base_url=clob_http_url, http=http),
            ws_url=clob_ws_url,
        )
        await ingestor.run(duration_seconds=duration_seconds)
    recorder.flush_all()


async def _resolve_markets(
    markets: str,
    *,
    gamma: GammaClient,
    recorder: ParquetRecorder,
) -> tuple[str, ...]:
    if markets == "all-active":
        snapshots = await gamma.fetch_all_active_markets()
        recorder.record_many(snapshots)
        recorder.flush_all()
        return tuple(token_id for snapshot in snapshots for token_id in snapshot.token_ids)
    return tuple(token_id.strip() for token_id in markets.split(",") if token_id.strip())


def _url_or_default(value: object, default: str) -> str:
    if value is None:
        return default
    text = str(value)
    return text.rstrip("/") if text.startswith("http") else text
