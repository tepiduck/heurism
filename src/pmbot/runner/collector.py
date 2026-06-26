"""CLI-facing unattended read-only data collector."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
from websockets.asyncio.client import connect

from pmbot.config import Settings
from pmbot.ingestion.clob import (
    DEFAULT_CLOB_HTTP_URL,
    DEFAULT_CLOB_WS_URL,
    BackoffPolicy,
    ClobHttpClient,
    MarketDataIngestor,
    cast_ws_message,
)
from pmbot.ingestion.gamma import DEFAULT_GAMMA_URL, GammaClient
from pmbot.ingestion.kalshi import (
    KALSHI_REST_URL,
    KALSHI_WS_URL,
    KalshiMetadataClient,
    KalshiRecorderAdapter,
    kalshi_ssl_context,
    kalshi_subscribe_message,
)
from pmbot.runner.allowlist import load_verified_pairs
from pmbot.runner.orchestrator import CollectorOrchestrator
from pmbot.runner.resolutions import GammaResolutionSource, KalshiResolutionSource, ResolutionPoller
from pmbot.runner.signal_logging import SignalEventObserver, SignalLoggingRecorder
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import ResolutionParquetStore, SignalParquetWriter
from pmbot.signals.contracts import VerifiedContractPair


async def run_collector(
    *,
    allowlist_path: Path,
    settings: Settings,
    duration_seconds: float | None = None,
    resolution_interval_seconds: float = 900.0,
) -> None:
    """Run the unattended read-only collector."""
    pairs = load_verified_pairs(allowlist_path)
    if not pairs:
        raise ValueError("allowlist contains no verified pairs")

    data_dir = Path(settings.DATA_DIR)
    state = CollectorStateStore(data_dir=data_dir, pairs_tracked=len(pairs))
    signal_writer = SignalParquetWriter(
        data_dir=data_dir,
        buffer_size=100,
        flush_interval_seconds=30.0,
    )
    observer = SignalEventObserver(pairs=pairs, signal_writer=signal_writer, state=state)
    recorder = SignalLoggingRecorder(
        data_dir=data_dir,
        observer=observer,
        buffer_size=100,
        flush_interval_seconds=30.0,
    )
    resolution_store = ResolutionParquetStore(data_dir=data_dir)

    async with httpx.AsyncClient(timeout=20.0) as http:
        gamma = GammaClient(
            base_url=_url_or_default(settings.GAMMA_URL, DEFAULT_GAMMA_URL),
            http=http,
        )
        kalshi_metadata = KalshiMetadataClient(base_url=KALSHI_REST_URL, http=http)
        poller = ResolutionPoller(
            pairs=pairs,
            sources=(
                GammaResolutionSource(gamma),
                KalshiResolutionSource(kalshi_metadata),
            ),
            store=resolution_store,
            state=state,
        )
        orchestrator = CollectorOrchestrator(
            venue_tasks=(
                PolymarketVenueTask(
                    pairs=pairs,
                    recorder=recorder,
                    gamma=gamma,
                    clob_http=ClobHttpClient(
                        base_url=_url_or_default(settings.CLOB_HTTP_URL, DEFAULT_CLOB_HTTP_URL),
                        http=http,
                    ),
                    ws_url=_url_or_default(settings.CLOB_WS_URL, DEFAULT_CLOB_WS_URL),
                ),
                KalshiVenueTask(
                    pairs=pairs,
                    recorder=recorder,
                    metadata=kalshi_metadata,
                    ws_url=KALSHI_WS_URL,
                ),
            ),
            resolution_poller=poller,
            state=state,
            backoff=BackoffPolicy(),
            resolution_interval_seconds=resolution_interval_seconds,
        )
        try:
            await orchestrator.run(duration_seconds=duration_seconds)
        finally:
            recorder.flush_all()


class PolymarketVenueTask:
    """Read-only Polymarket Gamma + CLOB ingestion task."""

    name = "polymarket"

    def __init__(
        self,
        *,
        pairs: tuple[VerifiedContractPair, ...],
        recorder: SignalLoggingRecorder,
        gamma: GammaClient,
        clob_http: ClobHttpClient,
        ws_url: str,
        run_slice_seconds: int = 3_600,
    ) -> None:
        self._pairs = pairs
        self._recorder = recorder
        self._gamma = gamma
        self._clob_http = clob_http
        self._ws_url = ws_url
        self._run_slice_seconds = run_slice_seconds

    async def run(self) -> None:
        """Fetch metadata and run a bounded CLOB ingestion slice."""
        snapshots = await self._gamma.fetch_all_active_markets()
        self._recorder.record_many(snapshots)
        self._recorder.flush_all()
        ingestor = MarketDataIngestor(
            token_ids=(pair.polymarket_asset_id for pair in self._pairs),
            recorder=self._recorder,
            clob_http=self._clob_http,
            ws_url=self._ws_url,
        )
        await ingestor.run(duration_seconds=self._run_slice_seconds)


class KalshiVenueTask:
    """Read-only Kalshi market metadata + websocket ingestion task."""

    name = "kalshi"

    def __init__(
        self,
        *,
        pairs: tuple[VerifiedContractPair, ...],
        recorder: SignalLoggingRecorder,
        metadata: KalshiMetadataClient,
        ws_url: str,
    ) -> None:
        self._pairs = pairs
        self._adapter = KalshiRecorderAdapter(recorder)
        self._metadata = metadata
        self._ws_url = ws_url

    async def run(self) -> None:
        """Fetch metadata and subscribe to Kalshi orderbook/trade channels."""
        for pair in self._pairs:
            self._adapter.record_market_metadata(
                json.loads((await self._metadata.fetch_market(pair.kalshi_ticker)).raw_json)
            )
        self._adapter.flush_all()
        async with connect(
            self._ws_url,
            ping_interval=20.0,
            ssl=kalshi_ssl_context(),
        ) as websocket:
            await websocket.send(
                json.dumps(
                    kalshi_subscribe_message(
                        channels=("orderbook_delta", "trade"),
                        market_tickers=tuple(pair.kalshi_ticker for pair in self._pairs),
                    )
                )
            )
            async for message in websocket:
                self._adapter.record_frame(cast_ws_message(message))


def _url_or_default(value: object, default: str) -> str:
    if value is None:
        return default
    text = str(value)
    return text.rstrip("/") if text.startswith(("http", "ws")) else text
