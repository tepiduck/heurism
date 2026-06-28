"""CLI-facing unattended read-only data collector."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

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
    KalshiAuth,
    KalshiMetadataClient,
    KalshiRecorderAdapter,
    kalshi_ssl_context,
    kalshi_subscribe_message,
    kalshi_websocket_headers,
)
from pmbot.ingestion.schemas import MetadataSnapshot
from pmbot.runner.allowlist import load_verified_pairs
from pmbot.runner.compaction import RawDataCompactionTask
from pmbot.runner.models import ResolutionRecord
from pmbot.runner.orchestrator import CollectorOrchestrator
from pmbot.runner.pairs import CollectedPairStore, PairRegistry
from pmbot.runner.resolutions import (
    GammaResolutionSource,
    KalshiResolutionSource,
    ResolutionPoller,
    ResolutionSource,
)
from pmbot.runner.signal_logging import (
    SignalComputationTask,
    SignalEventObserver,
    SignalLoggingRecorder,
)
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import ResolutionParquetStore, SignalParquetWriter
from pmbot.runner.world_cup_discovery import (
    WorldCupAllowlistRefresher,
    WorldCupMatchWinnerDiscovery,
)
from pmbot.signals.contracts import VerifiedContractPair

LOGGER = logging.getLogger(__name__)


async def run_collector(
    *,
    allowlist_path: Path,
    settings: Settings,
    duration_seconds: float | None = None,
    resolution_interval_seconds: float = 900.0,
    discovery_interval_seconds: float = 1800.0,
) -> None:
    """Run the unattended read-only collector."""
    pairs = load_verified_pairs(allowlist_path)

    data_dir = Path(settings.DATA_DIR)
    state = CollectorStateStore(data_dir=data_dir, pairs_tracked=len(pairs))
    pair_store = CollectedPairStore(data_dir=data_dir)
    pair_store.save(pairs, merge_existing=True)
    resolution_store = ResolutionParquetStore(data_dir=data_dir)

    async with httpx.AsyncClient(timeout=20.0) as http:
        gamma = GammaClient(
            base_url=_url_or_default(settings.GAMMA_URL, DEFAULT_GAMMA_URL),
            http=http,
        )
        kalshi_auth = _kalshi_auth_from_settings(settings)
        kalshi_metadata = KalshiMetadataClient(
            base_url=KALSHI_REST_URL,
            http=http,
            auth=kalshi_auth,
        )
        resolution_sources: tuple[ResolutionSource, ...] = (
            GammaResolutionSource(gamma),
            KalshiResolutionSource(kalshi_metadata),
        )
        live_pairs = await _record_startup_resolutions(
            pairs=pairs,
            sources=resolution_sources,
            store=resolution_store,
            state=state,
        )
        registry = PairRegistry(live_pairs)
        pair_store.save(registry.snapshot(), merge_existing=True)
        state.set_pairs_tracked(len(registry.snapshot()))

        signal_writer = SignalParquetWriter(
            data_dir=data_dir,
            buffer_size=settings.SIGNAL_RECORDER_BUFFER_SIZE,
            flush_interval_seconds=settings.RECORDER_FLUSH_INTERVAL_SECONDS,
        )
        observer = SignalEventObserver(
            pairs=live_pairs,
            signal_writer=signal_writer,
            state=state,
            signal_min_interval_ns=settings.SIGNAL_LOG_MIN_INTERVAL_MS * 1_000_000,
        )
        recorder = SignalLoggingRecorder(
            data_dir=data_dir,
            observer=observer,
            buffer_size=settings.RAW_RECORDER_BUFFER_SIZE,
            flush_interval_seconds=settings.RECORDER_FLUSH_INTERVAL_SECONDS,
            max_recorded_depth=settings.MAX_RECORDED_DEPTH,
        )
        discoverer = WorldCupMatchWinnerDiscovery(gamma=gamma, kalshi=kalshi_metadata)
        refresher = WorldCupAllowlistRefresher(
            registry=registry,
            discoverer=discoverer,
            resolution_sources=resolution_sources,
            resolution_store=resolution_store,
            state=state,
            observer=observer,
            pair_store=pair_store,
        )
        try:
            await refresher.refresh_once()
        except Exception:
            LOGGER.warning(
                "startup World Cup discovery failed; continuing with loaded allowlist",
                exc_info=True,
            )
        def drop_resolved_from_live_subscriptions(records: tuple[ResolutionRecord, ...]) -> None:
            _drop_resolved_live_pairs(
                records=records,
                registry=registry,
                observer=observer,
                state=state,
            )

        poller = ResolutionPoller(
            pairs=registry.snapshot,
            sources=resolution_sources,
            store=resolution_store,
            state=state,
            on_resolved=drop_resolved_from_live_subscriptions,
        )
        venue_tasks = (
            PolymarketVenueTask(
                registry=registry,
                recorder=recorder,
                gamma=gamma,
                clob_http=ClobHttpClient(
                    base_url=_url_or_default(
                        settings.CLOB_HTTP_URL,
                        DEFAULT_CLOB_HTTP_URL,
                    ),
                    http=http,
                ),
                ws_url=_url_or_default(settings.CLOB_WS_URL, DEFAULT_CLOB_WS_URL),
            ),
            KalshiVenueTask(
                registry=registry,
                recorder=recorder,
                metadata=kalshi_metadata,
                ws_url=KALSHI_WS_URL,
                auth=kalshi_auth,
            ),
            WorldCupDiscoveryRefreshTask(
                refresher=refresher,
                interval_seconds=discovery_interval_seconds,
            ),
            SignalComputationTask(
                observer=observer,
                interval_seconds=settings.SIGNAL_COMPUTE_INTERVAL_SECONDS,
            ),
            RawDataCompactionTask(
                data_dir=data_dir,
                interval_seconds=settings.COMPACTION_INTERVAL_SECONDS,
            ),
        )
        orchestrator = CollectorOrchestrator(
            venue_tasks=venue_tasks,
            resolution_poller=poller,
            state=state,
            backoff=BackoffPolicy(),
            resolution_interval_seconds=resolution_interval_seconds,
        )
        try:
            await orchestrator.run(duration_seconds=duration_seconds)
        finally:
            recorder.flush_all()


async def _record_startup_resolutions(
    *,
    pairs: tuple[VerifiedContractPair, ...],
    sources: tuple[ResolutionSource, ...],
    store: ResolutionParquetStore,
    state: CollectorStateStore | None = None,
) -> tuple[VerifiedContractPair, ...]:
    records: list[ResolutionRecord] = []
    for source in sources:
        try:
            records.extend(await source.fetch(pairs))
        except Exception:
            LOGGER.warning(
                "startup resolution source %s failed; continuing with live subscriptions",
                type(source).__name__,
                exc_info=True,
            )
    written = store.record_many(records)
    if state is not None:
        state.note_resolutions(written)
    resolved_market_ids = {record.market_id for record in records}
    return tuple(pair for pair in pairs if not _pair_is_resolved(pair, resolved_market_ids))


def _pair_is_resolved(pair: VerifiedContractPair, resolved_market_ids: set[str]) -> bool:
    return (
        pair.polymarket_condition_id in resolved_market_ids
        or pair.kalshi_ticker in resolved_market_ids
    )


def _drop_resolved_live_pairs(
    *,
    records: tuple[ResolutionRecord, ...],
    registry: PairRegistry,
    observer: SignalEventObserver | None = None,
    state: CollectorStateStore | None = None,
) -> tuple[VerifiedContractPair, ...]:
    """Remove resolved markets from live subscriptions while retaining persisted pairs."""
    resolved_market_ids = {record.market_id for record in records}
    if not resolved_market_ids:
        return registry.snapshot()
    next_pairs = tuple(
        pair for pair in registry.snapshot() if not _pair_is_resolved(pair, resolved_market_ids)
    )
    changed = registry.replace(next_pairs)
    if changed and observer is not None:
        observer.set_pairs(next_pairs)
    if changed and state is not None:
        state.set_pairs_tracked(len(next_pairs))
    return next_pairs


def _polymarket_book_token_ids(
    pairs: Sequence[VerifiedContractPair],
    snapshots: Sequence[MetadataSnapshot] = (),
) -> tuple[str, ...]:
    """Return every Polymarket token needed to reconstruct subscribed books."""
    tracked_conditions = {pair.polymarket_condition_id for pair in pairs}
    token_ids: list[str] = []
    seen: set[str] = set()
    for pair in pairs:
        for token_id in (pair.polymarket_asset_id, pair.polymarket_no_asset_id):
            if token_id is None or token_id in seen:
                continue
            seen.add(token_id)
            token_ids.append(token_id)
    for snapshot in snapshots:
        if snapshot.condition_id not in tracked_conditions:
            continue
        for token_id in snapshot.token_ids:
            if token_id in seen:
                continue
            seen.add(token_id)
            token_ids.append(token_id)
    return tuple(token_ids)


class PolymarketVenueTask:
    """Read-only Polymarket Gamma + CLOB ingestion task."""

    name = "polymarket"

    def __init__(
        self,
        *,
        registry: PairRegistry,
        recorder: SignalLoggingRecorder,
        gamma: GammaClient,
        clob_http: ClobHttpClient,
        ws_url: str,
        run_slice_seconds: int = 3_600,
    ) -> None:
        self._registry = registry
        self._recorder = recorder
        self._gamma = gamma
        self._clob_http = clob_http
        self._ws_url = ws_url
        self._run_slice_seconds = run_slice_seconds

    async def run(self) -> None:
        """Fetch metadata and run a bounded CLOB ingestion slice."""
        pairs, version = self._registry.snapshot_with_version()
        if not pairs:
            await self._registry.wait_for_change(version)
            return
        snapshots = _tracked_polymarket_metadata(
            await self._gamma.fetch_all_active_markets(),
            pairs,
        )
        self._recorder.record_many(snapshots)
        self._recorder.flush_all()
        ingestor = MarketDataIngestor(
            token_ids=_polymarket_book_token_ids(pairs, snapshots),
            recorder=self._recorder,
            clob_http=self._clob_http,
            ws_url=self._ws_url,
        )
        await _run_until_registry_change(
            registry=self._registry,
            version=version,
            task=asyncio.create_task(ingestor.run(duration_seconds=self._run_slice_seconds)),
        )


class KalshiVenueTask:
    """Read-only Kalshi market metadata + websocket ingestion task."""

    name = "kalshi"

    def __init__(
        self,
        *,
        registry: PairRegistry,
        recorder: SignalLoggingRecorder,
        metadata: KalshiMetadataClient,
        ws_url: str,
        auth: KalshiAuth | None = None,
    ) -> None:
        self._registry = registry
        self._adapter = KalshiRecorderAdapter(recorder)
        self._metadata = metadata
        self._ws_url = ws_url
        self._auth = auth

    async def run(self) -> None:
        """Fetch metadata and subscribe to Kalshi orderbook/trade channels."""
        pairs, version = self._registry.snapshot_with_version()
        if not pairs:
            await self._registry.wait_for_change(version)
            return
        for pair in pairs:
            self._adapter.record_market_metadata(
                json.loads((await self._metadata.fetch_market(pair.kalshi_ticker)).raw_json)
            )
        self._adapter.flush_all()
        async with connect(
            self._ws_url,
            ping_interval=20.0,
            ssl=kalshi_ssl_context(),
            additional_headers=(
                kalshi_websocket_headers(self._auth, ws_url=self._ws_url)
                if self._auth is not None
                else None
            ),
        ) as websocket:
            await websocket.send(
                json.dumps(
                    kalshi_subscribe_message(
                        channels=("orderbook_delta", "trade"),
                        market_tickers=tuple(pair.kalshi_ticker for pair in pairs),
                    )
                )
            )
            await self._record_until_registry_change(websocket, version=version)

    async def _record_until_registry_change(self, websocket: Any, *, version: int) -> None:
        change_task = asyncio.create_task(self._registry.wait_for_change(version))
        recv_task = asyncio.create_task(websocket.recv())
        try:
            while True:
                done, _pending = await asyncio.wait(
                    {change_task, recv_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if change_task in done:
                    return
                message = recv_task.result()
                self._adapter.record_frame(cast_ws_message(message))
                recv_task = asyncio.create_task(websocket.recv())
        finally:
            change_task.cancel()
            recv_task.cancel()
            await asyncio.gather(change_task, recv_task, return_exceptions=True)


class WorldCupDiscoveryRefreshTask:
    """Supervised task that refreshes World Cup match-winner pairs."""

    name = "world_cup_discovery"

    def __init__(
        self,
        *,
        refresher: WorldCupAllowlistRefresher,
        interval_seconds: float,
    ) -> None:
        self._refresher = refresher
        self._interval_seconds = interval_seconds

    async def run(self) -> None:
        """Refresh discovered World Cup pairs until cancelled."""
        await self._refresher.run(interval_seconds=self._interval_seconds)


async def _run_until_registry_change(
    *,
    registry: PairRegistry,
    version: int,
    task: asyncio.Task[None],
) -> None:
    change_task = asyncio.create_task(registry.wait_for_change(version))
    try:
        done, _pending = await asyncio.wait(
            {task, change_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if change_task in done and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return
        await task
    finally:
        if not task.done():
            task.cancel()
        change_task.cancel()
        await asyncio.gather(task, change_task, return_exceptions=True)


def _tracked_polymarket_metadata(
    snapshots: Sequence[MetadataSnapshot],
    pairs: Sequence[VerifiedContractPair],
) -> tuple[MetadataSnapshot, ...]:
    """Keep only metadata for markets the collector is actively tracking."""
    tracked_conditions = {pair.polymarket_condition_id for pair in pairs}
    return tuple(snapshot for snapshot in snapshots if snapshot.condition_id in tracked_conditions)


def _url_or_default(value: object, default: str) -> str:
    if value is None:
        return default
    text = str(value)
    return text.rstrip("/") if text.startswith(("http", "ws")) else text


def _kalshi_auth_from_settings(settings: Settings) -> KalshiAuth | None:
    if settings.KALSHI_API_KEY_ID is None and settings.KALSHI_PRIVATE_KEY_PATH is None:
        return None
    if settings.KALSHI_API_KEY_ID is None or settings.KALSHI_PRIVATE_KEY_PATH is None:
        raise ValueError("KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH must be set together")
    return KalshiAuth.from_private_key_file(
        key_id=settings.KALSHI_API_KEY_ID.get_secret_value(),
        private_key_path=settings.KALSHI_PRIVATE_KEY_PATH,
    )
