"""Generic read-only raw market-data harvest runner.

Harvest mode is intentionally single-venue raw capture: it records normalized
books, trades, gap logs, metadata, and optional resolved outcomes, but it never
constructs cross-venue pairs, verifies payout criteria, computes signals, or
routes execution.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from pmbot.config import Settings
from pmbot.ingestion.clob import (
    DEFAULT_CLOB_HTTP_URL,
    DEFAULT_CLOB_WS_URL,
    BackoffPolicy,
    ClobHttpClient,
    MarketDataIngestor,
    cast_ws_message,
)
from pmbot.ingestion.gamma import DEFAULT_GAMMA_URL, GammaClient, normalize_market
from pmbot.ingestion.kalshi import (
    KALSHI_REST_URL,
    KALSHI_WS_URL,
    KalshiAuth,
    KalshiMetadataClient,
    KalshiRecorderAdapter,
    kalshi_ssl_context,
    kalshi_subscribe_message,
    kalshi_websocket_headers,
    normalize_market_metadata,
)
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import (
    BookEvent,
    GapEvent,
    GapReason,
    MetadataSnapshot,
    NormalizedEvent,
    TradeEvent,
)
from pmbot.runner.models import ResolutionRecord
from pmbot.runner.orchestrator import CollectorOrchestrator
from pmbot.runner.state import HARVEST_STATUS_RELATIVE_PATH, CollectorStateStore
from pmbot.runner.streams import ResolutionParquetStore


@dataclass(frozen=True, slots=True)
class HarvestRequest:
    """User-selected raw harvest targets."""

    polymarket_markets_file: Path | None = None
    kalshi_tickers_file: Path | None = None
    kalshi_series: tuple[str, ...] = ()
    polymarket_series: tuple[str, ...] = ()
    polymarket_tags: tuple[str, ...] = ()
    duration_seconds: float | None = None
    poll_resolutions: bool = False
    resolution_interval_seconds: float = 900.0
    data_dir: Path | None = None
    status_file: Path | None = None


@dataclass(frozen=True, slots=True)
class HarvestSelection:
    """Resolved venue identifiers for one harvest run."""

    polymarket_token_ids: tuple[str, ...]
    polymarket_condition_ids: tuple[str, ...]
    kalshi_tickers: tuple[str, ...]
    metadata: tuple[MetadataSnapshot, ...]

    @property
    def market_count(self) -> int:
        """Return the operator-facing tracked market count."""
        polymarket_markets = set(self.polymarket_condition_ids) or set(self.polymarket_token_ids)
        return len(polymarket_markets) + len(set(self.kalshi_tickers))

    def venue_by_identifier(self) -> dict[str, str]:
        """Return identifier-to-venue hints for status attribution."""
        venues: dict[str, str] = {}
        for identifier in (*self.polymarket_token_ids, *self.polymarket_condition_ids):
            venues[identifier] = "polymarket"
        for ticker in self.kalshi_tickers:
            venues[ticker] = "kalshi"
        return venues

    def resolution_markets(self) -> tuple[HarvestResolutionMarket, ...]:
        """Return markets with stable IDs suitable for resolution polling."""
        return (
            *(
                HarvestResolutionMarket(venue="polymarket", market_id=condition_id)
                for condition_id in self.polymarket_condition_ids
            ),
            *(
                HarvestResolutionMarket(venue="kalshi", market_id=ticker)
                for ticker in self.kalshi_tickers
            ),
        )


@dataclass(frozen=True, slots=True)
class HarvestResolutionMarket:
    """One single-venue market whose resolved outcome may be polled later."""

    venue: str
    market_id: str


class HarvestResolutionSource(Protocol):
    """Read-only source for harvested market outcomes."""

    async def fetch(
        self,
        markets: tuple[HarvestResolutionMarket, ...],
    ) -> tuple[ResolutionRecord, ...]:
        """Fetch resolved outcomes for selected single-venue markets."""
        ...


class HarvestResolutionPoller:
    """Poll and idempotently persist outcomes for raw harvest markets."""

    def __init__(
        self,
        *,
        markets: tuple[HarvestResolutionMarket, ...]
        | Callable[[], tuple[HarvestResolutionMarket, ...]],
        sources: tuple[HarvestResolutionSource, ...],
        store: ResolutionParquetStore,
        state: CollectorStateStore | None = None,
    ) -> None:
        self._markets = markets
        self._sources = sources
        self._store = store
        self._state = state

    async def poll_once(self) -> int:
        """Run one resolution poll and return newly written records."""
        markets = self._markets() if callable(self._markets) else self._markets
        fetched: list[ResolutionRecord] = []
        for source in self._sources:
            fetched.extend(await source.fetch(markets))
        written = self._store.record_many(fetched)
        if self._state is not None:
            self._state.note_resolutions(written)
        return written

    async def run(self, *, interval_seconds: float = 900.0) -> None:
        """Poll forever at a fixed interval."""
        while True:
            await self.poll_once()
            await asyncio.sleep(interval_seconds)


class HarvestGammaResolutionSource:
    """Fetch resolved Polymarket outcomes for harvested condition IDs."""

    def __init__(self, client: GammaClient) -> None:
        self._client = client

    async def fetch(
        self,
        markets: tuple[HarvestResolutionMarket, ...],
    ) -> tuple[ResolutionRecord, ...]:
        wanted = {
            market.market_id for market in markets if market.venue == "polymarket"
        }
        if not wanted:
            return ()
        raw_markets = await self._client.fetch_markets(closed=True, limit=500)
        fetched_at = _now()
        records: list[ResolutionRecord] = []
        for raw in raw_markets:
            condition_id = _first_optional_str(raw, "conditionId", "condition_id", "market")
            if condition_id not in wanted:
                continue
            outcome = _resolved_outcome(raw)
            if outcome is None:
                continue
            records.append(
                ResolutionRecord(
                    venue="polymarket",
                    market_id=condition_id,
                    resolved_outcome=outcome,
                    resolved_at=_resolved_at(raw) or fetched_at,
                    fetched_at=fetched_at,
                    raw_json=json.dumps(raw, default=str, separators=(",", ":"), sort_keys=True),
                )
            )
        return tuple(records)


class HarvestKalshiResolutionSource:
    """Fetch resolved Kalshi outcomes for harvested tickers."""

    def __init__(self, client: KalshiMetadataClient) -> None:
        self._client = client

    async def fetch(
        self,
        markets: tuple[HarvestResolutionMarket, ...],
    ) -> tuple[ResolutionRecord, ...]:
        fetched_at = _now()
        records: list[ResolutionRecord] = []
        for market in markets:
            if market.venue != "kalshi":
                continue
            snapshot = await self._client.fetch_market(market.market_id)
            raw = json.loads(snapshot.raw_json, parse_float=Decimal)
            if not isinstance(raw, Mapping):
                continue
            outcome = _resolved_outcome(raw)
            if outcome is None:
                continue
            records.append(
                ResolutionRecord(
                    venue="kalshi",
                    market_id=market.market_id,
                    resolved_outcome=outcome,
                    resolved_at=_resolved_at(raw) or fetched_at,
                    fetched_at=fetched_at,
                    raw_json=snapshot.raw_json,
                )
            )
        return tuple(records)


class StaticHarvestResolutionSource:
    """Fixture harvest resolution source for tests."""

    def __init__(self, records: tuple[ResolutionRecord, ...]) -> None:
        self._records = records

    async def fetch(
        self,
        markets: tuple[HarvestResolutionMarket, ...],
    ) -> tuple[ResolutionRecord, ...]:
        del markets
        return self._records


class StatusTrackingRecorder(ParquetRecorder):
    """Parquet recorder that updates harvest status without computing signals."""

    def __init__(
        self,
        *,
        data_dir: Path,
        state: CollectorStateStore,
        venue_by_identifier: Mapping[str, str],
        buffer_size: int = 10_000,
        flush_interval_seconds: float = 60.0,
        max_recorded_depth: int | None = 10,
    ) -> None:
        super().__init__(
            data_dir=data_dir,
            buffer_size=buffer_size,
            flush_interval_seconds=flush_interval_seconds,
            max_recorded_depth=max_recorded_depth,
        )
        self._state = state
        self._venue_by_identifier = dict(venue_by_identifier)

    def record(self, event: NormalizedEvent) -> None:
        """Record one event and update harvest status."""
        super().record(event)
        self._observe(event)

    def record_many(self, events: Iterable[NormalizedEvent]) -> None:
        """Record many events and update harvest status."""
        event_tuple = tuple(events)
        ParquetRecorder.record_many(self, event_tuple)
        for event in event_tuple:
            self._observe(event)

    def flush_all(self) -> None:
        """Flush raw buffers and force the latest harvest status to disk."""
        super().flush_all()
        self._state.save()

    def _observe(self, event: NormalizedEvent) -> None:
        if isinstance(event, (BookEvent, TradeEvent)):
            self._state.note_frame(
                _venue_for_event(
                    self._venue_by_identifier,
                    asset_id=event.asset_id,
                    condition_id=event.condition_id,
                ),
                ts_local_ns=event.ts_local_ns,
            )
        elif isinstance(event, GapEvent):
            self._state.note_gap(
                _venue_for_event(
                    self._venue_by_identifier,
                    asset_id=event.asset_id,
                    condition_id=event.condition_id,
                ),
                ts_local_ns=event.ts_local_ns,
            )


class PolymarketHarvestTask:
    """Run raw Polymarket CLOB capture for selected token IDs."""

    name = "polymarket"

    def __init__(
        self,
        *,
        token_ids: tuple[str, ...],
        recorder: ParquetRecorder,
        clob_http: ClobHttpClient,
        ws_url: str,
        run_slice_seconds: int = 3_600,
    ) -> None:
        self._token_ids = token_ids
        self._recorder = recorder
        self._clob_http = clob_http
        self._ws_url = ws_url
        self._run_slice_seconds = run_slice_seconds

    async def run(self) -> None:
        """Run one supervised Polymarket ingestion slice."""
        ingestor = MarketDataIngestor(
            token_ids=self._token_ids,
            recorder=self._recorder,
            clob_http=self._clob_http,
            ws_url=self._ws_url,
        )
        await ingestor.run(duration_seconds=self._run_slice_seconds)


class KalshiHarvestTask:
    """Run raw Kalshi websocket capture for selected tickers."""

    name = "kalshi"

    def __init__(
        self,
        *,
        tickers: tuple[str, ...],
        recorder: ParquetRecorder,
        ws_url: str,
        auth: KalshiAuth | None = None,
    ) -> None:
        self._tickers = tickers
        self._adapter = KalshiRecorderAdapter(recorder)
        self._recorder = recorder
        self._ws_url = ws_url
        self._auth = auth

    async def run(self) -> None:
        """Subscribe to Kalshi orderbook/trade channels until cancelled or disconnected."""
        try:
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
                            market_tickers=self._tickers,
                        )
                    )
                )
                while True:
                    message = await websocket.recv()
                    self._adapter.record_frame(cast_ws_message(message))
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError) as exc:
            self._record_connection_loss(str(exc))
            raise

    def _record_connection_loss(self, details: str) -> None:
        for ticker in self._tickers:
            self._recorder.record(
                GapEvent(
                    ts_local_ns=time.time_ns(),
                    ts_monotonic_ns=time.monotonic_ns(),
                    ts_exchange=None,
                    condition_id=ticker,
                    asset_id=ticker,
                    reason=GapReason.CONNECTION_LOST,
                    details=f"Kalshi websocket connection lost: {details}",
                    seq_or_hash=None,
                )
            )
        self._recorder.flush_all()


async def run_harvest(*, request: HarvestRequest, settings: Settings) -> None:
    """Run generic single-venue raw market-data harvest."""
    data_dir = Path(request.data_dir or settings.DATA_DIR)
    status_file = request.status_file
    gamma_url = _url_or_default(settings.GAMMA_URL, DEFAULT_GAMMA_URL)
    clob_http_url = _url_or_default(settings.CLOB_HTTP_URL, DEFAULT_CLOB_HTTP_URL)
    clob_ws_url = _url_or_default(settings.CLOB_WS_URL, DEFAULT_CLOB_WS_URL)

    async with httpx.AsyncClient(timeout=20.0) as http:
        gamma = GammaClient(base_url=gamma_url, http=http)
        kalshi_auth = _kalshi_auth_from_settings(settings)
        kalshi_metadata = KalshiMetadataClient(
            base_url=KALSHI_REST_URL,
            http=http,
            auth=kalshi_auth,
        )
        selection = await resolve_harvest_selection(
            request,
            gamma=gamma,
            kalshi=kalshi_metadata,
        )
        if selection.market_count == 0:
            raise ValueError("no markets selected for harvest")

        state = CollectorStateStore(
            data_dir=data_dir,
            pairs_tracked=selection.market_count,
            status_path=status_file,
            relative_path=HARVEST_STATUS_RELATIVE_PATH,
        )
        recorder = StatusTrackingRecorder(
            data_dir=data_dir,
            state=state,
            venue_by_identifier=selection.venue_by_identifier(),
            buffer_size=settings.RAW_RECORDER_BUFFER_SIZE,
            flush_interval_seconds=settings.RECORDER_FLUSH_INTERVAL_SECONDS,
            max_recorded_depth=settings.MAX_RECORDED_DEPTH,
        )
        recorder.record_many(selection.metadata)
        recorder.flush_all()

        venue_tasks = _harvest_venue_tasks(
            selection=selection,
            recorder=recorder,
            clob_http=ClobHttpClient(base_url=clob_http_url, http=http),
            clob_ws_url=clob_ws_url,
            kalshi_auth=kalshi_auth,
        )
        poller = None
        if request.poll_resolutions:
            sources: tuple[HarvestResolutionSource, ...] = (
                HarvestGammaResolutionSource(gamma),
                HarvestKalshiResolutionSource(kalshi_metadata),
            )
            poller = HarvestResolutionPoller(
                markets=selection.resolution_markets(),
                sources=sources,
                store=ResolutionParquetStore(data_dir=data_dir),
                state=state,
            )
        orchestrator = CollectorOrchestrator(
            venue_tasks=venue_tasks,
            resolution_poller=poller,
            state=state,
            backoff=BackoffPolicy(),
            resolution_interval_seconds=request.resolution_interval_seconds,
        )
        try:
            await orchestrator.run(duration_seconds=request.duration_seconds)
        finally:
            recorder.flush_all()


def _harvest_venue_tasks(
    *,
    selection: HarvestSelection,
    recorder: ParquetRecorder,
    clob_http: ClobHttpClient,
    clob_ws_url: str,
    kalshi_auth: KalshiAuth | None,
) -> tuple[PolymarketHarvestTask | KalshiHarvestTask, ...]:
    tasks: list[PolymarketHarvestTask | KalshiHarvestTask] = []
    if selection.polymarket_token_ids:
        tasks.append(
            PolymarketHarvestTask(
                token_ids=selection.polymarket_token_ids,
                recorder=recorder,
                clob_http=clob_http,
                ws_url=clob_ws_url,
            )
        )
    if selection.kalshi_tickers:
        tasks.append(
            KalshiHarvestTask(
                tickers=selection.kalshi_tickers,
                recorder=recorder,
                ws_url=KALSHI_WS_URL,
                auth=kalshi_auth,
            )
        )
    return tuple(tasks)


async def resolve_harvest_selection(
    request: HarvestRequest,
    *,
    gamma: GammaClient,
    kalshi: KalshiMetadataClient,
) -> HarvestSelection:
    """Resolve list and group selectors into raw venue subscription IDs."""
    metadata: list[MetadataSnapshot] = []
    polymarket_tokens: list[str] = []
    polymarket_conditions: list[str] = []
    explicit_polymarket = _read_identifier_file(request.polymarket_markets_file)
    explicit_tokens, explicit_conditions = _split_polymarket_identifiers(explicit_polymarket)
    polymarket_tokens.extend(explicit_tokens)
    polymarket_conditions.extend(explicit_conditions)

    group_snapshots = await _fetch_polymarket_group_snapshots(
        gamma=gamma,
        series_slugs=request.polymarket_series,
        tag_slugs=request.polymarket_tags,
    )
    metadata.extend(group_snapshots)
    for snapshot in group_snapshots:
        polymarket_conditions.append(snapshot.condition_id)
        polymarket_tokens.extend(snapshot.token_ids)

    if explicit_conditions:
        resolved = await _resolve_polymarket_conditions(gamma, explicit_conditions)
        metadata.extend(resolved)
        for snapshot in resolved:
            polymarket_tokens.extend(snapshot.token_ids)

    kalshi_tickers = list(_read_identifier_file(request.kalshi_tickers_file))
    kalshi_snapshots = await _fetch_kalshi_series_snapshots(
        kalshi=kalshi,
        series_tickers=request.kalshi_series,
    )
    metadata.extend(kalshi_snapshots)
    kalshi_tickers.extend(snapshot.condition_id for snapshot in kalshi_snapshots)

    for ticker in tuple(kalshi_tickers):
        if ticker in {snapshot.condition_id for snapshot in kalshi_snapshots}:
            continue
        snapshot = await kalshi.fetch_market(ticker)
        metadata.append(snapshot)

    return HarvestSelection(
        polymarket_token_ids=_dedupe(polymarket_tokens),
        polymarket_condition_ids=_dedupe(polymarket_conditions),
        kalshi_tickers=_dedupe(kalshi_tickers),
        metadata=_dedupe_metadata(metadata),
    )


def _read_identifier_file(path: Path | None) -> tuple[str, ...]:
    if path is None:
        return ()
    identifiers: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        identifiers.extend(item.strip() for item in line.split(",") if item.strip())
    return tuple(identifiers)


def _split_polymarket_identifiers(values: Sequence[str]) -> tuple[list[str], list[str]]:
    tokens: list[str] = []
    conditions: list[str] = []
    for value in values:
        if value.isdecimal():
            tokens.append(value)
        else:
            conditions.append(value)
    return tokens, conditions


async def _fetch_polymarket_group_snapshots(
    *,
    gamma: GammaClient,
    series_slugs: Sequence[str],
    tag_slugs: Sequence[str],
    page_size: int = 100,
) -> tuple[MetadataSnapshot, ...]:
    snapshots: list[MetadataSnapshot] = []
    fetched_at = datetime.now(UTC).isoformat()
    for series_slug in series_slugs:
        snapshots.extend(
            await _fetch_polymarket_event_snapshots(
                gamma=gamma,
                fetched_at=fetched_at,
                page_size=page_size,
                series_slug=series_slug,
            )
        )
    for tag_slug in tag_slugs:
        snapshots.extend(
            await _fetch_polymarket_event_snapshots(
                gamma=gamma,
                fetched_at=fetched_at,
                page_size=page_size,
                tag_slug=tag_slug,
            )
        )
    return _dedupe_metadata(snapshots)


async def _fetch_polymarket_event_snapshots(
    *,
    gamma: GammaClient,
    fetched_at: str,
    page_size: int,
    series_slug: str | None = None,
    tag_slug: str | None = None,
) -> tuple[MetadataSnapshot, ...]:
    snapshots: list[MetadataSnapshot] = []
    offset = 0
    while True:
        try:
            events = await gamma.fetch_events(
                series_slug=series_slug,
                tag_slug=tag_slug,
                active=True,
                closed=False,
                limit=page_size,
                offset=offset,
            )
        except httpx.HTTPStatusError as exc:
            if offset > 0 and exc.response.status_code == 422:
                break
            raise
        snapshots.extend(_metadata_from_events(events, fetched_at=fetched_at))
        if len(events) < page_size:
            break
        offset += page_size
    return tuple(snapshots)


def _metadata_from_events(
    events: Sequence[Mapping[str, object]],
    *,
    fetched_at: str,
) -> tuple[MetadataSnapshot, ...]:
    snapshots: list[MetadataSnapshot] = []
    for event in events:
        raw_markets = event.get("markets")
        if not isinstance(raw_markets, Sequence) or isinstance(
            raw_markets,
            (str, bytes, bytearray),
        ):
            continue
        for raw_market in raw_markets:
            if not isinstance(raw_market, Mapping):
                continue
            try:
                snapshot = normalize_market(raw_market, fetched_at=fetched_at)
            except (KeyError, TypeError, json.JSONDecodeError):
                continue
            if snapshot.token_ids and snapshot.active is not False and snapshot.closed is not True:
                snapshots.append(snapshot)
    return tuple(snapshots)


async def _resolve_polymarket_conditions(
    gamma: GammaClient,
    condition_ids: Sequence[str],
) -> tuple[MetadataSnapshot, ...]:
    wanted = set(condition_ids)
    snapshots = await gamma.fetch_all_active_markets()
    by_condition = {snapshot.condition_id: snapshot for snapshot in snapshots}
    missing = sorted(wanted - set(by_condition))
    if missing:
        raise ValueError(f"could not resolve Polymarket condition IDs: {', '.join(missing)}")
    return tuple(by_condition[condition_id] for condition_id in condition_ids)


async def _fetch_kalshi_series_snapshots(
    *,
    kalshi: KalshiMetadataClient,
    series_tickers: Sequence[str],
    page_size: int = 100,
) -> tuple[MetadataSnapshot, ...]:
    snapshots: list[MetadataSnapshot] = []
    for series_ticker in series_tickers:
        cursor: str | None = None
        while True:
            page, cursor = await kalshi.fetch_markets(
                status="open",
                series_ticker=series_ticker,
                limit=page_size,
                cursor=cursor,
            )
            snapshots.extend(normalize_market_metadata(raw_market) for raw_market in page)
            if not page or cursor is None:
                break
    return _dedupe_metadata(snapshots)


def _dedupe(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def _dedupe_metadata(values: Iterable[MetadataSnapshot]) -> tuple[MetadataSnapshot, ...]:
    seen: set[str] = set()
    result: list[MetadataSnapshot] = []
    for value in values:
        if value.condition_id in seen:
            continue
        seen.add(value.condition_id)
        result.append(value)
    return tuple(result)


def _venue_for_event(
    venue_by_identifier: Mapping[str, str],
    *,
    asset_id: str | None,
    condition_id: str,
) -> str:
    for identifier in (asset_id, condition_id):
        if identifier is None:
            continue
        venue = venue_by_identifier.get(identifier)
        if venue is not None:
            return venue
    for identifier in (asset_id, condition_id):
        if identifier is None:
            continue
        venue = _infer_venue(identifier)
        if venue != "unknown":
            return venue
    return "unknown"


def _infer_venue(identifier: str) -> str:
    text = identifier.strip()
    if text.casefold().startswith("0x"):
        return "polymarket"
    if len(text) >= 20 and text.isdecimal():
        return "polymarket"
    if text.upper().startswith("KX"):
        return "kalshi"
    return "unknown"


def _resolved_outcome(raw: Mapping[str, object]) -> str | None:
    for key in (
        "resolved_outcome",
        "resolvedOutcome",
        "winningOutcome",
        "winner",
        "result",
        "resolution",
        "outcome",
    ):
        value = raw.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            return "yes" if value else "no"
        text = str(value).strip().casefold()
        if text in {"yes", "no", "true", "false", "1", "0"}:
            return "yes" if text in {"yes", "true", "1"} else "no"
        return text
    return None


def _resolved_at(raw: Mapping[str, object]) -> str | None:
    return _first_optional_str(
        raw,
        "resolved_at",
        "resolvedAt",
        "resolutionTime",
        "settlementTime",
        "expiration_time",
        "close_time",
    )


def _first_optional_str(raw: Mapping[str, object], *keys: str) -> str | None:
    for key in keys:
        value = raw.get(key)
        if value is not None:
            return str(value)
    return None


def _now() -> str:
    return datetime.now(UTC).isoformat()


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


def assert_harvest_read_only() -> None:
    """Document and assert that harvest mode has no execution or signal dependencies."""
    forbidden_symbols = (
        "CrossVenueSignal",
        "SignalEventObserver",
        "SignalLoggingRecorder",
        "SignalParquetWriter",
    )
    leaked = sorted(symbol for symbol in forbidden_symbols if symbol in globals())
    if leaked:
        raise AssertionError(f"harvest mode imports signal/execution symbols: {', '.join(leaked)}")
