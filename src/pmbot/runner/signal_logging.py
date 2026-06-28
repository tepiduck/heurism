"""Signal computation and logging from live normalized market updates."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pmbot.backtest.book import OrderBookState
from pmbot.backtest.engine import PointInTimeView, QuoteClock
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import (
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    NormalizedEvent,
    TradeEvent,
)
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import SignalParquetWriter
from pmbot.signals.contracts import VerifiedContractPair
from pmbot.signals.cross_venue import CrossVenueSignal, SignalValue

DEFAULT_SIGNAL_MIN_INTERVAL_NS = 250_000_000
DEFAULT_SIGNAL_COMPUTE_INTERVAL_SECONDS = 1.0
DEFAULT_RAW_RECORDER_BUFFER_SIZE = 10_000
_PairKey = tuple[str, str, str]
_SignalFingerprint = tuple[
    Decimal,
    Decimal,
    Decimal,
    Decimal,
    str,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
]


class SignalEventObserver:
    """Maintain point-in-time state for periodic signal computation."""

    def __init__(
        self,
        *,
        pairs: tuple[VerifiedContractPair, ...],
        signal_writer: SignalParquetWriter,
        state: CollectorStateStore | None = None,
        max_history_events: int = 10_000,
        max_history_trades: int = 2_000,
        signal_min_interval_ns: int = DEFAULT_SIGNAL_MIN_INTERVAL_NS,
    ) -> None:
        self._pairs = pairs
        self._signal_writer = signal_writer
        self._state = state
        self._signal_min_interval_ns = max(0, signal_min_interval_ns)
        self._book = OrderBookState()
        self._events: deque[BookEvent | TradeEvent | GapEvent] = deque(
            maxlen=max_history_events
        )
        self._trades: deque[TradeEvent] = deque(maxlen=max_history_trades)
        self._gap_assets: set[str] = set()
        self._gap_conditions: set[str] = set()
        self._quote_clocks: dict[str, QuoteClock] = {}
        self._venue_by_asset = _venue_map(pairs)
        self._signals_by_pair = _signal_map(pairs)
        self._pair_keys_by_identifier = _pair_identifier_map(pairs)
        self._dirty_pair_keys: set[_PairKey] = set()
        self._latest_book_ts_ns: int | None = None
        self._last_signal_fingerprints: dict[_PairKey, _SignalFingerprint] = {}
        self._last_signal_write_ns: dict[_PairKey, int] = {}

    @property
    def recent_event_count(self) -> int:
        """Return bounded event history size for diagnostics/tests."""
        return len(self._events)

    @property
    def recent_trade_count(self) -> int:
        """Return bounded trade history size for diagnostics/tests."""
        return len(self._trades)

    def observe(self, event: NormalizedEvent) -> None:
        """Observe one normalized event and log any computable signals."""
        self.observe_many((event,))

    def observe_many(self, events: Iterable[NormalizedEvent]) -> None:
        """Observe a parsed frame with only cheap per-event bookkeeping."""
        for event in events:
            if isinstance(event, GapEvent):
                self._apply_gap(event)
            elif isinstance(event, BookEvent):
                self._book.apply(event)
                self._apply_quote_clock(event)
                if event.event_type == BookEventType.SNAPSHOT:
                    self._gap_assets.discard(event.asset_id)
                    self._gap_conditions.discard(event.condition_id)
                self._note_frame(event.asset_id, event.ts_local_ns)
                self._latest_book_ts_ns = event.ts_local_ns
                self._dirty_pair_keys.update(self._pair_keys_by_identifier.get(event.asset_id, ()))
            elif isinstance(event, TradeEvent):
                self._trades.append(event)
                self._note_frame(event.asset_id, event.ts_local_ns)
            else:
                continue
            self._events.append(event)

    def flush(self) -> None:
        """Flush signal writer buffers."""
        self._signal_writer.flush_all()
        if self._state is not None:
            self._state.save()

    def compute_pending_signals(self) -> int:
        """Compute dirty pair signals synchronously, mainly for tests/tools."""
        job = self._snapshot_signal_job()
        if job is None:
            return 0
        try:
            values = job.compute()
        except Exception:
            self._dirty_pair_keys.update(job.pair_keys)
            raise
        return self._record_computed_signals(job.pair_keys, values)

    async def compute_pending_signals_async(self) -> int:
        """Snapshot state and compute dirty pair signals in a worker thread."""
        job = self._snapshot_signal_job()
        if job is None:
            return 0
        try:
            values = await asyncio.to_thread(job.compute)
        except Exception:
            self._dirty_pair_keys.update(job.pair_keys)
            raise
        return self._record_computed_signals(job.pair_keys, values)

    def set_pairs(self, pairs: tuple[VerifiedContractPair, ...]) -> None:
        """Replace the signal and venue-attribution pair set."""
        self._pairs = pairs
        self._venue_by_asset = _venue_map(pairs)
        self._signals_by_pair = _signal_map(pairs)
        self._pair_keys_by_identifier = _pair_identifier_map(pairs)
        active_keys = set(self._signals_by_pair)
        active_identifiers = set(self._pair_keys_by_identifier)
        active_condition_ids = {pair.polymarket_condition_id for pair in pairs}
        self._book.retain_assets(active_identifiers)
        self._quote_clocks = {
            asset_id: clock
            for asset_id, clock in self._quote_clocks.items()
            if asset_id in active_identifiers
        }
        self._gap_assets.intersection_update(active_identifiers)
        self._gap_conditions.intersection_update(active_condition_ids)
        self._dirty_pair_keys = {
            pair_key for pair_key in self._dirty_pair_keys if pair_key in active_keys
        }
        self._dirty_pair_keys.update(active_keys)
        self._last_signal_fingerprints = {
            key: value
            for key, value in self._last_signal_fingerprints.items()
            if key in active_keys
        }
        self._last_signal_write_ns = {
            key: value for key, value in self._last_signal_write_ns.items() if key in active_keys
        }

    def _apply_gap(self, event: GapEvent) -> None:
        venue = _venue_for_event(
            venue_by_identifier=self._venue_by_asset,
            asset_id=event.asset_id,
            condition_id=event.condition_id,
        )
        if self._state is not None:
            self._state.note_gap(venue, ts_local_ns=event.ts_local_ns)
        if event.asset_id is None:
            self._gap_conditions.add(event.condition_id)
            return
        self._gap_assets.add(event.asset_id)

    def _snapshot_signal_job(self) -> _SignalComputeJob | None:
        if self._latest_book_ts_ns is None or not self._dirty_pair_keys:
            return None
        pair_keys = frozenset(
            key for key in self._dirty_pair_keys if key in self._signals_by_pair
        )
        if not pair_keys:
            self._dirty_pair_keys.clear()
            return None
        self._dirty_pair_keys.difference_update(pair_keys)
        view = PointInTimeView(
            ts_local_ns=self._latest_book_ts_ns,
            _book=self._book.clone(),
            _trades=tuple(self._trades),
            _events=tuple(self._events),
            _gap_active_assets=frozenset(self._gap_assets),
            _gap_active_all=bool(self._gap_conditions),
            _quote_clocks=dict(self._quote_clocks),
        )
        return _SignalComputeJob(
            pair_keys=pair_keys,
            signals_by_pair={
                pair_key: self._signals_by_pair[pair_key] for pair_key in pair_keys
            },
            view=view,
        )

    def _record_computed_signals(
        self,
        pair_keys: frozenset[_PairKey],
        values: Iterable[SignalValue],
    ) -> int:
        recorded = 0
        for value in values:
            pair_key = (
                value.polymarket_condition_id,
                value.polymarket_asset_id,
                value.kalshi_ticker,
            )
            if pair_key not in pair_keys:
                continue
            if self._should_record_signal(pair_key, value):
                self._signal_writer.record_value(value)
                recorded += 1
        return recorded

    def _should_record_signal(self, pair_key: _PairKey, value: SignalValue) -> bool:
        fingerprint = _signal_fingerprint(value)
        previous = self._last_signal_fingerprints.get(pair_key)
        previous_write_ns = self._last_signal_write_ns.get(pair_key)
        signal_changed = previous is None or fingerprint != previous
        interval_elapsed = (
            previous_write_ns is None
            or self._signal_min_interval_ns == 0
            or value.ts_local_ns - previous_write_ns >= self._signal_min_interval_ns
        )
        if not signal_changed and not interval_elapsed:
            return False
        self._last_signal_fingerprints[pair_key] = fingerprint
        self._last_signal_write_ns[pair_key] = value.ts_local_ns
        return True

    def _note_frame(self, asset_id: str, ts_local_ns: int) -> None:
        if self._state is None:
            return
        self._state.note_frame(
            _venue_for_identifier(asset_id, self._venue_by_asset),
            ts_local_ns=ts_local_ns,
        )

    def _apply_quote_clock(self, event: BookEvent) -> None:
        ts_exchange_ns = _parse_exchange_time_ns(event.ts_exchange)
        if ts_exchange_ns is None:
            return
        current = self._quote_clocks.get(event.asset_id, QuoteClock(None, None))
        if event.side == BookSide.BID:
            bid_ns = (
                ts_exchange_ns
                if current.bid_ns is None
                else max(current.bid_ns, ts_exchange_ns)
            )
            self._quote_clocks[event.asset_id] = QuoteClock(bid_ns, current.ask_ns)
        elif event.side == BookSide.ASK:
            ask_ns = (
                ts_exchange_ns
                if current.ask_ns is None
                else max(current.ask_ns, ts_exchange_ns)
            )
            self._quote_clocks[event.asset_id] = QuoteClock(current.bid_ns, ask_ns)


@dataclass(frozen=True, slots=True)
class _SignalComputeJob:
    pair_keys: frozenset[_PairKey]
    signals_by_pair: dict[_PairKey, CrossVenueSignal]
    view: PointInTimeView

    def compute(self) -> tuple[SignalValue, ...]:
        values: list[SignalValue] = []
        for pair_key in sorted(self.pair_keys):
            signal = self.signals_by_pair.get(pair_key)
            if signal is not None:
                values.extend(signal.compute(self.view))
        return tuple(values)


class SignalComputationTask:
    """Periodically compute live signals away from ingestion callbacks."""

    name = "signal_computation"

    def __init__(
        self,
        *,
        observer: SignalEventObserver,
        interval_seconds: float = DEFAULT_SIGNAL_COMPUTE_INTERVAL_SECONDS,
    ) -> None:
        self._observer = observer
        self._interval_seconds = max(0.001, interval_seconds)

    async def run(self) -> None:
        """Compute dirty signals forever on a fixed cadence."""
        while True:
            await asyncio.sleep(self._interval_seconds)
            await self._observer.compute_pending_signals_async()


class SignalLoggingRecorder(ParquetRecorder):
    """Parquet recorder that mirrors normalized updates into the signal logger."""

    def __init__(
        self,
        *,
        data_dir: Path,
        observer: SignalEventObserver,
        buffer_size: int = DEFAULT_RAW_RECORDER_BUFFER_SIZE,
        flush_interval_seconds: float = 60.0,
        max_recorded_depth: int | None = 10,
    ) -> None:
        super().__init__(
            data_dir=data_dir,
            buffer_size=buffer_size,
            flush_interval_seconds=flush_interval_seconds,
            max_recorded_depth=max_recorded_depth,
        )
        self._observer = observer

    def record(self, event: NormalizedEvent) -> None:
        """Record and observe one normalized event."""
        super().record(event)
        self._observer.observe(event)

    def record_many(self, events: Iterable[NormalizedEvent]) -> None:
        """Record and observe many normalized events as one atomic frame."""
        event_tuple = tuple(events)
        ParquetRecorder.record_many(self, event_tuple)
        self._observer.observe_many(event_tuple)

    def flush_all(self) -> None:
        """Flush market-data and signal buffers."""
        super().flush_all()
        self._observer.flush()


def _venue_map(pairs: tuple[VerifiedContractPair, ...]) -> dict[str, str]:
    venue_by_asset: dict[str, str] = {}
    for pair in pairs:
        venue_by_asset[pair.polymarket_asset_id] = "polymarket"
        if pair.polymarket_no_asset_id is not None:
            venue_by_asset[pair.polymarket_no_asset_id] = "polymarket"
        venue_by_asset[pair.polymarket_condition_id] = "polymarket"
        venue_by_asset[pair.kalshi_ticker] = "kalshi"
    return venue_by_asset


def _pair_key(pair: VerifiedContractPair) -> _PairKey:
    return (pair.polymarket_condition_id, pair.polymarket_asset_id, pair.kalshi_ticker)


def _signal_map(pairs: tuple[VerifiedContractPair, ...]) -> dict[_PairKey, CrossVenueSignal]:
    return {_pair_key(pair): CrossVenueSignal((pair,)) for pair in pairs}


def _pair_identifier_map(pairs: tuple[VerifiedContractPair, ...]) -> dict[str, set[_PairKey]]:
    pairs_by_identifier: dict[str, set[_PairKey]] = {}
    for pair in pairs:
        pair_key = _pair_key(pair)
        identifiers = [pair.polymarket_asset_id, pair.kalshi_ticker]
        if pair.polymarket_no_asset_id is not None:
            identifiers.append(pair.polymarket_no_asset_id)
        for identifier in identifiers:
            pairs_by_identifier.setdefault(identifier, set()).add(pair_key)
    return pairs_by_identifier


def _signal_fingerprint(value: SignalValue) -> _SignalFingerprint:
    return (
        value.polymarket_probability,
        value.kalshi_probability,
        value.fair_probability,
        value.signal_value,
        value.direction.value,
        value.polymarket_yes_ask,
        value.polymarket_no_ask,
        value.yes_edge,
        value.no_edge,
    )


def _venue_for_event(
    *,
    venue_by_identifier: dict[str, str],
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


def _venue_for_identifier(identifier: str, venue_by_identifier: dict[str, str]) -> str:
    venue = venue_by_identifier.get(identifier)
    if venue is not None:
        return venue
    return _infer_venue(identifier)


def _infer_venue(identifier: str) -> str:
    text = identifier.strip()
    if text.casefold().startswith("0x"):
        return "polymarket"
    if len(text) >= 20 and text.isdecimal():
        return "polymarket"
    if text.upper().startswith("KX"):
        return "kalshi"
    return "unknown"


def _parse_exchange_time_ns(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        pass
    iso_value = stripped.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    parsed_utc = parsed.astimezone(UTC)
    return int(parsed_utc.timestamp()) * 1_000_000_000 + parsed_utc.microsecond * 1_000
