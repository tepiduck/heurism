"""Signal computation and logging from live normalized market updates."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path

from pmbot.backtest.book import OrderBookState
from pmbot.backtest.engine import PointInTimeView
from pmbot.backtest.models import BookSnapshot
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, BookEventType, GapEvent, NormalizedEvent, TradeEvent
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import SignalParquetWriter
from pmbot.signals.contracts import VerifiedContractPair
from pmbot.signals.cross_venue import CrossVenueSignal, SignalValue

DEFAULT_SIGNAL_MIN_INTERVAL_NS = 250_000_000
_PairKey = tuple[str, str, str]
_BookFingerprint = tuple[tuple[str, Decimal, Decimal], ...]
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
    """Maintain point-in-time state and persist signals on each market update."""

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
        self._venue_by_asset = _venue_map(pairs)
        self._signals_by_pair = _signal_map(pairs)
        self._pair_keys_by_identifier = _pair_identifier_map(pairs)
        self._book_fingerprints: dict[str, _BookFingerprint] = {}
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
        """Observe a parsed frame and log after the full batch is applied."""
        latest_signal_ts: int | None = None
        touched_book_assets: set[str] = set()
        for event in events:
            if isinstance(event, GapEvent):
                self._apply_gap(event)
            elif isinstance(event, BookEvent):
                self._book.apply(event)
                if event.event_type == BookEventType.SNAPSHOT:
                    self._gap_assets.discard(event.asset_id)
                    self._gap_conditions.discard(event.condition_id)
                self._note_frame(event.asset_id, event.ts_local_ns)
                latest_signal_ts = event.ts_local_ns
                touched_book_assets.add(event.asset_id)
            elif isinstance(event, TradeEvent):
                self._trades.append(event)
                self._note_frame(event.asset_id, event.ts_local_ns)
                latest_signal_ts = event.ts_local_ns
            else:
                continue
            self._events.append(event)
        changed_pair_keys = self._changed_pair_keys(touched_book_assets)
        if latest_signal_ts is not None and changed_pair_keys:
            self._log_signals(latest_signal_ts, changed_pair_keys)

    def flush(self) -> None:
        """Flush signal writer buffers."""
        self._signal_writer.flush_all()

    def set_pairs(self, pairs: tuple[VerifiedContractPair, ...]) -> None:
        """Replace the signal and venue-attribution pair set."""
        self._pairs = pairs
        self._venue_by_asset = _venue_map(pairs)
        self._signals_by_pair = _signal_map(pairs)
        self._pair_keys_by_identifier = _pair_identifier_map(pairs)
        active_keys = set(self._signals_by_pair)
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

    def _changed_pair_keys(self, touched_book_assets: set[str]) -> set[_PairKey]:
        changed_pair_keys: set[_PairKey] = set()
        for asset_id in touched_book_assets:
            fingerprint = _book_fingerprint(self._book.snapshot(asset_id))
            if self._book_fingerprints.get(asset_id) == fingerprint:
                continue
            self._book_fingerprints[asset_id] = fingerprint
            changed_pair_keys.update(self._pair_keys_by_identifier.get(asset_id, ()))
        return changed_pair_keys

    def _log_signals(self, ts_local_ns: int, pair_keys: Iterable[_PairKey]) -> None:
        view = PointInTimeView(
            ts_local_ns=ts_local_ns,
            _book=self._book,
            _trades=tuple(self._trades),
            _events=tuple(self._events),
            _gap_active_assets=frozenset(self._gap_assets),
            _gap_active_all=bool(self._gap_conditions),
        )
        for pair_key in sorted(pair_keys):
            signal = self._signals_by_pair.get(pair_key)
            if signal is None:
                continue
            for value in signal.compute(view):
                if self._should_record_signal(pair_key, value):
                    self._signal_writer.record_value(value)

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


class SignalLoggingRecorder(ParquetRecorder):
    """Parquet recorder that mirrors normalized updates into the signal logger."""

    def __init__(
        self,
        *,
        data_dir: Path,
        observer: SignalEventObserver,
        buffer_size: int = 100,
        flush_interval_seconds: float = 30.0,
    ) -> None:
        super().__init__(
            data_dir=data_dir,
            buffer_size=buffer_size,
            flush_interval_seconds=flush_interval_seconds,
        )
        self._observer = observer

    def record(self, event: NormalizedEvent) -> None:
        """Record and observe one normalized event."""
        super().record(event)
        self._observer.observe(event)

    def record_many(self, events: Iterable[NormalizedEvent]) -> None:
        """Record and observe many normalized events as one atomic frame."""
        event_tuple = tuple(events)
        for event in event_tuple:
            ParquetRecorder.record(self, event)
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


def _book_fingerprint(book: BookSnapshot) -> _BookFingerprint:
    return (
        *(("bid", level.price, level.size) for level in book.bids),
        *(("ask", level.price, level.size) for level in book.asks),
    )


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
