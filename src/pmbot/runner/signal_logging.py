"""Signal computation and logging from live normalized market updates."""

from __future__ import annotations

from collections import deque
from pathlib import Path

from pmbot.backtest.book import OrderBookState
from pmbot.backtest.engine import PointInTimeView
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, BookEventType, GapEvent, NormalizedEvent, TradeEvent
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import SignalParquetWriter
from pmbot.signals.contracts import VerifiedContractPair
from pmbot.signals.cross_venue import CrossVenueSignal


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
    ) -> None:
        self._pairs = pairs
        self._signal = CrossVenueSignal(pairs)
        self._signal_writer = signal_writer
        self._state = state
        self._book = OrderBookState()
        self._events: deque[BookEvent | TradeEvent | GapEvent] = deque(
            maxlen=max_history_events
        )
        self._trades: deque[TradeEvent] = deque(maxlen=max_history_trades)
        self._gap_assets: set[str] = set()
        self._gap_conditions: set[str] = set()
        self._venue_by_asset = _venue_map(pairs)

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
        if isinstance(event, GapEvent):
            self._apply_gap(event)
        elif isinstance(event, BookEvent):
            self._book.apply(event)
            if event.event_type == BookEventType.SNAPSHOT:
                self._gap_assets.discard(event.asset_id)
                self._gap_conditions.discard(event.condition_id)
            self._note_frame(event.asset_id, event.ts_local_ns)
        elif isinstance(event, TradeEvent):
            self._trades.append(event)
            self._note_frame(event.asset_id, event.ts_local_ns)
        else:
            return
        self._events.append(event)
        if isinstance(event, (BookEvent, TradeEvent)):
            self._log_signals(event.ts_local_ns)

    def flush(self) -> None:
        """Flush signal writer buffers."""
        self._signal_writer.flush_all()

    def set_pairs(self, pairs: tuple[VerifiedContractPair, ...]) -> None:
        """Replace the signal and venue-attribution pair set."""
        self._pairs = pairs
        self._signal = CrossVenueSignal(pairs)
        self._venue_by_asset = _venue_map(pairs)

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

    def _log_signals(self, ts_local_ns: int) -> None:
        view = PointInTimeView(
            ts_local_ns=ts_local_ns,
            _book=self._book,
            _trades=tuple(self._trades),
            _events=tuple(self._events),
            _gap_active_assets=frozenset(self._gap_assets),
            _gap_active_all=bool(self._gap_conditions),
        )
        for value in self._signal.compute(view):
            self._signal_writer.record_value(value)

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
