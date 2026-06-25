"""Signal computation and logging from live normalized market updates."""

from __future__ import annotations

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
    ) -> None:
        self._pairs = pairs
        self._signal = CrossVenueSignal(pairs)
        self._signal_writer = signal_writer
        self._state = state
        self._book = OrderBookState()
        self._events: list[BookEvent | TradeEvent | GapEvent] = []
        self._trades: list[TradeEvent] = []
        self._gap_assets: set[str] = set()
        self._gap_conditions: set[str] = set()
        self._venue_by_asset = _venue_map(pairs)

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

    def _apply_gap(self, event: GapEvent) -> None:
        venue = self._venue_by_asset.get(event.asset_id or event.condition_id, "unknown")
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
            self._venue_by_asset.get(asset_id, "unknown"),
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
        venue_by_asset[pair.polymarket_condition_id] = "polymarket"
        venue_by_asset[pair.kalshi_ticker] = "kalshi"
    return venue_by_asset
