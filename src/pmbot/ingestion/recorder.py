"""Buffered Parquet recorder for normalized ingestion events."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path
from typing import Any, TypeVar, cast

import polars as pl

from pmbot.ingestion.schemas import (
    DECIMAL_PRECISION,
    DECIMAL_SCALE,
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    MetadataSnapshot,
    NormalizedEvent,
    TradeEvent,
)

T = TypeVar("T")
DEFAULT_RECORDER_BUFFER_SIZE = 10_000
DEFAULT_MAX_RECORDED_DEPTH = 10
PARQUET_DECIMAL_QUANTUM = Decimal(1).scaleb(-DECIMAL_SCALE)
PARQUET_DECIMAL_MAX = Decimal(
    f"{'9' * (DECIMAL_PRECISION - DECIMAL_SCALE)}.{'9' * DECIMAL_SCALE}"
)
PARQUET_DECIMAL_MIN = -PARQUET_DECIMAL_MAX
METADATA_RAW_JSON_KEYS = frozenset(
    {
        "__event_end_date",
        "__event_slug",
        "__event_title",
        "acceptingOrders",
        "active",
        "clobTokenIds",
        "closed",
        "conditionId",
        "condition_id",
        "endDate",
        "endDateIso",
        "eventSlug",
        "eventTitle",
        "event_slug",
        "expiration_time",
        "expected_expiration_time",
        "expiration_value",
        "groupItemTitle",
        "market",
        "outcome",
        "outcomes",
        "question",
        "resolution",
        "resolutionTime",
        "resolvedOutcome",
        "resolved_outcome",
        "result",
        "slug",
        "status",
        "subtitle",
        "ticker",
        "title",
        "token_ids",
        "umaEndDate",
        "umaEndDateIso",
        "winner",
        "winningOutcome",
        "yes_sub_title",
    }
)


BOOK_SCHEMA: pl.Schema = pl.Schema(
    cast(
        Any,
        {
            "ts_local_ns": pl.Int64,
            "ts_monotonic_ns": pl.Int64,
            "ts_exchange": pl.Utf8,
            "condition_id": pl.Utf8,
            "asset_id": pl.Utf8,
            "event_type": pl.Utf8,
            "side": pl.Utf8,
            "price": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "size": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "seq_or_hash": pl.Utf8,
        },
    )
)

TRADE_SCHEMA: pl.Schema = pl.Schema(
    cast(
        Any,
        {
            "ts_local_ns": pl.Int64,
            "ts_monotonic_ns": pl.Int64,
            "ts_exchange": pl.Utf8,
            "condition_id": pl.Utf8,
            "asset_id": pl.Utf8,
            "price": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "size": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "taker_side": pl.Utf8,
            "trade_id": pl.Utf8,
        },
    )
)

GAP_SCHEMA: pl.Schema = pl.Schema(
    cast(
        Any,
        {
            "ts_local_ns": pl.Int64,
            "ts_monotonic_ns": pl.Int64,
            "ts_exchange": pl.Utf8,
            "condition_id": pl.Utf8,
            "asset_id": pl.Utf8,
            "reason": pl.Utf8,
            "details": pl.Utf8,
            "seq_or_hash": pl.Utf8,
        },
    )
)

METADATA_SCHEMA: pl.Schema = pl.Schema(
    cast(
        Any,
        {
            "fetched_at": pl.Utf8,
            "condition_id": pl.Utf8,
            "question": pl.Utf8,
            "outcomes": pl.Utf8,
            "token_ids": pl.Utf8,
            "resolution_or_end_time": pl.Utf8,
            "active": pl.Boolean,
            "closed": pl.Boolean,
            "liquidity": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "volume": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "raw_json": pl.Utf8,
        },
    )
)


class ParquetRecorder:
    """Buffer normalized events and write partitioned Parquet part files."""

    def __init__(
        self,
        *,
        data_dir: Path,
        buffer_size: int = DEFAULT_RECORDER_BUFFER_SIZE,
        flush_interval_seconds: float = 5.0,
        partition_date: Callable[[], date] | None = None,
        max_recorded_depth: int | None = DEFAULT_MAX_RECORDED_DEPTH,
    ) -> None:
        self._root = data_dir
        self._buffer_size = buffer_size
        self._flush_interval_seconds = flush_interval_seconds
        self._partition_date = partition_date or (lambda: datetime.now(UTC).date())
        self._buffers: dict[tuple[str, str], list[NormalizedEvent]] = {}
        self._last_flush_monotonic = time.monotonic()
        self._depth_filter = (
            None
            if max_recorded_depth is None or max_recorded_depth <= 0
            else _BookDepthWriteFilter(max_recorded_depth)
        )
        self._metadata_filter = _MetadataSnapshotWriteFilter()

    def record(self, event: NormalizedEvent) -> None:
        """Record one event and flush if size/time thresholds are met."""
        for writable_event in self._writable_events((event,)):
            self._buffer_event(writable_event)

    def record_many(self, events: Iterable[NormalizedEvent]) -> None:
        """Record many events."""
        for event in self._writable_events(events):
            self._buffer_event(event)

    def flush_all(self) -> None:
        """Flush all buffered streams."""
        for key in list(self._buffers):
            self.flush(key)

    def flush(self, key: tuple[str, str]) -> None:
        """Flush one buffered stream/market partition."""
        events = self._buffers.get(key)
        if not events:
            return
        stream, condition_id = key
        frame = _events_to_frame(stream, events)
        partition_dir = (
            self._root
            / "raw"
            / stream
            / f"date={self._partition_date().isoformat()}"
            / f"market={condition_id}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / f"part-{time.time_ns()}-{uuid.uuid4().hex}.parquet"
        frame.write_parquet(output_path)
        self._buffers[key] = []
        self._last_flush_monotonic = time.monotonic()

    def _should_flush(self, key: tuple[str, str]) -> bool:
        if len(self._buffers.get(key, [])) >= self._buffer_size:
            return True
        return (time.monotonic() - self._last_flush_monotonic) >= self._flush_interval_seconds

    def _buffer_event(self, event: NormalizedEvent) -> None:
        key = (_stream_for_event(event), _condition_for_event(event))
        self._buffers.setdefault(key, []).append(event)
        if self._should_flush(key):
            self.flush(key)

    def _writable_events(self, events: Iterable[NormalizedEvent]) -> tuple[NormalizedEvent, ...]:
        event_tuple = tuple(events)
        writable = (
            event_tuple if self._depth_filter is None else self._depth_filter.filter(event_tuple)
        )
        return self._metadata_filter.filter(writable)


class _MetadataSnapshotWriteFilter:
    """Suppress unchanged metadata snapshots within a recorder process."""

    def __init__(self) -> None:
        self._hash_by_condition: dict[str, str] = {}

    def filter(self, events: Sequence[NormalizedEvent]) -> tuple[NormalizedEvent, ...]:
        writable: list[NormalizedEvent] = []
        for event in events:
            if not isinstance(event, MetadataSnapshot):
                writable.append(event)
                continue
            condition_id = _condition_for_event(event)
            content_hash = _metadata_content_hash(event)
            if self._hash_by_condition.get(condition_id) == content_hash:
                continue
            self._hash_by_condition[condition_id] = content_hash
            writable.append(event)
        return tuple(writable)


class _BookDepthWriteFilter:
    """Project full book updates to a persisted top-N book stream."""

    def __init__(self, max_depth: int) -> None:
        self._max_depth = max_depth
        self._full: dict[str, dict[BookSide, dict[Decimal, Decimal]]] = {}
        self._persisted: dict[str, dict[BookSide, dict[Decimal, Decimal]]] = {}

    def filter(self, events: Sequence[NormalizedEvent]) -> tuple[NormalizedEvent, ...]:
        writable: list[NormalizedEvent] = []
        pending_snapshots: list[BookEvent] = []
        pending_key: tuple[str, int, str | None] | None = None
        for event in events:
            if isinstance(event, BookEvent) and event.event_type == BookEventType.SNAPSHOT:
                key = (event.asset_id, event.ts_local_ns, event.seq_or_hash)
                if pending_key is not None and key != pending_key:
                    writable.extend(self._flush_snapshot_group(pending_snapshots))
                    pending_snapshots = []
                pending_key = key
                pending_snapshots.append(event)
                continue
            if pending_snapshots:
                writable.extend(self._flush_snapshot_group(pending_snapshots))
                pending_snapshots = []
                pending_key = None
            if isinstance(event, BookEvent) and event.event_type == BookEventType.DELTA:
                writable.extend(self._filter_delta(event))
            else:
                writable.append(event)
        if pending_snapshots:
            writable.extend(self._flush_snapshot_group(pending_snapshots))
        return tuple(writable)

    def _flush_snapshot_group(self, events: Sequence[BookEvent]) -> list[BookEvent]:
        if not events:
            return []
        first = events[0]
        sides = self._full_book(first.asset_id)
        sides[BookSide.BID] = {}
        sides[BookSide.ASK] = {}
        event_by_level: dict[tuple[BookSide, Decimal], BookEvent] = {}
        for event in events:
            side_levels = sides[event.side]
            if event.size == Decimal("0"):
                side_levels.pop(event.price, None)
            else:
                side_levels[event.price] = event.size
                event_by_level[(event.side, event.price)] = event

        writable: list[BookEvent] = []
        persisted = self._persisted_book(first.asset_id)
        for side in (BookSide.BID, BookSide.ASK):
            top = self._top_levels(sides[side], side)
            persisted[side] = dict(top)
            for price, _size in top:
                source = event_by_level[(side, price)]
                writable.append(source)
        return writable

    def _filter_delta(self, event: BookEvent) -> list[BookEvent]:
        full_side = self._full_book(event.asset_id)[event.side]
        persisted_side = self._persisted_book(event.asset_id)[event.side]
        before = dict(persisted_side)
        if event.size == Decimal("0"):
            full_side.pop(event.price, None)
        else:
            full_side[event.price] = event.size
        after = dict(self._top_levels(full_side, event.side))
        persisted_side.clear()
        persisted_side.update(after)
        return [
            self._copy_book_event(event, price=price, size=size)
            for price, size in self._top_delta_changes(before, after, event.side)
        ]

    def _top_delta_changes(
        self,
        before: dict[Decimal, Decimal],
        after: dict[Decimal, Decimal],
        side: BookSide,
    ) -> tuple[tuple[Decimal, Decimal], ...]:
        prices = set(before) | set(after)
        ordered_prices = sorted(prices, reverse=side == BookSide.BID)
        changes: list[tuple[Decimal, Decimal]] = []
        for price in ordered_prices:
            before_size = before.get(price, Decimal("0"))
            after_size = after.get(price, Decimal("0"))
            if before_size != after_size:
                changes.append((price, after_size))
        return tuple(changes)

    def _full_book(self, asset_id: str) -> dict[BookSide, dict[Decimal, Decimal]]:
        return self._full.setdefault(asset_id, {BookSide.BID: {}, BookSide.ASK: {}})

    def _persisted_book(self, asset_id: str) -> dict[BookSide, dict[Decimal, Decimal]]:
        return self._persisted.setdefault(asset_id, {BookSide.BID: {}, BookSide.ASK: {}})

    def _top_levels(
        self,
        levels: dict[Decimal, Decimal],
        side: BookSide,
    ) -> tuple[tuple[Decimal, Decimal], ...]:
        return tuple(
            (price, size)
            for price, size in sorted(levels.items(), reverse=side == BookSide.BID)[
                : self._max_depth
            ]
        )

    @staticmethod
    def _copy_book_event(event: BookEvent, *, price: Decimal, size: Decimal) -> BookEvent:
        return BookEvent(
            ts_local_ns=event.ts_local_ns,
            ts_monotonic_ns=event.ts_monotonic_ns,
            ts_exchange=event.ts_exchange,
            condition_id=event.condition_id,
            asset_id=event.asset_id,
            event_type=event.event_type,
            side=event.side,
            price=price,
            size=size,
            seq_or_hash=event.seq_or_hash,
        )


def _events_to_frame(stream: str, events: Sequence[NormalizedEvent]) -> pl.DataFrame:
    if stream == "book":
        return pl.DataFrame([_book_row(event) for event in _only(events, BookEvent)]).cast(
            BOOK_SCHEMA
        )
    if stream == "trade":
        return pl.DataFrame([_trade_row(event) for event in _only(events, TradeEvent)]).cast(
            TRADE_SCHEMA
        )
    if stream == "gap-log":
        return pl.DataFrame([_gap_row(event) for event in _only(events, GapEvent)]).cast(GAP_SCHEMA)
    if stream == "metadata":
        return pl.DataFrame(
            [_metadata_row(event) for event in _only(events, MetadataSnapshot)]
        ).cast(METADATA_SCHEMA)
    raise ValueError(f"unknown stream: {stream}")


def _only(events: Sequence[NormalizedEvent], event_type: type[T]) -> list[T]:
    return [event for event in events if isinstance(event, event_type)]


def _book_row(event: BookEvent) -> dict[str, object]:
    return {
        "ts_local_ns": event.ts_local_ns,
        "ts_monotonic_ns": event.ts_monotonic_ns,
        "ts_exchange": event.ts_exchange,
        "condition_id": event.condition_id,
        "asset_id": event.asset_id,
        "event_type": event.event_type.value,
        "side": event.side.value,
        "price": _parquet_decimal(event.price),
        "size": _parquet_decimal(event.size),
        "seq_or_hash": event.seq_or_hash,
    }


def _trade_row(event: TradeEvent) -> dict[str, object]:
    row = asdict(event)
    row["price"] = _parquet_decimal(event.price)
    row["size"] = _parquet_decimal(event.size)
    return row


def _gap_row(event: GapEvent) -> dict[str, object]:
    row = asdict(event)
    row["reason"] = event.reason.value
    return row


def _metadata_row(event: MetadataSnapshot) -> dict[str, object]:
    return {
        "fetched_at": event.fetched_at,
        "condition_id": event.condition_id,
        "question": event.question,
        "outcomes": json.dumps(event.outcomes),
        "token_ids": json.dumps(event.token_ids),
        "resolution_or_end_time": event.resolution_or_end_time,
        "active": event.active,
        "closed": event.closed,
        "liquidity": _optional_parquet_decimal(event.liquidity),
        "volume": _optional_parquet_decimal(event.volume),
        "raw_json": _trim_metadata_raw_json(event.raw_json),
    }


def _optional_parquet_decimal(value: Decimal | None) -> Decimal | None:
    return None if value is None else _parquet_decimal(value)


def _parquet_decimal(value: Decimal) -> Decimal:
    """Clamp normalized Decimal values to the persisted Decimal(38, 12) schema."""
    clamped = min(max(value, PARQUET_DECIMAL_MIN), PARQUET_DECIMAL_MAX)
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION + 8
        return clamped.quantize(PARQUET_DECIMAL_QUANTUM, rounding=ROUND_HALF_UP)


def _metadata_content_hash(event: MetadataSnapshot) -> str:
    row = _metadata_row(event)
    row.pop("fetched_at", None)
    payload = json.dumps(row, default=str, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _trim_metadata_raw_json(raw_json: str) -> str:
    """Persist only metadata keys needed for historical pair reconstruction."""
    try:
        decoded = json.loads(raw_json)
    except json.JSONDecodeError:
        return ""
    if not isinstance(decoded, dict):
        return ""
    compact = {
        key: value
        for key, value in decoded.items()
        if key in METADATA_RAW_JSON_KEYS and value is not None
    }
    return json.dumps(compact, default=str, separators=(",", ":"), sort_keys=True)


def _stream_for_event(event: NormalizedEvent) -> str:
    if isinstance(event, BookEvent):
        return "book"
    if isinstance(event, TradeEvent):
        return "trade"
    if isinstance(event, GapEvent):
        return "gap-log"
    if isinstance(event, MetadataSnapshot):
        return "metadata"
    raise TypeError(f"unsupported event type: {type(event).__name__}")


def _condition_for_event(event: NormalizedEvent) -> str:
    condition_id = event.condition_id
    return condition_id if condition_id else "unknown"
