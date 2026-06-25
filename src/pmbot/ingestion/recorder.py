"""Buffered Parquet recorder for normalized ingestion events."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, TypeVar, cast

import polars as pl

from pmbot.ingestion.schemas import (
    DECIMAL_PRECISION,
    DECIMAL_SCALE,
    BookEvent,
    GapEvent,
    MetadataSnapshot,
    NormalizedEvent,
    TradeEvent,
)

T = TypeVar("T")


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
        buffer_size: int = 1_000,
        flush_interval_seconds: float = 5.0,
        partition_date: Callable[[], date] | None = None,
    ) -> None:
        self._root = data_dir
        self._buffer_size = buffer_size
        self._flush_interval_seconds = flush_interval_seconds
        self._partition_date = partition_date or (lambda: datetime.now(UTC).date())
        self._buffers: dict[tuple[str, str], list[NormalizedEvent]] = {}
        self._last_flush_monotonic = time.monotonic()

    def record(self, event: NormalizedEvent) -> None:
        """Record one event and flush if size/time thresholds are met."""
        key = (_stream_for_event(event), _condition_for_event(event))
        self._buffers.setdefault(key, []).append(event)
        if self._should_flush(key):
            self.flush(key)

    def record_many(self, events: Iterable[NormalizedEvent]) -> None:
        """Record many events."""
        for event in events:
            self.record(event)

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
        "price": event.price,
        "size": event.size,
        "seq_or_hash": event.seq_or_hash,
    }


def _trade_row(event: TradeEvent) -> dict[str, object]:
    return asdict(event)


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
        "liquidity": event.liquidity,
        "volume": event.volume,
        "raw_json": event.raw_json,
    }


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
