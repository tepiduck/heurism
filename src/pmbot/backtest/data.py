"""Load normalized WP-1 Parquet into one deterministic replay stream."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from pmbot.ingestion.schemas import (
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    GapReason,
    TradeEvent,
)

ReplayEvent = BookEvent | TradeEvent | GapEvent


def load_replay_events(data_dir: Path) -> tuple[ReplayEvent, ...]:
    """Read book, trade, and gap-log Parquet and merge by wall-clock timestamp."""
    events: list[ReplayEvent] = []
    events.extend(_load_book_events(data_dir / "raw" / "book"))
    events.extend(_load_trade_events(data_dir / "raw" / "trade"))
    events.extend(_load_gap_events(data_dir / "raw" / "gap-log"))
    return tuple(sorted(events, key=_sort_key))


def merge_events(events: Iterable[ReplayEvent]) -> tuple[ReplayEvent, ...]:
    """Merge in-memory replay events using the same deterministic ordering."""
    return tuple(sorted(events, key=_sort_key))


def _load_book_events(root: Path) -> list[BookEvent]:
    rows = _read_rows(root)
    return [
        BookEvent(
            ts_local_ns=int(row["ts_local_ns"]),
            ts_monotonic_ns=_optional_int(row.get("ts_monotonic_ns")),
            ts_exchange=_optional_str(row.get("ts_exchange")),
            condition_id=str(row["condition_id"]),
            asset_id=str(row["asset_id"]),
            event_type=BookEventType(str(row["event_type"])),
            side=BookSide(str(row["side"])),
            price=_decimal(row["price"]),
            size=_decimal(row["size"]),
            seq_or_hash=_optional_str(row.get("seq_or_hash")),
        )
        for row in rows
    ]


def _load_trade_events(root: Path) -> list[TradeEvent]:
    rows = _read_rows(root)
    return [
        TradeEvent(
            ts_local_ns=int(row["ts_local_ns"]),
            ts_monotonic_ns=_optional_int(row.get("ts_monotonic_ns")),
            ts_exchange=_optional_str(row.get("ts_exchange")),
            condition_id=str(row["condition_id"]),
            asset_id=str(row["asset_id"]),
            price=_decimal(row["price"]),
            size=_decimal(row["size"]),
            taker_side=_optional_str(row.get("taker_side")),
            trade_id=str(row["trade_id"]),
        )
        for row in rows
    ]


def _load_gap_events(root: Path) -> list[GapEvent]:
    rows = _read_rows(root)
    return [
        GapEvent(
            ts_local_ns=int(row["ts_local_ns"]),
            ts_monotonic_ns=_optional_int(row.get("ts_monotonic_ns")),
            ts_exchange=_optional_str(row.get("ts_exchange")),
            condition_id=str(row["condition_id"]),
            asset_id=_optional_str(row.get("asset_id")),
            reason=GapReason(str(row["reason"])),
            details=str(row["details"]),
            seq_or_hash=_optional_str(row.get("seq_or_hash")),
        )
        for row in rows
    ]


def _read_rows(root: Path) -> list[dict[str, Any]]:
    files = sorted(root.glob("**/*.parquet")) if root.exists() else []
    rows: list[dict[str, Any]] = []
    for file in files:
        rows.extend(pl.read_parquet(file).to_dicts())
    return rows


def _sort_key(event: ReplayEvent) -> tuple[int, int, str, str]:
    if isinstance(event, GapEvent):
        priority = 0
        asset_id = event.asset_id or ""
    elif isinstance(event, BookEvent):
        priority = 1
        asset_id = event.asset_id
    else:
        priority = 2
        asset_id = event.asset_id
    return (event.ts_local_ns, priority, asset_id, event.__class__.__name__)


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    if isinstance(value, Decimal):
        return int(value)
    raise TypeError(f"expected integer-compatible value, got {type(value).__name__}")


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
