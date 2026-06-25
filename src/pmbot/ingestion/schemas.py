"""Stable normalized market-data schemas.

The ingestion boundary preserves raw venue messages separately from the internal
schemas below so future venues can be adapted without changing downstream
feature, backtest, or risk code.

Book event stream: ``data/raw/book/date=YYYY-MM-DD/market={condition_id}/``
    ts_local_ns: int64 UTC wall-clock capture timestamp from ``time.time_ns()``
    ts_monotonic_ns: nullable int64 from ``time.monotonic_ns()`` for
        intra-session ordering only
    ts_exchange: nullable string venue timestamp, when supplied
    condition_id: string Polymarket condition ID
    asset_id: string CLOB token/asset ID
    event_type: string enum, one of ``snapshot`` or ``delta``
    side: string enum, one of ``bid`` or ``ask``
    price: Decimal(38, 12), never float
    size: Decimal(38, 12), never float
    seq_or_hash: nullable string sequence/checksum/hash supplied by the venue

Trade event stream: ``data/raw/trade/date=YYYY-MM-DD/market={condition_id}/``
    ts_local_ns: int64 UTC wall-clock capture timestamp from ``time.time_ns()``
    ts_monotonic_ns: nullable int64 from ``time.monotonic_ns()`` for
        intra-session ordering only
    ts_exchange: nullable string venue timestamp, when supplied
    condition_id: string Polymarket condition ID
    asset_id: string CLOB token/asset ID
    price: Decimal(38, 12), never float
    size: Decimal(38, 12), never float
    taker_side: nullable string, usually ``buy`` or ``sell`` when supplied
    trade_id: string venue ID or deterministic content hash when no venue ID exists

Gap-log stream: ``data/raw/gap-log/date=YYYY-MM-DD/market={condition_id}/``
    ts_local_ns: int64 UTC wall-clock capture timestamp from ``time.time_ns()``
    ts_monotonic_ns: nullable int64 from ``time.monotonic_ns()`` for
        intra-session ordering only
    ts_exchange: nullable string venue timestamp, when supplied
    condition_id: string market condition ID, or ``unknown`` when unavailable
    asset_id: nullable string CLOB token/asset ID
    reason: string enum describing the discontinuity
    details: string human-readable context for operators/backtests
    seq_or_hash: nullable string sequence/checksum/hash associated with the gap

Gamma metadata stream:
``data/raw/metadata/date=YYYY-MM-DD/market={condition_id}/``
    fetched_at: ISO-8601 UTC timestamp for the Gamma snapshot
    condition_id, question, outcomes, token_ids, resolution_or_end_time,
    active, closed, liquidity, volume, raw_json
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import TypeAlias

DECIMAL_PRECISION = 38
DECIMAL_SCALE = 12


class BookEventType(StrEnum):
    """Normalized order-book event types."""

    SNAPSHOT = "snapshot"
    DELTA = "delta"


class BookSide(StrEnum):
    """Normalized order-book sides."""

    BID = "bid"
    ASK = "ask"


class GapReason(StrEnum):
    """Reasons a recorded span should be treated as discontinuous."""

    RECONNECT = "reconnect"
    CONNECTION_LOST = "connection_lost"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    SEQUENCE_GAP = "sequence_gap"
    MISSING_SNAPSHOT = "missing_snapshot"


@dataclass(frozen=True, slots=True)
class BookEvent:
    """A single normalized L2 book price level."""

    ts_local_ns: int
    ts_monotonic_ns: int | None
    ts_exchange: str | None
    condition_id: str
    asset_id: str
    event_type: BookEventType
    side: BookSide
    price: Decimal
    size: Decimal
    seq_or_hash: str | None


@dataclass(frozen=True, slots=True)
class TradeEvent:
    """A single normalized public trade print."""

    ts_local_ns: int
    ts_monotonic_ns: int | None
    ts_exchange: str | None
    condition_id: str
    asset_id: str
    price: Decimal
    size: Decimal
    taker_side: str | None
    trade_id: str


@dataclass(frozen=True, slots=True)
class GapEvent:
    """A discontinuity marker for spans the backtester should distrust."""

    ts_local_ns: int
    ts_monotonic_ns: int | None
    ts_exchange: str | None
    condition_id: str
    asset_id: str | None
    reason: GapReason
    details: str
    seq_or_hash: str | None


@dataclass(frozen=True, slots=True)
class MetadataSnapshot:
    """A normalized Gamma market metadata snapshot."""

    fetched_at: str
    condition_id: str
    question: str | None
    outcomes: tuple[str, ...]
    token_ids: tuple[str, ...]
    resolution_or_end_time: str | None
    active: bool | None
    closed: bool | None
    liquidity: Decimal | None
    volume: Decimal | None
    raw_json: str


NormalizedEvent: TypeAlias = BookEvent | TradeEvent | GapEvent | MetadataSnapshot
