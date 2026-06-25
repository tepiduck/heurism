"""Runner-owned records for collector status, signals, and resolutions."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class SignalRecord:
    """Persisted point-in-time cross-venue forecast."""

    ts_local_ns: int
    polymarket_condition_id: str
    polymarket_asset_id: str
    kalshi_ticker: str
    p_hat: Decimal
    polymarket_probability: Decimal
    kalshi_probability: Decimal
    signal_value: Decimal
    direction: str
    confidence: Decimal
    rationale: str


@dataclass(frozen=True, slots=True)
class ResolutionRecord:
    """Persisted resolved binary market outcome."""

    venue: str
    market_id: str
    resolved_outcome: str
    resolved_at: str
    fetched_at: str
    raw_json: str | None = None


@dataclass(frozen=True, slots=True)
class VenueStatus:
    """Operator-facing venue health snapshot."""

    venue: str
    last_frame_ns: int | None
    frames_total: int
    reconnects: int
    gap_timestamps_ns: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CollectorStatus:
    """Operator-facing collector state."""

    started_at_ns: int
    updated_at_ns: int
    pairs_tracked: int
    resolutions_collected: int
    venues: tuple[VenueStatus, ...]
