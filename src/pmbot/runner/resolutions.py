"""Resolution polling for tracked Polymarket/Kalshi pairs."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from pmbot.ingestion.gamma import GammaClient
from pmbot.ingestion.kalshi import KalshiMetadataClient
from pmbot.runner.models import ResolutionRecord
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import ResolutionParquetStore
from pmbot.signals.contracts import VerifiedContractPair


class ResolutionSource(Protocol):
    """Read-only source for resolved outcomes."""

    async def fetch(
        self,
        pairs: tuple[VerifiedContractPair, ...],
    ) -> tuple[ResolutionRecord, ...]:
        """Fetch resolved outcomes for tracked pairs."""


class ResolutionPoller:
    """Poll resolution sources and persist idempotently."""

    def __init__(
        self,
        *,
        pairs: tuple[VerifiedContractPair, ...] | Callable[[], tuple[VerifiedContractPair, ...]],
        sources: tuple[ResolutionSource, ...],
        store: ResolutionParquetStore,
        state: CollectorStateStore | None = None,
        on_resolved: Callable[[tuple[ResolutionRecord, ...]], None] | None = None,
    ) -> None:
        self._pairs = pairs
        self._sources = sources
        self._store = store
        self._state = state
        self._on_resolved = on_resolved

    async def poll_once(self) -> int:
        """Run one resolution poll and return newly written rows."""
        fetched: list[ResolutionRecord] = []
        pairs = self._pairs() if callable(self._pairs) else self._pairs
        for source in self._sources:
            fetched.extend(await source.fetch(pairs))
        if fetched and self._on_resolved is not None:
            self._on_resolved(tuple(fetched))
        written = self._store.record_many(fetched)
        if self._state is not None:
            self._state.note_resolutions(written)
        return written

    async def run(self, *, interval_seconds: float = 900.0) -> None:
        """Poll forever at a fixed interval."""
        while True:
            await self.poll_once()
            await asyncio.sleep(interval_seconds)


class GammaResolutionSource:
    """Fetch resolved Polymarket outcomes from Gamma metadata."""

    def __init__(self, client: GammaClient) -> None:
        self._client = client

    async def fetch(
        self,
        pairs: tuple[VerifiedContractPair, ...],
    ) -> tuple[ResolutionRecord, ...]:
        wanted = {pair.polymarket_condition_id for pair in pairs}
        raw_markets = await self._client.fetch_markets(closed=True, limit=500)
        fetched_at = _now()
        records: list[ResolutionRecord] = []
        for raw in raw_markets:
            condition_id = _optional_str(
                raw.get("conditionId", raw.get("condition_id", raw.get("market")))
            )
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


class KalshiResolutionSource:
    """Fetch resolved Kalshi outcomes from market metadata."""

    def __init__(self, client: KalshiMetadataClient) -> None:
        self._client = client

    async def fetch(
        self,
        pairs: tuple[VerifiedContractPair, ...],
    ) -> tuple[ResolutionRecord, ...]:
        fetched_at = _now()
        records: list[ResolutionRecord] = []
        for pair in pairs:
            snapshot = await self._client.fetch_market(pair.kalshi_ticker)
            raw = json.loads(snapshot.raw_json, parse_float=Decimal)
            if not isinstance(raw, Mapping):
                continue
            outcome = _resolved_outcome(raw)
            if outcome is None:
                continue
            records.append(
                ResolutionRecord(
                    venue="kalshi",
                    market_id=pair.kalshi_ticker,
                    resolved_outcome=outcome,
                    resolved_at=_resolved_at(raw) or fetched_at,
                    fetched_at=fetched_at,
                    raw_json=snapshot.raw_json,
                )
            )
        return tuple(records)


class StaticResolutionSource:
    """Fixture source for tests."""

    def __init__(self, records: tuple[ResolutionRecord, ...]) -> None:
        self._records = records

    async def fetch(
        self,
        pairs: tuple[VerifiedContractPair, ...],
    ) -> tuple[ResolutionRecord, ...]:
        del pairs
        return self._records


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
            if text in {"yes", "true", "1"}:
                return "yes"
            return "no"
        return text
    return None


def _resolved_at(raw: Mapping[str, object]) -> str | None:
    for key in (
        "resolved_at",
        "resolvedAt",
        "resolutionTime",
        "settlementTime",
        "expiration_time",
        "close_time",
    ):
        value = _optional_str(raw.get(key))
        if value is not None:
            return value
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _now() -> str:
    return datetime.now(UTC).isoformat()
