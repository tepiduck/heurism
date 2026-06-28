"""Parquet streams owned by the unattended collector."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import polars as pl

from pmbot.ingestion.parquet_normalization import normalize_rows_for_schema
from pmbot.ingestion.schemas import DECIMAL_PRECISION, DECIMAL_SCALE
from pmbot.runner.models import ResolutionRecord, SignalRecord

if TYPE_CHECKING:
    from pmbot.signals.cross_venue import SignalValue

SIGNAL_SCHEMA: pl.Schema = pl.Schema(
    cast(
        Any,
        {
            "ts_local_ns": pl.Int64,
            "polymarket_condition_id": pl.Utf8,
            "polymarket_asset_id": pl.Utf8,
            "kalshi_ticker": pl.Utf8,
            "p_hat": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "polymarket_probability": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "kalshi_probability": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "signal_value": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "direction": pl.Utf8,
            "confidence": pl.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            "rationale": pl.Utf8,
        },
    )
)

RESOLUTION_SCHEMA: pl.Schema = pl.Schema(
    cast(
        Any,
        {
            "venue": pl.Utf8,
            "market_id": pl.Utf8,
            "resolved_outcome": pl.Utf8,
            "resolved_at": pl.Utf8,
            "fetched_at": pl.Utf8,
            "raw_json": pl.Utf8,
        },
    )
)


class SignalParquetWriter:
    """Buffered writer for signal forecast history."""

    def __init__(
        self,
        *,
        data_dir: Path,
        buffer_size: int = 10_000,
        flush_interval_seconds: float = 60.0,
        partition_date: Callable[[], date] | None = None,
    ) -> None:
        self._root = data_dir
        self._buffer_size = buffer_size
        self._flush_interval_seconds = flush_interval_seconds
        self._partition_date = partition_date or (lambda: datetime.now(UTC).date())
        self._buffers: dict[str, list[SignalRecord]] = {}
        self._last_flush_monotonic = time.monotonic()

    def record_value(self, value: SignalValue) -> None:
        """Record one computed signal value."""
        self.record(
            SignalRecord(
                ts_local_ns=value.ts_local_ns,
                polymarket_condition_id=value.polymarket_condition_id,
                polymarket_asset_id=value.polymarket_asset_id,
                kalshi_ticker=value.kalshi_ticker,
                p_hat=value.fair_probability,
                polymarket_probability=value.polymarket_probability,
                kalshi_probability=value.kalshi_probability,
                signal_value=value.signal_value,
                direction=value.direction.value,
                confidence=value.confidence,
                rationale=value.rationale,
            )
        )

    def record(self, record: SignalRecord) -> None:
        """Record one signal row and flush on size/time."""
        self._buffers.setdefault(record.polymarket_condition_id, []).append(record)
        if self._should_flush(record.polymarket_condition_id):
            self.flush(record.polymarket_condition_id)

    def flush_all(self) -> None:
        """Flush all buffered signal partitions."""
        for condition_id in list(self._buffers):
            self.flush(condition_id)

    def flush(self, condition_id: str) -> None:
        """Flush one market partition."""
        records = self._buffers.get(condition_id)
        if not records:
            return
        partition_dir = (
            self._root
            / "signals"
            / f"date={self._partition_date().isoformat()}"
            / f"market={condition_id}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / f"part-{time.time_ns()}-{uuid.uuid4().hex}.parquet"
        rows = normalize_rows_for_schema([asdict(record) for record in records], SIGNAL_SCHEMA)
        pl.DataFrame(rows).cast(SIGNAL_SCHEMA).write_parquet(output_path)
        self._buffers[condition_id] = []
        self._last_flush_monotonic = time.monotonic()

    def _should_flush(self, condition_id: str) -> bool:
        if len(self._buffers.get(condition_id, [])) >= self._buffer_size:
            return True
        return (time.monotonic() - self._last_flush_monotonic) >= self._flush_interval_seconds


class ResolutionParquetStore:
    """Idempotent writer for resolved outcomes."""

    def __init__(
        self,
        *,
        data_dir: Path,
        partition_date: Callable[[], date] | None = None,
    ) -> None:
        self._root = data_dir
        self._partition_date = partition_date or (lambda: datetime.now(UTC).date())
        self._seen = self._load_seen()

    @property
    def count(self) -> int:
        """Return distinct resolution rows known to the store."""
        return len(self._seen)

    def record_many(self, records: Iterable[ResolutionRecord]) -> int:
        """Write only unseen resolution records and return the number written."""
        unseen: list[ResolutionRecord] = []
        for record in records:
            key = self._key(record)
            if key in self._seen:
                continue
            self._seen.add(key)
            unseen.append(record)
        if not unseen:
            return 0
        by_market: dict[str, list[ResolutionRecord]] = {}
        for record in unseen:
            by_market.setdefault(record.market_id, []).append(record)
        for market_id, market_records in by_market.items():
            self._write_partition(market_id, market_records)
        return len(unseen)

    def _write_partition(self, market_id: str, records: Sequence[ResolutionRecord]) -> None:
        partition_dir = (
            self._root
            / "resolutions"
            / f"date={self._partition_date().isoformat()}"
            / f"market={market_id}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / f"part-{time.time_ns()}-{uuid.uuid4().hex}.parquet"
        rows = normalize_rows_for_schema([asdict(record) for record in records], RESOLUTION_SCHEMA)
        pl.DataFrame(rows).cast(RESOLUTION_SCHEMA).write_parquet(output_path)

    def _load_seen(self) -> set[tuple[str, str, str]]:
        root = self._root / "resolutions"
        if not root.exists():
            return set()
        seen: set[tuple[str, str, str]] = set()
        for path in root.glob("date=*/market=*/*.parquet"):
            frame = pl.read_parquet(path)
            for row in frame.iter_rows(named=True):
                seen.add(
                    (
                        str(row["venue"]),
                        str(row["market_id"]),
                        str(row["resolved_outcome"]),
                    )
                )
        return seen

    @staticmethod
    def _key(record: ResolutionRecord) -> tuple[str, str, str]:
        return record.venue, record.market_id, record.resolved_outcome


def resolution_record_from_mapping(row: dict[str, object]) -> ResolutionRecord:
    """Create a resolution record from a fixture or decoded JSON row."""
    return ResolutionRecord(
        venue=str(row["venue"]),
        market_id=str(row["market_id"]),
        resolved_outcome=str(row["resolved_outcome"]),
        resolved_at=str(row["resolved_at"]),
        fetched_at=str(row["fetched_at"]),
        raw_json=json.dumps(row.get("raw_json")) if row.get("raw_json") is not None else None,
    )
