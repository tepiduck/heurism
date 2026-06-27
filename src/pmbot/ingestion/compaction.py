"""Safe Parquet partition compaction for recorded raw market data."""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

RAW_STREAMS = ("book", "trade", "gap-log", "metadata")
COMPACTED_FILENAME = "compact.parquet"


@dataclass(frozen=True, slots=True)
class CompactionSummary:
    """Summary of one compaction pass."""

    partitions_seen: int = 0
    partitions_skipped_active: int = 0
    partitions_compacted: int = 0
    files_removed: int = 0
    rows_written: int = 0

    def merge(self, other: CompactionSummary) -> CompactionSummary:
        """Return the element-wise sum of two summaries."""
        return CompactionSummary(
            partitions_seen=self.partitions_seen + other.partitions_seen,
            partitions_skipped_active=(
                self.partitions_skipped_active + other.partitions_skipped_active
            ),
            partitions_compacted=self.partitions_compacted + other.partitions_compacted,
            files_removed=self.files_removed + other.files_removed,
            rows_written=self.rows_written + other.rows_written,
        )


def compact_raw_data(
    data_dir: Path,
    *,
    active_date: date | None = None,
    streams: Sequence[str] = RAW_STREAMS,
) -> CompactionSummary:
    """Compact closed raw market-data partitions under ``data_dir``."""
    raw_root = data_dir / "raw"
    if not raw_root.exists():
        return CompactionSummary()
    current_date = active_date or datetime.now(UTC).date()
    summary = CompactionSummary()
    for stream in streams:
        stream_root = raw_root / stream
        if not stream_root.exists():
            continue
        for partition_dir in _market_partition_dirs(stream_root):
            summary = summary.merge(_compact_partition(partition_dir, active_date=current_date))
    return summary


def compact_partition(
    partition_dir: Path,
    *,
    active_date: date | None = None,
) -> CompactionSummary:
    """Compact one ``date=.../market=...`` partition if it is closed."""
    return _compact_partition(
        partition_dir,
        active_date=active_date or datetime.now(UTC).date(),
    )


def _compact_partition(partition_dir: Path, *, active_date: date) -> CompactionSummary:
    partition_date = _date_partition(partition_dir)
    if partition_date is None:
        return CompactionSummary()
    if partition_date >= active_date:
        return CompactionSummary(partitions_seen=1, partitions_skipped_active=1)

    source_files = _source_files(partition_dir)
    if len(source_files) <= 1:
        return CompactionSummary(partitions_seen=1)

    schemas = [_schema(path) for path in source_files]
    first_schema = schemas[0]
    if any(schema != first_schema for schema in schemas[1:]):
        raise ValueError(f"cannot compact partition with mixed schemas: {partition_dir}")

    source_rows = sum(_row_count(path) for path in source_files)
    final_path = partition_dir / COMPACTED_FILENAME
    tmp_path = partition_dir / f".{COMPACTED_FILENAME}.{time.time_ns()}.tmp"
    try:
        pl.scan_parquet([str(path) for path in source_files]).sink_parquet(
            tmp_path,
            compression="zstd",
        )
        if _row_count(tmp_path) != source_rows:
            raise RuntimeError(f"compaction row-count verification failed for {partition_dir}")
        if _schema(tmp_path) != first_schema:
            raise RuntimeError(f"compaction schema verification failed for {partition_dir}")
        tmp_path.replace(final_path)
        removed = 0
        for path in source_files:
            if path == final_path:
                continue
            path.unlink()
            removed += 1
        return CompactionSummary(
            partitions_seen=1,
            partitions_compacted=1,
            files_removed=removed,
            rows_written=source_rows,
        )
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _market_partition_dirs(stream_root: Path) -> Iterable[Path]:
    yield from sorted(
        path
        for path in stream_root.glob("date=*/market=*")
        if path.is_dir()
    )


def _source_files(partition_dir: Path) -> tuple[Path, ...]:
    return tuple(
        sorted(
            path
            for path in partition_dir.glob("*.parquet")
            if not path.name.startswith(".")
        )
    )


def _date_partition(path: Path) -> date | None:
    for part in path.parts:
        if not part.startswith("date="):
            continue
        try:
            return date.fromisoformat(part.removeprefix("date="))
        except ValueError:
            return None
    return None


def _row_count(path: Path) -> int:
    return int(pl.scan_parquet(str(path)).select(pl.len()).collect().item())


def _schema(path: Path) -> pl.Schema:
    return pl.read_parquet(path, n_rows=0).schema
