from datetime import date
from decimal import Decimal

import polars as pl
import pytest
from typer.testing import CliRunner

from pmbot.cli import app
from pmbot.ingestion import compaction
from pmbot.ingestion.compaction import compact_partition, compact_raw_data
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide


def test_compaction_preserves_every_row_and_is_idempotent(tmp_path) -> None:
    partition = write_book_parts(tmp_path, date(2026, 6, 24), count=3)
    before = read_partition(partition)

    summary = compact_raw_data(tmp_path, active_date=date(2026, 6, 25))
    after = read_partition(partition)
    rerun = compact_raw_data(tmp_path, active_date=date(2026, 6, 25))

    assert summary.partitions_compacted == 1
    assert summary.files_removed == 3
    assert list(path.name for path in partition.glob("*.parquet")) == ["compact.parquet"]
    assert after.sort(after.columns).equals(before.sort(before.columns))
    assert rerun.partitions_compacted == 0
    assert rerun.files_removed == 0


def test_compaction_never_compacts_active_partition(tmp_path) -> None:
    partition = write_book_parts(tmp_path, date(2026, 6, 25), count=2)

    summary = compact_raw_data(tmp_path, active_date=date(2026, 6, 25))

    assert summary.partitions_skipped_active == 1
    assert summary.partitions_compacted == 0
    assert len(list(partition.glob("*.parquet"))) == 2


def test_compaction_verification_failure_leaves_originals(tmp_path, monkeypatch) -> None:
    partition = write_book_parts(tmp_path, date(2026, 6, 24), count=2)
    originals = sorted(partition.glob("*.parquet"))
    original_row_count = compaction._row_count

    def failing_row_count(path) -> int:
        if path.name.startswith(".compact"):
            return -1
        return original_row_count(path)

    monkeypatch.setattr(compaction, "_row_count", failing_row_count)

    with pytest.raises(RuntimeError, match="row-count verification failed"):
        compact_partition(partition, active_date=date(2026, 6, 25))

    assert sorted(partition.glob("*.parquet")) == originals
    assert not list(partition.glob(".*.tmp"))


def test_manual_compact_cli_runs_once(tmp_path) -> None:
    partition = write_book_parts(tmp_path, date(2026, 6, 24), count=2)
    runner = CliRunner()

    result = runner.invoke(app, ["compact", "--data-dir", str(tmp_path)])

    assert result.exit_code == 0
    assert "compacted=1" in result.output
    assert list(path.name for path in partition.glob("*.parquet")) == ["compact.parquet"]


def write_book_parts(tmp_path, partition_date: date, *, count: int):
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=1,
        flush_interval_seconds=999,
        partition_date=lambda: partition_date,
        max_recorded_depth=None,
    )
    for index in range(count):
        recorder.record(
            BookEvent(
                ts_local_ns=100 + index,
                ts_monotonic_ns=200 + index,
                ts_exchange=str(300 + index),
                condition_id="condition-1",
                asset_id="asset-yes",
                event_type=BookEventType.DELTA,
                side=BookSide.BID,
                price=Decimal(f"0.{50 + index}"),
                size=Decimal(str(10 + index)),
                seq_or_hash=f"seq-{index}",
            )
        )
    recorder.flush_all()
    return tmp_path / "raw" / "book" / f"date={partition_date.isoformat()}" / "market=condition-1"


def read_partition(partition) -> pl.DataFrame:
    return pl.read_parquet(sorted(partition.glob("*.parquet")))
