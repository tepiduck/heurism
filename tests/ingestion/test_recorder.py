from datetime import date
from decimal import Decimal

import polars as pl

from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide


def test_recorder_round_trip_preserves_decimal_schema_and_values(tmp_path) -> None:
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
        partition_date=lambda: date(2026, 6, 24),
    )
    event = BookEvent(
        ts_local_ns=123,
        ts_monotonic_ns=456,
        ts_exchange="1757908892351",
        condition_id="condition-1",
        asset_id="asset-yes",
        event_type=BookEventType.SNAPSHOT,
        side=BookSide.BID,
        price=Decimal("0.456"),
        size=Decimal("219.217767"),
        seq_or_hash="hash-1",
    )

    recorder.record(event)
    recorder.flush_all()

    files = list(
        (tmp_path / "raw" / "book" / "date=2026-06-24" / "market=condition-1").glob(
            "*.parquet"
        )
    )
    assert len(files) == 1
    frame = pl.read_parquet(files[0])

    assert frame.schema["price"] == pl.Decimal(38, 12)
    assert frame.schema["size"] == pl.Decimal(38, 12)
    assert frame["ts_monotonic_ns"][0] == 456
    assert frame["price"][0] == Decimal("0.456")
    assert frame["size"][0] == Decimal("219.217767")
