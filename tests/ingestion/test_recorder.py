import json
from datetime import date
from decimal import Decimal

import polars as pl

from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide, MetadataSnapshot


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


def test_recorder_caps_book_depth_and_emits_boundary_deltas(tmp_path) -> None:
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
        partition_date=lambda: date(2026, 6, 24),
        max_recorded_depth=2,
    )
    recorder.record_many(
        [
            book_event("asset-yes", BookSide.BID, "0.50", "10", seq="snapshot"),
            book_event("asset-yes", BookSide.BID, "0.49", "20", seq="snapshot"),
            book_event("asset-yes", BookSide.BID, "0.48", "30", seq="snapshot"),
            book_event("asset-yes", BookSide.ASK, "0.51", "10", seq="snapshot"),
            book_event("asset-yes", BookSide.ASK, "0.52", "20", seq="snapshot"),
            book_event("asset-yes", BookSide.ASK, "0.53", "30", seq="snapshot"),
        ]
    )
    recorder.record(
        book_event(
            "asset-yes",
            BookSide.BID,
            "0.50",
            "0",
            event_type=BookEventType.DELTA,
            seq="delta-1",
        )
    )
    recorder.record(
        book_event(
            "asset-yes",
            BookSide.BID,
            "0.47",
            "100",
            event_type=BookEventType.DELTA,
            seq="delta-2",
        )
    )
    recorder.flush_all()

    files = list(
        (tmp_path / "raw" / "book" / "date=2026-06-24" / "market=condition-1").glob(
            "*.parquet"
        )
    )
    frame = pl.read_parquet(files).sort(["ts_local_ns", "side", "price"])

    assert frame.select("event_type", "side", "price", "size", "seq_or_hash").rows() == [
        ("snapshot", "ask", Decimal("0.5100000000"), Decimal("10.0000000000"), "snapshot"),
        ("snapshot", "ask", Decimal("0.5200000000"), Decimal("20.0000000000"), "snapshot"),
        ("snapshot", "bid", Decimal("0.4900000000"), Decimal("20.0000000000"), "snapshot"),
        ("snapshot", "bid", Decimal("0.5000000000"), Decimal("10.0000000000"), "snapshot"),
        ("delta", "bid", Decimal("0.4800000000"), Decimal("30.0000000000"), "delta-1"),
        ("delta", "bid", Decimal("0.5000000000"), Decimal("0E-10"), "delta-1"),
    ]


def test_recorder_writes_metadata_only_when_content_changes_and_trims_raw_json(tmp_path) -> None:
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
        partition_date=lambda: date(2026, 6, 24),
    )

    recorder.record_many(
        [
            metadata_snapshot(fetched_at="2026-06-24T00:00:00Z"),
            metadata_snapshot(fetched_at="2026-06-24T00:01:00Z"),
        ]
    )
    recorder.flush_all()

    files = list(
        (tmp_path / "raw" / "metadata" / "date=2026-06-24" / "market=condition-1").glob(
            "*.parquet"
        )
    )
    frame = pl.read_parquet(files)
    raw_json = json.loads(frame["raw_json"][0])

    assert len(frame) == 1
    assert frame["fetched_at"].to_list() == ["2026-06-24T00:00:00Z"]
    assert raw_json == {
        "acceptingOrders": True,
        "active": True,
        "clobTokenIds": ["asset-yes", "asset-no"],
        "closed": False,
        "conditionId": "condition-1",
        "outcomes": ["Yes", "No"],
        "question": "Will Example win?",
        "slug": "fifwc-aaa-bbb-2026-06-24-aaa",
    }


def book_event(
    asset_id: str,
    side: BookSide,
    price: str,
    size: str,
    *,
    event_type: BookEventType = BookEventType.SNAPSHOT,
    seq: str,
) -> BookEvent:
    return BookEvent(
        ts_local_ns=123 if event_type == BookEventType.SNAPSHOT else 124,
        ts_monotonic_ns=456,
        ts_exchange="1757908892351",
        condition_id="condition-1",
        asset_id=asset_id,
        event_type=event_type,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        seq_or_hash=seq,
    )


def metadata_snapshot(*, fetched_at: str) -> MetadataSnapshot:
    raw_json = json.dumps(
        {
            "acceptingOrders": True,
            "active": True,
            "clobTokenIds": ["asset-yes", "asset-no"],
            "closed": False,
            "conditionId": "condition-1",
            "debugBlob": "x" * 10_000,
            "outcomes": ["Yes", "No"],
            "question": "Will Example win?",
            "slug": "fifwc-aaa-bbb-2026-06-24-aaa",
        }
    )
    return MetadataSnapshot(
        fetched_at=fetched_at,
        condition_id="condition-1",
        question="Will Example win?",
        outcomes=("Yes", "No"),
        token_ids=("asset-yes", "asset-no"),
        resolution_or_end_time=None,
        active=True,
        closed=False,
        liquidity=Decimal("1"),
        volume=Decimal("2"),
        raw_json=raw_json,
    )
