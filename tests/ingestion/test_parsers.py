import json
import time
from decimal import Decimal

from pmbot.ingestion.hashing import compute_order_book_hash
from pmbot.ingestion.parsers import parse_ws_frame
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide, TradeEvent


def test_parse_book_snapshot_frame_to_normalized_events() -> None:
    frame = {
        "event_type": "book",
        "asset_id": "asset-yes",
        "market": "condition-1",
        "bids": [{"price": ".48", "size": "30"}],
        "asks": [{"price": ".52", "size": "25"}],
        "timestamp": "1757908892351",
    }
    frame["hash"] = compute_order_book_hash(
        market="condition-1",
        asset_id="asset-yes",
        timestamp="1757908892351",
        bids=frame["bids"],
        asks=frame["asks"],
    )

    events = parse_ws_frame(json.dumps(frame), ts_local_ns=123, ts_monotonic_ns=321)

    assert events == [
        BookEvent(
            ts_local_ns=123,
            ts_monotonic_ns=321,
            ts_exchange="1757908892351",
            condition_id="condition-1",
            asset_id="asset-yes",
            event_type=BookEventType.SNAPSHOT,
            side=BookSide.BID,
            price=Decimal(".48"),
            size=Decimal("30"),
            seq_or_hash=frame["hash"],
        ),
        BookEvent(
            ts_local_ns=123,
            ts_monotonic_ns=321,
            ts_exchange="1757908892351",
            condition_id="condition-1",
            asset_id="asset-yes",
            event_type=BookEventType.SNAPSHOT,
            side=BookSide.ASK,
            price=Decimal(".52"),
            size=Decimal("25"),
            seq_or_hash=frame["hash"],
        ),
    ]


def test_parse_price_change_frame_to_delta_events() -> None:
    frame = {
        "event_type": "price_change",
        "market": "condition-1",
        "timestamp": "1757908892352",
        "price_changes": [
            {
                "asset_id": "asset-yes",
                "price": "0.49",
                "size": "20",
                "side": "BUY",
                "hash": "hash-1",
            },
            {
                "asset_id": "asset-no",
                "price": "0.51",
                "size": "0",
                "side": "SELL",
                "hash": "hash-2",
            },
        ],
    }

    events = parse_ws_frame(json.dumps(frame), ts_local_ns=456, ts_monotonic_ns=654)

    assert [event.event_type for event in events if isinstance(event, BookEvent)] == [
        BookEventType.DELTA,
        BookEventType.DELTA,
    ]
    assert [event.side for event in events if isinstance(event, BookEvent)] == [
        BookSide.BID,
        BookSide.ASK,
    ]
    assert [event.price for event in events if isinstance(event, BookEvent)] == [
        Decimal("0.49"),
        Decimal("0.51"),
    ]
    assert [event.ts_monotonic_ns for event in events if isinstance(event, BookEvent)] == [
        654,
        654,
    ]


def test_parse_last_trade_price_frame_to_trade_event() -> None:
    frame = {
        "asset_id": "asset-yes",
        "event_type": "last_trade_price",
        "market": "condition-1",
        "price": "0.456",
        "side": "BUY",
        "size": "219.217767",
        "timestamp": "1750428146322",
    }

    events = parse_ws_frame(json.dumps(frame), ts_local_ns=789, ts_monotonic_ns=987)

    assert len(events) == 1
    trade = events[0]
    assert isinstance(trade, TradeEvent)
    assert trade.price == Decimal("0.456")
    assert trade.size == Decimal("219.217767")
    assert trade.ts_monotonic_ns == 987
    assert trade.taker_side == "buy"
    assert trade.trade_id


def test_default_ts_local_ns_is_utc_wall_clock_nanoseconds() -> None:
    frame = {
        "event_type": "price_change",
        "market": "condition-1",
        "timestamp": "1757908892352",
        "price_changes": [
            {
                "asset_id": "asset-yes",
                "price": "0.49",
                "size": "20",
                "side": "BUY",
                "hash": "hash-1",
            }
        ],
    }

    before_ns = time.time_ns()
    events = parse_ws_frame(json.dumps(frame))
    after_ns = time.time_ns()

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, BookEvent)
    assert event.ts_local_ns > 1_700_000_000_000_000_000
    assert before_ns <= event.ts_local_ns <= after_ns
    assert event.ts_monotonic_ns is not None
