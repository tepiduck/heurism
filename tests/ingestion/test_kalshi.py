import asyncio
from decimal import Decimal

import httpx
import polars as pl
import pytest

from pmbot.ingestion.kalshi import (
    KalshiMetadataClient,
    KalshiNormalizer,
    KalshiRecorderAdapter,
    kalshi_subscribe_message,
)
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import (
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    GapReason,
    TradeEvent,
)


def test_kalshi_orderbook_snapshot_normalizes_yes_bids_and_implied_yes_asks() -> None:
    normalizer = KalshiNormalizer()

    events = normalizer.parse_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 2,
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "market_id": "market-id",
                "yes_dollars_fp": [["0.4200", "10.00"]],
                "no_dollars_fp": [["0.5600", "15.00"]],
            },
        },
        ts_local_ns=1_800_000_000_000_000_000,
        ts_monotonic_ns=1,
    )

    gaps = [event for event in events if isinstance(event, GapEvent)]
    book_events = tuple(event for event in events if isinstance(event, BookEvent))

    assert len(gaps) == 1
    assert gaps[0].reason == GapReason.RECONNECT
    assert book_events == (
        BookEvent(
            ts_local_ns=1_800_000_000_000_000_000,
            ts_monotonic_ns=1,
            ts_exchange=None,
            condition_id="KXTEST-26-YES",
            asset_id="KXTEST-26-YES",
            event_type=BookEventType.SNAPSHOT,
            side=BookSide.BID,
            price=Decimal("0.4200"),
            size=Decimal("10.00"),
            seq_or_hash="market-id",
        ),
        BookEvent(
            ts_local_ns=1_800_000_000_000_000_000,
            ts_monotonic_ns=1,
            ts_exchange=None,
            condition_id="KXTEST-26-YES",
            asset_id="KXTEST-26-YES",
            event_type=BookEventType.SNAPSHOT,
            side=BookSide.ASK,
            price=Decimal("0.4400"),
            size=Decimal("15.00"),
            seq_or_hash="market-id",
        ),
    )


def test_kalshi_orderbook_delta_emits_absolute_normalized_level_size() -> None:
    normalizer = KalshiNormalizer()
    normalizer.parse_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 2,
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "yes_dollars_fp": [["0.4200", "10.00"]],
                "no_dollars_fp": [["0.5600", "15.00"]],
            },
        },
        ts_local_ns=1,
    )

    events = normalizer.parse_payload(
        {
            "type": "orderbook_delta",
            "seq": 3,
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "price_dollars": "0.5600",
                "delta_fp": "-5.00",
                "side": "no",
                "ts_ms": 1_669_149_841_000,
            },
        },
        ts_local_ns=2,
    )

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, BookEvent)
    assert event.event_type == BookEventType.DELTA
    assert event.side == BookSide.ASK
    assert event.price == Decimal("0.4400")
    assert event.size == Decimal("10.00")
    assert event.seq_or_hash == "3"


def test_kalshi_price_parser_accepts_legacy_integer_cent_levels() -> None:
    normalizer = KalshiNormalizer()

    events = normalizer.parse_payload(
        {
            "type": "orderbook_snapshot",
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "yes": [[42, 10]],
                "no": [[56, 15]],
            },
        },
        ts_local_ns=1,
    )

    book_events = [event for event in events if isinstance(event, BookEvent)]

    assert book_events[0].price == Decimal("0.42")
    assert book_events[0].size == Decimal("10")
    assert book_events[1].price == Decimal("0.44")
    assert book_events[1].size == Decimal("15")


def test_kalshi_sequence_jump_emits_gap_event() -> None:
    normalizer = KalshiNormalizer()
    normalizer.parse_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 2,
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "yes_dollars_fp": [["0.4200", "10.00"]],
                "no_dollars_fp": [["0.5600", "15.00"]],
            },
        },
        ts_local_ns=1,
    )

    events = normalizer.parse_payload(
        {
            "type": "orderbook_delta",
            "sid": 2,
            "seq": 4,
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "price_dollars": "0.4200",
                "delta_fp": "1.00",
                "side": "yes",
            },
        },
        ts_local_ns=2,
    )

    gaps = [event for event in events if isinstance(event, GapEvent)]

    assert len(gaps) == 1
    assert gaps[0].reason == GapReason.SEQUENCE_GAP
    assert gaps[0].details == "Kalshi sequence jumped from 2 to 4"


def test_kalshi_delta_before_snapshot_emits_missing_snapshot_gap() -> None:
    normalizer = KalshiNormalizer()

    events = normalizer.parse_payload(
        {
            "type": "orderbook_delta",
            "sid": 2,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "price_dollars": "0.4200",
                "delta_fp": "5.00",
                "side": "yes",
            },
        },
        ts_local_ns=1,
    )

    gaps = [event for event in events if isinstance(event, GapEvent)]
    book_events = [event for event in events if isinstance(event, BookEvent)]

    assert len(gaps) == 1
    assert gaps[0].reason == GapReason.MISSING_SNAPSHOT
    assert book_events[0].size == Decimal("5.00")


def test_kalshi_trade_normalizes_yes_price_and_count() -> None:
    normalizer = KalshiNormalizer()

    events = normalizer.parse_payload(
        {
            "type": "trade",
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "KXTEST-26-YES",
                "yes_price_dollars": "0.360",
                "no_price_dollars": "0.640",
                "count_fp": "136.00",
                "taker_side": "yes",
                "ts_ms": 1_669_149_841_000,
            },
        },
        ts_local_ns=3,
    )

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, TradeEvent)
    assert event.price == Decimal("0.360")
    assert event.size == Decimal("136.00")
    assert event.taker_side == "buy"


def test_kalshi_recorder_adapter_reuses_shared_parquet_recorder(tmp_path) -> None:
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
    )
    adapter = KalshiRecorderAdapter(recorder)

    adapter.record_payload(
        {
            "type": "orderbook_snapshot",
            "msg": {
                "market_ticker": "KXTEST-26-YES",
                "market_id": "market-id",
                "yes_dollars_fp": [["0.4200", "10.00"]],
                "no_dollars_fp": [["0.5600", "15.00"]],
            },
        },
        ts_local_ns=1_800_000_000_000_000_000,
        ts_monotonic_ns=123,
    )
    adapter.flush_all()

    files = list(tmp_path.glob("raw/book/date=*/market=KXTEST-26-YES/*.parquet"))
    frame = pl.read_parquet(files)

    assert len(files) == 1
    assert frame["price"].to_list() == [Decimal("0.4200000000"), Decimal("0.4400000000")]
    assert frame["size"].to_list() == [Decimal("10.0000000000"), Decimal("15.0000000000")]


@pytest.mark.xfail(
    reason="TODO: capture real Kalshi orderbook_snapshot/orderbook_delta frames off-network",
)
def test_real_kalshi_frame_fixture_pending() -> None:
    raise AssertionError("real Kalshi frame fixture pending")


def test_kalshi_metadata_client_normalizes_open_markets_from_rest() -> None:
    async def run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/trade-api/v2/markets"
            return httpx.Response(
                200,
                json={
                    "markets": [
                        {
                            "ticker": "KXTEST-26-YES",
                            "title": "Will test resolve yes?",
                            "status": "open",
                            "expiration_time": "2026-12-31T00:00:00Z",
                            "liquidity": "123.45",
                            "volume": "678.90",
                        }
                    ],
                    "cursor": "",
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            snapshots = await KalshiMetadataClient(http=http).fetch_all_open_markets()

        assert len(snapshots) == 1
        assert snapshots[0].condition_id == "KXTEST-26-YES"
        assert snapshots[0].token_ids == ("KXTEST-26-YES",)
        assert snapshots[0].active is True
        assert snapshots[0].liquidity == Decimal("123.45")
        assert snapshots[0].volume == Decimal("678.90")

    asyncio.run(run())


def test_kalshi_subscribe_message_matches_documented_command_shape() -> None:
    assert kalshi_subscribe_message(
        channels=("orderbook_delta", "trade"),
        market_tickers=("KXTEST-26-YES",),
        message_id=7,
    ) == {
        "id": 7,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta", "trade"],
            "market_tickers": ["KXTEST-26-YES"],
        },
    }
