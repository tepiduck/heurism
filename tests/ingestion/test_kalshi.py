import asyncio
import json
from base64 import b64decode
from decimal import Decimal
from pathlib import Path

import httpx
import polars as pl
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from pmbot.ingestion.kalshi import (
    KALSHI_WS_PATH,
    KALSHI_WS_URL,
    KalshiAuth,
    KalshiMetadataClient,
    KalshiNormalizer,
    KalshiRecorderAdapter,
    kalshi_auth_headers,
    kalshi_request_path,
    kalshi_subscribe_message,
    kalshi_websocket_headers,
    load_kalshi_private_key,
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

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "ingestion"


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


def test_kalshi_auth_headers_sign_known_websocket_message(tmp_path) -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    key_path = tmp_path / "kalshi-test.pem"
    key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    loaded_key = load_kalshi_private_key(key_path)

    headers = kalshi_auth_headers(
        key_id="test-key-id",
        private_key=loaded_key,
        method="GET",
        path=KALSHI_WS_PATH,
        timestamp_ms=1_782_460_000_000,
    )

    assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == "1782460000000"
    loaded_key.public_key().verify(
        b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        b"1782460000000GET/trade-api/ws/v2",
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=32,
        ),
        hashes.SHA256(),
    )


def test_kalshi_websocket_headers_sign_documented_path() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    auth = KalshiAuth(key_id="test-key-id", private_key=private_key)

    headers = kalshi_websocket_headers(
        auth,
        ws_url=KALSHI_WS_URL,
        timestamp_ms=1_782_460_000_001,
    )

    assert kalshi_request_path(KALSHI_WS_URL) == KALSHI_WS_PATH
    private_key.public_key().verify(
        b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        b"1782460000001GET/trade-api/ws/v2",
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=32,
        ),
        hashes.SHA256(),
    )


def test_kalshi_metadata_client_adds_auth_headers() -> None:
    async def run() -> None:
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        auth = KalshiAuth(key_id="test-key-id", private_key=private_key)

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["KALSHI-ACCESS-KEY"] == "test-key-id"
            assert request.headers["KALSHI-ACCESS-TIMESTAMP"]
            assert request.headers["KALSHI-ACCESS-SIGNATURE"]
            return httpx.Response(200, json={"markets": [], "cursor": ""})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            page, cursor = await KalshiMetadataClient(http=http, auth=auth).fetch_markets()

        assert page == []
        assert cursor is None

    asyncio.run(run())


def test_real_kalshi_orderbook_frame_fixture_validation() -> None:
    snapshot = json.loads((FIXTURE_DIR / "kalshi_orderbook_snapshot.json").read_text())
    delta = json.loads((FIXTURE_DIR / "kalshi_orderbook_delta.json").read_text())

    normalizer = KalshiNormalizer()
    snapshot_events = normalizer.parse_payload(
        snapshot,
        ts_local_ns=1_782_543_311_728_000_000,
        ts_monotonic_ns=1,
    )
    snapshot_gaps = [event for event in snapshot_events if isinstance(event, GapEvent)]
    snapshot_books = [event for event in snapshot_events if isinstance(event, BookEvent)]

    assert snapshot["msg"]["market_ticker"] == "KXWCGAME-26JUN27CODUZB-COD"
    assert len(snapshot_gaps) == 1
    assert snapshot_gaps[0].reason == GapReason.RECONNECT
    assert len(snapshot_books) == 84
    assert snapshot_books[0] == BookEvent(
        ts_local_ns=1_782_543_311_728_000_000,
        ts_monotonic_ns=1,
        ts_exchange=None,
        condition_id="KXWCGAME-26JUN27CODUZB-COD",
        asset_id="KXWCGAME-26JUN27CODUZB-COD",
        event_type=BookEventType.SNAPSHOT,
        side=BookSide.BID,
        price=Decimal("0.0100"),
        size=Decimal("1644502.42"),
        seq_or_hash="7c61fe70-c821-434c-9276-f4fa8b653d3a",
    )
    first_ask = next(event for event in snapshot_books if event.side == BookSide.ASK)
    assert first_ask.price == Decimal("0.9900")
    assert first_ask.size == Decimal("1210816.01")

    delta_msg = delta["msg"]
    side = delta_msg["side"]
    raw_price = Decimal(delta_msg["price_dollars"])
    raw_delta = Decimal(delta_msg["delta_fp"])
    raw_levels = snapshot["msg"]["yes_dollars_fp" if side == "yes" else "no_dollars_fp"]
    prior_size = next(
        Decimal(raw_size)
        for raw_level_price, raw_size in raw_levels
        if Decimal(raw_level_price) == raw_price
    )
    expected_absolute_size = max(Decimal("0"), prior_size + raw_delta)

    delta_events = normalizer.parse_payload(
        delta,
        ts_local_ns=1_782_543_311_729_000_000,
        ts_monotonic_ns=2,
    )
    delta_gaps = [event for event in delta_events if isinstance(event, GapEvent)]
    delta_books = [event for event in delta_events if isinstance(event, BookEvent)]

    assert delta["seq"] == snapshot["seq"] + 1
    assert delta_gaps == []
    assert delta_books == [
        BookEvent(
            ts_local_ns=1_782_543_311_729_000_000,
            ts_monotonic_ns=2,
            ts_exchange="1782543311728",
            condition_id="KXWCGAME-26JUN27CODUZB-COD",
            asset_id="KXWCGAME-26JUN27CODUZB-COD",
            event_type=BookEventType.DELTA,
            side=BookSide.BID,
            price=Decimal("0.5900"),
            size=expected_absolute_size,
            seq_or_hash="2",
        )
    ]
    assert expected_absolute_size == Decimal("192456.86")


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
