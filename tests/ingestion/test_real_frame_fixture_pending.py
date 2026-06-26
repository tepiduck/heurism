from decimal import Decimal
from pathlib import Path

from pmbot.ingestion.parsers import parse_ws_frame
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide, TradeEvent

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "ingestion"


def test_real_clob_market_channel_frame_fixture_validation() -> None:
    frame = (FIXTURE_DIR / "polymarket_market_frame.json").read_text()

    events = parse_ws_frame(frame, ts_local_ns=1_782_456_291_107_000_000)

    assert events == [
        BookEvent(
            ts_local_ns=1_782_456_291_107_000_000,
            ts_monotonic_ns=None,
            ts_exchange="1782456291107",
            condition_id="0x7ad403c3508f8e3912940fd1a913f227591145ca0614074208e0b962d5fcc422",
            asset_id="16040015440196279900485035793550429453516625694844857319147506590755961451627",
            event_type=BookEventType.DELTA,
            side=BookSide.BID,
            price=Decimal("0.01"),
            size=Decimal("4100"),
            seq_or_hash="9c0b2537a9793847ea70b74ace251efbda2082fc",
        ),
        BookEvent(
            ts_local_ns=1_782_456_291_107_000_000,
            ts_monotonic_ns=None,
            ts_exchange="1782456291107",
            condition_id="0x7ad403c3508f8e3912940fd1a913f227591145ca0614074208e0b962d5fcc422",
            asset_id="94476829201604408463453426454480212459887267917122244941405244686637914508323",
            event_type=BookEventType.DELTA,
            side=BookSide.ASK,
            price=Decimal("0.99"),
            size=Decimal("4100"),
            seq_or_hash="f97bc37c1ae53dcfc85e992019bdc619c141465b",
        ),
    ]


def test_real_clob_trade_frame_fixture_validation() -> None:
    frame = (FIXTURE_DIR / "polymarket_trade_frame.json").read_text()

    events = parse_ws_frame(frame, ts_local_ns=1_782_456_556_509_000_000)

    assert events == [
        TradeEvent(
            ts_local_ns=1_782_456_556_509_000_000,
            ts_monotonic_ns=None,
            ts_exchange="1782456556509",
            condition_id="0xcb239105ed21a2420ba4d85090b9bc32755c56601ffdc528afd17fd6282fe930",
            asset_id="82254794773423437491598574581796732934760602553828664932786984448280136473130",
            price=Decimal("0.993"),
            size=Decimal("1.500428"),
            taker_side="buy",
            trade_id=(
                "0xe4f33eb2751e6f06def0085c5285d757d8c5e6a59056c583b3cf9303ae52568f"
            ),
        )
    ]
