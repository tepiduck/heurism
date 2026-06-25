from decimal import Decimal

from pmbot.backtest.models import BookLevel, BookSnapshot, OrderSide
from pmbot.features import depth, microprice, order_book_imbalance, spread


def test_orderbook_feature_math_matches_hand_computed_values() -> None:
    book = BookSnapshot(
        asset_id="asset-yes",
        bids=(BookLevel(price=Decimal("0.40"), size=Decimal("10")),),
        asks=(BookLevel(price=Decimal("0.60"), size=Decimal("30")),),
    )

    assert spread(book) == Decimal("0.20")
    assert depth(book, OrderSide.BUY) == Decimal("10")
    assert depth(book, OrderSide.SELL) == Decimal("30")
    assert order_book_imbalance(book) == Decimal("-0.5")
    assert microprice(book) == Decimal("0.45")
