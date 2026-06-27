from decimal import Decimal

from pmbot.backtest.models import BookLevel, BookSnapshot, OrderSide
from pmbot.features import BookSizeUnit, depth, microprice, order_book_imbalance, spread


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


def test_kalshi_dollar_notional_book_matches_equivalent_contract_book() -> None:
    polymarket_book = BookSnapshot(
        asset_id="pm-yes",
        bids=(BookLevel(price=Decimal("0.40"), size=Decimal("10")),),
        asks=(BookLevel(price=Decimal("0.60"), size=Decimal("30")),),
    )
    kalshi_book = BookSnapshot(
        asset_id="KXTEST",
        bids=(BookLevel(price=Decimal("0.40"), size=Decimal("4.00")),),
        asks=(BookLevel(price=Decimal("0.60"), size=Decimal("18.00")),),
    )

    assert microprice(kalshi_book, size_unit=BookSizeUnit.DOLLAR_NOTIONAL) == microprice(
        polymarket_book
    )
    assert order_book_imbalance(
        kalshi_book,
        size_unit=BookSizeUnit.DOLLAR_NOTIONAL,
    ) == order_book_imbalance(polymarket_book)
