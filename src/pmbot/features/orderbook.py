"""Point-in-time order-book features."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from pmbot.backtest.models import BookLevel, BookSnapshot, OrderSide


class BookSizeUnit(StrEnum):
    """Unit used by displayed order-book level sizes."""

    CONTRACTS = "contracts"
    DOLLAR_NOTIONAL = "dollar_notional"


def spread(book: BookSnapshot) -> Decimal | None:
    """Return best ask minus best bid."""
    if book.best_bid is None or book.best_ask is None:
        return None
    return book.best_ask.price - book.best_bid.price


def depth(
    book: BookSnapshot,
    side: OrderSide | None = None,
    levels: int | None = None,
    *,
    size_unit: BookSizeUnit = BookSizeUnit.CONTRACTS,
) -> Decimal:
    """Return visible depth for one side or both sides."""
    if side == OrderSide.BUY:
        return _sum_depth(book.bids, levels, size_unit=size_unit)
    if side == OrderSide.SELL:
        return _sum_depth(book.asks, levels, size_unit=size_unit)
    return _sum_depth(book.bids, levels, size_unit=size_unit) + _sum_depth(
        book.asks,
        levels,
        size_unit=size_unit,
    )


def order_book_imbalance(
    book: BookSnapshot,
    levels: int | None = 1,
    *,
    size_unit: BookSizeUnit = BookSizeUnit.CONTRACTS,
) -> Decimal | None:
    """Return (bid depth - ask depth) / (bid depth + ask depth)."""
    bid_depth = depth(book, OrderSide.BUY, levels, size_unit=size_unit)
    ask_depth = depth(book, OrderSide.SELL, levels, size_unit=size_unit)
    total = bid_depth + ask_depth
    if total == Decimal("0"):
        return None
    return (bid_depth - ask_depth) / total


def microprice(
    book: BookSnapshot,
    *,
    size_unit: BookSizeUnit = BookSizeUnit.CONTRACTS,
) -> Decimal | None:
    """Return top-of-book microprice as a probability."""
    if book.best_bid is None or book.best_ask is None:
        return None
    bid_size = _level_size(book.best_bid, size_unit)
    ask_size = _level_size(book.best_ask, size_unit)
    total_size = bid_size + ask_size
    if total_size == Decimal("0"):
        return None
    return (book.best_bid.price * ask_size + book.best_ask.price * bid_size) / total_size


def _sum_depth(
    levels: tuple[BookLevel, ...],
    limit: int | None,
    *,
    size_unit: BookSizeUnit,
) -> Decimal:
    total = Decimal("0")
    for level in levels[:limit]:
        total += _level_size(level, size_unit)
    return total


def _level_size(level: BookLevel, size_unit: BookSizeUnit) -> Decimal:
    if size_unit == BookSizeUnit.CONTRACTS:
        return level.size
    if size_unit == BookSizeUnit.DOLLAR_NOTIONAL:
        if level.price == Decimal("0"):
            return Decimal("0")
        return level.size / level.price
    raise ValueError(f"unsupported book size unit: {size_unit}")
