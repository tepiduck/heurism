"""Point-in-time order-book features."""

from __future__ import annotations

from decimal import Decimal

from pmbot.backtest.models import BookLevel, BookSnapshot, OrderSide


def spread(book: BookSnapshot) -> Decimal | None:
    """Return best ask minus best bid."""
    if book.best_bid is None or book.best_ask is None:
        return None
    return book.best_ask.price - book.best_bid.price


def depth(book: BookSnapshot, side: OrderSide | None = None, levels: int | None = None) -> Decimal:
    """Return visible depth for one side or both sides."""
    if side == OrderSide.BUY:
        return _sum_depth(book.bids, levels)
    if side == OrderSide.SELL:
        return _sum_depth(book.asks, levels)
    return _sum_depth(book.bids, levels) + _sum_depth(book.asks, levels)


def order_book_imbalance(book: BookSnapshot, levels: int | None = 1) -> Decimal | None:
    """Return (bid depth - ask depth) / (bid depth + ask depth)."""
    bid_depth = depth(book, OrderSide.BUY, levels)
    ask_depth = depth(book, OrderSide.SELL, levels)
    total = bid_depth + ask_depth
    if total == Decimal("0"):
        return None
    return (bid_depth - ask_depth) / total


def microprice(book: BookSnapshot) -> Decimal | None:
    """Return top-of-book microprice as a probability."""
    if book.best_bid is None or book.best_ask is None:
        return None
    total_size = book.best_bid.size + book.best_ask.size
    if total_size == Decimal("0"):
        return None
    return (
        book.best_bid.price * book.best_ask.size
        + book.best_ask.price * book.best_bid.size
    ) / total_size


def _sum_depth(levels: tuple[BookLevel, ...], limit: int | None) -> Decimal:
    total = Decimal("0")
    for level in levels[:limit]:
        total += level.size
    return total
