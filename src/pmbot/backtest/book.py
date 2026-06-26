"""Order-book reconstruction and depth walking."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from pmbot.backtest.models import (
    BookLevel,
    BookSnapshot,
    MarkingMode,
    OrderRequest,
    OrderSide,
    OrderType,
)
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide


class OrderBookState:
    """Mutable replay-time order-book state."""

    def __init__(self) -> None:
        self._bids: dict[str, dict[Decimal, Decimal]] = {}
        self._asks: dict[str, dict[Decimal, Decimal]] = {}
        self._snapshot_keys: dict[str, tuple[int, str | None]] = {}

    def apply(self, event: BookEvent) -> None:
        """Apply a normalized book event."""
        if event.event_type == BookEventType.SNAPSHOT:
            key = (event.ts_local_ns, event.seq_or_hash)
            if self._snapshot_keys.get(event.asset_id) != key:
                self._bids[event.asset_id] = {}
                self._asks[event.asset_id] = {}
                self._snapshot_keys[event.asset_id] = key
        levels = self._side(event.asset_id, event.side)
        if event.size == Decimal("0"):
            levels.pop(event.price, None)
        else:
            levels[event.price] = event.size

    def snapshot(self, asset_id: str) -> BookSnapshot:
        """Return a read-only sorted book snapshot for one asset."""
        bids = tuple(
            BookLevel(price=price, size=size)
            for price, size in sorted(self._bids.get(asset_id, {}).items(), reverse=True)
        )
        asks = tuple(
            BookLevel(price=price, size=size)
            for price, size in sorted(self._asks.get(asset_id, {}).items())
        )
        return BookSnapshot(asset_id=asset_id, bids=bids, asks=asks)

    def queue_ahead(
        self,
        order: OrderRequest,
        *,
        synthetic_ahead: Decimal = Decimal("0"),
    ) -> Decimal:
        """Displayed size ahead of a newly resting order at its limit level."""
        if order.limit_price is None:
            return synthetic_ahead
        levels = self._bids if order.side == OrderSide.BUY else self._asks
        return levels.get(order.asset_id, {}).get(order.limit_price, Decimal("0")) + synthetic_ahead

    def is_marketable(self, order: OrderRequest) -> bool:
        """Return whether an order crosses current displayed depth."""
        if order.order_type == OrderType.MARKET:
            return True
        if order.limit_price is None:
            return False
        snapshot = self.snapshot(order.asset_id)
        if order.side == OrderSide.BUY:
            return snapshot.best_ask is not None and order.limit_price >= snapshot.best_ask.price
        return snapshot.best_bid is not None and order.limit_price <= snapshot.best_bid.price

    def walk(self, order: OrderRequest) -> tuple[tuple[BookLevel, Decimal], ...]:
        """Walk displayed depth for a marketable order and cap at available size."""
        remaining = order.size
        fills: list[tuple[BookLevel, Decimal]] = []
        for level in self._walkable_levels(order):
            if remaining <= Decimal("0"):
                break
            fill_size = min(remaining, level.size)
            fills.append((level, fill_size))
            remaining -= fill_size
        return tuple(fills)

    def mid_prices(self) -> dict[str, Decimal]:
        """Return midpoint marks for assets with both sides present."""
        asset_ids = set(self._bids) | set(self._asks)
        marks: dict[str, Decimal] = {}
        for asset_id in asset_ids:
            snapshot = self.snapshot(asset_id)
            if snapshot.best_bid is not None and snapshot.best_ask is not None:
                marks[asset_id] = (snapshot.best_bid.price + snapshot.best_ask.price) / Decimal("2")
        return marks

    def mark_price(
        self,
        *,
        asset_id: str,
        position: Decimal,
        mode: MarkingMode,
    ) -> Decimal | None:
        """Return the conservative mark for an open position."""
        snapshot = self.snapshot(asset_id)
        if mode == MarkingMode.MID:
            if snapshot.best_bid is None or snapshot.best_ask is None:
                return None
            return (snapshot.best_bid.price + snapshot.best_ask.price) / Decimal("2")
        if mode == MarkingMode.LIQUIDATION:
            if position > Decimal("0"):
                return snapshot.best_bid.price if snapshot.best_bid is not None else None
            if position < Decimal("0"):
                return snapshot.best_ask.price if snapshot.best_ask is not None else None
            return Decimal("0")
        raise ValueError(f"unsupported marking mode: {mode}")

    def _walkable_levels(self, order: OrderRequest) -> Iterable[BookLevel]:
        snapshot = self.snapshot(order.asset_id)
        levels = snapshot.asks if order.side == OrderSide.BUY else snapshot.bids
        for level in levels:
            if (
                order.order_type == OrderType.MARKET
                or order.limit_price is None
                or (order.side == OrderSide.BUY and level.price <= order.limit_price)
                or (order.side == OrderSide.SELL and level.price >= order.limit_price)
            ):
                yield level

    def _side(self, asset_id: str, side: BookSide) -> dict[Decimal, Decimal]:
        books = self._bids if side == BookSide.BID else self._asks
        return books.setdefault(asset_id, {})
