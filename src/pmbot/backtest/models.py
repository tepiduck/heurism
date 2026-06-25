"""Backtest domain models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType

from pmbot.backtest.costs import CostBreakdown, LiquidityFlag


class OrderSide(StrEnum):
    """Order direction."""

    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    """Supported order types."""

    LIMIT = "limit"
    MARKET = "market"


@dataclass(frozen=True, slots=True)
class OrderRequest:
    """Strategy order intent emitted at decision time."""

    asset_id: str
    side: OrderSide
    size: Decimal
    order_type: OrderType = OrderType.LIMIT
    limit_price: Decimal | None = None
    client_order_id: str | None = None


@dataclass(frozen=True, slots=True)
class PendingOrder:
    """Order scheduled for market arrival after latency."""

    order_id: str
    request: OrderRequest
    decision_ts_local_ns: int
    arrival_ts_local_ns: int


@dataclass(slots=True)
class RestingOrder:
    """Limit order resting behind existing displayed queue."""

    order_id: str
    request: OrderRequest
    decision_ts_local_ns: int
    arrival_ts_local_ns: int
    remaining_size: Decimal
    queue_ahead: Decimal


@dataclass(frozen=True, slots=True)
class Fill:
    """A simulated execution fill."""

    order_id: str
    client_order_id: str | None
    asset_id: str
    side: OrderSide
    price: Decimal
    size: Decimal
    liquidity: LiquidityFlag
    decision_ts_local_ns: int
    arrival_ts_local_ns: int
    execution_ts_local_ns: int
    notional: Decimal
    fee: Decimal
    gas: Decimal
    total_cost: Decimal
    source_trade_id: str | None


@dataclass(frozen=True, slots=True)
class BookLevel:
    """One displayed book level."""

    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """Read-only asset book snapshot."""

    asset_id: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]

    @property
    def best_bid(self) -> BookLevel | None:
        """Highest bid level."""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> BookLevel | None:
        """Lowest ask level."""
        return self.asks[0] if self.asks else None


@dataclass(frozen=True, slots=True)
class PositionMark:
    """Position and mark-to-market state at one replay timestamp."""

    ts_local_ns: int
    cash: Decimal
    positions: Mapping[str, Decimal]
    mark_to_market: Decimal
    pnl: Decimal
    pnl_valid: bool

    @classmethod
    def create(
        cls,
        *,
        ts_local_ns: int,
        cash: Decimal,
        positions: Mapping[str, Decimal],
        mark_to_market: Decimal,
        pnl: Decimal,
        pnl_valid: bool,
    ) -> PositionMark:
        """Create with a read-only positions mapping."""
        return cls(
            ts_local_ns=ts_local_ns,
            cash=cash,
            positions=MappingProxyType(dict(positions)),
            mark_to_market=mark_to_market,
            pnl=pnl,
            pnl_valid=pnl_valid,
        )


def signed_position_delta(side: OrderSide, size: Decimal) -> Decimal:
    """Convert a fill side/size into signed inventory."""
    return size if side == OrderSide.BUY else -size


def signed_cash_delta(side: OrderSide, cost: CostBreakdown) -> Decimal:
    """Convert a fill side/cost into signed cash movement."""
    if side == OrderSide.BUY:
        return -(cost.notional + cost.total)
    return cost.notional - cost.total
