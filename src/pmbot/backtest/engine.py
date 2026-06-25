"""Deterministic event-driven backtester with structural look-ahead prevention."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from pmbot.backtest.book import OrderBookState
from pmbot.backtest.costs import CostConfig, CostModel, LiquidityFlag
from pmbot.backtest.data import ReplayEvent, load_replay_events, merge_events
from pmbot.backtest.models import (
    BookSnapshot,
    Fill,
    MarkingMode,
    OrderRequest,
    OrderSide,
    OrderType,
    PendingOrder,
    PositionMark,
    RestingOrder,
    signed_cash_delta,
    signed_position_delta,
)
from pmbot.ingestion.schemas import BookEvent, BookEventType, GapEvent, TradeEvent


class StrategyCallback(Protocol):
    """Strategy callback that can only receive point-in-time state."""

    def __call__(self, view: PointInTimeView) -> Iterable[OrderRequest]:
        """Return desired orders for the current replay timestamp."""


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    """Backtest execution realism configuration."""

    latency_ms: int = 150
    costs: CostConfig = field(default_factory=CostConfig)
    marking_mode: MarkingMode = MarkingMode.LIQUIDATION
    allow_zero_costs: bool = False


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """Structured replay outputs for scoring."""

    blotter: tuple[Fill, ...]
    pnl_series: tuple[PositionMark, ...]
    pnl_valid: bool


@dataclass(frozen=True, slots=True)
class PointInTimeView:
    """Read-only strategy view containing no future events."""

    ts_local_ns: int
    _book: OrderBookState
    _trades: tuple[TradeEvent, ...]
    _events: tuple[ReplayEvent, ...]
    _gap_active_assets: frozenset[str]
    _gap_active_all: bool

    def book(self, asset_id: str) -> BookSnapshot:
        """Return current book state as of ``ts_local_ns``."""
        return self._book.snapshot(asset_id)

    def trades(
        self,
        *,
        asset_id: str | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> tuple[TradeEvent, ...]:
        """Return trades bounded to the view timestamp, never the future."""
        effective_end = self.ts_local_ns if end_ns is None else min(end_ns, self.ts_local_ns)
        if start_ns is not None and start_ns > self.ts_local_ns:
            return ()
        return tuple(
            trade
            for trade in self._trades
            if (asset_id is None or trade.asset_id == asset_id)
            and (start_ns is None or trade.ts_local_ns >= start_ns)
            and trade.ts_local_ns <= effective_end
        )

    def events(self, *, end_ns: int | None = None) -> tuple[ReplayEvent, ...]:
        """Return replay events up to this view timestamp only."""
        effective_end = self.ts_local_ns if end_ns is None else min(end_ns, self.ts_local_ns)
        return tuple(event for event in self._events if event.ts_local_ns <= effective_end)

    def gap_active(self, asset_id: str) -> bool:
        """Return whether the asset is inside an untrusted gap span."""
        return self._gap_active_all or asset_id in self._gap_active_assets


class BacktestEngine:
    """Replay normalized events and simulate conservative fills."""

    def __init__(self, config: BacktestConfig | None = None) -> None:
        self._config = config or BacktestConfig()
        self._cost_model = CostModel(self._config.costs)
        if not self._config.allow_zero_costs and self._cost_model.is_zero_cost_schedule():
            raise ValueError("zero-cost backtests require allow_zero_costs=True")

    def run_from_data_dir(self, data_dir: Path, strategy: StrategyCallback) -> BacktestResult:
        """Load WP-1 Parquet data and run a replay."""
        return self.run(load_replay_events(data_dir), strategy)

    def run(self, events: Iterable[ReplayEvent], strategy: StrategyCallback) -> BacktestResult:
        """Run a deterministic event-driven replay."""
        state = _ReplayState(
            cost_model=self._cost_model,
            marking_mode=self._config.marking_mode,
        )
        replay_events = merge_events(events)
        index = 0
        while index < len(replay_events):
            event_ts = replay_events[index].ts_local_ns
            state.process_arrivals_until(event_ts)
            same_ts: list[ReplayEvent] = []
            while index < len(replay_events) and replay_events[index].ts_local_ns == event_ts:
                same_ts.append(replay_events[index])
                index += 1
            for event in same_ts:
                state.apply_event(event)
            view = state.view(event_ts)
            for request in strategy(view):
                state.schedule_order(request, decision_ts_local_ns=event_ts, config=self._config)
            state.mark(event_ts)
        state.process_arrivals_until(None)
        return BacktestResult(
            blotter=tuple(state.fills),
            pnl_series=tuple(state.pnl_series),
            pnl_valid=all(mark.pnl_valid for mark in state.pnl_series),
        )


class _ReplayState:
    def __init__(self, *, cost_model: CostModel, marking_mode: MarkingMode) -> None:
        self.book = OrderBookState()
        self.cost_model = cost_model
        self.marking_mode = marking_mode
        self.pending: list[PendingOrder] = []
        self.resting: list[RestingOrder] = []
        self.fills: list[Fill] = []
        self.pnl_series: list[PositionMark] = []
        self.trades: list[TradeEvent] = []
        self.events: list[ReplayEvent] = []
        self.gap_assets: set[str] = set()
        self.gap_conditions: set[str] = set()
        self.cash = Decimal("0")
        self.positions: dict[str, Decimal] = {}
        self._next_order_id = 1

    def process_arrivals_until(self, ts_local_ns: int | None) -> None:
        """Move orders whose latency has elapsed into market/resting state."""
        self.pending.sort(key=lambda order: (order.arrival_ts_local_ns, order.order_id))
        ready: list[PendingOrder] = []
        remaining: list[PendingOrder] = []
        for order in self.pending:
            if ts_local_ns is None or order.arrival_ts_local_ns <= ts_local_ns:
                ready.append(order)
            else:
                remaining.append(order)
        self.pending = remaining
        for order in ready:
            self._arrive(order)

    def apply_event(self, event: ReplayEvent) -> None:
        """Apply one replay event."""
        if isinstance(event, GapEvent):
            self._apply_gap(event)
        elif isinstance(event, BookEvent):
            self.book.apply(event)
            if event.event_type == BookEventType.SNAPSHOT:
                self.gap_assets.discard(event.asset_id)
                self.gap_conditions.discard(event.condition_id)
        elif isinstance(event, TradeEvent):
            self.trades.append(event)
            self._fill_resting_from_trade(event)
        self.events.append(event)

    def view(self, ts_local_ns: int) -> PointInTimeView:
        """Create a point-in-time strategy view."""
        return PointInTimeView(
            ts_local_ns=ts_local_ns,
            _book=self.book,
            _trades=tuple(self.trades),
            _events=tuple(self.events),
            _gap_active_assets=frozenset(self.gap_assets),
            _gap_active_all=bool(self.gap_conditions),
        )

    def schedule_order(
        self,
        request: OrderRequest,
        *,
        decision_ts_local_ns: int,
        config: BacktestConfig,
    ) -> None:
        """Schedule an order if the book is trusted at decision time."""
        if self._gap_active(request.asset_id):
            return
        order_id = f"order-{self._next_order_id:06d}"
        self._next_order_id += 1
        latency_ns = config.latency_ms * 1_000_000
        self.pending.append(
            PendingOrder(
                order_id=order_id,
                request=request,
                decision_ts_local_ns=decision_ts_local_ns,
                arrival_ts_local_ns=decision_ts_local_ns + latency_ns,
            )
        )

    def mark(self, ts_local_ns: int) -> None:
        """Record position and PnL state."""
        mtm = self.cash
        mark_valid = True
        for asset_id, position in self.positions.items():
            if position == Decimal("0"):
                continue
            if self._gap_active(asset_id):
                mark_valid = False
            mark_price = self.book.mark_price(
                asset_id=asset_id,
                position=position,
                mode=self.marking_mode,
            )
            mtm += position * (mark_price or Decimal("0"))
        self.pnl_series.append(
            PositionMark.create(
                ts_local_ns=ts_local_ns,
                cash=self.cash,
                positions=self.positions,
                mark_to_market=mtm,
                pnl=mtm,
                pnl_valid=mark_valid,
            )
        )

    def _arrive(self, order: PendingOrder) -> None:
        if self._gap_active(order.request.asset_id):
            return
        if self.book.is_marketable(order.request):
            remaining = self._fill_marketable(order)
            if remaining > Decimal("0") and order.request.order_type == OrderType.LIMIT:
                self._rest(order, remaining)
            return
        if order.request.order_type == OrderType.LIMIT:
            self._rest(order, order.request.size)

    def _rest(self, order: PendingOrder, remaining: Decimal) -> None:
        if order.request.limit_price is None:
            return
        self.resting.append(
            RestingOrder(
                order_id=order.order_id,
                request=order.request,
                decision_ts_local_ns=order.decision_ts_local_ns,
                arrival_ts_local_ns=order.arrival_ts_local_ns,
                remaining_size=remaining,
                queue_ahead=self.book.queue_ahead(order.request),
            )
        )

    def _fill_marketable(self, order: PendingOrder) -> Decimal:
        remaining = order.request.size
        for level, fill_size in self.book.walk(order.request):
            self._record_fill(
                order_id=order.order_id,
                request=order.request,
                price=level.price,
                size=fill_size,
                liquidity=LiquidityFlag.TAKER,
                decision_ts_local_ns=order.decision_ts_local_ns,
                arrival_ts_local_ns=order.arrival_ts_local_ns,
                execution_ts_local_ns=order.arrival_ts_local_ns,
                source_trade_id=None,
            )
            remaining -= fill_size
            if remaining <= Decimal("0"):
                return Decimal("0")
        return remaining

    def _fill_resting_from_trade(self, trade: TradeEvent) -> None:
        if self._gap_active(trade.asset_id):
            return
        still_resting: list[RestingOrder] = []
        for order in self.resting:
            if order.request.asset_id != trade.asset_id or not _trade_reaches_order(order, trade):
                still_resting.append(order)
                continue
            trade_volume_left = trade.size
            if order.queue_ahead > Decimal("0"):
                consumed_ahead = min(order.queue_ahead, trade_volume_left)
                order.queue_ahead -= consumed_ahead
                trade_volume_left -= consumed_ahead
            if trade_volume_left > Decimal("0"):
                fill_size = min(order.remaining_size, trade_volume_left)
                self._record_fill(
                    order_id=order.order_id,
                    request=order.request,
                    price=order.request.limit_price or trade.price,
                    size=fill_size,
                    liquidity=LiquidityFlag.MAKER,
                    decision_ts_local_ns=order.decision_ts_local_ns,
                    arrival_ts_local_ns=order.arrival_ts_local_ns,
                    execution_ts_local_ns=trade.ts_local_ns,
                    source_trade_id=trade.trade_id,
                )
                order.remaining_size -= fill_size
            if order.remaining_size > Decimal("0"):
                still_resting.append(order)
        self.resting = still_resting

    def _record_fill(
        self,
        *,
        order_id: str,
        request: OrderRequest,
        price: Decimal,
        size: Decimal,
        liquidity: LiquidityFlag,
        decision_ts_local_ns: int,
        arrival_ts_local_ns: int,
        execution_ts_local_ns: int,
        source_trade_id: str | None,
    ) -> None:
        cost = self.cost_model.calculate(price=price, size=size, liquidity=liquidity)
        self.cash += signed_cash_delta(request.side, cost)
        self.positions[request.asset_id] = self.positions.get(
            request.asset_id,
            Decimal("0"),
        ) + signed_position_delta(request.side, size)
        self.fills.append(
            Fill(
                order_id=order_id,
                client_order_id=request.client_order_id,
                asset_id=request.asset_id,
                side=request.side,
                price=price,
                size=size,
                liquidity=liquidity,
                decision_ts_local_ns=decision_ts_local_ns,
                arrival_ts_local_ns=arrival_ts_local_ns,
                execution_ts_local_ns=execution_ts_local_ns,
                notional=cost.notional,
                fee=cost.fee,
                gas=cost.gas,
                total_cost=cost.total,
                source_trade_id=source_trade_id,
            )
        )

    def _apply_gap(self, event: GapEvent) -> None:
        if event.asset_id is None:
            self.gap_conditions.add(event.condition_id)
            self.resting = []
            return
        self.gap_assets.add(event.asset_id)
        self.resting = [
            order for order in self.resting if order.request.asset_id != event.asset_id
        ]

    def _gap_active(self, asset_id: str) -> bool:
        return asset_id in self.gap_assets or bool(self.gap_conditions)


def _trade_reaches_order(order: RestingOrder, trade: TradeEvent) -> bool:
    if order.request.limit_price is None:
        return False
    taker_side = trade.taker_side.lower() if trade.taker_side is not None else None
    if order.request.side == OrderSide.BUY:
        return trade.price <= order.request.limit_price and taker_side in {None, "sell"}
    return trade.price >= order.request.limit_price and taker_side in {None, "buy"}
