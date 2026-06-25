from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from pmbot.backtest.costs import CostConfig, CostModel, LiquidityFlag
from pmbot.backtest.engine import BacktestConfig, BacktestEngine, PointInTimeView
from pmbot.backtest.models import OrderRequest, OrderSide, OrderType
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import (
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    GapReason,
    TradeEvent,
)

BASE_TS = 1_800_000_000_000_000_000
ASSET_ID = "asset-yes"
CONDITION_ID = "condition-1"


@dataclass
class EmitOnce:
    order: OrderRequest
    emitted: bool = False

    def __call__(self, view: PointInTimeView) -> Iterable[OrderRequest]:
        if self.emitted:
            return ()
        self.emitted = True
        return (self.order,)


def test_look_ahead_view_cannot_access_future_events() -> None:
    future_query_results: list[tuple[int, int]] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        future_trades = view.trades(start_ns=view.ts_local_ns + 1, end_ns=view.ts_local_ns + 100)
        future_events = tuple(
            event
            for event in view.events(end_ns=view.ts_local_ns + 100)
            if event.ts_local_ns > view.ts_local_ns
        )
        future_query_results.append((len(future_trades), len(future_events)))
        return ()

    events = [
        *snapshot(BASE_TS, bids=[("0.49", "10")], asks=[("0.51", "10")]),
        trade(BASE_TS + 10, price="0.51", size="5", taker_side="buy", trade_id="future"),
    ]

    BacktestEngine().run(events, strategy)

    assert future_query_results
    assert all(result == (0, 0) for result in future_query_results)


def test_no_phantom_fill_when_resting_price_never_trades_through() -> None:
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("5"),
        limit_price=Decimal("0.49"),
    )
    events = [
        *snapshot(BASE_TS, bids=[("0.49", "10")], asks=[("0.51", "10")]),
        trade(BASE_TS + 1, price="0.60", size="100", taker_side="buy", trade_id="not-through"),
    ]

    result = BacktestEngine().run(events, EmitOnce(order))

    assert result.blotter == ()


def test_queue_realism_fills_only_after_ahead_volume_and_allows_partial() -> None:
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("10"),
        limit_price=Decimal("0.50"),
    )
    events = [
        *snapshot(BASE_TS, bids=[("0.50", "100")], asks=[("0.52", "20")]),
        trade(BASE_TS + 1, price="0.50", size="60", taker_side="sell", trade_id="ahead-1"),
        trade(BASE_TS + 2, price="0.50", size="45", taker_side="sell", trade_id="partial-fill"),
    ]

    result = BacktestEngine().run(events, EmitOnce(order))

    assert len(result.blotter) == 1
    fill = result.blotter[0]
    assert fill.size == Decimal("5")
    assert fill.price == Decimal("0.50")
    assert fill.source_trade_id == "partial-fill"
    assert fill.liquidity == LiquidityFlag.MAKER


def test_marketable_order_walks_depth_with_slippage_and_depth_cap() -> None:
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("12"),
        order_type=OrderType.MARKET,
    )
    events = [
        *snapshot(
            BASE_TS,
            bids=[("0.49", "20")],
            asks=[("0.51", "5"), ("0.52", "5")],
        ),
        *delta(BASE_TS + 1, asks=[("0.52", "5")]),
    ]

    result = BacktestEngine().run(events, EmitOnce(order))

    assert [fill.size for fill in result.blotter] == [Decimal("5"), Decimal("5")]
    assert [fill.price for fill in result.blotter] == [Decimal("0.51"), Decimal("0.52")]
    filled = sum((fill.size for fill in result.blotter), Decimal("0"))
    notional = sum((fill.notional for fill in result.blotter), Decimal("0"))
    assert filled == Decimal("10")
    assert notional / filled == Decimal("0.515")
    assert all(fill.liquidity == LiquidityFlag.TAKER for fill in result.blotter)


def test_latency_fills_against_arrival_book_not_decision_book() -> None:
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("10"),
        order_type=OrderType.MARKET,
    )
    events = [
        *snapshot(BASE_TS, bids=[("0.49", "20")], asks=[("0.50", "10")]),
        *delta(BASE_TS + 5_000_000, asks=[("0.50", "0"), ("0.70", "10")]),
        trade(BASE_TS + 10_000_000, price="0.70", size="1", taker_side="buy", trade_id="clock"),
    ]

    result = BacktestEngine(BacktestConfig(latency_ms=10)).run(events, EmitOnce(order))

    assert len(result.blotter) == 1
    assert result.blotter[0].price == Decimal("0.70")
    assert result.blotter[0].arrival_ts_local_ns == BASE_TS + 10_000_000


def test_cost_parity_uses_shared_cost_model() -> None:
    config = CostConfig(
        maker_fee_rate=Decimal("0.001"),
        taker_fee_rate=Decimal("0.01"),
        gas_per_fill=Decimal("0.02"),
    )
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("10"),
        order_type=OrderType.MARKET,
    )
    events = [
        *snapshot(BASE_TS, bids=[("0.49", "20")], asks=[("0.50", "10")]),
        trade(BASE_TS + 1, price="0.50", size="1", taker_side="buy", trade_id="clock"),
    ]

    result = BacktestEngine(BacktestConfig(costs=config)).run(events, EmitOnce(order))
    fill = result.blotter[0]
    expected = CostModel(config).calculate(
        price=Decimal("0.50"),
        size=Decimal("10"),
        liquidity=LiquidityFlag.TAKER,
    )

    assert fill.notional == expected.notional
    assert fill.fee == expected.fee
    assert fill.gas == expected.gas
    assert fill.total_cost == expected.total


def test_gap_span_blocks_fills_and_invalidates_pnl() -> None:
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("10"),
        order_type=OrderType.MARKET,
    )
    events = [
        *snapshot(BASE_TS, bids=[("0.49", "20")], asks=[("0.50", "10")]),
        gap(BASE_TS + 1_000_000),
        trade(BASE_TS + 2_000_000, price="0.50", size="10", taker_side="buy", trade_id="gap-trade"),
    ]

    result = BacktestEngine(BacktestConfig(latency_ms=2)).run(events, EmitOnce(order))

    assert result.blotter == ()
    assert result.pnl_valid is False
    assert result.pnl_series[-1].pnl_valid is False


def test_deterministic_replay_produces_identical_blotters() -> None:
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("10"),
        order_type=OrderType.MARKET,
    )
    events = [
        *snapshot(BASE_TS, bids=[("0.49", "20")], asks=[("0.51", "5"), ("0.52", "5")]),
        trade(BASE_TS + 1, price="0.51", size="1", taker_side="buy", trade_id="clock"),
    ]

    first = BacktestEngine().run(events, EmitOnce(order))
    second = BacktestEngine().run(events, EmitOnce(order))

    assert first.blotter == second.blotter


def test_replay_from_parquet_fixture_outputs_blotter_and_pnl(tmp_path) -> None:
    recorder = ParquetRecorder(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
    events = [
        *snapshot(BASE_TS, bids=[("0.49", "20")], asks=[("0.51", "5"), ("0.52", "5")]),
        trade(BASE_TS + 1, price="0.51", size="1", taker_side="buy", trade_id="clock"),
    ]
    recorder.record_many(events)
    recorder.flush_all()
    order = OrderRequest(
        asset_id=ASSET_ID,
        side=OrderSide.BUY,
        size=Decimal("5"),
        order_type=OrderType.MARKET,
    )

    result = BacktestEngine().run_from_data_dir(tmp_path, EmitOnce(order))

    assert len(result.blotter) == 1
    assert result.blotter[0].price == Decimal("0.51")
    assert result.pnl_series
    assert result.pnl_series[-1].positions[ASSET_ID] == Decimal("5")


def snapshot(
    ts: int,
    *,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
) -> list[BookEvent]:
    events: list[BookEvent] = []
    for price, size in bids:
        events.append(book_event(ts, BookEventType.SNAPSHOT, BookSide.BID, price, size))
    for price, size in asks:
        events.append(book_event(ts, BookEventType.SNAPSHOT, BookSide.ASK, price, size))
    return events


def delta(
    ts: int,
    *,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> list[BookEvent]:
    events: list[BookEvent] = []
    for price, size in bids or []:
        events.append(book_event(ts, BookEventType.DELTA, BookSide.BID, price, size))
    for price, size in asks or []:
        events.append(book_event(ts, BookEventType.DELTA, BookSide.ASK, price, size))
    return events


def book_event(
    ts: int,
    event_type: BookEventType,
    side: BookSide,
    price: str,
    size: str,
) -> BookEvent:
    return BookEvent(
        ts_local_ns=ts,
        ts_monotonic_ns=None,
        ts_exchange=None,
        condition_id=CONDITION_ID,
        asset_id=ASSET_ID,
        event_type=event_type,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        seq_or_hash=f"{event_type.value}-{ts}",
    )


def trade(
    ts: int,
    *,
    price: str,
    size: str,
    taker_side: str,
    trade_id: str,
) -> TradeEvent:
    return TradeEvent(
        ts_local_ns=ts,
        ts_monotonic_ns=None,
        ts_exchange=None,
        condition_id=CONDITION_ID,
        asset_id=ASSET_ID,
        price=Decimal(price),
        size=Decimal(size),
        taker_side=taker_side,
        trade_id=trade_id,
    )


def gap(ts: int) -> GapEvent:
    return GapEvent(
        ts_local_ns=ts,
        ts_monotonic_ns=None,
        ts_exchange=None,
        condition_id=CONDITION_ID,
        asset_id=ASSET_ID,
        reason=GapReason.CONNECTION_LOST,
        details="fixture gap",
        seq_or_hash=None,
    )
