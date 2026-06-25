from collections.abc import Iterable
from decimal import Decimal

from pmbot.backtest.costs import CostConfig
from pmbot.backtest.engine import BacktestConfig, BacktestEngine, PointInTimeView
from pmbot.backtest.models import OrderRequest, OrderSide, OrderType
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide, TradeEvent
from pmbot.signals.contracts import VerifiedContractPair
from pmbot.signals.cross_venue import (
    CrossVenueSignal,
    CrossVenueSignalConfig,
    SignalDirection,
    guarded_log_odds_pool,
)

BASE_TS = 1_800_000_000_000_000_000
PM_CONDITION_ID = "pm-condition-1"
PM_ASSET_ID = "pm-yes-token"
KALSHI_TICKER = "KXTEST-YES"
PAIR = VerifiedContractPair(
    polymarket_condition_id=PM_CONDITION_ID,
    polymarket_asset_id=PM_ASSET_ID,
    kalshi_ticker=KALSHI_TICKER,
    rationale="fixture pair",
)
PAIR_INVERTED = VerifiedContractPair(
    polymarket_condition_id=PM_CONDITION_ID,
    polymarket_asset_id=PM_ASSET_ID,
    kalshi_ticker=KALSHI_TICKER,
    rationale="fixture pair with Kalshi YES inverted",
    invert_kalshi=True,
)


def test_signal_log_odds_double_counting_guard_matches_hand_computation() -> None:
    fair = guarded_log_odds_pool(
        polymarket_probability=Decimal("0.50"),
        kalshi_probability=Decimal("0.70"),
        kalshi_weight=Decimal("1"),
        double_counting_overlap=Decimal("0.50"),
    )

    assert fair.quantize(Decimal("0.000000000001")) == Decimal("0.604356076261")
    assert (fair - Decimal("0.50")).quantize(Decimal("0.000000000001")) == Decimal(
        "0.104356076261"
    )


def test_cross_venue_signal_produces_expected_deviation() -> None:
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0.50"),
            trade_threshold=Decimal("0.01"),
        ),
    )
    observed: list[Decimal] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if values:
            observed.append(values[0].signal_value.quantize(Decimal("0.000000000001")))
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
        ],
        strategy,
    )

    assert observed[-1] == Decimal("0.104356076261")


def test_cross_venue_signal_inverts_kalshi_probability_before_pooling() -> None:
    direct_signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0.50"),
            trade_threshold=Decimal("0.01"),
        ),
    )
    inverted_signal = CrossVenueSignal(
        (PAIR_INVERTED,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0.50"),
            trade_threshold=Decimal("0.01"),
        ),
    )
    direct: list[Decimal] = []
    inverted: list[Decimal] = []
    inverted_directions: list[SignalDirection] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        direct_values = direct_signal.compute(view)
        inverted_values = inverted_signal.compute(view)
        if direct_values and inverted_values:
            direct.append(direct_values[0].signal_value.quantize(Decimal("0.000000000001")))
            inverted.append(inverted_values[0].signal_value.quantize(Decimal("0.000000000001")))
            inverted_directions.append(inverted_values[0].direction)
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
        ],
        strategy,
    )

    assert direct[-1] == Decimal("0.104356076261")
    assert inverted[-1] == Decimal("-0.104356076261")
    assert direct[-1] == -inverted[-1]
    assert inverted_directions[-1] == SignalDirection.SELL


def test_cross_venue_signal_ignores_future_reference_data() -> None:
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0"),
            trade_threshold=Decimal("0.01"),
        ),
    )
    at_base: list[Decimal] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        if view.ts_local_ns == BASE_TS:
            values = signal.compute(view)
            if values:
                at_base.append(values[0].kalshi_probability)
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10"),
            *kalshi_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10"),
            *kalshi_snapshot(BASE_TS + 100, bid="0.79", ask="0.81", size="10"),
        ],
        strategy,
    )

    assert at_base == [Decimal("0.50")]


def test_signal_runs_through_backtester_fixture_and_emits_polymarket_order() -> None:
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0.50"),
            trade_threshold=Decimal("0.01"),
        ),
    )

    class SignalStrategy:
        def __init__(self) -> None:
            self.emitted = False
            self.last_signal = None

        def __call__(self, view: PointInTimeView) -> Iterable[OrderRequest]:
            values = signal.compute(view)
            if not values or self.emitted:
                return ()
            self.last_signal = values[0]
            if values[0].direction == SignalDirection.BUY:
                self.emitted = True
                return (
                    OrderRequest(
                        asset_id=PM_ASSET_ID,
                        side=OrderSide.BUY,
                        size=Decimal("2"),
                        order_type=OrderType.MARKET,
                    ),
                )
            return ()

    strategy = SignalStrategy()
    result = BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
            TradeEvent(
                ts_local_ns=BASE_TS + 2,
                ts_monotonic_ns=None,
                ts_exchange=None,
                condition_id=PM_CONDITION_ID,
                asset_id=PM_ASSET_ID,
                price=Decimal("0.51"),
                size=Decimal("1"),
                taker_side="buy",
                trade_id="clock",
            ),
        ],
        strategy,
    )

    assert strategy.last_signal is not None
    assert strategy.last_signal.direction == SignalDirection.BUY
    assert len(result.blotter) == 1
    assert result.blotter[0].asset_id == PM_ASSET_ID


def zero_cost_backtest_config() -> BacktestConfig:
    return BacktestConfig(latency_ms=0, costs=CostConfig.zero(), allow_zero_costs=True)


def pm_snapshot(ts: int, *, bid: str, ask: str, size: str) -> tuple[BookEvent, BookEvent]:
    return (
        book_event(ts, PM_CONDITION_ID, PM_ASSET_ID, BookSide.BID, bid, size),
        book_event(ts, PM_CONDITION_ID, PM_ASSET_ID, BookSide.ASK, ask, size),
    )


def kalshi_snapshot(ts: int, *, bid: str, ask: str, size: str) -> tuple[BookEvent, BookEvent]:
    return (
        book_event(ts, KALSHI_TICKER, KALSHI_TICKER, BookSide.BID, bid, size),
        book_event(ts, KALSHI_TICKER, KALSHI_TICKER, BookSide.ASK, ask, size),
    )


def book_event(
    ts: int,
    condition_id: str,
    asset_id: str,
    side: BookSide,
    price: str,
    size: str,
) -> BookEvent:
    return BookEvent(
        ts_local_ns=ts,
        ts_monotonic_ns=None,
        ts_exchange=None,
        condition_id=condition_id,
        asset_id=asset_id,
        event_type=BookEventType.SNAPSHOT,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        seq_or_hash=f"{asset_id}-{ts}",
    )
