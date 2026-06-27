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
    LeadLagSample,
    RegressionFeatures,
    ShrinkageRegressionObservation,
    SignalDirection,
    fit_shrinkage_regression,
    fit_shrinkage_train_test,
    guarded_log_odds_pool,
    lead_lag_diagnostics,
)

BASE_TS = 1_800_000_000_000_000_000
PM_CONDITION_ID = "pm-condition-1"
PM_ASSET_ID = "pm-yes-token"
PM_NO_ASSET_ID = "pm-no-token"
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
    polymarket_no_asset_id=PM_NO_ASSET_ID,
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
            *pm_no_snapshot(BASE_TS, bid="0.47", ask="0.53", size="10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
        ],
        strategy,
    )

    assert observed[-1] == Decimal("0.104356076261")


def test_kalshi_probability_uses_midpoint_not_dollar_weighted_microprice() -> None:
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0"),
            trade_threshold=Decimal("0.01"),
        ),
    )
    observed: list[Decimal] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if values:
            observed.append(values[0].kalshi_probability)
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.58", ask="0.59", size="10"),
            book_event(BASE_TS + 1, KALSHI_TICKER, KALSHI_TICKER, BookSide.BID, "0.58", "17644.81"),
            book_event(
                BASE_TS + 1,
                KALSHI_TICKER,
                KALSHI_TICKER,
                BookSide.ASK,
                "0.59",
                "822609.57",
            ),
        ],
        strategy,
    )

    assert observed[-1] == Decimal("0.585")


def test_cross_venue_probabilities_use_midpoints_for_both_venues() -> None:
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0"),
            trade_threshold=Decimal("0.01"),
        ),
    )
    observed: list[tuple[Decimal, Decimal]] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if values:
            observed.append(
                (
                    values[0].polymarket_probability,
                    values[0].kalshi_probability,
                )
            )
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            book_event(BASE_TS, PM_CONDITION_ID, PM_ASSET_ID, BookSide.BID, "0.40", "10"),
            book_event(BASE_TS, PM_CONDITION_ID, PM_ASSET_ID, BookSide.ASK, "0.60", "30"),
            book_event(BASE_TS + 1, KALSHI_TICKER, KALSHI_TICKER, BookSide.BID, "0.58", "1"),
            book_event(BASE_TS + 1, KALSHI_TICKER, KALSHI_TICKER, BookSide.ASK, "0.59", "100"),
        ],
        strategy,
    )

    assert observed[-1] == (Decimal("0.50"), Decimal("0.585"))


def test_wide_bid_ask_spread_marks_signal_low_confidence_hold() -> None:
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0"),
            trade_threshold=Decimal("0.01"),
            max_bid_ask_spread=Decimal("0.05"),
        ),
    )
    observed = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if values:
            observed.append(values[0])
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            book_event(BASE_TS, PM_CONDITION_ID, PM_ASSET_ID, BookSide.BID, "0.39", "10"),
            book_event(BASE_TS, PM_CONDITION_ID, PM_ASSET_ID, BookSide.ASK, "0.51", "10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
        ],
        strategy,
    )

    value = observed[-1]
    assert value.yes_edge is not None
    assert value.yes_edge > Decimal("0.01")
    assert value.direction == SignalDirection.HOLD
    assert value.confidence == Decimal("0")
    assert "Polymarket spread 0.12 exceeds 0.05" in value.rationale


def test_incoherent_three_way_probability_sum_marks_match_low_confidence_hold() -> None:
    match_pairs = (
        VerifiedContractPair(
            polymarket_condition_id="pm-rsa",
            polymarket_asset_id="pm-rsa-yes",
            kalshi_ticker="KXWCGAME-26JUN27RSACAN-RSA",
            rationale="fixture",
        ),
        VerifiedContractPair(
            polymarket_condition_id="pm-can",
            polymarket_asset_id="pm-can-yes",
            kalshi_ticker="KXWCGAME-26JUN27RSACAN-CAN",
            rationale="fixture",
        ),
        VerifiedContractPair(
            polymarket_condition_id="pm-tie",
            polymarket_asset_id="pm-tie-yes",
            kalshi_ticker="KXWCGAME-26JUN27RSACAN-TIE",
            rationale="fixture",
        ),
    )
    signal = CrossVenueSignal(
        match_pairs,
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0"),
            trade_threshold=Decimal("0.01"),
            max_bid_ask_spread=Decimal("0.05"),
            three_way_sum_tolerance=Decimal("0.05"),
        ),
    )
    observed = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if len(values) == 3:
            observed.append(values)
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *asset_snapshot(BASE_TS, "pm-rsa", "pm-rsa-yes", bid="0.34", ask="0.36"),
            *asset_snapshot(BASE_TS, "pm-can", "pm-can-yes", bid="0.44", ask="0.46"),
            *asset_snapshot(BASE_TS, "pm-tie", "pm-tie-yes", bid="0.19", ask="0.21"),
            *asset_snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN27RSACAN-RSA",
                "KXWCGAME-26JUN27RSACAN-RSA",
                bid="0.59",
                ask="0.61",
            ),
            *asset_snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN27RSACAN-CAN",
                "KXWCGAME-26JUN27RSACAN-CAN",
                bid="0.59",
                ask="0.61",
            ),
            *asset_snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN27RSACAN-TIE",
                "KXWCGAME-26JUN27RSACAN-TIE",
                bid="0.10",
                ask="0.12",
            ),
        ],
        strategy,
    )

    latest = observed[-1]
    assert {value.direction for value in latest} == {SignalDirection.HOLD}
    assert {value.confidence for value in latest} == {Decimal("0")}
    assert all(
        "Kalshi three-way probability sum 1.31 outside 1+/-0.05" in value.rationale
        for value in latest
    )


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
            *pm_no_snapshot(BASE_TS, bid="0.47", ask="0.53", size="10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
        ],
        strategy,
    )

    assert direct[-1] == Decimal("0.104356076261")
    assert inverted[-1] == Decimal("-0.104356076261")
    assert direct[-1] == -inverted[-1]
    assert inverted_directions[-1] == SignalDirection.SELL


def test_no_side_trade_gate_uses_no_ask_and_one_minus_p_hat() -> None:
    signal = CrossVenueSignal(
        (PAIR_INVERTED,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0.50"),
            trade_threshold=Decimal("0.01"),
        ),
    )
    observed: list[tuple[SignalDirection, Decimal | None, Decimal | None]] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if values:
            observed.append((values[0].direction, values[0].polymarket_no_ask, values[0].no_edge))
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10"),
            *pm_no_snapshot(BASE_TS, bid="0.44", ask="0.55", size="10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
        ],
        strategy,
    )

    direction, no_ask, no_edge = observed[-1]
    assert direction == SignalDirection.SELL
    assert no_ask == Decimal("0.55")
    assert no_edge is not None
    assert no_edge.quantize(Decimal("0.000000000001")) == Decimal("0.054356076261")


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


def test_signal_requires_fresh_exchange_time_not_local_capture_time() -> None:
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            kalshi_weight=Decimal("1"),
            double_counting_overlap=Decimal("0"),
            trade_threshold=Decimal("0.01"),
            max_quote_age_ns=10,
        ),
    )
    observed: list[Decimal] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if values:
            observed.append(values[0].fair_probability)
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10", exchange_ts=100),
            *kalshi_snapshot(
                BASE_TS + 1_000_000,
                bid="0.69",
                ask="0.71",
                size="10",
                exchange_ts=1_000_000_000_000,
            ),
        ],
        strategy,
    )

    assert observed == []


def test_fitted_shrinkage_regression_recovers_known_beta() -> None:
    observations = tuple(
        regression_row(pm, kalshi, beta=Decimal("0.40"))
        for pm, kalshi in (
            ("0.35", "0.40"),
            ("0.42", "0.50"),
            ("0.50", "0.55"),
            ("0.58", "0.65"),
            ("0.64", "0.70"),
            ("0.72", "0.76"),
        )
    )

    model = fit_shrinkage_regression(observations)
    report = model.restriction_report(test=observations)

    assert model.alpha.copy_abs() < Decimal("0.000000001")
    assert model.gamma.quantize(Decimal("0.000001")) == Decimal("1.000000")
    assert model.beta.quantize(Decimal("0.000001")) == Decimal("0.400000")
    assert report.holds is True
    assert report.out_of_sample_brier_skill is not None
    assert report.out_of_sample_brier_skill > 0


def test_shrinkage_train_test_helper_fits_on_train_and_reports_test() -> None:
    train = tuple(
        regression_row(pm, kalshi, beta=Decimal("0.40"))
        for pm, kalshi in (
            ("0.35", "0.40"),
            ("0.42", "0.50"),
            ("0.50", "0.55"),
            ("0.58", "0.65"),
            ("0.64", "0.70"),
            ("0.72", "0.76"),
        )
    )
    test = tuple(
        regression_row(pm, kalshi, beta=Decimal("0.40"))
        for pm, kalshi in (("0.38", "0.44"), ("0.62", "0.68"), ("0.75", "0.80"))
    )

    result = fit_shrinkage_train_test(train=train, test=test)

    assert result.model.beta.quantize(Decimal("0.000001")) == Decimal("0.400000")
    assert result.train_report.n_test == len(train)
    assert result.test_report.n_test == len(test)
    assert result.test_report.out_of_sample_brier_skill is not None


def test_signal_uses_fitted_regression_when_available() -> None:
    model = fit_shrinkage_regression(
        tuple(
            regression_row(pm, kalshi, beta=Decimal("0.25"))
            for pm, kalshi in (
                ("0.35", "0.40"),
                ("0.42", "0.50"),
                ("0.50", "0.55"),
                ("0.58", "0.65"),
                ("0.64", "0.70"),
                ("0.72", "0.76"),
            )
        )
    )
    signal = CrossVenueSignal(
        (PAIR,),
        CrossVenueSignalConfig(
            shrinkage_model=model,
            trade_threshold=Decimal("0.01"),
        ),
    )
    observed: list[tuple[str, Decimal, Decimal]] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        values = signal.compute(view)
        if values:
            observed.append(
                (
                    values[0].model_status,
                    values[0].resolution_probability,
                    values[0].convergence_logit_delta,
                )
            )
        return ()

    BacktestEngine(zero_cost_backtest_config()).run(
        [
            *pm_snapshot(BASE_TS, bid="0.49", ask="0.51", size="10"),
            *kalshi_snapshot(BASE_TS + 1, bid="0.69", ask="0.71", size="10"),
        ],
        strategy,
    )

    model_status, resolution_probability, convergence_delta = observed[-1]
    assert model_status == "fitted_shrinkage_regression"
    assert resolution_probability.quantize(Decimal("0.000000000001")) == Decimal(
        "0.552758990724"
    )
    assert convergence_delta > Decimal("0")


def test_lead_lag_placebos_do_not_invert_a_kalshi_leads_fixture() -> None:
    diagnostics = lead_lag_diagnostics(
        (
            lead_lag_row(1, "0.45", "0.55", "0.50", "0.55"),
            lead_lag_row(2, "0.55", "0.45", "0.50", "0.45"),
            lead_lag_row(3, "0.45", "0.55", "0.50", "0.55"),
            lead_lag_row(4, "0.55", "0.45", "0.50", "0.45"),
        ),
        polling_stride=3,
    )

    assert diagnostics.forward_score > 0
    assert diagnostics.reverse_placebo_score <= 0
    assert diagnostics.shifted_time_placebo_score <= diagnostics.forward_score * 0.25
    assert diagnostics.polling_frequency_score > 0
    assert diagnostics.passes_placebos is True


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


def pm_snapshot(
    ts: int,
    *,
    bid: str,
    ask: str,
    size: str,
    exchange_ts: int | None = None,
) -> tuple[BookEvent, BookEvent]:
    return (
        book_event(
            ts,
            PM_CONDITION_ID,
            PM_ASSET_ID,
            BookSide.BID,
            bid,
            size,
            exchange_ts=exchange_ts,
        ),
        book_event(
            ts,
            PM_CONDITION_ID,
            PM_ASSET_ID,
            BookSide.ASK,
            ask,
            size,
            exchange_ts=exchange_ts,
        ),
    )


def pm_no_snapshot(
    ts: int,
    *,
    bid: str,
    ask: str,
    size: str,
    exchange_ts: int | None = None,
) -> tuple[BookEvent, BookEvent]:
    return (
        book_event(
            ts,
            PM_CONDITION_ID,
            PM_NO_ASSET_ID,
            BookSide.BID,
            bid,
            size,
            exchange_ts=exchange_ts,
        ),
        book_event(
            ts,
            PM_CONDITION_ID,
            PM_NO_ASSET_ID,
            BookSide.ASK,
            ask,
            size,
            exchange_ts=exchange_ts,
        ),
    )


def kalshi_snapshot(
    ts: int,
    *,
    bid: str,
    ask: str,
    size: str,
    exchange_ts: int | None = None,
) -> tuple[BookEvent, BookEvent]:
    return (
        book_event(
            ts,
            KALSHI_TICKER,
            KALSHI_TICKER,
            BookSide.BID,
            bid,
            str(Decimal(size) * Decimal(bid)),
            exchange_ts=exchange_ts,
        ),
        book_event(
            ts,
            KALSHI_TICKER,
            KALSHI_TICKER,
            BookSide.ASK,
            ask,
            str(Decimal(size) * Decimal(ask)),
            exchange_ts=exchange_ts,
        ),
    )


def asset_snapshot(
    ts: int,
    condition_id: str,
    asset_id: str,
    *,
    bid: str,
    ask: str,
    exchange_ts: int | None = None,
) -> tuple[BookEvent, BookEvent]:
    return (
        book_event(
            ts,
            condition_id,
            asset_id,
            BookSide.BID,
            bid,
            "10",
            exchange_ts=exchange_ts,
        ),
        book_event(
            ts,
            condition_id,
            asset_id,
            BookSide.ASK,
            ask,
            "10",
            exchange_ts=exchange_ts,
        ),
    )


def book_event(
    ts: int,
    condition_id: str,
    asset_id: str,
    side: BookSide,
    price: str,
    size: str,
    exchange_ts: int | None = None,
) -> BookEvent:
    return BookEvent(
        ts_local_ns=ts,
        ts_monotonic_ns=None,
        ts_exchange=str(ts if exchange_ts is None else exchange_ts),
        condition_id=condition_id,
        asset_id=asset_id,
        event_type=BookEventType.SNAPSHOT,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        seq_or_hash=f"{asset_id}-{ts}",
    )


def regression_row(
    polymarket: str,
    kalshi: str,
    *,
    beta: Decimal,
) -> ShrinkageRegressionObservation:
    pm_probability = Decimal(polymarket)
    kalshi_probability = Decimal(kalshi)
    target_logit = logit(pm_probability) + beta * (
        logit(kalshi_probability) - logit(pm_probability)
    )
    return ShrinkageRegressionObservation(
        polymarket_probability=pm_probability,
        kalshi_probability=kalshi_probability,
        target_probability=inv_logit(target_logit),
        features=RegressionFeatures(),
    )


def lead_lag_row(
    ts: int,
    polymarket: str,
    kalshi: str,
    next_polymarket: str,
    next_kalshi: str,
) -> LeadLagSample:
    return LeadLagSample(
        ts_exchange_ns=ts,
        polymarket_probability=Decimal(polymarket),
        kalshi_probability=Decimal(kalshi),
        next_polymarket_probability=Decimal(next_polymarket),
        next_kalshi_probability=Decimal(next_kalshi),
    )


def logit(probability: Decimal) -> Decimal:
    return (probability / (Decimal("1") - probability)).ln()


def inv_logit(log_odds: Decimal) -> Decimal:
    return Decimal("1") / (Decimal("1") + (-log_odds).exp())
