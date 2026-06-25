from decimal import Decimal

from pmbot.backtest.costs import LiquidityFlag
from pmbot.backtest.engine import BacktestResult
from pmbot.backtest.models import Fill, OrderSide, PositionMark
from pmbot.eval.models import CrossVenueEvaluationObservation, FillAttribution, ParameterCandidate
from pmbot.eval.pnl import pnl_metrics
from pmbot.eval.shortfall import implementation_shortfall
from pmbot.eval.sweep import sweep_train_test


def test_pnl_metrics_ignore_invalid_gap_spans() -> None:
    result = BacktestResult(
        blotter=(),
        pnl_series=(
            mark(1, "0", True),
            mark(2, "1", True),
            mark(3, "100", False),
            mark(4, "4", True),
            mark(5, "6", True),
        ),
        pnl_valid=False,
    )

    metrics = pnl_metrics(result)

    assert metrics.total_pnl == Decimal("3")
    assert metrics.n_valid_marks == 4
    assert metrics.n_invalid_marks == 1
    assert metrics.max_drawdown == Decimal("0")


def test_implementation_shortfall_decomposition_sums_back_to_total_pnl() -> None:
    row = FillAttribution(
        fill=fill(price="0.48", size="10"),
        arrival_mid=Decimal("0.50"),
        fair_value=Decimal("0.60"),
        realized_value=Decimal("1"),
    )

    summary = implementation_shortfall([row])

    assert summary.total_pnl == Decimal("5.20")
    assert summary.alpha_pnl == Decimal("1.00")
    assert summary.execution_pnl == Decimal("0.20")
    assert summary.execution_cost == Decimal("-0.20")
    assert summary.residual_pnl == Decimal("4.00")
    assert summary.alpha_pnl + summary.execution_pnl + summary.residual_pnl == summary.total_pnl


def test_sweep_selects_parameters_on_train_only_then_validates_on_test() -> None:
    candidates = (
        ParameterCandidate(
            double_counting_overlap=Decimal("0"),
            confidence_scalar=Decimal("1"),
        ),
        ParameterCandidate(
            double_counting_overlap=Decimal("1"),
            confidence_scalar=Decimal("1"),
        ),
    )
    train = (
        cross_venue_row("train-1", polymarket="0.50", kalshi="0.80", realized=1),
        cross_venue_row("train-2", polymarket="0.50", kalshi="0.80", realized=1),
    )
    test = (
        cross_venue_row("test-1", polymarket="0.50", kalshi="0.80", realized=0),
        cross_venue_row("test-2", polymarket="0.50", kalshi="0.80", realized=0),
    )

    result = sweep_train_test(train=train, test=test, candidates=candidates)

    assert result.selected.double_counting_overlap == Decimal("0")
    assert set(result.train_performance.evaluated_market_ids) == {"train-1", "train-2"}
    assert set(result.test_performance.evaluated_market_ids) == {"test-1", "test-2"}
    assert result.test_performance.brier_skill < result.train_performance.brier_skill


def mark(ts: int, pnl: str, valid: bool) -> PositionMark:
    return PositionMark.create(
        ts_local_ns=ts,
        cash=Decimal("0"),
        positions={},
        mark_to_market=Decimal(pnl),
        pnl=Decimal(pnl),
        pnl_valid=valid,
    )


def fill(*, price: str, size: str) -> Fill:
    notional = Decimal(price) * Decimal(size)
    return Fill(
        order_id="order-1",
        client_order_id=None,
        asset_id="asset-1",
        side=OrderSide.BUY,
        price=Decimal(price),
        size=Decimal(size),
        liquidity=LiquidityFlag.TAKER,
        decision_ts_local_ns=1,
        arrival_ts_local_ns=2,
        execution_ts_local_ns=3,
        notional=notional,
        fee=Decimal("0"),
        gas=Decimal("0"),
        total_cost=Decimal("0"),
        source_trade_id=None,
    )


def cross_venue_row(
    market_id: str,
    *,
    polymarket: str,
    kalshi: str,
    realized: int,
) -> CrossVenueEvaluationObservation:
    return CrossVenueEvaluationObservation(
        market_id=market_id,
        ts_local_ns=1,
        polymarket_probability=Decimal(polymarket),
        kalshi_probability=Decimal(kalshi),
        realized=realized,
    )
