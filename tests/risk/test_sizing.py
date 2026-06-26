from decimal import Decimal

import pytest

from pmbot.backtest.costs import CostConfig, CostModel
from pmbot.risk.sizing import (
    BinarySide,
    ExecutableQuote,
    Sizer,
    SizingConfig,
    SizingMode,
    ValidationToken,
    independent_binary_kelly_fractions,
    lower_net_ev_quantile,
    no_kelly_fraction,
    optimize_mutually_exclusive_scenario,
    yes_kelly_fraction,
)


def test_side_specific_kelly_uses_executable_yes_and_no_asks() -> None:
    yes_fraction = yes_kelly_fraction(Decimal("0.60"), Decimal("0.55"))
    no_fraction = no_kelly_fraction(Decimal("0.40"), Decimal("0.55"))
    wrong_no_from_yes_formula = yes_kelly_fraction(Decimal("0.40"), Decimal("0.55"))

    assert yes_fraction.quantize(Decimal("0.000001")) == Decimal("0.111111")
    assert no_fraction.quantize(Decimal("0.000001")) == Decimal("0.111111")
    assert wrong_no_from_yes_formula == Decimal("0")


def test_scenario_optimizer_differs_from_naive_per_leg_kelly() -> None:
    probabilities = (Decimal("0.50"), Decimal("0.30"), Decimal("0.20"))
    executable_costs = (Decimal("0.40"), Decimal("0.25"), Decimal("0.15"))

    naive = independent_binary_kelly_fractions(
        probabilities=probabilities,
        costs=executable_costs,
    )
    optimized = optimize_mutually_exclusive_scenario(
        probabilities=probabilities,
        costs=executable_costs,
        lambda_fraction=Decimal("0.25"),
        exposure_cap=Decimal("0.60"),
        grid_step=Decimal("0.01"),
    )

    assert optimized.full_kelly_fractions != naive
    assert sum(optimized.full_kelly_fractions, Decimal("0")) <= Decimal("0.60")
    assert sum(optimized.target_fractions, Decimal("0")) <= Decimal("0.15")
    assert optimized.expected_log_growth > 0


def test_net_ev_quantile_gate_uses_forecast_uncertainty_and_round_trip_costs() -> None:
    cost_model = CostModel(CostConfig.zero())

    positive_lower_bound = lower_net_ev_quantile(
        probability_samples=(Decimal("0.62"), Decimal("0.63"), Decimal("0.66")),
        executable_price=Decimal("0.55"),
        exit_price=Decimal("0.55"),
        cost_model=cost_model,
        quantile=Decimal("0.05"),
    )
    negative_lower_bound = lower_net_ev_quantile(
        probability_samples=(Decimal("0.54"), Decimal("0.56"), Decimal("0.62")),
        executable_price=Decimal("0.55"),
        exit_price=Decimal("0.55"),
        cost_model=cost_model,
        quantile=Decimal("0.05"),
    )

    assert positive_lower_bound == Decimal("0.07")
    assert negative_lower_bound == Decimal("-0.01")


def test_sizer_applies_uncertainty_gate_and_capacity_limits() -> None:
    sizer = Sizer(
        SizingConfig(
            bankroll=Decimal("1000"),
            cost_config=CostConfig.zero(),
            max_market_notional=Decimal("1000"),
            max_aggregate_notional=Decimal("1000"),
            scenario_exposure_cap=Decimal("1"),
            correlation_exposure_cap=Decimal("1"),
        )
    )

    allowed = sizer.size_side(
        p_hat=Decimal("0.64"),
        quote=ExecutableQuote(
            market_id="fixture",
            side=BinarySide.YES,
            ask=Decimal("0.55"),
            size_available=Decimal("1000"),
        ),
        probability_samples=(Decimal("0.62"), Decimal("0.64"), Decimal("0.66")),
    )
    blocked = sizer.size_side(
        p_hat=Decimal("0.64"),
        quote=ExecutableQuote(
            market_id="fixture",
            side=BinarySide.YES,
            ask=Decimal("0.55"),
            size_available=Decimal("1000"),
        ),
        probability_samples=(Decimal("0.54"), Decimal("0.64"), Decimal("0.66")),
    )

    assert allowed.allowed is True
    assert allowed.notional > Decimal("0")
    assert blocked.allowed is False
    assert blocked.notional == Decimal("0")
    assert blocked.reason == "lower net-EV quantile is non-positive"


def test_live_sizing_requires_wp4_validation_token() -> None:
    config = SizingConfig(
        bankroll=Decimal("1000"),
        mode=SizingMode.LIVE,
        cost_config=CostConfig.zero(),
    )
    sizer = Sizer(config)

    with pytest.raises(PermissionError, match="WP-4 dual-go validation token"):
        sizer.size_side(
            p_hat=Decimal("0.64"),
            quote=ExecutableQuote(
                market_id="fixture",
                side=BinarySide.YES,
                ask=Decimal("0.55"),
                size_available=Decimal("10"),
            ),
            probability_samples=(Decimal("0.64"),),
        )


def test_live_sizing_with_validation_token_can_emit_fixture_size() -> None:
    config = SizingConfig(
        bankroll=Decimal("1000"),
        mode=SizingMode.LIVE,
        validation_token=ValidationToken("fixture-wp4-pass"),
        cost_config=CostConfig.zero(),
        max_market_notional=Decimal("1000"),
        max_aggregate_notional=Decimal("1000"),
        scenario_exposure_cap=Decimal("1"),
        correlation_exposure_cap=Decimal("1"),
    )

    decision = Sizer(config).size_side(
        p_hat=Decimal("0.64"),
        quote=ExecutableQuote(
            market_id="fixture",
            side=BinarySide.YES,
            ask=Decimal("0.55"),
            size_available=Decimal("1000"),
        ),
        probability_samples=(Decimal("0.62"), Decimal("0.64"), Decimal("0.66")),
    )

    assert decision.allowed is True
    assert decision.side == BinarySide.YES
    assert decision.notional > Decimal("0")
