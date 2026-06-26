"""WP-5 position sizing primitives.

The live sizing entry point is deliberately gated: fixture/paper sizing is
available by default, but nonzero live sizing requires a validation token that
attests WP-4's dual go/no-go passed on real data.

Sizing uses executable all-in prices, not mids. For binary markets the YES and
NO formulas are side-specific:

* buy YES: ``f* = (p_hat - q_yes) / (1 - q_yes)``
* buy NO: ``f* = ((1 - p_hat) - q_no) / (1 - q_no)``

For mutually exclusive outcomes, use scenario expected-log optimization over
the joint payout scenarios. The covariance/Sigma approximation is intentionally
not used here because exactly-one-wins tail structure matters.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum

from pmbot.backtest.costs import CostConfig, CostModel, LiquidityFlag

ZERO = Decimal("0")
ONE = Decimal("1")
MAX_POLICY_LAMBDA = Decimal("0.25")
DEFAULT_VALIDATION_SOURCE = "wp4_dual_go"


class SizingMode(StrEnum):
    """Sizing environment."""

    PAPER = "paper"
    LIVE = "live"


class BinarySide(StrEnum):
    """Binary prediction-market side to buy."""

    YES = "yes"
    NO = "no"


@dataclass(frozen=True, slots=True)
class ValidationToken:
    """Attestation that WP-4 validated the strategy on real data."""

    token: str
    source: str = DEFAULT_VALIDATION_SOURCE
    dual_go_passed: bool = True
    real_data: bool = True

    def is_valid(self) -> bool:
        """Return whether the token can unlock live sizing."""
        return (
            bool(self.token)
            and self.source == DEFAULT_VALIDATION_SOURCE
            and self.dual_go_passed
            and self.real_data
        )


@dataclass(frozen=True, slots=True)
class SizingConfig:
    """Policy and risk limits for WP-5 sizing.

    ``lambda_fraction`` is a risk policy parameter, not a theorem. It is capped
    at one-quarter Kelly to reflect misspecification, cross-trade estimation
    error, and drawdown tolerance. Stress it jointly with the net-EV uncertainty
    gate instead of blindly stacking conservatism.
    """

    bankroll: Decimal
    lambda_fraction: Decimal = MAX_POLICY_LAMBDA
    mode: SizingMode = SizingMode.PAPER
    validation_token: ValidationToken | None = None
    cost_config: CostConfig = field(default_factory=CostConfig)
    net_ev_quantile: Decimal = Decimal("0.05")
    risk_free_annual_rate: Decimal = ZERO
    max_market_notional: Decimal | None = None
    max_aggregate_notional: Decimal | None = None
    scenario_exposure_cap: Decimal = MAX_POLICY_LAMBDA
    correlation_exposure_cap: Decimal = MAX_POLICY_LAMBDA
    drawdown_kill_switch: bool = False

    def __post_init__(self) -> None:
        _require_positive(self.bankroll, "bankroll")
        _require_probability(self.lambda_fraction, "lambda_fraction")
        if self.lambda_fraction > MAX_POLICY_LAMBDA:
            raise ValueError("lambda_fraction must be <= 0.25")
        _require_probability(self.net_ev_quantile, "net_ev_quantile")
        _require_nonnegative(self.risk_free_annual_rate, "risk_free_annual_rate")
        _require_probability(self.scenario_exposure_cap, "scenario_exposure_cap")
        _require_probability(self.correlation_exposure_cap, "correlation_exposure_cap")
        if self.max_market_notional is not None:
            _require_nonnegative(self.max_market_notional, "max_market_notional")
        if self.max_aggregate_notional is not None:
            _require_nonnegative(self.max_aggregate_notional, "max_aggregate_notional")


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    """Executable ask and visible capacity for one binary side."""

    market_id: str
    side: BinarySide
    ask: Decimal
    size_available: Decimal
    exit_price: Decimal | None = None
    resolution_horizon_days: Decimal = ZERO

    def __post_init__(self) -> None:
        if not self.market_id:
            raise ValueError("market_id is required")
        _require_unit_interval_open_upper(self.ask, "ask")
        _require_nonnegative(self.size_available, "size_available")
        if self.exit_price is not None:
            _require_unit_interval_open_upper(self.exit_price, "exit_price")
        _require_nonnegative(self.resolution_horizon_days, "resolution_horizon_days")


@dataclass(frozen=True, slots=True)
class SizingDecision:
    """Result of a sizing decision for one side."""

    market_id: str
    side: BinarySide
    kelly_fraction: Decimal
    target_fraction: Decimal
    notional: Decimal
    size: Decimal
    q_all_in: Decimal
    lower_net_ev: Decimal
    allowed: bool
    reason: str


@dataclass(frozen=True, slots=True)
class ScenarioOptimizationResult:
    """Expected-log result for a mutually exclusive outcome set."""

    full_kelly_fractions: tuple[Decimal, ...]
    target_fractions: tuple[Decimal, ...]
    expected_log_growth: float
    objective_evaluations: int


@dataclass(frozen=True, slots=True)
class PolicyStressResult:
    """One point in a lambda/uncertainty-gate stress grid."""

    lambda_fraction: Decimal
    net_ev_quantile: Decimal
    decision: SizingDecision


def yes_kelly_fraction(p_hat: Decimal, q_yes: Decimal) -> Decimal:
    """Return the long-YES Kelly stake fraction from executable all-in cost."""
    _require_probability(p_hat, "p_hat")
    _require_unit_interval_open_upper(q_yes, "q_yes")
    return _positive_fraction((p_hat - q_yes) / (ONE - q_yes))


def no_kelly_fraction(p_hat: Decimal, q_no: Decimal) -> Decimal:
    """Return the long-NO Kelly stake fraction from the NO book's own ask."""
    _require_probability(p_hat, "p_hat")
    _require_unit_interval_open_upper(q_no, "q_no")
    return _positive_fraction(((ONE - p_hat) - q_no) / (ONE - q_no))


def side_specific_kelly_fraction(
    *,
    p_hat: Decimal,
    side: BinarySide,
    q_all_in: Decimal,
) -> Decimal:
    """Dispatch to the explicit YES or NO binary Kelly formula."""
    if side == BinarySide.YES:
        return yes_kelly_fraction(p_hat, q_all_in)
    return no_kelly_fraction(p_hat, q_all_in)


def executable_all_in_cost(
    *,
    ask: Decimal,
    cost_model: CostModel,
) -> Decimal:
    """Return executable ask plus per-contract taker entry cost."""
    _require_unit_interval_open_upper(ask, "ask")
    entry_cost = cost_model.calculate(
        price=ask,
        size=ONE,
        liquidity=LiquidityFlag.TAKER,
    ).total
    q_all_in = ask + entry_cost
    if q_all_in < ZERO:
        return ZERO
    return q_all_in


def round_trip_cost_per_contract(
    *,
    entry_price: Decimal,
    exit_price: Decimal,
    cost_model: CostModel,
) -> Decimal:
    """Return entry plus exit taker costs for one contract."""
    _require_unit_interval_open_upper(entry_price, "entry_price")
    _require_unit_interval_open_upper(exit_price, "exit_price")
    entry = cost_model.calculate(
        price=entry_price,
        size=ONE,
        liquidity=LiquidityFlag.TAKER,
    ).total
    exit_cost = cost_model.calculate(
        price=exit_price,
        size=ONE,
        liquidity=LiquidityFlag.TAKER,
    ).total
    return entry + exit_cost


def lower_net_ev_quantile(
    *,
    probability_samples: Sequence[Decimal],
    executable_price: Decimal,
    exit_price: Decimal,
    cost_model: CostModel,
    quantile: Decimal,
    horizon_days: Decimal = ZERO,
    risk_free_annual_rate: Decimal = ZERO,
) -> Decimal:
    """Return a lower bootstrap quantile of net EV after costs and lockup.

    ``probability_samples`` are forecast/parameter uncertainty draws for the
    side being bought. They are not binomial outcome volatility proxies.
    """
    if not probability_samples:
        raise ValueError("probability_samples must not be empty")
    _require_unit_interval_open_upper(executable_price, "executable_price")
    _require_unit_interval_open_upper(exit_price, "exit_price")
    _require_probability(quantile, "quantile")
    _require_nonnegative(horizon_days, "horizon_days")
    _require_nonnegative(risk_free_annual_rate, "risk_free_annual_rate")

    costs = round_trip_cost_per_contract(
        entry_price=executable_price,
        exit_price=exit_price,
        cost_model=cost_model,
    )
    opportunity_cost = _opportunity_cost(
        locked_price=executable_price,
        annual_rate=risk_free_annual_rate,
        horizon_days=horizon_days,
    )
    values: list[Decimal] = []
    for probability in probability_samples:
        _require_probability(probability, "probability_sample")
        values.append(probability - executable_price - costs - opportunity_cost)
    return _lower_quantile(values, quantile)


class Sizer:
    """WP-5 sizing entry point with live-mode validation gate."""

    def __init__(self, config: SizingConfig, cost_model: CostModel | None = None) -> None:
        self._config = config
        self._cost_model = cost_model or CostModel(config.cost_config)

    def size_binary(
        self,
        *,
        p_hat: Decimal,
        yes_quote: ExecutableQuote,
        no_quote: ExecutableQuote,
        probability_samples: Sequence[Decimal] = (),
        current_market_notional: Decimal = ZERO,
        aggregate_notional: Decimal = ZERO,
        scenario_group_notional: Decimal = ZERO,
        correlation_group_notional: Decimal = ZERO,
        drawdown_active: bool = False,
    ) -> SizingDecision:
        """Choose the better eligible YES/NO side for one binary market."""
        if yes_quote.side != BinarySide.YES:
            raise ValueError("yes_quote.side must be BinarySide.YES")
        if no_quote.side != BinarySide.NO:
            raise ValueError("no_quote.side must be BinarySide.NO")
        if yes_quote.market_id != no_quote.market_id:
            raise ValueError("YES and NO quotes must reference the same market_id")

        yes_decision = self.size_side(
            p_hat=p_hat,
            quote=yes_quote,
            probability_samples=probability_samples,
            current_market_notional=current_market_notional,
            aggregate_notional=aggregate_notional,
            scenario_group_notional=scenario_group_notional,
            correlation_group_notional=correlation_group_notional,
            drawdown_active=drawdown_active,
        )
        no_decision = self.size_side(
            p_hat=p_hat,
            quote=no_quote,
            probability_samples=probability_samples,
            current_market_notional=current_market_notional,
            aggregate_notional=aggregate_notional,
            scenario_group_notional=scenario_group_notional,
            correlation_group_notional=correlation_group_notional,
            drawdown_active=drawdown_active,
        )
        if yes_decision.notional > no_decision.notional:
            return yes_decision
        if no_decision.notional > yes_decision.notional:
            return no_decision
        if yes_decision.lower_net_ev >= no_decision.lower_net_ev:
            return yes_decision
        return no_decision

    def size_side(
        self,
        *,
        p_hat: Decimal,
        quote: ExecutableQuote,
        probability_samples: Sequence[Decimal] = (),
        current_market_notional: Decimal = ZERO,
        aggregate_notional: Decimal = ZERO,
        scenario_group_notional: Decimal = ZERO,
        correlation_group_notional: Decimal = ZERO,
        drawdown_active: bool = False,
    ) -> SizingDecision:
        """Size one explicitly requested side."""
        self._enforce_live_gate()
        _require_probability(p_hat, "p_hat")
        _require_nonnegative(current_market_notional, "current_market_notional")
        _require_nonnegative(aggregate_notional, "aggregate_notional")
        _require_nonnegative(scenario_group_notional, "scenario_group_notional")
        _require_nonnegative(correlation_group_notional, "correlation_group_notional")

        q_all_in = executable_all_in_cost(ask=quote.ask, cost_model=self._cost_model)
        if q_all_in >= ONE:
            return self._zero_decision(quote, q_all_in, "all-in executable cost is >= 1")
        if drawdown_active or self._config.drawdown_kill_switch:
            return self._zero_decision(quote, q_all_in, "drawdown kill-switch active")

        kelly_fraction = side_specific_kelly_fraction(
            p_hat=p_hat,
            side=quote.side,
            q_all_in=q_all_in,
        )
        side_samples = _side_probability_samples(
            p_hat=p_hat,
            side=quote.side,
            probability_samples=probability_samples,
        )
        exit_price = quote.exit_price if quote.exit_price is not None else quote.ask
        lower_net_ev = lower_net_ev_quantile(
            probability_samples=side_samples,
            executable_price=quote.ask,
            exit_price=exit_price,
            cost_model=self._cost_model,
            quantile=self._config.net_ev_quantile,
            horizon_days=quote.resolution_horizon_days,
            risk_free_annual_rate=self._config.risk_free_annual_rate,
        )
        if kelly_fraction <= ZERO:
            return self._zero_decision(quote, q_all_in, "kelly fraction is non-positive")
        if lower_net_ev <= ZERO:
            return self._zero_decision(quote, q_all_in, "lower net-EV quantile is non-positive")

        target_fraction = kelly_fraction * self._config.lambda_fraction
        requested_notional = self._config.bankroll * target_fraction
        notional = min(
            requested_notional,
            quote.ask * quote.size_available,
            _remaining_notional(
                cap=self._config.max_market_notional or self._config.bankroll,
                used=current_market_notional,
            ),
            _remaining_notional(
                cap=self._config.max_aggregate_notional or self._config.bankroll,
                used=aggregate_notional,
            ),
            _remaining_notional(
                cap=self._config.bankroll * self._config.scenario_exposure_cap,
                used=scenario_group_notional,
            ),
            _remaining_notional(
                cap=self._config.bankroll * self._config.correlation_exposure_cap,
                used=correlation_group_notional,
            ),
        )
        if notional <= ZERO:
            return self._zero_decision(
                quote,
                q_all_in,
                "risk, capacity, or exposure cap is binding",
            )
        size = notional / quote.ask
        return SizingDecision(
            market_id=quote.market_id,
            side=quote.side,
            kelly_fraction=kelly_fraction,
            target_fraction=notional / self._config.bankroll,
            notional=notional,
            size=size,
            q_all_in=q_all_in,
            lower_net_ev=lower_net_ev,
            allowed=True,
            reason="sized",
        )

    def _zero_decision(
        self,
        quote: ExecutableQuote,
        q_all_in: Decimal,
        reason: str,
    ) -> SizingDecision:
        return SizingDecision(
            market_id=quote.market_id,
            side=quote.side,
            kelly_fraction=ZERO,
            target_fraction=ZERO,
            notional=ZERO,
            size=ZERO,
            q_all_in=q_all_in,
            lower_net_ev=ZERO,
            allowed=False,
            reason=reason,
        )

    def _enforce_live_gate(self) -> None:
        if self._config.mode != SizingMode.LIVE:
            return
        token = self._config.validation_token
        if token is None or not token.is_valid():
            raise PermissionError(
                "live sizing requires a valid WP-4 dual-go validation token from real data"
            )


def optimize_mutually_exclusive_scenario(
    *,
    probabilities: Sequence[Decimal],
    costs: Sequence[Decimal],
    lambda_fraction: Decimal = MAX_POLICY_LAMBDA,
    exposure_cap: Decimal = ONE,
    max_fraction_per_leg: Decimal = ONE,
    grid_step: Decimal = Decimal("0.01"),
) -> ScenarioOptimizationResult:
    """Maximize expected log wealth for exactly-one-wins outcome legs.

    Fractions are stake fractions of bankroll. If scenario ``j`` resolves, leg
    ``j`` earns ``(1 - cost_j) / cost_j`` per staked dollar and every other leg
    loses its stake. The returned ``target_fractions`` apply the explicit
    fractional-Kelly policy after finding the full expected-log optimum.
    """
    _validate_scenario_inputs(
        probabilities=probabilities,
        costs=costs,
        lambda_fraction=lambda_fraction,
        exposure_cap=exposure_cap,
        max_fraction_per_leg=max_fraction_per_leg,
        grid_step=grid_step,
    )
    n = len(probabilities)
    best_fractions: tuple[Decimal, ...] = tuple(ZERO for _ in range(n))
    best_objective = _expected_log_growth(probabilities, costs, best_fractions)
    evaluations = 1

    values = _grid_values(
        upper=min(exposure_cap, max_fraction_per_leg),
        step=grid_step,
    )

    def visit(prefix: tuple[Decimal, ...], remaining: Decimal) -> None:
        nonlocal best_fractions
        nonlocal best_objective
        nonlocal evaluations
        if len(prefix) == n:
            objective = _expected_log_growth(probabilities, costs, prefix)
            evaluations += 1
            if objective > best_objective:
                best_fractions = prefix
                best_objective = objective
            return
        max_next = min(remaining, max_fraction_per_leg)
        for value in values:
            if value > max_next:
                break
            visit((*prefix, value), remaining - value)

    visit((), exposure_cap)
    target = tuple(_positive_fraction(fraction * lambda_fraction) for fraction in best_fractions)
    return ScenarioOptimizationResult(
        full_kelly_fractions=best_fractions,
        target_fractions=target,
        expected_log_growth=_expected_log_growth(probabilities, costs, target),
        objective_evaluations=evaluations,
    )


def independent_binary_kelly_fractions(
    *,
    probabilities: Sequence[Decimal],
    costs: Sequence[Decimal],
) -> tuple[Decimal, ...]:
    """Return naive independent YES Kelly fractions for comparison tests."""
    if len(probabilities) != len(costs):
        raise ValueError("probabilities and costs must have the same length")
    return tuple(
        yes_kelly_fraction(probability, cost)
        for probability, cost in zip(probabilities, costs, strict=True)
    )


def stress_test_side_policy(
    *,
    base_config: SizingConfig,
    p_hat: Decimal,
    quote: ExecutableQuote,
    probability_samples: Sequence[Decimal],
    lambda_fractions: Sequence[Decimal],
    net_ev_quantiles: Sequence[Decimal],
) -> tuple[PolicyStressResult, ...]:
    """Evaluate sizing under a joint lambda/gate grid."""
    results: list[PolicyStressResult] = []
    for lambda_fraction in lambda_fractions:
        for quantile in net_ev_quantiles:
            config = replace(
                base_config,
                lambda_fraction=lambda_fraction,
                net_ev_quantile=quantile,
            )
            decision = Sizer(config).size_side(
                p_hat=p_hat,
                quote=quote,
                probability_samples=probability_samples,
            )
            results.append(
                PolicyStressResult(
                    lambda_fraction=lambda_fraction,
                    net_ev_quantile=quantile,
                    decision=decision,
                )
            )
    return tuple(results)


def _validate_scenario_inputs(
    *,
    probabilities: Sequence[Decimal],
    costs: Sequence[Decimal],
    lambda_fraction: Decimal,
    exposure_cap: Decimal,
    max_fraction_per_leg: Decimal,
    grid_step: Decimal,
) -> None:
    if not probabilities:
        raise ValueError("probabilities must not be empty")
    if len(probabilities) != len(costs):
        raise ValueError("probabilities and costs must have the same length")
    probability_sum = sum(probabilities, ZERO)
    if abs(probability_sum - ONE) > Decimal("0.0000001"):
        raise ValueError("scenario probabilities must sum to 1")
    for probability in probabilities:
        _require_probability(probability, "scenario_probability")
    for cost in costs:
        _require_unit_interval_open_upper(cost, "scenario_cost")
    _require_probability(lambda_fraction, "lambda_fraction")
    if lambda_fraction > MAX_POLICY_LAMBDA:
        raise ValueError("lambda_fraction must be <= 0.25")
    _require_probability(exposure_cap, "exposure_cap")
    _require_probability(max_fraction_per_leg, "max_fraction_per_leg")
    _require_positive(grid_step, "grid_step")
    if grid_step > exposure_cap:
        raise ValueError("grid_step must be <= exposure_cap")


def _expected_log_growth(
    probabilities: Sequence[Decimal],
    costs: Sequence[Decimal],
    fractions: Sequence[Decimal],
) -> float:
    total = 0.0
    for scenario_index, probability in enumerate(probabilities):
        wealth = 1.0
        for leg_index, fraction in enumerate(fractions):
            cost = costs[leg_index]
            payoff = (ONE - cost) / cost if leg_index == scenario_index else Decimal("-1")
            wealth += float(fraction * payoff)
        if wealth <= 0.0 or not math.isfinite(wealth):
            return float("-inf")
        total += float(probability) * math.log(wealth)
    return total


def _grid_values(*, upper: Decimal, step: Decimal) -> tuple[Decimal, ...]:
    values: list[Decimal] = []
    count = int((upper / step).to_integral_value(rounding=ROUND_FLOOR))
    for index in range(count + 1):
        values.append(step * Decimal(index))
    if values[-1] != upper:
        values.append(upper)
    return tuple(values)


def _side_probability_samples(
    *,
    p_hat: Decimal,
    side: BinarySide,
    probability_samples: Sequence[Decimal],
) -> tuple[Decimal, ...]:
    source = tuple(probability_samples) if probability_samples else (p_hat,)
    if side == BinarySide.YES:
        return source
    return tuple(ONE - probability for probability in source)


def _lower_quantile(values: Sequence[Decimal], quantile: Decimal) -> Decimal:
    ordered = sorted(values)
    rank_decimal = (Decimal(len(ordered) - 1) * quantile).to_integral_value(
        rounding=ROUND_FLOOR
    )
    return ordered[int(rank_decimal)]


def _opportunity_cost(
    *,
    locked_price: Decimal,
    annual_rate: Decimal,
    horizon_days: Decimal,
) -> Decimal:
    if annual_rate == ZERO or horizon_days == ZERO:
        return ZERO
    return locked_price * annual_rate * horizon_days / Decimal("365")


def _remaining_notional(*, cap: Decimal, used: Decimal) -> Decimal:
    remaining = cap - used
    return remaining if remaining > ZERO else ZERO


def _positive_fraction(value: Decimal) -> Decimal:
    if value <= ZERO:
        return ZERO
    return value


def _require_probability(value: Decimal, name: str) -> None:
    if value < ZERO or value > ONE:
        raise ValueError(f"{name} must be between 0 and 1")


def _require_unit_interval_open_upper(value: Decimal, name: str) -> None:
    if value < ZERO or value >= ONE:
        raise ValueError(f"{name} must be >= 0 and < 1")


def _require_nonnegative(value: Decimal, name: str) -> None:
    if value < ZERO:
        raise ValueError(f"{name} must be non-negative")


def _require_positive(value: Decimal, name: str) -> None:
    if value <= ZERO:
        raise ValueError(f"{name} must be positive")
