"""Cross-venue Polymarket/Kalshi deviation signal.

WP-3 v3 keeps three surfaces separate:

* latent forecast midpoints for signal generation and Brier baselines;
* executable bid/ask prices for EV and trade gating;
* resolution-target and convergence-target outputs for downstream evaluation.

When fitted train-split regression coefficients are unavailable, the signal uses
an explicitly marked unfit fallback constant. Real deployment should replace the
fallback with coefficients fit and validated by WP-4 on resolved/markout data.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from pmbot.backtest.engine import PointInTimeView
from pmbot.backtest.models import BookSnapshot
from pmbot.features.orderbook import midpoint, spread
from pmbot.ingestion.schemas import BookEvent, BookSide
from pmbot.signals.contracts import VerifiedContractPair

ZERO = Decimal("0")
ONE = Decimal("1")
REGRESSION_FEATURE_NAMES = ("horizon_days", "liquidity", "quote_age_seconds", "category_code")


class SignalDirection(StrEnum):
    """Signal direction for the Polymarket leg."""

    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass(frozen=True, slots=True)
class RegressionFeatures:
    """Numeric X features for shrinkage regression."""

    horizon_days: Decimal = ZERO
    liquidity: Decimal = ZERO
    quote_age_seconds: Decimal = ZERO
    category_code: Decimal = ZERO

    def as_mapping(self) -> Mapping[str, Decimal]:
        """Return features by stable regression name."""
        return {
            "horizon_days": self.horizon_days,
            "liquidity": self.liquidity,
            "quote_age_seconds": self.quote_age_seconds,
            "category_code": self.category_code,
        }


DEFAULT_REGRESSION_FEATURES = RegressionFeatures()


@dataclass(frozen=True, slots=True)
class ShrinkageRegressionObservation:
    """One train/test row for ``z_hat = alpha + gamma*z_PM + beta*(z_K-z_PM)+X theta``."""

    polymarket_probability: Decimal
    kalshi_probability: Decimal
    target_probability: Decimal
    features: RegressionFeatures = DEFAULT_REGRESSION_FEATURES


@dataclass(frozen=True, slots=True)
class ShrinkageRestrictionReport:
    """Restriction tests for the fitted shrinkage regression."""

    alpha_zero: bool
    gamma_one: bool
    beta_in_unit_interval: bool
    holds: bool
    out_of_sample_brier_skill: float | None
    n_test: int


@dataclass(frozen=True, slots=True)
class ShrinkageTrainTestResult:
    """Train-fit model plus train/test restriction reports for WP-4 split wiring."""

    model: ShrinkageRegressionModel
    train_report: ShrinkageRestrictionReport
    test_report: ShrinkageRestrictionReport


@dataclass(frozen=True, slots=True)
class ShrinkageRegressionModel:
    """Fitted cross-venue shrinkage regression coefficients."""

    alpha: Decimal
    gamma: Decimal
    beta: Decimal
    feature_coefficients: Mapping[str, Decimal]
    n_observations: int

    def predict_logit(
        self,
        *,
        polymarket_probability: Decimal,
        kalshi_probability: Decimal,
        features: RegressionFeatures | None = None,
    ) -> Decimal:
        """Predict resolution-target log-odds from mids and X features."""
        effective_features = features or DEFAULT_REGRESSION_FEATURES
        pm_logit = _logit(polymarket_probability)
        kalshi_logit = _logit(kalshi_probability)
        prediction = self.alpha + self.gamma * pm_logit + self.beta * (kalshi_logit - pm_logit)
        feature_values = effective_features.as_mapping()
        for name, coefficient in self.feature_coefficients.items():
            prediction += coefficient * feature_values[name]
        return prediction

    def predict_probability(
        self,
        *,
        polymarket_probability: Decimal,
        kalshi_probability: Decimal,
        features: RegressionFeatures | None = None,
    ) -> Decimal:
        """Predict resolution-target probability."""
        return _inv_logit(
            self.predict_logit(
                polymarket_probability=polymarket_probability,
                kalshi_probability=kalshi_probability,
                features=features,
            )
        )

    def restriction_report(
        self,
        *,
        test: Sequence[ShrinkageRegressionObservation] = (),
        tolerance: Decimal = Decimal("0.05"),
    ) -> ShrinkageRestrictionReport:
        """Report alpha=0, gamma=1, 0<=beta<=1 and held-out Brier skill."""
        alpha_zero = abs(self.alpha) <= tolerance
        gamma_one = abs(self.gamma - ONE) <= tolerance
        beta_in_unit_interval = ZERO <= self.beta <= ONE
        skill = _out_of_sample_brier_skill(self, test) if test else None
        return ShrinkageRestrictionReport(
            alpha_zero=alpha_zero,
            gamma_one=gamma_one,
            beta_in_unit_interval=beta_in_unit_interval,
            holds=alpha_zero and gamma_one and beta_in_unit_interval,
            out_of_sample_brier_skill=skill,
            n_test=len(test),
        )


@dataclass(frozen=True, slots=True)
class LeadLagSample:
    """One exchange-timestamped sample for lead-lag diagnostics."""

    ts_exchange_ns: int
    polymarket_probability: Decimal
    kalshi_probability: Decimal
    next_polymarket_probability: Decimal
    next_kalshi_probability: Decimal


@dataclass(frozen=True, slots=True)
class LeadLagDiagnostics:
    """Placebo diagnostics for a proposed Kalshi-leads-Polymarket signal."""

    forward_score: float
    reverse_placebo_score: float
    shifted_time_placebo_score: float
    polling_frequency_score: float
    passes_placebos: bool


@dataclass(frozen=True, slots=True)
class CrossVenueSignalConfig:
    """Cross-venue signal parameters."""

    kalshi_weight: Decimal = Decimal("1")
    double_counting_overlap: Decimal = Decimal("0.50")
    trade_threshold: Decimal = Decimal("0.01")
    max_bid_ask_spread: Decimal | None = Decimal("0.10")
    three_way_sum_tolerance: Decimal | None = Decimal("0.05")
    max_quote_age_ns: int = 2_000_000_000
    require_kalshi_leads: bool = True
    shrinkage_model: ShrinkageRegressionModel | None = None
    fallback_unfit: bool = True
    convergence_horizon_ns: int = 1_000_000_000

    @property
    def max_reference_age_ns(self) -> int:
        """Backward-compatible alias for the old Kalshi-only freshness knob."""
        return self.max_quote_age_ns


@dataclass(frozen=True, slots=True)
class SignalValue:
    """One cross-venue signal observation."""

    polymarket_condition_id: str
    polymarket_asset_id: str
    kalshi_ticker: str
    ts_local_ns: int
    ts_exchange_ns: int
    polymarket_probability: Decimal
    kalshi_probability: Decimal
    fair_probability: Decimal
    signal_value: Decimal
    direction: SignalDirection
    confidence: Decimal
    rationale: str
    resolution_probability: Decimal
    convergence_logit_delta: Decimal
    convergence_horizon_ns: int
    polymarket_yes_ask: Decimal | None
    polymarket_no_ask: Decimal | None
    yes_edge: Decimal | None
    no_edge: Decimal | None
    pm_quote_age_ns: int
    kalshi_quote_age_ns: int
    kalshi_lead_ns: int
    model_status: str


class CrossVenueSignal:
    """Compute Polymarket/Kalshi deviation from a point-in-time view only."""

    def __init__(
        self,
        pairs: tuple[VerifiedContractPair, ...],
        config: CrossVenueSignalConfig | None = None,
    ) -> None:
        self._pairs = pairs
        self._config = config or CrossVenueSignalConfig()

    def compute(self, view: PointInTimeView) -> tuple[SignalValue, ...]:
        """Compute signals using only data exposed by the point-in-time view."""
        signals: list[SignalValue] = []
        probability_sum_guards = _three_way_probability_sum_guards(
            view,
            self._pairs,
            tolerance=self._config.three_way_sum_tolerance,
        )
        for pair in self._pairs:
            group_key = _kalshi_match_group_key(pair.kalshi_ticker)
            signal = self._compute_pair(
                view,
                pair,
                probability_sum_guard=(
                    probability_sum_guards.get(group_key) if group_key is not None else None
                ),
            )
            if signal is not None:
                signals.append(signal)
        return tuple(signals)

    def _compute_pair(
        self,
        view: PointInTimeView,
        pair: VerifiedContractPair,
        *,
        probability_sum_guard: _LowConfidenceGuard | None,
    ) -> SignalValue | None:
        pm_yes_book = view.book(pair.polymarket_asset_id)
        kalshi_book = view.book(pair.kalshi_ticker)
        pm_mid = midpoint(pm_yes_book)
        kalshi_mid = midpoint(kalshi_book)
        if pm_mid is None or kalshi_mid is None:
            return None
        if pair.invert_kalshi:
            kalshi_mid = ONE - kalshi_mid

        pm_clock = _quote_clock(view, pair.polymarket_asset_id)
        kalshi_clock = _quote_clock(view, pair.kalshi_ticker)
        if pm_clock is None or kalshi_clock is None:
            return None
        now_exchange_ns = max(pm_clock.latest_ns, kalshi_clock.latest_ns)
        pm_quote_age_ns = now_exchange_ns - pm_clock.earliest_ns
        kalshi_quote_age_ns = now_exchange_ns - kalshi_clock.earliest_ns
        if (
            pm_quote_age_ns > self._config.max_quote_age_ns
            or kalshi_quote_age_ns > self._config.max_quote_age_ns
        ):
            return None
        if self._config.require_kalshi_leads and kalshi_clock.earliest_ns < pm_clock.latest_ns:
            return None

        features = RegressionFeatures(
            quote_age_seconds=Decimal(max(pm_quote_age_ns, kalshi_quote_age_ns))
            / Decimal("1000000000")
        )
        resolution_probability, model_status = self._resolution_probability(
            pm_mid=pm_mid,
            kalshi_mid=kalshi_mid,
            features=features,
        )
        convergence_delta = _logit(resolution_probability) - _logit(pm_mid)
        yes_ask = _best_ask(pm_yes_book)
        no_ask = _no_ask(
            view,
            pair,
            now_exchange_ns=now_exchange_ns,
            max_quote_age_ns=self._config.max_quote_age_ns,
        )
        yes_edge = resolution_probability - yes_ask if yes_ask is not None else None
        no_edge = (ONE - resolution_probability) - no_ask if no_ask is not None else None
        direction = _direction_from_executable_edges(
            yes_edge=yes_edge,
            no_edge=no_edge,
            threshold=self._config.trade_threshold,
        )
        signal_value = resolution_probability - pm_mid
        confidence = _confidence(
            signal_value,
            max(pm_quote_age_ns, kalshi_quote_age_ns),
            self._config.max_quote_age_ns,
        )
        low_confidence_guard = _low_confidence_guard(
            pm_yes_book=pm_yes_book,
            kalshi_book=kalshi_book,
            max_bid_ask_spread=self._config.max_bid_ask_spread,
            probability_sum_guard=probability_sum_guard,
        )
        if low_confidence_guard is not None:
            direction = SignalDirection.HOLD
            confidence = ZERO
        return SignalValue(
            polymarket_condition_id=pair.polymarket_condition_id,
            polymarket_asset_id=pair.polymarket_asset_id,
            kalshi_ticker=pair.kalshi_ticker,
            ts_local_ns=view.ts_local_ns,
            ts_exchange_ns=now_exchange_ns,
            polymarket_probability=pm_mid,
            kalshi_probability=kalshi_mid,
            fair_probability=resolution_probability,
            signal_value=signal_value,
            direction=direction,
            confidence=confidence,
            rationale=_rationale(
                config=self._config,
                model_status=model_status,
                invert_kalshi=pair.invert_kalshi,
                low_confidence_guard=low_confidence_guard,
            ),
            resolution_probability=resolution_probability,
            convergence_logit_delta=convergence_delta,
            convergence_horizon_ns=self._config.convergence_horizon_ns,
            polymarket_yes_ask=yes_ask,
            polymarket_no_ask=no_ask,
            yes_edge=yes_edge,
            no_edge=no_edge,
            pm_quote_age_ns=pm_quote_age_ns,
            kalshi_quote_age_ns=kalshi_quote_age_ns,
            kalshi_lead_ns=kalshi_clock.earliest_ns - pm_clock.latest_ns,
            model_status=model_status,
        )

    def _resolution_probability(
        self,
        *,
        pm_mid: Decimal,
        kalshi_mid: Decimal,
        features: RegressionFeatures,
    ) -> tuple[Decimal, str]:
        model = self._config.shrinkage_model
        if model is not None:
            return (
                model.predict_probability(
                    polymarket_probability=pm_mid,
                    kalshi_probability=kalshi_mid,
                    features=features,
                ),
                "fitted_shrinkage_regression",
            )
        if not self._config.fallback_unfit:
            raise ValueError("cross-venue shrinkage model is unfit and fallback is disabled")
        return (
            guarded_log_odds_pool(
                polymarket_probability=pm_mid,
                kalshi_probability=kalshi_mid,
                kalshi_weight=self._config.kalshi_weight,
                double_counting_overlap=self._config.double_counting_overlap,
            ),
            "unfit_fallback_constant",
        )


def guarded_log_odds_pool(
    *,
    polymarket_probability: Decimal,
    kalshi_probability: Decimal,
    kalshi_weight: Decimal,
    double_counting_overlap: Decimal,
) -> Decimal:
    """Legacy unfit fallback pool on log-odds with a fixed overlap constant."""
    orthogonal_weight = kalshi_weight * (ONE - double_counting_overlap)
    pm_logit = _logit(polymarket_probability)
    kalshi_logit = _logit(kalshi_probability)
    return _inv_logit(pm_logit + orthogonal_weight * (kalshi_logit - pm_logit))


def fit_shrinkage_regression(
    observations: Sequence[ShrinkageRegressionObservation],
) -> ShrinkageRegressionModel:
    """Fit the v3 shrinkage regression on a train split."""
    if len(observations) < 3:
        raise ValueError("at least three observations are required to fit shrinkage regression")
    active_features = _active_feature_names(observations)
    rows: list[list[float]] = []
    targets: list[float] = []
    for observation in observations:
        pm_logit = _logit(observation.polymarket_probability)
        kalshi_logit = _logit(observation.kalshi_probability)
        feature_values = observation.features.as_mapping()
        rows.append(
            [
                1.0,
                float(pm_logit),
                float(kalshi_logit - pm_logit),
                *[float(feature_values[name]) for name in active_features],
            ]
        )
        targets.append(float(_logit(observation.target_probability)))
    coefficients = _ordinary_least_squares(rows, targets)
    feature_coefficients = {
        name: _decimal_from_float(coefficients[index + 3])
        for index, name in enumerate(active_features)
    }
    return ShrinkageRegressionModel(
        alpha=_decimal_from_float(coefficients[0]),
        gamma=_decimal_from_float(coefficients[1]),
        beta=_decimal_from_float(coefficients[2]),
        feature_coefficients=feature_coefficients,
        n_observations=len(observations),
    )


def fit_shrinkage_train_test(
    *,
    train: Sequence[ShrinkageRegressionObservation],
    test: Sequence[ShrinkageRegressionObservation],
) -> ShrinkageTrainTestResult:
    """Fit on train only and report restrictions on train and held-out test."""
    model = fit_shrinkage_regression(train)
    return ShrinkageTrainTestResult(
        model=model,
        train_report=model.restriction_report(test=train),
        test_report=model.restriction_report(test=test),
    )


def lead_lag_diagnostics(
    samples: Sequence[LeadLagSample],
    *,
    polling_stride: int = 2,
) -> LeadLagDiagnostics:
    """Run reverse-direction, shifted-time, and polling-frequency placebos."""
    if len(samples) < 3:
        raise ValueError("at least three samples are required for lead-lag diagnostics")
    ordered = sorted(samples, key=lambda sample: sample.ts_exchange_ns)
    forward_x = [
        float(_logit(sample.kalshi_probability) - _logit(sample.polymarket_probability))
        for sample in ordered
    ]
    forward_y = [
        float(_logit(sample.next_polymarket_probability) - _logit(sample.polymarket_probability))
        for sample in ordered
    ]
    reverse_x = [
        float(_logit(sample.polymarket_probability) - _logit(sample.kalshi_probability))
        for sample in ordered
    ]
    reverse_y = [
        float(_logit(sample.next_kalshi_probability) - _logit(sample.kalshi_probability))
        for sample in ordered
    ]
    shifted_score = _predictive_slope(forward_x[:-1], forward_y[1:])
    stride = max(1, polling_stride)
    sensitivity_score = _predictive_slope(forward_x[::stride], forward_y[::stride])
    forward_score = _predictive_slope(forward_x, forward_y)
    reverse_score = _predictive_slope(reverse_x, reverse_y)
    passes = (
        forward_score > 0
        and reverse_score <= 0
        and shifted_score <= max(0.0, forward_score * 0.25)
        and sensitivity_score > 0
    )
    return LeadLagDiagnostics(
        forward_score=forward_score,
        reverse_placebo_score=reverse_score,
        shifted_time_placebo_score=shifted_score,
        polling_frequency_score=sensitivity_score,
        passes_placebos=passes,
    )


@dataclass(frozen=True, slots=True)
class _QuoteClock:
    bid_ns: int
    ask_ns: int

    @property
    def latest_ns(self) -> int:
        return max(self.bid_ns, self.ask_ns)

    @property
    def earliest_ns(self) -> int:
        return min(self.bid_ns, self.ask_ns)


@dataclass(frozen=True, slots=True)
class _LowConfidenceGuard:
    reason: str


def _low_confidence_guard(
    *,
    pm_yes_book: BookSnapshot,
    kalshi_book: BookSnapshot,
    max_bid_ask_spread: Decimal | None,
    probability_sum_guard: _LowConfidenceGuard | None,
) -> _LowConfidenceGuard | None:
    reasons: list[str] = []
    if max_bid_ask_spread is not None:
        for venue, book in (("Polymarket", pm_yes_book), ("Kalshi", kalshi_book)):
            book_spread = spread(book)
            if book_spread is not None and book_spread > max_bid_ask_spread:
                reasons.append(
                    f"{venue} spread {book_spread} exceeds {max_bid_ask_spread}"
                )
    if probability_sum_guard is not None:
        reasons.append(probability_sum_guard.reason)
    if not reasons:
        return None
    return _LowConfidenceGuard(reason="; ".join(reasons))


def _three_way_probability_sum_guards(
    view: PointInTimeView,
    pairs: Sequence[VerifiedContractPair],
    *,
    tolerance: Decimal | None,
) -> dict[str, _LowConfidenceGuard]:
    if tolerance is None:
        return {}
    grouped: dict[str, list[VerifiedContractPair]] = {}
    for pair in pairs:
        group_key = _kalshi_match_group_key(pair.kalshi_ticker)
        if group_key is None:
            continue
        grouped.setdefault(group_key, []).append(pair)

    guards: dict[str, _LowConfidenceGuard] = {}
    for group_key, group_pairs in grouped.items():
        has_tie = any(_kalshi_leg(pair.kalshi_ticker) == "TIE" for pair in group_pairs)
        if len(group_pairs) < 3 or not has_tie:
            continue
        pm_probabilities: list[Decimal] = []
        kalshi_probabilities: list[Decimal] = []
        for pair in group_pairs:
            pm_mid = midpoint(view.book(pair.polymarket_asset_id))
            kalshi_mid = midpoint(view.book(pair.kalshi_ticker))
            if pm_mid is None or kalshi_mid is None:
                break
            if pair.invert_kalshi:
                kalshi_mid = ONE - kalshi_mid
            pm_probabilities.append(pm_mid)
            kalshi_probabilities.append(kalshi_mid)
        if len(pm_probabilities) != len(group_pairs):
            continue

        reasons: list[str] = []
        pm_sum = sum(pm_probabilities, ZERO)
        kalshi_sum = sum(kalshi_probabilities, ZERO)
        if abs(pm_sum - ONE) > tolerance:
            reasons.append(f"Polymarket three-way probability sum {pm_sum} outside 1+/-{tolerance}")
        if abs(kalshi_sum - ONE) > tolerance:
            reasons.append(f"Kalshi three-way probability sum {kalshi_sum} outside 1+/-{tolerance}")
        if reasons:
            guards[group_key] = _LowConfidenceGuard(reason="; ".join(reasons))
    return guards


def _kalshi_match_group_key(ticker: str) -> str | None:
    group_key, separator, leg = ticker.upper().rpartition("-")
    if not separator or not group_key or not leg:
        return None
    return group_key


def _kalshi_leg(ticker: str) -> str:
    return ticker.upper().rsplit("-", maxsplit=1)[-1]


def _quote_clock(view: PointInTimeView, asset_id: str) -> _QuoteClock | None:
    bid_ns: int | None = None
    ask_ns: int | None = None
    for event in view.events():
        if not isinstance(event, BookEvent) or event.asset_id != asset_id:
            continue
        ts_exchange_ns = _parse_exchange_time_ns(event.ts_exchange)
        if ts_exchange_ns is None:
            continue
        if event.side == BookSide.BID:
            bid_ns = ts_exchange_ns if bid_ns is None else max(bid_ns, ts_exchange_ns)
        elif event.side == BookSide.ASK:
            ask_ns = ts_exchange_ns if ask_ns is None else max(ask_ns, ts_exchange_ns)
    if bid_ns is None or ask_ns is None:
        return None
    return _QuoteClock(bid_ns=bid_ns, ask_ns=ask_ns)


def _parse_exchange_time_ns(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        pass
    iso_value = stripped.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    parsed_utc = parsed.astimezone(UTC)
    return int(parsed_utc.timestamp()) * 1_000_000_000 + parsed_utc.microsecond * 1_000


def _best_ask(book: BookSnapshot) -> Decimal | None:
    return book.best_ask.price if book.best_ask is not None else None


def _no_ask(
    view: PointInTimeView,
    pair: VerifiedContractPair,
    *,
    now_exchange_ns: int,
    max_quote_age_ns: int,
) -> Decimal | None:
    if pair.polymarket_no_asset_id is None:
        return None
    no_clock = _quote_clock(view, pair.polymarket_no_asset_id)
    if no_clock is None:
        return None
    if now_exchange_ns - no_clock.earliest_ns > max_quote_age_ns:
        return None
    no_book = view.book(pair.polymarket_no_asset_id)
    return _best_ask(no_book)


def _direction_from_executable_edges(
    *,
    yes_edge: Decimal | None,
    no_edge: Decimal | None,
    threshold: Decimal,
) -> SignalDirection:
    yes_candidate = yes_edge if yes_edge is not None else Decimal("-Infinity")
    no_candidate = no_edge if no_edge is not None else Decimal("-Infinity")
    if yes_candidate <= threshold and no_candidate <= threshold:
        return SignalDirection.HOLD
    if yes_candidate >= no_candidate:
        return SignalDirection.BUY
    return SignalDirection.SELL


def _confidence(signal_value: Decimal, quote_age_ns: int, max_quote_age_ns: int) -> Decimal:
    freshness = ONE
    if max_quote_age_ns > 0:
        freshness = max(ZERO, ONE - Decimal(quote_age_ns) / Decimal(max_quote_age_ns))
    return min(ONE, abs(signal_value) * Decimal("4") * freshness)


def _rationale(
    *,
    config: CrossVenueSignalConfig,
    model_status: str,
    invert_kalshi: bool,
    low_confidence_guard: _LowConfidenceGuard | None,
) -> str:
    if model_status == "fitted_shrinkage_regression":
        detail = "fitted shrinkage regression on log-odds mids"
    else:
        detail = (
            "UNFIT fallback constant: legacy log-odds pool with "
            f"{config.double_counting_overlap} overlap"
        )
    if invert_kalshi:
        detail += "; Kalshi probability inverted"
    if low_confidence_guard is not None:
        detail += f"; LOW CONFIDENCE: {low_confidence_guard.reason}"
    return detail


def _active_feature_names(
    observations: Sequence[ShrinkageRegressionObservation],
) -> tuple[str, ...]:
    active: list[str] = []
    for name in REGRESSION_FEATURE_NAMES:
        values = [observation.features.as_mapping()[name] for observation in observations]
        if any(value != ZERO for value in values):
            active.append(name)
    return tuple(active)


def _ordinary_least_squares(
    rows: Sequence[Sequence[float]],
    targets: Sequence[float],
) -> list[float]:
    column_count = len(rows[0])
    if len(rows) < column_count:
        raise ValueError("not enough observations for the active regression columns")
    matrix = [[0.0 for _ in range(column_count)] for _ in range(column_count)]
    vector = [0.0 for _ in range(column_count)]
    for row, target in zip(rows, targets, strict=True):
        for i in range(column_count):
            vector[i] += row[i] * target
            for j in range(column_count):
                matrix[i][j] += row[i] * row[j]
    return _solve_linear_system(matrix, vector)


def _solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    n = len(vector)
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]
    for column in range(n):
        pivot = max(range(column, n), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-12:
            raise ValueError("singular regression design")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        pivot_value = augmented[column][column]
        for entry in range(column, n + 1):
            augmented[column][entry] /= pivot_value
        for row in range(n):
            if row == column:
                continue
            factor = augmented[row][column]
            for entry in range(column, n + 1):
                augmented[row][entry] -= factor * augmented[column][entry]
    return [augmented[row][n] for row in range(n)]


def _out_of_sample_brier_skill(
    model: ShrinkageRegressionModel,
    test: Sequence[ShrinkageRegressionObservation],
) -> float:
    model_errors = []
    market_errors = []
    for observation in test:
        predicted = model.predict_probability(
            polymarket_probability=observation.polymarket_probability,
            kalshi_probability=observation.kalshi_probability,
            features=observation.features,
        )
        model_errors.append(float((predicted - observation.target_probability) ** 2))
        market_errors.append(
            float((observation.polymarket_probability - observation.target_probability) ** 2)
        )
    market_brier = sum(market_errors) / len(market_errors)
    if market_brier == 0.0:
        return 0.0
    model_brier = sum(model_errors) / len(model_errors)
    return 1.0 - model_brier / market_brier


def _predictive_slope(x_values: Sequence[float], y_values: Sequence[float]) -> float:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return 0.0
    mean_x = sum(x_values) / len(x_values)
    mean_y = sum(y_values) / len(y_values)
    denominator = sum((value - mean_x) ** 2 for value in x_values)
    if denominator == 0.0:
        return 0.0
    numerator = sum(
        (x_value - mean_x) * (y_value - mean_y)
        for x_value, y_value in zip(x_values, y_values, strict=True)
    )
    return numerator / denominator


def _decimal_from_float(value: float) -> Decimal:
    return Decimal(str(round(value, 12)))


def _logit(probability: Decimal) -> Decimal:
    clamped = min(Decimal("0.999999"), max(Decimal("0.000001"), probability))
    return (clamped / (ONE - clamped)).ln()


def _inv_logit(log_odds: Decimal) -> Decimal:
    return ONE / (ONE + (-log_odds).exp())
