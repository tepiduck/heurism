"""Train/test parameter sweep for WP-3 constants."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from pmbot.eval.models import (
    CrossVenueEvaluationObservation,
    ForecastObservation,
    ParameterCandidate,
    ParameterPerformance,
    SweepResult,
)
from pmbot.eval.scoring import score_forecasts
from pmbot.signals.cross_venue import guarded_log_odds_pool


def forecast_with_candidate(
    observation: CrossVenueEvaluationObservation,
    candidate: ParameterCandidate,
) -> ForecastObservation:
    """Transform raw cross-venue probabilities into a calibrated forecast."""
    pooled = guarded_log_odds_pool(
        polymarket_probability=observation.polymarket_probability,
        kalshi_probability=observation.kalshi_probability,
        kalshi_weight=Decimal("1"),
        double_counting_overlap=candidate.double_counting_overlap,
    )
    adjusted = observation.polymarket_probability + candidate.confidence_scalar * (
        pooled - observation.polymarket_probability
    )
    return ForecastObservation(
        market_id=observation.market_id,
        ts_local_ns=observation.ts_local_ns,
        model_probability=_clamp_probability(adjusted),
        market_mid=observation.polymarket_probability,
        realized=observation.realized,
        event_id=observation.event_id,
    )


def evaluate_candidate(
    observations: Sequence[CrossVenueEvaluationObservation],
    candidate: ParameterCandidate,
) -> ParameterPerformance:
    """Evaluate one candidate on one split."""
    forecasts = [forecast_with_candidate(observation, candidate) for observation in observations]
    scoring = score_forecasts(forecasts)
    return ParameterPerformance(
        candidate=candidate,
        score=scoring.brier_skill,
        brier_skill=scoring.brier_skill,
        total_pnl=sum((observation.pnl for observation in observations), Decimal("0")),
        evaluated_market_ids=tuple(observation.market_id for observation in observations),
    )


def select_parameter(
    train: Sequence[CrossVenueEvaluationObservation],
    candidates: Sequence[ParameterCandidate],
) -> ParameterPerformance:
    """Select the candidate with best train Brier skill, using train only."""
    if not train:
        raise ValueError("train split must not be empty")
    if not candidates:
        raise ValueError("at least one candidate is required")
    performances = [evaluate_candidate(train, candidate) for candidate in candidates]
    return max(performances, key=lambda performance: (performance.score, performance.total_pnl))


def sweep_train_test(
    *,
    train: Sequence[CrossVenueEvaluationObservation],
    test: Sequence[CrossVenueEvaluationObservation],
    candidates: Sequence[ParameterCandidate],
) -> SweepResult:
    """Select on train, then validate the train-selected candidate on held-out test."""
    train_performance = select_parameter(train, candidates)
    test_performance = evaluate_candidate(test, train_performance.candidate)
    return SweepResult(
        selected=train_performance.candidate,
        train_performance=train_performance,
        test_performance=test_performance,
        in_sample_out_of_sample_gap=train_performance.score - test_performance.score,
    )


def _clamp_probability(value: Decimal) -> Decimal:
    return min(Decimal("0.999999"), max(Decimal("0.000001"), value))
