"""Proper scoring rules for resolved binary markets."""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Sequence
from decimal import Decimal

from pmbot.eval.models import BootstrapInterval, ForecastObservation, ScoreSummary

EPSILON = 1e-12
SMALL_N_THRESHOLD = 30


def brier_score(forecasts: Sequence[Decimal], outcomes: Sequence[int]) -> float:
    """Return the mean Brier score."""
    _validate_lengths(forecasts, outcomes)
    if not forecasts:
        raise ValueError("at least one forecast is required")
    _validate_probabilities(forecasts)
    errors = [
        (_probability(forecast) - outcome) ** 2
        for forecast, outcome in zip(forecasts, outcomes, strict=True)
    ]
    return sum(errors) / len(errors)


def log_loss(forecasts: Sequence[Decimal], outcomes: Sequence[int]) -> float:
    """Return the mean binary log-loss."""
    _validate_lengths(forecasts, outcomes)
    if not forecasts:
        raise ValueError("at least one forecast is required")
    _validate_probabilities(forecasts)
    losses = []
    for forecast, outcome in zip(forecasts, outcomes, strict=True):
        probability = min(1.0 - EPSILON, max(EPSILON, _probability(forecast)))
        losses.append(
            -(
                outcome * math.log(probability)
                + (1 - outcome) * math.log(1 - probability)
            )
        )
    return sum(losses) / len(losses)


def score_forecasts(
    observations: Sequence[ForecastObservation],
    *,
    bootstrap_samples: int = 0,
    seed: int = 17,
) -> ScoreSummary:
    """Score model forecasts against the market-price baseline."""
    if not observations:
        raise ValueError("at least one forecast observation is required")
    model = [observation.model_probability for observation in observations]
    market = [observation.market_mid for observation in observations]
    outcomes = [observation.realized for observation in observations]
    brier_model = brier_score(model, outcomes)
    brier_market = brier_score(market, outcomes)
    skill = brier_skill(brier_model=brier_model, brier_market=brier_market)
    skill_ci = (
        _cluster_bootstrap_interval(
            observations,
            lambda sample: brier_skill_from_observations(sample),
            samples=bootstrap_samples,
            seed=seed,
        )
        if bootstrap_samples > 0
        else None
    )
    difference_ci = (
        _cluster_bootstrap_interval(
            observations,
            lambda sample: brier_difference_from_observations(sample),
            samples=bootstrap_samples,
            seed=seed + 1,
        )
        if bootstrap_samples > 0
        else None
    )
    distinct_event_count = len(_clusters(observations))
    return ScoreSummary(
        n=len(observations),
        distinct_event_count=distinct_event_count,
        brier_model=brier_model,
        brier_market=brier_market,
        brier_skill=skill,
        brier_difference=brier_market - brier_model,
        log_loss_model=log_loss(model, outcomes),
        log_loss_market=log_loss(market, outcomes),
        brier_skill_ci=skill_ci,
        brier_difference_ci=difference_ci,
        small_n=distinct_event_count < SMALL_N_THRESHOLD,
    )


def brier_skill(*, brier_model: float, brier_market: float) -> float:
    """Return Brier skill versus the market baseline."""
    if brier_market == 0:
        return 0.0 if brier_model == 0 else float("-inf")
    return 1.0 - brier_model / brier_market


def brier_skill_from_observations(observations: Sequence[ForecastObservation]) -> float:
    """Return Brier skill for a set of observations."""
    model = [observation.model_probability for observation in observations]
    market = [observation.market_mid for observation in observations]
    outcomes = [observation.realized for observation in observations]
    return brier_skill(
        brier_model=brier_score(model, outcomes),
        brier_market=brier_score(market, outcomes),
    )


def brier_difference_from_observations(observations: Sequence[ForecastObservation]) -> float:
    """Return paired Brier improvement: market baseline minus model score."""
    model = [observation.model_probability for observation in observations]
    market = [observation.market_mid for observation in observations]
    outcomes = [observation.realized for observation in observations]
    return brier_score(market, outcomes) - brier_score(model, outcomes)


def _cluster_bootstrap_interval(
    observations: Sequence[ForecastObservation],
    metric: Callable[[Sequence[ForecastObservation]], float],
    *,
    samples: int,
    seed: int,
) -> BootstrapInterval:
    grouped = tuple(_clusters(observations).values())
    rng = random.Random(seed)
    values: list[float] = []
    for _ in range(samples):
        resample: list[ForecastObservation] = []
        for _ in grouped:
            resample.extend(grouped[rng.randrange(len(grouped))])
        values.append(metric(resample))
    return BootstrapInterval(lower=_quantile(values, 0.025), upper=_quantile(values, 0.975))


def _clusters(
    observations: Sequence[ForecastObservation],
) -> dict[str, tuple[ForecastObservation, ...]]:
    grouped: dict[str, list[ForecastObservation]] = {}
    for observation in observations:
        grouped.setdefault(observation.event_id or observation.market_id, []).append(observation)
    return {key: tuple(value) for key, value in grouped.items()}


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        raise ValueError("cannot compute quantile of empty values")
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _validate_lengths(forecasts: Sequence[Decimal], outcomes: Sequence[int]) -> None:
    if len(forecasts) != len(outcomes):
        raise ValueError("forecasts and outcomes must have equal length")
    for outcome in outcomes:
        if outcome not in {0, 1}:
            raise ValueError("binary outcomes must be 0 or 1")


def _validate_probabilities(forecasts: Sequence[Decimal]) -> None:
    for forecast in forecasts:
        if forecast < Decimal("0") or forecast > Decimal("1"):
            raise ValueError(f"forecast probability must be between 0 and 1: {forecast}")


def _probability(value: Decimal) -> float:
    return float(value)
