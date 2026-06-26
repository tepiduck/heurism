"""WP-4 report assembly."""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from decimal import Decimal

from pmbot.backtest.engine import BacktestResult
from pmbot.eval.calibration import reliability_curve
from pmbot.eval.clv import cross_venue_clv
from pmbot.eval.models import (
    BootstrapInterval,
    ClvObservation,
    CrossVenueEvaluationObservation,
    EvaluationReport,
    FillAttribution,
    ForecastObservation,
    ParameterCandidate,
    ScoreSummary,
    VerdictSummary,
)
from pmbot.eval.pnl import pnl_metrics
from pmbot.eval.scoring import score_forecasts
from pmbot.eval.shortfall import implementation_shortfall
from pmbot.eval.sweep import sweep_train_test


def build_evaluation_report(
    *,
    forecasts: Sequence[ForecastObservation],
    backtest: BacktestResult,
    shortfall_rows: Sequence[FillAttribution],
    train: Sequence[CrossVenueEvaluationObservation],
    test: Sequence[CrossVenueEvaluationObservation],
    candidates: Sequence[ParameterCandidate],
    clv_observations: Sequence[ClvObservation] = (),
    calibration_bins: int = 10,
    bootstrap_samples: int = 100,
) -> EvaluationReport:
    """Build the WP-4 evaluation report from fixture or live-resolved data."""
    scoring = score_forecasts(
        forecasts,
        bootstrap_samples=bootstrap_samples,
    )
    verdict = _verdict(scoring, test, bootstrap_samples=bootstrap_samples)
    headline = _headline(scoring.brier_skill, small_n=scoring.small_n, verdict=verdict)
    return EvaluationReport(
        scoring=scoring,
        calibration=reliability_curve(
            forecasts,
            bin_count=calibration_bins,
            bootstrap_samples=bootstrap_samples,
        ),
        pnl=pnl_metrics(backtest, bootstrap_samples=bootstrap_samples),
        shortfall=implementation_shortfall(shortfall_rows),
        sweep=sweep_train_test(train=train, test=test, candidates=candidates),
        clv=cross_venue_clv(clv_observations) if clv_observations else None,
        verdict=verdict,
        headline=headline,
    )


def _headline(brier_skill: float, *, small_n: bool, verdict: VerdictSummary) -> str:
    if verdict.go:
        return "EDGE CONFIRMED: holdout proper-score improvement and net PnL are positive."
    if not verdict.proper_score_positive and not verdict.pnl_positive:
        return "NO-GO: holdout proper-score improvement and net PnL are not both positive."
    if not verdict.proper_score_positive:
        return "NO-GO: net PnL is positive, but holdout proper-score improvement is not."
    if not verdict.pnl_positive:
        return "NO-GO: holdout proper-score improvement is positive, but net PnL is not."
    if small_n:
        return (
            "PRELIMINARY POSITIVE BRIER SKILL: sample too small to conclude real edge."
        )
    if brier_skill <= 0:
        return "NO ALPHA: p-hat does not beat the market Brier baseline."
    return "NO-GO: positive Brier skill is not sufficient without positive net PnL."


def _verdict(
    scoring: ScoreSummary,
    test: Sequence[CrossVenueEvaluationObservation],
    *,
    bootstrap_samples: int,
) -> VerdictSummary:
    score_ci = scoring.brier_difference_ci
    proper_score_positive = score_ci is not None and score_ci.lower > 0
    net_pnl = sum((observation.pnl for observation in test), Decimal("0"))
    pnl_ci = (
        _cluster_pnl_interval(test, samples=bootstrap_samples, seed=41)
        if bootstrap_samples > 0 and test
        else None
    )
    pnl_positive = pnl_ci is not None and pnl_ci.lower > 0
    go = proper_score_positive and pnl_positive
    reason = (
        "both gates passed"
        if go
        else "requires positive clustered lower bounds for Brier improvement and net PnL"
    )
    return VerdictSummary(
        proper_score_positive=proper_score_positive,
        pnl_positive=pnl_positive,
        go=go,
        net_pnl=net_pnl,
        net_pnl_ci=pnl_ci,
        reason=reason,
    )


def _cluster_pnl_interval(
    observations: Sequence[CrossVenueEvaluationObservation],
    *,
    samples: int,
    seed: int,
) -> BootstrapInterval:
    clusters = tuple(_pnl_clusters(observations).values())
    rng = random.Random(seed)
    totals: list[float] = []
    for _ in range(samples):
        total = Decimal("0")
        for _ in clusters:
            total += clusters[rng.randrange(len(clusters))]
        totals.append(float(total))
    return BootstrapInterval(lower=_quantile(totals, 0.025), upper=_quantile(totals, 0.975))


def _pnl_clusters(
    observations: Sequence[CrossVenueEvaluationObservation],
) -> dict[str, Decimal]:
    clusters: dict[str, Decimal] = {}
    for observation in observations:
        key = observation.event_id or observation.market_id
        clusters[key] = clusters.get(key, Decimal("0")) + observation.pnl
    return clusters


def _quantile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight
