"""WP-4 report assembly."""

from __future__ import annotations

from collections.abc import Sequence

from pmbot.backtest.engine import BacktestResult
from pmbot.eval.calibration import reliability_curve
from pmbot.eval.clv import cross_venue_clv
from pmbot.eval.models import (
    ClvObservation,
    CrossVenueEvaluationObservation,
    EvaluationReport,
    FillAttribution,
    ForecastObservation,
    ParameterCandidate,
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
    headline = _headline(scoring.brier_skill)
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
        headline=headline,
    )


def _headline(brier_skill: float) -> str:
    if brier_skill <= 0:
        return "NO ALPHA: p-hat does not beat the market Brier baseline."
    return "POSITIVE BRIER SKILL: p-hat beats the market Brier baseline."
