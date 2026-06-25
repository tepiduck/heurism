"""Evaluation and calibration tools."""

from pmbot.eval.calibration import reliability_curve
from pmbot.eval.clv import cross_venue_clv
from pmbot.eval.models import (
    BootstrapInterval,
    CalibrationBin,
    CalibrationSummary,
    ClvObservation,
    ClvSummary,
    CrossVenueEvaluationObservation,
    EvaluationReport,
    FillAttribution,
    ForecastObservation,
    ParameterCandidate,
    ParameterPerformance,
    PnlMetrics,
    RealizedOutcome,
    ScoreSummary,
    ShortfallSummary,
    SweepResult,
)
from pmbot.eval.pnl import pnl_metrics
from pmbot.eval.report import build_evaluation_report
from pmbot.eval.scoring import brier_score, brier_skill, log_loss, score_forecasts
from pmbot.eval.shortfall import implementation_shortfall
from pmbot.eval.sweep import evaluate_candidate, select_parameter, sweep_train_test

__all__ = [
    "BootstrapInterval",
    "CalibrationBin",
    "CalibrationSummary",
    "ClvObservation",
    "ClvSummary",
    "CrossVenueEvaluationObservation",
    "EvaluationReport",
    "FillAttribution",
    "ForecastObservation",
    "ParameterCandidate",
    "ParameterPerformance",
    "PnlMetrics",
    "RealizedOutcome",
    "ScoreSummary",
    "ShortfallSummary",
    "SweepResult",
    "brier_score",
    "brier_skill",
    "build_evaluation_report",
    "cross_venue_clv",
    "evaluate_candidate",
    "implementation_shortfall",
    "log_loss",
    "pnl_metrics",
    "reliability_curve",
    "score_forecasts",
    "select_parameter",
    "sweep_train_test",
]
