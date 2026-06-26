"""Typed evaluation records and summaries for WP-4."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from pmbot.backtest.models import Fill


@dataclass(frozen=True, slots=True)
class RealizedOutcome:
    """Resolved binary outcome for one market."""

    market_id: str
    realized: int
    resolved_at: str | None = None


@dataclass(frozen=True, slots=True)
class ForecastObservation:
    """One point-in-time forecast paired with a resolved binary outcome."""

    market_id: str
    ts_local_ns: int
    model_probability: Decimal
    market_mid: Decimal
    realized: int
    event_id: str | None = None
    executable_price: Decimal | None = None

    @property
    def market_probability(self) -> Decimal:
        """Backward-compatible alias for the non-executable market mid baseline."""
        return self.market_mid


@dataclass(frozen=True, slots=True)
class CrossVenueEvaluationObservation:
    """Raw cross-venue inputs used to calibrate WP-3 constants."""

    market_id: str
    ts_local_ns: int
    polymarket_probability: Decimal
    kalshi_probability: Decimal
    realized: int
    pnl: Decimal = Decimal("0")
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class BootstrapInterval:
    """Bootstrap confidence interval."""

    lower: float
    upper: float


@dataclass(frozen=True, slots=True)
class ScoreSummary:
    """Proper scoring summary against the market-price baseline."""

    n: int
    distinct_event_count: int
    brier_model: float
    brier_market: float
    brier_skill: float
    brier_difference: float
    log_loss_model: float
    log_loss_market: float
    brier_skill_ci: BootstrapInterval | None = None
    brier_difference_ci: BootstrapInterval | None = None
    small_n: bool = False


@dataclass(frozen=True, slots=True)
class CalibrationBin:
    """Reliability-curve bin."""

    lower: float
    upper: float
    count: int
    mean_forecast: float | None
    empirical_frequency: float | None
    confidence_band: BootstrapInterval | None = None


@dataclass(frozen=True, slots=True)
class CalibrationSummary:
    """Calibration curve and expected calibration error."""

    n: int
    bins: tuple[CalibrationBin, ...]
    ece: float
    small_n: bool


@dataclass(frozen=True, slots=True)
class PnlMetrics:
    """PnL metrics computed over valid mark spans only."""

    total_pnl: Decimal
    sharpe: float | None
    max_drawdown: Decimal
    hit_rate: float | None
    n_valid_marks: int
    n_invalid_marks: int
    pnl_ci: BootstrapInterval | None = None


@dataclass(frozen=True, slots=True)
class FillAttribution:
    """Per-fill inputs for implementation-shortfall attribution."""

    fill: Fill
    arrival_mid: Decimal
    fair_value: Decimal
    realized_value: Decimal


@dataclass(frozen=True, slots=True)
class ShortfallSummary:
    """Execution and alpha decomposition of realized fill PnL."""

    total_pnl: Decimal
    alpha_pnl: Decimal
    execution_pnl: Decimal
    execution_cost: Decimal
    residual_pnl: Decimal
    n_fills: int


@dataclass(frozen=True, slots=True)
class ClvObservation:
    """Entry versus later Kalshi reference price."""

    market_id: str
    entry_ts_local_ns: int
    entry_price: Decimal
    later_kalshi_probability: Decimal
    side: str


@dataclass(frozen=True, slots=True)
class ClvSummary:
    """Cross-venue CLV diagnostic."""

    n: int
    mean_clv: Decimal


@dataclass(frozen=True, slots=True)
class ParameterCandidate:
    """Calibrated WP-3 constants candidate."""

    double_counting_overlap: Decimal
    confidence_scalar: Decimal


@dataclass(frozen=True, slots=True)
class ParameterPerformance:
    """Performance for one parameter candidate on one split."""

    candidate: ParameterCandidate
    score: float
    brier_skill: float
    total_pnl: Decimal
    evaluated_market_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SweepResult:
    """Train-selected parameters and held-out validation performance."""

    selected: ParameterCandidate
    train_performance: ParameterPerformance
    test_performance: ParameterPerformance
    in_sample_out_of_sample_gap: float


@dataclass(frozen=True, slots=True)
class VerdictSummary:
    """Dual go/no-go verdict from held-out proper scoring and actual-fill PnL."""

    proper_score_positive: bool
    pnl_positive: bool
    go: bool
    net_pnl: Decimal
    net_pnl_ci: BootstrapInterval | None
    reason: str


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    """Complete WP-4 fixture report."""

    scoring: ScoreSummary
    calibration: CalibrationSummary
    pnl: PnlMetrics
    shortfall: ShortfallSummary
    sweep: SweepResult
    clv: ClvSummary | None
    verdict: VerdictSummary
    headline: str
