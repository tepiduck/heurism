"""WP-4 live-data evaluation runner.

The runner deliberately recomputes forecasts from ``data/raw/book`` replay
state and never reads ``data/signals``. Historical signal parquet may mix model
versions, so this path creates a clean point-in-time p_hat series from the
current ``CrossVenueSignal`` implementation before scoring resolved markets.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from pmbot.backtest.engine import BacktestConfig, BacktestEngine, PointInTimeView
from pmbot.backtest.models import OrderRequest
from pmbot.eval.calibration import reliability_curve
from pmbot.eval.models import (
    CalibrationSummary,
    ForecastObservation,
    ScoreSummary,
)
from pmbot.eval.scoring import score_forecasts
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide
from pmbot.runner.allowlist import load_verified_pairs
from pmbot.signals.contracts import VerifiedContractPair
from pmbot.signals.cross_venue import (
    CrossVenueSignal,
    CrossVenueSignalConfig,
    RegressionFeatures,
    ShrinkageRegressionObservation,
    SignalValue,
    fit_shrinkage_regression,
)

DEFAULT_ALLOWLIST_PATH = Path("pairs.draft.jsonl")
DEFAULT_KNOCKOUT_START_DATE = date(2026, 7, 1)
DEFAULT_SMALL_N_THRESHOLD = 30


@dataclass(frozen=True, slots=True)
class EvalRunConfig:
    """Configuration for one immutable WP-4 eval run."""

    data_dir: Path
    report_path: Path
    allowlist_path: Path = DEFAULT_ALLOWLIST_PATH
    start_ns: int | None = None
    end_ns: int | None = None
    bootstrap_samples: int = 200
    calibration_bins: int = 10
    max_bid_ask_spread: Decimal | None = Decimal("0.10")
    three_way_sum_tolerance: Decimal | None = Decimal("0.05")
    max_quote_age_ns: int = 2_000_000_000
    require_kalshi_leads: bool = True
    min_top_of_book_size: Decimal | None = None
    min_distinct_events_for_verdict: int = DEFAULT_SMALL_N_THRESHOLD
    fit_regression: bool = False
    min_distinct_events_for_regression: int = DEFAULT_SMALL_N_THRESHOLD
    knockout_start_date: date | None = DEFAULT_KNOCKOUT_START_DATE
    include_knockout: bool = False

    def jsonable(self) -> dict[str, object]:
        """Return stable JSON-compatible config values."""
        return {
            "data_dir": str(self.data_dir),
            "allowlist_path": str(self.allowlist_path),
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "bootstrap_samples": self.bootstrap_samples,
            "calibration_bins": self.calibration_bins,
            "max_bid_ask_spread": _json_decimal(self.max_bid_ask_spread),
            "three_way_sum_tolerance": _json_decimal(self.three_way_sum_tolerance),
            "max_quote_age_ns": self.max_quote_age_ns,
            "require_kalshi_leads": self.require_kalshi_leads,
            "min_top_of_book_size": _json_decimal(self.min_top_of_book_size),
            "min_distinct_events_for_verdict": self.min_distinct_events_for_verdict,
            "fit_regression": self.fit_regression,
            "min_distinct_events_for_regression": self.min_distinct_events_for_regression,
            "knockout_start_date": (
                self.knockout_start_date.isoformat()
                if self.knockout_start_date is not None
                else None
            ),
            "include_knockout": self.include_knockout,
        }


@dataclass(frozen=True, slots=True)
class EvalSignalSample:
    """One recomputed signal plus eval-only guard annotations."""

    value: SignalValue
    pair: VerifiedContractPair
    guard_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvalObservationDecision:
    """One recomputed forecast and whether it entered scoring."""

    market_id: str
    event_id: str
    ts_local_ns: int
    p_hat: Decimal
    polymarket_mid: Decimal
    kalshi_mid: Decimal
    realized: int | None
    included: bool
    guard_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvalMetrics:
    """Scored metrics for included observations."""

    scoring: ScoreSummary
    calibration: CalibrationSummary
    dual_go_no_go: dict[str, object]
    regression_fit: dict[str, object]


def run_eval(config: EvalRunConfig) -> dict[str, object]:
    """Run WP-4 evaluation from raw books and write an immutable JSON report."""
    if config.report_path.exists():
        raise FileExistsError(config.report_path)
    pairs = load_verified_pairs(config.allowlist_path)
    if not pairs:
        raise ValueError(f"no verified pairs loaded from {config.allowlist_path}")
    events = _load_raw_replay_events(config)
    if not events:
        raise ValueError(f"no raw replay events found under {config.data_dir / 'raw'}")
    outcomes = _load_resolution_outcomes(config.data_dir)
    samples = _recompute_signal_samples(events=events, pairs=pairs, config=config)
    decisions = _decide_observations(samples=samples, outcomes=outcomes, config=config)
    included = tuple(_forecast_observation(decision) for decision in decisions if decision.included)
    if not included:
        raise ValueError("no resolved, guard-passing observations available for scoring")

    scoring = score_forecasts(included, bootstrap_samples=config.bootstrap_samples)
    calibration = reliability_curve(
        included,
        bin_count=config.calibration_bins,
        bootstrap_samples=config.bootstrap_samples,
    )
    regression_rows = tuple(
        _regression_observation(decision) for decision in decisions if decision.included
    )
    metrics = EvalMetrics(
        scoring=scoring,
        calibration=calibration,
        dual_go_no_go=_dual_go_no_go(scoring, config=config),
        regression_fit=_regression_fit_status(
            regression_rows,
            distinct_event_count=scoring.distinct_event_count,
            config=config,
        ),
    )
    payload = _manifest_payload(
        config=config,
        pairs=pairs,
        events=events,
        samples=samples,
        decisions=decisions,
        metrics=metrics,
    )
    config.report_path.parent.mkdir(parents=True, exist_ok=True)
    config.report_path.write_text(
        json.dumps(_jsonable(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return payload


def _load_raw_replay_events(config: EvalRunConfig) -> tuple[BookEvent, ...]:
    """Load only raw book rows; do not read precomputed signal parquet."""
    rows: list[dict[str, Any]] = []
    root = config.data_dir / "raw" / "book"
    for path in sorted(root.glob("**/*.parquet")) if root.exists() else ():
        frame = pl.read_parquet(path)
        if config.start_ns is not None:
            frame = frame.filter(pl.col("ts_local_ns") >= config.start_ns)
        if config.end_ns is not None:
            frame = frame.filter(pl.col("ts_local_ns") <= config.end_ns)
        rows.extend(frame.to_dicts())
    events = [
        BookEvent(
            ts_local_ns=int(row["ts_local_ns"]),
            ts_monotonic_ns=_optional_int(row.get("ts_monotonic_ns")),
            ts_exchange=_optional_str(row.get("ts_exchange")),
            condition_id=str(row["condition_id"]),
            asset_id=str(row["asset_id"]),
            event_type=BookEventType(str(row["event_type"])),
            side=BookSide(str(row["side"])),
            price=_decimal(row["price"]),
            size=_decimal(row["size"]),
            seq_or_hash=_optional_str(row.get("seq_or_hash")),
        )
        for row in rows
    ]
    return tuple(sorted(events, key=lambda event: (event.ts_local_ns, event.asset_id, event.side)))


def _recompute_signal_samples(
    *,
    events: tuple[BookEvent, ...],
    pairs: tuple[VerifiedContractPair, ...],
    config: EvalRunConfig,
) -> tuple[EvalSignalSample, ...]:
    signal = CrossVenueSignal(
        pairs,
        CrossVenueSignalConfig(
            max_bid_ask_spread=config.max_bid_ask_spread,
            three_way_sum_tolerance=config.three_way_sum_tolerance,
            max_quote_age_ns=config.max_quote_age_ns,
            require_kalshi_leads=config.require_kalshi_leads,
        ),
    )
    pairs_by_ticker = {pair.kalshi_ticker: pair for pair in pairs}
    samples: list[EvalSignalSample] = []

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        for value in signal.compute(view):
            pair = pairs_by_ticker[value.kalshi_ticker]
            samples.append(
                EvalSignalSample(
                    value=value,
                    pair=pair,
                    guard_reasons=_liquidity_guards(view, pair, config=config),
                )
            )
        return ()

    BacktestEngine(
        BacktestConfig(latency_ms=0, allow_zero_costs=True),
    ).run(events, strategy)
    return tuple(samples)


def _decide_observations(
    *,
    samples: Sequence[EvalSignalSample],
    outcomes: Mapping[tuple[str, str], int],
    config: EvalRunConfig,
) -> tuple[EvalObservationDecision, ...]:
    decisions: list[EvalObservationDecision] = []
    for sample in samples:
        value = sample.value
        pair = sample.pair
        realized = _realized_for_pair(pair, outcomes)
        guard_reasons = list(sample.guard_reasons)
        if realized is None:
            guard_reasons.append("unresolved: no binary outcome in data/resolutions")
        if value.confidence == Decimal("0") and "LOW CONFIDENCE:" in value.rationale:
            guard_reasons.append(_extract_low_confidence_reason(value.rationale))
        if _is_knockout_stage(pair, config.knockout_start_date) and not config.include_knockout:
            guard_reasons.append("knockout_stage_unverified_resolution_criteria")
        included = realized is not None and not guard_reasons
        decisions.append(
            EvalObservationDecision(
                market_id=pair.polymarket_condition_id,
                event_id=_event_id(pair),
                ts_local_ns=value.ts_local_ns,
                p_hat=value.fair_probability,
                polymarket_mid=value.polymarket_probability,
                kalshi_mid=value.kalshi_probability,
                realized=realized,
                included=included,
                guard_reasons=tuple(guard_reasons),
            )
        )
    return tuple(decisions)


def _load_resolution_outcomes(data_dir: Path) -> dict[tuple[str, str], int]:
    root = data_dir / "resolutions"
    outcomes: dict[tuple[str, str], int] = {}
    files = sorted(root.glob("**/*.parquet")) if root.exists() else []
    for path in files:
        for row in pl.read_parquet(path).to_dicts():
            venue = str(row["venue"])
            market_id = str(row["market_id"])
            realized = _binary_outcome(row.get("resolved_outcome"))
            if realized is not None:
                outcomes[(venue, market_id)] = realized
    return outcomes


def _realized_for_pair(
    pair: VerifiedContractPair,
    outcomes: Mapping[tuple[str, str], int],
) -> int | None:
    polymarket = outcomes.get(("polymarket", pair.polymarket_condition_id))
    if polymarket is not None:
        return polymarket
    kalshi = outcomes.get(("kalshi", pair.kalshi_ticker))
    if kalshi is None:
        return None
    return 1 - kalshi if pair.invert_kalshi else kalshi


def _forecast_observation(decision: EvalObservationDecision) -> ForecastObservation:
    if decision.realized is None:
        raise ValueError("cannot score unresolved observation")
    return ForecastObservation(
        market_id=decision.market_id,
        ts_local_ns=decision.ts_local_ns,
        model_probability=decision.p_hat,
        market_mid=decision.polymarket_mid,
        realized=decision.realized,
        event_id=decision.event_id,
    )


def _regression_observation(decision: EvalObservationDecision) -> ShrinkageRegressionObservation:
    if decision.realized is None:
        raise ValueError("cannot fit unresolved observation")
    return ShrinkageRegressionObservation(
        polymarket_probability=decision.polymarket_mid,
        kalshi_probability=decision.kalshi_mid,
        target_probability=Decimal(decision.realized),
        features=RegressionFeatures(),
    )


def _dual_go_no_go(scoring: ScoreSummary, *, config: EvalRunConfig) -> dict[str, object]:
    brier_ci_positive = (
        scoring.brier_difference_ci is not None and scoring.brier_difference_ci.lower > 0
    )
    log_loss_positive = scoring.log_loss_model < scoring.log_loss_market
    enough_events = scoring.distinct_event_count >= config.min_distinct_events_for_verdict
    go = brier_ci_positive and log_loss_positive and enough_events
    reasons: list[str] = []
    if not brier_ci_positive:
        reasons.append("Brier improvement clustered CI lower bound is not positive")
    if not log_loss_positive:
        reasons.append("model log-loss does not beat Polymarket midpoint baseline")
    if not enough_events:
        reasons.append(
            f"small_n: {scoring.distinct_event_count} distinct events < "
            f"{config.min_distinct_events_for_verdict}"
        )
    return {
        "brier_ci_positive": brier_ci_positive,
        "log_loss_positive": log_loss_positive,
        "enough_distinct_events": enough_events,
        "go": go,
        "reason": "; ".join(reasons) if reasons else "both scoring gates passed",
    }


def _regression_fit_status(
    rows: Sequence[ShrinkageRegressionObservation],
    *,
    distinct_event_count: int,
    config: EvalRunConfig,
) -> dict[str, object]:
    if not config.fit_regression:
        return {
            "status": "not_requested",
            "message": "Regression fitting is opt-in; scored current unfit-fallback p_hat.",
        }
    if distinct_event_count < config.min_distinct_events_for_regression:
        return {
            "status": "skipped_small_n",
            "message": (
                f"Regression requires >= {config.min_distinct_events_for_regression} "
                f"distinct resolved events; found {distinct_event_count}."
            ),
        }
    model = fit_shrinkage_regression(rows)
    return {
        "status": "fit",
        "n_observations": model.n_observations,
        "alpha": model.alpha,
        "gamma": model.gamma,
        "beta": model.beta,
        "feature_coefficients": dict(model.feature_coefficients),
    }


def _manifest_payload(
    *,
    config: EvalRunConfig,
    pairs: tuple[VerifiedContractPair, ...],
    events: tuple[BookEvent, ...],
    samples: tuple[EvalSignalSample, ...],
    decisions: tuple[EvalObservationDecision, ...],
    metrics: EvalMetrics,
) -> dict[str, object]:
    config_json = json.dumps(config.jsonable(), sort_keys=True, separators=(",", ":"))
    included = tuple(decision for decision in decisions if decision.included)
    excluded = tuple(decision for decision in decisions if not decision.included)
    small_n = (
        metrics.scoring.distinct_event_count < config.min_distinct_events_for_verdict
    )
    headline = (
        "PRELIMINARY DIRECTIONAL READ ONLY: small_n flag is active; this is not an edge verdict."
        if small_n
        else (
            "GO: p_hat beats the Polymarket midpoint baseline on both scoring gates."
            if metrics.dual_go_no_go["go"]
            else "NO-GO: p_hat does not pass both scoring gates."
        )
    )
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "config": config.jsonable(),
        "config_hash": hashlib.sha256(config_json.encode("utf-8")).hexdigest(),
        "data_time_range": {
            "start_ns": min(event.ts_local_ns for event in events),
            "end_ns": max(event.ts_local_ns for event in events),
        },
        "source": {
            "signals_from": "data/raw/book replay via CrossVenueSignal",
            "data_signals_used": False,
            "allowlist_path": str(config.allowlist_path),
        },
        "headline": headline,
        "small_n": {
            "flag": small_n,
            "message": (
                f"Preliminary directional read on {metrics.scoring.distinct_event_count} "
                "distinct resolved events, not a verdict."
                if small_n
                else "Distinct resolved event count meets configured threshold."
            ),
        },
        "n_pairs_loaded": len(pairs),
        "n_raw_book_events": len(events),
        "n_recomputed_signal_rows": len(samples),
        "n_observations": len(included),
        "distinct_event_count": metrics.scoring.distinct_event_count,
        "guards": _guard_summary(decisions),
        "metrics": {
            "scoring": asdict(metrics.scoring),
            "calibration": asdict(metrics.calibration),
            "dual_go_no_go": metrics.dual_go_no_go,
            "regression_fit": metrics.regression_fit,
        },
        "excluded_markets": _excluded_market_summaries(excluded),
    }


def _guard_summary(decisions: Sequence[EvalObservationDecision]) -> dict[str, object]:
    counter: Counter[str] = Counter()
    for decision in decisions:
        for reason in decision.guard_reasons:
            counter[_guard_key(reason)] += 1
    return {
        "excluded_signal_rows": sum(1 for decision in decisions if not decision.included),
        "included_signal_rows": sum(1 for decision in decisions if decision.included),
        "reason_counts": dict(sorted(counter.items())),
    }


def _excluded_market_summaries(
    decisions: Sequence[EvalObservationDecision],
) -> list[dict[str, object]]:
    by_market: dict[str, set[str]] = {}
    for decision in decisions:
        by_market.setdefault(decision.market_id, set()).update(decision.guard_reasons)
    return [
        {
            "market_id": market_id,
            "event_id": next(
                decision.event_id for decision in decisions if decision.market_id == market_id
            ),
            "guard_reasons": sorted(reasons),
        }
        for market_id, reasons in sorted(by_market.items())
    ]


def _liquidity_guards(
    view: PointInTimeView,
    pair: VerifiedContractPair,
    *,
    config: EvalRunConfig,
) -> tuple[str, ...]:
    if config.min_top_of_book_size is None:
        return ()
    guards: list[str] = []
    for venue, asset_id in (
        ("Polymarket", pair.polymarket_asset_id),
        ("Kalshi", pair.kalshi_ticker),
    ):
        book = view.book(asset_id)
        sizes = [
            level.size
            for level in (book.best_bid, book.best_ask)
            if level is not None
        ]
        if len(sizes) == 2 and min(sizes) < config.min_top_of_book_size:
            guards.append(
                f"low_liquidity: {venue} top size {min(sizes)} below {config.min_top_of_book_size}"
            )
    return tuple(guards)


def _is_knockout_stage(pair: VerifiedContractPair, knockout_start: date | None) -> bool:
    rationale = pair.rationale.casefold()
    if any(
        marker in rationale
        for marker in ("knockout", "round of", "quarterfinal", "semifinal", "final")
    ):
        return True
    event_date = _kalshi_ticker_date(pair.kalshi_ticker)
    return knockout_start is not None and event_date is not None and event_date >= knockout_start


def _kalshi_ticker_date(ticker: str) -> date | None:
    match = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", ticker.upper())
    if match is None:
        return None
    year = 2000 + int(match.group(1))
    month = {
        "JAN": 1,
        "FEB": 2,
        "MAR": 3,
        "APR": 4,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AUG": 8,
        "SEP": 9,
        "OCT": 10,
        "NOV": 11,
        "DEC": 12,
    }.get(match.group(2))
    if month is None:
        return None
    return date(year, month, int(match.group(3)))


def _event_id(pair: VerifiedContractPair) -> str:
    group, separator, _leg = pair.kalshi_ticker.rpartition("-")
    return group if separator else pair.polymarket_condition_id


def _binary_outcome(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip().casefold()
    if text in {"yes", "true", "1"}:
        return 1
    if text in {"no", "false", "0"}:
        return 0
    return None


def _extract_low_confidence_reason(rationale: str) -> str:
    _prefix, _separator, reason = rationale.partition("LOW CONFIDENCE:")
    return f"low_confidence: {reason.strip()}" if reason else "low_confidence"


def _guard_key(reason: str) -> str:
    return reason.split(":", 1)[0]


def _json_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return int(str(value))


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
