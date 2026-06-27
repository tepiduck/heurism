"""WP-4 live-data evaluation runner.

The runner deliberately recomputes forecasts from ``data/raw/book`` replay
state and never reads ``data/signals``. Historical signal parquet may mix model
versions, so this path creates a clean point-in-time p_hat series from the
current ``CrossVenueSignal`` implementation before scoring resolved markets.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
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
from pmbot.runner.pairs import CollectedPairStore
from pmbot.runner.world_cup_discovery import match_world_cup_match_winner_markets
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
BOOK_REPLAY_COLUMNS = (
    "ts_local_ns",
    "ts_monotonic_ns",
    "ts_exchange",
    "condition_id",
    "asset_id",
    "event_type",
    "side",
    "price",
    "size",
    "seq_or_hash",
)
METADATA_REPLAY_COLUMNS = ("condition_id", "raw_json")
LOGGER = logging.getLogger(__name__)


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
class EvalObservationDecision:
    """One recomputed forecast and whether it entered scoring."""

    market_id: str
    polymarket_asset_id: str
    kalshi_ticker: str
    event_id: str
    ts_local_ns: int
    p_hat: Decimal
    polymarket_mid: Decimal
    kalshi_mid: Decimal
    realized: int | None
    resolution_keys_tried: tuple[tuple[str, str], ...]
    included: bool
    guard_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvalMetrics:
    """Scored metrics for included observations."""

    scoring: ScoreSummary
    calibration: CalibrationSummary
    dual_go_no_go: dict[str, object]
    regression_fit: dict[str, object]


@dataclass(frozen=True, slots=True)
class RawReplayData:
    """Vectorized raw book replay input."""

    frame: pl.DataFrame
    row_count: int
    start_ns: int
    end_ns: int
    files_scanned: int


def _load_eval_pairs(
    config: EvalRunConfig,
    replay: RawReplayData,
) -> tuple[tuple[VerifiedContractPair, ...], dict[str, object]]:
    """Load verified pairs that correspond to markets actually present in raw books."""
    static_pairs = _load_static_allowlist_pairs(config.allowlist_path)
    collected_store = CollectedPairStore(data_dir=config.data_dir)
    stored_pairs = collected_store.load()
    metadata_pairs, metadata_source = _load_recorded_world_cup_pairs(config, replay)
    raw_assets = {str(value) for value in replay.frame["asset_id"].unique().to_list()}
    raw_conditions = {str(value) for value in replay.frame["condition_id"].unique().to_list()}

    combined: dict[tuple[str, str], VerifiedContractPair] = {}
    source_by_key: dict[tuple[str, str], str] = {}
    for pair in static_pairs:
        key = _pair_key(pair)
        combined[key] = pair
        source_by_key[key] = "static_allowlist"
    for pair in stored_pairs:
        key = _pair_key(pair)
        combined[key] = pair
        source_by_key[key] = "collector_verified_pair_store"
    for pair in metadata_pairs:
        key = _pair_key(pair)
        combined[key] = pair
        source_by_key[key] = "recorded_world_cup_metadata"

    collected_pairs = tuple(
        sorted(
            (
                pair
                for pair in combined.values()
                if pair.polymarket_asset_id in raw_assets and pair.kalshi_ticker in raw_assets
            ),
            key=lambda pair: (pair.kalshi_ticker, pair.polymarket_condition_id),
        )
    )
    source: dict[str, object] = {
        "pair_selection": (
            "static allowlist plus collector persisted verified pairs plus reconstructed "
            "recorded World Cup metadata, filtered to pairs whose Polymarket YES asset and "
            "Kalshi ticker both appear in raw books"
        ),
        "allowlist_path": str(config.allowlist_path),
        "collector_pair_store_path": str(collected_store.path),
        "static_pairs_loaded": len(static_pairs),
        "collector_pair_store_pairs_loaded": len(stored_pairs),
        "recorded_metadata_pairs_loaded": len(metadata_pairs),
        "candidate_pairs_loaded": len(combined),
        "collected_pairs_selected": len(collected_pairs),
        "raw_book_assets_seen": len(raw_assets),
        "raw_book_conditions_seen": len(raw_conditions),
        "selected_pair_sources": dict(
            sorted(Counter(source_by_key[_pair_key(pair)] for pair in collected_pairs).items())
        ),
        "recorded_metadata": metadata_source,
    }
    LOGGER.info(
        "eval: selected %s collected pairs (%s static, %s stored, %s metadata-derived, "
        "%s candidates)",
        len(collected_pairs),
        len(static_pairs),
        len(stored_pairs),
        len(metadata_pairs),
        len(combined),
    )
    return collected_pairs, source


def _load_static_allowlist_pairs(path: Path) -> tuple[VerifiedContractPair, ...]:
    if not path.exists():
        LOGGER.warning("eval: allowlist %s does not exist; relying on recorded metadata", path)
        return ()
    return load_verified_pairs(path)


def _load_recorded_world_cup_pairs(
    config: EvalRunConfig,
    replay: RawReplayData,
) -> tuple[tuple[VerifiedContractPair, ...], dict[str, object]]:
    raw_conditions = {str(value) for value in replay.frame["condition_id"].unique().to_list()}
    files = _metadata_parquet_files(config, raw_conditions)
    if not files:
        empty_source: dict[str, object] = {
            "metadata_files_scanned": 0,
            "metadata_market_partitions_requested": len(raw_conditions),
            "metadata_rows_loaded": 0,
            "polymarket_markets_loaded": 0,
            "kalshi_markets_loaded": 0,
        }
        return (), empty_source

    LOGGER.info(
        "eval: scanning %s metadata parquet files for %s raw book markets",
        len(files),
        len(raw_conditions),
    )
    frame = (
        pl.scan_parquet([str(path) for path in files])
        .select(METADATA_REPLAY_COLUMNS)
        .unique(subset=["condition_id", "raw_json"], maintain_order=True)
        .collect(engine="streaming")
    )
    polymarket_markets: dict[str, Mapping[str, object]] = {}
    kalshi_markets: dict[str, Mapping[str, object]] = {}
    for condition_id, raw_json in frame.iter_rows(named=False):
        raw = _json_mapping(raw_json)
        if raw is None:
            continue
        enriched = dict(raw)
        enriched.setdefault("condition_id", str(condition_id))
        if str(condition_id).startswith("KXWCGAME-"):
            enriched.setdefault("ticker", str(condition_id))
        if _metadata_row_looks_polymarket(enriched):
            polymarket_markets[_metadata_market_id(enriched, fallback=str(condition_id))] = enriched
        if _metadata_row_looks_kalshi(enriched):
            kalshi_markets[_metadata_market_id(enriched, fallback=str(condition_id))] = enriched

    pairs = match_world_cup_match_winner_markets(
        tuple(polymarket_markets.values()),
        tuple(kalshi_markets.values()),
    )
    source: dict[str, object] = {
        "metadata_files_scanned": len(files),
        "metadata_market_partitions_requested": len(raw_conditions),
        "metadata_rows_loaded": frame.height,
        "polymarket_markets_loaded": len(polymarket_markets),
        "kalshi_markets_loaded": len(kalshi_markets),
        "pairs_matched": len(pairs),
    }
    LOGGER.info(
        "eval: reconstructed %s World Cup pairs from recorded metadata (%s PM, %s Kalshi)",
        len(pairs),
        len(polymarket_markets),
        len(kalshi_markets),
    )
    return pairs, source


def _metadata_parquet_files(config: EvalRunConfig, market_ids: set[str]) -> tuple[Path, ...]:
    root = config.data_dir / "raw" / "metadata"
    if not root.exists() or not market_ids:
        return ()
    start_date = _ns_to_utc_date(config.start_ns) if config.start_ns is not None else None
    end_date = _ns_to_utc_date(config.end_ns) if config.end_ns is not None else None
    files: list[Path] = []
    for market_id in sorted(market_ids):
        for path in sorted(root.glob(f"date=*/market={market_id}/*.parquet")):
            partition_date = _date_partition(path)
            if partition_date is not None:
                if start_date is not None and partition_date < start_date:
                    continue
                if end_date is not None and partition_date > end_date:
                    continue
            files.append(path)
    return tuple(files)


def _json_mapping(value: object) -> Mapping[str, object] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, Mapping) else None


def _metadata_row_looks_polymarket(raw: Mapping[str, object]) -> bool:
    condition_id = raw.get("conditionId") or raw.get("condition_id")
    return (
        any(key in raw for key in ("clobTokenIds", "clob_token_ids"))
        or (isinstance(condition_id, str) and condition_id.startswith("0x"))
    )


def _metadata_row_looks_kalshi(raw: Mapping[str, object]) -> bool:
    ticker = raw.get("ticker") or raw.get("condition_id") or raw.get("conditionId")
    return isinstance(ticker, str) and ticker.startswith("KXWCGAME-")


def _metadata_market_id(raw: Mapping[str, object], *, fallback: str) -> str:
    for key in ("ticker", "conditionId", "condition_id", "market"):
        value = raw.get(key)
        if isinstance(value, str) and value:
            return value
    return fallback


def _pair_key(pair: VerifiedContractPair) -> tuple[str, str]:
    return pair.polymarket_condition_id, pair.kalshi_ticker


def run_eval(config: EvalRunConfig) -> dict[str, object]:
    """Run WP-4 evaluation from raw books and write an immutable JSON report."""
    if config.report_path.exists():
        raise FileExistsError(config.report_path)
    replay = _load_raw_replay_data(config)
    if replay.row_count == 0:
        raise ValueError(f"no raw replay events found under {config.data_dir / 'raw'}")
    pairs, pair_source = _load_eval_pairs(config, replay)
    if not pairs:
        raise ValueError(
            "no collected verified pairs available for eval; "
            f"source={pair_source}"
        )
    outcomes = _load_resolution_outcomes(config.data_dir)
    decisions, signal_count = _recompute_observation_decisions(
        replay=replay,
        pairs=pairs,
        outcomes=outcomes,
        config=config,
    )
    included = tuple(_forecast_observation(decision) for decision in decisions if decision.included)
    if not included:
        payload = _diagnostic_payload(
            config=config,
            pairs=pairs,
            pair_source=pair_source,
            replay=replay,
            signal_count=signal_count,
            decisions=decisions,
            outcomes=outcomes,
        )
        _write_report(config.report_path, payload)
        _log_no_observations_diagnostic(payload)
        return payload

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
        pair_source=pair_source,
        replay=replay,
        signal_count=signal_count,
        decisions=decisions,
        metrics=metrics,
    )
    _write_report(config.report_path, payload)
    return payload


def _load_raw_replay_data(config: EvalRunConfig) -> RawReplayData:
    """Load only raw book rows; do not read precomputed signal parquet."""
    root = config.data_dir / "raw" / "book"
    files = _book_parquet_files(config)
    if not files:
        return RawReplayData(
            frame=pl.DataFrame({column: [] for column in BOOK_REPLAY_COLUMNS}),
            row_count=0,
            start_ns=0,
            end_ns=0,
            files_scanned=0,
        )

    LOGGER.info("eval: scanning %s raw book parquet files under %s", len(files), root)
    scan = pl.scan_parquet([str(path) for path in files]).select(BOOK_REPLAY_COLUMNS)
    if config.start_ns is not None:
        scan = scan.filter(pl.col("ts_local_ns") >= config.start_ns)
    if config.end_ns is not None:
        scan = scan.filter(pl.col("ts_local_ns") <= config.end_ns)
    frame = scan.sort(["ts_local_ns", "asset_id", "side"]).collect(engine="streaming")
    row_count = frame.height
    if row_count == 0:
        return RawReplayData(
            frame=frame,
            row_count=0,
            start_ns=0,
            end_ns=0,
            files_scanned=len(files),
        )
    start_ns = _required_int(frame["ts_local_ns"].min())
    end_ns = _required_int(frame["ts_local_ns"].max())
    LOGGER.info(
        "eval: loaded %s raw book rows from %s files (%s..%s)",
        row_count,
        len(files),
        start_ns,
        end_ns,
    )
    return RawReplayData(
        frame=frame,
        row_count=row_count,
        start_ns=start_ns,
        end_ns=end_ns,
        files_scanned=len(files),
    )


def _iter_book_events(frame: pl.DataFrame) -> Iterable[BookEvent]:
    for (
        ts_local_ns,
        ts_monotonic_ns,
        ts_exchange,
        condition_id,
        asset_id,
        event_type,
        side,
        price,
        size,
        seq_or_hash,
    ) in frame.iter_rows(named=False):
        yield BookEvent(
            ts_local_ns=int(ts_local_ns),
            ts_monotonic_ns=_optional_int(ts_monotonic_ns),
            ts_exchange=_optional_str(ts_exchange),
            condition_id=str(condition_id),
            asset_id=str(asset_id),
            event_type=BookEventType(str(event_type)),
            side=BookSide(str(side)),
            price=_decimal(price),
            size=_decimal(size),
            seq_or_hash=_optional_str(seq_or_hash),
        )


def _book_parquet_files(config: EvalRunConfig) -> tuple[Path, ...]:
    root = config.data_dir / "raw" / "book"
    if not root.exists():
        return ()
    start_date = _ns_to_utc_date(config.start_ns) if config.start_ns is not None else None
    end_date = _ns_to_utc_date(config.end_ns) if config.end_ns is not None else None
    files: list[Path] = []
    for path in sorted(root.glob("date=*/market=*/*.parquet")):
        partition_date = _date_partition(path)
        if partition_date is not None:
            if start_date is not None and partition_date < start_date:
                continue
            if end_date is not None and partition_date > end_date:
                continue
        files.append(path)
    return tuple(files)


def _ns_to_utc_date(value: int) -> date:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).date()


def _date_partition(path: Path) -> date | None:
    for part in path.parts:
        if not part.startswith("date="):
            continue
        try:
            return date.fromisoformat(part.removeprefix("date="))
        except ValueError:
            return None
    return None


def _recompute_observation_decisions(
    *,
    replay: RawReplayData,
    pairs: tuple[VerifiedContractPair, ...],
    outcomes: Mapping[tuple[str, str], int],
    config: EvalRunConfig,
) -> tuple[tuple[EvalObservationDecision, ...], int]:
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
    decisions: list[EvalObservationDecision] = []
    signal_count = 0

    def strategy(view: PointInTimeView) -> Iterable[OrderRequest]:
        nonlocal signal_count
        for value in signal.compute(view):
            pair = pairs_by_ticker[value.kalshi_ticker]
            signal_count += 1
            decisions.append(
                _observation_decision(
                    value=value,
                    pair=pair,
                    guard_reasons=_liquidity_guards(view, pair, config=config),
                    outcomes=outcomes,
                    config=config,
                )
            )
        return ()

    LOGGER.info("eval: replaying %s sorted raw book events", replay.row_count)
    BacktestEngine(
        BacktestConfig(
            latency_ms=0,
            allow_zero_costs=True,
            retain_event_history=False,
        ),
    ).run_sorted(
        _iter_book_events(replay.frame),
        strategy,
        progress=_progress_logger(replay.row_count),
        total_events=replay.row_count,
    )
    LOGGER.info(
        "eval: replay complete; recomputed %s signal rows from %s events",
        signal_count,
        replay.row_count,
    )
    return tuple(decisions), signal_count


def _observation_decision(
    *,
    value: SignalValue,
    pair: VerifiedContractPair,
    guard_reasons: tuple[str, ...],
    outcomes: Mapping[tuple[str, str], int],
    config: EvalRunConfig,
) -> EvalObservationDecision:
    resolution_keys = _resolution_keys(pair)
    realized = _realized_for_pair(pair, outcomes)
    effective_guard_reasons = list(guard_reasons)
    if realized is None:
        effective_guard_reasons.append("unresolved: no binary outcome in data/resolutions")
    if value.confidence == Decimal("0") and "LOW CONFIDENCE:" in value.rationale:
        effective_guard_reasons.append(_extract_low_confidence_reason(value.rationale))
    if _is_knockout_stage(pair, config.knockout_start_date) and not config.include_knockout:
        effective_guard_reasons.append("knockout_stage_unverified_resolution_criteria")
    included = realized is not None and not effective_guard_reasons
    return EvalObservationDecision(
        market_id=pair.polymarket_condition_id,
        polymarket_asset_id=pair.polymarket_asset_id,
        kalshi_ticker=pair.kalshi_ticker,
        event_id=_event_id(pair),
        ts_local_ns=value.ts_local_ns,
        p_hat=value.fair_probability,
        polymarket_mid=value.polymarket_probability,
        kalshi_mid=value.kalshi_probability,
        realized=realized,
        resolution_keys_tried=resolution_keys,
        included=included,
        guard_reasons=tuple(effective_guard_reasons),
    )


def _progress_logger(total_events: int) -> Callable[[int, int | None, int], None]:
    next_percent = 10

    def progress(processed: int, total: int | None, ts_local_ns: int) -> None:
        nonlocal next_percent
        if total is None or total <= 0:
            return
        percent = int(processed * 100 / total)
        if percent < next_percent and processed < total:
            return
        LOGGER.info(
            "eval: replay %s%% (%s/%s events, ts=%s)",
            min(100, percent),
            processed,
            total,
            ts_local_ns,
        )
        while next_percent <= percent:
            next_percent += 10

    return progress


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


def _resolution_keys(pair: VerifiedContractPair) -> tuple[tuple[str, str], ...]:
    return (
        ("polymarket", pair.polymarket_condition_id),
        ("kalshi", pair.kalshi_ticker),
    )


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
    pair_source: Mapping[str, object],
    replay: RawReplayData,
    signal_count: int,
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
            "start_ns": replay.start_ns,
            "end_ns": replay.end_ns,
        },
        "source": {
            "signals_from": "data/raw/book replay via CrossVenueSignal",
            "data_signals_used": False,
            "allowlist_path": str(config.allowlist_path),
            "pair_source": dict(pair_source),
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
        "n_raw_book_events": replay.row_count,
        "n_raw_book_files_scanned": replay.files_scanned,
        "n_recomputed_signal_rows": signal_count,
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
        "pair_diagnostics": _pair_diagnostics(pairs, decisions, outcomes={}),
    }


def _diagnostic_payload(
    *,
    config: EvalRunConfig,
    pairs: tuple[VerifiedContractPair, ...],
    pair_source: Mapping[str, object],
    replay: RawReplayData,
    signal_count: int,
    decisions: tuple[EvalObservationDecision, ...],
    outcomes: Mapping[tuple[str, str], int],
) -> dict[str, object]:
    config_json = json.dumps(config.jsonable(), sort_keys=True, separators=(",", ":"))
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "diagnostic": True,
        "config": config.jsonable(),
        "config_hash": hashlib.sha256(config_json.encode("utf-8")).hexdigest(),
        "data_time_range": {
            "start_ns": replay.start_ns,
            "end_ns": replay.end_ns,
        },
        "source": {
            "signals_from": "data/raw/book replay via CrossVenueSignal",
            "data_signals_used": False,
            "allowlist_path": str(config.allowlist_path),
            "pair_source": dict(pair_source),
        },
        "headline": (
            "DIAGNOSTIC ONLY: no resolved, guard-passing observations available for scoring."
        ),
        "small_n": {
            "flag": True,
            "message": "No resolved, guard-passing observations were available.",
        },
        "n_pairs_loaded": len(pairs),
        "n_raw_book_events": replay.row_count,
        "n_raw_book_files_scanned": replay.files_scanned,
        "n_recomputed_signal_rows": signal_count,
        "n_observations": 0,
        "distinct_event_count": 0,
        "guards": _guard_summary(decisions),
        "pair_diagnostics": _pair_diagnostics(pairs, decisions, outcomes=outcomes),
        "excluded_markets": _excluded_market_summaries(decisions),
        "metrics": None,
    }


def _write_report(report_path: Path, payload: Mapping[str, object]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(_jsonable(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _log_no_observations_diagnostic(payload: Mapping[str, object]) -> None:
    guards = payload.get("guards")
    reason_counts = guards.get("reason_counts", {}) if isinstance(guards, Mapping) else {}
    LOGGER.warning(
        "eval: no resolved, guard-passing observations; reason_counts=%s",
        reason_counts,
    )
    pair_diagnostics = payload.get("pair_diagnostics")
    if not isinstance(pair_diagnostics, list):
        return
    for item in pair_diagnostics:
        if not isinstance(item, Mapping):
            continue
        LOGGER.warning(
            "eval: pair %s/%s samples=%s resolution_found=%s keys=%s guard_reasons=%s",
            item.get("polymarket_condition_id"),
            item.get("kalshi_ticker"),
            item.get("signal_samples"),
            item.get("resolution_found"),
            item.get("resolution_keys_tried"),
            item.get("guard_reason_counts"),
        )


def _pair_diagnostics(
    pairs: tuple[VerifiedContractPair, ...],
    decisions: Sequence[EvalObservationDecision],
    *,
    outcomes: Mapping[tuple[str, str], int],
) -> list[dict[str, object]]:
    by_pair: dict[tuple[str, str], list[EvalObservationDecision]] = {}
    for decision in decisions:
        by_pair.setdefault((decision.market_id, decision.kalshi_ticker), []).append(decision)

    diagnostics: list[dict[str, object]] = []
    for pair in pairs:
        key = _pair_key(pair)
        pair_decisions = by_pair.get(key, [])
        reason_counter: Counter[str] = Counter()
        for decision in pair_decisions:
            for reason in decision.guard_reasons:
                reason_counter[_guard_key(reason)] += 1
        realized = _realized_for_pair(pair, outcomes) if outcomes else None
        if outcomes:
            resolution_found = any(
                resolution_key in outcomes for resolution_key in _resolution_keys(pair)
            )
        else:
            resolution_found = any(decision.realized is not None for decision in pair_decisions)
            if pair_decisions:
                realized = pair_decisions[-1].realized
        diagnostics.append(
            {
                "polymarket_condition_id": pair.polymarket_condition_id,
                "polymarket_asset_id": pair.polymarket_asset_id,
                "kalshi_ticker": pair.kalshi_ticker,
                "event_id": _event_id(pair),
                "signal_samples": len(pair_decisions),
                "resolution_found": resolution_found,
                "realized": realized,
                "resolution_keys_tried": _resolution_key_dicts(_resolution_keys(pair)),
                "guard_reason_counts": dict(sorted(reason_counter.items())),
                "first_signal_ts_local_ns": (
                    min(decision.ts_local_ns for decision in pair_decisions)
                    if pair_decisions
                    else None
                ),
                "last_signal_ts_local_ns": (
                    max(decision.ts_local_ns for decision in pair_decisions)
                    if pair_decisions
                    else None
                ),
            }
        )
    return diagnostics


def _resolution_key_dicts(keys: Sequence[tuple[str, str]]) -> list[dict[str, str]]:
    return [{"venue": venue, "market_id": market_id} for venue, market_id in keys]


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


def _required_int(value: object) -> int:
    parsed = _optional_int(value)
    if parsed is None:
        raise ValueError("expected non-null integer")
    return parsed


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
