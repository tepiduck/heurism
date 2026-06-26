import json
from decimal import Decimal

from pmbot.backtest.costs import LiquidityFlag
from pmbot.backtest.engine import BacktestResult
from pmbot.backtest.models import Fill, OrderSide, PositionMark
from pmbot.eval.manifest import write_run_manifest
from pmbot.eval.models import (
    CrossVenueEvaluationObservation,
    EvaluationReport,
    FillAttribution,
    ForecastObservation,
    ParameterCandidate,
)
from pmbot.eval.report import build_evaluation_report


def test_positive_brier_skill_without_significant_pnl_is_not_confirmed_edge() -> None:
    fill = fixture_fill()

    report = build_evaluation_report(
        forecasts=(
            forecast("m1", "0.80", "0.60", 1),
            forecast("m2", "0.20", "0.40", 0),
        ),
        backtest=BacktestResult(
            blotter=(fill,),
            pnl_series=(mark(1, "0"), mark(2, "1")),
            pnl_valid=True,
        ),
        shortfall_rows=(
            FillAttribution(
                fill=fill,
                arrival_mid=Decimal("0.50"),
                fair_value=Decimal("0.60"),
                realized_value=Decimal("1"),
            ),
        ),
        train=(cross_venue_row("train", "0.50", "0.80", 1),),
        test=(cross_venue_row("test", "0.50", "0.80", 1),),
        candidates=(ParameterCandidate(Decimal("0"), Decimal("1")),),
        bootstrap_samples=0,
    )

    assert report.scoring.brier_skill > 0
    assert report.scoring.small_n is True
    assert report.headline == (
        "NO-GO: holdout proper-score improvement and net PnL are not both positive."
    )


def test_dual_verdict_requires_score_and_pnl_gates() -> None:
    a_positive_b_negative = fixture_report(
        forecasts=(forecast("m1", "0.80", "0.60", 1), forecast("m2", "0.20", "0.40", 0)),
        test_pnls=("-1", "-1"),
    )
    b_positive_a_negative = fixture_report(
        forecasts=(forecast("m1", "0.60", "0.80", 1), forecast("m2", "0.40", "0.20", 0)),
        test_pnls=("1", "1"),
    )
    both_positive = fixture_report(
        forecasts=(forecast("m1", "0.80", "0.60", 1), forecast("m2", "0.20", "0.40", 0)),
        test_pnls=("1", "1"),
    )

    assert a_positive_b_negative.verdict.proper_score_positive is True
    assert a_positive_b_negative.verdict.pnl_positive is False
    assert a_positive_b_negative.verdict.go is False
    assert b_positive_a_negative.verdict.proper_score_positive is False
    assert b_positive_a_negative.verdict.pnl_positive is True
    assert b_positive_a_negative.verdict.go is False
    assert both_positive.verdict.go is True
    assert both_positive.headline.startswith("EDGE CONFIRMED")


def test_eval_run_manifest_is_immutable_and_contains_reported_metrics(tmp_path) -> None:
    report = fixture_report(
        forecasts=(forecast("m1", "0.80", "0.60", 1), forecast("m2", "0.20", "0.40", 0)),
        test_pnls=("1", "1"),
    )
    manifest_path = tmp_path / "manifest.json"

    write_run_manifest(
        manifest_path,
        report=report,
        config={"overlap": "0", "confidence": "1"},
        data_start_ns=1,
        data_end_ns=2,
    )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["config_hash"]
    assert payload["data_time_range"] == {"end_ns": 2, "start_ns": 1}
    assert payload["n_observations"] == report.scoring.n
    assert payload["distinct_event_count"] == report.scoring.distinct_event_count
    assert payload["metrics"]["scoring"]["brier_skill"] == report.scoring.brier_skill

    try:
        write_run_manifest(
            manifest_path,
            report=report,
            config={"overlap": "0", "confidence": "1"},
            data_start_ns=1,
            data_end_ns=2,
        )
    except FileExistsError:
        pass
    else:
        raise AssertionError("manifest overwrite should be rejected")


def forecast(
    market_id: str,
    model_probability: str,
    market_probability: str,
    realized: int,
) -> ForecastObservation:
    return ForecastObservation(
        market_id=market_id,
        ts_local_ns=1,
        model_probability=Decimal(model_probability),
        market_mid=Decimal(market_probability),
        realized=realized,
        event_id=market_id,
    )


def cross_venue_row(
    market_id: str,
    polymarket_probability: str,
    kalshi_probability: str,
    realized: int,
) -> CrossVenueEvaluationObservation:
    return CrossVenueEvaluationObservation(
        market_id=market_id,
        ts_local_ns=1,
        polymarket_probability=Decimal(polymarket_probability),
        kalshi_probability=Decimal(kalshi_probability),
        realized=realized,
        event_id=market_id,
    )


def fixture_report(
    *,
    forecasts: tuple[ForecastObservation, ...],
    test_pnls: tuple[str, str],
) -> EvaluationReport:
    fill = fixture_fill()
    return build_evaluation_report(
        forecasts=forecasts,
        backtest=BacktestResult(
            blotter=(fill,),
            pnl_series=(mark(1, "0"), mark(2, "1")),
            pnl_valid=True,
        ),
        shortfall_rows=(
            FillAttribution(
                fill=fill,
                arrival_mid=Decimal("0.50"),
                fair_value=Decimal("0.60"),
                realized_value=Decimal("1"),
            ),
        ),
        train=(cross_venue_row("train", "0.50", "0.80", 1),),
        test=(
            CrossVenueEvaluationObservation(
                "test-a",
                1,
                Decimal("0.50"),
                Decimal("0.80"),
                1,
                Decimal(test_pnls[0]),
                "test-a",
            ),
            CrossVenueEvaluationObservation(
                "test-b",
                1,
                Decimal("0.50"),
                Decimal("0.80"),
                1,
                Decimal(test_pnls[1]),
                "test-b",
            ),
        ),
        candidates=(ParameterCandidate(Decimal("0"), Decimal("1")),),
        bootstrap_samples=50,
    )


def mark(ts: int, pnl: str) -> PositionMark:
    return PositionMark.create(
        ts_local_ns=ts,
        cash=Decimal("0"),
        positions={},
        mark_to_market=Decimal(pnl),
        pnl=Decimal(pnl),
        pnl_valid=True,
    )


def fixture_fill() -> Fill:
    return Fill(
        order_id="order-1",
        client_order_id=None,
        asset_id="asset-1",
        side=OrderSide.BUY,
        price=Decimal("0.50"),
        size=Decimal("1"),
        liquidity=LiquidityFlag.TAKER,
        decision_ts_local_ns=1,
        arrival_ts_local_ns=1,
        execution_ts_local_ns=1,
        notional=Decimal("0.50"),
        fee=Decimal("0"),
        gas=Decimal("0"),
        total_cost=Decimal("0"),
        source_trade_id=None,
    )
