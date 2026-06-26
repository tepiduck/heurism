import math
from decimal import Decimal

import pytest

from pmbot.eval.calibration import reliability_curve
from pmbot.eval.models import ForecastObservation
from pmbot.eval.scoring import brier_score, log_loss, score_forecasts


def test_brier_and_log_loss_match_hand_computed_values() -> None:
    forecasts = [Decimal("0.8"), Decimal("0.3")]
    outcomes = [1, 0]

    assert brier_score(forecasts, outcomes) == pytest.approx(0.065)
    assert log_loss(forecasts, outcomes) == pytest.approx(
        -(math.log(0.8) + math.log(0.7)) / 2
    )


def test_brier_rejects_out_of_range_probabilities() -> None:
    with pytest.raises(ValueError, match="between 0 and 1"):
        brier_score([Decimal("1.01")], [1])
    with pytest.raises(ValueError, match="between 0 and 1"):
        brier_score([Decimal("-0.01")], [0])


def test_calibration_binning_is_correct() -> None:
    observations = [
        forecast("a", "0.10", "0.10", 0),
        forecast("b", "0.20", "0.20", 1),
        forecast("c", "0.80", "0.80", 1),
        forecast("d", "0.90", "0.90", 1),
    ]

    summary = reliability_curve(observations, bin_count=2)

    assert summary.bins[0].count == 2
    assert summary.bins[0].mean_forecast == pytest.approx(0.15)
    assert summary.bins[0].empirical_frequency == pytest.approx(0.5)
    assert summary.bins[1].count == 2
    assert summary.bins[1].mean_forecast == pytest.approx(0.85)
    assert summary.bins[1].empirical_frequency == pytest.approx(1.0)
    assert summary.ece == pytest.approx(0.25)


def test_market_baseline_skill_zero_when_model_equals_market_and_positive_when_better() -> None:
    same_as_market = [
        forecast("a", "0.60", "0.60", 1),
        forecast("b", "0.40", "0.40", 0),
    ]
    better_than_market = [
        forecast("a", "0.80", "0.60", 1),
        forecast("b", "0.20", "0.40", 0),
    ]

    assert score_forecasts(same_as_market).brier_skill == pytest.approx(0.0)
    assert score_forecasts(better_than_market).brier_skill > 0


def test_scoring_uses_market_mid_not_executable_price_for_baseline() -> None:
    observations = [
        forecast("a", "0.50", "0.50", 1, executable_price="0.60"),
        forecast("b", "0.50", "0.50", 0, executable_price="0.60"),
    ]

    summary = score_forecasts(observations)

    assert summary.brier_skill == pytest.approx(0.0)
    ask_baseline_brier = brier_score([Decimal("0.60"), Decimal("0.60")], [1, 0])
    fake_ask_skill = 1 - summary.brier_model / ask_baseline_brier
    assert fake_ask_skill > 0


def test_cluster_bootstrap_uses_distinct_events_not_tick_count() -> None:
    observations = []
    for _ in range(30):
        observations.append(
            forecast("match-a-market", "0.80", "0.60", 1, event_id="match-a")
        )
        observations.append(
            forecast("match-b-market", "0.20", "0.40", 0, event_id="match-b")
        )

    summary = score_forecasts(observations, bootstrap_samples=25)

    assert summary.n == 60
    assert summary.distinct_event_count == 2
    assert summary.small_n is True
    assert summary.brier_skill_ci is not None


def forecast(
    market_id: str,
    model_probability: str,
    market_mid: str,
    realized: int,
    *,
    event_id: str | None = None,
    executable_price: str | None = None,
) -> ForecastObservation:
    return ForecastObservation(
        market_id=market_id,
        ts_local_ns=1,
        model_probability=Decimal(model_probability),
        market_mid=Decimal(market_mid),
        realized=realized,
        event_id=event_id,
        executable_price=Decimal(executable_price) if executable_price is not None else None,
    )
