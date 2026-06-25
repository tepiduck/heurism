"""Calibration diagnostics for binary probability forecasts."""

from __future__ import annotations

import math
import random
from collections.abc import Sequence

from pmbot.eval.models import (
    BootstrapInterval,
    CalibrationBin,
    CalibrationSummary,
    ForecastObservation,
)

SMALL_N_THRESHOLD = 30


def reliability_curve(
    observations: Sequence[ForecastObservation],
    *,
    bin_count: int = 10,
    bootstrap_samples: int = 0,
    seed: int = 23,
) -> CalibrationSummary:
    """Return reliability bins and Expected Calibration Error."""
    if not observations:
        raise ValueError("at least one forecast observation is required")
    if bin_count <= 0:
        raise ValueError("bin_count must be positive")
    base_bins = _bins_for_observations(observations, bin_count=bin_count)
    bands = (
        _bootstrap_bands(
            observations,
            bin_count=bin_count,
            samples=bootstrap_samples,
            seed=seed,
        )
        if bootstrap_samples > 0
        else [None] * bin_count
    )
    bins = tuple(
        CalibrationBin(
            lower=base.lower,
            upper=base.upper,
            count=base.count,
            mean_forecast=base.mean_forecast,
            empirical_frequency=base.empirical_frequency,
            confidence_band=bands[index],
        )
        for index, base in enumerate(base_bins)
    )
    return CalibrationSummary(
        n=len(observations),
        bins=bins,
        ece=expected_calibration_error(bins, n=len(observations)),
        small_n=len(observations) < SMALL_N_THRESHOLD,
    )


def expected_calibration_error(bins: Sequence[CalibrationBin], *, n: int) -> float:
    """Return ECE from populated reliability bins."""
    if n <= 0:
        raise ValueError("n must be positive")
    total = 0.0
    for item in bins:
        if item.mean_forecast is None or item.empirical_frequency is None:
            continue
        total += item.count / n * abs(item.empirical_frequency - item.mean_forecast)
    return total


def _bins_for_observations(
    observations: Sequence[ForecastObservation],
    *,
    bin_count: int,
) -> tuple[CalibrationBin, ...]:
    grouped: list[list[ForecastObservation]] = [[] for _ in range(bin_count)]
    for observation in observations:
        grouped[_bin_index(float(observation.model_probability), bin_count)].append(observation)

    bins: list[CalibrationBin] = []
    for index, items in enumerate(grouped):
        lower = index / bin_count
        upper = (index + 1) / bin_count
        if not items:
            bins.append(
                CalibrationBin(
                    lower=lower,
                    upper=upper,
                    count=0,
                    mean_forecast=None,
                    empirical_frequency=None,
                )
            )
            continue
        mean_forecast = sum(float(item.model_probability) for item in items) / len(items)
        empirical_frequency = sum(item.realized for item in items) / len(items)
        bins.append(
            CalibrationBin(
                lower=lower,
                upper=upper,
                count=len(items),
                mean_forecast=mean_forecast,
                empirical_frequency=empirical_frequency,
            )
        )
    return tuple(bins)


def _bootstrap_bands(
    observations: Sequence[ForecastObservation],
    *,
    bin_count: int,
    samples: int,
    seed: int,
) -> list[BootstrapInterval | None]:
    rng = random.Random(seed)
    by_bin: list[list[float]] = [[] for _ in range(bin_count)]
    for _ in range(samples):
        resample = [observations[rng.randrange(len(observations))] for _ in observations]
        for index, item in enumerate(_bins_for_observations(resample, bin_count=bin_count)):
            if item.empirical_frequency is not None:
                by_bin[index].append(item.empirical_frequency)
    return [
        BootstrapInterval(lower=_quantile(values, 0.025), upper=_quantile(values, 0.975))
        if values
        else None
        for values in by_bin
    ]


def _bin_index(probability: float, bin_count: int) -> int:
    clamped = min(1.0, max(0.0, probability))
    if clamped == 1.0:
        return bin_count - 1
    return int(clamped * bin_count)


def _quantile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight
