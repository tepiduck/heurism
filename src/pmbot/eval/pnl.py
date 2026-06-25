"""PnL metrics over trusted backtest mark spans."""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from decimal import Decimal

from pmbot.backtest.engine import BacktestResult
from pmbot.backtest.models import PositionMark
from pmbot.eval.models import BootstrapInterval, PnlMetrics


def pnl_metrics(
    result: BacktestResult,
    *,
    bootstrap_samples: int = 0,
    seed: int = 31,
) -> PnlMetrics:
    """Compute PnL metrics using only contiguous valid mark spans."""
    marks = result.pnl_series
    valid_marks = [mark for mark in marks if mark.pnl_valid]
    invalid_count = len(marks) - len(valid_marks)
    deltas = _valid_deltas(marks)
    total_pnl = sum(deltas, Decimal("0"))
    pnl_ci = (
        _bootstrap_pnl_interval(deltas, samples=bootstrap_samples, seed=seed)
        if bootstrap_samples > 0 and deltas
        else None
    )
    return PnlMetrics(
        total_pnl=total_pnl,
        sharpe=_sharpe(deltas),
        max_drawdown=_max_drawdown(marks),
        hit_rate=_hit_rate(deltas),
        n_valid_marks=len(valid_marks),
        n_invalid_marks=invalid_count,
        pnl_ci=pnl_ci,
    )


def _valid_deltas(marks: Sequence[PositionMark]) -> list[Decimal]:
    deltas: list[Decimal] = []
    previous: PositionMark | None = None
    for mark in marks:
        if not mark.pnl_valid:
            previous = None
            continue
        if previous is not None:
            deltas.append(mark.pnl - previous.pnl)
        previous = mark
    return deltas


def _max_drawdown(marks: Sequence[PositionMark]) -> Decimal:
    max_drawdown = Decimal("0")
    peak: Decimal | None = None
    for mark in marks:
        if not mark.pnl_valid:
            peak = None
            continue
        if peak is None or mark.pnl > peak:
            peak = mark.pnl
        drawdown = peak - mark.pnl
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _hit_rate(deltas: Sequence[Decimal]) -> float | None:
    nonzero = [delta for delta in deltas if delta != Decimal("0")]
    if not nonzero:
        return None
    wins = sum(1 for delta in nonzero if delta > Decimal("0"))
    return wins / len(nonzero)


def _sharpe(deltas: Sequence[Decimal]) -> float | None:
    if len(deltas) < 2:
        return None
    values = [float(delta) for delta in deltas]
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    std_dev = math.sqrt(variance)
    if std_dev == 0:
        return None
    return mean / std_dev * math.sqrt(len(values))


def _bootstrap_pnl_interval(
    deltas: Sequence[Decimal],
    *,
    samples: int,
    seed: int,
) -> BootstrapInterval:
    rng = random.Random(seed)
    totals: list[float] = []
    for _ in range(samples):
        total = sum((deltas[rng.randrange(len(deltas))] for _ in deltas), Decimal("0"))
        totals.append(float(total))
    return BootstrapInterval(lower=_quantile(totals, 0.025), upper=_quantile(totals, 0.975))


def _quantile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight
