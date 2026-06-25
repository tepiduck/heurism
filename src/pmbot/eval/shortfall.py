"""Implementation-shortfall and fill-level attribution."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from pmbot.backtest.models import OrderSide
from pmbot.eval.models import FillAttribution, ShortfallSummary


def implementation_shortfall(rows: Sequence[FillAttribution]) -> ShortfallSummary:
    """Decompose realized fill PnL into alpha, execution, and residual components."""
    total_pnl = Decimal("0")
    alpha_pnl = Decimal("0")
    execution_pnl = Decimal("0")
    for row in rows:
        signed_size = _signed_size(row)
        fill = row.fill
        total_pnl += signed_size * (row.realized_value - fill.price) - fill.total_cost
        alpha_pnl += signed_size * (row.fair_value - row.arrival_mid)
        execution_pnl += signed_size * (row.arrival_mid - fill.price)
    residual_pnl = total_pnl - alpha_pnl - execution_pnl
    return ShortfallSummary(
        total_pnl=total_pnl,
        alpha_pnl=alpha_pnl,
        execution_pnl=execution_pnl,
        execution_cost=-execution_pnl,
        residual_pnl=residual_pnl,
        n_fills=len(rows),
    )


def _signed_size(row: FillAttribution) -> Decimal:
    if row.fill.side == OrderSide.BUY:
        return row.fill.size
    return -row.fill.size
