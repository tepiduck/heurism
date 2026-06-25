"""Cross-venue CLV diagnostics."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from pmbot.eval.models import ClvObservation, ClvSummary


def cross_venue_clv(observations: Sequence[ClvObservation]) -> ClvSummary:
    """Return mean entry-to-later-Kalshi CLV as a noisy diagnostic."""
    if not observations:
        return ClvSummary(n=0, mean_clv=Decimal("0"))
    values = [_signed_clv(observation) for observation in observations]
    return ClvSummary(
        n=len(observations),
        mean_clv=sum(values, Decimal("0")) / Decimal(len(values)),
    )


def _signed_clv(observation: ClvObservation) -> Decimal:
    if observation.side == "buy":
        return observation.later_kalshi_probability - observation.entry_price
    if observation.side == "sell":
        return observation.entry_price - observation.later_kalshi_probability
    raise ValueError(f"unsupported CLV side: {observation.side}")
