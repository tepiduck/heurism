"""Cross-venue Polymarket/Kalshi deviation signal."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from pmbot.backtest.engine import PointInTimeView
from pmbot.features.orderbook import microprice
from pmbot.ingestion.schemas import BookEvent
from pmbot.signals.contracts import VerifiedContractPair


class SignalDirection(StrEnum):
    """Signal direction for the Polymarket leg."""

    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass(frozen=True, slots=True)
class CrossVenueSignalConfig:
    """Cross-venue signal parameters."""

    kalshi_weight: Decimal = Decimal("1")
    double_counting_overlap: Decimal = Decimal("0.50")
    trade_threshold: Decimal = Decimal("0.01")
    max_reference_age_ns: int = 2_000_000_000
    require_kalshi_leads: bool = True


@dataclass(frozen=True, slots=True)
class SignalValue:
    """One cross-venue signal observation."""

    polymarket_condition_id: str
    polymarket_asset_id: str
    kalshi_ticker: str
    ts_local_ns: int
    polymarket_probability: Decimal
    kalshi_probability: Decimal
    fair_probability: Decimal
    signal_value: Decimal
    direction: SignalDirection
    confidence: Decimal
    rationale: str


class CrossVenueSignal:
    """Compute Polymarket/Kalshi deviation from a point-in-time view only."""

    def __init__(
        self,
        pairs: tuple[VerifiedContractPair, ...],
        config: CrossVenueSignalConfig | None = None,
    ) -> None:
        self._pairs = pairs
        self._config = config or CrossVenueSignalConfig()

    def compute(self, view: PointInTimeView) -> tuple[SignalValue, ...]:
        """Compute signals using only data exposed by the point-in-time view."""
        signals: list[SignalValue] = []
        for pair in self._pairs:
            signal = self._compute_pair(view, pair)
            if signal is not None:
                signals.append(signal)
        return tuple(signals)

    def _compute_pair(
        self,
        view: PointInTimeView,
        pair: VerifiedContractPair,
    ) -> SignalValue | None:
        pm_book = view.book(pair.polymarket_asset_id)
        kalshi_book = view.book(pair.kalshi_ticker)
        pm_probability = microprice(pm_book)
        kalshi_probability = microprice(kalshi_book)
        if pm_probability is None or kalshi_probability is None:
            return None
        if pair.invert_kalshi:
            kalshi_probability = Decimal("1") - kalshi_probability

        pm_ts = _latest_book_ts(view, pair.polymarket_asset_id)
        kalshi_ts = _latest_book_ts(view, pair.kalshi_ticker)
        if pm_ts is None or kalshi_ts is None:
            return None
        reference_age = view.ts_local_ns - kalshi_ts
        if reference_age > self._config.max_reference_age_ns:
            return None
        if self._config.require_kalshi_leads and kalshi_ts < pm_ts:
            return None

        fair_probability = guarded_log_odds_pool(
            polymarket_probability=pm_probability,
            kalshi_probability=kalshi_probability,
            kalshi_weight=self._config.kalshi_weight,
            double_counting_overlap=self._config.double_counting_overlap,
        )
        signal_value = fair_probability - pm_probability
        direction = _direction(signal_value, self._config.trade_threshold)
        confidence = _confidence(signal_value, reference_age, self._config.max_reference_age_ns)
        return SignalValue(
            polymarket_condition_id=pair.polymarket_condition_id,
            polymarket_asset_id=pair.polymarket_asset_id,
            kalshi_ticker=pair.kalshi_ticker,
            ts_local_ns=view.ts_local_ns,
            polymarket_probability=pm_probability,
            kalshi_probability=kalshi_probability,
            fair_probability=fair_probability,
            signal_value=signal_value,
            direction=direction,
            confidence=confidence,
            rationale=(
                "log-odds update from Kalshi reference with "
                f"{self._config.double_counting_overlap} overlap removed"
                + ("; Kalshi probability inverted" if pair.invert_kalshi else "")
            ),
        )


def guarded_log_odds_pool(
    *,
    polymarket_probability: Decimal,
    kalshi_probability: Decimal,
    kalshi_weight: Decimal,
    double_counting_overlap: Decimal,
) -> Decimal:
    """Pool on log-odds while removing overlapping Kalshi information."""
    orthogonal_weight = kalshi_weight * (Decimal("1") - double_counting_overlap)
    pm_logit = _logit(polymarket_probability)
    kalshi_logit = _logit(kalshi_probability)
    return _inv_logit(pm_logit + orthogonal_weight * (kalshi_logit - pm_logit))


def _latest_book_ts(view: PointInTimeView, asset_id: str) -> int | None:
    timestamps = [
        event.ts_local_ns
        for event in view.events()
        if isinstance(event, BookEvent) and event.asset_id == asset_id
    ]
    if not timestamps:
        return None
    return max(timestamps)


def _direction(signal_value: Decimal, threshold: Decimal) -> SignalDirection:
    if signal_value > threshold:
        return SignalDirection.BUY
    if signal_value < -threshold:
        return SignalDirection.SELL
    return SignalDirection.HOLD


def _confidence(signal_value: Decimal, reference_age: int, max_reference_age_ns: int) -> Decimal:
    freshness = Decimal("1")
    if max_reference_age_ns > 0:
        freshness = max(
            Decimal("0"),
            Decimal("1") - Decimal(reference_age) / Decimal(max_reference_age_ns),
        )
    return min(Decimal("1"), abs(signal_value) * Decimal("4") * freshness)


def _logit(probability: Decimal) -> Decimal:
    clamped = min(Decimal("0.999999"), max(Decimal("0.000001"), probability))
    return (clamped / (Decimal("1") - clamped)).ln()


def _inv_logit(log_odds: Decimal) -> Decimal:
    return Decimal("1") / (Decimal("1") + (-log_odds).exp())
