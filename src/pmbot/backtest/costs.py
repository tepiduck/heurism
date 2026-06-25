"""Shared fill cost model for backtests and future execution adapters.

Polymarket International docs currently define taker fees as:
``fee = shares * fee_rate * price * (1 - price)``. The fee is symmetric
around 50% probability, rounded to 5 decimal places, and category dependent.

Polymarket US docs currently define the regulated exchange fee schedule as:
``fee = theta * contracts * price * (1 - price)`` with taker theta ``0.05``
and maker rebate theta ``-0.0125``. US fees/rebates are rounded to cents using
banker's rounding.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from enum import StrEnum


class LiquidityFlag(StrEnum):
    """Whether a fill removed or provided liquidity."""

    MAKER = "maker"
    TAKER = "taker"


class FeeVenue(StrEnum):
    """Supported venue fee schedules."""

    POLYMARKET_INTERNATIONAL = "polymarket_international"
    POLYMARKET_US = "polymarket_us"
    ZERO = "zero"


class FeeCategory(StrEnum):
    """Polymarket International fee categories."""

    CRYPTO = "crypto"
    SPORTS = "sports"
    FINANCE = "finance"
    POLITICS = "politics"
    ECONOMICS = "economics"
    CULTURE = "culture"
    WEATHER = "weather"
    OTHER = "other"
    MENTIONS = "mentions"
    TECH = "tech"
    GEOPOLITICS = "geopolitics"


INTERNATIONAL_TAKER_FEE_RATES: dict[FeeCategory, Decimal] = {
    FeeCategory.CRYPTO: Decimal("0.07"),
    FeeCategory.SPORTS: Decimal("0.03"),
    FeeCategory.FINANCE: Decimal("0.04"),
    FeeCategory.POLITICS: Decimal("0.04"),
    FeeCategory.ECONOMICS: Decimal("0.05"),
    FeeCategory.CULTURE: Decimal("0.05"),
    FeeCategory.WEATHER: Decimal("0.05"),
    FeeCategory.OTHER: Decimal("0.05"),
    FeeCategory.MENTIONS: Decimal("0.04"),
    FeeCategory.TECH: Decimal("0.04"),
    FeeCategory.GEOPOLITICS: Decimal("0"),
}

INTERNATIONAL_MAKER_REBATE_RATES: dict[FeeCategory, Decimal] = {
    FeeCategory.CRYPTO: Decimal("0.20"),
    FeeCategory.SPORTS: Decimal("0.25"),
    FeeCategory.FINANCE: Decimal("0.25"),
    FeeCategory.POLITICS: Decimal("0.25"),
    FeeCategory.ECONOMICS: Decimal("0.25"),
    FeeCategory.CULTURE: Decimal("0.25"),
    FeeCategory.WEATHER: Decimal("0.25"),
    FeeCategory.OTHER: Decimal("0.25"),
    FeeCategory.MENTIONS: Decimal("0.25"),
    FeeCategory.TECH: Decimal("0.25"),
    FeeCategory.GEOPOLITICS: Decimal("0"),
}

US_TAKER_THETA = Decimal("0.05")
US_MAKER_REBATE_THETA = Decimal("-0.0125")
INTERNATIONAL_FEE_QUANTUM = Decimal("0.00001")
US_FEE_QUANTUM = Decimal("0.01")


@dataclass(frozen=True, slots=True)
class CostConfig:
    """Configurable venue/category fees and per-fill gas."""

    venue: FeeVenue = FeeVenue.POLYMARKET_INTERNATIONAL
    category: FeeCategory = FeeCategory.OTHER
    gas_per_fill: Decimal = Decimal("0")

    @classmethod
    def zero(cls) -> CostConfig:
        """Return an intentionally cost-free config for narrow tests only."""
        return cls(venue=FeeVenue.ZERO, category=FeeCategory.GEOPOLITICS)


@dataclass(frozen=True, slots=True)
class CostBreakdown:
    """Cost components for one fill."""

    notional: Decimal
    fee: Decimal
    gas: Decimal
    total: Decimal


class CostModel:
    """Single source of truth for maker/taker fees and gas."""

    def __init__(self, config: CostConfig | None = None) -> None:
        self._config = config or CostConfig()

    @property
    def config(self) -> CostConfig:
        """Return immutable cost configuration."""
        return self._config

    def is_zero_cost_schedule(self) -> bool:
        """Return whether the configured schedule can produce no fees or gas."""
        taker = self.calculate(
            price=Decimal("0.50"),
            size=Decimal("100"),
            liquidity=LiquidityFlag.TAKER,
        )
        maker = self.calculate(
            price=Decimal("0.50"),
            size=Decimal("100"),
            liquidity=LiquidityFlag.MAKER,
        )
        return taker.total == Decimal("0") and maker.total == Decimal("0")

    def calculate(
        self,
        *,
        price: Decimal,
        size: Decimal,
        liquidity: LiquidityFlag,
    ) -> CostBreakdown:
        """Calculate fill costs from Decimal price and size."""
        notional = price * size
        fee = self._fee(price=price, size=size, liquidity=liquidity)
        gas = self._config.gas_per_fill
        return CostBreakdown(notional=notional, fee=fee, gas=gas, total=fee + gas)

    def _fee(self, *, price: Decimal, size: Decimal, liquidity: LiquidityFlag) -> Decimal:
        if self._config.venue == FeeVenue.ZERO:
            return Decimal("0")
        if self._config.venue == FeeVenue.POLYMARKET_INTERNATIONAL:
            return self._international_fee(price=price, size=size, liquidity=liquidity)
        if self._config.venue == FeeVenue.POLYMARKET_US:
            return self._us_fee(price=price, size=size, liquidity=liquidity)
        raise ValueError(f"unsupported fee venue: {self._config.venue}")

    def _international_fee(
        self,
        *,
        price: Decimal,
        size: Decimal,
        liquidity: LiquidityFlag,
    ) -> Decimal:
        taker_fee = (
            size
            * INTERNATIONAL_TAKER_FEE_RATES[self._config.category]
            * price
            * (Decimal("1") - price)
        )
        if liquidity == LiquidityFlag.MAKER:
            taker_fee *= -INTERNATIONAL_MAKER_REBATE_RATES[self._config.category]
        return _round_fee(taker_fee, INTERNATIONAL_FEE_QUANTUM)

    def _us_fee(
        self,
        *,
        price: Decimal,
        size: Decimal,
        liquidity: LiquidityFlag,
    ) -> Decimal:
        theta = US_TAKER_THETA if liquidity == LiquidityFlag.TAKER else US_MAKER_REBATE_THETA
        return _round_fee(
            size * theta * price * (Decimal("1") - price),
            US_FEE_QUANTUM,
        )


def _round_fee(value: Decimal, quantum: Decimal) -> Decimal:
    rounded = value.quantize(quantum, rounding=ROUND_HALF_EVEN)
    return Decimal("0") if rounded == Decimal("-0") else rounded
