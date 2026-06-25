"""Shared fill cost model for backtests and future execution adapters."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class LiquidityFlag(StrEnum):
    """Whether a fill removed or provided liquidity."""

    MAKER = "maker"
    TAKER = "taker"


@dataclass(frozen=True, slots=True)
class CostConfig:
    """Configurable fees and gas charged per fill."""

    maker_fee_rate: Decimal = Decimal("0")
    taker_fee_rate: Decimal = Decimal("0")
    gas_per_fill: Decimal = Decimal("0")


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

    def calculate(
        self,
        *,
        price: Decimal,
        size: Decimal,
        liquidity: LiquidityFlag,
    ) -> CostBreakdown:
        """Calculate fill costs from Decimal price and size."""
        notional = price * size
        fee_rate = (
            self._config.maker_fee_rate
            if liquidity == LiquidityFlag.MAKER
            else self._config.taker_fee_rate
        )
        fee = notional * fee_rate
        gas = self._config.gas_per_fill
        return CostBreakdown(notional=notional, fee=fee, gas=gas, total=fee + gas)
