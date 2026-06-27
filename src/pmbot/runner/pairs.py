"""In-memory tracked-pair registry for collector refreshes."""

from __future__ import annotations

import asyncio

from pmbot.signals.contracts import VerifiedContractPair


class PairRegistry:
    """Track the live verified-pair set and notify tasks when it changes."""

    def __init__(self, pairs: tuple[VerifiedContractPair, ...] = ()) -> None:
        self._pairs = _dedupe_pairs(pairs)
        self._version = 0
        self._changed = asyncio.Event()

    @property
    def version(self) -> int:
        """Return the current registry version."""
        return self._version

    def snapshot(self) -> tuple[VerifiedContractPair, ...]:
        """Return the current pair set."""
        return self._pairs

    def snapshot_with_version(self) -> tuple[tuple[VerifiedContractPair, ...], int]:
        """Return the current pair set and version atomically."""
        return self._pairs, self._version

    def replace(self, pairs: tuple[VerifiedContractPair, ...]) -> bool:
        """Replace pairs and return whether the effective set changed."""
        next_pairs = _dedupe_pairs(pairs)
        if next_pairs == self._pairs:
            return False
        self._pairs = next_pairs
        self._version += 1
        self._changed.set()
        self._changed = asyncio.Event()
        return True

    async def wait_for_change(self, version: int) -> None:
        """Block until the registry has advanced past ``version``."""
        if version != self._version:
            return
        await self._changed.wait()


def merge_tracked_pairs(
    existing: tuple[VerifiedContractPair, ...],
    discovered: tuple[VerifiedContractPair, ...],
    *,
    resolved_market_ids: set[str] | None = None,
) -> tuple[VerifiedContractPair, ...]:
    """Merge discovered pairs into existing pairs while dropping resolved markets."""
    resolved = resolved_market_ids or set()
    retained = tuple(pair for pair in existing if not pair_matches_market_ids(pair, resolved))
    return _dedupe_pairs((*retained, *discovered))


def pair_matches_market_ids(pair: VerifiedContractPair, market_ids: set[str]) -> bool:
    """Return whether a pair maps to any market identifier in ``market_ids``."""
    return pair.polymarket_condition_id in market_ids or pair.kalshi_ticker in market_ids


def _dedupe_pairs(pairs: tuple[VerifiedContractPair, ...]) -> tuple[VerifiedContractPair, ...]:
    by_key: dict[tuple[str, str], VerifiedContractPair] = {}
    for pair in pairs:
        by_key[(pair.polymarket_condition_id, pair.kalshi_ticker)] = pair
    return tuple(by_key[key] for key in sorted(by_key))
