"""In-memory tracked-pair registry for collector refreshes."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict
from pathlib import Path

from pmbot.signals.contracts import VerifiedContractPair

COLLECTED_PAIRS_RELATIVE_PATH = Path("collector") / "verified_pairs.jsonl"


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


class CollectedPairStore:
    """Persist the collector's effective verified pair set for offline eval."""

    def __init__(
        self,
        *,
        data_dir: Path,
        path: Path | None = None,
    ) -> None:
        self._path = path or data_dir / COLLECTED_PAIRS_RELATIVE_PATH

    @property
    def path(self) -> Path:
        """Return the JSONL path used by this store."""
        return self._path

    def save(
        self,
        pairs: tuple[VerifiedContractPair, ...],
        *,
        merge_existing: bool = False,
    ) -> None:
        """Atomically persist pairs, optionally unioning with the historical set."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        effective_pairs = (*self.load(), *pairs) if merge_existing else pairs
        tmp_path = self._path.with_name(f"{self._path.name}.{time.time_ns()}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            for pair in _dedupe_pairs(effective_pairs):
                handle.write(json.dumps(asdict(pair), sort_keys=True) + "\n")
        tmp_path.replace(self._path)

    def load(self) -> tuple[VerifiedContractPair, ...]:
        """Load persisted pairs, returning an empty tuple when none exist."""
        if not self._path.exists():
            return ()
        pairs: list[VerifiedContractPair] = []
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                continue
            pairs.append(
                VerifiedContractPair(
                    polymarket_condition_id=str(payload["polymarket_condition_id"]),
                    polymarket_asset_id=str(payload["polymarket_asset_id"]),
                    kalshi_ticker=str(payload["kalshi_ticker"]),
                    rationale=str(payload["rationale"]),
                    invert_kalshi=bool(payload.get("invert_kalshi", False)),
                    polymarket_no_asset_id=(
                        str(payload["polymarket_no_asset_id"])
                        if payload.get("polymarket_no_asset_id") is not None
                        else None
                    ),
                )
            )
        return _dedupe_pairs(tuple(pairs))


def _dedupe_pairs(pairs: tuple[VerifiedContractPair, ...]) -> tuple[VerifiedContractPair, ...]:
    by_key: dict[tuple[str, str], VerifiedContractPair] = {}
    for pair in pairs:
        by_key[(pair.polymarket_condition_id, pair.kalshi_ticker)] = pair
    return tuple(by_key[key] for key in sorted(by_key))
