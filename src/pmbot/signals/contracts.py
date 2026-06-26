"""Conservative Polymarket/Kalshi contract mapping."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ContractMetadata:
    """Resolution metadata used to compare cross-venue contracts."""

    venue: str
    identifier: str
    title: str
    rules: str
    close_time: str
    yes_payout_condition: str
    outcome: str = "yes"


@dataclass(frozen=True, slots=True)
class ManualAllowlistEntry:
    """Manual verified pair override."""

    polymarket_condition_id: str
    polymarket_asset_id: str
    kalshi_ticker: str
    polymarket_yes_payout_condition: str
    kalshi_yes_payout_condition: str
    invert: bool
    rationale: str
    polymarket_no_asset_id: str | None = None


@dataclass(frozen=True, slots=True)
class VerifiedContractPair:
    """A pair whose resolution criteria are verified identical."""

    polymarket_condition_id: str
    polymarket_asset_id: str
    kalshi_ticker: str
    rationale: str
    invert_kalshi: bool = False
    polymarket_no_asset_id: str | None = None


@dataclass(frozen=True, slots=True)
class RejectedContractPair:
    """A pair excluded from signal generation."""

    polymarket_condition_id: str
    kalshi_ticker: str
    reason: str


class ContractMapper:
    """Map contracts only when criteria match exactly or are manually allowed."""

    def __init__(self, allowlist: Iterable[ManualAllowlistEntry] = ()) -> None:
        self._allowlist = {
            (entry.polymarket_condition_id, entry.kalshi_ticker): entry for entry in allowlist
        }

    def verify_pair(
        self,
        *,
        polymarket: ContractMetadata,
        kalshi: ContractMetadata,
        polymarket_asset_id: str,
    ) -> VerifiedContractPair | RejectedContractPair:
        """Verify a candidate pair or reject it with a rationale."""
        manual = self._allowlist.get((polymarket.identifier, kalshi.identifier))
        if manual is not None:
            manual_polarity = _validate_manual_polarity(manual)
            if manual_polarity is not None:
                return RejectedContractPair(
                    polymarket_condition_id=manual.polymarket_condition_id,
                    kalshi_ticker=manual.kalshi_ticker,
                    reason=manual_polarity,
                )
            return VerifiedContractPair(
                polymarket_condition_id=manual.polymarket_condition_id,
                polymarket_asset_id=manual.polymarket_asset_id,
                kalshi_ticker=manual.kalshi_ticker,
                rationale=(
                    f"manual allowlist: {manual.rationale}; "
                    f"kalshi_inverted={manual.invert}"
                ),
                invert_kalshi=manual.invert,
                polymarket_no_asset_id=manual.polymarket_no_asset_id,
            )

        if _norm(polymarket.title) != _norm(kalshi.title):
            return RejectedContractPair(
                polymarket_condition_id=polymarket.identifier,
                kalshi_ticker=kalshi.identifier,
                reason="title/question mismatch",
            )
        if _norm(polymarket.rules) != _norm(kalshi.rules):
            return RejectedContractPair(
                polymarket_condition_id=polymarket.identifier,
                kalshi_ticker=kalshi.identifier,
                reason="resolution rules mismatch",
            )
        if polymarket.close_time != kalshi.close_time:
            return RejectedContractPair(
                polymarket_condition_id=polymarket.identifier,
                kalshi_ticker=kalshi.identifier,
                reason="expiration/close time mismatch",
            )
        if _norm(polymarket.outcome) != _norm(kalshi.outcome):
            return RejectedContractPair(
                polymarket_condition_id=polymarket.identifier,
                kalshi_ticker=kalshi.identifier,
                reason="outcome side mismatch",
            )
        if _norm(polymarket.yes_payout_condition) != _norm(kalshi.yes_payout_condition):
            return RejectedContractPair(
                polymarket_condition_id=polymarket.identifier,
                kalshi_ticker=kalshi.identifier,
                reason="yes payout condition mismatch",
            )
        return VerifiedContractPair(
            polymarket_condition_id=polymarket.identifier,
            polymarket_asset_id=polymarket_asset_id,
            kalshi_ticker=kalshi.identifier,
            rationale="exact title, rules, close time, outcome, and yes payout condition match",
        )


def save_verified_pairs(path: Path, pairs: Iterable[VerifiedContractPair]) -> None:
    """Persist verified pairs as JSON Lines."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for pair in pairs:
            handle.write(json.dumps(asdict(pair), sort_keys=True) + "\n")


def load_manual_allowlist(rows: Iterable[Mapping[str, object]]) -> tuple[ManualAllowlistEntry, ...]:
    """Load manual allowlist entries from dict-like rows."""
    return tuple(
        ManualAllowlistEntry(
            polymarket_condition_id=_required_row_str(row, "polymarket_condition_id"),
            polymarket_asset_id=_required_row_str(row, "polymarket_asset_id"),
            kalshi_ticker=_required_row_str(row, "kalshi_ticker"),
            polymarket_yes_payout_condition=_required_row_str(
                row,
                "polymarket_yes_payout_condition",
            ),
            kalshi_yes_payout_condition=_required_row_str(row, "kalshi_yes_payout_condition"),
            invert=_required_row_bool(row, "invert"),
            rationale=_required_row_str(row, "rationale"),
            polymarket_no_asset_id=_optional_row_str(row, "polymarket_no_asset_id"),
        )
        for row in rows
    )


def _validate_manual_polarity(entry: ManualAllowlistEntry) -> str | None:
    same_direction = _norm(entry.polymarket_yes_payout_condition) == _norm(
        entry.kalshi_yes_payout_condition
    )
    if entry.invert and same_direction:
        return "manual invert=True but YES payout conditions match"
    if not entry.invert and not same_direction:
        return "manual YES payout condition mismatch without invert=True"
    return None


def _required_row_str(row: Mapping[str, object], key: str) -> str:
    value = row[key]
    if isinstance(value, str):
        return value
    raise TypeError(f"{key} must be a string")


def _required_row_bool(row: Mapping[str, object], key: str) -> bool:
    value = row[key]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.casefold()
        if normalized in {"1", "true", "yes"}:
            return True
        if normalized in {"0", "false", "no"}:
            return False
    raise TypeError(f"{key} must be a bool")


def _optional_row_str(row: Mapping[str, object], key: str) -> str | None:
    value = row.get(key)
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    raise TypeError(f"{key} must be a string when supplied")


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()
