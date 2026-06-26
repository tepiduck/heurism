"""Load human-curated verified Polymarket/Kalshi pair allowlists."""

from __future__ import annotations

import csv
import json
import logging
import re
from collections.abc import Mapping
from pathlib import Path

from pmbot.signals.contracts import (
    ManualAllowlistEntry,
    VerifiedContractPair,
    load_manual_allowlist,
)

LOGGER = logging.getLogger(__name__)


def load_verified_pairs(path: Path) -> tuple[VerifiedContractPair, ...]:
    """Load verified pairs from JSON, JSONL, or CSV allowlist files."""
    rows = _load_rows(path)
    pairs: list[VerifiedContractPair] = []
    for row_number, row in rows:
        try:
            entry = load_manual_allowlist((row,))[0]
            pairs.append(_verified_pair(entry))
        except (KeyError, TypeError, ValueError) as exc:
            LOGGER.warning("skipping allowlist row %s: %s; row=%s", row_number, exc, row)
    return tuple(pairs)


def _load_rows(path: Path) -> tuple[tuple[int, Mapping[str, object]], ...]:
    suffix = path.suffix.casefold()
    if suffix == ".jsonl":
        rows: list[tuple[int, Mapping[str, object]]] = []
        for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                rows.append((index, _ensure_mapping(json.loads(line))))
            except (json.JSONDecodeError, TypeError) as exc:
                LOGGER.warning("skipping allowlist row %s: %s; row=%s", index, exc, line)
        return tuple(rows)
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise TypeError("allowlist JSON must contain a list of entries")
        rows = []
        for index, item in enumerate(payload, start=1):
            try:
                rows.append((index, _ensure_mapping(item)))
            except TypeError as exc:
                LOGGER.warning("skipping allowlist row %s: %s; row=%s", index, exc, item)
        return tuple(rows)
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return tuple((index, dict(row)) for index, row in enumerate(csv.DictReader(handle), 2))
    raise ValueError("allowlist must be .jsonl, .json, or .csv")


def _verified_pair(entry: ManualAllowlistEntry) -> VerifiedContractPair:
    same_direction = _norm(entry.polymarket_yes_payout_condition) == _norm(
        entry.kalshi_yes_payout_condition
    )
    if entry.invert and same_direction:
        raise ValueError(f"{entry.kalshi_ticker}: invert=True but YES conditions match")
    if not entry.invert and not same_direction:
        raise ValueError(f"{entry.kalshi_ticker}: opposite YES condition requires invert=True")
    return VerifiedContractPair(
        polymarket_condition_id=entry.polymarket_condition_id,
        polymarket_asset_id=entry.polymarket_asset_id,
        kalshi_ticker=entry.kalshi_ticker,
        rationale=f"runner allowlist: {entry.rationale}; kalshi_inverted={entry.invert}",
        invert_kalshi=entry.invert,
        polymarket_no_asset_id=entry.polymarket_no_asset_id,
    )


def _ensure_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError("allowlist entry must be an object")
    return value


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()
