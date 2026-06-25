"""Load human-curated verified Polymarket/Kalshi pair allowlists."""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping
from pathlib import Path

from pmbot.signals.contracts import (
    ManualAllowlistEntry,
    VerifiedContractPair,
    load_manual_allowlist,
)


def load_verified_pairs(path: Path) -> tuple[VerifiedContractPair, ...]:
    """Load verified pairs from JSON, JSONL, or CSV allowlist files."""
    rows = _load_rows(path)
    entries = load_manual_allowlist(rows)
    return tuple(_verified_pair(entry) for entry in entries)


def _load_rows(path: Path) -> tuple[Mapping[str, object], ...]:
    suffix = path.suffix.casefold()
    if suffix == ".jsonl":
        return tuple(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise TypeError("allowlist JSON must contain a list of entries")
        return tuple(_ensure_mapping(item) for item in payload)
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return tuple(dict(row) for row in csv.DictReader(handle))
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
    )


def _ensure_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError("allowlist entry must be an object")
    return value


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()
