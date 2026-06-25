"""Checksum helpers for venue order-book summaries."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence


def compute_order_book_hash(
    *,
    market: str,
    asset_id: str,
    timestamp: str,
    bids: Sequence[Mapping[str, object]],
    asks: Sequence[Mapping[str, object]],
) -> str:
    """Mirror Polymarket's documented client hash shape for book summaries."""
    summary = {
        "market": market,
        "asset_id": asset_id,
        "timestamp": timestamp,
        "hash": "",
        "bids": [_price_level(level) for level in bids],
        "asks": [_price_level(level) for level in asks],
    }
    payload = json.dumps(summary, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def hashes_match(expected: str, actual: str) -> bool:
    """Compare venue hashes while tolerating optional 0x prefixes."""
    return _normalize_hash(expected) == _normalize_hash(actual)


def _normalize_hash(value: str) -> str:
    normalized = value.lower()
    if normalized.startswith("0x"):
        return normalized[2:]
    return normalized


def _price_level(level: Mapping[str, object]) -> dict[str, str]:
    return {
        "price": str(level["price"]),
        "size": str(level["size"]),
    }
