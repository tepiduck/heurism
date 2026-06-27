"""Gamma metadata client and normalization."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import httpx

from pmbot.ingestion.parsers import parse_decimal
from pmbot.ingestion.schemas import MetadataSnapshot

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"


class GammaClient:
    """Small public Gamma API client for market metadata."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_GAMMA_URL,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http

    async def fetch_markets(
        self,
        *,
        active: bool | None = None,
        closed: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Mapping[str, object]]:
        """Fetch one page of Gamma markets."""
        params: dict[str, str | int | bool] = {"limit": limit, "offset": offset}
        if active is not None:
            params["active"] = active
        if closed is not None:
            params["closed"] = closed

        async with self._client() as client:
            response = await client.get(f"{self._base_url}/markets", params=params)
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=Decimal)
        if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes, bytearray)):
            raise TypeError("Gamma /markets response must be a list")
        return [cast(Mapping[str, object], item) for item in payload if isinstance(item, Mapping)]

    async def fetch_events(
        self,
        *,
        series_slug: str | None = None,
        active: bool | None = None,
        closed: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Mapping[str, object]]:
        """Fetch one page of Gamma events."""
        params: dict[str, str | int | bool] = {"limit": limit, "offset": offset}
        if series_slug is not None:
            params["series_slug"] = series_slug
        if active is not None:
            params["active"] = active
        if closed is not None:
            params["closed"] = closed

        async with self._client() as client:
            response = await client.get(f"{self._base_url}/events", params=params)
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=Decimal)
        if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes, bytearray)):
            raise TypeError("Gamma /events response must be a list")
        return [cast(Mapping[str, object], item) for item in payload if isinstance(item, Mapping)]

    async def fetch_all_active_markets(self, *, page_size: int = 100) -> list[MetadataSnapshot]:
        """Fetch all active, open Gamma markets with order-book metadata."""
        snapshots: list[MetadataSnapshot] = []
        offset = 0
        fetched_at = datetime.now(UTC).isoformat()
        while True:
            try:
                page = await self.fetch_markets(
                    active=True,
                    closed=False,
                    limit=page_size,
                    offset=offset,
                )
            except httpx.HTTPStatusError as exc:
                if offset > 0 and exc.response.status_code == 422:
                    break
                raise
            if not page:
                break
            for raw_market in page:
                snapshot = normalize_market(raw_market, fetched_at=fetched_at)
                if snapshot.token_ids:
                    snapshots.append(snapshot)
            if len(page) < page_size:
                break
            offset += page_size
        return snapshots

    def _client(self) -> _ClientContext:
        return _ClientContext(self._http)


class _ClientContext:
    def __init__(self, client: httpx.AsyncClient | None) -> None:
        self._client = client
        self._owned: httpx.AsyncClient | None = None

    async def __aenter__(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        self._owned = httpx.AsyncClient(timeout=20.0)
        return self._owned

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._owned is not None:
            await self._owned.aclose()


def normalize_market(raw_market: Mapping[str, object], *, fetched_at: str) -> MetadataSnapshot:
    """Normalize a Gamma market into the metadata snapshot schema."""
    condition_id = _first_present_str(raw_market, "conditionId", "condition_id", "market")
    return MetadataSnapshot(
        fetched_at=fetched_at,
        condition_id=condition_id,
        question=_optional_str(raw_market.get("question")),
        outcomes=tuple(_json_string_list(raw_market.get("outcomes"))),
        token_ids=tuple(_json_string_list(raw_market.get("clobTokenIds"))),
        resolution_or_end_time=_first_optional_str(
            raw_market,
            "resolutionTime",
            "endDateIso",
            "endDate",
            "umaEndDateIso",
            "umaEndDate",
        ),
        active=_optional_bool(raw_market.get("active")),
        closed=_optional_bool(raw_market.get("closed")),
        liquidity=_optional_decimal(
            raw_market.get("liquidityNum", raw_market.get("liquidity"))
        ),
        volume=_optional_decimal(raw_market.get("volumeNum", raw_market.get("volume"))),
        raw_json=json.dumps(raw_market, default=str, separators=(",", ":"), sort_keys=True),
    )


def _json_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        decoded = json.loads(value)
        if not isinstance(decoded, list):
            return []
        return [str(item) for item in decoded]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value]
    return []


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return parse_decimal(value)


def _first_present_str(raw: Mapping[str, object], *keys: str) -> str:
    value = _first_optional_str(raw, *keys)
    if value is None:
        raise KeyError(keys[0])
    return value


def _first_optional_str(raw: Mapping[str, object], *keys: str) -> str | None:
    for key in keys:
        value = _optional_str(raw.get(key))
        if value is not None:
            return value
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, int | Decimal):
        return str(value)
    raise TypeError(f"expected string-compatible value, got {type(value).__name__}")


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    raise TypeError(f"expected bool-compatible value, got {type(value).__name__}")
