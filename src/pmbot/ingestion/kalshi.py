"""Kalshi market-data normalization into the shared WP-1 schema.

Kalshi order books expose YES bids and NO bids only. For a YES-probability
asset, NO bids imply YES asks at ``1 - no_bid`` with the same size.
"""

from __future__ import annotations

import json
import ssl
import time
from base64 import b64encode
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

import certifi
import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from pmbot.ingestion.parsers import parse_decimal
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import (
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    GapReason,
    MetadataSnapshot,
    TradeEvent,
)

KALSHI_REST_URL = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"


@dataclass(frozen=True, slots=True)
class KalshiAuth:
    """Signed Kalshi request authentication material."""

    key_id: str
    private_key: rsa.RSAPrivateKey

    @classmethod
    def from_private_key_file(cls, *, key_id: str, private_key_path: Path) -> KalshiAuth:
        return cls(
            key_id=key_id,
            private_key=load_kalshi_private_key(private_key_path),
        )

    def headers_for_request(
        self,
        *,
        method: str,
        path: str,
        timestamp_ms: int | None = None,
    ) -> dict[str, str]:
        return kalshi_auth_headers(
            key_id=self.key_id,
            private_key=self.private_key,
            method=method,
            path=path,
            timestamp_ms=timestamp_ms,
        )


class KalshiMetadataClient:
    """Small Kalshi REST client for market metadata snapshots."""

    def __init__(
        self,
        *,
        base_url: str = KALSHI_REST_URL,
        http: httpx.AsyncClient | None = None,
        auth: KalshiAuth | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http
        self._auth = auth

    async def fetch_markets(
        self,
        *,
        status: str | None = None,
        series_ticker: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[list[Mapping[str, object]], str | None]:
        """Fetch one page from Kalshi's market list endpoint."""
        params: dict[str, str | int] = {"limit": limit}
        if status is not None:
            params["status"] = status
        if series_ticker is not None:
            params["series_ticker"] = series_ticker
        if cursor is not None:
            params["cursor"] = cursor

        url = f"{self._base_url}/markets"
        async with self._client() as client:
            response = await client.get(
                url,
                params=params,
                headers=self._headers_for_url(url),
            )
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=Decimal)
        if not isinstance(payload, Mapping):
            raise TypeError("Kalshi /markets response must be an object")
        raw_markets = payload.get("markets", [])
        if not isinstance(raw_markets, Sequence) or isinstance(raw_markets, (str, bytes)):
            raise TypeError("Kalshi /markets markets field must be a list")
        next_cursor = _optional_str(payload.get("cursor"))
        return (
            [cast(Mapping[str, object], item) for item in raw_markets if isinstance(item, Mapping)],
            next_cursor or None,
        )

    async def fetch_all_open_markets(self, *, page_size: int = 100) -> list[MetadataSnapshot]:
        """Fetch and normalize all open Kalshi markets."""
        snapshots: list[MetadataSnapshot] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.fetch_markets(
                status="open",
                limit=page_size,
                cursor=cursor,
            )
            snapshots.extend(normalize_market_metadata(raw_market) for raw_market in page)
            if not page or cursor is None:
                break
        return snapshots

    async def fetch_market(self, ticker: str) -> MetadataSnapshot:
        """Fetch and normalize one Kalshi market by ticker."""
        url = f"{self._base_url}/markets/{ticker}"
        async with self._client() as client:
            response = await client.get(url, headers=self._headers_for_url(url))
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=Decimal)
        if not isinstance(payload, Mapping):
            raise TypeError("Kalshi /markets/{ticker} response must be an object")
        raw_market = payload.get("market", payload)
        if not isinstance(raw_market, Mapping):
            raise TypeError("Kalshi market payload must be an object")
        return normalize_market_metadata(cast(Mapping[str, object], raw_market))

    def _client(self) -> _ClientContext:
        return _ClientContext(self._http)

    def _headers_for_url(self, url: str) -> dict[str, str]:
        if self._auth is None:
            return {}
        return self._auth.headers_for_request(method="GET", path=kalshi_request_path(url))


class KalshiRecorderAdapter:
    """Record Kalshi normalized market data through the shared recorder."""

    def __init__(
        self,
        recorder: ParquetRecorder,
        normalizer: KalshiNormalizer | None = None,
    ) -> None:
        self._recorder = recorder
        self._normalizer = normalizer or KalshiNormalizer()

    def record_frame(
        self,
        frame: str | bytes,
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> None:
        """Normalize and record one raw websocket frame."""
        self._recorder.record_many(
            self._normalizer.parse_frame(
                frame,
                ts_local_ns=ts_local_ns,
                ts_monotonic_ns=ts_monotonic_ns,
            )
        )

    def record_payload(
        self,
        payload: Mapping[str, object],
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> None:
        """Normalize and record one decoded websocket payload."""
        self._recorder.record_many(
            self._normalizer.parse_payload(
                payload,
                ts_local_ns=ts_local_ns,
                ts_monotonic_ns=ts_monotonic_ns,
            )
        )

    def record_market_metadata(self, raw_market: Mapping[str, object]) -> None:
        """Normalize and record one Kalshi market metadata snapshot."""
        self._recorder.record(normalize_market_metadata(raw_market))

    def flush_all(self) -> None:
        """Flush all buffered recorder streams."""
        self._recorder.flush_all()


class KalshiNormalizer:
    """Stateful converter from Kalshi raw frames to normalized events."""

    def __init__(self) -> None:
        self._yes_bids: dict[str, dict[Decimal, Decimal]] = {}
        self._no_bids: dict[str, dict[Decimal, Decimal]] = {}
        self._last_sequence: dict[str, int] = {}

    def parse_frame(
        self,
        frame: str | bytes,
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> tuple[BookEvent | TradeEvent | GapEvent, ...]:
        """Parse a raw Kalshi websocket frame."""
        payload = json.loads(frame.decode("utf-8") if isinstance(frame, bytes) else frame)
        return self.parse_payload(
            payload,
            ts_local_ns=ts_local_ns,
            ts_monotonic_ns=ts_monotonic_ns,
        )

    def parse_payload(
        self,
        payload: Mapping[str, object],
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> tuple[BookEvent | TradeEvent | GapEvent, ...]:
        """Parse an already-decoded Kalshi payload."""
        capture_local_ns = time.time_ns() if ts_local_ns is None else ts_local_ns
        capture_monotonic_ns = (
            time.monotonic_ns()
            if ts_local_ns is None and ts_monotonic_ns is None
            else ts_monotonic_ns
        )
        message_type = _required_str(payload, "type")
        raw_message = payload.get("msg")
        if not isinstance(raw_message, Mapping):
            return ()
        message = cast(Mapping[str, object], raw_message)
        ticker = _optional_str(message.get("market_ticker"))
        gaps = self._sequence_gaps(payload, ticker, capture_local_ns, capture_monotonic_ns)
        if message_type == "orderbook_snapshot":
            return (
                *gaps,
                self._reconnect_gap(message, capture_local_ns, capture_monotonic_ns),
                *self._parse_snapshot(message, capture_local_ns, capture_monotonic_ns),
            )
        if message_type == "orderbook_delta":
            return (
                *gaps,
                *self._parse_delta(payload, message, capture_local_ns, capture_monotonic_ns),
            )
        if message_type == "trade":
            return (*gaps, self._parse_trade(message, capture_local_ns, capture_monotonic_ns))
        return ()

    def _parse_snapshot(
        self,
        message: Mapping[str, object],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> tuple[BookEvent, ...]:
        ticker = _required_str(message, "market_ticker")
        seq_or_hash = _optional_str(message.get("market_id"))
        yes_levels = _levels(
            message.get("yes_dollars_fp", message.get("yes_dollars", message.get("yes")))
        )
        no_levels = _levels(
            message.get("no_dollars_fp", message.get("no_dollars", message.get("no")))
        )
        self._yes_bids[ticker] = dict(yes_levels)
        self._no_bids[ticker] = dict(no_levels)

        events: list[BookEvent] = []
        for price, size in yes_levels:
            events.append(
                _book_event(
                    ts_local_ns=ts_local_ns,
                    ts_monotonic_ns=ts_monotonic_ns,
                    ts_exchange=None,
                    ticker=ticker,
                    event_type=BookEventType.SNAPSHOT,
                    side=BookSide.BID,
                    price=price,
                    size=size,
                    seq_or_hash=seq_or_hash,
                )
            )
        for no_price, size in no_levels:
            events.append(
                _book_event(
                    ts_local_ns=ts_local_ns,
                    ts_monotonic_ns=ts_monotonic_ns,
                    ts_exchange=None,
                    ticker=ticker,
                    event_type=BookEventType.SNAPSHOT,
                    side=BookSide.ASK,
                    price=Decimal("1") - no_price,
                    size=size,
                    seq_or_hash=seq_or_hash,
                )
            )
        return tuple(events)

    def _parse_delta(
        self,
        payload: Mapping[str, object],
        message: Mapping[str, object],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> tuple[BookEvent | GapEvent, ...]:
        ticker = _required_str(message, "market_ticker")
        side = _required_str(message, "side")
        price = _parse_probability_price(_first_present(message, "price_dollars", "price"))
        delta = parse_decimal(_first_present(message, "delta_fp", "delta"))
        seq = _optional_str(payload.get("seq"))
        ts_exchange = _optional_str(message.get("ts_ms")) or _optional_str(message.get("ts"))
        missing_snapshot = ticker not in self._yes_bids and ticker not in self._no_bids
        gaps = (
            (
                GapEvent(
                    ts_local_ns=ts_local_ns,
                    ts_monotonic_ns=ts_monotonic_ns,
                    ts_exchange=ts_exchange,
                    condition_id=ticker,
                    asset_id=ticker,
                    reason=GapReason.MISSING_SNAPSHOT,
                    details="Kalshi delta arrived before a trusted snapshot",
                    seq_or_hash=seq,
                ),
            )
            if missing_snapshot
            else ()
        )

        if side == "yes":
            levels = self._yes_bids.setdefault(ticker, {})
            new_size = max(Decimal("0"), levels.get(price, Decimal("0")) + delta)
            levels[price] = new_size
            return (
                *gaps,
                _book_event(
                    ts_local_ns=ts_local_ns,
                    ts_monotonic_ns=ts_monotonic_ns,
                    ts_exchange=ts_exchange,
                    ticker=ticker,
                    event_type=BookEventType.DELTA,
                    side=BookSide.BID,
                    price=price,
                    size=new_size,
                    seq_or_hash=seq,
                ),
            )

        if side == "no":
            levels = self._no_bids.setdefault(ticker, {})
            new_size = max(Decimal("0"), levels.get(price, Decimal("0")) + delta)
            levels[price] = new_size
            return (
                *gaps,
                _book_event(
                    ts_local_ns=ts_local_ns,
                    ts_monotonic_ns=ts_monotonic_ns,
                    ts_exchange=ts_exchange,
                    ticker=ticker,
                    event_type=BookEventType.DELTA,
                    side=BookSide.ASK,
                    price=Decimal("1") - price,
                    size=new_size,
                    seq_or_hash=seq,
                ),
            )
        raise ValueError(f"unsupported Kalshi orderbook side: {side}")

    def _parse_trade(
        self,
        message: Mapping[str, object],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> TradeEvent:
        ticker = _required_str(message, "market_ticker")
        taker_side = _optional_str(message.get("taker_side"))
        return TradeEvent(
            ts_local_ns=ts_local_ns,
            ts_monotonic_ns=ts_monotonic_ns,
            ts_exchange=_optional_str(message.get("ts_ms")) or _optional_str(message.get("ts")),
            condition_id=ticker,
            asset_id=ticker,
            price=_parse_probability_price(message["yes_price_dollars"]),
            size=parse_decimal(_first_present(message, "count_fp", "count")),
            taker_side=_kalshi_taker_side(taker_side),
            trade_id=_required_str(message, "trade_id"),
        )

    def _sequence_gaps(
        self,
        payload: Mapping[str, object],
        ticker: str | None,
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> tuple[GapEvent, ...]:
        sequence = _optional_int(payload.get("seq"))
        if sequence is None:
            return ()
        key = _optional_str(payload.get("sid")) or ticker
        if key is None:
            return ()
        previous = self._last_sequence.get(key)
        self._last_sequence[key] = max(sequence, previous or sequence)
        if previous is None or sequence <= previous + 1:
            return ()
        condition_id = ticker or "unknown"
        return (
            GapEvent(
                ts_local_ns=ts_local_ns,
                ts_monotonic_ns=ts_monotonic_ns,
                ts_exchange=None,
                condition_id=condition_id,
                asset_id=ticker,
                reason=GapReason.SEQUENCE_GAP,
                details=f"Kalshi sequence jumped from {previous} to {sequence}",
                seq_or_hash=str(sequence),
            ),
        )

    def _reconnect_gap(
        self,
        message: Mapping[str, object],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> GapEvent:
        ticker = _required_str(message, "market_ticker")
        return GapEvent(
            ts_local_ns=ts_local_ns,
            ts_monotonic_ns=ts_monotonic_ns,
            ts_exchange=None,
            condition_id=ticker,
            asset_id=ticker,
            reason=GapReason.RECONNECT,
            details="Kalshi snapshot received; local book state resynchronized",
            seq_or_hash=_optional_str(message.get("market_id")),
        )


def normalize_market_metadata(raw_market: Mapping[str, object]) -> MetadataSnapshot:
    """Normalize Kalshi market metadata into the shared metadata stream."""
    ticker = _required_str(raw_market, "ticker")
    status = (_optional_str(raw_market.get("status")) or "").casefold()
    return MetadataSnapshot(
        fetched_at=datetime.now(UTC).isoformat(),
        condition_id=ticker,
        question=_optional_str(raw_market.get("title")),
        outcomes=("yes", "no"),
        token_ids=(ticker,),
        resolution_or_end_time=(
            _optional_str(raw_market.get("expiration_time"))
            or _optional_str(raw_market.get("close_time"))
            or _optional_str(raw_market.get("expected_expiration_time"))
        ),
        active=status in {"open", "active"},
        closed=status in {"settled", "closed"},
        liquidity=_first_optional_decimal(raw_market, "liquidity_dollars", "liquidity"),
        volume=_first_optional_decimal(raw_market, "volume_fp", "volume", "volume_24h"),
        raw_json=json.dumps(raw_market, default=str, separators=(",", ":"), sort_keys=True),
    )


def kalshi_subscribe_message(
    *,
    channels: Sequence[str],
    market_tickers: Sequence[str],
    message_id: int = 1,
) -> dict[str, object]:
    """Build a Kalshi websocket subscribe command."""
    return {
        "id": message_id,
        "cmd": "subscribe",
        "params": {
            "channels": list(channels),
            "market_tickers": list(market_tickers),
        },
    }


def kalshi_ssl_context() -> ssl.SSLContext:
    """Build a websocket SSL context from certifi's CA bundle."""
    return ssl.create_default_context(cafile=certifi.where())


def kalshi_websocket_headers(
    auth: KalshiAuth,
    *,
    ws_url: str = KALSHI_WS_URL,
    timestamp_ms: int | None = None,
) -> dict[str, str]:
    return auth.headers_for_request(
        method="GET",
        path=kalshi_request_path(ws_url),
        timestamp_ms=timestamp_ms,
    )


def kalshi_auth_headers(
    *,
    key_id: str,
    private_key: rsa.RSAPrivateKey,
    method: str,
    path: str,
    timestamp_ms: int | None = None,
) -> dict[str, str]:
    timestamp = str(int(time.time() * 1_000) if timestamp_ms is None else timestamp_ms)
    message = f"{timestamp}{method.upper()}{path}"
    signature = sign_kalshi_request(private_key, message)
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": signature,
    }


def sign_kalshi_request(private_key: rsa.RSAPrivateKey, message: str) -> str:
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=32,
        ),
        hashes.SHA256(),
    )
    return b64encode(signature).decode("ascii")


def load_kalshi_private_key(path: Path) -> rsa.RSAPrivateKey:
    private_key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise TypeError("Kalshi private key must be an RSA private key")
    return private_key


def kalshi_request_path(url_or_path: str) -> str:
    if url_or_path.startswith(("http://", "https://", "ws://", "wss://")):
        parsed = urlsplit(url_or_path)
        return parsed.path or "/"
    return url_or_path.split("?", 1)[0] or "/"


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


def _book_event(
    *,
    ts_local_ns: int,
    ts_monotonic_ns: int | None,
    ts_exchange: str | None,
    ticker: str,
    event_type: BookEventType,
    side: BookSide,
    price: Decimal,
    size: Decimal,
    seq_or_hash: str | None,
) -> BookEvent:
    return BookEvent(
        ts_local_ns=ts_local_ns,
        ts_monotonic_ns=ts_monotonic_ns,
        ts_exchange=ts_exchange,
        condition_id=ticker,
        asset_id=ticker,
        event_type=event_type,
        side=side,
        price=price,
        size=size,
        seq_or_hash=seq_or_hash,
    )


def _levels(value: object) -> tuple[tuple[Decimal, Decimal], ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("Kalshi orderbook levels must be a list")
    levels: list[tuple[Decimal, Decimal]] = []
    for item in value:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes, bytearray)):
            raise TypeError("Kalshi orderbook level must be a price/size pair")
        if len(item) != 2:
            raise ValueError("Kalshi orderbook level must contain price and size")
        levels.append((_parse_probability_price(item[0]), parse_decimal(item[1])))
    return tuple(levels)


def _parse_probability_price(value: object) -> Decimal:
    price = parse_decimal(value)
    if price > Decimal("1"):
        return price / Decimal("100")
    return price


def _kalshi_taker_side(value: str | None) -> str | None:
    if value == "yes":
        return "buy"
    if value == "no":
        return "sell"
    return value


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return parse_decimal(value)


def _first_optional_decimal(raw: Mapping[str, object], *keys: str) -> Decimal | None:
    for key in keys:
        value = _optional_decimal(raw.get(key))
        if value is not None:
            return value
    return None


def _required_str(message: Mapping[str, object], key: str) -> str:
    value = _optional_str(message.get(key))
    if value is None:
        raise KeyError(key)
    return value


def _first_present(message: Mapping[str, object], *keys: str) -> object:
    for key in keys:
        value = message.get(key)
        if value is not None:
            return value
    raise KeyError(keys[0])


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("boolean is not a valid sequence")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"expected int-compatible value, got {type(value).__name__}")


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, int | Decimal):
        return str(value)
    raise TypeError(f"expected string-compatible value, got {type(value).__name__}")
