"""Public CLOB HTTP and websocket ingestion clients."""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypeVar

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from pmbot.ingestion.processor import MarketDataProcessor
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, GapEvent, GapReason, NormalizedEvent

T = TypeVar("T")

DEFAULT_CLOB_HTTP_URL = "https://clob.polymarket.com"
DEFAULT_CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class ClobHttpClient:
    """Public CLOB read-only HTTP client."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_CLOB_HTTP_URL,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http

    async def get_order_book(self, token_id: str) -> list[BookEvent | GapEvent]:
        """Fetch and normalize a fresh order-book snapshot for one token ID."""
        payloads = await self.get_order_book_payloads([token_id])
        processor = MarketDataProcessor(verify_hashes=True)
        events: list[BookEvent | GapEvent] = []
        for payload in payloads:
            events.extend(
                event
                for event in processor.process_payload(payload)
                if isinstance(event, (BookEvent, GapEvent))
            )
        return events

    async def get_order_book_payloads(self, token_ids: Iterable[str]) -> list[object]:
        """Fetch raw order-book payloads for token IDs using batch requests."""
        payloads: list[object] = []
        token_id_list = list(token_ids)
        async with self._client() as client:
            for chunk in _chunks(token_id_list, 500):
                response = await client.post(
                    f"{self._base_url}/books",
                    json=[{"token_id": token_id} for token_id in chunk],
                )
                response.raise_for_status()
                payload = json.loads(response.text, parse_float=Decimal)
                if isinstance(payload, list):
                    payloads.extend(payload)
                else:
                    payloads.append(payload)
        return payloads

    async def get_order_book_via_get(self, token_id: str) -> list[BookEvent | GapEvent]:
        """Fetch and normalize a single snapshot via GET /book."""
        async with self._client() as client:
            response = await client.get(f"{self._base_url}/book", params={"token_id": token_id})
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=Decimal)
        processor = MarketDataProcessor(verify_hashes=True)
        return [
            event
            for event in processor.process_payload(payload)
            if isinstance(event, (BookEvent, GapEvent))
        ]

    def _client(self) -> _ClientContext:
        return _ClientContext(self._http)


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Exponential backoff settings for websocket reconnects."""

    initial_seconds: float = 1.0
    max_seconds: float = 30.0
    jitter_seconds: float = 0.25
    max_retries: int = 5

    def delay(self, attempt: int) -> float:
        base = min(self.max_seconds, self.initial_seconds * (2 ** max(0, attempt - 1)))
        jitter = float(random.uniform(0, self.jitter_seconds))
        return float(base + jitter)


class MarketDataIngestor:
    """Connect to the CLOB market websocket and record normalized events."""

    def __init__(
        self,
        *,
        token_ids: Iterable[str],
        recorder: ParquetRecorder,
        clob_http: ClobHttpClient,
        ws_url: str = DEFAULT_CLOB_WS_URL,
        backoff: BackoffPolicy | None = None,
        processor: MarketDataProcessor | None = None,
    ) -> None:
        self._token_ids = tuple(token_ids)
        self._recorder = recorder
        self._clob_http = clob_http
        self._ws_url = ws_url
        self._backoff = backoff or BackoffPolicy()
        self._processor = processor or MarketDataProcessor()

    async def run(self, *, duration_seconds: int) -> None:
        """Run ingestion for a bounded duration."""
        deadline = time.monotonic() + duration_seconds
        attempt = 0
        while time.monotonic() < deadline:
            try:
                await self._record_fresh_snapshots()
                await self._run_once(deadline=deadline)
                return
            except ConnectionClosed as exc:
                attempt += 1
                self._record_connection_loss(str(exc))
            except OSError as exc:
                attempt += 1
                self._record_connection_loss(str(exc))
            if attempt > self._backoff.max_retries:
                raise RuntimeError("CLOB websocket retry limit exceeded")
            await asyncio.sleep(
                min(self._backoff.delay(attempt), max(0.0, deadline - time.monotonic()))
            )
        self._recorder.flush_all()

    async def _run_once(self, *, deadline: float) -> None:
        async with connect(self._ws_url, ping_interval=None) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "assets_ids": list(self._token_ids),
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                )
            )
            heartbeat = asyncio.create_task(_heartbeat(websocket))
            try:
                while time.monotonic() < deadline:
                    timeout = max(0.0, deadline - time.monotonic())
                    if timeout == 0:
                        break
                    message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                    events = self._processor.process_frame(cast_ws_message(message))
                    self._recorder.record_many(events)
                    if _has_checksum_gap(events):
                        await self._record_fresh_snapshots()
            except TimeoutError:
                return
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                self._recorder.flush_all()

    async def _record_fresh_snapshots(self) -> None:
        try:
            payloads = await self._clob_http.get_order_book_payloads(self._token_ids)
        except httpx.HTTPError as exc:
            for token_id in self._token_ids:
                self._recorder.record(
                    GapEvent(
                        ts_local_ns=time.time_ns(),
                        ts_monotonic_ns=time.monotonic_ns(),
                        ts_exchange=None,
                        condition_id="unknown",
                        asset_id=token_id,
                        reason=GapReason.CONNECTION_LOST,
                        details=f"failed to fetch fresh snapshot: {exc}",
                        seq_or_hash=None,
                    )
                )
            self._recorder.flush_all()
            return

        for token_id, payload in zip(self._token_ids, payloads, strict=False):
            events = [
                event
                for event in self._processor.process_payload(payload)
                if isinstance(event, (BookEvent, GapEvent))
            ]
            self._recorder.record_many(events)
            condition_ids = {
                event.condition_id
                for event in events
                if isinstance(event, (BookEvent, GapEvent))
            }
            if not condition_ids:
                condition_ids = {"unknown"}
            for condition_id in condition_ids:
                self._recorder.record(
                    self._processor.connection_gap(
                        condition_id=condition_id,
                        asset_id=token_id,
                    )
                )
        self._recorder.flush_all()

    def _record_connection_loss(self, details: str) -> None:
        for token_id in self._token_ids:
            self._recorder.record(
                self._processor.dropped_connection_gap(
                    condition_id="unknown",
                    asset_id=token_id,
                    details=details,
                )
            )
        self._recorder.flush_all()


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


async def _heartbeat(websocket: Any) -> None:
    while True:
        await asyncio.sleep(10.0)
        await websocket.send("PING")


def _has_checksum_gap(events: Iterable[NormalizedEvent]) -> bool:
    return any(
        isinstance(event, GapEvent) and event.reason == GapReason.CHECKSUM_MISMATCH
        for event in events
    )


def cast_ws_message(message: str | bytes | bytearray | memoryview) -> str | bytes:
    if isinstance(message, memoryview):
        return message.tobytes()
    if isinstance(message, bytearray):
        return bytes(message)
    return message


def _chunks(items: list[T], size: int) -> Iterable[list[T]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]
