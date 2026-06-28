"""Stateful market-data processing for gap and checksum detection."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import cast

from pmbot.ingestion.hashing import compute_order_book_hash, hashes_match
from pmbot.ingestion.parsers import decode_ws_frame, parse_decimal, parse_ws_payload
from pmbot.ingestion.schemas import (
    BookEvent,
    BookSide,
    GapEvent,
    GapReason,
    TradeEvent,
)


@dataclass(slots=True)
class _BookState:
    condition_id: str
    asset_id: str
    timestamp: str
    min_order_size: str = ""
    tick_size: str = ""
    neg_risk: bool = False
    last_trade_price: str = ""
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)

    def levels(self, side: BookSide) -> list[dict[str, str]]:
        levels = self.bids if side == BookSide.BID else self.asks
        reverse = side == BookSide.ASK
        return [
            {"price": str(price), "size": str(size)}
            for price, size in sorted(levels.items(), reverse=reverse)
        ]

    def hash(self) -> str:
        return compute_order_book_hash(
            market=self.condition_id,
            asset_id=self.asset_id,
            timestamp=self.timestamp,
            bids=self.levels(BookSide.BID),
            asks=self.levels(BookSide.ASK),
            min_order_size=self.min_order_size,
            tick_size=self.tick_size,
            neg_risk=self.neg_risk,
            last_trade_price=self.last_trade_price,
        )


class MarketDataProcessor:
    """Parse raw frames and emit gap markers for discontinuities."""

    def __init__(self, *, verify_hashes: bool = True) -> None:
        self._verify_hashes = verify_hashes
        self._books: dict[str, _BookState] = {}
        self._last_sequence: dict[str, int] = {}

    def process_frame(
        self,
        frame: str | bytes,
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> list[BookEvent | TradeEvent | GapEvent]:
        """Process a raw websocket frame into normalized events and gaps."""
        payload = decode_ws_frame(frame)
        return self.process_payload(
            payload,
            ts_local_ns=ts_local_ns,
            ts_monotonic_ns=ts_monotonic_ns,
        )

    def process_payload(
        self,
        payload: object,
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> list[BookEvent | TradeEvent | GapEvent]:
        """Process an already-decoded payload into normalized events and gaps."""
        capture_local_ns = time.time_ns() if ts_local_ns is None else ts_local_ns
        capture_monotonic_ns = (
            time.monotonic_ns()
            if ts_local_ns is None and ts_monotonic_ns is None
            else ts_monotonic_ns
        )
        if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
            processed: list[BookEvent | TradeEvent | GapEvent] = []
            for item in payload:
                processed.extend(
                    self.process_payload(
                        item,
                        ts_local_ns=capture_local_ns,
                        ts_monotonic_ns=capture_monotonic_ns,
                    )
                )
            return processed
        if not isinstance(payload, Mapping):
            return []

        message = cast(Mapping[str, object], payload)
        gaps = self._update_state_and_find_gaps(
            message,
            capture_local_ns,
            capture_monotonic_ns,
        )
        events = parse_ws_payload(
            message,
            ts_local_ns=capture_local_ns,
            ts_monotonic_ns=capture_monotonic_ns,
        )
        gaps.extend(self._detect_sequence_gaps(events, capture_local_ns, capture_monotonic_ns))
        return [*events, *gaps]

    def connection_gap(
        self,
        *,
        condition_id: str,
        asset_id: str | None = None,
        details: str = "websocket connection re-established; fresh snapshot requested",
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> GapEvent:
        """Create a reconnect gap marker for a fresh connection boundary."""
        return GapEvent(
            ts_local_ns=time.time_ns() if ts_local_ns is None else ts_local_ns,
            ts_monotonic_ns=(
                time.monotonic_ns()
                if ts_local_ns is None and ts_monotonic_ns is None
                else ts_monotonic_ns
            ),
            ts_exchange=None,
            condition_id=condition_id,
            asset_id=asset_id,
            reason=GapReason.RECONNECT,
            details=details,
            seq_or_hash=None,
        )

    def dropped_connection_gap(
        self,
        *,
        condition_id: str,
        asset_id: str | None = None,
        details: str,
        ts_local_ns: int | None = None,
        ts_monotonic_ns: int | None = None,
    ) -> GapEvent:
        """Create a connection-loss gap marker."""
        return GapEvent(
            ts_local_ns=time.time_ns() if ts_local_ns is None else ts_local_ns,
            ts_monotonic_ns=(
                time.monotonic_ns()
                if ts_local_ns is None and ts_monotonic_ns is None
                else ts_monotonic_ns
            ),
            ts_exchange=None,
            condition_id=condition_id,
            asset_id=asset_id,
            reason=GapReason.CONNECTION_LOST,
            details=details,
            seq_or_hash=None,
        )

    def _update_state_and_find_gaps(
        self,
        message: Mapping[str, object],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> list[GapEvent]:
        event_type = _optional_str(message.get("event_type"))
        if event_type == "book" or (event_type is None and _is_book_snapshot(message)):
            return self._apply_snapshot(message, ts_local_ns, ts_monotonic_ns)
        if event_type == "price_change":
            return self._apply_delta(message, ts_local_ns, ts_monotonic_ns)
        return []

    def _apply_snapshot(
        self,
        message: Mapping[str, object],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> list[GapEvent]:
        condition_id = _required_str(message, "market")
        asset_id = _required_str(message, "asset_id")
        ts_exchange = _optional_str(message.get("timestamp")) or ""
        state = _BookState(
            condition_id=condition_id,
            asset_id=asset_id,
            timestamp=ts_exchange,
            min_order_size=_optional_str(message.get("min_order_size")) or "",
            tick_size=_optional_str(message.get("tick_size")) or "",
            neg_risk=_optional_bool(message.get("neg_risk")),
            last_trade_price=_optional_str(message.get("last_trade_price")) or "",
            bids=_levels_by_price(message.get("bids")),
            asks=_levels_by_price(message.get("asks")),
        )
        self._books[asset_id] = state

        expected_hash = _optional_str(message.get("hash"))
        if not self._verify_hashes or expected_hash is None:
            return []
        actual_hash = state.hash()
        if hashes_match(expected_hash, actual_hash):
            return []
        return [
            GapEvent(
                ts_local_ns=ts_local_ns,
                ts_monotonic_ns=ts_monotonic_ns,
                ts_exchange=ts_exchange or None,
                condition_id=condition_id,
                asset_id=asset_id,
                reason=GapReason.CHECKSUM_MISMATCH,
                details="snapshot book hash did not match local checksum",
                seq_or_hash=expected_hash,
            )
        ]

    def _apply_delta(
        self,
        message: Mapping[str, object],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> list[GapEvent]:
        condition_id = _required_str(message, "market")
        ts_exchange = _optional_str(message.get("timestamp")) or ""
        changes = message.get("price_changes")
        if not isinstance(changes, Sequence) or isinstance(changes, (str, bytes, bytearray)):
            return []

        gaps: list[GapEvent] = []
        for raw_change in changes:
            if not isinstance(raw_change, Mapping):
                continue
            change = cast(Mapping[str, object], raw_change)
            asset_id = _required_str(change, "asset_id")
            state = self._books.get(asset_id)
            has_trusted_snapshot = state is not None
            if state is None:
                state = _BookState(
                    condition_id=condition_id,
                    asset_id=asset_id,
                    timestamp=ts_exchange,
                )
                self._books[asset_id] = state
                gaps.append(
                    GapEvent(
                        ts_local_ns=ts_local_ns,
                        ts_monotonic_ns=ts_monotonic_ns,
                        ts_exchange=ts_exchange or None,
                        condition_id=condition_id,
                        asset_id=asset_id,
                        reason=GapReason.MISSING_SNAPSHOT,
                        details="delta arrived before a trusted snapshot",
                        seq_or_hash=_optional_str(change.get("hash")),
                    )
                )
            state.timestamp = ts_exchange
            side = _book_side_from_venue(_required_str(change, "side"))
            levels = state.bids if side == BookSide.BID else state.asks
            price = parse_decimal(change["price"])
            size = parse_decimal(change["size"])
            if size == Decimal("0"):
                levels.pop(price, None)
            else:
                levels[price] = size

            expected_hash = _optional_str(change.get("hash"))
            if (
                has_trusted_snapshot
                and self._verify_hashes
                and expected_hash is not None
                and not hashes_match(expected_hash, state.hash())
            ):
                gaps.append(
                    GapEvent(
                        ts_local_ns=ts_local_ns,
                        ts_monotonic_ns=ts_monotonic_ns,
                        ts_exchange=ts_exchange or None,
                        condition_id=condition_id,
                        asset_id=asset_id,
                        reason=GapReason.CHECKSUM_MISMATCH,
                        details="delta book hash did not match local checksum",
                        seq_or_hash=expected_hash,
                    )
                )
        return gaps

    def _detect_sequence_gaps(
        self,
        events: Sequence[BookEvent | TradeEvent],
        ts_local_ns: int,
        ts_monotonic_ns: int | None,
    ) -> list[GapEvent]:
        gaps: list[GapEvent] = []
        seen_in_message: set[tuple[str, int]] = set()
        for event in events:
            if not isinstance(event, BookEvent):
                continue
            sequence = _parse_sequence(event.seq_or_hash)
            if sequence is None:
                continue
            key = event.asset_id
            if (key, sequence) in seen_in_message:
                continue
            seen_in_message.add((key, sequence))
            previous = self._last_sequence.get(key)
            if previous is not None and sequence > previous + 1:
                gaps.append(
                    GapEvent(
                        ts_local_ns=ts_local_ns,
                        ts_monotonic_ns=ts_monotonic_ns,
                        ts_exchange=event.ts_exchange,
                        condition_id=event.condition_id,
                        asset_id=event.asset_id,
                        reason=GapReason.SEQUENCE_GAP,
                        details=f"sequence jumped from {previous} to {sequence}",
                        seq_or_hash=str(sequence),
                    )
                )
            if previous is None or sequence > previous:
                self._last_sequence[key] = sequence
        return gaps


def _levels_by_price(value: object) -> dict[Decimal, Decimal]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("book levels must be a list")
    levels: dict[Decimal, Decimal] = {}
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("book level must be an object")
        level = cast(Mapping[str, object], item)
        levels[parse_decimal(level["price"])] = parse_decimal(level["size"])
    return levels


def _is_book_snapshot(message: Mapping[str, object]) -> bool:
    return (
        "market" in message
        and "asset_id" in message
        and "bids" in message
        and "asks" in message
    )


def _book_side_from_venue(side: str) -> BookSide:
    normalized = side.upper()
    if normalized in {"BUY", "BID"}:
        return BookSide.BID
    if normalized in {"SELL", "ASK"}:
        return BookSide.ASK
    raise ValueError(f"unsupported book side: {side}")


def _parse_sequence(value: str | None) -> int | None:
    if value is None or not value.isdigit():
        return None
    return int(value)


def _required_str(message: Mapping[str, object], key: str) -> str:
    value = _optional_str(message.get(key))
    if value is None:
        raise KeyError(key)
    return value


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, int | Decimal):
        return str(value)
    raise TypeError(f"expected string-compatible value, got {type(value).__name__}")


def _optional_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    raise TypeError(f"expected bool-compatible value, got {type(value).__name__}")
