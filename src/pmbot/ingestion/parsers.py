"""Raw Polymarket websocket frame parsers."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import cast

from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide, TradeEvent


def parse_ws_frame(
    frame: str | bytes,
    ts_local_ns: int | None = None,
    ts_monotonic_ns: int | None = None,
) -> list[BookEvent | TradeEvent]:
    """Parse a raw websocket frame into normalized events."""
    payload = decode_ws_frame(frame)
    return parse_ws_payload(
        payload,
        ts_local_ns=ts_local_ns,
        ts_monotonic_ns=ts_monotonic_ns,
    )


def decode_ws_frame(frame: str | bytes) -> object:
    """Decode a websocket JSON frame without allowing binary float drift."""
    text = frame.decode("utf-8") if isinstance(frame, bytes) else frame
    if text in {"PING", "PONG", ""}:
        return {}
    return json.loads(text, parse_float=Decimal)


def parse_ws_payload(
    payload: object,
    ts_local_ns: int | None = None,
    ts_monotonic_ns: int | None = None,
) -> list[BookEvent | TradeEvent]:
    """Parse an already-decoded websocket payload."""
    capture_local_ns = time.time_ns() if ts_local_ns is None else ts_local_ns
    capture_monotonic_ns = (
        time.monotonic_ns()
        if ts_local_ns is None and ts_monotonic_ns is None
        else ts_monotonic_ns
    )
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        events: list[BookEvent | TradeEvent] = []
        for item in payload:
            events.extend(
                parse_ws_payload(
                    item,
                    ts_local_ns=capture_local_ns,
                    ts_monotonic_ns=capture_monotonic_ns,
                )
            )
        return events
    if not isinstance(payload, Mapping):
        return []
    return _parse_message(
        cast(Mapping[str, object], payload),
        capture_local_ns,
        capture_monotonic_ns,
    )


def _parse_message(
    message: Mapping[str, object],
    ts_local_ns: int,
    ts_monotonic_ns: int | None,
) -> list[BookEvent | TradeEvent]:
    event_type = _optional_str(message.get("event_type"))
    if event_type == "book":
        book_events: list[BookEvent | TradeEvent] = []
        book_events.extend(_parse_book(message, ts_local_ns, ts_monotonic_ns))
        return book_events
    if event_type == "price_change":
        delta_events: list[BookEvent | TradeEvent] = []
        delta_events.extend(_parse_price_change(message, ts_local_ns, ts_monotonic_ns))
        return delta_events
    if event_type == "last_trade_price":
        return [_parse_last_trade_price(message, ts_local_ns, ts_monotonic_ns)]
    return []


def _parse_book(
    message: Mapping[str, object],
    ts_local_ns: int,
    ts_monotonic_ns: int | None,
) -> list[BookEvent]:
    condition_id = _required_str(message, "market")
    asset_id = _required_str(message, "asset_id")
    ts_exchange = _optional_str(message.get("timestamp"))
    seq_or_hash = _seq_or_hash(message)
    events: list[BookEvent] = []
    for raw_side, side in (("bids", BookSide.BID), ("asks", BookSide.ASK)):
        for level in _level_sequence(message.get(raw_side)):
            events.append(
                BookEvent(
                    ts_local_ns=ts_local_ns,
                    ts_monotonic_ns=ts_monotonic_ns,
                    ts_exchange=ts_exchange,
                    condition_id=condition_id,
                    asset_id=asset_id,
                    event_type=BookEventType.SNAPSHOT,
                    side=side,
                    price=parse_decimal(level["price"]),
                    size=parse_decimal(level["size"]),
                    seq_or_hash=seq_or_hash,
                )
            )
    return events


def _parse_price_change(
    message: Mapping[str, object],
    ts_local_ns: int,
    ts_monotonic_ns: int | None,
) -> list[BookEvent]:
    condition_id = _required_str(message, "market")
    ts_exchange = _optional_str(message.get("timestamp"))
    changes = message.get("price_changes")
    if not isinstance(changes, Sequence) or isinstance(changes, (str, bytes, bytearray)):
        raise TypeError("price_change message requires a price_changes list")

    events: list[BookEvent] = []
    for raw_change in changes:
        if not isinstance(raw_change, Mapping):
            raise TypeError("price_changes entries must be objects")
        change = cast(Mapping[str, object], raw_change)
        events.append(
            BookEvent(
                ts_local_ns=ts_local_ns,
                ts_monotonic_ns=ts_monotonic_ns,
                ts_exchange=ts_exchange,
                condition_id=condition_id,
                asset_id=_required_str(change, "asset_id"),
                event_type=BookEventType.DELTA,
                side=_book_side_from_venue(_required_str(change, "side")),
                price=parse_decimal(change["price"]),
                size=parse_decimal(change["size"]),
                seq_or_hash=_seq_or_hash(change) or _seq_or_hash(message),
            )
        )
    return events


def _parse_last_trade_price(
    message: Mapping[str, object],
    ts_local_ns: int,
    ts_monotonic_ns: int | None,
) -> TradeEvent:
    condition_id = _required_str(message, "market")
    asset_id = _required_str(message, "asset_id")
    ts_exchange = _optional_str(message.get("timestamp"))
    price = parse_decimal(message["price"])
    size = parse_decimal(message["size"])
    taker_side = _optional_str(message.get("side"))
    if taker_side is not None:
        taker_side = taker_side.lower()
    return TradeEvent(
        ts_local_ns=ts_local_ns,
        ts_monotonic_ns=ts_monotonic_ns,
        ts_exchange=ts_exchange,
        condition_id=condition_id,
        asset_id=asset_id,
        price=price,
        size=size,
        taker_side=taker_side,
        trade_id=_trade_id(message, condition_id, asset_id, price, size, ts_exchange),
    )


def parse_decimal(value: object) -> Decimal:
    """Parse a venue price/size without accepting Python binary floats."""
    if isinstance(value, bool | float):
        raise TypeError("prices and sizes must not be binary floats")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | str):
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"invalid decimal value: {value}") from exc
    raise TypeError(f"unsupported decimal value type: {type(value).__name__}")


def _book_side_from_venue(side: str) -> BookSide:
    normalized = side.upper()
    if normalized == "BUY":
        return BookSide.BID
    if normalized == "SELL":
        return BookSide.ASK
    if normalized == "BID":
        return BookSide.BID
    if normalized == "ASK":
        return BookSide.ASK
    raise ValueError(f"unsupported book side: {side}")


def _level_sequence(value: object) -> Sequence[Mapping[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("book side levels must be a list")
    levels: list[Mapping[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("book side levels must be objects")
        levels.append(cast(Mapping[str, object], item))
    return levels


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


def _seq_or_hash(message: Mapping[str, object]) -> str | None:
    for key in ("sequence", "seq", "hash"):
        value = _optional_str(message.get(key))
        if value is not None:
            return value
    return None


def _trade_id(
    message: Mapping[str, object],
    condition_id: str,
    asset_id: str,
    price: Decimal,
    size: Decimal,
    ts_exchange: str | None,
) -> str:
    provided = _optional_str(message.get("trade_id")) or _optional_str(message.get("id"))
    if provided is not None:
        return provided
    payload = "|".join(
        [
            condition_id,
            asset_id,
            ts_exchange or "",
            str(price),
            str(size),
            _optional_str(message.get("side")) or "",
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()
