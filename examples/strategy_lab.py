"""An editable strategy starter. Run offline or replay your own recorded books."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pmbot.backtest import (
    BacktestConfig,
    BacktestEngine,
    OrderRequest,
    OrderSide,
    PointInTimeView,
)
from pmbot.backtest.data import ReplayEvent, load_replay_events
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide

DEMO_ASSET = "demo-yes"
BASE_TS = 1_800_000_000_000_000_000


@dataclass
class EntryStrategy:
    """Submit one limit buy when the best ask is at or below an entry threshold.

    Edit this callback to try your own signal. Submission is not a fill: the
    engine applies latency, available depth, queue position, and modeled costs.
    """

    asset_id: str
    entry_price: Decimal
    size: Decimal
    submitted: bool = False

    def __call__(self, view: PointInTimeView) -> Iterable[OrderRequest]:
        if self.submitted or view.gap_active(self.asset_id):
            return ()
        book = view.book(self.asset_id)
        if book.best_bid is None or book.best_ask is None:
            return ()
        if book.best_ask.price > self.entry_price:
            return ()
        self.submitted = True
        return (
            OrderRequest(
                asset_id=self.asset_id,
                side=OrderSide.BUY,
                size=self.size,
                limit_price=self.entry_price,
                client_order_id="entry",
            ),
        )


def demo_events() -> tuple[ReplayEvent, ...]:
    """Small, deterministic synthetic books; these are not historical prices."""
    events: list[ReplayEvent] = []
    quotes = [("0.49", "0.51"), ("0.47", "0.49"), ("0.50", "0.52"), ("0.53", "0.55")]
    for second, (bid, ask) in enumerate(quotes):
        for side, price in ((BookSide.BID, bid), (BookSide.ASK, ask)):
            events.append(
                BookEvent(
                    ts_local_ns=BASE_TS + second * 1_000_000_000,
                    ts_monotonic_ns=None,
                    ts_exchange=None,
                    condition_id="demo-condition",
                    asset_id=DEMO_ASSET,
                    event_type=BookEventType.SNAPSHOT,
                    side=side,
                    price=Decimal(price),
                    size=Decimal("100"),
                    seq_or_hash=f"demo-{second}",
                )
            )
    return tuple(events)


def run_experiment(
    events: tuple[ReplayEvent, ...],
    *,
    asset_id: str,
    entry_price: Decimal,
    size: Decimal,
    latency_ms: int,
    source: str,
) -> dict[str, object]:
    """Replay one experiment and return JSON-compatible research outputs."""
    if not entry_price.is_finite() or not Decimal("0") < entry_price < Decimal("1"):
        raise ValueError("entry-price must be a finite number between 0 and 1 (exclusive)")
    if not size.is_finite() or size <= 0:
        raise ValueError("size must be a finite positive number")
    if latency_ms < 0:
        raise ValueError("latency-ms must be nonnegative")
    if not any(isinstance(event, BookEvent) and event.asset_id == asset_id for event in events):
        raise ValueError(f"no recorded books found for asset {asset_id!r}")

    config = BacktestConfig(latency_ms=latency_ms)
    strategy = EntryStrategy(asset_id, entry_price, size)
    final_ts = max(event.ts_local_ns for event in events)

    def within_replay(view: PointInTimeView) -> Iterable[OrderRequest]:
        # Leave a later observation to mark every simulated arrival. The engine
        # otherwise drains pending orders against the last book at end of input.
        if view.ts_local_ns + latency_ms * 1_000_000 >= final_ts:
            return ()
        return strategy(view)

    result = BacktestEngine(config).run(events, within_replay)
    final = result.pnl_series[-1]
    return {
        "source": source,
        "strategy": "EntryStrategy",
        "parameters": {
            "asset_id": asset_id,
            "entry_price": str(entry_price),
            "size": str(size),
            "latency_ms": latency_ms,
        },
        "costs": asdict(config.costs),
        "marking_mode": config.marking_mode.value,
        "event_count": len(events),
        "start_ns": min(event.ts_local_ns for event in events),
        "end_ns": final_ts,
        "fills": [asdict(fill) for fill in result.blotter],
        "final_positions": dict(final.positions),
        "final_cash": str(final.cash),
        "final_marked_pnl": str(final.pnl) if final.pnl_valid else None,
        "final_mark_valid": final.pnl_valid,
        "all_marks_valid": result.pnl_valid,
        "invalid_mark_count": sum(not mark.pnl_valid for mark in result.pnl_series),
    }


def decimal_argument(value: str) -> Decimal:
    """Give CLI users a concise error for malformed decimal arguments."""
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("expected a decimal number") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entry-price", type=decimal_argument, default=Decimal("0.50"))
    parser.add_argument("--size", type=decimal_argument, default=Decimal("10"))
    parser.add_argument("--latency-ms", type=int, default=150)
    parser.add_argument("--data-dir", type=Path, help="Recorded data root containing raw/book.")
    parser.add_argument("--asset-id", help="Required with --data-dir; use a recorded token ID.")
    parser.add_argument("--report", type=Path, help="Save JSON to a new file; never overwrite.")
    args = parser.parse_args()
    if args.data_dir is not None and not args.asset_id:
        parser.error("--asset-id is required with --data-dir")
    if args.data_dir is None and args.asset_id is not None:
        parser.error("--asset-id requires --data-dir; the demo uses demo-yes")
    if args.report is not None and args.report.exists():
        parser.error(f"report already exists: {args.report}; choose a new filename")
    try:
        events = load_replay_events(args.data_dir) if args.data_dir is not None else demo_events()
        payload = run_experiment(
            events,
            asset_id=args.asset_id or DEMO_ASSET,
            entry_price=args.entry_price,
            size=args.size,
            latency_ms=args.latency_ms,
            source=str(args.data_dir) if args.data_dir is not None else "synthetic-demo",
        )
        output = json.dumps(payload, indent=2, default=str)
        if args.report is not None:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            with args.report.open("x", encoding="utf-8") as report:
                report.write(output + "\n")
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(output)


if __name__ == "__main__":
    main()
