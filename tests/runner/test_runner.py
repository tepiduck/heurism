import asyncio
import json
import logging
from decimal import Decimal

import polars as pl

from pmbot.ingestion.clob import BackoffPolicy
from pmbot.ingestion.schemas import (
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    GapReason,
    TradeEvent,
)
from pmbot.runner.allowlist import load_verified_pairs
from pmbot.runner.collector import _record_startup_resolutions
from pmbot.runner.models import CollectorStatus, ResolutionRecord, VenueStatus
from pmbot.runner.orchestrator import CollectorOrchestrator
from pmbot.runner.resolutions import ResolutionPoller, StaticResolutionSource
from pmbot.runner.signal_logging import SignalEventObserver, SignalLoggingRecorder
from pmbot.runner.state import STATUS_RELATIVE_PATH, CollectorStateStore, load_status, render_status
from pmbot.runner.streams import ResolutionParquetStore, SignalParquetWriter
from pmbot.signals.contracts import VerifiedContractPair

BASE_TS = 1_800_000_000_000_000_000
PAIR = VerifiedContractPair(
    polymarket_condition_id="pm-condition",
    polymarket_asset_id="pm-yes",
    kalshi_ticker="KXTEST",
    rationale="fixture",
)


def test_orchestrator_restarts_failed_venue_without_stopping_other(tmp_path, caplog) -> None:
    async def run() -> None:
        state = CollectorStateStore(data_dir=tmp_path, pairs_tracked=1)
        failed = FailsOnceTask()
        stable = StableTask()
        orchestrator = CollectorOrchestrator(
            venue_tasks=(failed, stable),
            state=state,
            backoff=BackoffPolicy(initial_seconds=0, max_seconds=0, jitter_seconds=0),
        )

        await orchestrator.run(duration_seconds=0.05)

        assert failed.runs >= 2
        assert stable.ticks > 0
        assert next(item for item in state.status.venues if item.venue == "failed").reconnects == 1
        assert "RuntimeError: boom" in caplog.text

    caplog.set_level(logging.WARNING, logger="pmbot.runner.orchestrator")
    asyncio.run(run())


def test_signal_logging_writes_expected_parquet_schema(tmp_path) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
    observer = SignalEventObserver(pairs=(PAIR,), signal_writer=writer)

    for event in (
        book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.BID, "0.49", "10"),
        book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.ASK, "0.51", "10"),
        book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.BID, "0.69", "10"),
        book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.71", "10"),
    ):
        observer.observe(event)
    observer.flush()

    files = list(tmp_path.glob("signals/date=*/market=pm-condition/*.parquet"))
    frame = pl.read_parquet(files)

    assert len(files) == 1
    assert frame.columns == [
        "ts_local_ns",
        "polymarket_condition_id",
        "polymarket_asset_id",
        "kalshi_ticker",
        "p_hat",
        "polymarket_probability",
        "kalshi_probability",
        "signal_value",
        "direction",
        "confidence",
        "rationale",
    ]
    assert frame["direction"].to_list() == ["buy"]
    assert frame["p_hat"].to_list()[0] > Decimal("0.60")


def test_signal_logging_records_snapshot_batch_atomically(tmp_path) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
    observer = SignalEventObserver(pairs=(PAIR,), signal_writer=writer)
    recorder = SignalLoggingRecorder(
        data_dir=tmp_path,
        observer=observer,
        buffer_size=100,
        flush_interval_seconds=999,
    )
    recorder.record_many(
        (
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.BID, "0.59", "10"),
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.ASK, "0.61", "10"),
        )
    )

    recorder.record_many(
        (
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.BID, "0.60", "100"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.98", "100"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.97", "100"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.61", "100"),
        )
    )
    recorder.flush_all()

    files = list(tmp_path.glob("signals/date=*/market=pm-condition/*.parquet"))
    frame = pl.read_parquet(files)

    assert len(frame) == 1
    assert frame["kalshi_probability"].to_list() == [Decimal("0.6050000000")]


def test_signal_logging_keeps_bounded_recent_history(tmp_path) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=10_000, flush_interval_seconds=999)
    observer = SignalEventObserver(
        pairs=(PAIR,),
        signal_writer=writer,
        max_history_events=12,
        max_history_trades=5,
    )

    for index in range(100):
        observer.observe(
            book_event(
                BASE_TS + index,
                "pm-condition",
                "pm-yes",
                BookSide.BID,
                "0.49",
                "10",
            )
        )
        observer.observe(trade_event(BASE_TS + index, f"trade-{index}"))

    assert observer.recent_event_count == 12
    assert observer.recent_trade_count == 5


def test_signal_logging_infers_venue_for_unmapped_polymarket_no_token(tmp_path) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=10_000, flush_interval_seconds=999)
    state = CollectorStateStore(data_dir=tmp_path, pairs_tracked=1)
    observer = SignalEventObserver(pairs=(PAIR,), signal_writer=writer, state=state)
    polymarket_no_asset_id = "1234567890123456789012345678901234567890"

    observer.observe(
        book_event(
            BASE_TS,
            "0xabc123",
            polymarket_no_asset_id,
            BookSide.BID,
            "0.49",
            "10",
        )
    )
    observer.observe(
        GapEvent(
            ts_local_ns=BASE_TS + 1,
            ts_monotonic_ns=None,
            ts_exchange=None,
            condition_id="0xabc123",
            asset_id=None,
            reason=GapReason.MISSING_SNAPSHOT,
            details="condition-level gap",
            seq_or_hash=None,
        )
    )
    observer.observe(
        GapEvent(
            ts_local_ns=BASE_TS + 2,
            ts_monotonic_ns=None,
            ts_exchange=None,
            condition_id="pm-condition",
            asset_id=polymarket_no_asset_id,
            reason=GapReason.CONNECTION_LOST,
            details="asset-level gap",
            seq_or_hash=None,
        )
    )
    observer.observe(
        GapEvent(
            ts_local_ns=BASE_TS + 3,
            ts_monotonic_ns=None,
            ts_exchange=None,
            condition_id="KXUNMAPPED",
            asset_id=None,
            reason=GapReason.RECONNECT,
            details="ticker-level gap",
            seq_or_hash=None,
        )
    )

    venues = {venue.venue: venue for venue in state.status.venues}

    assert "unknown" not in venues
    assert venues["polymarket"].frames_total == 1
    assert len(venues["polymarket"].gap_timestamps_ns) == 2
    assert len(venues["kalshi"].gap_timestamps_ns) == 1


def test_resolution_poller_is_idempotent(tmp_path) -> None:
    async def run() -> None:
        record = ResolutionRecord(
            venue="polymarket",
            market_id="pm-condition",
            resolved_outcome="yes",
            resolved_at="2026-01-01T00:00:00Z",
            fetched_at="2026-01-01T00:01:00Z",
            raw_json=None,
        )
        store = ResolutionParquetStore(data_dir=tmp_path)
        poller = ResolutionPoller(
            pairs=(PAIR,),
            sources=(StaticResolutionSource((record, record)),),
            store=store,
        )

        assert await poller.poll_once() == 1
        assert await poller.poll_once() == 0
        assert store.count == 1

    asyncio.run(run())


def test_startup_resolution_filter_skips_closed_pair_without_reconnect_churn(tmp_path) -> None:
    async def run() -> None:
        live_pair = VerifiedContractPair(
            polymarket_condition_id="pm-live",
            polymarket_asset_id="pm-live-yes",
            kalshi_ticker="KXLIVE",
            rationale="fixture",
        )
        record = ResolutionRecord(
            venue="kalshi",
            market_id=PAIR.kalshi_ticker,
            resolved_outcome="yes",
            resolved_at="2026-01-01T00:00:00Z",
            fetched_at="2026-01-01T00:01:00Z",
            raw_json=None,
        )
        state = CollectorStateStore(data_dir=tmp_path, pairs_tracked=2)
        store = ResolutionParquetStore(data_dir=tmp_path)

        live_pairs = await _record_startup_resolutions(
            pairs=(PAIR, live_pair),
            sources=(StaticResolutionSource((record,)),),
            store=store,
            state=state,
        )

        assert live_pairs == (live_pair,)
        assert store.count == 1
        assert state.status.resolutions_collected == 1
        assert all(venue.reconnects == 0 for venue in state.status.venues)

    asyncio.run(run())


def test_resolution_poller_failure_does_not_stop_venue_tasks(tmp_path, caplog) -> None:
    async def run() -> None:
        state = CollectorStateStore(data_dir=tmp_path, pairs_tracked=1)
        stable = StableTask()
        store = ResolutionParquetStore(data_dir=tmp_path)
        poller = ResolutionPoller(
            pairs=(PAIR,),
            sources=(FailingResolutionSource(),),
            store=store,
        )
        orchestrator = CollectorOrchestrator(
            venue_tasks=(stable,),
            resolution_poller=poller,
            state=state,
            resolution_interval_seconds=0.005,
            backoff=BackoffPolicy(initial_seconds=0, max_seconds=0, jitter_seconds=0),
        )

        await orchestrator.run(duration_seconds=0.05)

        assert stable.ticks > 0
        assert "RuntimeError: gamma 500" in caplog.text
        poller_status = next(
            item for item in state.status.venues if item.venue == "resolution_poller"
        )
        assert poller_status.reconnects > 0

    caplog.set_level(logging.WARNING, logger="pmbot.runner.orchestrator")
    asyncio.run(run())


def test_allowlist_loader_skips_bad_rows_with_warning(tmp_path, caplog) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    good_one = allowlist_row("pm-1", "asset-1", "KXONE")
    bad_missing_field = {"polymarket_condition_id": "pm-bad"}
    good_two = allowlist_row("pm-2", "asset-2", "KXTWO")
    allowlist.write_text(
        "\n".join(
            [
                json.dumps(good_one),
                json.dumps(bad_missing_field),
                "{not json",
                json.dumps(good_two),
            ]
        ),
        encoding="utf-8",
    )

    caplog.set_level(logging.WARNING, logger="pmbot.runner.allowlist")
    pairs = load_verified_pairs(allowlist)

    assert [pair.kalshi_ticker for pair in pairs] == ["KXONE", "KXTWO"]
    assert "skipping allowlist row 2" in caplog.text
    assert "skipping allowlist row 3" in caplog.text


def test_status_renders_from_fixture_state() -> None:
    status = CollectorStatus(
        started_at_ns=BASE_TS,
        updated_at_ns=BASE_TS + 60_000_000_000,
        pairs_tracked=2,
        resolutions_collected=3,
        venues=(
            VenueStatus("polymarket", BASE_TS + 55_000_000_000, 120, 1, (BASE_TS,)),
            VenueStatus("kalshi", None, 0, 0, ()),
        ),
    )

    rendered = render_status(status, now_ns=BASE_TS + 60_000_000_000)

    assert "uptime: 1m00s" in rendered
    assert "pairs tracked: 2" in rendered
    assert "resolutions collected: 3" in rendered
    assert "polymarket: freshness=5s ago" in rendered
    assert "kalshi: freshness=never" in rendered


def test_status_handles_corrupt_or_partial_status_file(tmp_path) -> None:
    status_path = tmp_path / STATUS_RELATIVE_PATH
    status_path.parent.mkdir(parents=True)

    status_path.write_text("{not json", encoding="utf-8")
    assert load_status(tmp_path) is None
    assert "unavailable" in render_status(load_status(tmp_path))

    status_path.write_text(json.dumps({"started_at_ns": BASE_TS}), encoding="utf-8")
    assert load_status(tmp_path) is None

    store = CollectorStateStore(data_dir=tmp_path, pairs_tracked=3)
    assert store.status.pairs_tracked == 3


class FailsOnceTask:
    name = "failed"

    def __init__(self) -> None:
        self.runs = 0

    async def run(self) -> None:
        self.runs += 1
        if self.runs == 1:
            raise RuntimeError("boom")
        await asyncio.sleep(0.1)


class StableTask:
    name = "stable"

    def __init__(self) -> None:
        self.ticks = 0

    async def run(self) -> None:
        while True:
            self.ticks += 1
            await asyncio.sleep(0.005)


class FailingResolutionSource:
    async def fetch(self, pairs) -> tuple[ResolutionRecord, ...]:
        del pairs
        raise RuntimeError("gamma 500")


def book_event(
    ts_local_ns: int,
    condition_id: str,
    asset_id: str,
    side: BookSide,
    price: str,
    size: str,
) -> BookEvent:
    return BookEvent(
        ts_local_ns=ts_local_ns,
        ts_monotonic_ns=None,
        ts_exchange=str(ts_local_ns),
        condition_id=condition_id,
        asset_id=asset_id,
        event_type=BookEventType.SNAPSHOT,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        seq_or_hash=f"{asset_id}-{ts_local_ns}",
    )


def trade_event(ts_local_ns: int, trade_id: str) -> TradeEvent:
    return TradeEvent(
        ts_local_ns=ts_local_ns,
        ts_monotonic_ns=None,
        ts_exchange=str(ts_local_ns),
        condition_id="pm-condition",
        asset_id="pm-yes",
        price=Decimal("0.50"),
        size=Decimal("1"),
        taker_side="buy",
        trade_id=trade_id,
    )


def allowlist_row(condition_id: str, asset_id: str, ticker: str) -> dict[str, object]:
    return {
        "polymarket_condition_id": condition_id,
        "polymarket_asset_id": asset_id,
        "kalshi_ticker": ticker,
        "polymarket_yes_payout_condition": "Example wins",
        "kalshi_yes_payout_condition": "Example wins",
        "invert": False,
        "rationale": "fixture",
    }
