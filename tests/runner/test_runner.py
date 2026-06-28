import asyncio
import json
import logging
import time
from decimal import Decimal

import polars as pl

import pmbot.runner.state as state_module
from pmbot.ingestion.clob import BackoffPolicy
from pmbot.ingestion.schemas import (
    BookEvent,
    BookEventType,
    BookSide,
    GapEvent,
    GapReason,
    MetadataSnapshot,
    TradeEvent,
)
from pmbot.runner.allowlist import load_verified_pairs
from pmbot.runner.collector import (
    _drop_resolved_live_pairs,
    _record_startup_resolutions,
    _tracked_polymarket_metadata,
)
from pmbot.runner.models import CollectorStatus, ResolutionRecord, VenueStatus
from pmbot.runner.orchestrator import CollectorOrchestrator
from pmbot.runner.pairs import CollectedPairStore, PairRegistry
from pmbot.runner.resolutions import ResolutionPoller, StaticResolutionSource
from pmbot.runner.signal_logging import SignalEventObserver, SignalLoggingRecorder
from pmbot.runner.state import STATUS_RELATIVE_PATH, CollectorStateStore, load_status, render_status
from pmbot.runner.streams import ResolutionParquetStore, SignalParquetWriter
from pmbot.signals.contracts import VerifiedContractPair
from pmbot.signals.cross_venue import CrossVenueSignal

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


def test_orchestrator_preserves_backoff_for_fast_exiting_venue(tmp_path) -> None:
    async def run() -> None:
        state = CollectorStateStore(data_dir=tmp_path, pairs_tracked=1)
        fast = FastExitTask()
        orchestrator = CollectorOrchestrator(
            venue_tasks=(fast,),
            state=state,
            backoff=BackoffPolicy(initial_seconds=0.02, max_seconds=0.02, jitter_seconds=0),
            min_successful_uptime_seconds=30.0,
        )

        await orchestrator.run(duration_seconds=0.09)

        assert 2 <= fast.runs <= 6

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
    observer.compute_pending_signals()
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
    observer.compute_pending_signals()
    recorder.flush_all()

    files = list(tmp_path.glob("signals/date=*/market=pm-condition/*.parquet"))
    frame = pl.read_parquet(files)

    assert len(frame) == 1
    assert frame["kalshi_probability"].to_list() == [Decimal("0.6050000000")]


def test_signal_logging_depth_cap_does_not_change_midpoint(tmp_path) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
    observer = SignalEventObserver(pairs=(PAIR,), signal_writer=writer)
    recorder = SignalLoggingRecorder(
        data_dir=tmp_path,
        observer=observer,
        buffer_size=100,
        flush_interval_seconds=999,
        max_recorded_depth=1,
    )
    recorder.record_many(
        (
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.BID, "0.59", "10"),
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.BID, "0.58", "10"),
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.ASK, "0.62", "10"),
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.ASK, "0.61", "10"),
        )
    )
    recorder.record_many(
        (
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.BID, "0.60", "100"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.BID, "0.59", "100"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.98", "100"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.61", "100"),
        )
    )
    observer.compute_pending_signals()
    recorder.flush_all()

    signal_frame = pl.read_parquet(
        list(tmp_path.glob("signals/date=*/market=pm-condition/*.parquet"))
    )
    pm_frame = pl.read_parquet(
        list(tmp_path.glob("raw/book/date=*/market=pm-condition/*.parquet"))
    )
    kalshi_frame = pl.read_parquet(list(tmp_path.glob("raw/book/date=*/market=KXTEST/*.parquet")))

    assert signal_frame["polymarket_probability"].to_list() == [Decimal("0.6000000000")]
    assert signal_frame["kalshi_probability"].to_list() == [Decimal("0.6050000000")]
    assert pm_frame["price"].to_list() == [Decimal("0.5900000000"), Decimal("0.6100000000")]
    assert kalshi_frame["price"].to_list() == [
        Decimal("0.6000000000"),
        Decimal("0.6100000000"),
    ]


def test_signal_logging_static_prices_write_one_signal_row(tmp_path) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
    observer = SignalEventObserver(
        pairs=(PAIR,),
        signal_writer=writer,
        signal_min_interval_ns=250_000_000,
    )
    recorder = SignalLoggingRecorder(
        data_dir=tmp_path,
        observer=observer,
        buffer_size=1_000,
        flush_interval_seconds=999,
    )
    recorder.record_many(
        (
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.BID, "0.49", "10"),
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.ASK, "0.51", "10"),
        )
    )
    recorder.record_many(
        (
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.BID, "0.69", "10"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.71", "10"),
        )
    )
    observer.compute_pending_signals()

    for index in range(20):
        recorder.record_many(
            (
                book_event(BASE_TS + 2 + index, "KXTEST", "KXTEST", BookSide.BID, "0.69", "10"),
                book_event(BASE_TS + 2 + index, "KXTEST", "KXTEST", BookSide.ASK, "0.71", "10"),
            )
        )
        observer.compute_pending_signals()
    recorder.flush_all()

    files = list(tmp_path.glob("signals/date=*/market=pm-condition/*.parquet"))
    frame = pl.read_parquet(files)

    assert len(frame) == 1
    assert frame["kalshi_probability"].to_list() == [Decimal("0.7000000000")]


def test_signal_logging_preserves_changed_signal_inside_throttle_window(tmp_path) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
    observer = SignalEventObserver(
        pairs=(PAIR,),
        signal_writer=writer,
        signal_min_interval_ns=10_000_000_000,
    )
    recorder = SignalLoggingRecorder(
        data_dir=tmp_path,
        observer=observer,
        buffer_size=1_000,
        flush_interval_seconds=999,
    )
    recorder.record_many(
        (
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.BID, "0.49", "10"),
            book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.ASK, "0.51", "10"),
        )
    )
    recorder.record_many(
        (
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.BID, "0.69", "10"),
            book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.71", "10"),
        )
    )
    observer.compute_pending_signals()
    recorder.record_many(
        (
            book_event(BASE_TS + 2, "KXTEST", "KXTEST", BookSide.BID, "0.70", "10"),
            book_event(BASE_TS + 2, "KXTEST", "KXTEST", BookSide.ASK, "0.72", "10"),
        )
    )
    observer.compute_pending_signals()
    recorder.flush_all()

    files = list(tmp_path.glob("signals/date=*/market=pm-condition/*.parquet"))
    frame = pl.read_parquet(files)

    assert frame["kalshi_probability"].to_list() == [
        Decimal("0.7000000000"),
        Decimal("0.7100000000"),
    ]


def test_high_frame_throughput_does_not_compute_signals_per_event(
    tmp_path,
    monkeypatch,
) -> None:
    async def run() -> None:
        writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=10_000)
        observer = SignalEventObserver(pairs=(PAIR,), signal_writer=writer)
        recorder = SignalLoggingRecorder(
            data_dir=tmp_path,
            observer=observer,
            buffer_size=1_000_000,
            flush_interval_seconds=999,
        )

        def forbidden_compute(
            self: CrossVenueSignal,
            view: object,
        ) -> tuple[object, ...]:
            del self, view
            raise AssertionError("signal compute ran on the ingestion path")

        monkeypatch.setattr(CrossVenueSignal, "compute", forbidden_compute)
        keepalive_ticks = 0
        keepalive_running = True

        async def keepalive() -> None:
            nonlocal keepalive_ticks
            while keepalive_running:
                await asyncio.sleep(0)
                keepalive_ticks += 1

        keepalive_task = asyncio.create_task(keepalive())
        for index in range(3_000):
            ts = BASE_TS + index
            recorder.record_many(
                (
                    book_event(ts, "pm-condition", "pm-yes", BookSide.BID, "0.49", "10"),
                    book_event(ts, "pm-condition", "pm-yes", BookSide.ASK, "0.51", "10"),
                    book_event(ts, "KXTEST", "KXTEST", BookSide.BID, "0.69", "10"),
                    book_event(ts, "KXTEST", "KXTEST", BookSide.ASK, "0.71", "10"),
                )
            )
            if index % 25 == 0:
                await asyncio.sleep(0)
        keepalive_running = False
        await keepalive_task

        assert keepalive_ticks > 10

    asyncio.run(run())


def test_async_signal_compute_does_not_starve_keepalive(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
        observer = SignalEventObserver(pairs=(PAIR,), signal_writer=writer)
        recorder = SignalLoggingRecorder(
            data_dir=tmp_path,
            observer=observer,
            buffer_size=100,
            flush_interval_seconds=999,
        )
        original_compute = CrossVenueSignal.compute

        def slow_compute(self: CrossVenueSignal, view):
            time.sleep(0.03)
            return original_compute(self, view)

        monkeypatch.setattr(CrossVenueSignal, "compute", slow_compute)
        recorder.record_many(
            (
                book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.BID, "0.49", "10"),
                book_event(BASE_TS, "pm-condition", "pm-yes", BookSide.ASK, "0.51", "10"),
                book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.BID, "0.69", "10"),
                book_event(BASE_TS + 1, "KXTEST", "KXTEST", BookSide.ASK, "0.71", "10"),
            )
        )
        keepalive_ticks = 0
        keepalive_running = True

        async def keepalive() -> None:
            nonlocal keepalive_ticks
            while keepalive_running:
                await asyncio.sleep(0.001)
                keepalive_ticks += 1

        keepalive_task = asyncio.create_task(keepalive())
        recorded = await observer.compute_pending_signals_async()
        keepalive_running = False
        await keepalive_task
        observer.flush()

        assert recorded == 1
        assert keepalive_ticks >= 2

    asyncio.run(run())


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


def test_signal_logging_prunes_inactive_pairs_from_periodic_compute(
    tmp_path,
    monkeypatch,
) -> None:
    writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=10_000, flush_interval_seconds=999)
    live_pair = VerifiedContractPair(
        polymarket_condition_id="pm-live",
        polymarket_asset_id="pm-live-yes",
        kalshi_ticker="KXLIVE",
        rationale="fixture",
    )
    resolved_pairs = tuple(
        VerifiedContractPair(
            polymarket_condition_id=f"pm-resolved-{index:02d}",
            polymarket_asset_id=f"pm-resolved-{index:02d}-yes",
            kalshi_ticker=f"KXRESOLVED{index:02d}",
            rationale="fixture",
        )
        for index in range(30)
    )
    observer = SignalEventObserver(
        pairs=(*resolved_pairs, live_pair),
        signal_writer=writer,
    )
    for index, pair in enumerate((*resolved_pairs, live_pair)):
        ts = BASE_TS + index
        observer.observe_many(
            (
                book_event(
                    ts,
                    pair.polymarket_condition_id,
                    pair.polymarket_asset_id,
                    BookSide.BID,
                    "0.49",
                    "10",
                ),
                book_event(
                    ts,
                    pair.polymarket_condition_id,
                    pair.polymarket_asset_id,
                    BookSide.ASK,
                    "0.51",
                    "10",
                ),
                book_event(ts, pair.kalshi_ticker, pair.kalshi_ticker, BookSide.BID, "0.69", "10"),
                book_event(ts, pair.kalshi_ticker, pair.kalshi_ticker, BookSide.ASK, "0.71", "10"),
            )
        )
    seen_pair_keys: list[tuple[str, str, str]] = []
    original_compute = CrossVenueSignal.compute

    def recording_compute(self: CrossVenueSignal, view):
        seen_pair_keys.extend(
            (
                pair.polymarket_condition_id,
                pair.polymarket_asset_id,
                pair.kalshi_ticker,
            )
            for pair in self._pairs
        )
        return original_compute(self, view)

    monkeypatch.setattr(CrossVenueSignal, "compute", recording_compute)

    observer.set_pairs((live_pair,))
    assert observer.compute_pending_signals() == 1

    assert seen_pair_keys == [
        (
            live_pair.polymarket_condition_id,
            live_pair.polymarket_asset_id,
            live_pair.kalshi_ticker,
        )
    ]


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


def test_polymarket_metadata_filter_keeps_only_tracked_markets() -> None:
    snapshots = (
        runner_metadata("pm-condition"),
        runner_metadata("scanned-only-condition"),
    )

    assert _tracked_polymarket_metadata(snapshots, (PAIR,)) == (snapshots[0],)


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


def test_resolution_poller_drops_resolved_pairs_from_live_registry_but_keeps_eval_store(
    tmp_path,
) -> None:
    async def run() -> None:
        live_pair = VerifiedContractPair(
            polymarket_condition_id="pm-live",
            polymarket_asset_id="pm-live-yes",
            kalshi_ticker="KXLIVE",
            rationale="fixture",
        )
        resolved_pairs = tuple(
            VerifiedContractPair(
                polymarket_condition_id=f"pm-resolved-{index:02d}",
                polymarket_asset_id=f"pm-resolved-{index:02d}-yes",
                kalshi_ticker=f"KXRESOLVED{index:02d}",
                rationale="fixture",
            )
            for index in range(50)
        )
        all_pairs = (*resolved_pairs, live_pair)
        registry = PairRegistry(all_pairs)
        pair_store = CollectedPairStore(data_dir=tmp_path)
        pair_store.save(all_pairs)
        state = CollectorStateStore(data_dir=tmp_path, pairs_tracked=len(all_pairs))
        writer = SignalParquetWriter(data_dir=tmp_path, buffer_size=10_000)
        observer = SignalEventObserver(pairs=all_pairs, signal_writer=writer, state=state)
        records = tuple(
            ResolutionRecord(
                venue="kalshi",
                market_id=pair.kalshi_ticker,
                resolved_outcome="yes",
                resolved_at="2026-01-01T00:00:00Z",
                fetched_at="2026-01-01T00:01:00Z",
                raw_json=None,
            )
            for pair in resolved_pairs
        )
        store = ResolutionParquetStore(data_dir=tmp_path)
        poller = ResolutionPoller(
            pairs=registry.snapshot,
            sources=(StaticResolutionSource(records),),
            store=store,
            state=state,
            on_resolved=lambda fetched: _drop_resolved_live_pairs(
                records=fetched,
                registry=registry,
                observer=observer,
                state=state,
            ),
        )

        assert await poller.poll_once() == len(resolved_pairs)

        venues = {venue.venue: venue for venue in state.status.venues}
        assert registry.snapshot() == (live_pair,)
        assert live_pair in pair_store.load()
        assert all(pair in pair_store.load() for pair in resolved_pairs)
        assert state.status.pairs_tracked == 1
        assert all(venue.reconnects == 0 for venue in venues.values())
        assert all(not venue.gap_timestamps_ns for venue in venues.values())

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


def test_state_store_resets_started_at_for_new_process(tmp_path, monkeypatch) -> None:
    status_path = tmp_path / STATUS_RELATIVE_PATH
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        json.dumps(
            {
                "started_at_ns": BASE_TS - 3_600_000_000_000,
                "updated_at_ns": BASE_TS - 1_000_000_000,
                "pairs_tracked": 1,
                "resolutions_collected": 7,
                "venues": [
                    {
                        "venue": "polymarket",
                        "last_frame_ns": BASE_TS - 5_000_000_000,
                        "frames_total": 42,
                        "reconnects": 2,
                        "gap_timestamps_ns": [BASE_TS - 10_000_000_000],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    times = iter((BASE_TS, BASE_TS + 1, BASE_TS + 2))
    monkeypatch.setattr(state_module.time, "time_ns", lambda: next(times))

    store = CollectorStateStore(data_dir=tmp_path, pairs_tracked=3)
    persisted = load_status(tmp_path)

    assert store.status.started_at_ns == BASE_TS
    assert store.status.updated_at_ns == BASE_TS + 1
    assert store.status.pairs_tracked == 3
    assert store.status.resolutions_collected == 7
    assert store.status.venues[0].frames_total == 42
    assert persisted is not None
    assert persisted.started_at_ns == BASE_TS
    assert "uptime: 1m00s" in render_status(persisted, now_ns=BASE_TS + 60_000_000_000)


class FailsOnceTask:
    name = "failed"

    def __init__(self) -> None:
        self.runs = 0

    async def run(self) -> None:
        self.runs += 1
        if self.runs == 1:
            raise RuntimeError("boom")
        await asyncio.sleep(0.1)


class FastExitTask:
    name = "fast"

    def __init__(self) -> None:
        self.runs = 0

    async def run(self) -> None:
        self.runs += 1


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


def runner_metadata(condition_id: str) -> MetadataSnapshot:
    return MetadataSnapshot(
        fetched_at="2026-06-24T00:00:00Z",
        condition_id=condition_id,
        question="Fixture?",
        outcomes=("Yes", "No"),
        token_ids=("yes", "no"),
        resolution_or_end_time=None,
        active=True,
        closed=False,
        liquidity=None,
        volume=None,
        raw_json=json.dumps({"conditionId": condition_id}),
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
