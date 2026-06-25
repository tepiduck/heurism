import asyncio
from decimal import Decimal

import polars as pl

from pmbot.ingestion.clob import BackoffPolicy
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide
from pmbot.runner.models import CollectorStatus, ResolutionRecord, VenueStatus
from pmbot.runner.orchestrator import CollectorOrchestrator
from pmbot.runner.resolutions import ResolutionPoller, StaticResolutionSource
from pmbot.runner.signal_logging import SignalEventObserver
from pmbot.runner.state import CollectorStateStore, render_status
from pmbot.runner.streams import ResolutionParquetStore, SignalParquetWriter
from pmbot.signals.contracts import VerifiedContractPair

BASE_TS = 1_800_000_000_000_000_000
PAIR = VerifiedContractPair(
    polymarket_condition_id="pm-condition",
    polymarket_asset_id="pm-yes",
    kalshi_ticker="KXTEST",
    rationale="fixture",
)


def test_orchestrator_restarts_failed_venue_without_stopping_other(tmp_path) -> None:
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
        ts_exchange=None,
        condition_id=condition_id,
        asset_id=asset_id,
        event_type=BookEventType.SNAPSHOT,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        seq_or_hash=f"{asset_id}-{ts_local_ns}",
    )
