import asyncio
import json
import logging
from decimal import Decimal

import polars as pl

from pmbot.ingestion.clob import BackoffPolicy
from pmbot.ingestion.kalshi import KalshiRecorderAdapter, normalize_market_metadata
from pmbot.ingestion.schemas import MetadataSnapshot
from pmbot.runner.harvest import (
    HarvestRequest,
    HarvestResolutionMarket,
    HarvestResolutionPoller,
    StaticHarvestResolutionSource,
    StatusTrackingRecorder,
    assert_harvest_read_only,
    resolve_harvest_selection,
)
from pmbot.runner.models import ResolutionRecord
from pmbot.runner.orchestrator import CollectorOrchestrator
from pmbot.runner.state import HARVEST_STATUS_RELATIVE_PATH, CollectorStateStore, load_status
from pmbot.runner.streams import ResolutionParquetStore

BASE_TS = 1_800_000_000_000_000_000


def test_harvest_selection_loads_lists_and_group_markets(tmp_path) -> None:
    async def run() -> None:
        polymarket_file = tmp_path / "polymarket.txt"
        polymarket_file.write_text(
            "\n".join(
                [
                    "# comments and commas are accepted",
                    "0xexplicit-condition",
                    "1234567890123456789012345678901234567890, 987654321098765432109876543210",
                ]
            ),
            encoding="utf-8",
        )
        kalshi_file = tmp_path / "kalshi.txt"
        kalshi_file.write_text("KXEXPLICIT-YES\nKXEXPLICIT-NO, KXEXPLICIT-TIE\n", encoding="utf-8")

        selection = await resolve_harvest_selection(
            HarvestRequest(
                polymarket_markets_file=polymarket_file,
                kalshi_tickers_file=kalshi_file,
                kalshi_series=("KXSERIES",),
                polymarket_series=("politics",),
                polymarket_tags=("sports",),
            ),
            gamma=FakeGammaClient(),
            kalshi=FakeKalshiClient(),
        )

        assert selection.polymarket_condition_ids == (
            "0xexplicit-condition",
            "0xgroup-politics",
            "0xgroup-sports",
        )
        assert selection.polymarket_token_ids == (
            "1234567890123456789012345678901234567890",
            "987654321098765432109876543210",
            "pm-group-politics-yes",
            "pm-group-politics-no",
            "pm-group-sports-yes",
            "pm-group-sports-no",
            "pm-explicit-yes",
            "pm-explicit-no",
        )
        assert selection.kalshi_tickers == (
            "KXEXPLICIT-YES",
            "KXEXPLICIT-NO",
            "KXEXPLICIT-TIE",
            "KXSERIES-YES",
        )
        assert selection.market_count == 7
        assert {snapshot.condition_id for snapshot in selection.metadata} == {
            "0xexplicit-condition",
            "0xgroup-politics",
            "0xgroup-sports",
            "KXEXPLICIT-YES",
            "KXEXPLICIT-NO",
            "KXEXPLICIT-TIE",
            "KXSERIES-YES",
        }

    asyncio.run(run())


def test_harvest_records_raw_book_trade_schema_from_mocked_frame(tmp_path) -> None:
    state = CollectorStateStore(
        data_dir=tmp_path,
        pairs_tracked=1,
        relative_path=HARVEST_STATUS_RELATIVE_PATH,
    )
    recorder = StatusTrackingRecorder(
        data_dir=tmp_path,
        state=state,
        venue_by_identifier={"KXTEST-YES": "kalshi"},
        buffer_size=100,
        flush_interval_seconds=999,
    )
    adapter = KalshiRecorderAdapter(recorder)

    adapter.record_payload(
        {
            "type": "orderbook_snapshot",
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST-YES",
                "market_id": "market-id",
                "yes_dollars_fp": [["0.4200", "10.00"]],
                "no_dollars_fp": [["0.5600", "15.00"]],
            },
        },
        ts_local_ns=BASE_TS,
        ts_monotonic_ns=123,
    )
    adapter.record_payload(
        {
            "type": "trade",
            "seq": 2,
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "KXTEST-YES",
                "yes_price_dollars": "0.4200",
                "no_price_dollars": "0.5800",
                "count_fp": "7.00",
                "taker_side": "yes",
                "ts_ms": 1_800_000_000_000,
            },
        },
        ts_local_ns=BASE_TS + 1,
        ts_monotonic_ns=124,
    )
    recorder.flush_all()

    book_frame = pl.read_parquet(list(tmp_path.glob("raw/book/date=*/market=KXTEST-YES/*.parquet")))
    trade_frame = pl.read_parquet(
        list(tmp_path.glob("raw/trade/date=*/market=KXTEST-YES/*.parquet"))
    )
    status = load_status(tmp_path, relative_path=HARVEST_STATUS_RELATIVE_PATH)

    assert book_frame.columns == [
        "ts_local_ns",
        "ts_monotonic_ns",
        "ts_exchange",
        "condition_id",
        "asset_id",
        "event_type",
        "side",
        "price",
        "size",
        "seq_or_hash",
    ]
    assert trade_frame.columns == [
        "ts_local_ns",
        "ts_monotonic_ns",
        "ts_exchange",
        "condition_id",
        "asset_id",
        "price",
        "size",
        "taker_side",
        "trade_id",
    ]
    assert book_frame["price"].to_list() == [Decimal("0.4200000000"), Decimal("0.4400000000")]
    assert trade_frame["price"].to_list() == [Decimal("0.4200000000")]
    assert trade_frame["size"].to_list() == [Decimal("7.0000000000")]
    assert status is not None
    assert next(venue for venue in status.venues if venue.venue == "kalshi").frames_total == 3


def test_harvest_resolution_poller_is_idempotent(tmp_path) -> None:
    async def run() -> None:
        record = ResolutionRecord(
            venue="kalshi",
            market_id="KXTEST-YES",
            resolved_outcome="yes",
            resolved_at="2026-01-01T00:00:00Z",
            fetched_at="2026-01-01T00:01:00Z",
            raw_json=None,
        )
        store = ResolutionParquetStore(data_dir=tmp_path)
        poller = HarvestResolutionPoller(
            markets=(HarvestResolutionMarket(venue="kalshi", market_id="KXTEST-YES"),),
            sources=(StaticHarvestResolutionSource((record, record)),),
            store=store,
        )

        assert await poller.poll_once() == 1
        assert await poller.poll_once() == 0
        assert store.count == 1

    asyncio.run(run())


def test_harvest_supervisor_restarts_failed_venue_without_stopping_other(tmp_path, caplog) -> None:
    async def run() -> None:
        state = CollectorStateStore(
            data_dir=tmp_path,
            pairs_tracked=2,
            relative_path=HARVEST_STATUS_RELATIVE_PATH,
        )
        failed = FailsOnceHarvestTask()
        stable = StableHarvestTask()
        orchestrator = CollectorOrchestrator(
            venue_tasks=(failed, stable),
            state=state,
            backoff=BackoffPolicy(initial_seconds=0, max_seconds=0, jitter_seconds=0),
        )

        await orchestrator.run(duration_seconds=0.05)

        assert failed.runs >= 2
        assert stable.ticks > 0
        assert next(item for item in state.status.venues if item.venue == "kalshi").reconnects == 1
        assert "RuntimeError: kalshi websocket 500" in caplog.text

    caplog.set_level(logging.WARNING, logger="pmbot.runner.orchestrator")
    asyncio.run(run())


def test_harvest_mode_has_no_signal_or_execution_path() -> None:
    assert_harvest_read_only()


class FakeGammaClient:
    async def fetch_events(
        self,
        *,
        series_slug: str | None = None,
        tag_slug: str | None = None,
        active: bool | None = None,
        closed: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        del active, closed, limit
        if offset:
            return []
        slug = series_slug or tag_slug
        assert slug is not None
        return [{"markets": [polymarket_market(f"0xgroup-{slug}", f"pm-group-{slug}")]}]

    async def fetch_all_active_markets(self) -> list[MetadataSnapshot]:
        return [
            MetadataSnapshot(
                fetched_at="2026-01-01T00:00:00Z",
                condition_id="0xexplicit-condition",
                question="Fixture?",
                outcomes=("Yes", "No"),
                token_ids=("pm-explicit-yes", "pm-explicit-no"),
                resolution_or_end_time=None,
                active=True,
                closed=False,
                liquidity=None,
                volume=None,
                raw_json=json.dumps({"conditionId": "0xexplicit-condition"}),
            )
        ]


class FakeKalshiClient:
    async def fetch_markets(
        self,
        *,
        status: str | None = None,
        series_ticker: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, object]], str | None]:
        del limit
        assert status == "open"
        assert series_ticker == "KXSERIES"
        if cursor is not None:
            return [], None
        return [kalshi_market("KXSERIES-YES")], None

    async def fetch_market(self, ticker: str) -> MetadataSnapshot:
        return normalize_market_metadata(kalshi_market(ticker))


class FailsOnceHarvestTask:
    name = "kalshi"

    def __init__(self) -> None:
        self.runs = 0

    async def run(self) -> None:
        self.runs += 1
        if self.runs == 1:
            raise RuntimeError("kalshi websocket 500")
        await asyncio.sleep(0.1)


class StableHarvestTask:
    name = "polymarket"

    def __init__(self) -> None:
        self.ticks = 0

    async def run(self) -> None:
        while True:
            self.ticks += 1
            await asyncio.sleep(0.005)


def polymarket_market(condition_id: str, token_prefix: str) -> dict[str, object]:
    return {
        "conditionId": condition_id,
        "question": "Fixture?",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": [f"{token_prefix}-yes", f"{token_prefix}-no"],
        "active": True,
        "closed": False,
        "liquidity": "10.00",
        "volume": "20.00",
    }


def kalshi_market(ticker: str) -> dict[str, object]:
    return {
        "ticker": ticker,
        "title": "Fixture?",
        "status": "open",
        "expiration_time": "2026-12-31T00:00:00Z",
        "liquidity": "123.45",
        "volume": "678.90",
    }
