import asyncio
from collections.abc import Iterable

import httpx
import polars as pl

from pmbot.ingestion.clob import BackoffPolicy, ClobHttpClient, MarketDataIngestor
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import GapReason


def test_market_data_ingestor_records_fresh_snapshots_and_reconnect_gaps(tmp_path) -> None:
    async def run() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            recorder = ParquetRecorder(
                data_dir=tmp_path,
                buffer_size=100,
                flush_interval_seconds=999,
            )
            ingestor = FailsOnceIngestor(
                token_ids=("asset-a", "asset-b"),
                recorder=recorder,
                clob_http=ClobHttpClient(base_url="https://mock-clob", http=http),
                backoff=BackoffPolicy(initial_seconds=0, max_seconds=0, jitter_seconds=0),
            )

            await ingestor.run(duration_seconds=1)

        gap_files = list((tmp_path / "raw" / "gap-log").glob("date=*/market=*/*.parquet"))
        gap_frame = pl.concat([pl.read_parquet(path) for path in gap_files])
        reconnects = gap_frame.filter(pl.col("reason") == GapReason.RECONNECT.value)

        assert ingestor.run_once_calls == 2
        assert set(reconnects["asset_id"].to_list()) == {"asset-a", "asset-b"}
        assert set(reconnects["condition_id"].to_list()) == {"condition-a", "condition-b"}

    asyncio.run(run())


class FailsOnceIngestor(MarketDataIngestor):
    def __init__(
        self,
        *,
        token_ids: Iterable[str],
        recorder: ParquetRecorder,
        clob_http: ClobHttpClient,
        backoff: BackoffPolicy,
    ) -> None:
        super().__init__(
            token_ids=token_ids,
            recorder=recorder,
            clob_http=clob_http,
            backoff=backoff,
        )
        self.run_once_calls = 0

    async def _run_once(self, *, deadline: float) -> None:
        del deadline
        self.run_once_calls += 1
        if self.run_once_calls == 1:
            raise OSError("simulated websocket disconnect")


def handler(request: httpx.Request) -> httpx.Response:
    assert request.url.path == "/books"
    return httpx.Response(
        200,
        json=[
            {
                "event_type": "book",
                "asset_id": "asset-a",
                "market": "condition-a",
                "bids": [{"price": "0.49", "size": "10"}],
                "asks": [{"price": "0.51", "size": "10"}],
                "timestamp": "1",
            },
            {
                "event_type": "book",
                "asset_id": "asset-b",
                "market": "condition-b",
                "bids": [{"price": "0.39", "size": "8"}],
                "asks": [{"price": "0.61", "size": "8"}],
                "timestamp": "1",
            },
        ],
    )
