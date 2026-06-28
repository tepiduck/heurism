import asyncio
from collections.abc import Iterable
from decimal import Decimal

import httpx
import polars as pl

from pmbot.ingestion.clob import BackoffPolicy, ClobHttpClient, MarketDataIngestor
from pmbot.ingestion.parsers import parse_ws_frame
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


def test_polymarket_extreme_precision_book_frame_records_without_decimal_scale_error(
    tmp_path,
) -> None:
    frame = """
    {
      "event_type": "book",
      "asset_id": "asset-precision",
      "market": "condition-precision",
      "bids": [
        {
          "price": "0.123456789012345678",
          "size": "123.123456789012345678"
        }
      ],
      "asks": [
        {
          "price": "0.000000000000000000000000000000000000001",
          "size": "0.000000000000000000000000000000000000001"
        }
      ],
      "timestamp": "1"
    }
    """
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
    )

    recorder.record_many(parse_ws_frame(frame, ts_local_ns=1, ts_monotonic_ns=2))
    recorder.flush_all()

    book_frame = pl.read_parquet(
        list(tmp_path.glob("raw/book/date=*/market=condition-precision/*.parquet"))
    ).sort("side")

    assert book_frame.schema["price"] == pl.Decimal(38, 12)
    assert book_frame.schema["size"] == pl.Decimal(38, 12)
    assert book_frame["price"].to_list() == [
        Decimal("0E-12"),
        Decimal("0.123456789012"),
    ]
    assert book_frame["size"].to_list() == [
        Decimal("0E-12"),
        Decimal("123.123456789012"),
    ]


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
                "bids": [
                    {
                        "price": "0.000000000000000000000000000000000000001",
                        "size": "0.000000000000000000000000000000000000001",
                    }
                ],
                "asks": [{"price": "0.510000000000000001", "size": "10.123456789012345678"}],
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
