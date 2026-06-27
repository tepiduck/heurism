import json
from decimal import Decimal

import pytest

from pmbot.eval.runner import EvalRunConfig, run_eval
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide
from pmbot.runner.models import ResolutionRecord
from pmbot.runner.streams import ResolutionParquetStore

BASE_TS = 1_800_000_000_000_000_000


def test_eval_runner_recomputes_signals_from_raw_books_and_writes_manifest(tmp_path) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    report = tmp_path / "out.json"
    pair = pair_row(
        condition_id="pm-good",
        asset_id="pm-good-yes",
        ticker="KXWCGAME-26JUN25GOOD-GOD",
    )
    allowlist.write_text(json.dumps(pair), encoding="utf-8")
    write_books(
        tmp_path,
        [
            *snapshot(BASE_TS, "pm-good", "pm-good-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25GOOD-GOD",
                "KXWCGAME-26JUN25GOOD-GOD",
                bid="0.79",
                ask="0.81",
            ),
        ],
    )
    write_resolutions(tmp_path, ("kalshi", "KXWCGAME-26JUN25GOOD-GOD", "yes"))

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            bootstrap_samples=10,
            calibration_bins=5,
            fit_regression=True,
        )
    )
    saved = json.loads(report.read_text(encoding="utf-8"))

    assert payload["source"]["data_signals_used"] is False
    assert saved["config_hash"]
    assert saved["n_raw_book_events"] == 4
    assert saved["n_recomputed_signal_rows"] == 1
    assert saved["n_observations"] == 1
    assert saved["distinct_event_count"] == 1
    assert saved["metrics"]["scoring"]["brier_skill"] > 0
    assert saved["metrics"]["regression_fit"]["status"] == "skipped_small_n"
    assert saved["small_n"]["flag"] is True
    assert "PRELIMINARY DIRECTIONAL READ ONLY" in saved["headline"]

    with pytest.raises(FileExistsError):
        run_eval(
            EvalRunConfig(
                data_dir=tmp_path,
                report_path=report,
                allowlist_path=allowlist,
            )
        )


def test_eval_runner_flags_guards_without_silently_dropping_context(tmp_path) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    report = tmp_path / "guarded.json"
    allowlist.write_text(
        "\n".join(
            json.dumps(row)
            for row in (
                pair_row("pm-good", "pm-good-yes", "KXWCGAME-26JUN25GOOD-GOD"),
                pair_row("pm-wide", "pm-wide-yes", "KXWCGAME-26JUN25WIDE-WID"),
                pair_row(
                    "pm-ko",
                    "pm-ko-yes",
                    "KXWCGAME-26JUL03KNOCK-KNO",
                    rationale="knockout round; criteria unverified",
                ),
            )
        ),
        encoding="utf-8",
    )
    write_books(
        tmp_path,
        [
            *snapshot(BASE_TS, "pm-good", "pm-good-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25GOOD-GOD",
                "KXWCGAME-26JUN25GOOD-GOD",
                bid="0.79",
                ask="0.81",
            ),
            *snapshot(BASE_TS, "pm-wide", "pm-wide-yes", bid="0.30", ask="0.60"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25WIDE-WID",
                "KXWCGAME-26JUN25WIDE-WID",
                bid="0.79",
                ask="0.81",
            ),
            *snapshot(BASE_TS, "pm-ko", "pm-ko-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUL03KNOCK-KNO",
                "KXWCGAME-26JUL03KNOCK-KNO",
                bid="0.79",
                ask="0.81",
            ),
        ],
    )
    write_resolutions(
        tmp_path,
        ("kalshi", "KXWCGAME-26JUN25GOOD-GOD", "yes"),
        ("kalshi", "KXWCGAME-26JUN25WIDE-WID", "yes"),
        ("kalshi", "KXWCGAME-26JUL03KNOCK-KNO", "yes"),
    )

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            bootstrap_samples=0,
            max_bid_ask_spread=Decimal("0.05"),
        )
    )

    assert payload["n_observations"] == 1
    assert payload["guards"]["excluded_signal_rows"] == 2
    assert payload["guards"]["reason_counts"]["low_confidence"] == 1
    assert payload["guards"]["reason_counts"]["knockout_stage_unverified_resolution_criteria"] == 1
    excluded = {item["market_id"]: item for item in payload["excluded_markets"]}
    assert excluded["pm-wide"]["guard_reasons"][0].startswith("low_confidence:")
    assert excluded["pm-ko"]["guard_reasons"] == [
        "knockout_stage_unverified_resolution_criteria"
    ]


def write_books(tmp_path, events: list[BookEvent]) -> None:
    recorder = ParquetRecorder(data_dir=tmp_path, buffer_size=100, flush_interval_seconds=999)
    recorder.record_many(events)
    recorder.flush_all()


def write_resolutions(tmp_path, *rows: tuple[str, str, str]) -> None:
    store = ResolutionParquetStore(data_dir=tmp_path)
    store.record_many(
        ResolutionRecord(
            venue=venue,
            market_id=market_id,
            resolved_outcome=outcome,
            resolved_at="2026-06-25T23:00:00Z",
            fetched_at="2026-06-26T00:00:00Z",
            raw_json=None,
        )
        for venue, market_id, outcome in rows
    )


def snapshot(
    ts: int,
    condition_id: str,
    asset_id: str,
    *,
    bid: str,
    ask: str,
) -> list[BookEvent]:
    return [
        book(ts, condition_id, asset_id, BookSide.BID, bid),
        book(ts, condition_id, asset_id, BookSide.ASK, ask),
    ]


def book(
    ts: int,
    condition_id: str,
    asset_id: str,
    side: BookSide,
    price: str,
) -> BookEvent:
    return BookEvent(
        ts_local_ns=ts,
        ts_monotonic_ns=None,
        ts_exchange=str(ts),
        condition_id=condition_id,
        asset_id=asset_id,
        event_type=BookEventType.SNAPSHOT,
        side=side,
        price=Decimal(price),
        size=Decimal("10"),
        seq_or_hash=f"{asset_id}-{ts}",
    )


def pair_row(
    condition_id: str,
    asset_id: str,
    ticker: str,
    *,
    rationale: str = "fixture pair",
) -> dict[str, object]:
    return {
        "polymarket_condition_id": condition_id,
        "polymarket_asset_id": asset_id,
        "kalshi_ticker": ticker,
        "polymarket_yes_payout_condition": f"{ticker} yes",
        "kalshi_yes_payout_condition": f"{ticker} yes",
        "invert": False,
        "rationale": rationale,
    }
