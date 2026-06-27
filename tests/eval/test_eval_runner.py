import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pmbot.eval.runner import EvalRunConfig, run_eval
from pmbot.ingestion.recorder import ParquetRecorder
from pmbot.ingestion.schemas import BookEvent, BookEventType, BookSide, MetadataSnapshot
from pmbot.runner.models import ResolutionRecord
from pmbot.runner.pairs import CollectedPairStore
from pmbot.runner.streams import ResolutionParquetStore
from pmbot.signals.contracts import VerifiedContractPair

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
            *snapshot(BASE_TS - 100, "pm-good", "pm-good-yes", bid="0.10", ask="0.20"),
            *snapshot(BASE_TS, "pm-good", "pm-good-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25GOOD-GOD",
                "KXWCGAME-26JUN25GOOD-GOD",
                bid="0.79",
                ask="0.81",
            ),
            *snapshot(
                BASE_TS + 100,
                "KXWCGAME-26JUN25GOOD-GOD",
                "KXWCGAME-26JUN25GOOD-GOD",
                bid="0.10",
                ask="0.20",
            ),
        ],
    )
    write_resolutions(tmp_path, ("kalshi", "KXWCGAME-26JUN25GOOD-GOD", "yes"))

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            start_ns=BASE_TS,
            end_ns=BASE_TS + 1,
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


def test_eval_runner_writes_diagnostics_when_no_observations_pass(tmp_path) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    report = tmp_path / "diagnostic.json"
    allowlist.write_text(
        json.dumps(pair_row("pm-unresolved", "pm-unresolved-yes", "KXWCGAME-26JUN25MISS-MIS")),
        encoding="utf-8",
    )
    write_books(
        tmp_path,
        [
            *snapshot(BASE_TS, "pm-unresolved", "pm-unresolved-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25MISS-MIS",
                "KXWCGAME-26JUN25MISS-MIS",
                bid="0.59",
                ask="0.61",
            ),
        ],
    )

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            bootstrap_samples=0,
        )
    )
    saved = json.loads(report.read_text(encoding="utf-8"))

    assert payload["diagnostic"] is True
    assert saved["n_recomputed_signal_rows"] == 1
    assert saved["n_observations"] == 0
    assert saved["guards"]["reason_counts"] == {"unresolved": 1}
    diagnostic = saved["pair_diagnostics"][0]
    assert diagnostic["signal_samples"] == 1
    assert diagnostic["resolution_found"] is False
    assert diagnostic["resolution_keys_tried"] == [
        {"venue": "polymarket", "market_id": "pm-unresolved"},
        {"venue": "kalshi", "market_id": "KXWCGAME-26JUN25MISS-MIS"},
    ]


def test_eval_runner_scores_metadata_discovered_collected_pairs(tmp_path) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    report = tmp_path / "metadata.json"
    allowlist.write_text("", encoding="utf-8")
    write_metadata(
        tmp_path,
        polymarket_metadata(
            condition_id="pm-discovered",
            yes_token="pm-discovered-yes",
            no_token="pm-discovered-no",
        ),
        kalshi_metadata("KXWCGAME-26JUN25ABCDEF-ABC"),
    )
    write_books(
        tmp_path,
        [
            *snapshot(BASE_TS, "pm-discovered", "pm-discovered-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25ABCDEF-ABC",
                "KXWCGAME-26JUN25ABCDEF-ABC",
                bid="0.59",
                ask="0.61",
            ),
        ],
    )
    write_resolutions(tmp_path, ("kalshi", "KXWCGAME-26JUN25ABCDEF-ABC", "yes"))

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            bootstrap_samples=0,
        )
    )

    assert payload["n_observations"] == 1
    assert payload["source"]["pair_source"]["static_pairs_loaded"] == 0
    assert payload["source"]["pair_source"]["recorded_metadata_pairs_loaded"] == 1
    assert payload["source"]["pair_source"]["raw_history_pairs"] == 1
    assert payload["source"]["pair_source"]["scoreable_pairs_selected"] == 1


def test_eval_runner_scores_settled_pair_from_historical_metadata(tmp_path) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    report = tmp_path / "settled-metadata.json"
    allowlist.write_text("", encoding="utf-8")
    write_metadata(
        tmp_path,
        polymarket_metadata(
            condition_id="pm-settled",
            yes_token="pm-settled-yes",
            no_token="pm-settled-no",
            closed=True,
            resolved_outcome="yes",
        ),
        kalshi_metadata(
            "KXWCGAME-26JUN25ABCDEF-ABC",
            status="settled",
            expiration_value="Alfa",
        ),
    )
    write_books(
        tmp_path,
        [
            *snapshot(BASE_TS, "pm-settled", "pm-settled-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25ABCDEF-ABC",
                "KXWCGAME-26JUN25ABCDEF-ABC",
                bid="0.59",
                ask="0.61",
            ),
        ],
    )
    write_resolutions(tmp_path, ("kalshi", "KXWCGAME-26JUN25ABCDEF-ABC", "yes"))

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            bootstrap_samples=0,
        )
    )

    assert payload["n_observations"] == 1
    assert payload["source"]["pair_source"]["recorded_metadata"]["live_pairs_matched"] == 0
    assert payload["source"]["pair_source"]["recorded_metadata"]["historical_pairs_matched"] == 1
    assert payload["source"]["pair_source"]["scoreable_pairs_selected"] == 1


def test_eval_runner_scores_collector_persisted_pairs(tmp_path) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    report = tmp_path / "stored.json"
    allowlist.write_text("", encoding="utf-8")
    stored_pair = VerifiedContractPair(
        polymarket_condition_id="pm-stored",
        polymarket_asset_id="pm-stored-yes",
        polymarket_no_asset_id="pm-stored-no",
        kalshi_ticker="KXWCGAME-26JUN25STORED-STR",
        rationale="collector discovered pair",
    )
    CollectedPairStore(data_dir=tmp_path).save((stored_pair,))
    write_books(
        tmp_path,
        [
            *snapshot(BASE_TS, "pm-stored", "pm-stored-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25STORED-STR",
                "KXWCGAME-26JUN25STORED-STR",
                bid="0.59",
                ask="0.61",
            ),
        ],
    )
    write_resolutions(tmp_path, ("kalshi", "KXWCGAME-26JUN25STORED-STR", "yes"))

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            bootstrap_samples=0,
        )
    )

    assert payload["n_observations"] == 1
    assert payload["n_scoreable_pairs"] == 1
    assert payload["source"]["pair_source"]["collector_pair_store_pairs_loaded"] == 1
    assert payload["source"]["pair_source"]["selected_pair_sources"] == {
        "collector_verified_pair_store": 1
    }


def test_eval_runner_prefers_scoreable_history_over_unsettled_live_pairs(tmp_path) -> None:
    allowlist = tmp_path / "pairs.jsonl"
    report = tmp_path / "scoreable-only.json"
    allowlist.write_text("", encoding="utf-8")
    settled_pair = VerifiedContractPair(
        polymarket_condition_id="pm-settled",
        polymarket_asset_id="pm-settled-yes",
        kalshi_ticker="KXWCGAME-26JUN25SETTLED-SET",
        rationale="historical discovered pair",
    )
    live_pair = VerifiedContractPair(
        polymarket_condition_id="pm-live",
        polymarket_asset_id="pm-live-yes",
        kalshi_ticker="KXWCGAME-26JUN27LIVE-LIV",
        rationale="current discovered pair",
    )
    CollectedPairStore(data_dir=tmp_path).save((settled_pair, live_pair))
    write_books(
        tmp_path,
        [
            *snapshot(BASE_TS, "pm-settled", "pm-settled-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN25SETTLED-SET",
                "KXWCGAME-26JUN25SETTLED-SET",
                bid="0.59",
                ask="0.61",
            ),
            *snapshot(BASE_TS, "pm-live", "pm-live-yes", bid="0.49", ask="0.51"),
            *snapshot(
                BASE_TS + 1,
                "KXWCGAME-26JUN27LIVE-LIV",
                "KXWCGAME-26JUN27LIVE-LIV",
                bid="0.69",
                ask="0.71",
            ),
        ],
    )
    write_resolutions(
        tmp_path,
        ("kalshi", "KXWCGAME-26JUN25SETTLED-SET", "yes"),
        ("kalshi", "KXWCGAME-26JUN27LIVE-LIV", ""),
    )

    payload = run_eval(
        EvalRunConfig(
            data_dir=tmp_path,
            report_path=report,
            allowlist_path=allowlist,
            bootstrap_samples=0,
        )
    )

    assert payload["n_pairs_loaded"] == 1
    assert payload["n_scoreable_pairs"] == 1
    assert payload["n_observations"] == 1
    assert payload["source"]["pair_source"]["raw_history_pairs"] == 2
    assert payload["source"]["pair_source"]["scoreable_markets"] == [
        {
            "polymarket_condition_id": "pm-settled",
            "polymarket_asset_id": "pm-settled-yes",
            "kalshi_ticker": "KXWCGAME-26JUN25SETTLED-SET",
        }
    ]


def write_books(tmp_path, events: list[BookEvent]) -> None:
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
        partition_date=lambda: datetime.fromtimestamp(BASE_TS / 1_000_000_000, UTC).date(),
    )
    recorder.record_many(events)
    recorder.flush_all()


def write_metadata(tmp_path, *snapshots: MetadataSnapshot) -> None:
    recorder = ParquetRecorder(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
        partition_date=lambda: datetime.fromtimestamp(BASE_TS / 1_000_000_000, UTC).date(),
    )
    recorder.record_many(snapshots)
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


def polymarket_metadata(
    *,
    condition_id: str,
    yes_token: str,
    no_token: str,
    closed: bool = False,
    resolved_outcome: str | None = None,
) -> MetadataSnapshot:
    raw = {
        "conditionId": condition_id,
        "question": "Will Alfa win on 2026-06-25?",
        "outcomes": ["Yes", "No"],
        "clobTokenIds": [yes_token, no_token],
        "active": not closed,
        "closed": closed,
        "acceptingOrders": not closed,
        "slug": "fifwc-abc-def-2026-06-25-abc",
        "__event_slug": "fifwc-abc-def-2026-06-25",
        "__event_title": "Alfa vs Delta Winner?",
    }
    if resolved_outcome is not None:
        raw["resolved_outcome"] = resolved_outcome
    return MetadataSnapshot(
        fetched_at="2026-06-25T12:00:00Z",
        condition_id=condition_id,
        question=str(raw["question"]),
        outcomes=("Yes", "No"),
        token_ids=(yes_token, no_token),
        resolution_or_end_time=None,
        active=not closed,
        closed=closed,
        liquidity=None,
        volume=None,
        raw_json=json.dumps(raw),
    )


def kalshi_metadata(
    ticker: str,
    *,
    status: str = "open",
    expiration_value: str = "",
) -> MetadataSnapshot:
    raw = {
        "ticker": ticker,
        "title": "Alfa vs Delta Winner?",
        "yes_sub_title": "Reg Time: Alfa",
        "status": status,
        "expiration_time": "2026-06-25T23:00:00Z",
        "expiration_value": expiration_value,
    }
    return MetadataSnapshot(
        fetched_at="2026-06-25T12:00:00Z",
        condition_id=ticker,
        question=str(raw["title"]),
        outcomes=("yes", "no"),
        token_ids=(ticker,),
        resolution_or_end_time=None,
        active=status in {"open", "active"},
        closed=status not in {"open", "active"},
        liquidity=None,
        volume=None,
        raw_json=json.dumps(raw),
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
