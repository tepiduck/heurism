"""Exercise the public starter exactly as a new contributor would run it."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "examples" / "strategy_lab.py"


def run_lab(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def test_offline_starter_reports_fill_costs_inventory_and_marked_pnl(tmp_path: Path) -> None:
    report = tmp_path / "results" / "baseline.json"
    result = run_lab("--report", str(report))

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload == json.loads(report.read_text())
    assert payload["source"] == "synthetic-demo"
    assert len(payload["fills"]) == 1
    assert payload["fills"][0]["price"] == "0.49"
    assert payload["fills"][0]["fee"] == "0.12495"
    assert payload["final_positions"] == {"demo-yes": "10"}
    assert payload["final_marked_pnl"] == "0.27505"
    assert payload["all_marks_valid"] is True


@pytest.mark.parametrize("args", [("--entry-price", "0.48"), ("--latency-ms", "3000")])
def test_no_signal_or_arrival_beyond_replay_produces_no_fills(args: tuple[str, str]) -> None:
    result = run_lab(*args)

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["fills"] == []
    assert payload["final_positions"] == {}
    assert payload["final_marked_pnl"] == "0"


@pytest.mark.parametrize(
    "args",
    [
        ("--entry-price", "NaN"),
        ("--entry-price", "not-a-number"),
        ("--size", "0"),
        ("--size", "Infinity"),
        ("--latency-ms", "-1"),
        ("--data-dir", "missing-recording"),
        ("--data-dir", "missing-recording", "--asset-id", "missing-asset"),
    ],
)
def test_invalid_experiments_fail_with_a_readable_error(args: tuple[str, ...]) -> None:
    result = run_lab(*args)

    assert result.returncode == 2
    assert "error:" in result.stderr
    assert "Traceback" not in result.stderr


def test_existing_report_is_preserved(tmp_path: Path) -> None:
    report = tmp_path / "baseline.json"
    report.write_text("previous experiment\n")

    result = run_lab("--report", str(report))

    assert result.returncode == 2
    assert "report already exists" in result.stderr
    assert report.read_text() == "previous experiment\n"


def test_recorded_books_use_the_supplied_asset_and_surface_an_invalid_mark(tmp_path: Path) -> None:
    book_dir = tmp_path / "raw" / "book"
    book_dir.mkdir(parents=True)
    rows = []
    for ts, event_type, side, price, size in [
        (1_000_000_000, "snapshot", "bid", "0.49", "20"),
        (1_000_000_000, "snapshot", "ask", "0.50", "20"),
        (2_000_000_000, "delta", "bid", "0.49", "0"),
    ]:
        rows.append(
            {
                "ts_local_ns": ts,
                "condition_id": "recorded-condition",
                "asset_id": "recorded-yes",
                "event_type": event_type,
                "side": side,
                "price": price,
                "size": size,
            }
        )
    pl.DataFrame(rows).write_parquet(book_dir / "sample.parquet")

    result = run_lab("--data-dir", str(tmp_path), "--asset-id", "recorded-yes")

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert len(payload["fills"]) == 1
    assert payload["final_positions"] == {"recorded-yes": "10"}
    assert payload["final_marked_pnl"] is None
    assert payload["all_marks_valid"] is False
    assert payload["invalid_mark_count"] == 1
