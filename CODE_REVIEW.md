# Code Review — full pass before MacBook deployment

Reviewed by parallel audits of ingestion, backtest internals, eval, and runner. Headline:

- ✅ **Read-only safety CONFIRMED.** No order/trade/sign/wallet path is reachable from the collector. `src/pmbot/execution/__init__.py` is an empty stub; nothing imports it. The collector cannot trade.
- ✅ **Eval verdict logic is HONEST.** The headline Brier-skill baseline is computed from the real market price (not from p̂), and the train/test sweep has no leakage in code. The two highest-risk "fake edge" paths are clean.
- ❌ **One CRITICAL bug crashes the live recorder on connect.** Plus HIGH runner bugs that break 24/7 operation, and data-fidelity bugs that would silently poison backtests.

Fix in tiers. **Tier 1 must land before deploying tonight.**

---

## TIER 1 — Deploy blockers (collector won't survive without these)

**T1.1 — CRITICAL: `NameError` crashes the CLOB recorder on every connect/reconnect.**
`src/pmbot/ingestion/clob.py:~210` — inside `_record_fresh_snapshots`, `asset_id=token_id` references `token_id`, which is never bound in that scope. `_record_fresh_snapshots` is awaited at the top of `run()` before the recv loop, so the **first** call throws, and the `except ConnectionClosed/OSError` handlers don't catch `NameError` → the ingestor dies. The live CLOB path is non-functional as written. This is also why tonight's `pmbot ingest` network test would fail confusingly. **No test exercises `MarketDataIngestor.run` / `_record_fresh_snapshots` / reconnect at all** — add one.

**T1.2 — HIGH: unbounded memory + O(n²) CPU in signal logging → OOM over a multi-week run.**
`src/pmbot/runner/signal_logging.py:32-33,53,70-80` — `_events` and `_trades` are lists that only ever grow, and `_log_signals` rebuilds a `PointInTimeView` with `tuple(self._events)` / `tuple(self._trades)` on **every** event, copying the entire history each time. Over weeks this OOMs the laptop and slows to a crawl. Window/prune the history (the signal only needs recent context + the current book), and stop re-copying full history per event.

**T1.3 — HIGH: one transient HTTP error kills the whole collector silently.**
`src/pmbot/runner/orchestrator.py:46,73-82` — the resolution poller (`_run_resolution_poller`) has no try/except, and `run()` uses `asyncio.gather(*tasks)` without `return_exceptions=True`. A single Gamma/Kalshi 500 propagates and tears down the venue tasks too — the process just exits. Supervise the poller like the venue tasks (catch, log, backoff, restart), and/or use `return_exceptions=True` with per-task restart.

**T1.4 — HIGH: one malformed allowlist row aborts startup with a bare traceback.**
`src/pmbot/runner/allowlist.py:28-31,48-63` — a single bad JSONL line or inconsistent row raises and prevents the collector from starting, with no logging. Decide intentionally: skip-bad-row-with-warning (preferred for unattended) and log which row/why, OR keep fail-fast but emit a clear message naming the bad row.

**T1.5 — MEDIUM: swallowed exceptions leave you blind.**
`src/pmbot/runner/orchestrator.py:63-65` — venue exceptions are discarded (`except Exception: attempt += 1`), so `pmbot status` shows `reconnects=900, frames/min=0` with no reason. Log the exception type/message on each reconnect so a real failure (auth change, schema break, disk full) is diagnosable.

**T1.6 — MEDIUM: `pmbot status` crashes on a corrupt/partial status file.**
`src/pmbot/runner/state.py:131-154,161` and `cli.py:88-92` — `load_status` handles missing/non-dict but not malformed JSON or missing keys → `JSONDecodeError`/`KeyError` traceback instead of the friendly "unavailable". Wrap in try/except → return `None`. (Writes are already atomic via tmp+rename — good.)

---

## TIER 2 — Data fidelity (fix before trusting ANY backtest/verdict; these silently corrupt results)

**T2.1 — HIGH: Kalshi order-book sizes are likely wrong (unit mismatch).**
`src/pmbot/ingestion/kalshi.py:255-262,280` — snapshot levels parse `*_dollars_fp` sizes, but deltas apply `delta_fp` (integer contract counts) to those levels — different units combined as if the same. Validate against a REAL Kalshi frame and fix the unit handling. Currently silently records wrong sizes.

**T2.2 — HIGH: Kalshi has no gap/sequence/checksum detection.**
`src/pmbot/ingestion/kalshi.py` — carries `seq` but never compares consecutive values or emits a `GapEvent`. A dropped delta leaves the reconstructed book permanently wrong with no marker telling the backtester to distrust the span. Mirror the CLOB processor's gap detection.

**T2.3 — CRITICAL (backtest PnL): missing-side liquidation mark folds `None`→0 instead of invalidating.**
`src/pmbot/backtest/book.py:106-111` + `engine.py:234` — for an open position when the relevant book side is empty, `mark_price` returns `None`, and `engine.py:234` does `mtm += position * (mark_price or Decimal("0"))` → marks the position to **0** silently. A short with no ask marks to 0 = **flatters PnL**; neither branch sets `mark_valid = False`. Fix: when the liquidation side is missing for a nonzero position, set `mark_valid = False` (like gap spans), never fold `None` to 0. Same pattern for MID mode.

**T2.4 — HIGH: marketable-then-rest remainder gets `queue_ahead=0` (front of queue) → optimistic fills.**
`src/pmbot/backtest/book.py:53-58` + `engine.py:267` — when a marketable order partially fills and rests the remainder at a price that improved the book, `queue_ahead` returns 0, modeling the remainder as front-of-queue. Optimistic. Capture queue position conservatively for the post-cross remainder.

**T2.5 — Retire the real-frame xfail.** Capture ~30s of REAL Polymarket + Kalshi frames (off-SWG) and freeze as fixtures. This validates the parsers, the checksum/sequence logic, and T2.1 against actual venue semantics — all currently unverified against real data.

---

## TIER 3 — Eval hardening (verdict integrity; core logic already good)

**T3.1 — Add a real no-leakage regression test.** `tests/eval/test_attribution_sweep.py` checks the selected param is train-best but does NOT assert that test data can't change the selection. Construct data where test-best ≠ train-best and assert `selected` follows train regardless of test. (Code is correct; guard it.)

**T3.2 — Range-validate probabilities at the scoring boundary.** `scoring.py` — `brier_score` doesn't clamp/validate `0≤p≤1` (log_loss does). A bad upstream forecast (>1) silently corrupts the headline Brier instead of erroring. Validate at the public scoring entry points.

**T3.3 — Headline must respect `small_n`.** `report.py:57` prints "POSITIVE BRIER SKILL" even at n=3. Gate the positive-edge headline on `small_n` so a tiny-sample fluke doesn't read as confirmed edge.

**T3.4 — Minor:** sparse-bin bootstrap bands understate uncertainty (calibration.py:111-130); CLV ignores the `invert_kalshi` flag (clv.py); document Sharpe's √n basis. Low priority.

---

## Recommended order
1. **Tier 1** (so tonight's deployment survives) → capture real frames (T2.5) → deploy collector, start recording.
2. **Tier 2** data-fidelity fixes in parallel while data accumulates — must be green before WP-4 produces a verdict you trust.
3. **Tier 3** before WP-5 sizing.
