# Polymarket Microstructure Bot — Engineering Plan

**Chief engineer:** Claude · **Implementer:** Codex · **Owner:** Jeffrey
**Stack:** Python 3.11+ · **Edge thesis:** microstructure (order-book imbalance, microprice, cross-market mispricing)
**Mode:** real-money, validation-gated. The system trades live capital. The only gate is the owner's own go-signal: flip `MODE=live LIVE=1` against a funded wallet and it places real orders. The backtest/paper run exists to confirm the signal makes money *before* you fund it — that's capital protection, not a delay.

---

## Edge thesis (what we're actually betting on)

The market price already encodes the crowd's probability. We only profit when we estimate the *next* price better than the book does, faster than it reprices. Our edge is engineering, not exotic ML:

1. **Order-book imbalance (OBI)** predicts short-horizon price drift.
2. **Microprice** (depth-weighted mid) is a better fair-value estimate than the mid.
3. **Cross-market mispricing** — correlated or logically-linked markets that drift out of arbitrage (e.g. "Candidate X wins" vs "Party Y wins", or mutually-exclusive outcomes summing ≠ 1).

If we can't beat the book in a clean event-driven backtest with realistic fills + fees + latency, we do not deploy a dollar. That gate is non-negotiable.

---

## Architecture

```
ingestion (CLOB websocket + Gamma REST)
    → recorder (Parquet/DuckDB time series)
        → feature engine (OBI, microprice, spread, cross-market)
            → signal/model
                → backtester (event-driven replay, realistic fills)
                    → calibration + eval (Brier, reliability, PnL, Sharpe)
                        → risk + sizing (fractional Kelly, limits, kill-switch)
                            → execution (paper | live behind flag)
                                → live loop + monitoring
```

## APIs
- **Gamma API** — market metadata, resolution rules, market discovery.
- **CLOB API** — REST for snapshots/orders; **websocket** for L2 book + trade stream.
- **Execution** — CLOB orders are EIP-712 signed; needs wallet key handling (kept out of repo, env-only).

---

## Work-package sequence (each = one Codex task with acceptance criteria)

> Methodology corrections from the literature review live in `STRATEGY.md` (v2). Where that doc and this list disagree, STRATEGY.md wins. Key deltas already folded into WP-3/4/5 below.

- **WP-0 — Scaffold:** ✅ DONE (reviewed, approved). Repo layout, config, secrets, env, lint/test/CI, typed settings. Follow-up required: add explicit tests for the live-mode safety gate (see WP-0 review note below).
- **WP-1 — Ingestion + recorder:** websocket L2 book + trades + Gamma metadata → Parquet, with reconnect/gap-detection. Venue-agnostic recorder so a sharper reference venue can plug in later. ← CURRENT
- **WP-2 — Backtester:** event-driven replay over recorded data; realistic fills, **fees + gas**, configurable latency. Every downstream metric is meaningless without correct cost modeling.
- **WP-3 — Feature library + first signal:** OBI, microprice, spread, depth. **First signal is cross-venue deviation** (Polymarket vs a sharper reference), blended on the **log-odds scale + extremizing**, with a **double-counting guard** (update only on the orthogonal/private component). OBI→drift is research-only, expected to fail net of fees. *(Corrected from v1, which led with OBI.)*
- **WP-4 — Eval:** **primary = proper scoring rules (Brier, log-loss) over many resolved markets + calibration curves + implementation shortfall.** Cross-venue CLV is a **secondary** diagnostic only (no sharp close on Polymarket; inefficiency rises near resolution). *(Corrected from v1, which made CLV primary.)*
- **WP-5 — Risk + sizing:** **fractional Kelly (≤¼ haircut), simultaneous/vector Kelly with covariance, opportunity-cost-adjusted edge for locked capital**, per-market & aggregate position limits, capacity cap, correlation cap, drawdown kill-switch.
- **WP-6 — Execution layer:** ← FAST-TRACKED, builds in parallel with WP-2. CLOB order signing/placement; `paper` and `live` adapters behind one interface. `live` enabled by `MODE=live LIVE=1`. Built so that the moment the signal validates, the trade path is already done — owner funds the wallet and flips the switch.
- **WP-7 — Live loop + monitoring:** orchestrate ingest→signal→size→execute; logging, alerting, heartbeat, panic-flatten.

**Rule for Codex on every WP:** ship tests, type hints, and a runnable `make`/CLI entrypoint. No WP is "done" until its acceptance criteria pass and tests are green.

**Parallel track:** WP-6 (execution) has no dependency on the backtester or signal — it only needs WP-1's market plumbing. Build it alongside WP-2 so the live trade path is ready the instant the signal validates. Critical-path to first live trade: WP-1 → WP-3 (signal) → WP-5 (sizing) → WP-6 already done → fund + flip. WP-2/WP-4 run in parallel and are what tell you whether to flip.

---

## WP-0 — Repo scaffold (paste this to Codex first)

> **Task:** Scaffold a Python 3.11 project named `pmbot` for a Polymarket microstructure trading research+execution system. Do NOT implement trading logic yet — scaffolding only.
>
> **Deliverables:**
> - `pyproject.toml` using `uv` (or `poetry`), Python 3.11, deps: `httpx`, `websockets`, `polars`, `duckdb`, `pydantic`, `pydantic-settings`, `typer`, `rich`, `pytest`, `ruff`, `mypy`.
> - Package layout:
>   ```
>   src/pmbot/
>     config.py        # pydantic-settings, reads .env, NEVER hardcodes secrets
>     ingestion/
>     features/
>     backtest/
>     signals/
>     risk/
>     execution/
>     cli.py           # typer app, `pmbot --help`
>   tests/
>   data/              # gitignored, parquet lands here
>   ```
> - `config.py`: typed settings for `CLOB_HTTP_URL`, `CLOB_WS_URL`, `GAMMA_URL`, `WALLET_PRIVATE_KEY` (optional, env-only), `MODE` (`paper`|`live`, default `paper`), `DATA_DIR`. Reject `MODE=live` unless `LIVE=1` env is also set.
> - `.env.example` (no real values), `.gitignore` (ignores `.env`, `data/`, `__pycache__`).
> - `ruff` + `mypy` config, a `Makefile` (or `justfile`) with `lint`, `typecheck`, `test`, `run` targets.
> - GitHub Actions CI running lint + typecheck + tests.
> - One smoke test asserting `pmbot.config.Settings` loads and defaults to `MODE=paper`.
>
> **Acceptance criteria:**
> - `make lint typecheck test` passes clean.
> - `pmbot --help` runs.
> - No secret values committed anywhere; `MODE` defaults to `paper` and cannot be `live` without `LIVE=1`.
> - Confirm with the directory tree and the contents of `config.py` before adding any other code.

---

## WP-0 — Review note (APPROVED, one required follow-up)

Code reviewed (not just CI status). Good: `MODE` defaults to `PAPER`; `WALLET_PRIVATE_KEY` is `SecretStr` with `repr=False`; live gate fails closed (raises at load); secrets + `data/` gitignored.

**Required before WP-2 (security-critical — the gate is the core safety mechanism and is currently untested):**

> Add tests to `tests/test_config.py` for the live-mode gate:
> 1. `MODE=live` with `LIVE` unset → `Settings()` raises `ValueError`.
> 2. `MODE=live` with `LIVE=0` → raises `ValueError`.
> 3. `MODE=live` with `LIVE=1` → loads, `MODE == Mode.LIVE`.
> 4. Default (no `MODE`) → `MODE == Mode.PAPER`.
> Use `monkeypatch` for env vars so tests are hermetic. `make test` must cover these.

---

## WP-1 — Ingestion + recorder (paste to Codex)

> **Task:** Implement WP-1 — live market-data ingestion + recorder for Polymarket. Read-only public data; no auth, no orders. Stay in `src/pmbot/ingestion/`.
>
> Consult current Polymarket docs (CLOB websocket + Gamma REST) for exact message formats; build to the documented schema but keep our INTERNAL normalized schema decoupled from the raw API so a second venue can plug in later.
>
> **Components:**
> 1. **Gamma metadata client** (httpx): fetch market metadata — `condition_id`, question, outcomes, `token_ids` (asset_ids), resolution/end time, active/closed flags, liquidity, volume. Persist snapshots with a `fetched_at` timestamp.
> 2. **CLOB websocket client** (websockets): subscribe to the market channel for a configurable list of `token_ids`; capture L2 order-book (snapshot + deltas) and trade events.
> 3. **Recorder:** append normalized events to Parquet via polars/pyarrow, partitioned `data/raw/{stream}/date=YYYY-MM-DD/market={condition_id}/`. Buffer and flush on size/time.
>
> **Normalized schemas** (stable, documented in a module docstring):
> - book event: `ts_local_ns` (capture time), `ts_exchange` (nullable), `condition_id`, `asset_id`, `event_type {snapshot|delta}`, `side {bid|ask}`, `price`, `size`, `seq_or_hash` (nullable).
> - trade event: `ts_local_ns`, `ts_exchange` (nullable), `condition_id`, `asset_id`, `price`, `size`, `taker_side` (nullable), `trade_id`.
> - **PRICES AND SIZES AS `Decimal` (or integer ticks) — never float.** Correctness requirement; float drift corrupts microstructure signals.
>
> **Robustness (required):**
> - Reconnect with exponential backoff + jitter, capped retries, heartbeat/ping.
> - On every (re)connect, request a fresh book snapshot and emit a `GapEvent` marking the discontinuity so the backtester knows which spans are untrustworthy.
> - If the venue provides a book hash/checksum, verify it; on mismatch, resync and log a `GapEvent`.
> - A separate gap-log stream persisted alongside the data.
>
> **CLI:** `pmbot ingest --markets <token_ids|all-active> --duration <seconds>`.
>
> **Tests** (must run in CI with NO live network — use recorded/mocked frames):
> - parser: raw ws frames → normalized events (incl. snapshot vs delta).
> - recorder round-trip: write events → read Parquet back → assert schema + `Decimal` fidelity.
> - gap detection: simulate a dropped connection / seq jump → assert a `GapEvent` is emitted.
>
> **Acceptance:**
> - `make lint typecheck test` green; CI has no live dependency.
> - Manual live smoke (not CI): `pmbot ingest --markets all-active --duration 30` writes non-empty Parquet + a gap-log.
> - Print the normalized schema docstrings and the test output when done. Do not start WP-2.

**Review gate:** when WP-1 returns, paste the schema docstrings + test output. I'll review fill fidelity and gap handling before WP-2 (backtester), where look-ahead bias and fill realism are the make-or-break risks.

---

## WP-6 — Execution layer (FAST-TRACKED, build in parallel with WP-2; paste to Codex)

> **Task:** Implement WP-6 — the order execution layer for Polymarket CLOB. Build both `paper` and `live` adapters behind one interface so strategy code never knows which is active. Stay in `src/pmbot/execution/`. Depends only on WP-1 plumbing + `config.py`; does NOT need the backtester or signal.
>
> **Interface (single abstraction):**
> ```
> class ExecutionAdapter(Protocol):
>     async def place_order(self, order: Order) -> OrderResult: ...
>     async def cancel_order(self, order_id: str) -> bool: ...
>     async def get_open_orders(self) -> list[OpenOrder]: ...
>     async def get_positions(self) -> list[Position]: ...
>     async def get_balance(self) -> Balance: ...
> ```
> `Order` carries: `condition_id`, `asset_id`, `side {buy|sell}`, `price` (Decimal), `size` (Decimal), `tif {GTC|FOK|GTD}`, `order_type {limit|market}`, optional `client_order_id`.
>
> **PaperAdapter (default):** simulates fills against the live WP-1 order book — match against current best bid/ask + depth, apply the same fee/gas model WP-2 uses (import it, single source of truth). Tracks simulated positions/balance in memory + a Parquet trade log. This is what runs during validation.
>
> **LiveAdapter (real money):** signs and submits CLOB orders.
> - EIP-712 order signing per current Polymarket CLOB docs (consult them). Use the official `py-clob-client` if it's current and maintained; otherwise implement signing directly with `eth-account`.
> - Private key loaded ONLY from env/`WALLET_PRIVATE_KEY` (already a `SecretStr`) or an encrypted local keystore file unlocked at startup. Never logged, never written to disk in plaintext, never in tracebacks.
> - Idempotency: attach `client_order_id` to every order; on reconnect, reconcile open orders before re-sending so a dropped ack never double-submits.
> - Rate-limit handling + ret/backoff on transient API errors; hard-fail (no retry) on auth/balance errors.
>
> **Safety gates (engineering guardrails, not friction):**
> - `LiveAdapter` refuses to instantiate unless `MODE=live` AND `LIVE=1` (reuse the config gate).
> - Config caps enforced at the adapter boundary, read from settings: `MAX_ORDER_USD`, `MAX_OPEN_NOTIONAL_USD`, `MAX_ORDERS_PER_MIN`. An order exceeding a cap is rejected before signing. These protect against a bug emptying the wallet — set them high if you want, but they exist.
> - `--yes` flag (or `AUTO_CONFIRM=1`) bypasses any interactive prompt so the bot runs unattended/automated. Default in unattended mode is to trust the caps, not prompt.
> - A `panic_flatten()` method that cancels all open orders and market-sells to flat — wired to SIGINT/SIGTERM and callable by the risk kill-switch.
>
> **CLI:** `pmbot exec selftest` (paper adapter round-trip), `pmbot exec balance`, `pmbot exec flatten`.
>
> **Tests (CI, NO live network, NO real key):**
> - paper adapter: place → simulated fill against a fixture book → position/balance update; fee model matches WP-2.
> - live adapter: signing produces a valid EIP-712 signature for a known fixture (verify recovered address), mocked transport — never hits the real API in CI.
> - cap enforcement: order over `MAX_ORDER_USD` is rejected pre-sign.
> - idempotency: simulated dropped-ack does not double-submit.
> - gate: `LiveAdapter` raises unless `MODE=live` + `LIVE=1`.
>
> **Acceptance:**
> - `make lint typecheck test` green; CI uses no real key and no live network.
> - `pmbot exec selftest` runs the paper round-trip.
> - Report: confirm the private key never appears in logs/reprs, and show the cap-enforcement + idempotency test output.

**Note on the wallet:** fund a **dedicated** trading wallet with only the capital you're willing to put at risk — never your main wallet. The caps above are the backstop against a code bug; the dedicated wallet is the backstop against the caps failing. Both are cheap insurance on real money.

**Critical-path reminder:** with WP-6 done in parallel, first live trade = WP-1 ✅ → WP-3 signal → WP-5 sizing → wire them into WP-6's live adapter → fund wallet → `MODE=live LIVE=1 pmbot run --yes`. WP-2/WP-4 tell you whether the signal is worth funding; they don't block the trade path being ready.

---

## WP-1 — Review note (corrections required before WP-2)

WP-1 approved structurally (gate tests in first, hermetic CI, Decimal throughout, raw/normalized split, fails closed on block). Two required fixes:

1. **`ts_local_ns` must be wall-clock UTC ns (`time.time_ns()`), not `monotonic_ns()`.** Monotonic epoch is undefined, resets per process, and isn't comparable across processes — breaks cross-session concatenation, latency-vs-`ts_exchange`, and (critically) cross-venue timestamp alignment for the WP-3 signal. Add optional `ts_monotonic_ns` secondary for intra-session ordering. Schema change — do before recording volume.
2. **Validate the websocket parser against REAL frames.** The corporate SWG `403` meant only Gamma + gap-log actually wrote; the CLOB book/trade parser never saw a live frame. Capture ~30s of real market-channel frames from a non-SWG network (hotspot/VPS), freeze as a fixture, assert the parser handles real frames. The SWG also blocks the bot from reaching Polymarket on this network → VPS needed for deployment anyway.

---

## Reference venue decision (for WP-3 cross-venue signal): **Kalshi**

Chosen over Betfair/Pinnacle because: US-legal official API (Betfair blocks US persons; Pinnacle has no public API and is sports-only), contracts are **direct binary analogs** to Polymarket (clean price-to-price comparison, no odds conversion), and documented lead-lag + arbitrage vs Polymarket. Less sharp than Betfair, but fine for v1 whose job is proving the pipeline. Recorder is venue-agnostic → add Betfair/Pinnacle later via VPS for a sharper sports reference.

---

## WP-3 — Feature library + cross-venue signal (paste to Codex after WP-2 review)

> **Task:** Implement WP-3 — microstructure feature library + the first trading signal (cross-venue deviation, Polymarket vs Kalshi). Stay in `src/pmbot/features/` and `src/pmbot/signals/`. Signal computation must be unit-testable independent of the backtester; integrate with WP-2 for evaluation.
>
> **Components:**
> 1. **Kalshi ingestion adapter** — reuse WP-1's venue-agnostic recorder; record Kalshi market list + L1/L2 quotes to the same normalized schema (wall-clock `ts_local_ns`). Kalshi has an official REST + websocket API; consult current docs.
> 2. **Contract-mapping module (the hard part — conservative):** map Polymarket `condition_id` ↔ Kalshi market ticker for events that resolve on **identically the same criteria**. Support a manual allowlist/override. Emit a pair ONLY when confident the two contracts resolve identically (same event, same source, same expiry handling); flag and EXCLUDE ambiguous pairs. Persist the verified pair set with the matching rationale.
> 3. **Feature library:** order-book imbalance (OBI), microprice (depth-weighted mid), spread, depth — computed point-in-time from the recorded book. (Built per plan; OBI→drift is research-only.)
> 4. **Cross-venue signal:**
>    - For each verified pair, compute Polymarket microprice and Kalshi microprice as probabilities.
>    - Estimate **lead-lag** (which venue moves first) so we never trade on a stale reference; only act when Kalshi is leading / fresh.
>    - Compute fair value `p̂` via **log-odds pool** of {Polymarket prior, Kalshi reference}, then trade the deviation `p̂ − Polymarket_microprice`. Apply the **double-counting guard**: update off the Polymarket price only by the component of the Kalshi signal not already reflected in it.
>    - Output per-market: signal value, direction, and a confidence (feeds WP-4 calibration + WP-5 sizing).
>
> **NO LOOK-AHEAD (critical):** the signal at time `t` may use only data with `ts_local_ns ≤ t`. This is the cardinal backtest sin — enforce it structurally (the signal function receives a point-in-time view, never the full series) and add a test that feeds future data and asserts it's ignored.
>
> **Tests (CI, fixtures, no live network):**
> - feature math: OBI/microprice/spread on a fixture book match hand-computed values.
> - contract mapper: known matched pair maps; known mismatched/ambiguous pair is excluded.
> - signal: produces expected deviation on a fixture; log-odds blend + double-counting guard verified numerically.
> - look-ahead guard: future data is provably ignored.
>
> **Acceptance:** `make lint typecheck test` green; signal runs over recorded data via the WP-2 backtester; print the look-ahead test and a sample signal output. Do not start WP-5 wiring until reviewed.
