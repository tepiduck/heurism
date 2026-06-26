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
- **WP-4 — Eval + calibration:** ← CURRENT. **primary = proper scoring rules (Brier, log-loss) over many resolved markets vs the market-price baseline + calibration curves + implementation shortfall.** ALSO calibrates the WP-3 magic constants (`double_counting_overlap`, confidence scalar) via train/test sweep. Cross-venue CLV is a **secondary** diagnostic only. *(Corrected from v1, which made CLV primary.)*
- **WP-5 — Risk + sizing:** **fractional Kelly (≤¼ haircut), simultaneous/vector Kelly with covariance, opportunity-cost-adjusted edge for locked capital**, per-market & aggregate position limits, capacity cap, correlation cap, drawdown kill-switch. **HARD RULE: WP-5 must not run before WP-4 — sizing on uncalibrated signal magnitude is the overbetting failure mode. WP-4 gates WP-5.**
- **WP-6 — Execution layer:** ← FAST-TRACKED, builds in parallel with WP-2. CLOB order signing/placement; `paper` and `live` adapters behind one interface. `live` enabled by `MODE=live LIVE=1`. Built so that the moment the signal validates, the trade path is already done — owner funds the wallet and flips the switch.
- **WP-7 — Live loop + monitoring:** orchestrate ingest→signal→size→execute; logging, alerting, heartbeat, panic-flatten.
- **WP-8 — Cross-market arbitrage (penciled, AFTER single-leg validates):** true dual-leg arb over verified contract pairs (buy YES cheap venue + NO dear venue for combined cost <$1). Builds on the WP-3 mapper + a dual-venue executor extending WP-6. Deferred: most-competed, fee-hostile near p=0.5, and intensifies the US-geofencing legal exposure (requires trading both Kalshi + Polymarket as a US person). Do not build until the cross-venue single-leg signal is shown to pay in WP-4.

### Backlog — methodology upgrades from external review (see STRATEGY.md "v3")
These refine WP-3/4/5 before any real-money sizing (none block data collection):
- **WP-3:** replace hardcoded `overlap` with the estimated regression `ẑ=α+γz_PM+β(z_K−z_PM)+Xθ`; separate resolution-model vs convergence-model targets; use exchange timestamps + placebo tests for lead-lag; separate forecast mid from executable bid/ask.
- **WP-4:** dual go/no-go (paired proper-score on holdout AND net-PnL on predeclared rule); **event-clustered** bootstrap (a match = ~1 obs, not N ticks); baseline must be a mid, not an ask (scoring vs an ask fakes skill); markout-PnL eval for convergence trades; single immutable run manifest.
- **WP-5:** side-specific Kelly with executable all-in cost; scenario-based `max E[log(1+fᵀR)]` for mutually-exclusive markets (not `Σ⁻¹μ`); define `σ̂` as forecast uncertainty (not `√(p̂(1−p̂))`) or use a net-EV quantile gate; round-trip two-sided fees.

### WP-3 status: ✅ implemented, with one required fix
Cross-venue signal + feature lib + conservative mapper done. **Required fix before trusting it: the mapper has NO YES/NO polarity check** — mapping PM-YES to an oppositely-defined Kalshi-YES flips the signal sign and trades backwards with full confidence. See fix below. Also flagged for WP-4 to calibrate: `double_counting_overlap=0.5` (hardcoded; collapses the guard to an equal-weight log-odds average), the `confidence = |signal|×4×freshness` scalar, and the "Kalshi leads" guard which is currently a freshness filter, not true lead-lag.

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

---

## Mapper polarity fix (do first — small, prevents wrong-way trades; paste to Codex)

> **Task:** Harden the contract mapper against YES/NO polarity inversion in `src/pmbot/signals/contracts.py`. Currently a Polymarket YES token can be mapped to an oppositely-defined Kalshi YES, flipping the signal sign and trading backwards.
> - Add an explicit `polarity` / orientation to `ContractMetadata` and `ManualAllowlistEntry` describing what the priced YES token pays out on (the resolution condition in normalized form), for BOTH venues.
> - `verify_pair` must REJECT unless the two YES definitions denote the same outcome direction. If a venue's comparable token is the opposite side, require the allowlist entry to specify an explicit `invert=True` mapping (so the signal can flip Kalshi's probability to 1−p before pooling), and thread that inversion through to the signal.
> - `CrossVenueSignal` must apply the verified inversion (use `1 − kalshi_probability` when the pair is inverted) BEFORE the log-odds pool.
> - Tests: a same-direction pair verifies; an opposite-direction pair without `invert` is rejected; an `invert=True` pair produces a correctly sign-flipped signal vs the non-inverted case.
> **Acceptance:** `make lint typecheck test` green; show the inversion test.

---

## WP-4 — Eval + calibration (paste to Codex after the polarity fix)

> **Task:** Implement WP-4 — evaluation + calibration. This is the instrument that decides whether the signal has real edge and converts WP-3's guessed constants into validated numbers. Stay in a new `src/pmbot/eval/`. Build/test on fixtures now; real verdict comes once live resolved-market data exists.
>
> **Inputs:** backtest results (blotter, pnl_series) from WP-2; signal forecasts `p̂` at decision times from WP-3; realized binary market outcomes (resolutions) from Gamma/Kalshi metadata.
>
> **Components:**
> 1. **Proper scoring:** Brier score + log-loss of `p̂` vs realized outcomes across resolved markets. Compute the SAME scores for the Polymarket microprice as a baseline forecast. **Headline metric = Brier skill of `p̂` over the market price.** If `p̂` does not beat the market's own Brier, there is no alpha — report this front and center.
> 2. **Calibration:** reliability curve (bin `p̂` vs empirical frequency) + Expected Calibration Error, with bootstrap confidence bands; flag small-n loudly.
> 3. **PnL metrics:** total PnL, Sharpe, max drawdown, hit rate — computed ONLY over `pnl_valid` spans (respect WP-2's scoped gap invalidation).
> 4. **Implementation shortfall:** per fill, arrival-mid (decision price) vs actual fill price → execution cost; aggregate. Decompose total PnL into `alpha (p̂ beat market) + execution (fills beat arrival mid) + noise`.
> 5. **Constant calibration (replaces WP-3 guesses):** grid-sweep `double_counting_overlap` and the confidence scalar; select the value maximizing **out-of-sample** Brier-skill / PnL on a train split, validate on a held-out split. Report in-sample vs out-of-sample gap. NO LEAKAGE: the test split must use the train-selected param; add a test asserting no peeking. Walk-forward if feasible.
> 6. **Secondary — cross-venue CLV:** entry price vs Kalshi's later price; report as a noisy diagnostic, not a headline.
> 7. **Significance + sample size:** bootstrap CIs on Brier-skill and PnL; report n resolved markets; explicitly flag when n is too small to conclude (it will be, early).
>
> **Tests (fixtures):** Brier/log-loss vs hand-computed; calibration binning; market-baseline comparison; implementation-shortfall decomposition; sweep has no train/test leakage.
>
> **Acceptance:** `make lint typecheck test` green; produce a sample eval report over a fixture showing Brier-skill vs market, calibration data, PnL/Sharpe/DD, implementation shortfall, and the chosen overlap/confidence with the in/out-of-sample gap. **WP-4 gates WP-5 — do not start sizing until this is reviewed and the signal shows positive out-of-sample Brier-skill.**

**Reality note:** WP-4 machinery can be built/tested on fixtures now, but a real edge verdict needs live recorded + resolved data — still blocked by the SWG/parser-probation item. The calibration is only as trustworthy as the data feeding it.

---

## WP-7a — Data-collection runner (paste to Codex; deploy on the home MacBook)

> **Task:** Implement a persistent, unattended data-collection runner — WP-7 minus all trading. Its job: record Polymarket + Kalshi market data continuously, log every signal `p̂` with timestamps, and collect market resolutions, so WP-4 can later score real out-of-sample Brier skill. NO orders, no execution, read-only. Add `src/pmbot/runner/` and CLI wiring.
>
> **Components:**
> 1. **Orchestrator:** run Polymarket (CLOB ws + Gamma) and Kalshi ingestion concurrently, feeding the existing venue-agnostic recorder. One process, supervised async tasks; if one venue's stream dies, it reconnects (WP-1 backoff) without killing the other.
> 2. **Signal logging (critical for WP-4):** for each verified contract pair, compute the cross-venue `SignalValue` on each update and persist it to a `signals/` Parquet stream with `ts_local_ns`, `p̂`, both venue probabilities, direction, confidence. This prediction history is what gets joined to resolutions to score Brier — without it there is no verdict.
> 3. **Resolution poller:** periodically (configurable, e.g. every 15 min) query Gamma + Kalshi for resolved outcomes of tracked markets; persist outcome labels (`condition_id`/`ticker`, resolved_outcome, resolved_at) to a `resolutions/` stream. Idempotent; never double-record.
> 4. **Verified-pair allowlist:** load the curated allowlist (the human-maintained set of short-horizon Polymarket↔Kalshi pairs); only track + signal those pairs.
> 5. **Observability:** structured heartbeat log (per-venue last-frame time, frames/min, reconnects, gaps in last hour); a `pmbot status` CLI showing uptime, per-venue freshness, n pairs tracked, n resolutions collected, gap count last 24h.
> 6. **macOS unattended operation:** ship a `launchd` plist (KeepAlive=true so it auto-restarts on crash/reboot) and document running under `caffeinate -i` (or the plist equivalent) to prevent sleep. Document: disable system sleep, "prevent sleep on lid close", keep plugged in.
>
> **Robustness:** survive network blips, venue restarts, and process restarts without data loss beyond the gap (mark gaps, never silently drop). Flush Parquet frequently so a hard kill loses < 1 min.
>
> **Tests (CI, no live network):** orchestrator restarts a failed venue task without stopping the other; signal-logging writes the expected Parquet schema; resolution poller is idempotent (same resolution twice → one record); `status` renders from fixture state.
>
> **Acceptance:** `make lint typecheck test` green. Manual (on the off-SWG MacBook): `pmbot run-collector --allowlist <file>` records book+trade+signal+resolution Parquet and `pmbot status` shows live freshness. Include the launchd plist + a short "run 24/7 on macOS" README section. No orders anywhere.

**Parallel human task:** curate the verified-pair allowlist — short-horizon (daily/hourly) markets that exist on BOTH Polymarket and Kalshi and resolve within the recording window, with verified matching YES-payout polarity. This is manual and gates how many pairs the runner can track.
