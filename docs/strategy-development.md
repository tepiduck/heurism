# Develop a strategy

The strategy lab connects an editable Python callback to Heurism's existing
backtester. Run commands from the repository root after `uv sync --dev --locked`.

## 1. Run the baseline

```bash
uv run python examples/strategy_lab.py --report reports/baseline.json
```

The synthetic market has four book snapshots. The default strategy waits
until the ask reaches `0.50` or less, then submits one limit buy for ten
contracts. The simulator applies 150 ms of latency and its default cost
model. Later quotes determine the open position's final liquidation mark.

Change one assumption at a time and save results to distinct files:

```bash
uv run python examples/strategy_lab.py --entry-price 0.52 --report reports/entry-052.json
uv run python examples/strategy_lab.py --latency-ms 1500 --report reports/latency-1500.json
```

The report includes parameters, source, replay timestamps, cost configuration,
fills, cash, inventory, marked PnL, and validity flags. Decimal values are
serialized as strings to preserve precision.

## 2. Replace the entry rule

Edit `EntryStrategy` in [`examples/strategy_lab.py`](../examples/strategy_lab.py),
or copy the file into your own experiment. The callback runs once per replay
timestamp, after the events at that timestamp have been applied.

| API | Meaning |
| --- | --- |
| `view.ts_local_ns` | Current replay timestamp in nanoseconds |
| `view.book(asset_id)` | Current sorted bids and asks; either side can be empty |
| `view.trades(asset_id=..., start_ns=...)` | Observed trades up to the current timestamp |
| `view.events()` | Observed events; history is enabled by default |
| `view.gap_active(asset_id)` | Whether a data gap makes the asset untrusted |
| `OrderRequest(...)` | An order intent, subject to latency and fill simulation |

Return `()` when there is no action. Use `Decimal` for prices and sizes.
Reject incomplete books and skip active data gaps. Use a fresh strategy
instance for each experiment so state does not leak between runs. Treat the
view as callback-scoped; read the values you need during that invocation.

The starter sets `submitted` when it sends an order, not when a fill occurs.
It intentionally has no exit or re-entry logic. The callback currently has
no public inventory, fill-notification, or order-cancellation API. Strategies
requiring those capabilities need to extend the engine interface; counting
submitted orders as filled positions is not a substitute.

## 3. Replay your recordings

Collect data with `heurism harvest`, then supply its data root and an asset
ID present in the recorded books:

```bash
uv run python examples/strategy_lab.py --data-dir data/my-market --asset-id YOUR_CLOB_TOKEN_ID --entry-price 0.50 --report reports/recorded-run.json
```

The loader reads `raw/book`, `raw/trade`, and `raw/gap-log` Parquet partitions.
A missing or incorrect asset ID produces an error instead of an empty success
report. Small recordings are best for iteration: this loader reads the
selected directory into memory.

You can also use the backtester directly from Python:

```python
from pathlib import Path
from pmbot.backtest import BacktestConfig, BacktestEngine

# strategy is your callback implementing __call__(view) -> Iterable[OrderRequest].
engine = BacktestEngine(BacktestConfig(latency_ms=150))
result = engine.run_from_data_dir(Path("data/my-market"), strategy)
```

`result.blotter` contains fills and modeled costs. `result.pnl_series`
contains positions, cash, marks, and per-mark validity. The strategy lab stops
submitting orders within one latency window of the replay's end so that a
later observation can mark each arrival. When using the engine directly,
account for its end-of-input behavior: pending orders are drained against
the final book, and those arrivals may occur after the last PnL mark.

## 4. Interpret the result

- `final_marked_pnl` includes modeled fill costs and values open inventory
  at the final bid for longs or ask for shorts. It is not realized profit
  from closing every position. An invalid final mark is reported as `null`.
- `all_marks_valid` is false if any held position passed through an untrusted
  gap or lacked a required mark. A valid final mark does not repair earlier gaps.
- A submitted order can remain unfilled or fill only partially. Inspect the
  blotter and inventory alongside PnL.
- Default costs use the repository's Polymarket International `other`
  category. Set `BacktestConfig.costs` for your experiment and check its
  assumptions against your data. Kalshi fees are not implemented, and the
  engine applies one cost configuration across a replay.
- The simulator models displayed depth and queue position. It does not
  model capital or margin constraints, market impact, or every exchange rule;
  repeated orders do not consume a shared pool of historical liquidity.

The included `pmbot.eval` modules provide scoring, calibration, PnL metrics,
and train/test parameter sweeps. The `heurism eval` CLI specifically runs
the built-in cross-venue forecast pipeline; it does not load arbitrary
strategy callbacks. Treat the demo as a development fixture, then test your
hypothesis on separate recorded data with explicit cost and latency assumptions.
