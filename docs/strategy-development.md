# Writing a strategy

Start with [the strategy lab](../examples/strategy_lab.py). It's a single
Python file you can edit or copy for your own experiments. Run the commands
below from the repository root after following the [setup instructions](../README.md#get-started).

## Run the example

```bash
uv run python examples/strategy_lab.py --report reports/baseline.json
```

The example has four synthetic book snapshots. It submits one limit buy for
ten contracts when the ask reaches `0.50` or less, with 150 ms of simulated
latency. Later quotes determine the value of the open position.

Change one setting at a time to see how it affects the result:

```bash
uv run python examples/strategy_lab.py --entry-price 0.52 --report reports/entry-052.json
uv run python examples/strategy_lab.py --latency-ms 1500 --report reports/latency-1500.json
```

Each report records the settings, data source, time range, fills, costs, and
final inventory and PnL. Reports won't overwrite an existing file. Prices
and amounts are stored as decimal strings to preserve precision.

## Change the trading rule

Edit `EntryStrategy.__call__`. The engine calls it once per replay
timestamp, after applying the events at that time.

| API | What it gives you |
| --- | --- |
| `view.ts_local_ns` | The current replay time in nanoseconds |
| `view.book(asset_id)` | Sorted bids and asks for an asset |
| `view.trades(asset_id=..., start_ns=...)` | Trades observed so far, optionally filtered by asset and start time |
| `view.events()` | Events observed so far, when history is enabled |
| `view.gap_active(asset_id)` | Whether a data gap makes the asset's book unreliable |
| `OrderRequest(...)` | An order for the engine to simulate |

Return orders when your rule triggers, or `()` to wait. Use `Decimal` for
prices and sizes, check for missing book sides, and skip active data gaps.
Create a fresh strategy instance for each run. Read the view inside the
callback rather than holding onto it for later.

The example marks an order as submitted as soon as it sends the request.
That doesn't mean the order filled. It has no exit or re-entry rule, and the
callback API doesn't yet expose inventory, fill notifications, or order
cancellation. Strategies that depend on those features will need an engine
extension.

## Use recorded data

Collect a sample with `heurism harvest`, then pass the data directory and
the asset's CLOB token ID:

```bash
uv run python examples/strategy_lab.py --data-dir data/my-market --asset-id YOUR_CLOB_TOKEN_ID --report reports/recorded-run.json
```

The loader reads Parquet files under `raw/book`, `raw/trade`, and
`raw/gap-log`. Start with a small recording: it loads the directory into
memory. If the asset has no recorded books, the example reports an error.

To use the engine in your own script, pass it a strategy callback:

```python
from pathlib import Path
from pmbot.backtest import BacktestConfig, BacktestEngine

# Pass your strategy instance as the second argument.
engine = BacktestEngine(BacktestConfig(latency_ms=150))
result = engine.run_from_data_dir(Path("data/my-market"), strategy)
```

`result.blotter` holds the simulated fills. `result.pnl_series` holds the
cash, positions, and PnL at each observation.

At the end of a replay, the engine processes pending orders against the
last book. Those fills can occur after the final PnL observation. The
strategy lab avoids this by stopping new orders one latency window before
the end. Account for that behavior if you call the engine directly.

## Read the results

`final_marked_pnl` includes modeled costs and values open positions at the
final bid for longs or ask for shorts. It isn't the profit from actually
closing those positions. If the final book can't support a valid mark,
the report uses `null`.

Check `all_marks_valid` and `invalid_mark_count` as well. A valid final
price doesn't fix an earlier gap in the recording. Orders may also fill
partially or remain unfilled, so look at the fills and inventory alongside
PnL.

A few assumptions matter when you move beyond the example:

- The default cost model uses the repository's Polymarket International
  `other` category. Set `BacktestConfig.costs` to suit your experiment.
  Kalshi fees aren't implemented, and one cost configuration applies to
  the whole replay.
- Fills use displayed depth and queue position. The engine doesn't model
  capital limits, margin, or market impact. Separate orders can reuse the
  same historical liquidity.
- Synthetic examples help check code. To evaluate a strategy, use separate
  recorded data for tuning and testing, with explicit fee and latency
  assumptions.

The [`eval` modules](../src/pmbot/eval) include forecast scoring, calibration,
PnL metrics, and parameter sweeps. The `heurism eval` command runs the
included cross-venue forecast model; it doesn't load custom strategy
callbacks.
