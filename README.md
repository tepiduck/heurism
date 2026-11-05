# Heurism

[![CI](https://github.com/tepiduck/heurism/actions/workflows/ci.yml/badge.svg)](https://github.com/tepiduck/heurism/actions/workflows/ci.yml)

**A Python research platform for building and testing prediction-market strategies.**

Collect Polymarket and Kalshi market data, write your own strategy in Python,
and replay it against recorded order books. Heurism provides data ingestion,
an event-driven backtester, and evaluation tools so you can focus on the
ideas you want to test.

Start with the offline strategy lab, change its entry rule, then bring your
own market data. No account or API credentials are needed for the demo.

## What you can build

| Workflow | Included tools |
| --- | --- |
| Build datasets | Read-only collectors, normalized Parquet books and trades, gap logs, resolution polling |
| Develop strategies | Python callbacks with point-in-time books, trades, and gap state |
| Simulate execution | Market and limit orders, configurable latency, depth-based fills, queue modeling, and modeled costs |
| Inspect results | Fill blotters, inventory, marked PnL, and data-validity flags |
| Evaluate forecasts | Brier and log-loss scoring, calibration, clustered intervals, and parameter sweeps |

Heurism is a local Python toolkit and CLI. Order execution is simulated;
live order placement is not implemented. The existing cross-venue strategy
is one research example you can study and extend.

## Quick start

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:

```bash
git clone https://github.com/tepiduck/heurism.git
cd heurism
uv sync --dev --locked
uv run python examples/strategy_lab.py
```

The project uses **Python 3.11**; uv can install the required interpreter.
The demo runs entirely offline after dependency installation. It submits a
limit buy when the ask falls to or below a threshold and prints JSON containing
the parameters, modeled costs, fills, inventory, and final marked PnL.

Try a different threshold or execution delay:

```bash
uv run python examples/strategy_lab.py --entry-price 0.52 --latency-ms 500
uv run python examples/strategy_lab.py --entry-price 0.48
uv run python examples/strategy_lab.py --report reports/first-run.json
```

The `0.48` threshold produces no orders on the demo books. Report files are
created without overwriting earlier runs. Demo prices are **synthetic** and
exist to illustrate the workflow; their PnL is not a performance result.

## Develop your own strategy

Open [`examples/strategy_lab.py`](examples/strategy_lab.py) and edit
`EntryStrategy.__call__`. A strategy receives the current market view and
returns zero or more `OrderRequest` objects. Read the current order book with
`view.book(asset_id)`, inspect observed trades with `view.trades()`, and return
`()` to wait. The example includes the complete callback and experiment runner.

The starter makes at most one entry request per run, keeping the first
experiment easy to inspect. Follow the [strategy development guide](docs/strategy-development.md)
for the callback API, recorded-data replay, result interpretation, and current
simulation limits.

## Bring your own data

Put current Polymarket condition IDs or CLOB token IDs in a text file, one
per line, then collect a bounded sample:

```bash
uv run heurism harvest --polymarket-markets markets.txt --duration 300 --data-dir data/my-market
```

Replay the recorded books using the specific CLOB token ID you want to trade:

```bash
uv run python examples/strategy_lab.py --data-dir data/my-market --asset-id YOUR_CLOB_TOKEN_ID --report reports/my-market.json
```

For Kalshi collection, configure `KALSHI_API_KEY_ID` and
`KALSHI_PRIVATE_KEY_PATH` in a local `.env` using [`.env.example`](.env.example).
The [market-data guide](docs/market-data.md) covers both venues, status checks,
cross-venue evaluation, and macOS background collection.

## Evaluate cross-venue forecasts

The built-in `eval` command evaluates the included Polymarket/Kalshi signal
using recorded books, resolved outcomes, and a verified contract-pair allowlist:

```bash
uv run heurism eval --data-dir data --allowlist path/to/verified_pairs.jsonl --report reports/forecast-eval.json
```

This command recomputes forecasts from raw books and reports scoring,
calibration, and sample-size guards. It is separate from the custom-strategy
backtest starter. Build your own current pair mapping using the
[allowlist guide](ALLOWLIST_GUIDE.md); `pairs.draft.jsonl` contains historical
research pairs.

## Project map

```text
examples/strategy_lab.py  Editable offline and recorded-data experiment
src/pmbot/
  ingestion/             Venue adapters and normalized storage
  backtest/              Replay engine, order models, and costs
  features/              Order-book features
  signals/               Contract matching and cross-venue signals
  eval/                  Forecast and backtest evaluation components
  risk/                  Sizing utilities
  runner/                Collection, discovery, and process supervision
tests/                   Unit and regression tests
docs/                    Usage and strategy development guides
```

The Python package remains `pmbot`. Both `heurism` and `pmbot` invoke the
same CLI, so existing imports and scripts continue to work.

## Development

```bash
uv run ruff check .
uv run mypy
uv run pytest
uv run heurism --help
```

For a new strategy, start by defining the hypothesis, add a deterministic
fixture that tests its behavior, then evaluate it on held-out recorded data.
Include latency, cost assumptions, and data-quality flags when comparing runs.
