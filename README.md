# Heurism

[![CI](https://github.com/tepiduck/heurism/actions/workflows/ci.yml/badge.svg)](https://github.com/tepiduck/heurism/actions/workflows/ci.yml)

Heurism is a Python platform for developing and testing prediction-market
strategies. It collects Polymarket and Kalshi data, replays order books, and
lets you test your own trading rules with simulated fills, latency, and costs.

You can start with the offline example, change the strategy, and then try it
on data you've collected. The collectors are read-only, and all order
execution happens in the backtester.

## Get started

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```bash
git clone https://github.com/tepiduck/heurism.git
cd heurism
uv sync --dev --locked
uv run python examples/strategy_lab.py
```

The project uses Python 3.11, which uv can install for you. Once dependencies
are installed, the example runs offline without API keys.

It buys when the ask reaches a chosen price and prints a JSON report with
fills, fees, inventory, and profit or loss (PnL). The prices are synthetic:
this is a small example for learning the API, not evidence of a profitable
strategy.

Try changing the entry price or latency, or save a report:

```bash
uv run python examples/strategy_lab.py --entry-price 0.52 --latency-ms 500
uv run python examples/strategy_lab.py --report reports/first-run.json
```

## Write a strategy

Start with [`examples/strategy_lab.py`](examples/strategy_lab.py).
Edit `EntryStrategy.__call__` to read the current book and return
`OrderRequest` objects. Return `()` when you want to wait.

The example makes one entry request per run. The
[strategy guide](docs/strategy-development.md) explains the callback API,
how to replay your own recordings, and what the simulator currently supports.

## Collect market data

Put Polymarket condition IDs or CLOB token IDs in `markets.txt`, one per
line, then record a five-minute sample:

```bash
uv run heurism harvest --polymarket-markets markets.txt --duration 300 --data-dir data/my-market
```

Replay it with the CLOB token ID of the outcome you want to trade:

```bash
uv run python examples/strategy_lab.py --data-dir data/my-market --asset-id YOUR_CLOB_TOKEN_ID
```

The [data guide](docs/market-data.md) covers Kalshi setup, collection options,
and the included cross-venue forecast evaluator. That evaluator compares
Polymarket and Kalshi contracts with matching outcomes and reports forecast
scores, calibration, and sample sizes.

## Contributing

The Python package is `pmbot`; the `heurism` and `pmbot` commands run the
same CLI. The main code is in [`src/pmbot`](src/pmbot), with tests organized
by module in [`tests`](tests).

To check a change:

```bash
uv run ruff check .
uv run mypy
uv run pytest
```

Bug fixes, examples, and improvements to the backtester are welcome. For a
bug report, include the command you ran, the error, and a small sample that
reproduces it. For a strategy example, explain the rule and the data, fees,
and latency you used.
