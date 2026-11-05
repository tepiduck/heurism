# Collecting and evaluating market data

Run these commands from the repository root after following the
[setup instructions](../README.md#get-started). Both `heurism` and `pmbot`
run the same CLI.

## Collect a sample

Use `harvest` to record selected markets. Put Polymarket condition IDs or
CLOB token IDs in a text file, one per line:

```bash
uv run heurism harvest --polymarket-markets markets.txt --duration 300 --data-dir data/my-market
```

For Kalshi, put market tickers in a separate file:

```bash
uv run heurism harvest --kalshi-tickers tickers.txt --duration 300 --data-dir data/kalshi
```

You can pass both files in one command. Blank lines, lines starting with
`#`, and comma-separated IDs are supported. Use current market IDs; the
checked-in `pairs.draft.jsonl` contains historical World Cup pairs.

To configure Kalshi authentication, copy [`.env.example`](../.env.example)
to `.env` and set both `KALSHI_API_KEY_ID` and
`KALSHI_PRIVATE_KEY_PATH`. Keep the key file under the ignored `secrets/`
directory or outside the repository.

Harvest writes normalized books, trades, metadata, and gap logs under the
chosen data directory. Add `--poll-resolutions` to record outcomes as they
become available. Omit `--duration` to keep collecting until you stop it.

You can also select groups using `--kalshi-series`,
`--polymarket-series`, or `--polymarket-tag`. Use the current identifiers
for the groups you want. Harvest requires a selection; it doesn't subscribe
to every active market by default.

## Check a recording

```bash
uv run heurism harvest-status --data-dir data/my-market
```

Check that frame timestamps stay recent and that book or trade files
appear under `raw/`. A folder containing only metadata and gap logs doesn't
mean market data is arriving.

Use separate data directories for separate collection jobs. You can also
set a custom heartbeat path with `--status-file`; pass the same path to
`harvest-status`. To combine small files in closed Parquet partitions:

```bash
uv run heurism compact --data-dir data/my-market
```

The [strategy guide](strategy-development.md#use-recorded-data) explains
how to replay these files.

## Match contracts across venues

The cross-venue model compares contracts for the same outcome. Similar
titles aren't enough: check the event, threshold, resolution time and time
zone, settlement source, and rules for ties, cancellations, or voids.

Record verified pairs in a JSONL file, one JSON object per line. This shows
the format; replace the placeholder IDs and conditions before using it:

```jsonl
{"polymarket_condition_id":"YOUR_CONDITION_ID","polymarket_asset_id":"YOUR_YES_TOKEN_ID","kalshi_ticker":"YOUR_KALSHI_TICKER","polymarket_yes_payout_condition":"The same verified outcome","kalshi_yes_payout_condition":"The same verified outcome","invert":false,"rationale":"Checked both venues' event, timing, settlement source, and rules"}
```

`polymarket_asset_id` is the YES token. You can also provide
`polymarket_no_asset_id` if you have the NO token.

Use the same payout description when both YES contracts pay on the same
outcome. If their outcomes are exact opposites, describe each one and set
`invert` to `true`; the model then uses `1 - p_kalshi`. The loader checks
these descriptions for consistency, but it cannot verify the venue rules
for you. For example, a team's NO outcome may include both a draw and an
opponent win, so it isn't automatically equivalent to the opponent's YES.

## Run the cross-venue collector

```bash
uv run heurism run-collector --allowlist path/to/verified_pairs.jsonl
uv run heurism status
```

The collector records both venues, the model's forecasts, and resolved
outcomes. It also includes World Cup-specific discovery at startup and on
a timer, so its subscriptions can grow beyond the supplied allowlist. Use
`harvest` when you want to collect only the markets you select.

## Evaluate forecasts

Once you have books and resolved outcomes, run:

```bash
uv run heurism eval --data-dir data --allowlist path/to/verified_pairs.jsonl --report reports/forecast-eval.json
```

The evaluator rebuilds forecasts from `raw/book` and joins them to
`resolutions`. It doesn't use saved `signals`, which may have been produced
by older model versions.

The report includes forecast scores, calibration, event counts, excluded
observations, and the configuration used. A `small_n` flag marks runs with
too few distinct resolved events for a verdict; the default threshold is
30. Multiple ticks from one event aren't independent outcomes.

Regression fitting is off by default. `--fit-regression` enables it once
enough distinct events are available. Some defaults are specific to the
original World Cup experiment, including three-way outcome checks and a
knockout-stage filter. Review `uv run heurism eval --help` before using the
model on other markets.

To limit a run to the most recent six hours, add `--hours 6`. For older
recordings, also set `--end-ns` to a timestamp within the recording; otherwise
the window ends at the current time. Choose a new report filename for
each run.

## Longer runs on macOS

To keep a foreground collector running while preventing idle sleep:

```bash
caffeinate -i uv run heurism run-collector --allowlist path/to/verified_pairs.jsonl
```

For a background job, the [launchd template](../ops/launchd/com.pmbot.collector.plist)
sets up automatic restarts and log files. Replace `__PROJECT_DIR__` and
`__ALLOWLIST_PATH__` with absolute paths, check the path to `uv`, and create
`data/collector` for the logs before installing it.
