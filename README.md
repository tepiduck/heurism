# pmbot

Python 3.11 scaffold for Polymarket microstructure trading research and execution.

## Data Collector

Run the read-only collector with a human-curated verified-pair allowlist:

```bash
pmbot run-collector --allowlist path/to/verified_pairs.jsonl
```

The collector records Polymarket and Kalshi market data, cross-venue signal
forecasts, gap logs, status heartbeats, and resolved outcomes. It never places
orders.

Check freshness and heartbeat state:

```bash
pmbot status
```

## Raw Harvest Mode

Use `pmbot harvest` for generic single-venue raw dataset building. Harvest mode
records normalized order books, trades, gap logs, metadata, and optional
resolved outcomes under `data/raw/`, but it does not construct cross-venue
pairs, run contract mapping or resolution-criteria verification, compute
signals, place orders, or touch execution paths.

Select targeted markets explicitly:

```bash
pmbot harvest \
  --polymarket-markets path/to/polymarket_ids.txt \
  --kalshi-tickers path/to/kalshi_tickers.txt \
  --duration 300
```

Each Polymarket list row may contain condition IDs or numeric CLOB token/asset
IDs. Each Kalshi list row contains market tickers. Blank lines and `#` comments
are ignored, and comma-separated identifiers are accepted.

You can also harvest targeted groups:

```bash
pmbot harvest \
  --kalshi-series KXWCGAME \
  --polymarket-series soccer-fifwc \
  --data-dir data/harvest_wc \
  --status-file data/harvest_wc/status.json \
  --poll-resolutions
```

Check the separate harvest heartbeat with:

```bash
pmbot harvest-status --data-dir data/harvest_wc --status-file data/harvest_wc/status.json
```

Harvest runs can use a different `--data-dir` and `--status-file` from the
strategy collector, so both processes can run independently without clobbering
each other's `status.json`.

There is intentionally no default "all active markets" harvest. Subscribing to
thousands of live markets can hit websocket and connection limits and flood
disk. Prefer explicit lists, venue series, or Polymarket tag/series slugs. If an
all-active mode is added later, it should batch subscriptions and warn loudly.

## Run 24/7 On macOS

The launchd template lives at `ops/launchd/com.pmbot.collector.plist`. Replace
`__PROJECT_DIR__` and `__ALLOWLIST_PATH__` with absolute paths, and adjust the
`uv` path if the target Mac does not use `/opt/homebrew/bin/uv`, then install it:

```bash
mkdir -p data/collector
cp ops/launchd/com.pmbot.collector.plist ~/Library/LaunchAgents/com.pmbot.collector.plist
launchctl load ~/Library/LaunchAgents/com.pmbot.collector.plist
launchctl start com.pmbot.collector
```

The plist runs the collector under `caffeinate -i` and sets `KeepAlive=true`, so
launchd restarts it after crashes and reboots. Keep the MacBook plugged in,
disable system sleep in Battery settings, and prevent lid-close sleep if the
machine will run closed. For a manual foreground run, use:

```bash
caffeinate -i pmbot run-collector --allowlist path/to/verified_pairs.jsonl
```
