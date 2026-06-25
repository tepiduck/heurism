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
