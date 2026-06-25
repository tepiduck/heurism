# Tonight's Setup Runbook — Home MacBook Data Collector

Goal: get the older MacBook recording Polymarket + Kalshi data 24/7 on your home wifi. Read-only, no trading. Do these in order; stop and note any step that errors.

> **Prereq before you start:** make sure WP-7a (the data-collection runner) is implemented by Codex. If `pmbot run-collector` doesn't exist yet, paste the WP-7a prompt to Codex first (it's in `ENGINEERING_PLAN.md`). Steps 1–4 work regardless; steps 6–8 need the collector built.

---

## 1. Install the toolchain

Open Terminal on the MacBook:

```
# Homebrew (skip if installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# uv (Python runner), git, node
brew install uv git node
```

## 2. Clone the repo

```
cd ~
git clone git@github.com:tepiduck/heurism.git
cd heurism
```

If the SSH clone prompts for a key / permission denied, this MacBook doesn't have a GitHub SSH key yet. Quickest path: clone over HTTPS instead —
`git clone https://github.com/tepiduck/heurism.git` — and sign in when prompted.

## 3. Install deps + sanity-check

```
uv sync
make lint typecheck test
```

You should see the tests pass (one xfail is expected — the real-frame fixture).

## 4. THE network test (most important step)

This confirms the MacBook can actually reach the venues from home wifi — the thing your work laptop's proxy blocked:

```
uv run pmbot ingest --markets all-active --duration 30
```

- **Success looks like:** Parquet files appear under `data/raw/book/...` and `data/raw/trade/...`, not just `metadata/` and `gap-log/`.
- **If you get an SSL/403 error:** this network is also behind a proxy — stop and tell me; we'll need a different network or a VPS.

If it succeeded, you've also just captured real frames. That's the data we need to retire the parser-probation xfail — keep the run output / Parquet; we can turn it into the fixture next.

## 5. Put the verified pairs in place

Create the allowlist file from the verified pairs (use `ALLOWLIST_GUIDE.md` + the draft candidate list). Save it as `pairs.jsonl` in the repo root, one verified pair per line.

> Only include pairs you've actually verified end-to-end. Ten solid pairs beat fifty sloppy ones.

## 6. Prevent sleep (so it records overnight)

- System Settings → Battery / Energy Saver → set **Turn display off** is fine, but **never sleep**; uncheck "put hard disks to sleep".
- Keep it **plugged in**.
- A laptop sleeps when the lid closes. Either keep the lid open, or run the collector under `caffeinate` (next step), which holds the system awake.

## 7. Start the collector

Simplest for night one (keeps it awake + running in the foreground; use a Terminal you leave open, or `tmux`):

```
caffeinate -i uv run pmbot run-collector --allowlist pairs.jsonl
```

For permanent auto-restart across reboots, install the `launchd` plist that ships with WP-7a (see the collector's README section). Do that once you've confirmed night one worked.

## 8. Verify it's alive

In another Terminal:

```
cd ~/heurism && uv run pmbot status
```

You want to see: recent per-venue last-frame times (freshness in seconds, not hours), pairs tracked > 0, and the Parquet files under `data/raw/` growing over time.

---

## Daily, after tonight

- `pmbot status` once a day to confirm it's still recording and reconnecting through any blips.
- Watch CLV (the fast, noisy read) as resolutions trickle in.
- Let `n` grow. The longer it runs, the tighter the eventual Brier verdict — don't rush to sizing.

## If something breaks

Note the exact error and which step, and send it over. The collector marks gaps rather than silently dropping data, so a reboot or wifi blip mid-run is fine — you just lose that span, not the dataset.
