# Verified Contract Pair Allowlist — Curation Guide

The cross-venue signal is valid **only if** a Polymarket market and a Kalshi market resolve on *identically the same event*. A wrong match manufactures fake edge, and real money then trades on it. The auto-mapper is deliberately strict (exact string match), so in practice every real pair comes through this human-curated allowlist.

**Rule zero: when in doubt, leave it out.** Ten rock-solid pairs beat fifty sloppy ones — a single mismatched pair can dominate your Brier score with fake signal and invalidate the whole verdict.

---

## What the runner consumes (per pair)

Matches `ManualAllowlistEntry` / `ContractMetadata` in `src/pmbot/signals/contracts.py`:

- `polymarket_condition_id` — the market's condition ID
- `polymarket_asset_id` — the **YES outcome token** ID (one of the `clobTokenIds`)
- `kalshi_ticker` — the Kalshi market ticker
- `invert` — `true` if Kalshi's YES pays on the **opposite** condition (signal then uses `1 − p_kalshi`)
- `polymarket_yes_payout` / `kalshi_yes_payout` — normalized text of what each YES pays out on
- `rationale` — one line on why you verified this pair (and the resolution-source note)

---

## Step 1 — Find candidate overlaps

Target **short-horizon** markets that resolve in hours/days and exist on **both** venues:

- **Crypto thresholds** — "Will BTC/ETH be above $X on <date>" (daily/weekly). Most reliable overlap.
- **Macro/econ** — CPI prints, Fed rate decisions, jobs numbers, "Fed cut in <month>".
- **Sports** — same-day game outcomes, if both list the same game.
- **Near-term news/politics** — daily/weekly resolutions.

Avoid long-dated markets (elections months out) for the validation window — they won't resolve in time.

**Where to pull IDs and rules:**

- Polymarket — Gamma API markets endpoint (or the market page): grab `condition_id`, `clobTokenIds` (YES/NO token IDs), and the full resolution text.
- Kalshi — their markets/events API (or the site): grab the `ticker` and the rules/settlement text.

---

## Step 2 — The matching checklist (verify ALL before adding)

For each candidate pair, confirm every item. Any failure → reject.

1. **Same underlying variable** — same asset / index / event.
2. **Same threshold, exactly** — `$100,000` vs `$100,001`, and `≥` vs `>` both matter. Off by a dollar or a boundary = a different market.
3. **Same resolution instant + timezone** — "by Dec 31, 11:59 PM ET" must be the same moment on both venues. (The mapper rejects close-time mismatches anyway.)
4. **Same resolution SOURCE** — *the subtle one.* Which price feed / authority settles it? If Polymarket resolves on one BTC index and Kalshi on another, prices can diverge right at the boundary, so the contracts are **not** truly identical even if the wording matches. Note the source in your rationale.
5. **Polarity** — does Polymarket-YES pay on the *same* condition as Kalshi-YES? If the liquid/listed Kalshi side is the opposite (e.g. "No, below $X"), set `invert: true` so the signal flips `1 − p`.
6. **Edge cases** — tie/push, void/invalid, early resolution. If the venues handle these differently and it's material, reject.

---

## Step 3 — Record it (template)

JSON Lines, one pair per row:

```json
{"polymarket_condition_id": "0xABC...", "polymarket_asset_id": "1234...YES", "kalshi_ticker": "KXBTC-25DEC31-B100000", "invert": false, "polymarket_yes_payout": "BTC >= $100,000 at 2025-12-31 23:59 ET per <source>", "kalshi_yes_payout": "BTC >= $100,000 at 2025-12-31 23:59 ET per <source>", "rationale": "Same asset/threshold/close/source; YES==YES; verified 2026-06-24"}
```

---

## Worked example — a daily BTC market

1. Polymarket lists "Will BTC be above $105k on June 30?" → pull `condition_id` + the YES `clobTokenId` + resolution text (note the price source + 11:59 PM ET close).
2. Kalshi lists "BTC above $105,000 on Jun 30" → pull the `ticker` + settlement text (note *its* price source + close).
3. Run the checklist: same asset ✓, same $105,000 and `≥` ✓, same close instant/TZ ✓, **same source?** — if PM uses one index and Kalshi another, flag it in rationale or reject. YES==YES so `invert: false`.
4. Only if all green, add the row.

---

## Rule zero (again, because it matters)

Conservative beats comprehensive. A wrong match doesn't just add noise — it adds *confident* fake signal that the sizing layer will happily bet real money on. Exclude anything you can't verify end to end.
