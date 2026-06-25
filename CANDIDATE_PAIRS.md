# Draft Candidate Pairs — live scan 2026-06-25

Best-effort scan of live Polymarket + Kalshi short-horizon markets. **Everything here needs your final end-to-end verify before going in the real allowlist** (your money, your call). IDs for 6/25–6/26 events may resolve before you finish setup — treat specific rows as *format templates* and refresh IDs for current games.

---

## Headline finding: the "obvious" crypto overlap is contaminated — do NOT use it

Daily BTC/ETH threshold markets (e.g. "BTC above $58,000 on June 26") exist on both venues and look like a clean match. **They are not**, on two axes that fail the checklist:

- **Different settlement instant.** Polymarket resolves off the Binance BTC/USDT 1-minute close at **noon ET (16:00Z)**. The matching Kalshi daily resolves at **5 PM EDT (21:00Z)** — ~5 hours apart, and no Kalshi daily resolves at noon ET.
- **Different price feed.** Polymarket = Binance BTC/USDT spot, single 1-min close. Kalshi = **CF Benchmarks BRTI** (60-second index average). Different number, different methodology — they can resolve *oppositely* near the strike.

This fails checklist #3 (same instant) and #4 (same source). The conservative mapper would correctly **reject** these. They're a *correlation*, not the same event — feeding them to the cross-venue signal would inject garbage. **Excluded from the seed allowlist.** (This is exactly the failure mode the checklist exists to catch.)

---

## Recommended seed universe: World Cup single-team match-winner legs

These are the **cleanest verified matches** and — conveniently — a renewable daily stream of fast-resolving markets over your whole recording window. Ideal validation set.

**Mapping rule (mechanical):**
- Polymarket: "Will [Team] win on [DATE]?" — binary YES/NO, YES = that team wins in regulation.
- Kalshi: `KXWCGAME-<DATE><MATCHUP>-<TEAM>` — the single-team leg of a 3-way (Team / Tie / Opponent).
- Pair **only the matching team leg**; YES==YES so `invert: false`.
- **Do NOT** map Polymarket-NO to Kalshi's opponent leg — PM-NO = (draw OR opponent wins), which is the Tie+Opponent legs combined, not a single Kalshi market.

**Per-pair verify before adding:**
1. Both settle on **regulation time** (90' + stoppage, excluding ET/penalties). Group games can't go to penalties so it's usually moot, but confirm for any knockout pairs.
2. Same match, same date.
3. Kalshi `close_time` is the bracket-wide date, NOT match end — don't read it as the event time; settlement is at match conclusion.

### Format-example rows (from 6/25 games — refresh IDs for current games)

```jsonl
{"polymarket_condition_id": "0x32552863fe8014da2613037970661f0d49f415c86a88cb3d65af7a43395bc4ad", "polymarket_asset_id": "47236739815607347436394828740644657912816815268002585518946427187074399713739", "kalshi_ticker": "KXWCGAME-26JUN25TURUSA-USA", "invert": false, "polymarket_yes_payout": "USA wins Turkiye-USA in regulation, 2026-06-25", "kalshi_yes_payout": "USA wins Turkiye-USA", "rationale": "Same match, same team leg, YES==YES; verify regulation-time settlement"}
{"polymarket_condition_id": "0xe690620297dfea974d20df84b4cf90460e46a26ec864353482717b90509a3c0b", "polymarket_asset_id": "101279272797775203956659007084828831261331275405585994446661991301016814257223", "kalshi_ticker": "KXWCGAME-26JUN25ECUGER-GER", "invert": false, "polymarket_yes_payout": "Germany wins Ecuador-Germany in regulation, 2026-06-25", "kalshi_yes_payout": "Germany wins Ecuador-Germany", "rationale": "Same match, same team leg, YES==YES; verify regulation-time settlement"}
```

### Upcoming games to seed fresh (Kalshi tickers; pull matching Polymarket conditionId/YES-token from Gamma the day of)
- 6/26: `KXWCGAME-26JUN26SENIRQ-{SEN,IRQ}` (Senegal v Iraq)
- 6/27: `KXWCGAME-26JUN27CODUZB-{COD,UZB}` (Congo DR v Uzbekistan)
- 6/28: `KXWCGAME-26JUN28RSACAN-{RSA,CAN}` (South Africa v Canada)

To get the Polymarket side for a current game: query Gamma `/markets` for "Will [Team] win on [DATE]?", take `conditionId` + the YES `clobTokenId` (mind the `outcomes` array order).

---

## What the scan did NOT find / check
- **No CPI / Fed / jobs** economic markets in the short-horizon window on either venue right now.
- **No same-day single-game sports outside the World Cup** were cross-checked.
- **No liquidity/order-book** check — these are event matches only, not a spread/depth assessment. Thin pairs won't be tradable even if they match.

## Practical takeaway
Seed the allowlist with **World Cup single-team legs**, refreshed as games come up (there's a steady supply for weeks). Skip crypto until/unless you find a Kalshi event whose instant *and* feed match Polymarket's — none currently do. This gives you a clean, fast-resolving validation set, which is exactly what WP-4 needs to produce a real Brier verdict.
