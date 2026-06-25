# Trading Strategy — Evidence-Reviewed (v2)

**Status:** revised after literature review (June 2026). Supersedes the informal v1 in `ENGINEERING_PLAN.md`.
**Principle:** every component below was checked against academic + sharp-practitioner literature. Where v1 was wrong, it's corrected and the citation is given. We apply known methods rigorously; we do not invent.

---

## What the review changed (read this first)

| Component | v1 (intuition) | v2 (evidence-based) | Why |
|---|---|---|---|
| Edge metric | CLV as first-class | **Brier/log-loss over many resolved markets** primary; CLV demoted to cross-venue diagnostic | No sharp "close" on Polymarket; inefficiency *rises* near resolution (anti-convergence) |
| Signal blend | Inverse-variance on probabilities | **Geometric mean of odds (log-odds pool) + modest extremizing** | Averaging probs in [0,1] is underconfident & uncalibrated |
| Market-as-prior | Update on all signals | Update **only on the orthogonal/private component** | Grossman-Stiglitz: public info already in price → double-counting |
| Behavioral edge | Fade partisan flow | **Buy compressed favorites; fade deep longshots** | Political markets compress toward 50% (underconfidence), not one-sided overpricing |
| Statistical edge rank | OBI/microstructure first | **Cross-venue deviation vs sharper book (Pinnacle/Betfair) first** | Polymarket lags the sharpest venues; it is not the price leader |

---

## The model — five layers

Statistical edge says **WHAT and HOW MUCH**; microstructure says **WHEN and HOW to execute**. Kept separate so PnL is attributable.

### Layer 1 — Fair-value estimator
Output a posterior `p̂` and its uncertainty per market. The market microprice is the **prior**, and it's a *strong* one (prediction markets match or beat polls), so we anchor on it and move only with genuinely private evidence.

- **Blend on the log-odds scale**, not raw probabilities: combine signals via the **geometric mean of odds** (logarithmic opinion pool), then apply an **extremizing factor ≈1.5–2.5** — but only to the extent signals are *independent*. Shrink the factor toward 1 when signals overlap or may share a bias. (Satopää/Mellers/Tetlock; GJP best practice.)
- **Double-counting guard (critical):** decompose each signal into the part already implied by the market price vs the orthogonal/private part. Update `p̂` off the price **only by the orthogonal component.** A public poll everyone sees is mostly already in the price — re-adding it at full weight over-updates. (Grossman-Stiglitz; informational substitutes.)
- **Known-bias priors:** apply documented structural corrections — fade deep longshots at the tails (longshot overpricing) and lean into mid-range political favorites (compression toward 50% ⇒ favorites underpriced). Regime-dependent; validate per market type, don't apply blindly.

Signal sources, in order of demonstrated sharpness:
1. **Cross-venue reference** — Pinnacle/Betfair closing/exchange lines are the efficiency benchmark (Pinnacle football r²≈0.997). Polymarket *lags* these. Trade Polymarket's deviation from the sharper venue. Highest-precision input.
2. Favorite–longshot / compression corrections (above).
3. Own forecasting model only where it has a genuine niche edge (sports Elo, econ nowcast).

### Layer 2 — Edge gate
`edge = p̂ − microprice`. Trade only when `|edge| > cost + adverse_selection + k·σ(p̂)`. Wide posterior ⇒ *raise* the threshold, don't just shrink size. Uncertain alpha doesn't trade.

### Layer 3 — Execution policy (microstructure)
Microstructure is **cost reduction, not alpha**, on a venue this slow. Post passively (capture spread) when edge is small and not decaying; cross the book when edge is large or time-sensitive. Use order-book imbalance / microprice to *time entry and dodge adverse selection only*. (We still build the OBI→drift signal but expect it not to clear fees — let the backtest decide.)

### Layer 4 — Sizing
- **Fractional Kelly with a hard haircut (≤¼).** Justified by estimation uncertainty + risk aversion, not folklore. Overestimated edge ⇒ catastrophic overbetting; sizing, not edge selection, is what kills most Polymarket accounts.
- **Simultaneous / vector Kelly:** size the portfolio jointly, `w* ∝ Σ⁻¹μ`. Positively-correlated bets must be treated as one larger exposure — correlation sharply *reduces* per-bet size. (Thorp.) For *mutually-exclusive* outcomes within one market, the joint optimum is **more** aggressive than per-bet Kelly and may include hedge legs. (Whelan 2023.)
- **Opportunity-cost-adjusted edge for locked capital:** capital is frozen until resolution and can't compound, breaking Kelly's reinvestment assumption. Discount edge by the risk-free return over the lockup and treat locked margin as reducing effective bankroll. A 2¢ edge locked 3 months can lose to a T-bill — annualize before sizing.
- **Capacity cap:** never size beyond book depth. This binds long before Kelly does.

### Layer 5 — Measurement & risk (the discipline layer)
- **Primary edge metric: proper scoring rules** — Brier and log-loss of `p̂` vs realized outcomes, across *many* resolved markets (single markets are uninformative). Calibration curves *only alongside* a proper score (calibration alone is trivially gamed).
- **Secondary diagnostic: cross-venue CLV** — did our entry beat the sharper reference venue's later price? Use as a faster, noisier check; do **not** rely on it as primary (no sharp close on Polymarket; anti-convergence near resolution).
- **Execution attribution: implementation shortfall** — did fills beat arrival mid? Decompose every trade into `alpha + execution + noise` so we know which layer breaks when it bleeds.
- **Risk:** drawdown kill-switch, per-market & aggregate inventory caps, cross-market correlation cap, capital-lockup accounting.
- **Sample-size reality:** establishing edge needs hundreds of resolved markets via proper scoring; do not conclude edge from a handful of wins. Most wallets that look skilled are riding variance.

---

## Honest edge ranking (what actually pays here)

1. **Structural arbitrage** (outcomes not summing to $1, linked-market dominance violations) — *canary first*: run the scanner to measure how inefficient the market is. Mostly competed in liquid markets; survives in thin/linked ones.
2. **Cross-venue deviation** vs Pinnacle/Betfair — highest-Sharpe statistical signal, but windows are seconds-to-minutes and fee-sensitive; needs low-latency plumbing and capacity is tiny.
3. **Compression / longshot corrections** — buy underpriced political favorites; fade deep longshots. Modest, regime-dependent.
4. **Passive spread capture** with OBI-skewed quotes — harvest while waiting; risk is adverse selection.
5. **OBI momentum** — research only; expected to fail net of fees.

**Binding constraint is capacity, not edge.** These markets absorb little capital — which is exactly why pros ignore them and a careful student can profit. Don't plan to scale.

---

## Plan deltas for Codex (update to `ENGINEERING_PLAN.md`)

- **WP-3 (signal):** first signal is **cross-venue deviation** (Polymarket vs a sharper reference), not OBI. Blend via log-odds pool + extremizing. Implement the double-counting decomposition.
- **WP-4 (eval):** primary metrics are **Brier / log-loss / calibration** over resolved markets + **implementation shortfall**; CLV-vs-reference is a secondary panel. (Previously CLV was primary — corrected.)
- **WP-5 (sizing):** vector/simultaneous Kelly with covariance, fractional haircut ≤¼, opportunity-cost-adjusted edge, capacity cap.
- **WP-2 (backtester):** must model fees + gas + realistic fills end-to-end; every metric above is meaningless without it.

---

## Sources

Kelly / sizing: Whelan 2023 (karlwhelan.com/Papers/KellyJuly2023.pdf); Thorp (gwern.net/doc/statistics/decision/2006-thorp.pdf); MacLean–Thorp–Ziemba (stat.berkeley.edu/~aldous/157/Papers/Good_Bad_Kelly.pdf); Busseti–Ryu–Boyd, risk-constrained Kelly (arxiv.org/pdf/1603.06183); Meister, Kelly for prediction markets (arxiv.org/html/2412.14144v1).
CLV / metrics: Buchdahl via Pinnacle (pinnacleoddsdropper.com/blog/closing-line-value--clv-demystified-by-expert-joseph-buchdahl); Polymarket inefficiency study (dlnews.com/articles/markets/polymarket-kalshi-prediction-markets-not-so-reliable-says-study); Follymarket forecast assessment (nickhaubri.ch/blog/follymarket-polymarket-forecast-assessment).
Aggregation: Satopää et al. (arxiv.org/pdf/1506.06405); "two reasons to extremize" (faculty.wharton.upenn.edu/wp-content/uploads/2015/07/2015---two-reasons-to-make-aggregated-probability-forecasts_1.pdf); geometric-mean-of-odds (forum.effectivealtruism.org/posts/sMjcjnnpoAQCcedL2); GJP science (goodjudgment.com/about/the-science-of-superforecasting).
Efficiency / prior: Grossman-Stiglitz overview (optionstradingiq.com/grossman-stiglitz-paradox); informational substitutes (arxiv.org/pdf/1703.08636); Anatomy of Polymarket (arxiv.org/html/2603.03136v1); whales bleed EV (papers.ssrn.com/sol3/papers.cfm?abstract_id=6322678).
Edges: Price Discovery in Modern Prediction Markets (papers.ssrn.com/sol3/papers.cfm?abstract_id=5331995); Le 2026, Decomposing Crowd Wisdom / compression (arxiv.org/html/2602.19520v1); Pinnacle closing-line efficiency (football-data.co.uk/blog/pinnacle_efficiency.php); QuantPedia FLB review (quantpedia.com/systematic-edges-in-prediction-markets); Whelan Kalshi maker/taker (karlwhelan.com/Papers/Kalshi.pdf).
