# nse-momentum-rank-history

Daily momentum rank snapshot for all stocks in the watchlist, feeding a
bump-chart visualization of how rankings shift day to day.

This is a **read-only companion** to the monthly `nse-momentum` rebalance
system — same scoring formula, no shared state, no positions, no
rebalancing. It exists purely to build up `data/rank_history.csv` one row
per stock per trading day.

## Setup

1. Copy your current watchlist into `data/watchlist.csv` (same `Symbol`
   column format as `nse-momentum`).
2. Copy `functions/project.yml` from the template in this repo (or ask
   Claude to regenerate it) and fill in your own credentials. **Never commit
   this file** — it's gitignored on purpose.
3. Deploy with `doctl serverless deploy functions/`.
4. Confirm a scheduled trigger is attached (see open question on which
   namespace/account has a free trigger slot).

## Data format — `data/rank_history.csv`

| column | meaning |
|---|---|
| date | trading date, `YYYY-MM-DD` |
| symbol | NSE symbol |
| rank | rank that day (1 = best) |
| score | `(1M + 3M + 6M return) / 1M volatility` |
| price | close price that day |
| r1m / r3m / r6m | trailing returns (%) |
| vol1m | trailing 1-month volatility |

## Known limitation

The daily job does a full read-modify-write of `rank_history.csv` via the
GitHub Contents API on every run. That API caps file content at ~1MB.
With ~450 stocks/day, that's roughly 1–2 years of daily history before
this needs to move to either a monthly-file split or the Git Data API for
larger commits. Not urgent, but worth revisiting once the file starts
approaching that size.
