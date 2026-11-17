#!/usr/bin/env python3
"""
One-off: build data/market_trend.csv for the days before the live job started
recording it.

For every run date in data/rank_history.csv (so the dots line up with the chart
columns exactly, holiday-run duplicates included) it works out what the live
job would have recorded that morning: the market trend as of the last bar
BEFORE that date. It re-uses the daily_run function's own SuperTrend and
summary code, so the numbers follow the identical rules (ATR 10, x3, Wilder;
stocks with fewer than 30 bars excluded; >50% majority; NEUTRAL on a tie).

SuperTrend only looks backwards, so one fetch per symbol (1 year of bars, ~5
minutes for the whole watchlist) is enough to reconstruct every past day with
no look-ahead.

Run from the repo root, after the standard rank backfill is in place (this
script takes its dates from data/rank_history.csv), and not near the 08:50 IST
daily run:

    git pull
    python tools/backfill_market_trend.py --dry-run     # fetch + show, write nothing
    python tools/backfill_market_trend.py               # writes data/market_trend.csv
    git add data/market_trend.csv && git commit -m "Backfill market trend" && git push

Dates already in data/market_trend.csv (e.g. rows written by the live job) are
never changed; for those the script just reports whether its numbers agree.
Safe to re-run.

Approximations (same as any backfill): today's watchlist is applied to past
dates, and Yahoo's split-adjusted history is used, so a past day can differ
slightly from what a live run would have seen at the time.
"""
import argparse
import bisect
import csv
import importlib.util
import os
import sys
import time

FN_PATH = 'functions/packages/nse_momentum_ranking/daily_run/__main__.py'
WATCHLIST_PATH = 'data/watchlist.csv'
HISTORY_PATH = 'data/rank_history.csv'
TREND_PATH = 'data/market_trend.csv'

fn = None   # the daily_run module, loaded in main()


def load_function_module():
    if not os.path.exists(FN_PATH):
        sys.exit(f'{FN_PATH} not found — run this from the repo root.')
    spec = importlib.util.spec_from_file_location('daily_run_main', FN_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_watchlist():
    with open(WATCHLIST_PATH, newline='', encoding='utf-8') as f:
        syms = [(r.get('Symbol') or '').strip() for r in csv.DictReader(f)]
    return list(dict.fromkeys(s for s in syms if s))


def read_run_dates():
    with open(HISTORY_PATH, newline='', encoding='utf-8') as f:
        dates = {r['date'] for r in csv.DictReader(f)}
    return sorted(fn.parse_date(d) for d in dates)


def read_existing_trend(header):
    """Returns {date_str: raw line (no line ending)} for rows already in the file."""
    if not os.path.exists(TREND_PATH):
        return {}
    with open(TREND_PATH, newline='', encoding='utf-8') as f:
        text = f.read()
    lines = [ln.rstrip('\r') for ln in text.split('\n') if ln.strip()]
    if not lines:
        return {}
    if lines[0].strip() != header:
        sys.exit(f'{TREND_PATH}: header does not match.\n  found:    {lines[0]}\n  expected: {header}')
    return {ln.split(',')[0]: ln for ln in lines[1:]}


def fetch_all(symbols, sleep):
    bars_by_symbol, failed = {}, []
    for i, sym in enumerate(symbols, 1):
        bars = fn.fetch_closes(sym, period='1y')
        time.sleep(sleep)
        if bars:
            bars_by_symbol[sym] = bars
        else:
            failed.append(sym)
        if i % 50 == 0:
            print(f'  fetched {i}/{len(symbols)}  (failed so far: {len(failed)})')
    return bars_by_symbol, failed


def direction_map(bars):
    """{bar_date: +1/-1} for the dates on which this stock would have counted live."""
    st = fn.supertrend_series(bars)
    index_of = {fn.bar_date(b[0]): i for i, b in enumerate(bars)}
    out = {}
    for ts, direction, _ in st:
        d = fn.bar_date(ts)
        if index_of[d] + 1 >= fn.ST_MIN_BARS:           # same "enough bars" rule as the live job
            out[d] = direction
    return out


def session_dates(dir_maps, n_symbols):
    """Dates on which at least half the stocks have a reading = real trading sessions."""
    counts = {}
    for m in dir_maps:
        for d in m:
            counts[d] = counts.get(d, 0) + 1
    return sorted(d for d, c in counts.items() if c * 2 >= n_symbols)


def build_rows(dir_maps, run_dates, sessions, n_watchlist):
    rows, skipped = {}, []
    for run_date in run_dates:
        pos = bisect.bisect_left(sessions, run_date)       # sessions strictly before run_date
        if pos == 0:
            skipped.append((run_date, 'no earlier session in the fetched history'))
            continue
        session = sessions[pos - 1]
        readings = [(session, m[session]) for m in dir_maps if session in m]
        if len(readings) < fn.MIN_SCORED_FRACTION * n_watchlist:
            skipped.append((run_date, f'only {len(readings)}/{n_watchlist} stocks have a reading'))
            continue
        row = fn.summarize_market_trend(readings)
        row['date'] = run_date.isoformat()
        rows[row['date']] = row
    return rows, skipped


def line_for(row, fields):
    return ','.join(str(row[f]) for f in fields)


def main():
    global fn
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='fetch and show, but do not write')
    ap.add_argument('--sleep', type=float, default=0.3, help='seconds between Yahoo calls')
    args = ap.parse_args()

    fn = load_function_module()
    fields = fn.TREND_CFG['fields']
    header = ','.join(fields)

    symbols = read_watchlist()
    run_dates = read_run_dates()
    existing = read_existing_trend(header)
    print(f'{len(symbols)} watchlist symbols | {len(run_dates)} run dates '
          f'({run_dates[0]} .. {run_dates[-1]}) | {len(existing)} trend rows already in file')

    print('Fetching 1y of bars per symbol (about 5 minutes)...')
    bars_by_symbol, failed = fetch_all(symbols, args.sleep)
    if failed:
        print(f'  no data for {len(failed)} symbol(s): {", ".join(failed[:20])}')
    if len(bars_by_symbol) < fn.MIN_SCORED_FRACTION * len(symbols):
        sys.exit('Too few symbols fetched — Yahoo may be rate-limiting. Try again later.')

    dir_maps = [direction_map(b) for b in bars_by_symbol.values()]
    sessions = session_dates(dir_maps, len(bars_by_symbol))
    rows, skipped = build_rows(dir_maps, run_dates, sessions, len(symbols))

    new_dates = [d for d in sorted(rows) if d not in existing]
    print(f'\n{"run date":<11} {"session":<11} {"trend":<8} {"up":>5} {"down":>5} {"total":>5}')
    for d in sorted(rows):
        r = rows[d]
        tag = '' if d in new_dates else '  (already in file)'
        print(f'{d:<11} {r["session_date"]:<11} {r["trend"]:<8} {r["up"]:>5} {r["down"]:>5} {r["total"]:>5}{tag}')
    for d, why in skipped:
        print(f'{d}  SKIPPED: {why}')

    # Agreement check against rows the live job already wrote.
    for d in sorted(set(rows) & set(existing)):
        live = existing[d].split(',')
        live_up, live_down = int(live[3]), int(live[4])
        mine = rows[d]
        verdict = 'match' if (live_up, live_down) == (mine['up'], mine['down']) else \
            f'DIFFERS (live {live_up}/{live_down}, backfill {mine["up"]}/{mine["down"]})'
        print(f'check {d}: {verdict}')

    if not new_dates:
        print('\nNothing new to add.')
        return
    ups = sum(1 for d in new_dates if rows[d]['trend'] == 'UP')
    downs = sum(1 for d in new_dates if rows[d]['trend'] == 'DOWN')
    print(f'\n{len(new_dates)} new day(s): {ups} UP, {downs} DOWN, {len(new_dates) - ups - downs} NEUTRAL')

    if args.dry_run:
        print('Dry run — nothing written.')
        return

    combined = dict(existing)
    for d in new_dates:
        combined[d] = line_for(rows[d], fields)
    text = header + '\r\n' + ''.join(combined[d] + '\r\n' for d in sorted(combined))
    with open(TREND_PATH, 'w', newline='', encoding='utf-8') as f:
        f.write(text)
    print(f'Wrote {TREND_PATH} ({len(combined)} rows). Now commit and push (see the docstring).')


if __name__ == '__main__':
    main()
