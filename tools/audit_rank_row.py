#!/usr/bin/env python3
"""
Read-only audit: re-compute one day's stored ranks from completed-session data
and compare with what the daily job wrote.

For the chosen date D it fetches each stock's bars, DROPS every bar dated on or
after D, re-runs the same scoring maths the function uses, and compares price,
score, returns, volatility (and, on a full run, rank) with the stored row.

For every stock that differs it also tests two explanations:
  * "stub bar"      - the stored numbers are what you get if a placeholder bar
                      (a copy of the last close) had been appended;
  * "latest missing" - the stored numbers are what you get if the newest
                      completed bar had been missing (stale data).

Nothing is written anywhere. Run from the repo root:

    python tools/audit_rank_row.py                    # latest date, standard formula, all stocks (~5 min)
    python tools/audit_rank_row.py --sample 60        # quick spot check (~1 min; no rank comparison)
    python tools/audit_rank_row.py --profile short    # the 10D/15D/1M file
    python tools/audit_rank_row.py --date 2026-11-17
"""
import argparse
import csv
import importlib.util
import os
import random
import sys
import time
from collections import Counter

FN_PATH = 'functions/packages/nse_momentum_ranking/daily_run/__main__.py'


def load_function_module():
    if not os.path.exists(FN_PATH):
        sys.exit(f'{FN_PATH} not found — run this from the repo root.')
    spec = importlib.util.spec_from_file_location('daily_run_main', FN_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def recompute(fn, cfg, closes):
    """Same arithmetic and rounding as score_stock_all_profiles, for one profile."""
    if len(closes) < cfg['min_bars']:
        return None
    returns = [fn.compute_return(closes, w) for w in cfg['windows']]
    vol = fn.compute_volatility(closes, cfg['vol_window'])
    if any(r is None for r in returns) or vol is None or vol == 0:
        return None
    row = {'price': round(closes[-1], 2), 'score': round(sum(returns) / vol, 4)}
    for key, val in zip(cfg['return_keys'], returns):
        row[key] = round(val, 2)
    row[cfg['vol_key']] = round(vol, 4)
    return row


def differs(stored, mine, keys):
    return [k for k in keys if abs(float(stored[k]) - mine[k]) > 1e-6]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='run date to audit (default: latest in the file)')
    ap.add_argument('--profile', default='standard', choices=['standard', 'short'])
    ap.add_argument('--sample', type=int, default=0, help='check only N random stocks')
    ap.add_argument('--sleep', type=float, default=0.3)
    args = ap.parse_args()

    fn = load_function_module()
    cfg = fn.PROFILES[args.profile]
    path = cfg['history_file']
    keys = ['price', 'score'] + cfg['return_keys'] + [cfg['vol_key']]

    with open(path, newline='', encoding='utf-8') as f:
        all_rows = list(csv.DictReader(f))
    day = args.date or max(r['date'] for r in all_rows)
    stored = {r['symbol']: r for r in all_rows if r['date'] == day}
    if not stored:
        sys.exit(f'No rows dated {day} in {path}.')
    before = fn.parse_date(day)
    symbols = sorted(stored)
    full_run = not args.sample or args.sample >= len(symbols)
    if not full_run:
        symbols = sorted(random.Random(1).sample(symbols, args.sample))
    print(f'Auditing {path} for {day} ({args.profile}): {len(stored)} stored rows, checking {len(symbols)}')

    last_dates, no_hl, dropped = Counter(), [], []
    recomputed, unscorable, fetch_failed = {}, [], []
    for i, sym in enumerate(symbols, 1):
        bars = fn.fetch_closes(sym, period='1y')
        time.sleep(args.sleep)
        if not bars:
            fetch_failed.append(sym)
            continue
        kept = [b for b in bars if fn.bar_date(b[0]) < before]
        if len(kept) != len(bars):
            dropped.append(sym)
        if not kept:
            unscorable.append(sym)
            continue
        last_dates[fn.bar_date(kept[-1][0]).isoformat()] += 1
        if kept[-1][1] is None or kept[-1][2] is None:
            no_hl.append(sym)
        closes = [b[3] for b in kept]
        mine = recompute(fn, cfg, closes)
        if mine is None:
            unscorable.append(sym)
            continue
        recomputed[sym] = {'row': mine, 'closes': closes}
        if i % 50 == 0:
            print(f'  {i}/{len(symbols)}')

    print(f'\nLatest completed bar before {day}, per stock: ' +
          ', '.join(f'{d}: {n}' for d, n in sorted(last_dates.items(), reverse=True)))
    if dropped:
        print(f'Bars dated {day} or later exist (and were ignored) for {len(dropped)} stock(s) right now '
              f'(expected if you run this after the market has opened).')
    if no_hl:
        print(f'Latest completed bar has no high/low for {len(no_hl)} stock(s): {", ".join(no_hl[:10])}')
    if fetch_failed:
        print(f'Fetch failed for {len(fetch_failed)}: {", ".join(fetch_failed[:10])}')
    if unscorable:
        print(f'Could not re-score {len(unscorable)} (too little history): {", ".join(unscorable[:10])}')

    mismatched, explained = [], Counter()
    for sym, rec in recomputed.items():
        bad = differs(stored[sym], rec['row'], keys)
        if not bad:
            continue
        closes = rec['closes']
        why = 'unexplained'
        stub = recompute(fn, cfg, closes + [closes[-1]])
        stale = recompute(fn, cfg, closes[:-1])
        if stub is not None and not differs(stored[sym], stub, keys):
            why = 'stub bar'
        elif stale is not None and not differs(stored[sym], stale, keys):
            why = 'latest missing'
        explained[why] += 1
        mismatched.append((sym, bad, why))

    checked = len(recomputed)
    print(f'\nCompared {checked} stocks: {checked - len(mismatched)} identical, {len(mismatched)} differ.')
    if mismatched:
        print('Explanations: ' + ', '.join(f'{k}: {v}' for k, v in explained.items()))
        for sym, bad, why in mismatched[:15]:
            parts = ', '.join(f'{k} stored {stored[sym][k]} vs now {recomputed[sym]["row"][k]}' for k in bad[:3])
            print(f'  {sym:<12} [{why}] {parts}')
        if len(mismatched) > 15:
            print(f'  ... and {len(mismatched) - 15} more')

    if full_run and not (fetch_failed or unscorable):
        order = sorted(recomputed, key=lambda s: recomputed[s]['row']['score'], reverse=True)
        mine_rank = {s: i + 1 for i, s in enumerate(order)}
        rank_diff = sum(1 for s in order if int(stored[s]['rank']) != mine_rank[s])
        print(f'Rank: {len(order) - rank_diff}/{len(order)} identical to the stored rank.')

    print('\nVerdict: ' + ('stored rows match completed-session data.' if not mismatched else
          'the stored rows used data that differs from the completed sessions — see the explanations above.'))


if __name__ == '__main__':
    main()
