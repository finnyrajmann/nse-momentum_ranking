"""
NSE Momentum Daily Rank Snapshot — DO Functions Entry Point
=============================================================
Runs every trading day before market open. Scores the full watchlist using
TWO momentum formulas from the same fetched price data, and appends one
rank snapshot row per stock, per formula, to two growing history files on
GitHub.

    Standard : (1M return + 3M return + 6M return) / 1M volatility
    Short    : (5D return + 10D return + 15D return) / 5D volatility

Both formulas score off the same single Yahoo fetch per symbol — no extra
API calls for the short formula, it's just a different set of lookback
windows applied to the same closing-price series.

This job does NOT rebalance, does NOT touch positions, and does NOT affect
the monthly momentum system in nse-momentum. It exists purely to build a
day-by-day rank history for the bump-chart visualization.

Fetch strategy: sequential per-symbol requests via Yahoo's chart API
(same as the monthly rebalance system's fetch_closes()), with a sleep
between calls. No yfinance dependency — the earlier concurrent-fetch data
corruption issue (fixed in trade-data-analysis) came from parallel threads,
not from this approach, so a plain sequential fetch never had that problem.
This also keeps the deployed function well under DO's 48MB build size
limit, which yfinance's dependency chain (notably curl_cffi) blew past.

No pip installs needed — requests + standard library only, same as your
other four working systems.
"""

import os
import csv
import time
import base64
import smtplib
import traceback
from io import StringIO
from datetime import datetime, date
import requests
from email.mime.text import MIMEText

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

SLEEP = 0.3  # seconds between Yahoo Finance calls, same as monthly system

WATCHLIST_FILE = 'data/watchlist.csv'

# Score formula profiles. Both are computed from a single fetched closing
# price series per symbol — 'windows' are the return lookbacks (summed),
# 'vol_window' is the lookback for the volatility (std dev of daily
# returns) denominator.
PROFILES = {
    'standard': {
        'windows': [21, 63, 126],       # ~1M, ~3M, ~6M trading days
        'vol_window': 21,               # ~1M
        'history_file': 'data/rank_history.csv',
        'fields': ['date', 'symbol', 'rank', 'score', 'price',
                   'r1m', 'r3m', 'r6m', 'vol1m'],
        'return_keys': ['r1m', 'r3m', 'r6m'],
        'vol_key': 'vol1m',
    },
    'short': {
        'windows': [5, 10, 15],         # 5D, 10D, 15D
        'vol_window': 5,                # 5D
        'history_file': 'data/rank_history_short.csv',
        'fields': ['date', 'symbol', 'rank', 'score', 'price',
                   'ret5', 'ret10', 'ret15', 'vol5'],
        'return_keys': ['ret5', 'ret10', 'ret15'],
        'vol_key': 'vol5',
    },
}

# Need enough bars for the deepest lookback across all profiles, plus a
# small buffer, or a symbol gets skipped entirely (both formulas).
MAX_LOOKBACK = max(w for p in PROFILES.values() for w in p['windows'])
MIN_BARS_REQUIRED = MAX_LOOKBACK + 5


# ─────────────────────────────────────────────
# GITHUB REST API
# ─────────────────────────────────────────────

def github_get(repo, path, pat):
    url = f'https://api.github.com/repos/{repo}/contents/{path}'
    headers = {'Authorization': f'token {pat}',
               'Accept': 'application/vnd.github.v3+json'}
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    content = base64.b64decode(data['content']).decode('utf-8')
    return content, data['sha']


def github_put(repo, path, pat, content, sha, message):
    url = f'https://api.github.com/repos/{repo}/contents/{path}'
    headers = {'Authorization': f'token {pat}',
               'Accept': 'application/vnd.github.v3+json'}
    payload = {
        'message': message,
        'content': base64.b64encode(content.encode('utf-8')).decode('utf-8'),
        'sha': sha,
    }
    r = requests.put(url, headers=headers, json=payload, timeout=15)
    r.raise_for_status()


def parse_csv(content):
    return list(csv.DictReader(StringIO(content)))


def to_csv(rows, fieldnames):
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


# ─────────────────────────────────────────────
# YAHOO FINANCE (raw requests, sequential — same pattern as monthly system)
# ─────────────────────────────────────────────

def fetch_closes(symbol, period='1y'):
    """Fetch daily close prices for a symbol. Returns list of (timestamp, close) or None."""
    ticker  = symbol.upper().strip() + '.NS'
    url     = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}'
    params  = {'range': period, 'interval': '1d', 'events': 'history'}
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r    = requests.get(url, params=params, headers=headers, timeout=15)
        data = r.json()
        res  = data['chart']['result'][0]
        closes = res['indicators']['quote'][0]['close']
        timestamps = res['timestamp']
        pairs = [(t, c) for t, c in zip(timestamps, closes) if c is not None]
        return pairs
    except Exception as e:
        print(f'  {symbol}: fetch error — {e}')
        return None


def is_market_open():
    """
    Weekday check only. We run before market open (8:50 AM IST), so
    comparing the last fetched bar's date to "today" always fails —
    today's candle doesn't exist yet at that hour, holiday or not.
    Trade-off: this won't detect actual NSE holidays, so a holiday run
    will just re-write yesterday's closing prices under today's date
    (a harmless flat/no-movement row in the chart), rather than the
    prior bug of silently skipping every single day forever.
    """
    today = date.today()
    is_weekday = today.weekday() < 5  # Mon=0 ... Fri=4
    print(f'  Market check: today = {today} ({today.strftime("%A")}), weekday = {is_weekday}')
    return is_weekday


# ─────────────────────────────────────────────
# MOMENTUM SCORING
# ─────────────────────────────────────────────

def compute_return(closes, lookback_days):
    if len(closes) < lookback_days + 1:
        return None
    price_now, price_then = closes[-1], closes[-(lookback_days + 1)]
    if price_then == 0:
        return None
    return (price_now - price_then) / price_then * 100


def compute_volatility(closes, lookback_days):
    if len(closes) < lookback_days + 1:
        return None
    subset = closes[-(lookback_days + 1):]
    daily_returns = [
        (subset[i] - subset[i - 1]) / subset[i - 1] * 100
        for i in range(1, len(subset)) if subset[i - 1] != 0
    ]
    if len(daily_returns) < 5:
        return None
    n = len(daily_returns)
    mean = sum(daily_returns) / n
    variance = sum((r - mean) ** 2 for r in daily_returns) / (n - 1)
    return variance ** 0.5


def score_stock_all_profiles(symbol):
    """
    Fetch a symbol's price history ONCE, then score it under every profile
    in PROFILES using that same series. Returns a dict keyed by profile
    name -> result dict, omitting any profile that couldn't be scored
    (e.g. insufficient bars, zero volatility). Returns None if the fetch
    itself failed or there's not enough history for any profile at all.
    """
    pairs = fetch_closes(symbol, period='1y')
    time.sleep(SLEEP)
    if not pairs or len(pairs) < MIN_BARS_REQUIRED:
        return None

    closes = [c for _, c in pairs]
    price = closes[-1]

    results = {}
    for profile_name, cfg in PROFILES.items():
        returns = [compute_return(closes, w) for w in cfg['windows']]
        vol = compute_volatility(closes, cfg['vol_window'])
        if any(r is None for r in returns) or vol is None or vol == 0:
            continue
        score = sum(returns) / vol
        row = {
            'symbol': symbol.strip().upper(),
            'price': round(price, 2),
            'score': round(score, 4),
        }
        for key, val in zip(cfg['return_keys'], returns):
            row[key] = round(val, 2)
        row[cfg['vol_key']] = round(vol, 4)
        results[profile_name] = row

    return results if results else None


def score_universe(symbols):
    """
    Score and rank all stocks sequentially — one Yahoo request at a time,
    same pattern as the monthly rebalance system's rank_universe(). Scores
    every profile per symbol from that single fetch, then ranks each
    profile's list independently (a symbol failing one profile's minimum
    bar count doesn't block it from the other).
    """
    print(f'  Scoring {len(symbols)} stocks (sequential, all profiles per fetch)...')
    scored = {name: [] for name in PROFILES}

    for i, symbol in enumerate(symbols):
        results = score_stock_all_profiles(symbol)
        if results:
            for profile_name, row in results.items():
                scored[profile_name].append(row)
        if (i + 1) % 50 == 0:
            counts = ', '.join(f'{k}={len(v)}' for k, v in scored.items())
            print(f'  Progress: {i + 1}/{len(symbols)} — scored so far: {counts}')

    for profile_name, rows in scored.items():
        rows.sort(key=lambda x: x['score'], reverse=True)
        for i, s in enumerate(rows):
            s['rank'] = i + 1
        print(f'  [{profile_name}] scored {len(rows)}/{len(symbols)} symbols')

    return scored


# ─────────────────────────────────────────────
# FAILURE ALERT (success is silent by design)
# ─────────────────────────────────────────────

def send_failure_email(error_text):
    sender = os.environ.get('GMAIL_SENDER')
    password = os.environ.get('GMAIL_APP_PASSWORD')
    recipient = os.environ.get('GMAIL_RECIPIENT')
    if not all([sender, password, recipient]):
        print('  Email env vars missing, skipping failure alert.')
        return
    today = datetime.now().strftime('%d %b %Y')
    msg = MIMEText(f'Momentum daily rank snapshot failed on {today}.\n\n{error_text}')
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = f'\u26a0\ufe0f Momentum Rank Snapshot FAILED \u2014 {today}'
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        print('  Failure alert sent.')
    except Exception as e:
        print(f'  Could not send failure alert: {e}')


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main(args):
    print('\n' + '=' * 55)
    print('  NSE MOMENTUM \u2014 DAILY RANK SNAPSHOT (dual formula)')
    print('=' * 55)

    pat = os.environ.get('GITHUB_PAT')
    repo_name = os.environ.get('GITHUB_REPO')
    today_str = datetime.now().strftime('%Y-%m-%d')

    try:
        print('\n[1/4] Checking market status...')
        if not is_market_open():
            print('  Market closed today \u2014 skipping.')
            return {'statusCode': 200, 'body': 'Market closed'}

        print('\n[2/4] Loading watchlist...')
        wl_content, _ = github_get(repo_name, WATCHLIST_FILE, pat)
        watchlist = parse_csv(wl_content)
        symbols = [row['Symbol'] for row in watchlist]
        print(f'  Watchlist: {len(symbols)} symbols')

        print('\n[3/4] Scoring universe (sequential fetch, both formulas)...')
        scored = score_universe(symbols)

        # Guard each profile independently — if one formula comes up short
        # (e.g. widespread data gaps for its longer lookback), skip only
        # that profile's write rather than failing the whole run.
        write_plan = {}
        for profile_name, rows in scored.items():
            if len(rows) < 50:
                msg = f'[{profile_name}] only {len(rows)} stocks scored \u2014 too few, skipping write'
                print(f'  WARNING: {msg}')
                continue
            write_plan[profile_name] = rows

        if not write_plan:
            msg = 'No profile had enough scored stocks \u2014 skipping all writes'
            print(f'  ERROR: {msg}')
            send_failure_email(msg)
            return {'statusCode': 500, 'body': msg}

        print('\n[4/4] Appending to rank history file(s) on GitHub...')
        total_written = {}
        for profile_name, rows in write_plan.items():
            cfg = PROFILES[profile_name]
            hist_file = cfg['history_file']
            fields = cfg['fields']

            hist_content, hist_sha = github_get(repo_name, hist_file, pat)
            existing_rows = parse_csv(hist_content)

            new_rows = [dict(row, date=today_str) for row in rows]
            all_rows = existing_rows + new_rows

            github_put(
                repo_name, hist_file, pat,
                to_csv(all_rows, fields),
                hist_sha, f'[{profile_name}] Rank snapshot \u2014 {today_str} ({len(new_rows)} stocks)'
            )
            total_written[profile_name] = len(new_rows)
            print(f'  [{profile_name}] wrote {len(new_rows)} rows to {hist_file}')

        summary = ', '.join(f'{k}={v}' for k, v in total_written.items())
        print(f'\n  Done. {summary}\n')
        return {'statusCode': 200, 'body': f'Wrote rows: {summary}'}

    except Exception as e:
        error_text = f'{str(e)}\n\n{traceback.format_exc()}'
        print(f'\n  ERROR: {error_text}')
        send_failure_email(error_text)
        return {'statusCode': 500, 'body': str(e)}
