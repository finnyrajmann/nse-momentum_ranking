"""
NSE Momentum Daily Rank Snapshot — DO Functions Entry Point
=============================================================
Runs every trading day before market open. Scores the full watchlist using
the same momentum formula as the monthly rebalance system, and appends one
rank snapshot row per stock to a growing history file on GitHub.

    Score = (1M return + 3M return + 6M return) / 1M volatility

This job does NOT rebalance, does NOT touch positions, and does NOT affect
the monthly momentum system in nse-momentum. It exists purely to build a
day-by-day rank history for the bump-chart visualization.

Fetch strategy: a single batched yf.download() call per run (threads=False),
avoiding the per-symbol concurrent-fetch data corruption seen in earlier
systems (same fix applied in trade-data-analysis).

Requires: yfinance (see requirements.txt in this function's folder)
"""

import os
import csv
import base64
import smtplib
import traceback
from io import StringIO
from datetime import datetime, date
import requests
import yfinance as yf
from email.mime.text import MIMEText

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

DAYS_1M = 21
DAYS_3M = 63
DAYS_6M = 126

WATCHLIST_FILE = 'data/watchlist.csv'
HISTORY_FILE   = 'data/rank_history.csv'
MARKET_CHECK_SYM = 'RELIANCE'

HISTORY_FIELDS = ['date', 'symbol', 'rank', 'score', 'price',
                   'r1m', 'r3m', 'r6m', 'vol1m']


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
# MARKET DAY CHECK
# ─────────────────────────────────────────────

def is_market_open():
    """Quick single-symbol check to detect trading holidays/weekends."""
    try:
        df = yf.download(MARKET_CHECK_SYM + '.NS', period='5d', interval='1d',
                          threads=False, progress=False, auto_adjust=True)
        if df.empty:
            return False
        last_date = df.index[-1].date()
        today = date.today()
        print(f'  Market check: last bar = {last_date}, today = {today}')
        return last_date == today
    except Exception as e:
        print(f'  Market check failed: {e}')
        return False


# ─────────────────────────────────────────────
# BATCHED MOMENTUM SCORING
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


def score_universe(symbols):
    """
    Single batched download for the whole watchlist, then score each symbol
    from the in-memory result. No per-symbol network calls — this is the
    fix for the concurrent-fetch corruption issue seen in earlier systems.
    """
    tickers = [s.strip().upper() + '.NS' for s in symbols]
    print(f'  Batched download: {len(tickers)} tickers...')

    data = yf.download(
        tickers, period='1y', interval='1d',
        group_by='ticker', threads=False, progress=False, auto_adjust=True
    )

    scored = []
    for symbol, ticker in zip(symbols, tickers):
        try:
            if len(tickers) == 1:
                closes = data['Close'].dropna().tolist()
            else:
                closes = data[ticker]['Close'].dropna().tolist()
        except Exception:
            continue

        if len(closes) < DAYS_6M + 5:
            continue

        r1m = compute_return(closes, DAYS_1M)
        r3m = compute_return(closes, DAYS_3M)
        r6m = compute_return(closes, DAYS_6M)
        vol1m = compute_volatility(closes, DAYS_1M)

        if any(v is None for v in [r1m, r3m, r6m, vol1m]) or vol1m == 0:
            continue

        score = (r1m + r3m + r6m) / vol1m
        scored.append({
            'symbol': symbol.strip().upper(),
            'score': round(score, 4),
            'price': round(closes[-1], 2),
            'r1m': round(r1m, 2),
            'r3m': round(r3m, 2),
            'r6m': round(r6m, 2),
            'vol1m': round(vol1m, 4),
        })

    scored.sort(key=lambda x: x['score'], reverse=True)
    for i, s in enumerate(scored):
        s['rank'] = i + 1

    print(f'  Scored {len(scored)}/{len(symbols)} symbols')
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
    print('  NSE MOMENTUM \u2014 DAILY RANK SNAPSHOT')
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

        print('\n[3/4] Scoring universe (batched fetch)...')
        ranked = score_universe(symbols)
        if len(ranked) < 50:
            msg = f'Only {len(ranked)} stocks scored \u2014 too few, skipping write'
            print(f'  ERROR: {msg}')
            send_failure_email(msg)
            return {'statusCode': 500, 'body': msg}

        print('\n[4/4] Appending to rank history on GitHub...')
        hist_content, hist_sha = github_get(repo_name, HISTORY_FILE, pat)
        existing_rows = parse_csv(hist_content)

        new_rows = [
            {
                'date': today_str,
                'symbol': s['symbol'],
                'rank': s['rank'],
                'score': s['score'],
                'price': s['price'],
                'r1m': s['r1m'],
                'r3m': s['r3m'],
                'r6m': s['r6m'],
                'vol1m': s['vol1m'],
            }
            for s in ranked
        ]

        all_rows = existing_rows + new_rows
        github_put(
            repo_name, HISTORY_FILE, pat,
            to_csv(all_rows, HISTORY_FIELDS),
            hist_sha, f'Rank snapshot \u2014 {today_str} ({len(new_rows)} stocks)'
        )

        print(f'\n  Done. Wrote {len(new_rows)} rows for {today_str}.\n')
        return {'statusCode': 200, 'body': f'Wrote {len(new_rows)} rows'}

    except Exception as e:
        error_text = f'{str(e)}\n\n{traceback.format_exc()}'
        print(f'\n  ERROR: {error_text}')
        send_failure_email(error_text)
        return {'statusCode': 500, 'body': str(e)}
