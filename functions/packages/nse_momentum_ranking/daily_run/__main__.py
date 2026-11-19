"""
NSE Momentum Daily Rank Snapshot — DO Functions Entry Point
=============================================================
Runs every trading day before market open. Scores the full watchlist using
TWO momentum formulas from the same fetched price data, records a daily
market trend, and appends one snapshot per day to growing history files on
GitHub.

    Standard : (1M return + 3M return + 6M return) / 1M volatility
    Short    : (10D return + 15D return + 1M return) / 10D volatility
    Trend    : SuperTrend (ATR 10, x3, Wilder) of every watchlist stock;
               UP/DOWN when more than 50% of stocks agree (NEUTRAL on a tie)

All of it comes from ONE Yahoo fetch per symbol (1 year of daily bars).
This job does NOT rebalance and does NOT touch positions.

Files (all under data/ in the GitHub repo)
------------------------------------------
  rank_history.csv, rank_history_short_v2.csv   one block of rows per run date
  market_trend.csv   date (run date), session_date (the market session the
                     numbers are as of - use this to join against trade logs),
                     trend, up, down, total, pct_up
  nse_holidays.csv   trading calendar (Date,Type,Description)
  run_status.json    outcome of the latest run, read by the viewer

Run dates and sessions
----------------------
A run on date D (before the open) works "as of the previous session": every
bar dated on or after D is ignored, so a placeholder or live intraday bar can
never leak in. The expected session is the previous trading day per the holiday
calendar.

Yahoo's lag and the reconcile step
----------------------------------
Yahoo often has no data yet for the previous session on a third or more of
the stocks at 08:50 IST (rows exist but all values are empty); it fills in
later the same day. So every run does two things:
  1. records today's rows as before (rank rows are provisional; the trend row is
     written only if at least 95% of stocks have a bar for the expected session,
     otherwise it is deferred), and
  2. RECONCILES the last few run dates (default 3): each date whose expected
     session is now complete (>= 95% of stocks) is re-scored from the bars just
     fetched, and its rows are replaced in place if they differ (missing trend
     rows are inserted). Dates that are still incomplete are left alone.
  Nothing outside that window is touched; untouched rows stay byte-identical;
  a replaced block may not change size by more than 10%; the header must match.

Alert policy (no news is good news)
-----------------------------------
Lag, provisional rows, deferred trend rows and reconcile updates are logged,
never emailed. An email goes out only for real problems: an exception; a
failed or refused write (schema mismatch, size guard); Yahoo returning data for
fewer than 80% of the watchlist; fewer than 80% of stocks scoring for a formula;
a run date that is STILL incomplete when it is about to age out of the window;
and calendar problems (missing / expired / ending within 14 days, or NIFTY and
the stocks both disagreeing with the calendar). Every run also writes
data/run_status.json (outcome ok / warning / failed / skipped) so the viewer can
show whether the job ran, and how, without any email.

Invoke with {"dry_run": true} to do everything except write to GitHub, and
{"force": true} to run on a non-session day. {"reconcile_days": 80} widens the
reconcile window (one-off repair of history); 0 turns reconcile off.

No pip installs needed — requests + standard library only.
"""

import os
import csv
import re
import json
import bisect
import time
import base64
import smtplib
import traceback
from io import StringIO
from collections import Counter
from datetime import datetime, date, timedelta, timezone
import requests
from email.header import Header
from email.mime.text import MIMEText

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

SLEEP = 0.3                 # seconds between Yahoo Finance calls
FETCH_RETRIES = 1           # extra attempts after the first for a failed Yahoo call
RETRY_SLEEP = 2.0           # pause before retrying a plain failure
RATE_LIMIT_SLEEP = 6.0      # pause before retrying an HTTP 429

IST = timezone(timedelta(hours=5, minutes=30))

WATCHLIST_FILE = 'data/watchlist.csv'
HOLIDAY_FILE = 'data/nse_holidays.csv'
STATUS_FILE = 'data/run_status.json'

# Share of the watchlist that must have data / score for a write to go ahead.
MIN_SCORED_FRACTION = 0.80    # below this: skip the write and send a failure alert
# Share of stocks that must have a bar for a session before its rows are final.
READY_FRACTION = 0.95

RECONCILE_DAYS = 3            # how many past run dates each run re-checks
MAX_DAILY_WINDOW = 10         # a window wider than this is a one-off repair, not the daily routine
BLOCK_TOLERANCE = 0.10        # a replaced day's row count may change by at most this share

BAR_BUFFER = 5              # extra bars required beyond the deepest lookback

# Market trend (SuperTrend). Traditional parameters: ATR period 10, multiplier 3.
ST_PERIOD = 10
ST_MULT = 3.0
ST_MIN_BARS = 30            # fewer bars than this and the stock is left out of the trend count
TREND_CFG = {
    'history_file': 'data/market_trend.csv',
    'fields': ['date', 'session_date', 'trend', 'up', 'down', 'total', 'pct_up'],
    'label': 'Market trend',
    'unit': 'row',
    'insert_missing': True,   # reconcile may add a trend row for a run date that has none
}

# Score formula profiles. Both are computed from a single fetched closing
# price series per symbol — 'windows' are the return lookbacks (summed),
# 'vol_window' is the lookback for the volatility (std dev of daily
# returns) denominator. 'min_bars' is derived below, per profile.
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
        'windows': [10, 15, 21],        # 10D, 15D, ~1M
        'vol_window': 10,               # 10D
        # New file name: the earlier 5D/10D/15D history stays untouched in
        # data/rank_history_short.csv; this one is created on the first run.
        'history_file': 'data/rank_history_short_v2.csv',
        'fields': ['date', 'symbol', 'rank', 'score', 'price',
                   'ret10', 'ret15', 'ret1m', 'vol10'],
        'return_keys': ['ret10', 'ret15', 'ret1m'],
        'vol_key': 'vol10',
    },
}

for _cfg in PROFILES.values():
    _cfg['min_bars'] = max(_cfg['windows'] + [_cfg['vol_window']]) + BAR_BUFFER

TARGETS = dict(PROFILES)
TARGETS['trend'] = TREND_CFG


# ─────────────────────────────────────────────
# GITHUB REST API
# ─────────────────────────────────────────────

def _contents_url(repo, path):
    return f'https://api.github.com/repos/{repo}/contents/{path}'


def _gh_headers(pat, accept='application/vnd.github.v3+json'):
    return {'Authorization': f'token {pat}', 'Accept': accept}


def github_get(repo, path, pat, allow_missing=False):
    """
    Return (text, sha) for a file, or (None, None) if it does not exist and
    allow_missing is set.

    The JSON contents response carries the file inline only up to 1 MiB; for
    bigger files `content` comes back empty with encoding "none". In that
    case the body is fetched a second time with the raw media type. Either
    way the number of bytes read must equal the size GitHub reports —
    otherwise we raise instead of handing back a partial/empty file.
    """
    url = _contents_url(repo, path)
    r = requests.get(url, headers=_gh_headers(pat), timeout=30)
    if r.status_code == 404 and allow_missing:
        return None, None
    r.raise_for_status()
    meta = r.json()
    if isinstance(meta, list):
        raise ValueError(f'{path} is a directory, not a file')

    sha, size = meta['sha'], meta.get('size')
    if meta.get('encoding') == 'base64' and meta.get('content'):
        raw = base64.b64decode(meta['content'])
    elif size == 0:
        raw = b''
    else:
        r2 = requests.get(url, headers=_gh_headers(pat, 'application/vnd.github.raw'), timeout=120)
        r2.raise_for_status()
        raw = r2.content

    if size is not None and len(raw) != size:
        raise RuntimeError(
            f'{path}: read {len(raw)} bytes but GitHub reports {size} — refusing to continue')
    return raw.decode('utf-8'), sha


def github_put(repo, path, pat, content, sha, message):
    payload = {
        'message': message,
        'content': base64.b64encode(content.encode('utf-8')).decode('utf-8'),
    }
    if sha:
        payload['sha'] = sha          # omitted when creating a new file
    r = requests.put(_contents_url(repo, path), headers=_gh_headers(pat), json=payload, timeout=120)
    if not r.ok:
        raise RuntimeError(f'GitHub PUT {path} failed: HTTP {r.status_code} {r.text[:300]}')


def parse_csv(content):
    return list(csv.DictReader(StringIO(content)))


def csv_text(rows, fieldnames, include_header):
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction='ignore')
    if include_header:
        writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


# ─────────────────────────────────────────────
# TRADING CALENDAR (explicit holiday list — not inferred from Yahoo)
# ─────────────────────────────────────────────

HOLIDAYS = set()
CALENDAR_THROUGH = None     # date through which the holiday list is known complete
CALENDAR_OK = False


def parse_date(v):
    try:
        return datetime.strptime(str(v).strip()[:10], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None


def load_calendar(content):
    """Parse data/nse_holidays.csv (Date,Type,Description)."""
    global HOLIDAYS, CALENDAR_THROUGH, CALENDAR_OK
    HOLIDAYS, CALENDAR_THROUGH = set(), None
    for r in parse_csv(content):
        t = (r.get('Type') or '').strip().upper()
        d = parse_date(r.get('Date'))
        if d is None:
            continue
        if t == 'HOLIDAY':
            HOLIDAYS.add(d)
        elif t == 'COVERS_THROUGH':
            CALENDAR_THROUGH = d
    CALENDAR_OK = True


def is_trading_day(d):
    return d.weekday() < 5 and d not in HOLIDAYS


def prev_trading_day(d):
    d -= timedelta(days=1)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


def calendar_notices(today):
    notes = []
    if not CALENDAR_OK:
        notes.append(f'Holiday calendar ({HOLIDAY_FILE}) could not be loaded — only weekends are '
                     f'treated as non-trading days.')
    elif CALENDAR_THROUGH is None or today > CALENDAR_THROUGH:
        notes.append(f'Holiday calendar is only confirmed through {CALENDAR_THROUGH} — update '
                     f'{HOLIDAY_FILE} and its COVERS_THROUGH row.')
    elif (CALENDAR_THROUGH - today).days < 14:
        notes.append(f'Holiday calendar ends {CALENDAR_THROUGH} — add the next holidays soon.')
    return notes


# ─────────────────────────────────────────────
# YAHOO FINANCE (raw requests, sequential)
# ─────────────────────────────────────────────

def fetch_chart(ticker, period='1y'):
    """
    Fetch daily bars for a Yahoo ticker. Returns a list of (timestamp, high,
    low, close), oldest first, for bars that have a close — or None. Rows whose
    values are all empty (Yahoo's lag) are dropped. One retry on errors / rate
    limits / server errors; an HTTP 404 (unknown ticker) is not retried.
    """
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}'
    params = {'range': period, 'interval': '1d', 'events': 'history'}
    headers = {'User-Agent': 'Mozilla/5.0'}
    last_err = None
    for attempt in range(FETCH_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=15)
            if r.status_code == 404:
                last_err = 'HTTP 404'
                break
            if r.status_code == 429 or r.status_code >= 500:
                raise RuntimeError(f'HTTP {r.status_code}')
            res = r.json()['chart']['result'][0]
            q = res['indicators']['quote'][0]
            closes = q['close']
            highs = q.get('high') or [None] * len(closes)
            lows = q.get('low') or [None] * len(closes)
            return [(t, h, l, c) for t, h, l, c in zip(res['timestamp'], highs, lows, closes)
                    if c is not None]
        except Exception as e:
            last_err = e
            if attempt < FETCH_RETRIES:
                time.sleep(RATE_LIMIT_SLEEP if '429' in str(e) else RETRY_SLEEP)
    print(f'  {ticker}: fetch failed — {last_err}')
    return None


def fetch_closes(symbol, period='1y'):
    return fetch_chart(symbol.upper().strip() + '.NS', period)


def fetch_ref_date(before=None):
    """
    Newest completed-session bar date on ^NSEI (IST), ignoring bars dated on or
    after `before` — used only as a cross-check of the calendar.
    """
    pairs = fetch_chart('^NSEI', '5d')
    if not pairs:
        return None
    dates = [bar_date(p[0]) for p in pairs]
    if before is not None:
        dates = [d for d in dates if d < before]
    return max(dates) if dates else None


# ─────────────────────────────────────────────
# MARKET TREND (SuperTrend per stock, majority vote across the watchlist)
# ─────────────────────────────────────────────

def bar_date(ts):
    """Calendar date (IST) of a Yahoo daily bar timestamp."""
    return datetime.fromtimestamp(ts, IST).date()


def supertrend_series(bars, period=ST_PERIOD, mult=ST_MULT):
    """
    SuperTrend exactly as TradingView's ta.supertrend (ATR via Wilder/RMA
    seeded with an SMA, bands carried forward, trend flips when the close
    crosses the active band; the first bar with an ATR starts in a downtrend).

    bars: (timestamp, high, low, close), oldest first. Returns a list of
    (timestamp, direction, line) for every bar from the first one that has an
    ATR; direction is +1 for an uptrend and -1 for a downtrend, line is the
    active SuperTrend value (lower band in an uptrend, upper band in a
    downtrend). Returns [] if there are too few bars.
    """
    rows = [b for b in bars if b[1] is not None and b[2] is not None and b[3] is not None]
    n = len(rows)
    if n < period + 1:
        return []

    tr = []
    for i, (_, h, l, c) in enumerate(rows):
        if i == 0:
            tr.append(h - l)
        else:
            pc = rows[i - 1][3]
            tr.append(max(h - l, abs(h - pc), abs(l - pc)))

    atr = sum(tr[:period]) / period            # ATR at index period-1
    out = []
    f_ub = f_lb = None
    trend = None
    for i in range(period - 1, n):
        if i >= period:
            atr = (atr * (period - 1) + tr[i]) / period
        t, h, l, c = rows[i]
        hl2 = (h + l) / 2
        ub = hl2 + mult * atr
        lb = hl2 - mult * atr
        if f_ub is None:                        # first bar with an ATR
            f_ub, f_lb, trend = ub, lb, -1
        else:
            pc = rows[i - 1][3]
            f_lb = lb if (lb > f_lb or pc < f_lb) else f_lb
            f_ub = ub if (ub < f_ub or pc > f_ub) else f_ub
            if trend == -1:                     # downtrend: active line is the upper band
                trend = 1 if c > f_ub else -1
            else:                               # uptrend: active line is the lower band
                trend = -1 if c < f_lb else 1
        out.append((t, trend, f_lb if trend == 1 else f_ub))
    return out


def summarize_market_trend(trends):
    """
    trends: list of (bar_date, direction) — each stock's latest SuperTrend
    reading. The session is the most common latest-bar date (ties: the later
    one); stocks whose latest bar is on another date are left out so a stale
    stock is never counted as today's trend. Returns the row dict for
    data/market_trend.csv (without 'date'), or None if there is nothing.
    """
    if not trends:
        return None
    counts = Counter(d for d, _ in trends)
    session = max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
    dirs = [x for d, x in trends if d == session]
    total = len(dirs)
    up = sum(1 for x in dirs if x > 0)
    down = total - up
    if up * 2 > total:
        label = 'UP'
    elif down * 2 > total:
        label = 'DOWN'
    else:
        label = 'NEUTRAL'
    return {
        'session_date': session.isoformat(),
        'trend': label,
        'up': up,
        'down': down,
        'total': total,
        'pct_up': round(up / total * 100, 2),
    }


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


def score_bars(symbol, bars, before=None):
    """
    Score one symbol from bars already fetched, under every profile, and read
    its SuperTrend direction. Bars dated on or after `before` are ignored.
    Returns (results, trend, reason, info): results is a dict profile name ->
    row (profiles the symbol is too short for, or that have zero volatility,
    are omitted) or None; trend is (bar_date, direction) or None; reason is
    'insufficient_data' when results is None; info describes the data
    (last_close date, how many bars were ignored, whether the last bar lacked
    high/low) for diagnostics.
    """
    info = {'last_close': None, 'dropped': 0, 'hl_missing': False}
    if before is not None:
        kept = [b for b in bars if bar_date(b[0]) < before]
        info['dropped'] = len(bars) - len(kept)
        bars = kept

    closes = [b[3] for b in bars]
    if not closes:
        return None, None, 'insufficient_data', info
    price = closes[-1]
    info['last_close'] = bar_date(bars[-1][0])
    info['hl_missing'] = bars[-1][1] is None or bars[-1][2] is None

    trend = None
    if len(bars) >= ST_MIN_BARS:
        st = supertrend_series(bars)
        if st:
            trend = (bar_date(st[-1][0]), st[-1][1])

    results = {}
    for profile_name, cfg in PROFILES.items():
        if len(closes) < cfg['min_bars']:
            continue
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

    if not results:
        return None, trend, 'insufficient_data', info
    return results, trend, None, info


def score_stock_all_profiles(symbol, before=None):
    """Fetch one symbol and score it (see score_bars). reason 'fetch_failed' on a failed fetch."""
    bars = fetch_closes(symbol, period='1y')
    time.sleep(SLEEP)
    if bars is None:
        return None, None, 'fetch_failed', {'last_close': None, 'dropped': 0, 'hl_missing': False}
    return score_bars(symbol, bars, before)


def fetch_universe(symbols):
    """One sequential Yahoo fetch per symbol. Returns (bars_by_symbol, fetch_failed)."""
    print(f'  Fetching {len(symbols)} stocks (sequential, 1y of daily bars each)...')
    bars_by_symbol, fetch_failed = {}, []
    for i, symbol in enumerate(symbols):
        sym = symbol.strip().upper()
        bars = fetch_closes(sym, period='1y')
        time.sleep(SLEEP)
        if bars is None:
            fetch_failed.append(sym)
        else:
            bars_by_symbol[sym] = bars
        if (i + 1) % 50 == 0:
            print(f'  Progress: {i + 1}/{len(symbols)} (fetch failed so far: {len(fetch_failed)})')
    return bars_by_symbol, fetch_failed


def evaluate_asof(symbols, bars_by_symbol, fetch_failed, before):
    """
    Score and rank every stock as the run on date `before` would have seen it:
    only bars dated before `before` are used. Pure computation — no network.

    Returns a dict with: scored[profile] (ranked rows), missing[profile] (symbols
    that did not score, by reason), summary (market-trend row or None), expected
    (the session the run should be based on), coverage (share of fetched stocks
    with a bar on that session), ready (coverage >= READY_FRACTION), diag.
    """
    scored = {name: [] for name in PROFILES}
    missing = {name: {'fetch_failed': [], 'insufficient_data': []} for name in PROFILES}
    trends = []
    diag = {'trend_date': {}, 'dropped': [], 'hl_missing': []}
    last_close = {}

    for symbol in symbols:
        sym = symbol.strip().upper()
        bars = bars_by_symbol.get(sym)
        if bars is None:
            for name in PROFILES:
                missing[name]['fetch_failed'].append(sym)
            continue
        results, trend, reason, info = score_bars(sym, bars, before)
        if trend is not None:
            trends.append(trend)
        last_close[sym] = info['last_close']
        diag['trend_date'][sym] = trend[0] if trend else None
        if info['dropped']:
            diag['dropped'].append(sym)
        if info['hl_missing']:
            diag['hl_missing'].append(sym)
        for name in PROFILES:
            if results and name in results:
                scored[name].append(results[name])
            else:
                missing[name][reason or 'insufficient_data'].append(sym)

    for rows in scored.values():
        rows.sort(key=lambda x: x['score'], reverse=True)
        for i, s in enumerate(rows):
            s['rank'] = i + 1

    expected = prev_trading_day(before)
    fresh = sum(1 for d in last_close.values() if d == expected)
    coverage = fresh / len(bars_by_symbol) if bars_by_symbol else 0.0
    summary = summarize_market_trend(trends)
    return {
        'before': before,
        'scored': scored,
        'missing': missing,
        'summary': summary,
        'expected': expected,
        'coverage': coverage,
        'ready': coverage >= READY_FRACTION - 1e-9,
        'diag': diag,
    }


def describe_trend_gap(diag, session_iso, limit=8):
    """One-line breakdown of where each stock's latest SuperTrend reading falls."""
    labels = {s: (d.isoformat() if d else 'none') for s, d in diag['trend_date'].items()}
    counts = Counter(labels.values())
    parts = ['latest SuperTrend date per stock \u2014 '
             + ', '.join(f'{k}: {v}' for k, v in sorted(counts.items(), reverse=True))]
    off = [s for s, lab in labels.items() if lab != session_iso]
    if off:
        parts.append('off-session examples: ' + ', '.join(f'{s} ({labels[s]})' for s in off[:limit]))
    if diag['dropped']:
        parts.append(f"bars dated today or later ignored for {len(diag['dropped'])} stocks")
    if diag['hl_missing']:
        parts.append(f"latest bar had no high/low for {len(diag['hl_missing'])} stocks "
                     f"(e.g. {', '.join(diag['hl_missing'][:limit])})")
    return ' | '.join(parts)


def describe_missing(m, limit=15):
    parts = []
    for label, key in (('fetch failed', 'fetch_failed'), ('too little history', 'insufficient_data')):
        syms = m[key]
        if syms:
            shown = ', '.join(syms[:limit]) + (f' … (+{len(syms) - limit} more)' if len(syms) > limit else '')
            parts.append(f'{label}: {shown}')
    return '; '.join(parts) if parts else 'no detail'


# ─────────────────────────────────────────────
# HISTORY FILES: append today, reconcile recent days (with guards)
# ─────────────────────────────────────────────

DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def split_blocks(text):
    """
    Parse a history file into (header, order, blocks): order is the list of run
    dates (ascending), blocks[date] the raw lines of that day. Raises if a date
    is out of order or split across non-adjacent lines.
    """
    lines = [ln.rstrip('\r') for ln in text.split('\n')]
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        raise RuntimeError('history file is empty')
    header, order, blocks = lines[0].strip(), [], {}
    for ln in lines[1:]:
        d = ln.split(',', 1)[0]
        if not DATE_RE.match(d):
            raise RuntimeError(f'line does not start with a date: {ln[:80]!r}')
        if d not in blocks:
            if order and d < order[-1]:
                raise RuntimeError(f'dates out of order around {d}')
            order.append(d)
            blocks[d] = []
        elif d != order[-1]:
            raise RuntimeError(f'rows for {d} are not contiguous')
        blocks[d].append(ln)
    return header, order, blocks


def format_lines(rows, fields, date_str):
    """CSV lines (no line endings) for rows dated date_str, formatted exactly as the writer does."""
    text = csv_text([dict(r, date=date_str) for r in rows], fields, include_header=False)
    return [ln for ln in text.split('\r\n') if ln]


def update_history_text(existing, path, cfg, replacements, today_str, today_rows):
    """
    Build the new text of a history file.

    replacements: {run date: rows} to re-compute in place (a date that is not in
    the file is only added when cfg['insert_missing'] is set). today_rows: rows to
    append for today, or None. Everything else stays byte-identical. Returns
    (new_text, report); report lists replaced dates (with changed-row counts),
    inserted dates, guarded dates (size change too large) and whether today was
    appended or skipped.
    """
    fields = cfg['fields']
    header_expected = ','.join(fields)
    report = {'replaced': {}, 'inserted': [], 'guarded': [], 'appended': False, 'skipped_today': None}

    if existing is None or not existing.strip():
        order, blocks = [], {}
    else:
        header, order, blocks = split_blocks(existing)
        if header != header_expected:
            raise RuntimeError(
                f'{path}: header does not match the expected schema.\n'
                f'  found:    {header}\n  expected: {header_expected}')

    for d in sorted(replacements):
        new_lines = format_lines(replacements[d], fields, d)
        if d in blocks:
            old = blocks[d]
            if old and abs(len(new_lines) - len(old)) > BLOCK_TOLERANCE * len(old):
                report['guarded'].append((d, len(old), len(new_lines)))
                continue
            if new_lines != old:
                report['replaced'][d] = sum((Counter(new_lines) - Counter(old)).values())
                blocks[d] = new_lines
        elif cfg.get('insert_missing'):
            bisect.insort(order, d)
            blocks[d] = new_lines
            report['inserted'].append(d)

    if today_rows:
        last = order[-1] if order else None
        if last is not None and last >= today_str:
            report['skipped_today'] = last
        else:
            order.append(today_str)
            blocks[today_str] = format_lines(today_rows, fields, today_str)
            report['appended'] = True

    text = header_expected + '\r\n' + ''.join(ln + '\r\n' for d in order for ln in blocks[d])
    return text, report


def commit_message(name, cfg, report, today_str, today_count):
    parts = []
    if report['appended']:
        parts.append(f"{cfg.get('label', 'Rank snapshot')} \u2014 {today_str} "
                     f"({today_count} {cfg.get('unit', 'stocks')})")
    fixed = sorted(set(report['replaced']) | set(report['inserted']))
    if fixed:
        parts.append('reconciled ' + ', '.join(fixed))
    return f'[{name}] ' + '; '.join(parts)


# ─────────────────────────────────────────────
# EMAIL (success is silent by design; failures and notices only)
# ─────────────────────────────────────────────

def _send_email(subject, body):
    sender = os.environ.get('GMAIL_SENDER')
    password = os.environ.get('GMAIL_APP_PASSWORD')
    recipients = [r.strip() for r in os.environ.get('GMAIL_RECIPIENT', '').split(',') if r.strip()]
    if not (sender and password and recipients):
        print('  Email env vars missing, skipping email.')
        return
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = Header(subject, 'utf-8')
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        print('  Email sent.')
    except Exception as e:
        print(f'  Could not send email: {e}')


def send_failure_email(error_text, dry_run=False):
    today = datetime.now(IST).strftime('%d %b %Y')
    prefix = '[DRY RUN] ' if dry_run else ''
    _send_email(f'{prefix}\u26a0\ufe0f Momentum Rank Snapshot FAILED \u2014 {today}',
                f'Momentum daily rank snapshot failed on {today}.\n\n{error_text}')


def send_notice_email(lines, dry_run=False):
    today = datetime.now(IST).strftime('%d %b %Y')
    prefix = '[DRY RUN] ' if dry_run else ''
    body = f'Momentum daily rank snapshot — notices for {today}:\n\n' + '\n'.join(f'- {l}' for l in lines)
    _send_email(f'{prefix}Momentum Rank Snapshot \u2014 notice \u2014 {today}', body)


# ─────────────────────────────────────────────
# RUN STATUS (read by the viewer — the "did it run, and how" signal without email)
# ─────────────────────────────────────────────

def write_status(repo, pat, status, dry_run):
    """Best effort: a failure here never affects the run."""
    if dry_run:
        return
    try:
        status = dict(status, finished_at=datetime.now(IST).isoformat(timespec='seconds'))
        _, sha = github_get(repo, STATUS_FILE, pat, allow_missing=True)
        github_put(repo, STATUS_FILE, pat, json.dumps(status, indent=2) + '\n', sha,
                   f"[status] {status['outcome']} \u2014 {status['run_date']}")
    except Exception as e:
        print(f'  Could not write {STATUS_FILE}: {e}')


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def _flag(v):
    return str(v).strip().lower() in ('1', 'true', 'yes', 'y')


def main(args):
    args = args or {}
    dry_run = _flag(args.get('dry_run', False))
    force = _flag(args.get('force', False))
    try:
        reconcile_days = max(0, int(args.get('reconcile_days', RECONCILE_DAYS)))
    except (TypeError, ValueError):
        reconcile_days = RECONCILE_DAYS

    print('\n' + '=' * 55)
    print('  NSE MOMENTUM \u2014 DAILY RANK SNAPSHOT' + ('  [DRY RUN]' if dry_run else ''))
    print('=' * 55)

    pat = os.environ.get('GITHUB_PAT')
    repo_name = os.environ.get('GITHUB_REPO')
    now_ist = datetime.now(IST)
    today = now_ist.date()
    today_str = today.isoformat()
    notices = []          # emailed (outcome: warning)
    problems = []         # emailed as a failure (outcome: failed)
    status = {'run_date': today_str, 'outcome': 'ok', 'message': ''}

    try:
        print('\n[1/6] Checking trading calendar...')
        try:
            cal_text, _ = github_get(repo_name, HOLIDAY_FILE, pat, allow_missing=True)
            if cal_text is not None:
                load_calendar(cal_text)
                print(f'  Calendar: {len(HOLIDAYS)} holidays, confirmed through {CALENDAR_THROUGH}')
            else:
                print(f'  WARNING: {HOLIDAY_FILE} not found')
        except Exception as e:
            print(f'  WARNING: could not load {HOLIDAY_FILE}: {e}')
        notices.extend(calendar_notices(today))

        if not is_trading_day(today) and not force:
            reason = ('Weekend' if today.weekday() >= 5 else 'NSE trading holiday') + f' ({today})'
            print(f'  {reason} \u2014 nothing to do.')
            if notices and today.weekday() < 5:
                send_notice_email(notices, dry_run)
            status.update(outcome='skipped', message=reason)
            write_status(repo_name, pat, status, dry_run)
            return {'statusCode': 200, 'body': f'{reason}: skipped'}

        print('\n[2/6] Loading watchlist...')
        wl_text, _ = github_get(repo_name, WATCHLIST_FILE, pat)
        raw_symbols = [row['Symbol'].strip() for row in parse_csv(wl_text) if (row.get('Symbol') or '').strip()]
        symbols = list(dict.fromkeys(s.upper() for s in raw_symbols))      # de-duplicate, keep order
        if len(symbols) != len(raw_symbols):
            print(f'  Note: {len(raw_symbols) - len(symbols)} duplicate symbol(s) in watchlist ignored')
        n = len(symbols)
        print(f'  Watchlist: {n} symbols')

        print('\n[3/6] Fetching prices...')
        bars_by_symbol, fetch_failed = fetch_universe(symbols)
        if len(bars_by_symbol) < MIN_SCORED_FRACTION * n:
            problems.append(f'Yahoo returned data for only {len(bars_by_symbol)}/{n} stocks '
                            f'(fetch failed: {", ".join(fetch_failed[:15])}) \u2014 nothing was written.')

        print('\n[4/6] Scoring today and checking recent days...')
        ev_today = evaluate_asof(symbols, bars_by_symbol, fetch_failed, today)
        expected = ev_today['expected']
        summary = ev_today['summary']
        session_iso = summary['session_date'] if summary else None
        diag = ev_today['diag']
        for name in PROFILES:
            m = ev_today['missing'][name]
            print(f"  [{name}] scored {len(ev_today['scored'][name])}/{n} "
                  f"(fetch failed: {len(m['fetch_failed'])}, too little history: {len(m['insufficient_data'])})")
        print(f"  Expected session {expected}: {ev_today['coverage']:.0%} of stocks have its bar "
              f"({'complete' if ev_today['ready'] else 'still filling in'})")
        if diag['dropped']:
            print(f"  Ignored bars dated {today} or later for {len(diag['dropped'])} stock(s)")
        print(f"  Trend data: {describe_trend_gap(diag, expected.isoformat())}")

        # Calendar cross-check. Only worth an email when NIFTY *and* the stocks both
        # disagree with the calendar (a lagging index alone is just Yahoo).
        nifty_date = fetch_ref_date(today)
        print(f'  Calendar expects session {expected} | NIFTY newest completed bar: {nifty_date} '
              f'| majority of stocks: {session_iso}')
        if nifty_date and nifty_date != expected and session_iso and session_iso != expected.isoformat():
            notices.append(f"Calendar expects the last session to be {expected}, but NIFTY ({nifty_date}) and "
                           f"most stocks ({session_iso}) both show another date \u2014 the holiday list may be wrong.")

        # --- today's rows --------------------------------------------------
        fetch_ok = len(bars_by_symbol) >= MIN_SCORED_FRACTION * n
        today_rows = {}
        if fetch_ok:
            for name, rows in ev_today['scored'].items():
                frac = len(rows) / n if n else 0.0
                if frac < MIN_SCORED_FRACTION:
                    problems.append(f"[{name}] only {len(rows)}/{n} stocks scored ({frac:.0%}) \u2014 below "
                                    f"{MIN_SCORED_FRACTION:.0%}, today's rows not written. Not scored \u2014 "
                                    f"{describe_missing(ev_today['missing'][name])}")
                else:
                    today_rows[name] = rows
        if fetch_ok and summary and ev_today['ready'] and session_iso == expected.isoformat():
            today_rows['trend'] = [summary]
            print(f"  Market trend: {summary['trend']} \u2014 {summary['up']} up / {summary['down']} down "
                  f"of {summary['total']} ({summary['pct_up']}% up), session {summary['session_date']}")
        else:
            print(f"  Market trend row for {today_str} deferred \u2014 the session is not complete yet; "
                  f"a later run fills it in.")

        # --- read history files and decide which past days to reconcile -----
        print('\n[5/6] Reading history files...')
        existing, read_failures = {}, []
        for name, cfg in TARGETS.items():
            try:
                existing[name] = github_get(repo_name, cfg['history_file'], pat, allow_missing=True)
            except Exception as e:
                read_failures.append(f'[{name}] could not read {cfg["history_file"]}: {e}')
        problems.extend(read_failures)

        window = []
        std_text = existing.get('standard', (None, None))[0]
        if fetch_ok and reconcile_days and std_text and std_text.strip():
            try:
                _, std_order, _ = split_blocks(std_text)
                window = [d for d in std_order if d < today_str][-reconcile_days:]
            except Exception as e:
                problems.append(f'[standard] could not read the run dates for reconcile: {e}')

        replacements = {name: {} for name in TARGETS}
        pending = []
        for d in window:
            ev = evaluate_asof(symbols, bars_by_symbol, fetch_failed, parse_date(d))
            if not ev['ready']:
                pending.append((d, ev['coverage'], ev['expected']))
                continue
            for name in PROFILES:
                if ev['scored'][name]:
                    replacements[name][d] = ev['scored'][name]
            if ev['summary'] and ev['summary']['session_date'] == ev['expected'].isoformat():
                replacements['trend'][d] = [ev['summary']]
        if window:
            print(f"  Reconcile window: {window[0]} .. {window[-1]} ({len(window)} run dates); "
                  f"complete: {len(window) - len(pending)}, still filling in: {len(pending)}")
            for d, cov, exp in pending[:10]:
                print(f"    {d}: {cov:.0%} of stocks have the {exp} bar yet")
        if (pending and len(window) >= reconcile_days and pending[0][0] == window[0]
                and reconcile_days <= MAX_DAILY_WINDOW):
            d, cov, exp = pending[0]
            problems.append(f"Run date {d} could not be finalized after {reconcile_days} runs \u2014 only "
                            f"{cov:.0%} of stocks have a bar for {exp}. Its rows stay provisional; "
                            f"re-run with reconcile_days once Yahoo has the data.")

        print('\n[6/6] Writing history files...')
        written, reconciled = {}, {}
        for name, cfg in TARGETS.items():
            if name not in existing:
                continue
            text0, sha = existing[name]
            rows_today = today_rows.get(name)
            if not rows_today and not replacements[name]:
                continue
            try:
                text, report = update_history_text(text0, cfg['history_file'], cfg, replacements[name],
                                                   today_str, rows_today)
            except Exception as e:
                problems.append(f'[{name}] FAILED: {e}')
                print(f'  [{name}] FAILED: {e}')
                continue
            for d, old_n, new_n in report['guarded']:
                problems.append(f'[{name}] reconcile refused for {d}: its row count would change from '
                                f'{old_n} to {new_n} (more than {BLOCK_TOLERANCE:.0%}).')
            if report['skipped_today']:
                print(f"  [{name}] {cfg['history_file']} already has rows through {report['skipped_today']} "
                      f"\u2014 nothing appended.")
                if report['skipped_today'] > today_str:
                    notices.append(f"[{name}] history file already contains a date in the future "
                                   f"({report['skipped_today']}).")
            fixed = sorted(set(report['replaced']) | set(report['inserted']))
            if fixed:
                reconciled[name] = fixed
                for d in fixed:
                    what = (f"inserted" if d in report['inserted'] else f"{report['replaced'][d]} rows changed")
                    print(f'    [{name}] {d}: {what}')
            if text == text0:
                continue
            if dry_run:
                print(f"  [{name}] DRY RUN \u2014 would write {cfg['history_file']} "
                      f"(append: {report['appended']}, reconciled dates: {len(fixed)}, {len(text):,} chars)")
                if report['appended']:
                    written[name] = len(rows_today)
                continue
            try:
                github_put(repo_name, cfg['history_file'], pat, text, sha,
                           commit_message(name, cfg, report, today_str, len(rows_today or [])))
                print(f"  [{name}] wrote {cfg['history_file']} (append: {report['appended']}, "
                      f"reconciled dates: {len(fixed)})")
                if report['appended']:
                    written[name] = len(rows_today)
            except Exception as e:
                problems.append(f'[{name}] write FAILED: {e}')
                print(f'  [{name}] write FAILED: {e}')

        # --- wrap up --------------------------------------------------------
        parts = [f'{k}={v}' for k, v in written.items()] or ['no new rows']
        body = 'Wrote rows: ' + ', '.join(parts)
        if reconciled:
            body += ' | reconciled: ' + ', '.join(f'{k} {len(v)} date(s)' for k, v in reconciled.items())
        if pending:
            body += f' | {len(pending)} recent date(s) still filling in'

        status.update(
            wrote=written,
            reconciled=reconciled,
            provisional=[today_str] if not ev_today['ready'] else [],
            pending=[d for d, _, _ in pending],
        )
        if problems:
            lines = list(problems)
            if written:
                lines.append('Written successfully: ' + ', '.join(f'{k} ({v} rows)' for k, v in written.items()))
            if notices:
                lines.append('Other notices: ' + ' | '.join(notices))
            text = '\n\n'.join(lines)
            print(f'  ERROR: {text}')
            status.update(outcome='failed', message=text[:500])
            write_status(repo_name, pat, status, dry_run)
            send_failure_email(text, dry_run)
            return {'statusCode': 500, 'body': text}

        if notices:
            status.update(outcome='warning', message=' | '.join(notices)[:500])
            send_notice_email(notices, dry_run)
        write_status(repo_name, pat, status, dry_run)
        print(f'\n  Done. {body}\n')
        return {'statusCode': 200, 'body': body}

    except Exception as e:
        error_text = f'{str(e)}\n\n{traceback.format_exc()}'
        print(f'\n  ERROR: {error_text}')
        status.update(outcome='failed', message=str(e)[:500])
        write_status(repo_name, pat, status, dry_run)
        send_failure_email(error_text, dry_run)
        return {'statusCode': 500, 'body': str(e)}
