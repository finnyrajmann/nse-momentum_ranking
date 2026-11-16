"""
NSE Momentum Daily Rank Snapshot — DO Functions Entry Point
=============================================================
Runs every trading day before market open. Scores the full watchlist using
TWO momentum formulas from the same fetched price data, and appends one
rank snapshot row per stock, per formula, to two growing history files on
GitHub.

    Standard : (1M return + 3M return + 6M return) / 1M volatility
    Short    : (10D return + 15D return + 1M return) / 10D volatility

Both formulas score off the same single Yahoo fetch per symbol — no extra
API calls for the short formula, it's just a different set of lookback
windows applied to the same closing-price series.

This job does NOT rebalance, does NOT touch positions, and does NOT affect
the monthly momentum system in nse-momentum. It exists purely to build a
day-by-day rank history for the bump-chart visualization.

Behaviour notes
---------------
- Trading calendar: data/nse_holidays.csv (Date,Type,Description; Type is
  HOLIDAY or COVERS_THROUGH) — the same explicit-list method used by the
  nse-bb_ema_cross system. A day is a session if it is Mon-Fri and not a
  listed holiday; non-session days are skipped quietly. If the calendar is
  missing/expired/about to expire, or disagrees with NIFTY's newest bar, a
  short notice email is sent. If it cannot be loaded at all, the job falls
  back to weekday-only and says so in the notice.
- History files are APPENDED to, never rebuilt: the new file text is always
  (existing text + new rows). The existing text is read in full (raw media
  type, so files over GitHub's 1 MiB JSON limit work) and its byte length is
  verified against GitHub's reported size, so a truncated/empty read can
  never turn into an overwrite.
- Guards per history file: header must match the expected schema; if the
  file's last date is already today (or later) the write is skipped; a file
  that does not exist yet is created with a header.
- Alerting: failure email on any error; a warning notice when fewer than 95%
  of the watchlist scores for a profile; the profile's write is skipped (and
  a failure email sent) when fewer than 80% scores. A failed write for one
  profile does not stop the other.
- Market trend: from the same Yahoo fetch (highs/lows/closes) every stock's
  SuperTrend (ATR 10, multiplier 3, Wilder smoothing — TradingView's
  ta.supertrend) is evaluated; the day's market trend is UP/DOWN when more
  than 50% of the watchlist's stocks are up/down (NEUTRAL on an exact tie).
  One row per day goes to data/market_trend.csv: date (run date, like the
  rank files), session_date (the market session the data is as of — use this
  to join against trade logs), trend, up, down, total, pct_up.
- Invoke with {"dry_run": true} to do everything except write to GitHub
  (emails are prefixed [DRY RUN]); {"force": true} runs on a non-session day.

No pip installs needed — requests + standard library only.
"""

import os
import csv
import re
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
BAR_FINAL_HOUR_IST = 16     # today's daily bar counts as complete only after this hour (IST)

WATCHLIST_FILE = 'data/watchlist.csv'
HOLIDAY_FILE = 'data/nse_holidays.csv'

# Share of the watchlist that must score for a profile to be written.
WARN_SCORED_FRACTION = 0.95   # below this: still write, but send a notice
MIN_SCORED_FRACTION = 0.80    # below this: skip the write and send a failure alert

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


def last_completed_session(now_ist):
    """Most recent session whose daily bar should be final at now_ist."""
    d = now_ist.date()
    if is_trading_day(d) and now_ist.hour >= BAR_FINAL_HOUR_IST:
        return d
    return prev_trading_day(d)


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
    low, close), oldest first, for bars that have a close — or None. One retry on errors / rate limits / server errors; an
    HTTP 404 (unknown ticker) is not retried.
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


def fetch_ref_date():
    """Newest bar date on ^NSEI (IST) — used only as a cross-check of the calendar."""
    pairs = fetch_chart('^NSEI', '5d')
    if not pairs:
        return None
    return datetime.fromtimestamp(pairs[-1][0], IST).date()


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


def score_stock_all_profiles(symbol):
    """
    Fetch a symbol's price history ONCE, then score it under every profile
    in PROFILES using that same series, and read its SuperTrend direction
    for the market trend. Returns (results, trend, reason): results is a
    dict profile name -> row (profiles the symbol is too short for, or that
    have zero volatility, are omitted) or None; trend is (bar_date,
    direction) or None; reason is 'fetch_failed' / 'insufficient_data' when
    results is None.
    """
    bars = fetch_closes(symbol, period='1y')
    time.sleep(SLEEP)
    if bars is None:
        return None, None, 'fetch_failed'

    closes = [b[3] for b in bars]
    if not closes:
        return None, None, 'insufficient_data'
    price = closes[-1]

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
        return None, trend, 'insufficient_data'
    return results, trend, None


def score_universe(symbols):
    """
    Score and rank all stocks sequentially — one Yahoo request at a time.
    Every profile is scored per symbol from that single fetch, then each
    profile's list is ranked independently (a symbol that is too new for the
    standard formula can still be ranked on the short one).

    Returns (scored, missing, trends): scored[profile] is the ranked row
    list; missing[profile] lists the symbols that did not score, by reason;
    trends is the list of (bar_date, direction) SuperTrend readings.
    """
    print(f'  Scoring {len(symbols)} stocks (sequential, all profiles per fetch)...')
    scored = {name: [] for name in PROFILES}
    missing = {name: {'fetch_failed': [], 'insufficient_data': []} for name in PROFILES}
    trends = []

    for i, symbol in enumerate(symbols):
        results, trend, reason = score_stock_all_profiles(symbol)
        if trend is not None:
            trends.append(trend)
        sym = symbol.strip().upper()
        for name in PROFILES:
            if results and name in results:
                scored[name].append(results[name])
            else:
                missing[name][reason or 'insufficient_data'].append(sym)
        if (i + 1) % 50 == 0:
            counts = ', '.join(f'{k}={len(v)}' for k, v in scored.items())
            print(f'  Progress: {i + 1}/{len(symbols)} — scored so far: {counts}')

    for profile_name, rows in scored.items():
        rows.sort(key=lambda x: x['score'], reverse=True)
        for i, s in enumerate(rows):
            s['rank'] = i + 1
        m = missing[profile_name]
        print(f'  [{profile_name}] scored {len(rows)}/{len(symbols)} symbols '
              f'(fetch failed: {len(m["fetch_failed"])}, too little history: {len(m["insufficient_data"])})')

    return scored, missing, trends


def describe_missing(m, limit=15):
    parts = []
    for label, key in (('fetch failed', 'fetch_failed'), ('too little history', 'insufficient_data')):
        syms = m[key]
        if syms:
            shown = ', '.join(syms[:limit]) + (f' … (+{len(syms) - limit} more)' if len(syms) > limit else '')
            parts.append(f'{label}: {shown}')
    return '; '.join(parts) if parts else 'no detail'


# ─────────────────────────────────────────────
# HISTORY FILE APPEND (with guards)
# ─────────────────────────────────────────────

DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def last_data_date(text):
    """Date in the first column of the last data line, or None if header only."""
    parts = text.rstrip().rsplit('\n', 1)
    if len(parts) < 2:
        return None
    last = parts[1].split(',')[0].strip()
    if not DATE_RE.match(last):
        raise RuntimeError(f'last line of history file does not start with a date: {parts[1][:80]!r}')
    return last


def append_snapshot(repo, pat, name, cfg, rows, today_str, dry_run):
    """
    Append today's rows to a profile's history file. Returns (status, detail)
    with status 'written', 'dry_run' or 'skipped'. Raises on schema
    mismatch, an unreadable file, or anything that would shrink the file.
    """
    path, fields = cfg['history_file'], cfg['fields']
    existing, sha = github_get(repo, path, pat, allow_missing=True)
    new_rows = [dict(r, date=today_str) for r in rows]

    if existing is None or not existing.strip():
        text = csv_text(new_rows, fields, include_header=True)       # new (or blank) file
    else:
        header = existing.split('\n', 1)[0].strip()
        if header != ','.join(fields):
            raise RuntimeError(
                f'{path}: header does not match the expected schema.\n'
                f'  found:    {header}\n  expected: {",".join(fields)}')
        last_date = last_data_date(existing)
        if last_date is not None and last_date >= today_str:
            return 'skipped', last_date
        body = existing if existing.endswith('\n') else existing + '\r\n'
        text = body + csv_text(new_rows, fields, include_header=False)
        if not text.startswith(existing) or len(text) <= len(existing):
            raise RuntimeError(f'{path}: new content would not extend the existing file — aborting')

    label, unit = cfg.get('label', 'Rank snapshot'), cfg.get('unit', 'stocks')
    message = f'[{name}] {label} \u2014 {today_str} ({len(new_rows)} {unit})'
    if dry_run:
        print(f'  [{name}] DRY RUN — would write {len(new_rows)} rows to {path} ({len(text):,} chars)')
        return 'dry_run', None
    github_put(repo, path, pat, text, sha, message)
    print(f'  [{name}] wrote {len(new_rows)} rows to {path} ({len(text):,} chars)')
    return 'written', None


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
# MAIN
# ─────────────────────────────────────────────

def _flag(v):
    return str(v).strip().lower() in ('1', 'true', 'yes', 'y')


def main(args):
    args = args or {}
    dry_run = _flag(args.get('dry_run', False))
    force = _flag(args.get('force', False))

    print('\n' + '=' * 55)
    print('  NSE MOMENTUM \u2014 DAILY RANK SNAPSHOT (dual formula)' + ('  [DRY RUN]' if dry_run else ''))
    print('=' * 55)

    pat = os.environ.get('GITHUB_PAT')
    repo_name = os.environ.get('GITHUB_REPO')
    now_ist = datetime.now(IST)
    today = now_ist.date()
    today_str = today.isoformat()
    notices = []
    nifty_flagged = False

    try:
        print('\n[1/5] Checking trading calendar...')
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
            return {'statusCode': 200, 'body': f'{reason}: skipped'}

        ref_date = last_completed_session(now_ist)
        nifty_date = fetch_ref_date()
        print(f'  Last completed session (calendar): {ref_date} | NIFTY newest bar: {nifty_date}')
        if nifty_date and nifty_date != ref_date:
            nifty_flagged = True
            notices.append(f"Calendar expects the last session to be {ref_date} but NIFTY's newest bar is "
                           f'{nifty_date} \u2014 either Yahoo is lagging/ahead or the holiday list is wrong.')

        print('\n[2/5] Loading watchlist...')
        wl_text, _ = github_get(repo_name, WATCHLIST_FILE, pat)
        raw_symbols = [row['Symbol'].strip() for row in parse_csv(wl_text) if (row.get('Symbol') or '').strip()]
        symbols = list(dict.fromkeys(raw_symbols))            # de-duplicate, keep order
        if len(symbols) != len(raw_symbols):
            print(f'  Note: {len(raw_symbols) - len(symbols)} duplicate symbol(s) in watchlist ignored')
        print(f'  Watchlist: {len(symbols)} symbols')

        print('\n[3/5] Scoring universe (sequential fetch, both formulas)...')
        scored, missing, trends = score_universe(symbols)
        summary = summarize_market_trend(trends)

        # Guard each profile independently.
        problems = []
        write_plan = {}
        n = len(symbols)
        for name, rows in scored.items():
            frac = len(rows) / n if n else 0.0
            detail = describe_missing(missing[name])
            if frac < MIN_SCORED_FRACTION:
                problems.append(f'[{name}] only {len(rows)}/{n} stocks scored ({frac:.0%}) \u2014 below '
                                f'{MIN_SCORED_FRACTION:.0%}, write skipped. Not scored \u2014 {detail}')
                continue
            if frac < WARN_SCORED_FRACTION:
                notices.append(f'[{name}] {len(rows)}/{n} stocks scored ({frac:.0%}) \u2014 below the '
                               f'{WARN_SCORED_FRACTION:.0%} warning level. Not scored \u2014 {detail}')
            write_plan[name] = rows

        # Market trend: written as one row per day, guarded like the profiles.
        if summary is None:
            problems.append('[trend] SuperTrend could not be computed for any stock \u2014 write skipped.')
        else:
            tfrac = summary['total'] / n if n else 0.0
            print(f"  Market trend: {summary['trend']} \u2014 {summary['up']} up / {summary['down']} down "
                  f"of {summary['total']} ({summary['pct_up']}% up), session {summary['session_date']}")
            if tfrac < MIN_SCORED_FRACTION:
                problems.append(f"[trend] SuperTrend available for only {summary['total']}/{n} stocks "
                                f"({tfrac:.0%}) \u2014 below {MIN_SCORED_FRACTION:.0%}, write skipped.")
            else:
                if tfrac < WARN_SCORED_FRACTION:
                    notices.append(f"[trend] SuperTrend available for {summary['total']}/{n} stocks "
                                   f"({tfrac:.0%}) \u2014 below the {WARN_SCORED_FRACTION:.0%} warning level.")
                write_plan['trend'] = [summary]
            if summary['session_date'] != ref_date.isoformat() and not nifty_flagged:
                notices.append(f"Most stocks' latest bar is {summary['session_date']} but the calendar "
                               f"expects {ref_date} \u2014 Yahoo lag, or the holiday list is wrong.")

        targets = dict(PROFILES)
        targets['trend'] = TREND_CFG

        print('\n[4/5] Appending to rank history file(s) on GitHub...')
        written, skipped = {}, {}
        failures = []
        for name, rows in write_plan.items():
            try:
                status, detail = append_snapshot(repo_name, pat, name, targets[name], rows, today_str, dry_run)
            except Exception as e:
                failures.append(f'[{name}] write FAILED: {e}')
                print(f'  [{name}] write FAILED: {e}')
                continue
            if status == 'skipped':
                skipped[name] = detail
                print(f'  [{name}] {targets[name]["history_file"]} already has rows through {detail} '
                      f'\u2014 nothing appended.')
                if detail > today_str:
                    notices.append(f'[{name}] history file already contains a date in the future ({detail}).')
            else:
                written[name] = len(rows)

        print('\n[5/5] Wrapping up...')
        summary = ', '.join(f'{k}={v}' for k, v in written.items()) or 'none'
        if skipped:
            summary += ' | skipped (already present): ' + ', '.join(f'{k} ({v})' for k, v in skipped.items())

        if problems or failures:
            lines = problems + failures
            if written:
                lines.append('Written successfully: ' + ', '.join(f'{k} ({v} rows)' for k, v in written.items()))
            if notices:
                lines.append('Other notices: ' + ' | '.join(notices))
            text = '\n\n'.join(lines)
            print(f'  ERROR: {text}')
            send_failure_email(text, dry_run)
            return {'statusCode': 500, 'body': text}

        if notices:
            send_notice_email(notices, dry_run)
        print(f'\n  Done. Wrote rows: {summary}\n')
        return {'statusCode': 200, 'body': f'Wrote rows: {summary}'}

    except Exception as e:
        error_text = f'{str(e)}\n\n{traceback.format_exc()}'
        print(f'\n  ERROR: {error_text}')
        send_failure_email(error_text, dry_run)
        return {'statusCode': 500, 'body': str(e)}
