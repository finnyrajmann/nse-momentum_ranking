#!/usr/bin/env python3
"""
One-off: restore the history lost when data/rank_history.csv was reset on
2026-10-02 (it had crossed GitHub's 1 MiB inline-read limit).

Takes the last full pre-wipe version from git (default commit 9e25646, which
holds 2026-08-12 .. 2026-10-01) and puts those rows in front of the CURRENT
file (2026-10-02 onward). Nothing is recomputed — rows are copied byte-for-byte.

Run from the repo root, AFTER the fixed daily_run function is deployed and has
completed one successful live run, and not near the 08:50 IST run time:

    git checkout master && git pull
    python tools/backfill_standard.py --dry-run     # check the numbers first
    python tools/backfill_standard.py               # writes data/rank_history.csv
    git add data/rank_history.csv && git commit -m "Backfill rank history Aug 12 - Oct 1" && git push

Safe to re-run: if the file already starts before the old range ends it stops
without changing anything.
"""
import argparse
import re
import subprocess
import sys

PATH = 'data/rank_history.csv'
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def dates_and_keys(text, label):
    lines = text.split('\n')
    header = lines[0].strip()
    dates, keys = set(), set()
    for ln in lines[1:]:
        if not ln.strip():
            continue
        cells = ln.split(',')
        if not DATE_RE.match(cells[0]):
            sys.exit(f'{label}: line does not start with a date: {ln[:80]!r}')
        dates.add(cells[0])
        key = (cells[0], cells[1])
        if key in keys:
            sys.exit(f'{label}: duplicate (date, symbol) row {key}')
        keys.add(key)
    return header, dates, keys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--commit', default='9e25646', help='last full pre-wipe commit')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    old = subprocess.run(['git', 'show', f'{args.commit}:{PATH}'],
                         capture_output=True, check=True).stdout.decode('utf-8')
    with open(PATH, newline='', encoding='utf-8') as f:      # newline='' keeps CRLF as-is
        cur = f.read()

    oh, old_dates, old_keys = dates_and_keys(old, f'{args.commit}:{PATH}')
    ch, cur_dates, cur_keys = dates_and_keys(cur, PATH)

    if oh != ch:
        sys.exit(f'Header mismatch:\n  old:     {oh}\n  current: {ch}')
    if not old_dates or not cur_dates:
        sys.exit('One of the files has no data rows — nothing to merge.')
    if min(cur_dates) <= max(old_dates):
        print(f'Current file already starts at {min(cur_dates)}, which is not after the old '
              f'range end {max(old_dates)} — looks already merged (or overlapping). No change.')
        return
    if old_keys & cur_keys:
        sys.exit('Overlapping (date, symbol) rows between old and current — aborting.')

    cur_body = cur.split('\n', 1)[1]
    old_text = old if old.endswith('\n') else old + '\r\n'
    merged = old_text + cur_body

    _, m_dates, m_keys = dates_and_keys(merged, 'merged')
    print(f'old     : {len(old_dates):>3} days  {min(old_dates)} .. {max(old_dates)}')
    print(f'current : {len(cur_dates):>3} days  {min(cur_dates)} .. {max(cur_dates)}')
    print(f'merged  : {len(m_dates):>3} days  {min(m_dates)} .. {max(m_dates)}  '
          f'{len(m_keys):,} rows, {len(merged.encode("utf-8")):,} bytes')

    if args.dry_run:
        print('Dry run — nothing written.')
        return
    with open(PATH, 'w', newline='', encoding='utf-8') as f:
        f.write(merged)
    print(f'Wrote {PATH}. Now commit and push (see the docstring).')


if __name__ == '__main__':
    main()
