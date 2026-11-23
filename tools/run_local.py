#!/usr/bin/env python3
"""
Run the daily_run function on THIS machine - the very same code that runs on
DigitalOcean - with its whole log printed in the terminal. Use it for dry runs
and for the one-off history repair, because manual invocations on DigitalOcean
do not show up in `doctl sls activations`.

It needs GITHUB_PAT and GITHUB_REPO (the same values the DO function uses). They
are read, in this order, from: the environment; a .env file in the repo root
(KEY=value lines); the `environment:` block of functions/project.yml, where the
deployed function's variables normally live. Values are never printed. Keep those
files out of git (check with `git check-ignore .env functions/project.yml`).

Emails are switched off for local runs (the log shows what would have been
reported) unless you pass --email.

    python3 tools/run_local.py --dry-run --reconcile-only --reconcile-days 80   # preview the repair, write nothing
    python3 tools/run_local.py --reconcile-only --reconcile-days 80             # do the repair (asks you to confirm)
    python3 tools/run_local.py --dry-run                                        # an ordinary dry run (3-day window)

--reconcile-only means "only re-check past days": today's rows and run_status.json
are never written, and it works on weekends too. Use it for repairs.

Run it from the repo root, and not between about 08:45 and 09:15 IST, when the
scheduled run is writing to the same repo.
"""
import argparse
import importlib.util
import os
import re
import sys

FN_PATH = 'functions/packages/nse_momentum_ranking/daily_run/__main__.py'
PROJECT_YML = 'functions/project.yml'


def load_env(path):
    """Minimal .env reader: KEY=value, optional 'export ', optional quotes, # comments."""
    if not os.path.exists(path):
        return False
    with open(path, encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            if line.startswith('export '):
                line = line[len('export '):]
            key, _, value = line.partition('=')
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)
    return True


def _clean(value):
    """Strip quotes (taking only what is inside them) or, if unquoted, a trailing ' # comment'."""
    value = value.strip()
    if value[:1] in ('"', "'"):
        end = value.find(value[0], 1)
        if end != -1:
            return value[1:end]
    return value.split(' #')[0].strip()


def load_project_env(path):
    """
    Fallback: read the `environment:` mapping(s) of a DigitalOcean project.yml
    (KEY: value lines indented under `environment:`). A value written as ${NAME}
    is taken from the process environment if NAME is set there, else skipped.
    """
    if not os.path.exists(path):
        return False
    with open(path, encoding='utf-8') as f:
        lines = f.read().splitlines()
    found, i = False, 0
    while i < len(lines):
        m = re.match(r'^(\s*)environment:\s*(#.*)?$', lines[i])
        i += 1
        if not m:
            continue
        base = len(m.group(1))
        while i < len(lines):
            line = lines[i]
            if not line.strip() or line.strip().startswith('#'):
                i += 1
                continue
            if len(line) - len(line.lstrip()) <= base:
                break
            key, sep, value = line.strip().partition(':')
            if sep and key:
                value = _clean(value)
                ph = re.fullmatch(r'\$\{(\w+)\}', value)
                if ph:
                    value = os.environ.get(ph.group(1), '')
                if value:
                    os.environ.setdefault(key.strip(), value)
                    found = True
            i += 1
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='do everything except write to GitHub')
    ap.add_argument('--force', action='store_true', help='run even on a weekend/holiday')
    ap.add_argument('--reconcile-only', action='store_true', help='only re-check past days; never write today\'s rows or the status file')
    ap.add_argument('--reconcile-days', type=int, default=None, help='window of past run dates to re-check (default: the function\'s own, 3)')
    ap.add_argument('--env', default='.env', help='path of the .env file (default: .env)')
    ap.add_argument('--project', default=PROJECT_YML, help='project.yml to fall back on (default: functions/project.yml)')
    ap.add_argument('--email', action='store_true', help='allow the function to send its alert emails')
    ap.add_argument('--yes', action='store_true', help='do not ask for confirmation before a real (non dry-run) run')
    args = ap.parse_args()

    if not os.path.exists(FN_PATH):
        sys.exit(f'{FN_PATH} not found - run this from the repo root.')
    sources = []
    if load_env(args.env):
        sources.append(args.env)
    missing = [k for k in ('GITHUB_PAT', 'GITHUB_REPO') if not os.environ.get(k)]
    if missing and load_project_env(args.project):
        sources.append(args.project)
    missing = [k for k in ('GITHUB_PAT', 'GITHUB_REPO') if not os.environ.get(k)]
    if missing:
        sys.exit(f'Missing {", ".join(missing)}. Set them in the environment, in {args.env}, '
                 f'or in the environment: block of {args.project}.')
    print(f'Credentials loaded from: {", ".join(sources) if sources else "the environment"} (values are not shown)')
    if not args.email:
        for k in ('GMAIL_SENDER', 'GMAIL_APP_PASSWORD', 'GMAIL_RECIPIENT'):
            os.environ.pop(k, None)

    payload = {}
    if args.dry_run:
        payload['dry_run'] = True
    if args.force:
        payload['force'] = True
    if args.reconcile_only:
        payload['reconcile_only'] = True
    if args.reconcile_days is not None:
        payload['reconcile_days'] = args.reconcile_days

    if not args.dry_run and not args.yes:
        print(f'This will WRITE to GitHub repo {os.environ["GITHUB_REPO"]} (history files and run_status.json).')
        if input('Type yes to continue: ').strip().lower() != 'yes':
            sys.exit('Cancelled.')

    spec = importlib.util.spec_from_file_location('daily_run_main', FN_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    result = mod.main(payload)
    print('\nRESULT:', result)
    sys.exit(0 if result.get('statusCode') == 200 else 1)


if __name__ == '__main__':
    main()
