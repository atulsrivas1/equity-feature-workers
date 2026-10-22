"""Run the synthetic EF1 examples into one new directory and write a local index.html.

Local demonstration only: every input is synthetic, no unit tests are run, and the
page lists the gaps that remain open. Daily observations needs a corpus and is not run.
"""
from __future__ import annotations

import argparse
from html import escape
import json
from pathlib import Path
import subprocess
import sys

WORKERS = Path(__file__).resolve().parents[1]

# (example, label, extra arguments); the label becomes part of the output file stem.
_CASES = (
    ('ef1_regular_first_trade_price', 'R001', ('--case', 'R001')),
    ('ef1_regular_first_trade_price', 'R019', ('--case', 'R019')),
    ('ef1_reference_close', 'C01', ('--case', 'C01_within_tolerance')),
    ('ef1_reference_close', 'C02', ('--case', 'C02_outside_tolerance')),
    ('ef1_reference_close', 'C03', ('--case', 'C03_absent_authoritative_close')),
    ('ef1_regular_bar_prices', 'R01', ('--case', 'R01_two_independent_endpoints')),
    ('ef1_regular_bar_prices', 'R08', ('--case', 'R08_unknown_before_first')),
    ('ef1_gap_endpoint', 'E01', ('--case', 'E01_up_reversal')),
    ('ef1_gap_endpoint', 'E26', ('--case', 'E26_bar_close_at_cutoff')),
    ('ef1_clock', 'C20-present', ('--cutoff', '20', '--proof', 'present')),
    ('ef1_clock', 'C20-missing', ('--cutoff', '20', '--proof', 'missing')),
    ('ef1_clock', 'C20-unknown', ('--cutoff', '20', '--proof', 'unknown')),
    ('ef1_clock', 'C20-future', ('--cutoff', '20', '--proof', 'future')),
    ('ef1_anchors', 'trades-A01', ('--mode', 'trades', '--case', 'A01')),
    ('ef1_anchors', 'trades-A02', ('--mode', 'trades', '--case', 'A02')),
    ('ef1_anchors', 'bars-A01', ('--mode', 'bars_actual_notional', '--case', 'A01')),
    ('ef1_anchors', 'bars-A01-missing-notional', ('--mode', 'bars_actual_notional', '--case', 'A01-missing-notional')),
    ('ef1_status', 'S01', ('--case', 'S01')),
    ('ef1_status', 'S02', ('--case', 'S02')),
    ('ef1_status', 'S03', ('--case', 'S03')),
)

_GAPS = (
    'No real N: DuckDB source was read; every input here is synthetic.',
    'No installed-wheel qualification: examples ran from source directories on sys.path.',
    'No original-source parity or rights established.',
    'The full EQ164 gate remains open.',
    'Daily observations was not run: it needs a --corpus input that is not available here.',
)

# -I ignores PYTHONPATH, so the launcher sets sys.path in code and runs the example as __main__.
_LAUNCHER = ('import runpy, sys; sys.path[:0] = sys.argv[1].split("|"); script = sys.argv[2]; '
             'sys.argv = sys.argv[2:]; runpy.run_path(script, run_name="__main__")')


def source_dirs(core: Path, io: Path) -> list[str]:
    """Every package src directory of the three repositories."""
    found = [str(path) for root in (core, io, WORKERS) for path in sorted(root.glob('packages/*/src'))]
    if not found:
        raise SystemExit('No packages/*/src directories found; check --core and --io')
    return found


def run_case(example: str, label: str, extra: tuple[str, ...], out: Path, paths: str) -> dict:
    stem = example + '-' + label
    database, page = out / (stem + '.duckdb'), out / (stem + '.html')
    command = [sys.executable, '-I', '-B', '-c', _LAUNCHER, paths, str(WORKERS / 'examples' / (example + '.py')),
        '--output-db', str(database), '--html', str(page), '--run-id', 'ef1-demo-' + stem, *extra]
    done = subprocess.run(command, capture_output=True, text=True, timeout=600, cwd=out)
    flags = None
    for line in done.stdout.splitlines():
        try:
            parsed = json.loads(line)
        except ValueError:
            continue
        if isinstance(parsed, dict) and 'verified_readback' in parsed:
            flags = parsed
    if done.returncode != 0:
        status = 'FAILED'
    elif flags is None:
        status = 'ran, no match line printed'
    else:
        status = 'ran'
    return dict(example=example, label=label, code=done.returncode, status=status, flags=flags,
        stderr=done.stderr.strip()[-400:], database=database.name if database.exists() else None,
        page=page.name if page.exists() else None)


def render(rows: list[dict]) -> str:
    body = []
    for row in rows:
        flags = row['flags'] or {}
        shown = ', '.join(key + '=' + str(flags[key]) for key in
            ('verified_readback', 'verified_reopen', 'independent_literal_match') if key in flags)
        links = ' '.join('<a href="' + escape(row[key]) + '">' + name + '</a>'
            for key, name in (('page', 'html'), ('database', 'duckdb')) if row[key])
        body.append('<tr><td>' + escape(row['example']) + '</td><td>' + escape(row['label']) + '</td><td>'
            + str(row['code']) + '</td><td>' + escape(row['status']) + '</td><td>' + escape(shown or '-')
            + ('<br><code>' + escape(row['stderr']) + '</code>' if row['code'] else '')
            + '</td><td>' + (links or '-') + '</td></tr>')
    body.append('<tr><td>ef1_daily_observations</td><td>-</td><td>-</td><td>not run</td>'
        '<td>needs a --corpus input that is not available</td><td>-</td></tr>')
    failed = sum(1 for row in rows if row['code'] != 0)
    return ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF1 local results demo</title>'
        '<style>body{font-family:sans-serif;margin:2em}table{border-collapse:collapse}'
        'td,th{border:1px solid #888;padding:4px 8px;text-align:left;vertical-align:top}</style>'
        '<h1>EF1 local results demo</h1>'
        '<p><strong>All data on this page and in the linked files is synthetic.</strong> '
        'No unit tests were run; each example was run once as a script and its printed result recorded.</p>'
        '<p>' + str(len(rows)) + ' runs, ' + str(failed) + ' failed.</p>'
        '<table><tr><th>Example</th><th>Case</th><th>Exit</th><th>Status</th><th>Printed flags</th>'
        '<th>Outputs</th></tr>' + ''.join(body) + '</table>'
        '<h2>Remaining gaps</h2><ul>' + ''.join('<li>' + escape(gap) + '</li>' for gap in _GAPS)
        + '</ul><p>No JavaScript or remote assets.</p></html>')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output_dir', type=Path, help='absolute path of a new directory')
    parser.add_argument('--core', type=Path, default=WORKERS.parent / 'equity-features')
    parser.add_argument('--io', type=Path, default=WORKERS.parent / 'equity-feature-io')
    args = parser.parse_args(argv)
    if not args.output_dir.is_absolute() or args.output_dir.exists() or not args.output_dir.parent.is_dir():
        parser.error('Require a new absolute output directory with an existing parent')
    paths = '|'.join(source_dirs(args.core.resolve(), args.io.resolve()))
    args.output_dir.mkdir()
    rows = []
    for example, label, extra in _CASES:
        rows.append(run_case(example, label, extra, args.output_dir, paths))
        print(rows[-1]['example'], rows[-1]['label'], rows[-1]['status'], flush=True)
    with (args.output_dir / 'index.html').open('x', encoding='utf-8', newline='\n') as target:
        target.write(render(rows))
    return 0 if all(row['code'] == 0 for row in rows) else 1


if __name__ == '__main__':
    raise SystemExit(main())
