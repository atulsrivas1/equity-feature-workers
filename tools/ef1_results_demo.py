"""Run the synthetic EF1 and EF2 examples into one new directory and write a local index.html.

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

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ef1_readable_view  # noqa: E402

WORKERS = Path(__file__).resolve().parents[1]

# (example, label, extra arguments); the label becomes part of the output file stem.
_CASES = (
    ('ef1_regular_first_trade_price', 'R001', ('--case', 'R001')),
    ('ef1_regular_first_trade_price', 'R019', ('--case', 'R019')),
    ('ef1_reference_close', 'C01', ('--case', 'C01_within_tolerance')),
    ('ef1_reference_close', 'C02', ('--case', 'C02_outside_tolerance')),
    ('ef1_reference_close', 'C03', ('--case', 'C03_absent_authoritative_close')),
    ('ef1_venue_status', 'MV01', ('--case', 'MV01')),
    ('ef1_venue_status', 'MV02', ('--case', 'MV02')),
    ('ef1_venue_status', 'MV03', ('--case', 'MV03')),
    ('ef1_venue_status', 'MV04', ('--case', 'MV04')),
    ('ef1_venue_status', 'MV05', ('--case', 'MV05')),
    ('ef1_venue_status', 'MV06', ('--case', 'MV06')),
    ('ef1_venue_status', 'MV07', ('--case', 'MV07')),
    ('ef1_venue_status', 'MV08', ('--case', 'MV08')),
    ('ef1_venue_status', 'MV09', ('--case', 'MV09')),
    ('ef1_venue_status', 'MV10', ('--case', 'MV10')),
    ('ef1_venue_status', 'MV11', ('--case', 'MV11')),
    ('ef1_venue_status', 'MV12', ('--case', 'MV12')),
    ('ef1_venue_status', 'MV13', ('--case', 'MV13')),
    ('ef1_venue_status', 'MV14', ('--case', 'MV14')),
    ('ef1_venue_status', 'MV15', ('--case', 'MV15')),
    ('ef1_venue_status', 'MV16', ('--case', 'MV16')),
    ('ef1_venue_status', 'MV17', ('--case', 'MV17')),
    ('ef1_venue_status', 'MV18', ('--case', 'MV18')),
    ('ef1_venue_status', 'MV19', ('--case', 'MV19')),
    ('ef1_venue_status', 'MV20', ('--case', 'MV20')),
    ('ef1_venue_status', 'MV21', ('--case', 'MV21')),
    ('ef1_venue_status', 'MV22', ('--case', 'MV22')),
    ('ef1_venue_status', 'MV23', ('--case', 'MV23')),
    ('ef1_venue_status', 'MV24', ('--case', 'MV24')),
    ('ef1_venue_status', 'MV25', ('--case', 'MV25')),
    ('ef1_venue_status', 'MV26', ('--case', 'MV26')),
    ('ef1_venue_status', 'MV27', ('--case', 'MV27')),
    ('ef1_venue_status', 'MV28', ('--case', 'MV28')),
    ('ef1_venue_status', 'MV29', ('--case', 'MV29')),
    ('ef1_venue_status', 'MV30', ('--case', 'MV30')),
    ('ef1_venue_status', 'MV31', ('--case', 'MV31')),
    ('ef1_venue_status', 'MV32', ('--case', 'MV32')),
    ('ef1_venue_status', 'MV33', ('--case', 'MV33')),
    ('ef1_venue_status', 'MV34', ('--case', 'MV34')),
    ('ef1_venue_status', 'MV35', ('--case', 'MV35')),
    ('ef1_venue_status', 'MV36', ('--case', 'MV36')),
    ('ef1_venue_status', 'MV37', ('--case', 'MV37')),
    ('ef1_venue_status', 'MV40', ('--case', 'MV40')),
    ('ef1_venue_status', 'MV41', ('--case', 'MV41')),
    ('ef1_venue_status', 'MV42', ('--case', 'MV42')),
    ('ef1_venue_status', 'MV43', ('--case', 'MV43')),
    ('ef1_venue_status', 'MV44', ('--case', 'MV44')),
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
    ('ef2_money_flow', 'M01', ('--case', 'M01')),
    ('ef2_money_flow', 'M02', ('--case', 'M02')),
    ('ef2_money_flow', 'M03', ('--case', 'M03')),
    ('ef2_money_flow', 'M04', ('--case', 'M04')),
    ('ef2_money_flow', 'M05', ('--case', 'M05')),
    ('ef2_money_flow', 'M06', ('--case', 'M06')),
    ('ef2_money_flow', 'M07', ('--case', 'M07')),
    ('ef2_money_flow', 'M08', ('--case', 'M08')),
    ('ef2_money_flow', 'M09', ('--case', 'M09')),
    ('ef2_money_flow', 'M10', ('--case', 'M10')),
    ('ef2_money_flow', 'M11', ('--case', 'M11')),
    ('ef2_money_flow', 'M12', ('--case', 'M12')),
    ('ef2_money_flow', 'M13', ('--case', 'M13')),
    ('ef2_money_flow', 'M14', ('--case', 'M14')),
    ('ef2_money_flow', 'M15', ('--case', 'M15')),
    ('ef2_money_flow', 'M16', ('--case', 'M16')),
    ('ef2_money_flow', 'M17', ('--case', 'M17')),
    ('ef2_money_flow', 'M18', ('--case', 'M18')),
    ('ef2_money_flow', 'M19', ('--case', 'M19')),
    ('ef2_money_flow', 'M20', ('--case', 'M20')),
    ('ef2_money_flow', 'M21', ('--case', 'M21')),
    ('ef2_money_flow', 'M22', ('--case', 'M22')),
    ('ef2_money_flow', 'M23', ('--case', 'M23')),
    ('ef2_money_flow', 'M24', ('--case', 'M24')),
    ('ef2_money_flow', 'M25', ('--case', 'M25')),
    ('ef2_money_flow', 'M26', ('--case', 'M26')),
    ('ef2_money_flow', 'M27', ('--case', 'M27')),
    ('ef2_money_flow', 'M28', ('--case', 'M28')),
    ('ef2_money_flow', 'M29', ('--case', 'M29')),
    ('ef2_money_flow', 'M30', ('--case', 'M30')),
    ('ef2_money_flow', 'M31', ('--case', 'M31')),
    ('ef2_money_flow', 'M32', ('--case', 'M32')),
    ('ef2_money_flow', 'M33', ('--case', 'M33')),
    ('ef2_money_flow', 'M34', ('--case', 'M34')),
    ('ef2_money_flow', 'M35', ('--case', 'M35')),
    ('ef2_money_flow', 'M36', ('--case', 'M36')),
    ('ef2_retracement', 'T01', ('--case', 'T01')),
    ('ef2_retracement', 'T02', ('--case', 'T02')),
    ('ef2_retracement', 'T03', ('--case', 'T03')),
    ('ef2_retracement', 'T04', ('--case', 'T04')),
    ('ef2_retracement', 'T05', ('--case', 'T05')),
    ('ef2_retracement', 'T06', ('--case', 'T06')),
    ('ef2_retracement', 'T07', ('--case', 'T07')),
    ('ef2_retracement', 'T08', ('--case', 'T08')),
    ('ef2_retracement', 'T09', ('--case', 'T09')),
    ('ef2_retracement', 'T10', ('--case', 'T10')),
    ('ef2_retracement', 'T11', ('--case', 'T11')),
    ('ef2_retracement', 'T12', ('--case', 'T12')),
    ('ef2_retracement', 'T13', ('--case', 'T13')),
    ('ef2_retracement', 'T14', ('--case', 'T14')),
    ('ef2_retracement', 'T15', ('--case', 'T15')),
    ('ef2_retracement', 'T16', ('--case', 'T16')),
    ('ef2_retracement', 'T17', ('--case', 'T17')),
    ('ef2_retracement', 'T18', ('--case', 'T18')),
    ('ef2_retracement', 'T19', ('--case', 'T19')),
    ('ef2_retracement', 'T20', ('--case', 'T20')),
    ('ef2_retracement', 'T21', ('--case', 'T21')),
    ('ef2_retracement', 'T22', ('--case', 'T22')),
    ('ef2_retracement', 'T23', ('--case', 'T23')),
    ('ef2_retracement', 'T24', ('--case', 'T24')),
    ('ef2_retracement', 'T25', ('--case', 'T25')),
    ('ef2_retracement', 'T26', ('--case', 'T26')),
    ('ef2_retracement', 'T27', ('--case', 'T27')),
    ('ef2_retracement', 'T28', ('--case', 'T28')),
    ('ef2_retracement', 'T29', ('--case', 'T29')),
    ('ef2_retracement', 'T30', ('--case', 'T30')),
    ('ef2_retracement', 'T31', ('--case', 'T31')),
    ('ef2_retracement', 'T32', ('--case', 'T32')),
    ('ef2_momentum', 'M01', ('--case', 'M01')),
    ('ef2_momentum', 'M02', ('--case', 'M02')),
    ('ef2_momentum', 'M03', ('--case', 'M03')),
    ('ef2_momentum', 'M04', ('--case', 'M04')),
    ('ef2_momentum', 'M05', ('--case', 'M05')),
    ('ef2_momentum', 'M06', ('--case', 'M06')),
    ('ef2_momentum', 'M07_5', ('--case', 'M07_5')),
    ('ef2_momentum', 'M07_6', ('--case', 'M07_6')),
    ('ef2_momentum', 'M07_7', ('--case', 'M07_7')),
    ('ef2_momentum', 'M07_8', ('--case', 'M07_8')),
    ('ef2_momentum', 'M07_9', ('--case', 'M07_9')),
    ('ef2_momentum', 'M07_10', ('--case', 'M07_10')),
    ('ef2_momentum', 'M05b', ('--case', 'M05b')),
    ('ef2_momentum', 'M25', ('--case', 'M25')),
    ('ef2_momentum', 'M26', ('--case', 'M26')),
    ('ef2_momentum', 'M08', ('--case', 'M08')),
    ('ef2_momentum', 'M09', ('--case', 'M09')),
    ('ef2_momentum', 'M10', ('--case', 'M10')),
    ('ef2_momentum', 'M11', ('--case', 'M11')),
    ('ef2_momentum', 'M12', ('--case', 'M12')),
    ('ef2_momentum', 'M13', ('--case', 'M13')),
    ('ef2_momentum', 'M14', ('--case', 'M14')),
    ('ef2_momentum', 'M15', ('--case', 'M15')),
    ('ef2_momentum', 'M16', ('--case', 'M16')),
    ('ef2_momentum', 'M17', ('--case', 'M17')),
    ('ef2_momentum', 'M18', ('--case', 'M18')),
    ('ef2_momentum', 'M19', ('--case', 'M19')),
    ('ef2_momentum', 'M20', ('--case', 'M20')),
    ('ef2_momentum', 'M22', ('--case', 'M22')),
    ('ef2_momentum', 'M23', ('--case', 'M23')),
    ('ef2_momentum', 'M24', ('--case', 'M24')),
    ('ef2_momentum', 'D02_inside', ('--case', 'D02_inside')),
    ('ef2_momentum', 'D02_outside_up', ('--case', 'D02_outside_up')),
    ('ef2_momentum', 'D02_outside_down', ('--case', 'D02_outside_down')),
    ('ef2_momentum', 'D02_tie_positive', ('--case', 'D02_tie_positive')),
    ('ef2_momentum', 'D02_shift_up', ('--case', 'D02_shift_up')),
    ('ef2_momentum', 'D02_shift_down', ('--case', 'D02_shift_down')),
    ('ef2_momentum', 'D02_gap_up', ('--case', 'D02_gap_up')),
    ('ef2_momentum', 'D02_zero_up', ('--case', 'D02_zero_up')),
    ('ef2_momentum', 'D02_tie_zero', ('--case', 'D02_tie_zero')),
    ('ef2_momentum', 'D02_flat2', ('--case', 'D02_flat2')),
    ('ef2_momentum', 'D01', ('--case', 'D01')),
    ('ef2_momentum', 'D03_3', ('--case', 'D03_3')),
    ('ef2_momentum', 'D03_4', ('--case', 'D03_4')),
    ('ef2_momentum', 'D03_5', ('--case', 'D03_5')),
    ('ef2_momentum', 'D03_7', ('--case', 'D03_7')),
    ('ef2_momentum', 'D04', ('--case', 'D04')),
    ('ef2_momentum', 'D05', ('--case', 'D05')),
    ('ef2_momentum', 'D06_3', ('--case', 'D06_3')),
    ('ef2_momentum', 'D06_4', ('--case', 'D06_4')),
    ('ef2_momentum', 'D06_5', ('--case', 'D06_5')),
    ('ef2_momentum', 'D06_6', ('--case', 'D06_6')),
    ('ef2_momentum', 'D07', ('--case', 'D07')),
    ('ef2_momentum', 'D08', ('--case', 'D08')),
    ('ef2_momentum', 'D09', ('--case', 'D09')),
    ('ef2_momentum', 'D10', ('--case', 'D10')),
    ('ef2_momentum', 'D11', ('--case', 'D11')),
    ('ef2_momentum', 'D12', ('--case', 'D12')),
    ('ef2_momentum', 'D13', ('--case', 'D13')),
    ('ef2_momentum', 'D14', ('--case', 'D14')),
    ('ef2_momentum', 'D15', ('--case', 'D15')),
    ('ef2_momentum', 'D16', ('--case', 'D16')),
    ('ef2_momentum', 'D17', ('--case', 'D17')),
    ('ef2_momentum', 'D18', ('--case', 'D18')),
    ('ef2_momentum', 'D20', ('--case', 'D20')),
    ('ef2_momentum', 'D21', ('--case', 'D21')),
    ('ef2_momentum', 'D22', ('--case', 'D22')),
    ('ef2_momentum', 'D19', ('--case', 'D19')),
    ('ef2_momentum', 'T01', ('--case', 'T01')),
    ('ef2_momentum', 'T02', ('--case', 'T02')),
    ('ef2_momentum', 'T03', ('--case', 'T03')),
    ('ef2_momentum', 'T04', ('--case', 'T04')),
    ('ef2_momentum', 'T05', ('--case', 'T05')),
    ('ef2_momentum', 'T06', ('--case', 'T06')),
    ('ef2_momentum', 'T07', ('--case', 'T07')),
    ('ef2_momentum', 'T08', ('--case', 'T08')),
    ('ef2_momentum', 'T09', ('--case', 'T09')),
    ('ef2_momentum', 'T10', ('--case', 'T10')),
    ('ef2_momentum', 'T11', ('--case', 'T11')),
    ('ef2_momentum', 'T12', ('--case', 'T12')),
    ('ef2_momentum', 'T13', ('--case', 'T13')),
    ('ef2_momentum', 'T14', ('--case', 'T14')),
    ('ef2_momentum', 'T15', ('--case', 'T15')),
    ('ef2_momentum', 'T16', ('--case', 'T16')),
    ('ef2_momentum', 'T17', ('--case', 'T17')),
    ('ef2_momentum', 'T18', ('--case', 'T18')),
    ('ef2_momentum', 'T19', ('--case', 'T19')),
    ('ef2_momentum', 'T20', ('--case', 'T20')),
    ('ef2_momentum', 'T21', ('--case', 'T21')),
    ('ef2_momentum', 'T22', ('--case', 'T22')),
    ('ef2_momentum', 'T23', ('--case', 'T23')),
    ('ef2_momentum', 'T24', ('--case', 'T24')),
    ('ef2_momentum', 'T25', ('--case', 'T25')),
    ('ef2_momentum', 'T27', ('--case', 'T27')),
    ('ef2_momentum', 'T28', ('--case', 'T28')),
    ('ef2_momentum', 'T29', ('--case', 'T29')),
    ('ef2_momentum', 'T30', ('--case', 'T30')),
    ('ef2_momentum', 'P01', ('--case', 'P01')),
    ('ef2_momentum', 'P02', ('--case', 'P02')),
    ('ef2_momentum', 'P03', ('--case', 'P03')),
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
            for key, name in (('view', 'readable'), ('page', 'raw json'), ('database', 'duckdb')) if row.get(key))
        body.append('<tr><td>' + escape(row['example']) + '</td><td>' + escape(row['label']) + '</td><td>'
            + str(row['code']) + '</td><td>' + escape(row['status']) + '</td><td>' + escape(shown or '-')
            + ('<br><code>' + escape(row['stderr']) + '</code>' if row['code'] else '')
            + '</td><td>' + escape(row.get('summary') or '-') + '</td><td>' + (links or '-') + '</td></tr>')
    body.append('<tr><td>ef1_daily_observations</td><td>-</td><td>-</td><td>not run</td>'
        '<td>needs a --corpus input that is not available</td><td>-</td><td>-</td></tr>')
    failed = sum(1 for row in rows if row['code'] != 0)
    return ('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>EF1 local results demo</title>'
        '<style>body{font-family:sans-serif;margin:1em}.wrap{overflow-x:auto}table{border-collapse:collapse}'
        'td,th{border:1px solid #888;padding:4px 8px;text-align:left;vertical-align:top}</style>'
        '<h1>EF1 local results demo</h1>'
        '<p><strong>All data on this page and in the linked files is synthetic.</strong> '
        'No unit tests were run; each example was run once as a script and its printed result recorded.</p>'
        '<p>' + str(len(rows)) + ' runs, ' + str(failed) + ' failed.</p>'
        '<div class="wrap"><table><tr><th>Example</th><th>Case</th><th>Exit</th><th>Status</th><th>Printed flags</th>'
        '<th>Values</th><th>Outputs</th></tr>' + ''.join(body) + '</table></div>'
        '<h2>Remaining gaps</h2><ul>' + ''.join('<li>' + escape(gap) + '</li>' for gap in _GAPS)
        + '</ul><p>No JavaScript or remote assets.</p></html>')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output_dir', type=Path, help='absolute path of a new directory')
    parser.add_argument('--core', type=Path, default=WORKERS.parent / 'equity-features')
    parser.add_argument('--io', type=Path, default=WORKERS.parent / 'equity-feature-io')
    parser.add_argument('--only', default='', help='run only cases whose stem contains this text (default: all)')
    args = parser.parse_args(argv)
    if not args.output_dir.is_absolute() or args.output_dir.exists() or not args.output_dir.parent.is_dir():
        parser.error('Require a new absolute output directory with an existing parent')
    paths = '|'.join(source_dirs(args.core.resolve(), args.io.resolve()))
    args.output_dir.mkdir()
    rows = []
    for example, label, extra in _CASES:
        if args.only not in example + '-' + label:
            continue
        rows.append(run_case(example, label, extra, args.output_dir, paths))
        try:  # the readable view only reads the finished page; a failure here never changes the run result
            rows[-1].update(ef1_readable_view.write_view(rows[-1], args.output_dir, _GAPS))
        except (OSError, ValueError, KeyError) as error:
            rows[-1]['summary'] = 'readable view failed: ' + str(error)
        print(rows[-1]['example'], rows[-1]['label'], rows[-1]['status'], flush=True)
    with (args.output_dir / 'index.html').open('x', encoding='utf-8', newline='\n') as target:
        target.write(render(rows))
    return 0 if all(row['code'] == 0 for row in rows) else 1


if __name__ == '__main__':
    raise SystemExit(main())
