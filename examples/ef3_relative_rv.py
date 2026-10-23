"""Synthetic contract fixtures -> EF3 worker -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture: RS01-RS24 (relative statistics), DV01-DV17 (daily log volatility) and
RV01-RV21 (intraday realized variance), 62 in total, plus the prefix-property fixtures PX01 (on RS02) and PX02 (on
DV02). Each fixture is run once per selected output (one-output selection, Amendment 1 item 2) and once jointly
when it selects several outputs; every run goes through the adapter, the worker, the pure library, a separate DuckDB
sink, SDK readback, replay and close/reopen. PX03 is not constructible with later bars under the R17 context and is
not run here (the contract checks it through RV21, which is a case above).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict
from html import escape
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

CONTRACT = 'RELATIVE_RV_CONTRACT_PROPOSAL.json'
FAMILIES = ('relative_stats', 'daily_log_volatility', 'intraday_realized_variance')
RESULT_COUNT = 62
PREFIX_CASES = ('PX01', 'PX02')
PREFIX_BASE = {'PX01': 'RS02', 'PX02': 'DV02'}


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef3' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def fixtures_by_id(document: dict) -> dict:
    """Result fixtures (62) and prefix-property fixtures (PX01, PX02) by contract id."""
    rows = {row['id']: row for family in FAMILIES for row in document['fixtures'][family]}
    rows.update({row['id']: row for row in document['fixtures']['prefix_property'] if row['id'] in PREFIX_CASES})
    return rows


def accept(reference: dict, value) -> bool:
    """Value acceptance: 'bits' is bit-exact (with +0.0 where stated); 'r17_tol' is abs(y-r) <= max(8*ulp(r), 1e-12*abs(r))."""
    want = reference['value']
    if want is None or value is None:
        return want is None and value is None
    r = float.fromhex(want['hex'])
    if want['acceptance'] == 'bits':
        return value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r)
    return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))


def witness(row, prefixed: bool) -> str:
    """Contract witness text: the input role (relative statistics only) and the row id; excluded rows carry their reason."""
    text = (row.input_id + ':' if prefixed else '') + row.row_id
    if row.use == 'excluded':
        return 'excluded:' + text + ':' + row.exclusion_reason.value
    return text


def runs_for(fixture: dict) -> list[dict]:
    """Each run selects a set of outputs and carries the contract expectations for exactly those outputs."""
    expected = fixture['expected']
    if fixture['id'] in PREFIX_CASES:
        return [dict(variant=variant, features=tuple(sorted(expected)), expected=expected) for variant in range(3)]
    runs = [dict(variant=None, features=(feature,), expected={feature: expected[feature]}) for feature in sorted(expected)]
    if len(expected) > 1:
        runs.append(dict(variant=None, features=tuple(sorted(expected)), expected=expected))
    return runs


def relative_inputs(fixture: dict, variant: int | None, scale: int, limit: int):
    from equity_feature_duckdb.relative_rv import (availability_for, close_batch, daily_grid, daily_sessions,
        relative_config)
    inst, bench = list(fixture['inst']), list(fixture['bench'])
    if variant is not None:
        inst += SUFFIX_RELATIVE[variant][0]
        bench += SUFFIX_RELATIVE[variant][1]
    sessions = daily_sessions(len(inst))
    grid = daily_grid(sessions, target_index=fixture['t'])
    batches = (close_batch('X', grid, inst, input_id='instrument', scale=scale),
        close_batch('B', grid, bench, input_id='benchmark', scale=scale))
    config = relative_config(grid, benchmark_id='B', window=fixture['W'], min_pairs=fixture['minp'],
        availability=availability_for(sessions, fixture['K']), scale=scale, evidence_limit=limit)
    return grid, config, batches, ('instrument', 'benchmark')


def logvol_inputs(fixture: dict, variant: int | None, scale: int, limit: int):
    from equity_feature_duckdb.relative_rv import (availability_for, close_batch, daily_grid, daily_sessions,
        log_volatility_config)
    slots = list(fixture['slots'])
    if variant is not None:
        slots += SUFFIX_LOGVOL[variant]
    sessions = daily_sessions(len(slots))
    grid = daily_grid(sessions, target_index=fixture['t'])
    batches = (close_batch('X', grid, slots, input_id='daily', scale=scale),)
    config = log_volatility_config(grid, period=fixture['N'], annualization_factor=fixture['A'],
        availability=availability_for(sessions, fixture['K']), scale=scale, evidence_limit=limit)
    return grid, config, batches, ('daily',)


def realized_inputs(fixture: dict, features: tuple[str, ...], scale: int, limit: int):
    from equity_feature_duckdb.relative_rv import (availability_for, intraday_batch, make_intraday_context,
        realized_variance_config, synthetic_intraday_session)
    slots = list(fixture['slots'])
    session, runs = synthetic_intraday_session(len(slots))
    context = make_intraday_context('X', sessions=session, runs=runs, anchor_index=fixture['a'],
        target_index=fixture['t'])
    batches = (intraday_batch(context, slots, input_id='bars', scale=scale),)
    # The annualization factor is bound exactly when volatility is selected (library rule); variance-only runs omit it.
    factor = fixture['A'] if 'intraday.realized_volatility' in features else None
    config = realized_variance_config(context, annualization_factor=factor,
        availability=availability_for(session, fixture['K']), scale=scale, evidence_limit=limit)
    return context, config, batches, ('bars:000',)


# Prefix-property suffixes (contract PX01 and PX02): slots appended after the target, which can never influence it.
SUFFIX_RELATIVE = (
    ([{'state': 'priced', 'c': 1000000, 'known_at': 50}, {'state': 'priced', 'c': 1, 'known_at': 50}],
     [{'state': 'priced', 'c': 1000000, 'known_at': 50}, {'state': 'priced', 'c': 1, 'known_at': 50}]),
    ([{'state': 'absent'}, {'state': 'absent'}],
     [{'state': 'priced', 'c': 1000000, 'known_at': 50}, {'state': 'priced', 'c': 1, 'known_at': 50}]),
    ([{'state': 'future', 'known_at': 150}, {'state': 'future', 'known_at': 150}],
     [{'state': 'priced', 'c': 1000000, 'known_at': 50}, {'state': 'priced', 'c': 1, 'known_at': 50}]),
)
SUFFIX_LOGVOL = (
    [{'state': 'priced', 'c': 1, 'known_at': 50}, {'state': 'priced', 'c': 1000000000, 'known_at': 50}],
    [{'state': 'absent'}],
    [{'state': 'gap'}],
)


def declaration(role: str, batch) -> InputManifest:
    raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
        InputBinding(role, batch.kind, batch.metadata))


def compare(result, fixture: dict, run: dict) -> list:
    """Values (acceptance rule), status, reasons, counts and witnesses against the contract literal for this run."""
    expected = run['expected']
    limit = fixture['evidence_limit']
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(expected)):
        problems.append(['selected outputs', [c.feature_id for c in result.values], sorted(expected)])
    prefixed = fixture_family(fixture) == 'relative_stats'
    found = {feature: [] for feature in expected}
    for row in result.evidence:
        found[row.feature_id].append(witness(row, prefixed))
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = expected[column.feature_id]
        stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['status'], want['reasons'], want['expected'], want['observed'])
        if not (accept(want, value) and stored == literal):
            problems.append([column.feature_id, 'value/quality', value, stored, literal])
        # Per-output witnesses hold for a one-output selection (Amendment 1 item 2); a joint run is checked flat below.
        if len(run['features']) == 1 and found[column.feature_id] != want.get('witnesses', []):
            problems.append([column.feature_id, 'witnesses', found[column.feature_id], want.get('witnesses', [])])
    if len(run['features']) > 1:
        want_joint = fixture.get('joint_selection_evidence') or [
            text for feature in sorted(expected) for text in expected[feature].get('witnesses', [])]
        want_joint = want_joint[:limit]
        flat = [witness(row, prefixed) for row in result.evidence]
        if flat != want_joint:
            problems.append(['joint evidence', flat, want_joint])
    return problems


def ordered(items, outcome) -> list:
    """Readbacks in item order, matched by task digest."""
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


def build_item(fixture: dict, run: dict, number: int, run_id: str, scope: str, limit: int):
    """One worker item for one run, plus the library grid or context it was built from (for the readback check)."""
    label = '%s-%d' % (run_id, number)
    features = run['features']
    family = fixture_family(fixture)
    if family == 'relative_stats':
        from equity_feature_workers.equity_relative_rv import make_ef3_relative_work
        from equity_features.ef3 import DailyGrid
        grid, config, batches, roles = relative_inputs(fixture, run['variant'], fixture['scale'], limit)
        library_grid = DailyGrid(grid.instrument_id, grid.calendar_id, grid.grid_id, grid.revision, grid.sessions,
            grid.target_index)
        decls = tuple(declaration(role, batch) for role, batch in zip(roles, batches))
        item = make_ef3_relative_work(config, grid=library_grid, features=features, inputs=decls, batches=batches,
            instrument_id='X', job_id='job-' + label, generation_id='generation-' + label, partition_id='X-S',
            destination_scope=scope)
    elif family == 'daily_log_volatility':
        from equity_feature_workers.equity_relative_rv import make_ef3_log_volatility_work
        from equity_features.ef3 import DailyGrid
        grid, config, batches, roles = logvol_inputs(fixture, run['variant'], fixture['scale'], limit)
        library_grid = DailyGrid(grid.instrument_id, grid.calendar_id, grid.grid_id, grid.revision, grid.sessions,
            grid.target_index)
        decls = tuple(declaration(role, batch) for role, batch in zip(roles, batches))
        item = make_ef3_log_volatility_work(config, grid=library_grid, features=features, inputs=decls,
            batches=batches, instrument_id='X', job_id='job-' + label, generation_id='generation-' + label,
            partition_id='X-S', destination_scope=scope)
    else:
        from equity_feature_workers.equity_relative_rv import make_ef3_realized_variance_work
        context, config, batches, roles = realized_inputs(fixture, features, fixture['scale'], limit)
        decls = tuple(declaration(role, batch) for role, batch in zip(roles, batches))
        item = make_ef3_realized_variance_work(config, features=features, inputs=decls, batches=batches,
            instrument_id='X', job_id='job-' + label, generation_id='generation-' + label, partition_id='X-S',
            destination_scope=scope)
    return item


def fixture_family(fixture: dict) -> str:
    if 'family' in fixture:
        return fixture['family']
    prefix = {'RS': 'relative_stats', 'DV': 'daily_log_volatility', 'RV': 'intraday_realized_variance'}
    return prefix[fixture['id'][:2]]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', required=True, help='contract result fixture id, e.g. RS01, DV04, RV21, PX01')
    parser.add_argument('--output-db', type=Path, required=True)
    parser.add_argument('--html', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    args = parser.parse_args(argv)
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()
            or any(ord(character) < 32 or ord(character) > 126 for character in args.run_id)):
        parser.error('Require new distinct absolute output paths, existing parents and bounded ASCII run identity')
    if len(str(args.output_db)) + 32 >= 260:
        parser.error('Output path exceeds bounded Windows sidecar headroom')
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.ef1_runner import (run_ef3_log_volatility, run_ef3_realized_variance, run_ef3_relative)

    document = contract_document(args.contract)
    fixtures = fixtures_by_id(document)
    if len(fixtures) != RESULT_COUNT + len(PREFIX_CASES) or args.case not in fixtures:
        parser.error('Contract fixtures differ from the example case list')
    fixture = dict(fixtures[args.case])
    if args.case in PREFIX_CASES:
        base = fixtures[PREFIX_BASE[args.case]]
        fixture = dict(base, id=args.case, family=fixture_family(base))
    # fixture_family() reads the family of a PX case from its base; the scale and limits are the base fixture's.
    family = fixture_family(fixture)
    limit = fixture['evidence_limit']
    scope = 'ef3-' + family.replace('_', '-') + '-synthetic'
    runner = {'relative_stats': run_ef3_relative, 'daily_log_volatility': run_ef3_log_volatility,
        'intraday_realized_variance': run_ef3_realized_variance}[family]
    runs = runs_for(fixture)
    items = [build_item(fixture, run, number, args.run_id, scope, limit) for number, run in enumerate(runs)]
    limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576, max_total_bytes=1048576 * len(items),
        max_result_cells=4 * len(items), max_evidence_rows=max(1, min(limit, 40960)) * len(items))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = runner(tuple(items), sink, requirements=limits)
        if len(outcome.readbacks) != len(items) or not all(row.verified for row in outcome.readbacks):
            print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]}, sort_keys=True))
            return 1
        rows = ordered(items, outcome)
        stored = [row.results[0] for row in rows]
        problems = []
        for run, result in zip(runs, stored):
            found = compare(result, fixture, run)
            if found:
                problems.append(found)
        if problems:
            raise AssertionError('Calculated result differs from the contract fixture literal: ' + json.dumps(problems, default=str))
        sdk = b'\n'.join(encode_result(result) for result in stored)
        replay = runner(tuple(items), sink, requirements=limits)
        if (len(replay.readbacks) != len(items) or not all(row.verified for row in replay.readbacks)
                or [row.output for row in ordered(items, replay)] != [row.output for row in rows]):
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        for row, result in zip(rows, stored):
            restored = reopened.read(row.output.receipt)
            verify_receipt(row.output.receipt, row.output.envelope, restored)
            if tuple(map(encode_result, restored)) != (encode_result(result),):
                raise AssertionError('Full stored-result reopen differs')
    outputs = ', '.join(sorted(fixture['expected']))
    document_html = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF3 ' + escape(family) + '</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>EF3 ' + escape(family.replace('_', ' ')) + ' (contract fixture ' + escape(args.case) + ')</h1>'
        '<p>' + escape(fixture['description']) + '. Actual input adapter  ->  worker  ->  pure library  ->  separate DuckDB sink. '
        'Contract-literal match (value by acceptance rule, status, reasons, counts, witnesses and joint evidence), '
        'SDK readback, idempotent replay and close/reopen verified. Runs: ' + str(len(runs)) + '.</p>'
        '<p>Selected outputs across runs: ' + escape(outputs) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(rows[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every return, beta, correlation, logarithm, variance and annualization '
        'value is the library result. No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document_html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(runs), 'task_sha256': items[0].task.task_sha256,
        'content_sha256': rows[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
