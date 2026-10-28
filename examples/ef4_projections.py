"""Synthetic contract fixtures -> EF4 projections workers -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture id of the EF4-I2 projections contract (EQ-172): SP01-SP22 (session projection),
BP01-BP19 (bar projection), RX01-RX39 (relative excess and core benchmark outputs) and RP01-RP31 (rate projection), 111
in total including the prefix fixtures SP17, BP16, SP12, BP12 and RX18. Each fixture is run once with all of its listed
outputs (the joint run, the only valid selection for a relative-excess or rate configuration whose optional parameters
follow the selected outputs) and, for the session and bar projections whose configuration has no per-output
parameter, once per selected output as well. Every run goes through the supplied-input adapter
(equity_feature_duckdb.ef4_projections), the worker, the pure library, a separate DuckDB sink, SDK readback, replay and
close/reopen.

``--refusals`` drives the 114 typed refusal fixtures (SPR, BPR, RXR and RPR) through the worker path and reports, per
fixture, the stage at which it was refused (``adapter_build``: the supplied-input adapter and the shared batch rules
while the batches and envelope are built; ``grid_build``: the library grid class built by the caller from the adapter
codec; ``worker_admission``: the work item build; ``compute``: the library calculation; ``library_capability``: a
capability call that the worker does not expose), the contract code, the library's own code for the same inputs and
the worker code the caller receives.

The contract fixtures are mutated by the contract fixture harness (``ef4i_fixture_expand.py``: ``default_ctx`` and
``set_path``), which lives outside this repository. Give its path with ``--harness`` or the ``EF4I_HARNESS``
environment variable; its sha256 is printed. No entity, instrument, benchmark, session, grid or reference identifier is
fixed in the worker: the example binds the compact fixture names to synthetic ids (``--binding A`` or ``B``).
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import math
import os
import re
from collections.abc import Mapping
from dataclasses import asdict
from html import escape
from pathlib import Path

from equity_feature_contracts import ContractError, InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

CONTRACT = 'PATH_PROJECTIONS_CONTRACT_PROPOSAL.json'
FIXTURE_KEYS = ('session_fixtures', 'bar_fixtures', 'relative_fixtures', 'rate_fixtures')
RESULT_COUNT = 111
REFUSAL_COUNT = 114
ROLES = {'session_projection': ('daily',), 'bar_projection': ('bars',), 'relative_excess': ('legs', 'membership'),
         'rate_projection': ('tape', 'legs')}
# The contract fixture notation (PATH_PROJECTIONS_CONTRACT_PROPOSAL.json fixture_notation) documents known_at 50 for a slot
# without ka; the I/O adapter never defaults, so the driver supplies the documented token explicitly.
FIXTURE_DEFAULT_KA = 50
# Worker code expected for a library code (the worker collapses library codes into its own taxonomy).
WORKER_OF = {'INCONSISTENT_IDENTITY': 'RESULT_IDENTITY_MISMATCH', 'OVERFLOW': 'CALCULATION_FAILED'}


class Renamer(Mapping):
    """Caller-side binding of the compact fixture names to synthetic ids (one binding per run)."""

    def __init__(self, prefix: str, instrument: str, bar_session: str, day_prefix: str) -> None:
        self.prefix, self.instrument, self.bar_session, self.day_prefix = prefix, instrument, bar_session, day_prefix

    def get(self, key, default=None):
        if not isinstance(key, str):
            return default
        if key == '':
            return key
        if key == 'X':
            return self.instrument
        if key == 'S1':
            return self.bar_session
        day = re.fullmatch(r'D(\d+)', key)
        return self.day_prefix + day.group(1) if day else self.prefix + key

    def __getitem__(self, key):
        return self.get(key)

    def __iter__(self):
        return iter(())

    def __len__(self):
        return 0


def ids_for(P, binding: str):
    """Two synthetic id bindings; a result must not depend on which one is used."""
    if binding == 'A':
        return P.ProjectionIds(namespace='NSA', instrument_id='INST-A', calendar_id='CAL-A', grid_id='GRID-A',
            revision='rA', day_session_prefix='DAYA-', bar_session_id='BARSA', target_session_id='TGTA',
            names=Renamer('A-', 'INST-A', 'BARSA', 'DAYA-'))
    return P.ProjectionIds(namespace='NSB', instrument_id='zz-inst-b', calendar_id='cal_b', grid_id='grid-b',
        revision='rb2', day_session_prefix='bday.', bar_session_id='bar-session-b', target_session_id='tgt-b',
        names=Renamer('b_', 'zz-inst-b', 'bar-session-b', 'bday.'))


def load_harness(path: Path | None):
    """The contract fixture harness module (default_ctx, set_path); outside this repository. Returns (module, sha256)."""
    path = path or (Path(os.environ['EF4I_HARNESS']) if os.environ.get('EF4I_HARNESS') else None)
    if path is None or not path.is_file():
        raise SystemExit('Give the contract fixture harness with --harness or EF4I_HARNESS')
    spec = importlib.util.spec_from_file_location('ef4i_harness', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, hashlib.sha256(path.read_bytes()).hexdigest()


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef4' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def fixtures_by_id(document: dict) -> dict:
    rows: dict = {}
    for key in FIXTURE_KEYS:
        rows.update({row['id']: row for row in document[key]})
    return rows


def runs_for(fixture: dict) -> list[dict]:
    """The joint run of every listed output; plus one run per output where the configuration has no per-output parameter."""
    expected = fixture['expected']
    runs = [dict(features=tuple(sorted(expected)), expected=expected)]
    if len(expected) > 1 and fixture['producer'] in ('session_projection', 'bar_projection'):
        runs += [dict(features=(feature,), expected={feature: expected[feature]}) for feature in sorted(expected)]
    return runs


# ---- request, grid and work item ------------------------------------------------------------------------------------

def core_grid(request):
    """The library grid object built from the adapter codec (None for the fact producers); the digest must be equal."""
    from equity_features import ef3, ef4
    g = request.grid
    if request.producer == 'session_projection':
        grid = ef3.DailyGrid(g.instrument_id, g.calendar_id, g.grid_id, g.revision, g.sessions, g.target_index)
    elif request.producer == 'bar_projection':
        grid = ef4.BarGrid(g.instrument_id, g.calendar_id, g.grid_id, g.revision, g.session, g.bar_ns, g.bar_count,
                           g.target_index)
    else:
        return None
    if grid.digest != g.digest:
        raise AssertionError('Adapter grid codec digest differs from the library grid digest')
    return grid


def declaration(role: str, batch) -> InputManifest:
    raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
        InputBinding(role, batch.kind, batch.metadata))


def make_work(request, grid, features, label: str, scope: str):
    """Worker work item for one run; the adapter's batches are bound to their contract roles by manifest."""
    from equity_feature_workers import equity_projections as W
    present = tuple((batch, role) for batch, role in zip(request.batches, ROLES[request.producer]) if batch is not None)
    common = dict(features=tuple(features), inputs=tuple(declaration(role, batch) for batch, role in present),
        batches=tuple(batch for batch, _ in present), job_id='job-' + label, generation_id='generation-' + label,
        destination_scope=scope)
    if request.producer in ('session_projection', 'bar_projection'):
        maker = W.make_session_projection_work if request.producer == 'session_projection' else W.make_bar_projection_work
        return maker(request.config, grid=grid, instrument_id=grid.instrument_id,
            partition_id=grid.instrument_id + '-' + request.config.session.session_id, **common)
    maker = W.make_relative_excess_work if request.producer == 'relative_excess' else W.make_rate_projection_work
    return maker(request.config, instrument_id=request.entity.instrument_id,
        partition_id=request.entity.instrument_id + '-' + request.entity.session_id, **common)


def runner_of(producer: str):
    from equity_feature_workers import ef1_runner as R
    return dict(session_projection=R.run_ef4_session_projection, bar_projection=R.run_ef4_bar_projection,
        relative_excess=R.run_ef4_relative_excess, rate_projection=R.run_ef4_rate_projection)[producer]


# ---- comparison with the contract literals --------------------------------------------------------------------------

def accept(reference: dict, value) -> bool:
    """Value acceptance by the contract's rule: bool, exact integer or coefficient, bit-exact float64, +0.0, tolerance."""
    rule = reference['acceptance']
    if rule == 'exact_bool':
        return value is reference['exact']
    if rule == 'exact_integer':
        return type(value) is int and value == int(reference['exact'])
    if type(value) is not float:
        return False
    if rule in ('exact_float64', 'exact_plus_zero'):
        return value.hex() == float.hex(reference['float64']) and math.copysign(1.0, value) == math.copysign(1.0, reference['float64'])
    r = reference['float64']
    return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))


def bound_witness(witness: str, ids) -> str:
    """A contract witness name with its instrument component bound to the synthetic id (slot, bar and tape names have none)."""
    parts = witness.split(':')
    if parts[0] in ('leg', 'membership') and len(parts) > 1:
        parts[1] = ids.bind(parts[1])
    return ':'.join(parts)


def compare(result, fixture: dict, run: dict, ids) -> list:
    """Values (acceptance rule), status, reasons, counts and witnesses against the contract literal for this run."""
    expected = run['expected']
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(expected)):
        problems.append(['selected outputs', [c.feature_id for c in result.values], sorted(expected)])
    found = {feature: [] for feature in expected}
    for row in result.evidence:
        found[row.feature_id].append(row.row_id)
    # Witnesses are compared unless the result-wide evidence cap truncated a joint selection.
    untruncated = len(run['features']) == 1 or fixture['params']['evidence_limit'] >= sum(len(w.get('witnesses', [])) for w in expected.values())
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = expected[column.feature_id]
        stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['status'], want['reasons'], want['expected'], want['observed'])
        if not ((value is None and want['status'] != 'available') or (want['status'] == 'available' and accept(want['value'], value))) or stored != literal:
            problems.append([column.feature_id, 'value/quality', value, stored, literal])
        if untruncated and found[column.feature_id] != [bound_witness(w, ids) for w in want.get('witnesses', [])]:
            problems.append([column.feature_id, 'witnesses', found[column.feature_id], want.get('witnesses', [])])
    return problems


def ordered(items, outcome) -> list:
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


# ---- refusal fixtures through the worker path -----------------------------------------------------------------------

def _library_code(request, grid):
    """The library's own refusal code for the same batches and configuration (the worker does not return it)."""
    from equity_features import ef4
    try:
        ef4.compute_ef4_path_projections(request.batches, request.config, features=request.features, grid=grid,
                                         entity=request.entity)
        return None
    except ContractError as error:
        return error.code.value


def _run_refusal(H, P, ids, fixtures: dict, entry: dict) -> dict:
    """Build (adapter), admit (worker), calculate (worker) and record the library's own code beside the worker's."""
    from equity_features import ef4
    from equity_feature_workers.commands import CommandError
    record = {'id': entry['id'], 'producer': entry['producer'], 'contract_code': entry['error_code'],
        'raised_by': entry['raised_by'], 'contract_stage': entry['stage'], 'stage': None, 'adapter_layer': None,
        'library_code': None, 'worker_code': None}
    base = fixtures[entry['base']]
    obj = {'fixture': copy.deepcopy(base), 'ctx': H.default_ctx(base)}
    for mutation in entry['mutation']:
        if 'set' in mutation:
            H.set_path(obj, mutation['set'], mutation['value'])
        else:
            H.set_path(obj, mutation['del'], delete=True)
    fixture, ctx = obj['fixture'], obj['ctx']
    try:
        request = P.build_request(entry['producer'], fixture, ctx, ids, default_known_at=FIXTURE_DEFAULT_KA)
    except P.ProjectionStageError as error:
        record.update(stage='adapter_build', adapter_layer=error.stage, library_code=(error.code.value if error.code else None))
        return _finish(record)
    if request.capability != 'batch':  # update, restore or merge: a library capability call; the worker has no such path
        record.update(stage='library_capability')
        for feature in request.features:
            try:
                ef4.require_ef4_path_projections_capability(feature, request.capability)
            except ContractError as error:
                record['library_code'] = error.code.value
                break
        return _finish(record)
    try:
        grid = core_grid(request)
    except ContractError as error:
        record.update(stage='grid_build', library_code=error.code.value)
        return _finish(record)
    library = _library_code(request, grid)
    try:
        item = make_work(request, grid, request.features, 'refusal-' + entry['id'], 'ef4-refusal')
    except CommandError as error:
        record.update(stage='worker_admission', worker_code=error.code.value, worker_contract_code=error.contract_code, library_code=library)
        return _finish(record)
    try:
        item.calculate(item.task, item.batches)
        record.update(stage='no_refusal')
    except CommandError as error:
        record.update(stage='compute', worker_code=error.code.value, worker_contract_code=error.contract_code, library_code=library)
    return _finish(record)


def _finish(record: dict) -> dict:
    """Matched: the contract code equals the code of the layer that refused (adapter, library capability or library).

    The worker code is checked for a compute refusal (the worker collapses library codes into its own taxonomy); an
    admission refusal is recorded with whichever worker code the admission raised.
    """
    record['library_code'] = (record['library_code'] or '').upper() or None
    library = record['library_code'] or ''
    record['matched'] = record['contract_code'] == library
    if record['stage'] == 'compute':
        record['worker_code_ok'] = record['worker_code'] == WORKER_OF.get(library, 'INVALID_COMMAND')
    return record


def run_refusals(H, document: dict, only: set[str] | None, binding: str) -> list:
    """The refusal fixtures in contract order; each row names the stage, the contract code and the codes raised."""
    from equity_feature_duckdb import ef4_projections as P
    fixtures = fixtures_by_id(document)
    ids = ids_for(P, binding)
    return [_run_refusal(H, P, ids, fixtures, entry) for entry in document['refusal_fixtures']
            if only is None or entry['id'] in only]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--case', help='contract result fixture id, e.g. SP01, BP12, RX28, RP31')
    parser.add_argument('--refusals', action='store_true', help='drive the 114 refusal fixtures through the worker')
    parser.add_argument('--only', default='', help='with --refusals: comma-separated refusal ids to run (default: all)')
    parser.add_argument('--binding', choices=('A', 'B'), default='A', help='synthetic id binding for the compact names')
    parser.add_argument('--output-db', type=Path)
    parser.add_argument('--html', type=Path)
    parser.add_argument('--run-id', default='ef4-projections')
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    parser.add_argument('--harness', type=Path, default=None, help='contract fixture harness (or EF4I_HARNESS)')
    args = parser.parse_args(argv)
    document = contract_document(args.contract)
    H, harness_sha256 = load_harness(args.harness)
    if args.refusals:
        only = {part for part in args.only.split(',') if part} or None
        rows = run_refusals(H, document, only, args.binding)
        print(json.dumps(rows, indent=1, sort_keys=True))
        print(json.dumps({'harness_sha256': harness_sha256, 'refusals': len(rows)}, sort_keys=True))
        return 0 if all(row['matched'] and row['stage'] != 'no_refusal' for row in rows) else 1
    if not args.case or not args.output_db or not args.html:
        parser.error('--case, --output-db and --html are required for a result case')
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()):
        parser.error('Require new distinct absolute output paths, existing parents and bounded ASCII run identity')
    from equity_feature_duckdb import ef4_projections as P
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb_sink import DuckDBSink

    fixtures = fixtures_by_id(document)
    if args.case not in fixtures or len(fixtures) != RESULT_COUNT or len(document['refusal_fixtures']) != REFUSAL_COUNT:
        parser.error('Contract result fixtures differ from the example case list')
    fixture = fixtures[args.case]
    producer = fixture['producer']
    ids = ids_for(P, args.binding)
    request = P.build_request(producer, fixture, H.default_ctx(fixture), ids, default_known_at=FIXTURE_DEFAULT_KA)
    grid = core_grid(request)
    runner = runner_of(producer)
    scope = 'ef4-' + producer.replace('_', '-') + '-synthetic'
    runs = runs_for(fixture)
    items = [make_work(request, grid, run['features'], '%s-%s-%d' % (args.run_id, args.case, number), scope)
             for number, run in enumerate(runs)]
    limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576, max_total_bytes=10485760,
        max_result_cells=100000, max_evidence_rows=100000)  # the sink's own ceilings
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = runner(tuple(items), sink, requirements=limits)
        if len(outcome.readbacks) != len(items) or not all(row.verified for row in outcome.readbacks):
            print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]}, sort_keys=True))
            return 1
        rows = ordered(items, outcome)
        stored = [row.results[0] for row in rows]
        problems = [found for run, result in zip(runs, stored) for found in [compare(result, fixture, run, ids)] if found]
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
    html = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF4 ' + escape(producer) + '</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>EF4 ' + escape(producer.replace('_', ' ')) + ' (contract fixture ' + escape(args.case) + ')</h1>'
        '<p>' + escape(fixture['description']) + '. Supplied-input adapter  ->  worker  ->  pure library  ->  separate DuckDB sink. '
        'Contract-literal match (value by acceptance rule, status, reasons, counts and witnesses), '
        'SDK readback, idempotent replay and close/reopen verified. Runs: ' + str(len(runs)) + '.</p>'
        '<p>Selected outputs across runs: ' + escape(outputs) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(rows[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every return, excess, fraction, drawdown, SMA comparison, dollar volume and rate is the library result. '
        'No remote assets or JavaScript.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(runs), 'binding': args.binding, 'harness_sha256': harness_sha256,
        'task_sha256': items[0].task.task_sha256, 'content_sha256': rows[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
