"""Synthetic contract fixtures -> EF4 quote and VWAP workers -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture id: Q01-Q30 (quote dynamics) and V01-V40 without V27 (VWAP path), 69 in total
including the prefix fixtures Q20, V03, V12, V13 and V19. Each fixture is run once per selected output (one-output
selection) and once jointly when it selects several outputs; every run goes through the supplied-input adapter
(equity_feature_duckdb.quote_vwap), the worker, the pure library, a separate DuckDB sink, SDK readback, replay and
close/reopen.

``--refusals`` drives the 80 typed refusal fixtures (RQ01-RQ44 and RV01-RV44 as listed in the contract) through the
worker path and reports, per fixture, the stage at which it was refused (``adapter_build``: the adapter or the shared
batch validators while the batches are built; ``worker_admission``: the work item build; ``compute``: the library
calculation), the contract code, the library's own code for the same inputs and the worker code the caller receives.

The contract's compact fixture notation is expanded and mutated by the contract fixture harness
(``ef4h_fixture_expand.py``: ``build_model`` and ``apply_ops``), which lives outside this repository. Give its path with
``--harness`` or the ``EF4H_HARNESS`` environment variable. No entity, instrument, session, role or grid identifier is
fixed here: they come from the fixture (the harness placeholders), the adapter or the configuration.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
from dataclasses import asdict
from html import escape
from pathlib import Path

from equity_feature_contracts import InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

CONTRACT = 'QUOTE_VWAP_CONTRACT_PROPOSAL.json'
RESULT_COUNT = 69
CURRENCY = 'USD'  # synthetic price currency of the example
COV = {'complete': 'complete', 'incomplete': 'incomplete', 'absent': 'absent'}
INT_OUTS = {'quote.change.pair_count', 'quote.change.midpoint_change_count', 'vwap.path.point_count'}
POLICY = {'quote_dynamics': 'ef4-quote1', 'vwap_path': 'ef4-vwap1'}
PRODUCER_KEYS = {
    'quote_dynamics': ('policy_version', 'sampling', 'evidence_limit'),
    'vwap_path': ('policy_version', 'series_source', 'evidence_limit', 'grid_version', 'series_policy_id'),
}
NON_PARAM_KEYS = {'outputs', 'capability', 'price_scale', 'basis'}
ROLES = {'quote_dynamics': ('bounds', 'quotes'), 'vwap_path': ('bounds', 'grid', 'trades', 'bars', 'proof')}
# Worker code expected for a library code (the worker collapses library codes into its own taxonomy).
WORKER_OF = {'INCONSISTENT_IDENTITY': 'RESULT_IDENTITY_MISMATCH', 'OVERFLOW': 'CALCULATION_FAILED'}


def load_harness(path: Path | None):
    """The contract fixture harness module (build_model, apply_ops); outside this repository."""
    path = path or (Path(os.environ['EF4H_HARNESS']) if os.environ.get('EF4H_HARNESS') else None)
    if path is None or not path.is_file():
        raise SystemExit('Give the contract fixture harness with --harness or EF4H_HARNESS')
    spec = importlib.util.spec_from_file_location('ef4h_harness', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef4' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def fixtures_by_id(document: dict) -> dict:
    rows: dict = {}
    for key in ('quote_fixtures', 'vwap_fixtures'):
        rows.update({row['id']: row for row in document[key]})
    return rows


def runs_for(fixture: dict) -> list[dict]:
    """One run per selected output, plus one joint run when the fixture selects several outputs."""
    expected = fixture['expected']
    runs = [dict(features=(feature,), expected={feature: expected[feature]}) for feature in sorted(expected)]
    if len(expected) > 1:
        runs.append(dict(features=tuple(sorted(expected)), expected=expected))
    return runs


# ---- supplied-input construction from the harness model (the adapter does all batch and config building) ------------

def model_of(H, fixture: dict) -> dict:
    """Harness model of a fixture; a null role (not delivered) is built from an empty stand-in and then undelivered."""
    tmp = json.loads(json.dumps(fixture))
    nulls = []
    for key, stand_in in (('quotes', {'coverage': 'absent', 'rows': []}), ('trades', {'coverage': 'absent', 'rows': []}),
            ('bars', {'slots': []})):
        if key in tmp and tmp[key] is None:
            tmp[key] = stand_in
            nulls.append(key)
    model = H.build_model(tmp)
    for key in nulls:
        model[key] = None
    if fixture.get('proof') is not None and not fixture['proof'].get('present', True):
        model['proof'] = []
    return model


def identity_of(model: dict):
    """Target entity and session of a base model: the instrument and session of its bounds fact (fixture data)."""
    from equity_feature_contracts import EntityKey
    fact = model['bounds'][0]
    return EntityKey(fact['instrument'], fact['session'])


def convert(qv, model: dict, base: dict, entity, session):
    """Model (harness form, possibly mutated) -> adapter request. Raises ContractError at the I/O boundary."""
    from equity_feature_contracts import CanonicalBatch, DataKind as Kind, Parameter, PriceUnit
    producer, config = model['producer'], model['config']

    def wire(symbol):
        assert symbol.startswith('x1:B(') and symbol.endswith(')'), symbol
        return qv.reference(symbol[5:-1])  # the contract notation x1:B(text) is the closed wire grammar

    def fact(d):
        return qv.Fact(d['instrument'], d['session'], wire(d['reference_id']) if d['reference_id'].startswith('x1:B(')
                       else d['reference_id'], d['kind'], d['start'], d['end'], d['text'], d['known_at'])

    def unit_of(scale, flag):
        return PriceUnit(scale, CURRENCY) if flag else None

    bounds_facts = model['bounds']
    scope = qv.Scope(bounds_facts[0]['start'], bounds_facts[0]['end'], 'interval:i1') if bounds_facts else qv.Scope(
        base['interval']['start_ns'], base['interval']['end_ns'], 'interval:i1')
    bounds = qv.reference_batch([fact(d) for d in bounds_facts], scope=scope, declared=qv.COMPLETE, input_id='bounds')
    timing = qv.availability(model['C'], base['knowledge_cutoff_ns'])
    names = PRODUCER_KEYS[producer]
    params = [Parameter(k, config[k]) for k in names if k in config]
    params += [Parameter(k, config[k]) for k in config if k not in names and k not in NON_PARAM_KEYS]  # unknown, supplied
    params = tuple(params)
    config_unit = PriceUnit(config['price_scale'], CURRENCY)
    if producer == 'quote_dynamics':
        q = model['quotes']
        quotes = None
        if q is not None:
            declared = COV[base['quotes']['coverage']] if base.get('quotes') else qv.ABSENT
            rows = [qv.QuoteRow(r['instrument'], r['session'], r['event_id'], r['event_ns'], r['order_key'], r['bid'],
                                r['ask'], r['bid_size'], r['ask_size'], r['known_at']) for r in q['rows']]
            quotes = qv.quote_batch(rows, scope=scope, sampling=q['sampling'], unit=unit_of(q['scale'], q['unit']),
                                    declared=declared, basis=qv.adjustment(q['basis']), input_id='quotes')
            if q.get('kind', 'quote') != 'quote':  # the adapter fixes the role kind; a supplied other kind meets the constructor
                quotes = CanonicalBatch(Kind(q['kind']), quotes.columns, quotes.metadata)
        return qv.quote_request(entity=entity, session=session, timing=timing, config_unit=config_unit, params=params,
                                bounds=bounds, quotes=quotes)
    grid = qv.reference_batch([fact(d) for d in model['grid']], scope=scope, declared=qv.COMPLETE,
                              input_id='grid') if model['grid'] else None
    trades = None
    if model['trades'] is not None:
        t = model['trades']
        rows = [qv.TradeRow(r['instrument'], r['session'], r['event_id'], r['event_ns'], r['order_key'], r['eligible'],
                            r['price'], r['size'], r['known_at']) for r in t['rows']]
        trades = qv.trade_batch(rows, scope=scope, unit=unit_of(t['scale'], t['unit']),
            declared=(COV[base['trades']['coverage']] if base.get('trades') else (qv.COMPLETE if rows else qv.ABSENT)),
            basis=qv.adjustment(t['basis']), input_id='trades')
    bars = None
    if model['bars'] is not None:
        bb = model['bars']
        edge_times = [d['start'] for d in model['grid']] if model['grid'] else []
        rows = [qv.BarRow(r['instrument'], r['session'], r['start'], r['end'], r['volume'], r['notional'], r['known_at'],
                          ohlc=r.get('ohlc')) for r in bb['rows']]
        starts = {r['start'] for r in bb['rows']}
        gaps = []
        for k, slot in enumerate(base['bars']['slots'] if base.get('bars') else []):
            if slot['state'] == 'gap' and k + 1 < len(edge_times) and edge_times[k] not in starts:
                gaps.append(qv.Gap(edge_times[k], edge_times[k + 1]))
        bars = qv.bar_batch(rows, gaps, scope=scope, unit=unit_of(bb['scale'], bb['unit']),
                            basis=qv.adjustment(bb['basis']), input_id='bars')
    proof = qv.reference_batch([fact(d) for d in model['proof']], scope=scope, declared=qv.COMPLETE,
                               input_id='proof') if model['proof'] else None
    return qv.vwap_request(entity=entity, session=session, timing=timing, config_unit=config_unit, params=params,
                           bounds=bounds, grid=grid, trades=trades, bars=bars, proof=proof)


def declaration(role: str, batch) -> InputManifest:
    raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
        InputBinding(role, batch.kind, batch.metadata))


def make_work(request, features, label: str, scope: str):
    """Worker work item for one run; the adapter's batches are bound to their contract roles by manifest."""
    from equity_feature_workers.equity_quote_vwap import make_quote_dynamics_work, make_vwap_path_work
    maker = {'quote_dynamics': make_quote_dynamics_work, 'vwap_path': make_vwap_path_work}[request.producer]
    present = tuple((batch, role) for batch, role in zip(request.slots, ROLES[request.producer]) if batch is not None)
    decls = tuple(declaration(role, batch) for batch, role in present)
    return maker(request.config, features=features, inputs=decls, batches=tuple(batch for batch, _ in present),
        instrument_id=request.entity.instrument_id, job_id='job-' + label, generation_id='generation-' + label,
        partition_id=request.entity.instrument_id + '-' + request.entity.session_id, destination_scope=scope)


def accept(reference: dict, value) -> bool:
    """Value acceptance by the contract's rule: bit-exact float64, +0.0 where stated, int64 exact, or the tolerance."""
    want = reference['value']
    if want is None or value is None:
        return want is None and value is None
    if 'int64' in want:
        return type(value) is int and value == want['int64']
    r = float.fromhex(want['hex'])
    if want['acceptance'] in ('exact_float64', 'exact_plus_zero'):
        return value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r)
    return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))


def compare(result, fixture: dict, run: dict) -> list:
    """Values (acceptance rule), status, reasons, counts and witnesses against the contract literal for this run."""
    expected = run['expected']
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(expected)):
        problems.append(['selected outputs', [c.feature_id for c in result.values], sorted(expected)])
    found = {feature: [] for feature in expected}
    for row in result.evidence:
        found[row.feature_id].append(row.row_id)
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = expected[column.feature_id]
        stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['status'], want['reasons'], want['expected'], want['observed'])
        if not (accept(want, value) and stored == literal):
            problems.append([column.feature_id, 'value/quality', value, stored, literal])
        if len(run['features']) == 1 and found[column.feature_id] != want.get('witnesses', []):
            problems.append([column.feature_id, 'witnesses', found[column.feature_id], want.get('witnesses', [])])
    return problems


def ordered(items, outcome) -> list:
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


# ---- refusal fixtures through the worker path -----------------------------------------------------------------------

def _run_refusal(H, qv, document: dict, fixtures: dict, entry: dict) -> dict:
    """Build (adapter), admit (worker), calculate (worker) and record the library's own code beside the worker's."""
    from equity_feature_contracts import ContractError, SessionSpec
    from equity_feature_workers.commands import CommandError
    from equity_features import ef4
    record = {'id': entry['id'], 'rule': entry['rule'], 'contract_code': entry['error_code'],
        'raised_by': entry['raised_by'], 'stage': None, 'library_code': None, 'worker_code': None}
    base = fixtures[entry['base']]
    base_model = model_of(H, base)
    entity = identity_of(base_model)
    session = SessionSpec(qv.NAMESPACE, entity.session_id, 0, 10 ** 12, 'UTC')
    model = H.apply_ops(model_of(H, base), entry['mutations'])
    features = tuple(model['config'].get('outputs', base['params']['outputs']))
    try:
        request = convert(qv, model, base, entity, session)
    except ContractError as error:
        record.update(stage='adapter_build', library_code=error.code.value)
        return _finish(record)
    capability = model['config'].get('capability', 'batch')
    if capability != 'batch':  # update, restore or merge: a library capability call; the worker exposes no such path
        try:
            ef4.require_quote_vwap_capability(features[0], capability)
            record.update(stage='library_capability', library_code=None)
        except ContractError as error:
            record.update(stage='library_capability', library_code=error.code.value)
        return _finish(record)
    library = _library_code(ef4, request, features)
    try:
        item = make_work(request, features, 'refusal-' + entry['id'], 'ef4-refusal')
    except CommandError as error:
        record.update(stage='worker_admission', worker_code=error.code.value, library_code=library)
        return _finish(record)
    try:
        item.calculate(item.task, item.batches)
        record.update(stage='no_refusal')
    except CommandError as error:
        record.update(stage='compute', worker_code=error.code.value, library_code=library)
    return _finish(record)


def _library_code(ef4, request, features):
    """The library's own refusal code for the same batches and configuration (the worker does not return it)."""
    from equity_feature_contracts import ContractError
    try:
        ef4.compute_quote_vwap(request.slots, request.config, entity=request.entity, features=features)
        return None
    except ContractError as error:
        return error.code.value


def _finish(record: dict) -> dict:
    """Matched: the contract code equals the code of the layer that refused (adapter, library capability or library).

    The worker code is checked for a compute refusal (the worker collapses library codes into its own taxonomy); an
    admission refusal is recorded with whichever worker code the admission raised.
    """
    library = (record['library_code'] or '').upper()
    record['matched'] = record['contract_code'] == library
    if record['stage'] == 'compute':
        record['worker_code_ok'] = record['worker_code'] == WORKER_OF.get(library, 'INVALID_COMMAND')
    return record


def run_refusals(H, document: dict, only: set[str] | None) -> list:
    """The refusal fixtures in contract order; each row names the stage, the contract code and the codes raised."""
    from equity_feature_duckdb import quote_vwap as qv
    fixtures = fixtures_by_id(document)
    return [_run_refusal(H, qv, document, fixtures, entry) for entry in document['refusal_fixtures']
            if only is None or entry['id'] in only]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--case', help='contract result fixture id, e.g. Q01, V12, V39')
    parser.add_argument('--refusals', action='store_true', help='drive the 80 refusal fixtures through the worker')
    parser.add_argument('--only', default='', help='with --refusals: comma-separated refusal ids to run (default: all)')
    parser.add_argument('--output-db', type=Path)
    parser.add_argument('--html', type=Path)
    parser.add_argument('--run-id', default='ef4-quote-vwap')
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    parser.add_argument('--harness', type=Path, default=None, help='contract fixture harness (or EF4H_HARNESS)')
    args = parser.parse_args(argv)
    document = contract_document(args.contract)
    H = load_harness(args.harness)
    if args.refusals:
        only = {part for part in args.only.split(',') if part} or None
        rows = run_refusals(H, document, only)
        print(json.dumps(rows, indent=1, sort_keys=True))
        return 0 if all(row['matched'] and row['stage'] != 'no_refusal' for row in rows) else 1
    if not args.case or not args.output_db or not args.html:
        parser.error('--case, --output-db and --html are required for a result case')
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()):
        parser.error('Require new distinct absolute output paths, existing parents and bounded ASCII run identity')
    from equity_feature_contracts import SessionSpec
    from equity_feature_duckdb import quote_vwap as qv
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.ef1_runner import run_ef4_quote_dynamics, run_ef4_vwap_path

    fixtures = fixtures_by_id(document)
    if args.case not in fixtures or len(fixtures) != RESULT_COUNT:
        parser.error('Contract result fixtures differ from the example case list')
    fixture = fixtures[args.case]
    model = model_of(H, fixture)
    producer = model['producer']
    entity = identity_of(model)
    session = SessionSpec(qv.NAMESPACE, entity.session_id, 0, 10 ** 12, 'UTC')
    runner = {'quote_dynamics': run_ef4_quote_dynamics, 'vwap_path': run_ef4_vwap_path}[producer]
    scope = 'ef4-' + producer.replace('_', '-') + '-synthetic'
    request = convert(qv, model, fixture, entity, session)
    runs = runs_for(fixture)
    items = [make_work(request, run['features'], '%s-%s-%d' % (args.run_id, args.case, number), scope)
             for number, run in enumerate(runs)]
    limit = fixture['params']['evidence_limit']
    limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576, max_total_bytes=1048576 * len(items),
        max_result_cells=16 * len(items), max_evidence_rows=max(1, min(limit, 40960)) * len(items))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, scope, limits=limits) as sink:
        outcome = runner(tuple(items), sink, requirements=limits)
        if len(outcome.readbacks) != len(items) or not all(row.verified for row in outcome.readbacks):
            print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]}, sort_keys=True))
            return 1
        rows = ordered(items, outcome)
        stored = [row.results[0] for row in rows]
        problems = [found for run, result in zip(runs, stored) for found in [compare(result, fixture, run)] if found]
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
        'Contract-literal match (value by acceptance rule, status, reasons, counts, witnesses and joint evidence), '
        'SDK readback, idempotent replay and close/reopen verified. Runs: ' + str(len(runs)) + '.</p>'
        '<p>Selected outputs across runs: ' + escape(outputs) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(rows[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every spread, midpoint, microprice, VWAP, slope, extremum and count is the library result. '
        'No remote assets or JavaScript.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(runs), 'task_sha256': items[0].task.task_sha256,
        'content_sha256': rows[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
