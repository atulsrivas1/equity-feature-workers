"""Synthetic contract fixtures -> EF4 tape workers -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture id: W01-W22b (tape window, 50), TK01-TK19 (notional top-K, 25) and PM01-PM06
(partition merge, 7), 82 in total, including the prefix pairs (TK01/TK10c, PM01/PM03, W07b/W07d). A window fixture is run
once per selected output (one-output selection) and once jointly; a top-K fixture is run once; a merge fixture runs every
partition through the top-K worker (checked against the partition's literal partial) and then the merge through the merge
worker (checked against the merged literal; the nested PM06 also runs its inner merge). Every run goes through the
supplied-input adapter (equity_feature_duckdb.tape), the worker, the pure library, a separate DuckDB sink, SDK readback,
replay and close/reopen. ``--case G01|G02|G03`` runs a generated differential fixture (G01 and G02 window totals for every
window, G03 single pass and the index-mod-4 merge for every K) through the same path.

``--refusals`` drives the 75 typed refusal fixtures (RF01-RF75 as listed in the contract) through the worker path and
reports, per fixture, the stage at which it was refused (``adapter_build``: the adapter or the shared batch validators while
the batches are built; ``worker_admission``: the work item build; ``compute``: the library calculation;
``library_capability`` / ``library_merge_order`` / ``no_worker_path``: refusals the worker has no path for), the contract
code, the library's own code for the same inputs and the worker code the caller receives.

The contract's compact fixture notation is expanded and mutated by the contract fixture harness (``ef4g_fixture_expand.py``:
``expand_trades``, ``expand_seed``, ``absolutize``, ``apply_ops``, ``BINDINGS``, ``lcg_tape``), which lives outside this
repository (vendored in core at ``tools/contract_fixtures/ef4g_fixture_expand.py``). Give its path with ``--harness`` or the ``EF4G_HARNESS``
environment variable. No entity, instrument, session, role, seed or grid identifier is fixed here: they come from the harness
binding (``--binding``), the fixture, the adapter or the configuration.
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
import sys
from dataclasses import asdict, replace
from fractions import Fraction
from html import escape
from pathlib import Path

from equity_feature_contracts import Coverage, DataKind, InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

CONTRACT = 'TAPE_CONTRACT_PROPOSAL.json'
RESULT_COUNT = 82
TOP_K = 'tape.top_k_notional'
POLICY = {'window': 'ef4-tape1', 'topk': 'ef4-notional1'}
WINDOW_KEYS = ('policy_version', 'window_ns', 'seed_policy', 'evidence_limit')
TOPK_KEYS = ('policy_version', 'top_k', 'evidence_limit')
# Worker code expected for a library code (the worker collapses library codes into its own taxonomy).
WORKER_OF = {'INCONSISTENT_IDENTITY': 'RESULT_IDENTITY_MISMATCH', 'OVERFLOW': 'CALCULATION_FAILED'}
# Contract codes the adapter cannot reproduce (io EF4_TAPE_INPUTS.md): a shared validator refuses first with another code.
ADAPTER_CODE_DEVIATIONS = {'RF18': 'UNSUPPORTED_SAMPLING', 'RF35': 'DUPLICATE'}
SEED_NOTATION = re.compile(r'^x1:B\((.*)\)$')


def load_harness(path: Path | None):
    """The contract fixture harness module; vendored in core."""
    path = path or (Path(os.environ['EF4G_HARNESS']) if os.environ.get('EF4G_HARNESS') else None)
    if path is None or not path.is_file():
        raise SystemExit('Give the contract fixture harness with --harness or EF4G_HARNESS')
    spec = importlib.util.spec_from_file_location('ef4g_harness', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def harness_sha256(path: Path | None) -> str:
    path = path or Path(os.environ['EF4G_HARNESS'])
    return hashlib.sha256(path.read_bytes()).hexdigest()


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef4' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def fixtures_by_id(document: dict) -> dict:
    rows: dict = {}
    for key, kind in (('window_fixtures', 'window'), ('topk_fixtures', 'topk'), ('merge_fixtures', 'merge')):
        rows.update({row['id']: dict(row, _kind=kind) for row in document[key]})
    return rows


# ---- supplied-input construction from the harness expansion (the adapter does all batch and config building) ---------

def trade_supply(T, fx: dict, rows, *, input_id: str = 'trades', scale=None, adjustment='raw', policy=None, scope_end=None,
        include_opening: bool = False, include_closing: bool = False):
    """The supplied TRADE batch of a fixture (or of one partition) with its supplied metadata; rows keep their order."""
    columns = fx.get('columns', ['price', 'size', 'known_at_ns'])
    delivered = tuple(c for c in T.TRADE_PAYLOAD if c in columns or (c == 'condition' and any('condition' in r for r in rows)))
    coverage = fx.get('coverage')
    cover = None
    if coverage is not None or 'coverage_observed' in fx:
        cover = Coverage(coverage['expected'] if coverage else len(rows), fx.get('coverage_observed', len(rows)),
                         coverage['complete'] if coverage else True)
    return T.TradeSupply(rows=tuple(rows), delivered=delivered, coverage=cover,
        scale=fx.get('batch_price_scale', scale), adjustment=fx.get('batch_adjustment', adjustment),
        kind=DataKind(fx.get('batch_kind', 'trade')), scope_policy=fx.get('scope_policy', policy),
        scope_start_ns=fx.get('scope_start'), scope_end_ns=fx.get('scope_end', scope_end),
        scope_include_opening_auction=fx.get('scope_include_opening_auction', include_opening),
        scope_include_closing_auction=fx.get('scope_include_closing_auction', include_closing), input_id=input_id)


def seed_supply(T, H, fx: dict, b: dict):
    """The supplied `ef4.tick.seed` facts of a fixture (None when no seed key); the id is the harness notation's text."""
    seed = H.expand_seed(fx, b)
    if seed is None:
        return None
    match = SEED_NOTATION.match(seed['reference_id'])
    if match is None:
        raise SystemExit('Unexpected seed notation ' + seed['reference_id'])
    fact = {k: seed[k] for k in ('fact_kind', 'instrument_id', 'session_id', 'effective_start_ns', 'effective_end_ns',
        'text', 'price', 'known_at_ns')}
    fact.update(factor_num=None, factor_den=None)
    return T.SeedSupply(facts=tuple(dict(fact) for _ in range(seed['count'])), seed_set_id=match.group(1),
        scale=seed['scale'], adjustment=seed['adjustment'])


def without_parameters(config, names):
    """The adapter's envelope without parameters the fixture deleted (the adapter passes parameters as supplied)."""
    return replace(config, parameters=tuple(p for p in config.parameters if p.name not in names)) if names else config


def window_request(H, T, fx: dict, b: dict):
    """(case, config): the adapter request of a window fixture (possibly mutated by a refusal recipe)."""
    fx = H.absolutize(fx)
    p = fx['params']
    rows = H.expand_trades(fx, b)
    policy = fx.get('config_policy', 'P')
    inc_open, inc_close = fx.get('include_opening_auction', False), fx.get('include_closing_auction', False)
    trades = None if rows is None else trade_supply(T, fx, rows, policy=policy, include_opening=inc_open,
        include_closing=inc_close)
    case = T.window_case(instrument_id=b['entity'], session_id=b['session'], open_ns=fx['open'], cutoff_ns=fx['C'],
        knowledge_ns=fx['K'], session_close_ns=fx.get('close', fx['C']), price_scale=fx['price_scale'],
        eligibility_policy=policy, window_ns=p.get('window_ns'), seed_policy=p.get('seed_policy'),
        evidence_limit=p.get('evidence_limit'), policy_version=p.get('policy_version'), trades=trades,
        seed=seed_supply(T, H, fx, b), extra_parameters={k: v for k, v in p.items() if k not in WINDOW_KEYS},
        mode=fx.get('mode', 'known_at'), include_opening_auction=inc_open, include_closing_auction=inc_close)
    return case, without_parameters(case.config(), {k for k in WINDOW_KEYS if k not in p})


def top_k_request(H, T, fx: dict, b: dict, *, rows=None, input_id: str = 'trades', entity_of=None):
    """(case, config): the adapter request of a top-K fixture or of one partition."""
    fx = H.absolutize(fx)
    p = fx['params']
    rows = H.expand_trades(fx, b) if rows is None else rows
    policy = fx.get('config_policy', fx.get('eligibility_policy', 'P'))
    inc_open, inc_close = fx.get('include_opening_auction', False), fx.get('include_closing_auction', False)
    instrument = b['entity'] if entity_of is None else entity_of
    trades = None if rows is None else trade_supply(T, fx, rows, input_id=input_id, policy=policy, scale=fx['price_scale'],
        adjustment=fx.get('adjustment', 'raw'), scope_end=fx['C'], include_opening=inc_open, include_closing=inc_close)
    case = T.top_k_case(instrument_id=instrument, session_id=b['session'], open_ns=fx['open'], cutoff_ns=fx['C'],
        knowledge_ns=fx['K'], session_close_ns=fx.get('close', fx['C']), price_scale=fx['price_scale'],
        eligibility_policy=policy, top_k=p.get('top_k'), evidence_limit=p.get('evidence_limit'),
        policy_version=p.get('policy_version'), trades=trades,
        extra_parameters={k: v for k, v in p.items() if k not in TOPK_KEYS}, mode=fx.get('mode', 'known_at'),
        include_opening_auction=inc_open, include_closing_auction=inc_close)
    return case, without_parameters(case.config(), {k for k in TOPK_KEYS if k not in p})


def partition_requests(H, T, fx: dict, b: dict):
    """One top-K request per merge partition; a partition may carry its own params, scale, cutoffs, entity and policy."""
    fx = H.absolutize(fx)
    out = []
    for part in fx['partitions']:
        merged = dict(fx, params=part['params'], trades=part['trades'])
        merged.pop('partitions', None)
        for key in ('price_scale', 'adjustment', 'C', 'K', 'eligibility_policy'):
            if key in part:
                merged[key] = part[key]
        rows = H.expand_trades(merged, b)
        other = part.get('entity', '$ENTITY') == '$OTHER_ENTITY'
        if other:
            rows = [dict(r, instrument_id=b['other_entity']) for r in rows]
        out.append(top_k_request(H, T, merged, b, rows=rows, input_id=part['label'],
            entity_of=b['other_entity'] if other else None))
    return out


def declaration(role: str, batch) -> InputManifest:
    raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
        InputBinding(role, batch.kind, batch.metadata))


WINDOW_ROLES = ('trades', 'seed')


def make_work(case, config, features, label: str, scope: str):
    """Worker work item for one window or top-K run; the adapter's batches are bound to their library slots by role."""
    from equity_feature_workers.equity_tape import make_tape_top_k_work, make_tape_window_work
    maker = make_tape_window_work if case.producer == 'window' else make_tape_top_k_work
    roles = WINDOW_ROLES if case.producer == 'window' else ('trades',)
    present = tuple((batch, role) for batch, role in zip(case.slots, roles) if batch is not None)
    return maker(config, features=features, inputs=tuple(declaration(role, batch) for batch, role in present),
        batches=tuple(batch for batch, _ in present), instrument_id=case.entity.instrument_id, job_id='job-' + label,
        generation_id='generation-' + label, partition_id=case.entity.instrument_id + '-' + case.entity.session_id,
        destination_scope=scope)


def make_merge_work(cases, config, tree, label: str, scope: str):
    """Worker work item for a merge: `tree` is the nested tuple of case indexes; roles spell the tree (p0.p1.trades)."""
    from equity_feature_workers.equity_tape import make_tape_top_k_merge_work
    placed: list[tuple[str, int]] = []

    def walk(node, prefix: str) -> None:
        for i, child in enumerate(node):
            if isinstance(child, tuple):
                walk(child, prefix + 'p%d.' % i)
            else:
                placed.append((prefix + 'p%d.trades' % i, child))
    walk(tree, '')
    present = [(role, cases[index].slots[0]) for role, index in placed if cases[index].slots[0] is not None]
    return make_tape_top_k_merge_work(config, features=(TOP_K,),
        inputs=tuple(declaration(role, batch) for role, batch in present), batches=tuple(batch for _, batch in present),
        instrument_id=cases[0].entity.instrument_id, job_id='job-' + label, generation_id='generation-' + label,
        partition_id=cases[0].entity.instrument_id + '-' + cases[0].entity.session_id, destination_scope=scope)


# ---- comparison with the contract literals -------------------------------------------------------------------------

def accept(spec: dict, value) -> bool:
    """Value acceptance by the contract's rule: exact integer, bit-exact float64, +0.0 where stated, or the tolerance."""
    if spec['acceptance'] == 'exact_integer':
        return type(value) is int and Fraction(value) == Fraction(spec['exact'])
    if type(value) is not float:
        return False
    r = float.fromhex(spec['hex'])
    if spec['acceptance'] in ('exact_float64', 'exact_plus_zero'):
        return value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r)
    return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))


def witnesses_of(result, feature: str) -> list:
    return [row.row_id for row in result.evidence if row.feature_id == feature]


def boundaries_of(result, feature: str) -> list:
    return [row.boundary for row in result.evidence if row.feature_id == feature]


def compare_window(result, expected: dict, wire_seed: str | None, features: tuple) -> list:
    problems = []
    if tuple(column.feature_id for column in result.values) != tuple(sorted(features)):
        problems.append(['selected outputs', [c.feature_id for c in result.values], sorted(features)])
    for column in result.values:
        quality, = (row for row in result.quality if row.feature_id == column.feature_id)
        value, = column.values
        want = expected[column.feature_id]
        stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
        literal = (want['status'], want['reasons'], want['expected'], want['observed'])
        ok = (value is None) if want['status'] != 'available' else accept(want['value'], value)
        if not (ok and stored == literal):
            problems.append([column.feature_id, 'value/quality', value, stored, literal])
        witnesses = [w.replace('$SEED_REF', wire_seed or '') for w in want.get('witnesses', [])]
        if witnesses_of(result, column.feature_id) != witnesses:
            problems.append([column.feature_id, 'witnesses', witnesses_of(result, column.feature_id), witnesses])
    return problems


def compare_top_k(result, want: dict) -> list:
    """Status, reasons, counts and the notional-ordered rows against a top-K (or merge or partial) literal."""
    problems = []
    quality, = result.quality
    cell, = result.values[0].values
    stored = (quality.status.value, [reason.value for reason in quality.reasons], quality.expected, quality.observed)
    literal = (want['status'], want['reasons'], want['expected'], want['observed'])
    if stored != literal:
        problems.append(['quality', stored, literal])
    rows = want.get('rows') if 'rows' in want else (want.get('value') or {}).get('rows')
    if want['status'] != 'available':
        if cell is not None:
            problems.append(['value on unavailable', repr(cell)])
        return problems
    got = [(r.event_id, r.price, r.size, r.price * r.size) for r in cell.rows]
    literal_rows = [(r['event_id'], r['price'], r['size'], r['notional']) for r in rows]
    if got != literal_rows or (want.get('value') and cell.k != want['value']['k']):
        problems.append(['rows', got, literal_rows])
    names = [w.split(':', 1)[1] for w in want['witnesses']] if 'witnesses' in want else [r[0] for r in literal_rows]
    if witnesses_of(result, TOP_K) != names:
        problems.append(['witnesses', witnesses_of(result, TOP_K), names])
    if 'evidence_boundaries' in want and boundaries_of(result, TOP_K) != want['evidence_boundaries']:
        problems.append(['boundaries', boundaries_of(result, TOP_K), want['evidence_boundaries']])
    return problems


def ordered(items, outcome) -> list:
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


# ---- refusal fixtures through the worker path -----------------------------------------------------------------------

def _library_code(ef4, case, config, features):
    """The library's own refusal code for the same batches and configuration (the worker does not return it)."""
    from equity_feature_contracts import ContractError
    try:
        ef4.compute_ef4_tape(case.slots, config, entity=case.entity, features=features)
        return None
    except ContractError as error:
        return error.code.value


def _library_merge_code(ef4, cases, configs, order=None):
    """Library code of the flat merge of the per-partition partials (each computed with its own configuration)."""
    from equity_feature_contracts import ContractError
    try:
        parts = []
        for number, (case, config) in enumerate(zip(cases, configs)):
            result = ef4.compute_ef4_tape(case.slots, config, entity=case.entity, features=(TOP_K,))
            if order is not None and number == 0:  # a supplied partial whose rows are in the delivered order
                cell, = result.values[0].values
                rows = sorted(cell.rows, key=lambda r: order.index(r.event_id))
                moved = object.__new__(type(cell))
                object.__setattr__(moved, 'k', cell.k)
                object.__setattr__(moved, 'rows', tuple(rows))
                result = replace(result, values=(replace(result.values[0], values=(moved,)),))
            parts.append(ef4.NotionalPartial(config, case.entity, result))
        ef4.merge_trade_top_k_notional(parts)
        return None
    except ContractError as error:
        return error.code.value


def _run_refusal(H, T, fixtures: dict, entry: dict, b: dict) -> dict:
    from equity_feature_contracts import ContractError
    from equity_feature_workers.commands import CommandError
    from equity_features import ef4
    record = {'id': entry['id'], 'rule': entry.get('rule'), 'contract_code': entry['error_code'],
        'raised_by': entry['raised_by'], 'scenario': entry['scenario'], 'stage': None, 'library_code': None,
        'worker_code': None}
    fx = H.apply_ops(copy.deepcopy(fixtures[entry['base']]), entry['mutate'])
    scope = 'ef4-tape-refusal'
    if entry['scenario'] == 'merge':
        return _run_merge_refusal(H, T, ef4, fx, entry, b, record, scope, fixtures)
    selection = tuple(fx.get('selection') or ([TOP_K] if entry['scenario'] == 'topk' else list(fx['expected'])))
    capability = fx.get('capability', 'batch')
    if capability != 'batch':  # update, restore or window merge: a library capability call; the worker has no such path
        try:
            ef4.require_ef4_tape_capability(selection[0], capability)
            record.update(stage='no_worker_path', library_code=None)
        except ContractError as error:
            record.update(stage='no_worker_path', library_code=error.code.value)
        return _finish(record)
    try:
        case, config = (window_request(H, T, fx, b) if entry['scenario'] == 'window' else top_k_request(H, T, fx, b))
    except ContractError as error:
        record.update(stage='adapter_build', library_code=error.code.value)
        return _finish(record)
    library = _library_code(ef4, case, config, selection)
    try:
        item = make_work(case, config, selection, 'refusal-' + entry['id'], scope)
    except CommandError as error:
        record.update(stage='worker_admission', worker_code=error.code.value, library_code=library)
        return _finish(record)
    try:
        item.calculate(item.task, item.batches)
        record.update(stage='no_refusal')
    except CommandError as error:
        record.update(stage='compute', worker_code=error.code.value, library_code=library)
    return _finish(record)


def _run_merge_refusal(H, T, ef4, fx, entry, b, record, scope, fixtures):
    from equity_feature_contracts import ContractError
    from equity_feature_workers.commands import CommandError
    parts = fx['partitions']
    if not parts:  # zero partials: the worker cannot build a merge with no partition
        try:
            ef4.merge_trade_top_k_notional([])
        except ContractError as error:
            record.update(library_code=error.code.value)
        from equity_feature_workers.equity_tape import make_tape_top_k_merge_work
        case, config = partition_requests(H, T, fixtures[entry['base']], b)[0]
        try:
            make_tape_top_k_merge_work(config, features=(TOP_K,), inputs=(), batches=(), instrument_id=case.entity.instrument_id,
                job_id='job-' + entry['id'], generation_id='generation-' + entry['id'], partition_id='partition',
                destination_scope=scope)
            record.update(stage='no_refusal')
        except CommandError as error:
            record.update(stage='worker_admission', worker_code=error.code.value)
        return _finish(record)
    try:
        requests = partition_requests(H, T, fx, b)
    except ContractError as error:
        record.update(stage='adapter_build', library_code=error.code.value)
        return _finish(record)
    cases, configs = [r[0] for r in requests], [r[1] for r in requests]
    order = parts[0].get('partial_delivered_order')
    record['library_code'] = _library_merge_code(ef4, cases, configs, order)
    if order is not None:  # the worker computes its own partials; it has no supplied-partial input
        record['stage'] = 'no_worker_path'
        return _finish(record)
    base = cases[0]
    if any((c.settings['policy_version'], c.settings['top_k'], c.settings['evidence_limit'], c.availability.knowledge_cutoff_ns)
            != (base.settings['policy_version'], base.settings['top_k'], base.settings['evidence_limit'],
                base.availability.knowledge_cutoff_ns) for c in cases[1:]):
        record['stage'] = 'no_worker_path'  # one reviewed configuration per task: a partition cannot carry its own
        return _finish(record)
    try:
        item = make_merge_work(cases, configs[0], tuple(range(len(cases))), 'refusal-' + entry['id'], scope)
    except CommandError as error:
        record.update(stage='worker_admission', worker_code=error.code.value)
        return _finish(record)
    try:
        item.calculate(item.task, item.batches)
        record.update(stage='no_refusal')
    except CommandError as error:
        record.update(stage='compute', worker_code=error.code.value)
    return _finish(record)


def _finish(record: dict) -> dict:
    """Matched: the contract code equals the code of the layer that refused (adapter, library capability or library)."""
    library = (record['library_code'] or '').upper()
    deviation = ADAPTER_CODE_DEVIATIONS.get(record['id'])
    record['matched'] = record['contract_code'] == library
    if deviation is not None and library == deviation and record['stage'] == 'adapter_build':
        record['known_adapter_deviation'] = deviation
    if record['stage'] == 'compute':
        record['worker_code_ok'] = record['worker_code'] == WORKER_OF.get(library, 'INVALID_COMMAND')
    return record


def run_refusals(H, T, document: dict, only: set[str] | None, b: dict) -> list:
    fixtures = fixtures_by_id(document)
    return [_run_refusal(H, T, fixtures, entry, b) for entry in document['refusal_fixtures']
            if only is None or entry['id'] in only]


# ---- result cases -----------------------------------------------------------------------------------------------------

def tree_of(count: int, inner=None):
    """Flat merge tree over `count` partitions, or ((inner...), rest...) when an inner merge is named by index."""
    if inner is None:
        return tuple(range(count))
    rest = tuple(i for i in range(count) if i not in inner)
    return (tuple(inner),) + rest


def build_plan(H, T, document: dict, fixtures: dict, case_id: str, b: dict, scope: str, run_id: str):
    """Work items with a checker each: [(kind, item, checker)] for a result fixture or a generated fixture."""
    plan: list = []
    if case_id in fixtures:
        fx = fixtures[case_id]
        kind = fx['_kind']
        if kind == 'window':
            case, config = window_request(H, T, fx, b)
            seed = seed_supply(T, H, H.absolutize(fx), b)
            wire = T.seed_reference(seed.seed_set_id) if seed is not None else None
            base = tuple(fx.get('selection') or sorted(fx['expected']))
            groups = [(f,) for f in base] + ([base] if len(base) > 1 else [])
            for number, features in enumerate(groups):
                item = make_work(case, config, features, '%s-%s-%d' % (run_id, case_id, number), scope)
                plan.append(('window', item, (lambda r, f=features: compare_window(r, fx['expected'], wire, f))))
        elif kind == 'topk':
            case, config = top_k_request(H, T, fx, b)
            item = make_work(case, config, (TOP_K,), '%s-%s-0' % (run_id, case_id), scope)
            plan.append(('topk', item, (lambda r: compare_top_k(r, fx['expected'][TOP_K]))))
        else:
            requests = partition_requests(H, T, fx, b)
            cases, configs = [r[0] for r in requests], [r[1] for r in requests]
            for number, (part, case, config) in enumerate(zip(fx['partitions'], cases, configs)):
                item = make_work(case, config, (TOP_K,), '%s-%s-p%d' % (run_id, case_id, number), scope)
                plan.append(('topk', item, (lambda r, w=part['expected']: compare_top_k(r, w))))
            nested = fx.get('nested')
            if nested:
                labels = [p['label'] for p in fx['partitions']]
                inner = tuple(labels.index(x) for x in nested['inner'])
                item = make_merge_work([cases[i] for i in inner], configs[inner[0]], tuple(range(len(inner))),
                    '%s-%s-inner' % (run_id, case_id), scope)
                plan.append(('merge', item, (lambda r: compare_top_k(r, nested['inner_expected']))))
                tree = tree_of(len(cases), inner)
            else:
                tree = tree_of(len(cases))
            item = make_merge_work(cases, configs[0], tree, '%s-%s-merge' % (run_id, case_id), scope)
            plan.append(('merge', item, (lambda r: compare_top_k(r, fx['expected_merged']))))
        return plan
    generator = next((g for g in document['generator_fixtures'] if g['id'] == case_id), None)
    if generator is None:
        raise SystemExit('Unknown case ' + case_id)
    rows = H.lcg_tape(generator)
    first = rows[0]
    cutoff = rows[-1]['event_ns'] + 1
    unit = dict(instrument_id=first['instrument_id'], session_id=first['session_id'], open_ns=0, cutoff_ns=cutoff,
        knowledge_ns=cutoff, session_close_ns=cutoff, price_scale=2, eligibility_policy='P')
    delivered = ('price', 'size', 'known_at_ns')
    if generator['kind'] == 'window':
        seed = None
        if generator.get('seed_policy') == 'supplied':
            seed = T.SeedSupply(facts=({'fact_kind': T.SEED_KIND, 'instrument_id': first['instrument_id'],
                'session_id': first['session_id'], 'effective_start_ns': -1, 'effective_end_ns': 0,
                'text': generator['seed']['sign'], 'price': generator['seed']['price'], 'factor_num': None,
                'factor_den': None, 'known_at_ns': 0},), seed_set_id=SEED_NOTATION.match(H.BINDINGS[b['_index']]['seed_ref']).group(1))
        for number, w in enumerate(generator['windows']):
            case = T.window_case(**unit, window_ns=cutoff if w == 'session' else w, seed_policy=generator['seed_policy'],
                evidence_limit=10, policy_version=POLICY['window'], trades=T.TradeSupply(rows=tuple(rows), delivered=delivered),
                seed=seed)
            features = tuple(sorted(H.FEAT.values()))
            item = make_work(case, case.config(), features, '%s-%s-%d' % (run_id, case_id, number), scope)
            plan.append(('window', item, (lambda r, key=str(w): compare_generated_window(H, r, generator['expected'][key]))))
        return plan
    for number, k in enumerate(generator['ks']):
        ids = generator['expected'][str(k)]
        case = T.top_k_case(**unit, top_k=k, evidence_limit=k, policy_version=POLICY['topk'],
            trades=T.TradeSupply(rows=tuple(rows), delivered=delivered))
        plan.append(('topk', make_work(case, case.config(), (TOP_K,), '%s-%s-k%d' % (run_id, case_id, k), scope),
            (lambda r, ids=ids: compare_generated_ids(r, ids))))
        count = generator['partitions']
        cases = [T.top_k_case(**unit, top_k=k, evidence_limit=k, policy_version=POLICY['topk'],
            trades=T.TradeSupply(rows=tuple(r for j, r in enumerate(rows) if j % count == i), delivered=delivered,
                input_id='part%d' % i)) for i in range(count)]
        item = make_merge_work(cases, cases[0].config(), tuple(range(count)), '%s-%s-m%d' % (run_id, case_id, k), scope)
        plan.append(('merge', item, (lambda r, ids=ids: compare_generated_ids(r, ids))))
    return plan


def compare_generated_window(H, result, want: dict) -> list:
    problems = []
    for column in result.values:
        value, = column.values
        key = H.KEY[column.feature_id]
        fraction = None if want[key] is None else Fraction(want[key])
        if key == 'imb':
            ok = value is None if fraction is None else type(value) is float and value == fraction.numerator / fraction.denominator
        else:
            ok = type(value) is int and fraction is not None and Fraction(value) == fraction
        if not ok:
            problems.append([column.feature_id, value, want[key]])
    return problems


def compare_generated_ids(result, ids: list) -> list:
    cell, = result.values[0].values
    got = [r.event_id for r in cell.rows]
    return [] if got == ids else [['ids', got[:5], ids[:5]]]


def limits_for(plan: list) -> SinkRequirements:
    """Sink caps sized to the run count; chunk, cell and evidence caps are the sink's own maxima."""
    count = len(plan)
    return SinkRequirements(max_results=count, max_chunk_bytes=1048576, max_total_bytes=min(10485760, 1048576 * count),
        max_result_cells=100000, max_evidence_rows=100000)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--case', help='contract result fixture id (W01, TK01, PM01) or generator id (G01-G03)')
    parser.add_argument('--refusals', action='store_true', help='drive the 75 refusal fixtures through the worker')
    parser.add_argument('--only', default='', help='with --refusals: comma-separated refusal ids to run (default: all)')
    parser.add_argument('--output-db', type=Path)
    parser.add_argument('--html', type=Path)
    parser.add_argument('--run-id', default='ef4-tape')
    parser.add_argument('--binding', type=int, default=0, help='harness identity binding index (0 or 1)')
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    parser.add_argument('--harness', type=Path, default=None, help='contract fixture harness (or EF4G_HARNESS)')
    args = parser.parse_args(argv)
    document = contract_document(args.contract)
    H = load_harness(args.harness)
    from equity_feature_duckdb import tape as T
    b = dict(H.BINDINGS[args.binding], _index=args.binding)
    if args.refusals:
        only = {part for part in args.only.split(',') if part} or None
        rows = run_refusals(H, T, document, only, b)
        print(json.dumps(rows, indent=1, sort_keys=True))
        bad = [r for r in rows if r['stage'] == 'no_refusal' or (not r['matched'] and 'known_adapter_deviation' not in r)]
        return 0 if not bad else 1
    if not args.case or not args.output_db or not args.html:
        parser.error('--case, --output-db and --html are required for a result case')
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()):
        parser.error('Require new distinct absolute output paths, existing parents and bounded ASCII run identity')
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.ef1_runner import run_ef4_tape_top_k, run_ef4_tape_top_k_merge, run_ef4_tape_window

    fixtures = fixtures_by_id(document)
    if len(fixtures) != RESULT_COUNT:
        parser.error('Contract result fixtures differ from the example case list')
    scope = 'ef4-tape-synthetic'
    plan = build_plan(H, T, document, fixtures, args.case, b, scope, args.run_id)
    fixture = fixtures.get(args.case)
    limits = limits_for(plan)
    runners = {'window': run_ef4_tape_window, 'topk': run_ef4_tape_top_k, 'merge': run_ef4_tape_top_k_merge}
    output = separate_output_path(args.output_db, ())
    items = [item for _, item, _ in plan]
    stored: list = [None] * len(plan)
    receipts: list = [None] * len(plan)
    with DuckDBSink(output, scope, limits=limits) as sink:
        for kind in ('window', 'topk', 'merge'):
            positions = [n for n, (k, _, _) in enumerate(plan) if k == kind]
            if not positions:
                continue
            outcome = runners[kind](tuple(items[n] for n in positions), sink, requirements=limits)
            if len(outcome.readbacks) != len(positions) or not all(row.verified for row in outcome.readbacks):
                print(json.dumps({'verified_readback': False, 'reason': [row.reason for row in outcome.readbacks]},
                    sort_keys=True))
                return 1
            for n, row in zip(positions, ordered([items[n] for n in positions], outcome)):
                stored[n], receipts[n] = row.results[0], row
        problems = [found for (_, _, check), result in zip(plan, stored) for found in [check(result)] if found]
        if problems:
            raise AssertionError('Calculated result differs from the contract fixture literal: ' + json.dumps(problems, default=str))
        sdk = b'\n'.join(encode_result(result) for result in stored)
        for kind in ('window', 'topk', 'merge'):
            positions = [n for n, (k, _, _) in enumerate(plan) if k == kind]
            if not positions:
                continue
            replay = runners[kind](tuple(items[n] for n in positions), sink, requirements=limits)
            if (len(replay.readbacks) != len(positions) or not all(row.verified for row in replay.readbacks)
                    or [row.output for row in ordered([items[n] for n in positions], replay)]
                    != [receipts[n].output for n in positions]):
                raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, scope, limits=limits) as reopened:
        for row, result in zip(receipts, stored):
            restored = reopened.read(row.output.receipt)
            verify_receipt(row.output.receipt, row.output.envelope, restored)
            if tuple(map(encode_result, restored)) != (encode_result(result),):
                raise AssertionError('Full stored-result reopen differs')
    title = fixture['description'] if fixture else next(g['description'] for g in document['generator_fixtures'] if g['id'] == args.case)
    html = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF4 tape ' + escape(args.case) + '</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>EF4 tape (contract fixture ' + escape(args.case) + ')</h1>'
        '<p>' + escape(title) + '. Supplied-input adapter  ->  worker  ->  pure library  ->  separate DuckDB sink. '
        'Contract-literal match (value by acceptance rule, status, reasons, counts and witnesses), SDK readback, idempotent '
        'replay and close/reopen verified. Runs: ' + str(len(plan)) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(receipts[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every tick sign, window total, notional, rank and merge is the library result. '
        'No remote assets or JavaScript.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(plan), 'task_sha256': items[0].task.task_sha256,
        'content_sha256': receipts[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
