"""Synthetic contract fixtures -> EF4 path efficiency worker -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture PE01-PE31 of the frozen path efficiency and choppiness contract (EQ-161,
``ef4-path1``, config ``ef4.path_efficiency``). Each fixture is run once per selected output (one-output selection)
and once jointly when it selects several outputs; every run goes through the I/O adapter
(``equity_feature_duckdb.path_efficiency``), the worker, the pure library, a separate DuckDB sink, SDK readback,
idempotent replay and close/reopen, and is compared with the contract literal (value by the contract acceptance rule,
status, reasons, counts and witnesses).

Compact fixture notation is expanded here to supplied rows (market cutoff C is the end of the last delivered slot,
knowledge cutoff K = C + 100 s, ``ka`` mapped to K - (100 - ka) s). Slots after an explicit ``target`` (PE25, the
prefix fixture) are dropped because the R17 context ends at its target; the contract equivalence PE25 = PE04 is checked
by the contract literal of PE25 itself.

``--refusals`` drives all 39 refusal recipes PR01-PR39 from the contract: the recorded mutation is applied to its base
fixture, then the adapter, the worker build (work item admission) and the worker calculation are run, and the stage
and codes are printed per recipe. Nothing is published on a refusal.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
from dataclasses import asdict, replace
from html import escape
from pathlib import Path

from equity_feature_contracts import (
    AdjustmentSpec, AvailabilitySpec, BatchMetadata, ContractError, Coverage, InputBinding, InputScope, Parameter,
    PriceUnit, SessionSpec, SourceBinding)
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

CONTRACT = 'PATH_EFFICIENCY_CONTRACT_PROPOSAL.json'
RESULT_COUNT = 31
REFUSAL_COUNT = 39
T0 = 1_700_000_000_000_000_000
BAR = 60 * 10 ** 9
DAY = 86_400 * 10 ** 9
SEC = 10 ** 9
SCOPE = 'ef4-path-efficiency-synthetic'
# Fixture-driver opt-in (io #50 pattern): the compact notation's default known_at token (K - 50 s) is stated by the
# driver and passed explicitly; nothing below defaults a missing known_at on its own.
FIXTURE_DEFAULT_KA = 50


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features_intraday
        override = Path(equity_features_intraday.__file__).resolve().parents[4] / 'docs' / 'ef4' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def accept(reference: dict, value) -> bool:
    """exact_float64 and exact_plus_zero are bit-exact (+0.0 never -0.0); tolerance is abs(y-r) <= max(8*ulp(r), 1e-12*abs(r))."""
    want = reference['value']
    if want is None or value is None:
        return want is None and value is None
    r = float.fromhex(want['hex'])
    if want['acceptance'] in ('exact_float64', 'exact_plus_zero'):
        return value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r)
    if want['acceptance'] == 'tolerance':
        return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))
    return False


def runs_for(fixture: dict) -> list[dict]:
    """One run per selected output plus one joint run; each carries the contract expectations for its outputs."""
    expected = fixture['expected']
    runs = [dict(features=(feature,), expected={feature: expected[feature]}) for feature in sorted(expected)]
    if len(expected) > 1:
        runs.append(dict(features=tuple(sorted(expected)), expected=expected))
    return runs


def default_ctx(fixture: dict) -> dict:
    unit = fixture['unit']
    return {'config_unit': dict(unit), 'batch': {'unit': dict(unit), 'adjustment': 'raw'}, 'config_adjustment': 'raw',
            'capability': 'batch', 'features': list(fixture['outputs'])}


def set_path(root, path: str, value=None, delete: bool = False) -> None:
    """Apply one recorded recipe step ('set' or 'del' on a dotted path) in place."""
    parts = path.split('.')
    cur = root
    for key in parts[:-1]:
        cur = cur[int(key)] if isinstance(cur, list) else cur[key]
    last = parts[-1]
    if isinstance(cur, list):
        index = int(last)
        if delete:
            del cur[index]
        elif index == len(cur):
            cur.append(value)
        else:
            cur[index] = value
    elif delete:
        del cur[last]
    else:
        cur[last] = value


def explicit_batch(context, entries, cols, price, adjustment, n, daily, dur, *, default_known_at: int):
    """One chunk with explicit identity rows for wire-form recipes (identity from the wire override or the grid)."""
    from equity_feature_duckdb.path_efficiency import make_path_batch, make_slot_coverage
    rows, intervals = [], []
    names = ('instrument_id', 'session_id', 'start_ns', 'end_ns', *cols, 'known_at_ns')
    for i, entry in enumerate(entries):
        start, end = T0 + i * dur, T0 + (i + 1) * dur
        default = {'inst': context.instrument_id, 'ss': (f'D{i}' if daily else context.sessions[0].session_id),
                   'start': start, 'end': end}
        if entry is None or entry in ('absent', 'gap', 'empty'):
            if entry == 'gap':
                intervals.append(make_slot_coverage(i, start, end, expected=1, observed=0, complete=False))
            elif entry == 'empty':
                intervals.append(make_slot_coverage(i, start, end, expected=0, observed=0, complete=True))
            continue
        if 'wire' in entry:
            record = {**default, **{k: v for k, v in entry['wire'].items() if k != 'row'}}
            values = tuple(entry['wire']['row'])
            known_at = T0 + n * dur + default_known_at * SEC  # K - default_known_at s (fixture compact notation)
        else:
            record, values, known_at = default, tuple(entry['values']), entry['known_at_ns']
        rows.append((record['inst'], record['ss'], record['start'], record['end'], *values, known_at))
        intervals.append(make_slot_coverage(i, start, end, expected=1, observed=1, complete=True))
    scope = InputScope(min(s.open_ns for s in context.sessions), max(s.close_ns for s in context.sessions),
                       'ef4-path-regular')
    metadata = BatchMetadata('SYN', SourceBinding('SYN-SRC', 'snap1', 'ef4-path-map1', 'bars-000'),
                             Coverage(None, len(rows), False), price, adjustment, 'none', 'shares', 'declared',
                             'reject', scope, tuple(intervals))
    return make_path_batch(names, tuple(rows), metadata=metadata)


def expand(fixture: dict, ctx: dict, *, default_known_at: int):
    """Fixture and context -> (context, chunks, config, features, capability). Adapter refusals raise ContractError."""
    from equity_feature_duckdb.path_efficiency import (
        CLOSE_COLUMNS, OHLCV_COLUMNS, make_path_chunks, make_path_config, make_path_context)
    from equity_feature_duckdb.relative_rv import IntradayRunSpec
    p = fixture['params']
    target = fixture.get('target', len(fixture['slots']) - 1)
    slots = fixture['slots'][: target + 1]
    n = len(slots)
    profile = p.get('epoch_profile')
    daily = profile == 'governed_history'
    dur = DAY if daily else BAR
    cutoff = T0 + n * dur
    knowledge = cutoff + 100 * SEC
    cols = OHLCV_COLUMNS if fixture['columns'] == 'ohlcv' else CLOSE_COLUMNS
    width = len(cols)

    def known(ka):
        return None if ka is None else knowledge - (100 - ka) * SEC

    def entry(slot):
        if slot == 'null':  # row present, null volume and null OHLC (or null close)
            return {'values': (None,) * width, 'known_at_ns': known(default_known_at)}
        if isinstance(slot, str) or slot is None or (isinstance(slot, dict) and 'wire' in slot):
            return slot
        if isinstance(slot, dict) and 'row' in slot:
            row = slot['row']
            return {'values': tuple(row) if row is not None else (None,) * width, 'known_at_ns': known(slot['ka'])}
        return {'values': tuple(slot), 'known_at_ns': known(default_known_at)}

    entries = [entry(s) for s in slots]
    if daily:
        sessions = tuple(SessionSpec('SYN', f'D{i}', T0 + i * DAY, T0 + (i + 1) * DAY, 'UTC') for i in range(n))
        runs = tuple(IntradayRunSpec(f'D{i}', T0 + i * DAY, DAY, 1) for i in range(n))
    else:
        sessions = (SessionSpec('SYN', 'S1', T0, cutoff, 'UTC'),)
        runs = (IntradayRunSpec('S1', T0, BAR, n),)
    context = make_path_context('X', calendar_id='SYN-CAL', grid_id='SYN-GRID', revision='r1', sessions=sessions,
                                runs=runs, anchor_index=0, target_index=n - 1, epoch_profile=profile)
    config_unit, batch_unit = ctx.get('config_unit'), ctx['batch']['unit']
    batch_price = PriceUnit(batch_unit['scale'], batch_unit['currency'])
    batch_adjustment = AdjustmentSpec(basis=ctx['batch']['adjustment'])
    if any(isinstance(e, dict) and 'wire' in e for e in entries):
        chunks = (explicit_batch(context, entries, cols, batch_price, batch_adjustment, n, daily, dur,
                                 default_known_at=default_known_at),)
    else:
        chunks = make_path_chunks(context, entries, input_id='bars', source_id='SYN-SRC', snapshot_id='snap1',
                                  namespace='SYN', price_unit=batch_price, adjustment=batch_adjustment, columns=cols,
                                  eligibility_policy='ef4-path-regular')
    price = PriceUnit(config_unit['scale'], config_unit['currency']) if config_unit is not None else None
    config = make_path_config(context, window=p.get('window'), epoch_profile=profile,
                              availability=AvailabilitySpec(cutoff, knowledge, knowledge),
                              adjustment=AdjustmentSpec(basis=ctx.get('config_adjustment', 'raw')),
                              price_unit=price, evidence_limit=p.get('evidence_limit'),
                              policy_version=p.get('policy_version'))
    # A surplus parameter is not representable by the adapter; the recipe's extra key is appended raw.
    extra = [k for k in p if k not in ('policy_version', 'window', 'epoch_profile', 'evidence_limit')]
    if extra:
        config = replace(config, parameters=config.parameters + tuple(Parameter(k, p[k]) for k in extra))
    return context, chunks, config, tuple(ctx.get('features', fixture['outputs'])), ctx.get('capability', 'batch')


def declarations(chunks) -> tuple[InputManifest, ...]:
    """One manifest per BAR chunk, in the library's role naming ``bars:NNN``."""
    result = []
    for number, batch in enumerate(chunks):
        role = f'bars:{number:03}'
        raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True,
                         allow_nan=False).encode('ascii')
        coverage = batch.metadata.coverage
        state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
        result.append(InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
                                    InputBinding(role, batch.kind, batch.metadata)))
    return tuple(result)


def build_item(fixture: dict, features: tuple[str, ...], label: str):
    from equity_feature_workers.equity_path_efficiency import make_ef4_path_efficiency_work
    context, chunks, config, _, _ = expand(fixture, default_ctx(fixture), default_known_at=FIXTURE_DEFAULT_KA)
    return make_ef4_path_efficiency_work(config, features=features, inputs=declarations(chunks), batches=chunks,
        instrument_id=context.instrument_id, job_id='job-' + label, generation_id='generation-' + label,
        partition_id='X-S', destination_scope=SCOPE)


def compare(result, fixture: dict, run: dict) -> list:
    """Values (acceptance rule), status, reasons, counts and witnesses against the contract literal for this run."""
    expected = run['expected']
    limit = fixture['params']['evidence_limit']
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
        # Per-output witnesses hold for a one-output selection; a joint run is checked flat below.
        if len(run['features']) == 1 and found[column.feature_id] != want.get('witnesses', []):
            problems.append([column.feature_id, 'witnesses', found[column.feature_id], want.get('witnesses', [])])
    if len(run['features']) > 1:
        want_joint = [text for feature in sorted(expected) for text in expected[feature].get('witnesses', [])][:limit]
        flat = [row.row_id for row in result.evidence]
        if flat != want_joint:
            problems.append(['joint evidence', flat, want_joint])
    return problems


def ordered(items, outcome) -> list:
    """Readbacks in item order, matched by task digest."""
    by_task = {row.task_sha256: row for row in outcome.readbacks}
    return [by_task[item.task.task_sha256] for item in items]


# ---- refusal recipes PR01-PR39 through the worker path -------------------------------------------------------------

def _code(error) -> str:
    return str(getattr(error.code, 'value', error.code)).upper()


def _direct_config(recipe: dict, base: dict):
    """For a configuration recipe the adapter refused: hand the worker the same fault as a raw ConfigSpec."""
    from equity_feature_workers.commands import CommandError
    from equity_feature_workers.equity_path_efficiency import make_ef4_path_efficiency_work
    steps = recipe['mutation']
    if not all(step.get('set', step.get('del', '')).startswith(('fixture.params.', 'ctx.config_unit')) for step in steps):
        return None  # a batch fault: the batch cannot be constructed, so the worker is never reached
    context, chunks, config, features, _ = expand(base, default_ctx(base), default_known_at=FIXTURE_DEFAULT_KA)
    params = {p.name: p.value for p in config.parameters}
    unit = config.price_unit
    try:
        for step in steps:
            path = step.get('set', step.get('del'))
            if path == 'ctx.config_unit':
                unit = None
            elif 'set' in step:
                params[path.split('.')[-1]] = step['value']
            else:
                params.pop(path.split('.')[-1], None)
        raw = replace(config, parameters=tuple(Parameter(k, v) for k, v in params.items()), price_unit=unit)
        make_ef4_path_efficiency_work(raw, features=features, inputs=declarations(chunks), batches=chunks,
            instrument_id=context.instrument_id, job_id='job-direct', generation_id='generation-direct',
            partition_id='refusal', destination_scope=SCOPE)
    except CommandError as error:
        return 'build:' + error.code.value
    except ContractError as error:
        return 'constructor:' + _code(error)
    return 'not_refused'


def run_refusal(recipe: dict, base: dict) -> dict:
    """Apply the recorded mutation to its base fixture and record where it was refused and with which codes."""
    from equity_feature_workers.commands import CommandError
    from equity_feature_workers.equity_path_efficiency import compute_path_efficiency_inputs, make_ef4_path_efficiency_work
    import equity_features_intraday as library
    obj = {'fixture': copy.deepcopy(base), 'ctx': default_ctx(base)}
    for step in recipe['mutation']:
        if 'set' in step:
            set_path(obj, step['set'], step['value'])
        else:
            set_path(obj, step['del'], delete=True)
    row = dict(id=recipe['id'], contract_code=recipe['error_code'], stage='not_refused', worker_code=None,
               library_code=None)
    try:
        context, chunks, config, features, capability = expand(obj['fixture'], obj['ctx'], default_known_at=FIXTURE_DEFAULT_KA)
    except ContractError as error:
        row.update(stage='adapter', library_code=_code(error))  # the adapter (shared validators/builders) refused
        row['worker_direct'] = _direct_config(recipe, base)
        return row
    # The library's own code for the same inputs (the worker collapses codes into its own taxonomy).
    try:
        ic = library.IntradayContext.from_json(context.to_json())
        for feature in features:
            library.require_path_efficiency_capability(feature, capability)
        library.compute_path_efficiency(chunks, ic, config, features=features)
    except ContractError as error:
        row['library_code'] = _code(error)
    if capability != 'batch':
        row['stage'] = 'not_a_worker_path'  # the worker exposes no update, restore or merge
        return row
    try:
        item = make_ef4_path_efficiency_work(config, features=features, inputs=declarations(chunks), batches=chunks,
            instrument_id=context.instrument_id, job_id='job-' + recipe['id'], generation_id='generation-' + recipe['id'],
            partition_id='refusal', destination_scope=SCOPE)
    except CommandError as error:
        row.update(stage='build', worker_code=error.code.value, worker_contract_code=error.contract_code)
        return row
    except ContractError as error:
        row.update(stage='build', worker_code='contract:' + _code(error))
        return row
    try:
        compute_path_efficiency_inputs(item.task, item.batches)
    except CommandError as error:
        row.update(stage='compute', worker_code=error.code.value, worker_contract_code=error.contract_code)
        return row
    return row


def run_refusals(document: dict) -> list:
    fixtures = {row['id']: row for row in document['path_fixtures']}
    return [run_refusal(recipe, fixtures[recipe['base']]) for recipe in document['refusal_fixtures']]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', help='contract result fixture id, e.g. PE01, PE28')
    parser.add_argument('--refusals', action='store_true', help='drive the 39 refusal recipes through the worker')
    parser.add_argument('--output-db', type=Path)
    parser.add_argument('--html', type=Path)
    parser.add_argument('--run-id')
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    args = parser.parse_args(argv)
    document = contract_document(args.contract)
    if len(document['path_fixtures']) != RESULT_COUNT or len(document['refusal_fixtures']) != REFUSAL_COUNT:
        parser.error('Contract fixtures differ from the example case list')
    if args.refusals:
        rows = run_refusals(document)
        for row in rows:
            print(json.dumps(row, sort_keys=True))
        bad = [r['id'] for r in rows if r['stage'] == 'not_refused' or (r['library_code'] or r['contract_code']) != r['contract_code']]
        print(json.dumps({'refusals': len(rows), 'mismatch': bad}, sort_keys=True))
        return 1 if bad else 0
    fixtures = {row['id']: row for row in document['path_fixtures']}
    if args.case not in fixtures or args.output_db is None or args.html is None or args.run_id is None:
        parser.error('--case (PE01-PE31), --output-db, --html and --run-id are required (or --refusals)')
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
    from equity_feature_workers.ef1_runner import run_ef4_path_efficiency

    fixture = fixtures[args.case]
    limit = fixture['params']['evidence_limit']
    runs = runs_for(fixture)
    items = [build_item(fixture, run['features'], '%s-%d' % (args.run_id, number)) for number, run in enumerate(runs)]
    limits = SinkRequirements(max_results=len(items), max_chunk_bytes=1048576, max_total_bytes=1048576 * len(items),
        max_result_cells=64 * len(items), max_evidence_rows=max(1, min(limit, 40960)) * len(items))
    output = separate_output_path(args.output_db, ())
    with DuckDBSink(output, SCOPE, limits=limits) as sink:
        outcome = run_ef4_path_efficiency(tuple(items), sink, requirements=limits)
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
        replay = run_ef4_path_efficiency(tuple(items), sink, requirements=limits)
        if (len(replay.readbacks) != len(items) or not all(row.verified for row in replay.readbacks)
                or [row.output for row in ordered(items, replay)] != [row.output for row in rows]):
            raise AssertionError('Replay identity/readback mismatch')
    with DuckDBSink(output, SCOPE, limits=limits) as reopened:
        for row, result in zip(rows, stored):
            restored = reopened.read(row.output.receipt)
            verify_receipt(row.output.receipt, row.output.envelope, restored)
            if tuple(map(encode_result, restored)) != (encode_result(result),):
                raise AssertionError('Full stored-result reopen differs')
    outputs = ', '.join(sorted(fixture['expected']))
    document_html = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF4 path efficiency</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>EF4 path efficiency and choppiness (contract fixture ' + escape(args.case) + ')</h1>'
        '<p>' + escape(fixture['description']) + '. Actual input adapter  ->  worker  ->  pure library  ->  separate DuckDB sink. '
        'Contract-literal match (value by acceptance rule, status, reasons, counts, witnesses), '
        'SDK readback, idempotent replay and close/reopen verified. Runs: ' + str(len(runs)) + '.</p>'
        '<p>Selected outputs across runs: ' + escape(outputs) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(rows[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every net displacement, path length, ratio, logarithm, true range, window '
        'range and choppiness value is the library result. No remote assets or JavaScript. Browser inspection remains a separate gate.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(document_html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(runs), 'task_sha256': items[0].task.task_sha256,
        'content_sha256': rows[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
