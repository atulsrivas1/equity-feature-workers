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
import sys
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
    batches = (close_batch('X', grid, inst, input_id='instrument', scale=scale, known_default=50),
        close_batch('B', grid, bench, input_id='benchmark', scale=scale, known_default=50))
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
    batches = (close_batch('X', grid, slots, input_id='daily', scale=scale, known_default=50),)
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
    batches = (intraday_batch(context, slots, input_id='bars', scale=scale, known_default=50),)
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


# ---- refusal fixtures RF01-RF48 through the worker path ------------------------------------------------------------
# The contract JSON stores only the error code and a description per refusal, so each case builds its violation on a
# valid base here. A case is driven through the I/O adapter (build), the worker (admission, then calculation) and the
# library directly; a case matches only when the adapter or worker reports the contract code AND the library raises
# the same code. update/restore/merge (RF20, RF32, RF48) have no worker path (a work item carries no capability
# request), so they are checked against the library capability gate and reported with stage 'library_capability'.

_RELATIVE_FEATURES = ('relative.beta', 'relative.correlation', 'relative.residual_return')
_LOGVOL_FEATURES = ('history.log_return_volatility',)
_PRICED = [{'state': 'priced', 'c': c, 'known_at': 50} for c in (10000, 10200, 10100, 10500, 10400, 10600)]
_BENCH = [{'state': 'priced', 'c': c, 'known_at': 50} for c in (20000, 20100, 20300, 20200, 20400, 20500)]


def _rel_base(**over):
    from equity_feature_duckdb.relative_rv import (availability_for, close_batch, daily_grid, daily_sessions,
        relative_config)
    sessions = daily_sessions(6)
    grid = daily_grid(sessions, target_index=5)
    params = dict(benchmark_id='B', window=3, min_pairs=2, evidence_limit=10, scale=2)
    params.update(over)
    config = relative_config(grid, availability=availability_for(sessions, 100), **params)
    batches = (close_batch('X', grid, _PRICED, input_id='instrument', scale=2, known_default=50),
        close_batch('B', grid, _BENCH, input_id='benchmark', scale=2, known_default=50))
    return dict(kind='relative', grid=grid, config=config, batches=batches, roles=('instrument', 'benchmark'),
        features=_RELATIVE_FEATURES, instrument='X')


def _vol_base(**over):
    from equity_feature_duckdb.relative_rv import (availability_for, close_batch, daily_grid, daily_sessions,
        log_volatility_config)
    sessions = daily_sessions(6)
    grid = daily_grid(sessions, target_index=5)
    params = dict(period=3, annualization_factor=252, evidence_limit=10, scale=2)
    params.update(over)
    config = log_volatility_config(grid, availability=availability_for(sessions, 100), **params)
    batches = (close_batch('X', grid, _PRICED, input_id='daily', scale=2, known_default=50),)
    return dict(kind='logvol', grid=grid, config=config, batches=batches, roles=('daily',),
        features=_LOGVOL_FEATURES, instrument='X')


def _rv_base(*, factor=252, features=('intraday.realized_variance', 'intraday.realized_volatility'), market_cutoff=None,
        mode='reset', **over):
    from equity_feature_duckdb.relative_rv import (availability_for, intraday_batch, make_intraday_context,
        realized_variance_config, synthetic_intraday_session)
    session, runs = synthetic_intraday_session(6)
    context = make_intraday_context('X', sessions=session, runs=runs, anchor_index=0, target_index=5, mode=mode)
    params = dict(evidence_limit=10, scale=2)
    params.update(over)
    config = realized_variance_config(context, annualization_factor=factor,
        availability=availability_for(session, 100, market_cutoff), **params)
    batches = (intraday_batch(context, _PRICED, input_id='bars', scale=2, known_default=50),)
    return dict(kind='rv', context=context, config=config, batches=batches, roles=('bars:000',), features=features,
        instrument='X')


def _drop(spec, name):
    import dataclasses
    config = spec['config']
    return dict(spec, config=dataclasses.replace(config, parameters=tuple(p for p in config.parameters if p.name != name)))


def _rows(batch):
    return [tuple(column.values[i] for column in batch.columns) for i in range(batch.row_count)]


def _rebuild(batch, rows, *, scale=2, namespace='SYN'):
    """Same metadata (coverage entries) with explicit rows; the shared validators run inside, so this can refuse."""
    from equity_feature_duckdb import relative_rv as r
    names = tuple(column.name for column in batch.columns)
    meta = batch.metadata
    return r._bar_batch(names, rows, meta.interval_coverage, meta.scope, input_id=meta.source.input_id,
        scale=scale, namespace=namespace, count=len(rows))


def _with_batches(spec, *batches):
    return dict(spec, batches=tuple(batches))


def _adjusted(batch):
    import dataclasses
    from equity_feature_contracts import AdjustmentSpec
    return dataclasses.replace(batch, metadata=dataclasses.replace(batch.metadata,
        adjustment=AdjustmentSpec(basis='split', policy_version='adj-v1', action_snapshot='act-1', anchor='anchor-1')))


def _relative_refusal(rid: str) -> dict:
    from equity_feature_duckdb.relative_rv import close_batch
    spec = _rel_base()
    grid, (inst, bench) = spec['grid'], spec['batches']
    if rid == 'RF01':
        return _rel_base(window=1)
    if rid == 'RF02':
        return _rel_base(window=True)
    if rid == 'RF03':
        return _rel_base(window=3, min_pairs=4)
    if rid == 'RF04':
        return _rel_base(min_pairs=1)
    if rid == 'RF05':
        return _drop(spec, 'window')
    if rid == 'RF06':
        return _rel_base(benchmark_id='')
    if rid == 'RF07':
        return _rel_base(benchmark_id=['B', 'C'])
    if rid == 'RF08':
        return _rel_base(evidence_limit=10001)
    if rid == 'RF09':
        return _drop(spec, 'policy_version')
    if rid == 'RF10':
        return _rel_base(policy_version='ef3-rel2')
    if rid == 'RF11':
        return _with_batches(spec, inst, close_batch('Q', grid, _BENCH, input_id='benchmark', scale=2, known_default=50))
    if rid == 'RF12':
        return _with_batches(spec, close_batch('B', grid, _PRICED, input_id='instrument', scale=2, known_default=50), bench)
    if rid == 'RF13':
        rows = _rows(inst)
        rows[1] = ('Z',) + rows[1][1:]
        return _with_batches(spec, _rebuild(inst, rows), bench)
    if rid == 'RF14':
        return _with_batches(spec, inst, close_batch('B', grid, _BENCH, input_id='benchmark', scale=2, namespace='OTHER', known_default=50))
    if rid == 'RF15':
        return _with_batches(spec, inst, close_batch('B', grid, _BENCH, input_id='benchmark', scale=3, known_default=50))
    if rid == 'RF16':
        return _with_batches(spec, inst, _adjusted(bench))
    if rid == 'RF17':
        return _with_batches(spec, _rebuild(inst, _rows(inst)[::-1]), bench)
    if rid == 'RF18':
        rows = _rows(inst)
        return _with_batches(spec, _rebuild(inst, rows + [rows[0]]), bench)
    if rid == 'RF19':
        return _with_batches(spec, close_batch('X', grid, [dict(_PRICED[0], c=0)] + _PRICED[1:], input_id='instrument', scale=2, known_default=50), bench)
    if rid == 'RF20':
        return dict(spec, capability=('relative.beta', 'update'))
    if rid == 'RF21':
        return dict(spec, features=('history.return_volatility',))
    raise KeyError(rid)


def _logvol_refusal(rid: str) -> dict:
    from equity_feature_duckdb.relative_rv import close_batch
    spec = _vol_base()
    grid, (daily,) = spec['grid'], spec['batches']
    if rid == 'RF22':
        return _vol_base(period=1)
    if rid == 'RF23':
        return _drop(spec, 'annualization_factor')
    if rid == 'RF24':
        return _vol_base(evidence_limit=10001)
    if rid == 'RF25':
        return _vol_base(policy_version='ef3-dlv2')
    if rid == 'RF26':
        return _with_batches(spec, close_batch('X', grid, [dict(_PRICED[0], c=-1)] + _PRICED[1:], input_id='daily', scale=2, known_default=50))
    if rid == 'RF27':
        return _with_batches(spec, _rebuild(daily, _rows(daily)[::-1]))
    if rid == 'RF28':
        rows = _rows(daily)
        return _with_batches(spec, _rebuild(daily, rows + [rows[0]]))
    if rid == 'RF29':
        return _with_batches(spec, close_batch('X', grid, _PRICED, input_id='daily', scale=3, known_default=50))
    if rid == 'RF30':
        return _with_batches(spec, _adjusted(daily))
    if rid == 'RF31':
        rows = _rows(daily)
        rows[1] = ('Z',) + rows[1][1:]
        return _with_batches(spec, _rebuild(daily, rows))
    if rid == 'RF32':
        return dict(spec, capability=('history.log_return_volatility', 'merge'))
    raise KeyError(rid)


def _rv_refusal(rid: str) -> dict:
    from equity_feature_duckdb.relative_rv import intraday_batch
    spec = _rv_base()
    context, (bars,) = spec['context'], spec['batches']

    def edited(index, column, value):
        rows = _rows(bars)
        row = list(rows[index])
        row[column] = value
        rows[index] = tuple(row)
        return _with_batches(spec, _rebuild(bars, rows))
    if rid == 'RF33':
        return _rv_base(factor=None)
    if rid == 'RF34':
        return _rv_base(factor=252, features=('intraday.realized_variance',))
    if rid == 'RF35':
        return _rv_base(factor=0)
    if rid == 'RF36':
        return _rv_base(mode='carry')
    if rid == 'RF37':
        return _rv_base(evidence_limit=10001)
    if rid == 'RF38':
        return _rv_base(sampling='every_other_slot')
    if rid == 'RF39':
        return _rv_base(policy_version='ef3-rv2')
    if rid == 'RF40':
        return edited(2, 7, None)  # positive volume, null close
    if rid == 'RF41':
        return edited(2, 8, 0)  # zero volume, non-null OHLC
    if rid == 'RF42':
        return edited(2, 8, -5)
    if rid == 'RF43':
        return _rv_base(market_cutoff=1000)
    if rid == 'RF44':
        return _with_batches(spec, _rebuild(bars, _rows(bars)[::-1]))
    if rid == 'RF45':
        rows = _rows(bars)
        return _with_batches(spec, _rebuild(bars, rows + [rows[0]]))
    if rid == 'RF46':
        return _with_batches(spec, intraday_batch(context, _PRICED, input_id='bars', scale=3, known_default=50))
    if rid == 'RF47':
        return _with_batches(spec, _adjusted(bars))
    if rid == 'RF48':
        return dict(spec, capability=('intraday.realized_variance', 'restore'))
    raise KeyError(rid)


def build_refusal(rid: str) -> dict:
    number = int(rid[2:])
    return (_relative_refusal if number <= 21 else _logvol_refusal if number <= 32 else _rv_refusal)(rid)


def _library_code(spec) -> str | None:
    """The library's own refusal code for the same batches and configuration (the worker does not return it)."""
    from equity_feature_contracts import ContractError
    try:
        if spec['kind'] == 'rv':
            import equity_features_intraday as intraday
            context = intraday.IntradayContext.from_json(spec['context'].to_json())
            intraday.compute_realized_variance(spec['batches'] or None, context, spec['config'], features=spec['features'])
        else:
            from equity_features import ef3
            grid = ef3.DailyGrid(*(getattr(spec['grid'], name) for name in
                ('instrument_id', 'calendar_id', 'grid_id', 'revision', 'sessions', 'target_index')))
            if spec['kind'] == 'relative':
                ef3.compute_relative_stats(spec['batches'], grid, spec['config'], features=spec['features'])
            else:
                ef3.compute_log_volatility(spec['batches'], grid, spec['config'], features=spec['features'])
        return None
    except ContractError as error:
        return error.code.value


def _capability_code(spec) -> str | None:
    from equity_feature_contracts import ContractError
    feature, capability = spec['capability']
    try:
        if spec['kind'] == 'rv':
            import equity_features_intraday as intraday
            intraday.require_realized_variance_capability(feature, capability)
        else:
            from equity_features import ef3
            ef3.require_ef3_capability(feature, capability)
    except ContractError as error:
        return error.code.value
    return None


def _make_work(spec, rid: str):
    from equity_feature_workers import equity_relative_rv as w
    inputs = tuple(declaration(role, batch) for role, batch in zip(spec['roles'], spec['batches']))
    common = dict(features=spec['features'], inputs=inputs, batches=spec['batches'], instrument_id=spec['instrument'],
        job_id='job-' + rid, generation_id='generation-' + rid, partition_id='refusal', destination_scope='ef3-refusal')
    if spec['kind'] == 'rv':
        return w.make_ef3_realized_variance_work(spec['config'], **common)
    from equity_features import ef3
    grid = spec['grid']
    library_grid = ef3.DailyGrid(grid.instrument_id, grid.calendar_id, grid.grid_id, grid.revision, grid.sessions,
        grid.target_index)
    make = w.make_ef3_relative_work if spec['kind'] == 'relative' else w.make_ef3_log_volatility_work
    return make(spec['config'], grid=library_grid, **common)


def _run_one(rid: str, fixture: dict) -> dict:
    """Build (adapter), admit (worker), calculate (worker) and record the library's own code beside the worker's."""
    from equity_feature_contracts import ContractError
    from equity_feature_workers.commands import CommandError
    record = {'id': rid, 'contract_code': fixture['error_code'], 'stage': None, 'adapter_code': None,
        'worker_code': None, 'worker_contract_code': None, 'library_code': None}
    try:
        spec = build_refusal(rid)
    except ContractError as error:
        record.update(stage='adapter_build', adapter_code=error.code.value)
        return _finish(record)
    if 'capability' in spec:
        record.update(stage='library_capability', library_code=_capability_code(spec))
        return _finish(record)
    try:
        item = _make_work(spec, rid)
    except CommandError as error:
        record.update(stage='worker_admission', worker_code=error.code.value, worker_contract_code=error.contract_code,
            library_code=_library_code(spec))
        return _finish(record)
    except ContractError as error:
        record.update(stage='worker_admission_untranslated', adapter_code=error.code.value)
        return _finish(record)
    try:
        item.calculate(item.task, item.batches)
        record.update(stage='no_refusal')
    except CommandError as error:
        record.update(stage='compute', worker_code=error.code.value, worker_contract_code=error.contract_code,
            library_code=_library_code(spec))
    return _finish(record)


def _finish(record: dict) -> dict:
    """Matched: the code the adapter or worker reports equals the fixture code, and the library raises the same code."""
    want = record['contract_code'].lower()
    stage = record['stage']
    if stage == 'adapter_build':
        record['matched'] = record['adapter_code'] == want
    elif stage == 'library_capability':
        record['matched'] = record['library_code'] == want
    else:
        record['matched'] = (stage in ('worker_admission', 'compute') and record['worker_contract_code'] == want
            and record['library_code'] == want)
    return record


def run_refusals(document: dict) -> list:
    """All 48 refusal fixtures in contract order; each row names the stage and the codes reported by each layer."""
    fixtures = {row['id']: row for row in document['fixtures']['refusals']}
    if len(fixtures) != 48:
        raise ValueError('Contract refusal fixtures differ from the example case list')
    return [_run_one(rid, fixtures[rid]) for rid in sorted(fixtures, key=lambda key: int(key[2:]))]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', help='contract result fixture id, e.g. RS01, DV04, RV21, PX01')
    parser.add_argument('--refusals', action='store_true', help='drive the 48 refusal fixtures RF01-RF48 through the worker')
    parser.add_argument('--output-db', type=Path)
    parser.add_argument('--html', type=Path)
    parser.add_argument('--run-id')
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    args = parser.parse_args(argv)
    if args.refusals:
        rows = run_refusals(contract_document(args.contract))
        print(json.dumps(rows, indent=1, sort_keys=True))
        print('refusals driven %d, matched %d' % (len(rows), sum(row['matched'] for row in rows)), file=sys.stderr)
        return 0 if all(row['matched'] for row in rows) else 1
    if not (args.case and args.output_db and args.html and args.run_id):
        parser.error('--case, --output-db, --html and --run-id are required for a result case')
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
