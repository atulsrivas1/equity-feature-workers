"""Synthetic contract fixtures -> EF3 robust worker -> NEW separate DuckDB -> verified HTML.

One case is one contract result fixture id: H01-H40 (history), C01-C20 (cohort, through the same history producer),
P01-P32 (volume pace) and R01-R18 (mean ratio), 124 in total including the prefix fixtures H25, P09, P18 and R16.
Each fixture is run once per selected output (one-output selection) and once jointly when it selects several outputs;
every run goes through the supplied-input adapter (equity_feature_duckdb.robust_pace), the worker, the pure library,
a separate DuckDB sink, SDK readback, replay and close/reopen.

``--refusals`` drives the 48 typed refusal fixtures RF01-RF48 through the worker path and reports, per fixture, the
stage at which it was refused (``build``: adapter or worker admission; ``compute``: the library calculation), the
contract code, the library's own code and the worker code the caller receives.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict
from html import escape
from pathlib import Path

from equity_feature_contracts import DataKind, InputBinding
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result, verify_receipt
from equity_feature_workers import InputManifest

CONTRACT = 'ROBUST_PACE_CONTRACT_PROPOSAL.json'
RESULT_COUNT = 124
PRODUCER = {'H': 'robust_history', 'C': 'robust_history', 'P': 'volume_pace_robust', 'R': 'volume_ratio_mean'}
ENTITY = 'Q9'  # the entity instrument of single-instrument runs (the adapter's reference rows use the same id)


def contract_document(override: Path | None) -> dict:
    """The frozen contract JSON in the core repository (or an explicit copy)."""
    if override is None:
        import equity_features
        override = Path(equity_features.__file__).resolve().parents[4] / 'docs' / 'ef3' / CONTRACT
    with override.open(encoding='utf-8') as source:
        return json.load(source)


def fixtures_by_id(document: dict) -> dict:
    rows: dict = {}
    for key in ('history_fixtures', 'cohort_fixtures', 'pace_fixtures', 'mean_fixtures'):
        rows.update({row['id']: row for row in document[key]})
    return rows


def accept(reference: dict, value) -> bool:
    """Value acceptance by the contract's rule: bit-exact float64, +0.0 where stated, int64 exact, or R17 tolerance."""
    want = reference['value']
    if want is None or value is None:
        return want is None and value is None
    if 'int64' in want:
        return value == want['int64']
    r = float.fromhex(want['hex'])
    if want['acceptance'] in ('exact_float64', 'exact_plus_zero'):
        return value.hex() == r.hex() and math.copysign(1.0, value) == math.copysign(1.0, r)
    return abs(value - r) <= max(8 * math.ulp(r), 1e-12 * abs(r))


def runs_for(fixture: dict) -> list[dict]:
    """One run per selected output, plus one joint run when the fixture selects several outputs."""
    expected = fixture['expected']
    runs = [dict(features=(feature,), expected={feature: expected[feature]}) for feature in sorted(expected)]
    if len(expected) > 1:
        runs.append(dict(features=tuple(sorted(expected)), expected=expected))
    return runs


# ---- supplied-input construction from the contract notation (the adapter does all batch and config building) ----------

def _sessions_for(count: int, closes: dict | None = None):
    from equity_feature_duckdb.robust_pace import session
    return [session(i, close=(closes or {}).get(i, 300)) for i in range(count)]


def _cell(token):
    from equity_feature_duckdb.robust_pace import parse_cell
    return parse_cell(token)


def _history_inputs(fixture: dict, features: tuple[str, ...]):
    """History (and cohort) producer: adapter batches and configuration for the selected outputs."""
    from equity_feature_duckdb import robust_pace as p
    if fixture['id'].startswith('C'):
        return _cohort_inputs(fixture, features)
    slots = list(fixture['slots'])
    appended = fixture.get('appended_rows', [])
    last = max([len(slots)] + [row['session'] for row in appended])
    sessions = []
    cells = {}
    for index, token in enumerate(slots):
        cell = _cell(token)
        cells[index] = cell
    cells[len(slots)] = _cell(fixture['target'])
    for index in range(last + 1):
        excluded = index in cells and cells[index].excluded
        sessions.append(p.Session(str(index), index * p.SESSION_STEP, index * p.SESSION_STEP + p.SESSION_SPAN_DEFAULT,
            excluded, cells[index].exclusion_known_at if index in cells else p.KNOWN_AT_DEFAULT))
    entries = [(ENTITY, sessions[i], cell) for i, cell in sorted(cells.items())]
    entries += [(ENTITY, sessions[row['session']], p.Cell(p.ROW, row['value'], row['known_at'])) for row in appended]
    target = sessions[len(slots)]
    field = fixture.get('field', 'volume')
    scale = fixture.get('scale') or 0
    series = p.daily_batch(entries, field=field, scale=scale, input_id='series', role='session')
    sessions_batch = p.sessions_batch(sessions, input_id='sessions', instrument_id=ENTITY)
    params = fixture['params']
    # Prefix fixtures append later governed sessions; the market cutoff then covers the last appended session
    # (the batches validate every row against the cutoff) while the target stays the configured session.
    timing = p.availability(max(s.close_ns for s in sessions), fixture['K'])
    quantile = tuple(params['quantile']) if 'quantile' in params and 'robust.quantile' in features else None
    config = p.robust_history_config(target, timing=timing, field=field, scale=scale, window=params['window'],
        min_history=params['min_history'], quantile=quantile, evidence_limit=params['evidence_limit'])
    return config, (series, sessions_batch), (('series', 'session'), ('sessions', 'session')), sessions


def _cohort_inputs(fixture: dict, features: tuple[str, ...]):
    from equity_feature_duckdb import robust_pace as p
    target = p.session(0)
    members = fixture['members']
    field = 'close' if fixture.get('scale') is not None else 'volume'
    scale = fixture.get('scale') or 0
    entries = [(name, target, _cell(token)) for name, token in members.items()]
    cohort = p.daily_batch(entries, field=field, scale=scale, input_id='cohort', role='member')
    population = p.population_batch(list(members), target, input_id='population')
    timing = p.availability(target.close_ns, fixture['K'])
    config = p.robust_history_config(target, timing=timing, field=field, scale=scale, window=1, min_history=1,
        min_population=fixture['params']['min_population'], evidence_limit=fixture['params']['evidence_limit'])
    return config, (population, cohort), (('population', 'session'), ('cohort_values', 'member')), [target]


def _pace_inputs(fixture: dict, features: tuple[str, ...]):
    from equity_feature_duckdb import robust_pace as p
    prior = fixture['prior_sessions']
    spec = dict(fixture['target_session'])
    extra_target_close = 200 if fixture['id'] == 'P18' else None
    if extra_target_close is not None:
        spec['close'], spec['C'] = 200, 1500
    sessions = [p.session(i, close=row['close'], excluded=row.get('ex', False)) for i, row in enumerate(prior)]
    target = p.session(len(prior), close=spec['close'])
    sessions.append(target)
    offsets = tuple(fixture['params']['grid_offsets_ns'])
    entries = []
    for session_obj, row in zip(sessions[:-1], prior):
        for c, token in enumerate(row['cells']):
            entries.append((session_obj, c, p.parse_bucket(token)))
    if spec['cells'] is not None:
        for c, token in enumerate(spec['cells']):
            entries.append((target, c, p.parse_bucket(token)))
    if fixture['id'] == 'P18':
        appended = p.session(len(prior) + 1, close=300)
        sessions.append(appended)
        entries.append((appended, 1, p.Cell(p.ROW, 12345, 150)))
    grid = p.grid_batch(offsets, target, input_id='grid', instrument_id=ENTITY)
    sessions_batch = p.sessions_batch(sessions, input_id='sessions', instrument_id=ENTITY)
    bars = p.bars_batch(entries, offsets, input_id='bars', instrument_id=ENTITY)
    params = fixture['params']
    timing = p.availability(target.open_ns + spec['C'], fixture['K'])
    pair = tuple(params['scale']) if 'scale' in params and 'pace.robust_z' in features else None
    config = p.volume_pace_config(target, timing=timing, profile=params['profile'], window=params['window'],
        min_history=params['min_history'], scale_pair=pair, evidence_limit=params['evidence_limit'])
    return config, (grid, sessions_batch, bars), (('grid', 'session'), ('sessions', 'session'), ('bars', 'bar')), sessions


def _mean_inputs(fixture: dict, features: tuple[str, ...]):
    from equity_feature_duckdb import robust_pace as p
    slots = list(fixture['slots'])
    appended = fixture.get('appended_rows', [])
    last = max([len(slots)] + [row['session'] for row in appended])
    sessions = [p.session(i) for i in range(last + 1)]
    cells = {i: _cell(token) for i, token in enumerate(slots)}
    cells[len(slots)] = _cell(fixture['target'])
    entries = [(ENTITY, sessions[i], cell) for i, cell in sorted(cells.items())]
    entries += [(ENTITY, sessions[row['session']], p.Cell(p.ROW, row['value'], row['known_at'])) for row in appended]
    series = p.daily_batch(entries, field='volume', scale=0, input_id='series', role='session')
    sessions_batch = p.sessions_batch(sessions, input_id='sessions', instrument_id=ENTITY)
    params = fixture['params']
    target = sessions[len(slots)]
    config = p.mean_ratio_config(target, timing=p.availability(max(s.close_ns for s in sessions), fixture['K']),
        numerator_window=params['numerator_window'], baseline_window=params['baseline_window'],
        numerator_inclusion=params['numerator_inclusion'], baseline_inclusion=params['baseline_inclusion'],
        evidence_limit=params['evidence_limit'])
    return config, (series, sessions_batch), (('series', 'session'), ('sessions', 'session')), sessions


BUILDERS = {'robust_history': _history_inputs, 'volume_pace_robust': _pace_inputs, 'volume_ratio_mean': _mean_inputs}


def declaration(role: str, batch) -> InputManifest:
    raw = json.dumps(asdict(batch), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('ascii')
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(raw).hexdigest(), batch.metadata.source.snapshot_id, state,
        InputBinding(role, batch.kind, batch.metadata))


def make_work(producer: str, config, features, batches_and_roles, sessions, label: str, scope: str, instrument: str):
    """Worker work item for one run; the adapter batches are bound to their contract roles by manifest."""
    from equity_feature_workers.equity_robust_pace import (make_robust_history_work, make_volume_pace_work,
        make_volume_ratio_mean_work)
    maker = {'robust_history': make_robust_history_work, 'volume_pace_robust': make_volume_pace_work,
        'volume_ratio_mean': make_volume_ratio_mean_work}[producer]
    batches, roles = batches_and_roles
    present = tuple((batch, role) for batch, role in zip(batches, roles) if batch is not None)
    decls = tuple(declaration(role[0], batch) for batch, role in present)
    governed = tuple(s for s in sessions if s.session_id == config.session.session_id)  # the library's target-only window
    return maker(config, features=features, inputs=decls, batches=tuple(batch for batch, _ in present),
        instrument_id=instrument, governed=governed, job_id='job-' + label, generation_id='generation-' + label, partition_id=ENTITY + '-S',
        destination_scope=scope)


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


# ---- refusal fixtures RF01-RF48 through the worker path ------------------------------------------------------------

_DAILY_PRICE = ('instrument_id', 'session_id', 'start_ns', 'end_ns', 'open', 'high', 'low', 'close', 'volume', 'known_at_ns')
_BAR_NAMES = ('instrument_id', 'session_id', 'start_ns', 'end_ns', 'volume', 'known_at_ns')
_REFERENCE_NAMES = ('instrument_id', 'session_id', 'reference_id', 'fact_kind', 'effective_start_ns', 'effective_end_ns',
                    'text', 'known_at_ns')
_REFUSAL_FEATURES = ('robust.median',)


def _adapter():
    from equity_feature_duckdb import robust_pace
    return robust_pace


def _raw(kind, names, rows, *, unit=None, input_id='raw', bounds=(0, 1), intervals=()):
    """A canonical batch from explicit rows; the shared validators run inside, so their refusals are build-stage."""
    p = _adapter()
    return p._batch(kind, names, rows, intervals, unit, input_id=input_id, bounds=bounds, count=len(rows))


def _history(config, series, sessions_batch, sessions, features=_REFUSAL_FEATURES, *, population=None, cohort=None,
        instrument=ENTITY):
    return dict(producer='robust_history', config=config, batches=(series, sessions_batch, population, cohort),
        features=features, sessions=sessions, instrument=instrument)


def _pace(config, grid, sessions_batch, bars, sessions, features=('pace.ratio',)):
    return dict(producer='volume_pace_robust', config=config, batches=(grid, sessions_batch, bars), features=features,
        sessions=sessions, instrument=ENTITY)


def _mean(config, series, sessions_batch, sessions, features=('mean.ratio',)):
    return dict(producer='volume_ratio_mean', config=config, batches=(series, sessions_batch), features=features,
        sessions=sessions, instrument=ENTITY)


def _base_history(*, field='volume', scale=0, window=5, min_history=5, quantile=None, min_population=None):
    """Valid history base: prior sessions 0..4 and target session 5, volume 10..60."""
    p = _adapter()
    sessions = [p.session(i) for i in range(6)]
    rows = [(ENTITY, sessions[i], p.Cell(p.ROW, v, 50)) for i, v in enumerate((10, 20, 30, 40, 50, 60))]
    series = p.daily_batch(rows, field=field, scale=scale, input_id='series', role='session')
    sessions_batch = p.sessions_batch(sessions, input_id='sessions', instrument_id=ENTITY)
    config = p.robust_history_config(sessions[5], timing=p.availability(sessions[5].close_ns, 100), field=field,
        scale=scale, window=window, min_history=min_history, quantile=quantile, min_population=min_population,
        evidence_limit=100)
    return sessions, series, sessions_batch, config


def _base_pace(*, grid_version=None, profile='bucket', scale_pair=None, window=5, min_history=5, cutoff=200):
    p = _adapter()
    prior = [p.session(i) for i in range(5)]
    target = p.session(5)
    sessions = prior + [target]
    offsets = (0, 100, 200, 300)
    rows = [(s, c, p.Cell(p.ROW, 100 * (i + 1) + c, 50)) for i, s in enumerate(prior) for c in range(3)]
    rows.append((target, 1, p.Cell(p.ROW, 450, 50)))
    kwargs = {} if grid_version is None else {'grid_version': grid_version}
    grid = p.grid_batch(offsets, target, input_id='grid', instrument_id=ENTITY, **kwargs)
    sessions_batch = p.sessions_batch(sessions, input_id='sessions', instrument_id=ENTITY)
    bars = p.bars_batch(rows, offsets, input_id='bars', instrument_id=ENTITY)
    config = p.volume_pace_config(target, timing=p.availability(target.open_ns + cutoff, 100), profile=profile,
        window=window, min_history=min_history, scale_pair=scale_pair, evidence_limit=100)
    return sessions, grid, sessions_batch, bars, config


def _base_mean(**params):
    p = _adapter()
    sessions = [p.session(i) for i in range(5)]
    rows = [(ENTITY, sessions[i], p.Cell(p.ROW, v, 50)) for i, v in enumerate((10, 20, 30, 40, 50))]
    series = p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session')
    sessions_batch = p.sessions_batch(sessions, input_id='sessions', instrument_id=ENTITY)
    values = dict(numerator_window=2, baseline_window=4, numerator_inclusion='prior_only',
        baseline_inclusion='prior_only', evidence_limit=100)
    values.update(params)
    config = p.mean_ratio_config(sessions[4], timing=p.availability(sessions[4].close_ns, 100), **values)
    return sessions, series, sessions_batch, config


def build_refusal(rid: str) -> dict:
    """The refusal's violation on a valid base. Raises the adapter's refusal when the batch itself is refused."""
    p = _adapter()
    sessions, series, sb, config = _base_history()
    if rid == 'RF01':  # two DAILY rows for one (instrument, session)
        rows = [(ENTITY, sessions[0], p.Cell(p.ROW, 1, 50)), (ENTITY, sessions[0], p.Cell(p.ROW, 2, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid == 'RF02':  # DAILY rows overlapping in time for one instrument
        rows = [(ENTITY, p.Session('0', 0, 300), p.Cell(p.ROW, 1, 50)), (ENTITY, p.Session('1', 100, 400), p.Cell(p.ROW, 2, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid == 'RF03':  # negative volume
        rows = [(ENTITY, sessions[0], p.Cell(p.ROW, -1, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid == 'RF04':  # zero volume with a non-null price
        rows = [(ENTITY, '0', 0, 300, 5, 5, 5, 5, 0, 50)]
        return _history(config, _raw(DataKind.DAILY, _DAILY_PRICE, rows, unit=p.price_unit(0), input_id='series',
            bounds=(0, 300)), sb, sessions, _REFUSAL_FEATURES)
    if rid == 'RF05':  # positive volume with a null price
        rows = [(ENTITY, '0', 0, 300, None, None, None, None, 7, 50)]
        return _history(config, _raw(DataKind.DAILY, _DAILY_PRICE, rows, unit=p.price_unit(0), input_id='series',
            bounds=(0, 300)), sb, sessions)
    if rid == 'RF06':  # a row ending after the market cutoff (forming session)
        rows = [(ENTITY, p.Session('5', 5000, 14000), p.Cell(p.ROW, 9, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid == 'RF07':  # row start not before end
        rows = [(ENTITY, '0', 300, 300, 1, 1, 1, 1, 1, 50)]
        return _history(config, _raw(DataKind.DAILY, _DAILY_PRICE, rows, unit=p.price_unit(0), input_id='series',
            bounds=(0, 300)), sb, sessions)
    if rid == 'RF08':  # DAILY price batch without a price unit
        rows = [(ENTITY, '0', 0, 300, 1, 1, 1, 1, 1, 50)]
        return _history(config, _raw(DataKind.DAILY, _DAILY_PRICE, rows, unit=None, input_id='series', bounds=(0, 300)),
            sb, sessions)
    if rid == 'RF15':  # volume 2^63 in an int64 column
        rows = [(ENTITY, sessions[0], p.Cell(p.ROW, 2 ** 63, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid == 'RF16':  # float in the int64 volume column
        rows = [(ENTITY, sessions[0], p.Cell(p.ROW, 1.5, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid == 'RF12':  # two reference rows with one (instrument, session, reference_id)
        rows = [(ENTITY, '0', 'x1:' + 'QQ', 'ef3.session.bounds', 0, 300, 'snap', 50),
                (ENTITY, '0', 'x1:' + 'QQ', 'ef3.session.bounds', 0, 300, 'snap', 50)]
        return _history(config, series, _raw(DataKind.REFERENCE, _REFERENCE_NAMES, rows, input_id='sessions',
            bounds=(0, 300)), sessions)
    if rid == 'RF13':  # decreasing grid edges
        return _pace(*_pace_with_grid([0, 200, 100, 300]))
    if rid == 'RF14':  # session bounds fact with start >= end
        rows = [(ENTITY, '0', 'x1:' + 'Qg', 'ef3.session.bounds', 500, 500, 'snap', 50)]
        return _history(config, series, _raw(DataKind.REFERENCE, _REFERENCE_NAMES, rows, input_id='sessions',
            bounds=(500, 500)), sessions)
    if rid in ('RF17', 'RF18', 'RF19', 'RF21', 'RF22', 'RF25', 'RF47', 'RF20'):
        kwargs = {'RF17': dict(window=0, min_history=1), 'RF18': dict(window=3, min_history=4),
            'RF19': dict(window=4097, min_history=1), 'RF20': dict(window=5, min_history=5, field='bogus'),
            'RF21': dict(window=5, min_history=5, quantile=(3, 2)), 'RF22': dict(window=5, min_history=5, quantile=(2, 4)),
            'RF25': dict(window=5, min_history=5, min_population=0), 'RF47': dict(window=5, min_history=5, quantile=(-1, 2))}[rid]
        field = kwargs.pop('field', 'volume')
        config = p.robust_history_config(sessions[5], timing=p.availability(sessions[5].close_ns, 100), field=field,
            scale=0, evidence_limit=100, **kwargs)
        return _history(config, series, sb, sessions)
    if rid in ('RF23', 'RF24', 'RF29'):  # missing parameter, unknown parameter, unsupported policy label
        params = {'RF23': [('policy_version', 'ef3-robust1'), ('field', 'volume'), ('window', 5), ('evidence_limit', 100)],
            'RF24': [('policy_version', 'ef3-robust1'), ('field', 'volume'), ('window', 5), ('min_history', 5),
                     ('evidence_limit', 100), ('extra', 1)],
            'RF29': [('policy_version', 'ef3-robust9'), ('field', 'volume'), ('window', 5), ('min_history', 5),
                     ('evidence_limit', 100)]}[rid]
        config = _raw_config(sessions[5], 'ef3.robust_history', 'ef3-robust-history-v1', params, 5, p.price_unit(0))
        return _history(config, series, sb, sessions)
    if rid == 'RF30':  # a feature outside the headers
        return _history(config, series, sb, sessions, ('robust.foo',))
    if rid == 'RF26':  # scale denominator 0
        sessions_p, grid, sbp, bars, _ = _base_pace()
        config = p.volume_pace_config(sessions_p[5], timing=p.availability(sessions_p[5].open_ns + 200, 100),
            profile='bucket', window=5, min_history=5, scale_pair=(3, 0), evidence_limit=100)
        return _pace(config, grid, sbp, bars, sessions_p, ('pace.robust_z',))
    if rid == 'RF27':  # profile outside cumulative, bucket
        sessions_p, grid, sbp, bars, _ = _base_pace()
        config = _raw_config(sessions_p[5], 'ef3.volume_pace_robust', 'ef3-volume-pace-robust-v1',
            [('policy_version', 'ef3-pace1'), ('profile', 'weekly'), ('window', 5), ('min_history', 5),
             ('grid_version', 'ef3-grid1'), ('evidence_limit', 100)], 5, p.price_unit(0))
        return _pace(config, grid, sbp, bars, sessions_p)
    if rid == 'RF28':  # grid with a single edge
        sessions_p, _, sbp, bars, config = _base_pace()
        grid = p.grid_batch([0], sessions_p[5], input_id='grid', instrument_id=ENTITY)
        return _pace(config, grid, sbp, bars, sessions_p)
    if rid == 'RF40':  # first grid edge not 0
        sessions_p, _, sbp, bars, config = _base_pace()
        grid = p.grid_batch([10, 100, 200, 300], sessions_p[5], input_id='grid', instrument_id=ENTITY)
        return _pace(config, grid, sbp, bars, sessions_p)
    if rid == 'RF39':  # config grid_version differs from the grid text
        sessions_p, grid, sbp, bars, config = _base_pace(grid_version='ef3-grid2')
        return _pace(config, grid, sbp, bars, sessions_p)
    if rid == 'RF37':  # bar starting off every grid edge
        sessions_p, grid, sbp, _, config = _base_pace()
        rows = [(ENTITY, '5', sessions_p[5].open_ns + 50, sessions_p[5].open_ns + 100, 9, 50)]
        bad = _raw(DataKind.BAR, _BAR_NAMES, rows, unit=p.price_unit(0), input_id='bars', bounds=(5000, 5100))
        return _pace(config, grid, sbp, bad, sessions_p)
    if rid == 'RF38':  # bar for a bucket beyond the actual session close
        sessions_p, grid, _, _, config = _base_pace()
        short = p.session(0, close=200)
        sessions_p = [short] + sessions_p[1:]
        sbp = p.sessions_batch(sessions_p, input_id='sessions', instrument_id=ENTITY)
        bars = p.bars_batch([(p.session(0), 2, p.Cell(p.ROW, 5, 50))], (0, 100, 200, 300), input_id='bars',
            instrument_id=ENTITY)
        return _pace(config, grid, sbp, bars, sessions_p)
    if rid == 'RF31':  # update, restore or merge requested
        return dict(capability='update', features=('robust.median',))
    if rid == 'RF11':  # roles with different namespaces
        series_other = p.daily_batch([(ENTITY, sessions[0], p.Cell(p.ROW, 1, 50))], field='volume', scale=0,
            input_id='series', role='session')
        series_other = _replace_namespace(series_other, 'OTHER')
        return _history(config, series_other, sb, sessions)
    if rid == 'RF10':  # series and cohort_values with different adjustment basis
        return _history(config, _replace_adjustment(series), sb, sessions)
    if rid == 'RF09':  # series and cohort_values with different price scale or currency
        return _cohort_with_series(scale_series=1, scale_cohort=2)
    if rid == 'RF41':  # exclusion fact supplied to the mean producer
        ss = [p.session(i, excluded=(i == 1)) for i in range(5)]
        rows = [(ENTITY, ss[i], p.Cell(p.ROW, 5, 50)) for i in range(5)]
        return _mean(_base_mean()[3], p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'),
            p.sessions_batch(ss, input_id='sessions', instrument_id=ENTITY), ss)
    if rid == 'RF42':  # series instrument differs from the target entity
        rows = [('OTHER', sessions[0], p.Cell(p.ROW, 1, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid == 'RF43':  # series price scale differs from the configured price unit
        cfg = p.robust_history_config(sessions[5], timing=p.availability(sessions[5].close_ns, 100), field='close',
            scale=2, window=5, min_history=5, evidence_limit=100)
        rows = [(ENTITY, sessions[i], p.Cell(p.ROW, 5, 50)) for i in range(6)]
        return _history(cfg, p.daily_batch(rows, field='close', scale=3, input_id='series', role='session'), sb, sessions)
    if rid == 'RF45':  # exclusion fact on the target session
        ss = sessions[:5] + [p.Session('5', sessions[5].open_ns, sessions[5].close_ns, True, 50)]
        return _history(config, series, p.sessions_batch(ss, input_id='sessions', instrument_id=ENTITY), ss)
    if rid == 'RF46':  # exclusion fact with an empty reason label
        rows = [(ENTITY, '2', 'x1:' + 'ZXg', 'ef3.session.excluded', 2000, 2300, '', 50)]
        return _history(config, series, _raw(DataKind.REFERENCE, _REFERENCE_NAMES, rows, input_id='sessions',
            bounds=(2000, 2300)), sessions)
    if rid == 'RF48':  # two exclusion facts on one session with distinct reference ids
        ss = [p.session(i, excluded=(i == 2)) for i in range(6)]
        base = p.sessions_batch(ss, input_id='sessions', instrument_id=ENTITY)
        rows = [tuple(column.values[i] for column in base.columns) for i in range(base.row_count)]
        rows.append((ENTITY, '2', 'x1:' + 'YTI', 'ef3.session.excluded', 2000, 2300, 'b', 50))
        return _history(config, series, _raw(DataKind.REFERENCE, _REFERENCE_NAMES, rows, input_id='sessions',
            bounds=(0, 5300)), ss)
    if rid == 'RF44':  # bounds facts overlapping
        rows = [(ENTITY, '0', 'x1:' + 'Qg', 'ef3.session.bounds', 0, 300, 'snap', 50),
                (ENTITY, '1', 'x1:' + 'Qg', 'ef3.session.bounds', 200, 500, 'snap', 50)]
        return _history(config, series, _raw(DataKind.REFERENCE, _REFERENCE_NAMES, rows, input_id='sessions',
            bounds=(0, 500)), sessions)
    if rid == 'RF35':  # exclusion fact for a session with no bounds fact
        rows = [(ENTITY, '9', 'x1:' + 'WFg', 'ef3.session.excluded', 9000, 9300, 'a', 50)]
        return _history(config, series, _raw(DataKind.REFERENCE, _REFERENCE_NAMES, rows, input_id='sessions',
            bounds=(9000, 9300)), sessions)
    if rid == 'RF36':  # series row whose session bounds differ from the bounds fact
        rows = [(ENTITY, p.Session('5', sessions[5].open_ns, sessions[5].close_ns - 1), p.Cell(p.ROW, 60, 50))]
        return _history(config, p.daily_batch(rows, field='volume', scale=0, input_id='series', role='session'), sb, sessions)
    if rid in ('RF32', 'RF33', 'RF34'):
        return _cohort_refusal(rid)
    raise KeyError(rid)


def _raw_config(target, identity, algorithm, params, window, unit):
    """A configuration with an explicit parameter list (the adapter's own envelope, no defaults added)."""
    p = _adapter()
    from equity_feature_contracts import Parameter
    return p._named(identity, algorithm, tuple(Parameter(name, value) for name, value in params), window, target,
        p.availability(target.close_ns, 100), unit)


def _pace_with_grid(offsets):
    sessions, _, sbp, bars, config = _base_pace()
    p = _adapter()
    grid = p.grid_batch(offsets, sessions[5], input_id='grid', instrument_id=ENTITY)
    return config, grid, sbp, bars, sessions


def _replace_namespace(batch, namespace):
    import dataclasses
    return dataclasses.replace(batch, metadata=dataclasses.replace(batch.metadata, namespace=namespace))


def _replace_adjustment(batch):
    import dataclasses
    from equity_feature_contracts import AdjustmentSpec
    return dataclasses.replace(batch, metadata=dataclasses.replace(batch.metadata,
        adjustment=AdjustmentSpec(basis='adjusted', policy_version='adj-v1')))


def _cohort_with_series(*, scale_series, scale_cohort):
    p = _adapter()
    target = p.session(0)
    members = {'A': 10100, 'B': 9950}
    cohort = p.daily_batch([(m, target, p.Cell(p.ROW, v, 50)) for m, v in members.items()], field='close',
        scale=scale_cohort, input_id='cohort', role='member')
    series = p.daily_batch([(ENTITY, target, p.Cell(p.ROW, 10100, 50))], field='close', scale=scale_series,
        input_id='series', role='session')
    population = p.population_batch(list(members), target, input_id='population')
    sessions_batch = p.sessions_batch([target], input_id='sessions', instrument_id='A')
    config = p.robust_history_config(target, timing=p.availability(target.close_ns, 100), field='close', scale=scale_cohort,
        window=1, min_history=1, min_population=1, evidence_limit=100)
    return _history(config, series, sessions_batch, [target], ('robust.median', 'cohort.rank'),
        population=population, cohort=cohort, instrument='A')


def _cohort_refusal(rid):
    p = _adapter()
    target = p.session(0)
    members = {'A': 10100, 'B': 9950}
    if rid == 'RF32':  # target instrument is not a declared population member
        population = p.population_batch(['B'], target, input_id='population')
        cohort = p.daily_batch([('B', target, p.Cell(p.ROW, 9950, 50))], field='close', scale=2, input_id='cohort', role='member')
        instrument = 'A'
    elif rid == 'RF33':  # cohort value row for a non-member
        population = p.population_batch(['A'], target, input_id='population')
        cohort = p.daily_batch([('A', target, p.Cell(p.ROW, 10100, 50)), ('Z', target, p.Cell(p.ROW, 1, 50))],
            field='close', scale=2, input_id='cohort', role='member')
        instrument = 'A'
    else:  # RF34: cohort value row whose session is not the target session
        population = p.population_batch(['A', 'B'], target, input_id='population')
        other = p.Session('1', 0, 300)  # same bounds window, a session id other than the target's
        cohort = p.daily_batch([('A', target, p.Cell(p.ROW, 10100, 50)), ('B', other, p.Cell(p.ROW, 1, 50))],
            field='close', scale=2, input_id='cohort', role='member')
        instrument = 'A'
    config = p.robust_history_config(target, timing=p.availability(target.close_ns, 100), field='close', scale=2,
        window=1, min_history=1, min_population=1, evidence_limit=100)
    sessions_batch = p.sessions_batch([target], input_id='sessions', instrument_id=instrument)
    series = None
    return _history(config, series, sessions_batch, [target], ('cohort.rank',), population=population, cohort=cohort,
        instrument=instrument)


def _run_one(rid: str, fixture: dict) -> dict:
    """Build (adapter), admit (worker), calculate (worker) and record the library's own code beside the worker's."""
    from equity_feature_contracts import ContractError, EntityKey
    from equity_feature_workers.commands import CommandError
    from equity_features import ef3
    from equity_feature_workers.equity_robust_pace import (make_robust_history_work, make_volume_pace_work,
        make_volume_ratio_mean_work)
    record = {'id': rid, 'contract_code': fixture['error_code'], 'raised_by': fixture['raised_by'], 'stage': None,
        'library_code': None, 'worker_code': None}
    try:
        spec = build_refusal(rid)
    except ContractError as error:
        record.update(stage='adapter_build', library_code=error.code.value)
        return _finish(record)
    if 'capability' in spec:
        try:
            ef3.require_ef3_robust_capability('robust.median', spec['capability'])
            record.update(stage='library_capability', library_code=None)
        except ContractError as error:
            record.update(stage='library_capability', library_code=error.code.value)
        return _finish(record)
    producer = spec['producer']
    maker = {'robust_history': make_robust_history_work, 'volume_pace_robust': make_volume_pace_work,
        'volume_ratio_mean': make_volume_ratio_mean_work}[producer]
    present = tuple((batch, role) for batch, role in zip(spec['batches'], _ROLES[producer]) if batch is not None)
    decls = tuple(declaration(role[0], batch) for batch, role in present)
    batches = tuple(batch for batch, _ in present)
    config, features, instrument = spec['config'], spec['features'], spec['instrument']
    governed = tuple(s for s in spec['sessions'] if s.session_id == config.session.session_id)
    entity = EntityKey(instrument, config.session.session_id)
    try:
        item = maker(config, features=features, inputs=decls, batches=batches, instrument_id=instrument,
            governed=governed, job_id='job-' + rid, generation_id='generation-' + rid, partition_id='refusal',
            destination_scope='ef3-refusal')
    except CommandError as error:
        record.update(stage='worker_admission', worker_code=error.code.value, worker_contract_code=error.contract_code)
        record['library_code'] = _library_code(ef3, spec, entity)
        return _finish(record)
    except ContractError as error:
        record.update(stage='worker_admission', library_code=error.code.value)
        return _finish(record)
    try:
        item.calculate(item.task, item.batches)
        record.update(stage='no_refusal')
    except CommandError as error:
        record.update(stage='compute', worker_code=error.code.value, worker_contract_code=error.contract_code)
        record['library_code'] = _library_code(ef3, spec, entity)
    return _finish(record)


_ROLES = {'robust_history': (('series', 'session'), ('sessions', 'session'), ('population', 'session'),
    ('cohort_values', 'member')), 'volume_pace_robust': (('grid', 'session'), ('sessions', 'session'), ('bars', 'bar')),
    'volume_ratio_mean': (('series', 'session'), ('sessions', 'session'))}


def _library_code(ef3, spec, entity):
    """The library's own refusal code for the same batches and configuration (the worker does not return it)."""
    from equity_feature_contracts import ContractError
    try:
        ef3.compute_ef3_robust(spec['batches'], spec['config'], entity=entity, features=spec['features'])
        return None
    except ContractError as error:
        return error.code.value


def _finish(record: dict) -> dict:
    expected = record['contract_code']
    record['matched'] = expected.lower() == record['library_code']
    return record


def run_refusals(document: dict) -> list:
    """All 48 refusal fixtures in contract order; each row names the stage, the contract code and the codes raised."""
    fixtures = {row['id']: row for row in document['refusal_fixtures']}
    return [_run_one(rid, fixtures[rid]) for rid in sorted(fixtures, key=lambda key: int(key[2:]))]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--case', help='contract result fixture id, e.g. H01, C10b, P09, R16')
    parser.add_argument('--refusals', action='store_true', help='drive the 48 refusal fixtures through the worker')
    parser.add_argument('--output-db', type=Path)
    parser.add_argument('--html', type=Path)
    parser.add_argument('--run-id', default='ef3-robust')
    parser.add_argument('--contract', type=Path, default=None, help='contract JSON; default is the core repository copy')
    args = parser.parse_args(argv)
    document = contract_document(args.contract)
    if args.refusals:
        rows = run_refusals(document)
        print(json.dumps(rows, indent=1, sort_keys=True))
        return 0 if all(row['matched'] for row in rows) else 1
    if not args.case or not args.output_db or not args.html:
        parser.error('--case, --output-db and --html are required for a result case')
    if (not args.output_db.is_absolute() or not args.html.is_absolute()
            or args.output_db.resolve() == args.html.resolve() or args.output_db.exists() or args.html.exists()
            or not args.html.parent.is_dir() or not args.output_db.parent.is_dir()
            or not 1 <= len(args.run_id) <= 64 or not args.run_id.isascii()):
        parser.error('Require new distinct absolute output paths, existing parents and bounded ASCII run identity')
    from equity_feature_duckdb.ef1 import separate_output_path
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_workers.ef1_runner import run_ef3_robust_history, run_ef3_volume_pace, run_ef3_volume_ratio_mean

    fixtures = fixtures_by_id(document)
    if args.case not in fixtures or len(fixtures) != RESULT_COUNT:
        parser.error('Contract result fixtures differ from the example case list')
    fixture = fixtures[args.case]
    producer = PRODUCER[args.case[0]]
    runner = {'robust_history': run_ef3_robust_history, 'volume_pace_robust': run_ef3_volume_pace,
        'volume_ratio_mean': run_ef3_volume_ratio_mean}[producer]
    scope = 'ef3-' + producer.replace('_', '-') + '-synthetic'
    runs = runs_for(fixture)
    items, plans = [], []
    for number, run in enumerate(runs):
        config, batches, roles, sessions = BUILDERS[producer](fixture, run['features'])
        label = '%s-%s-%d' % (args.run_id, args.case, number)
        instrument = fixture['target_member'] if args.case.startswith('C') else ENTITY
        item = make_work(producer, config, run['features'], (batches, roles), sessions, label, scope, instrument)
        items.append(item)
    limit = fixture['params']['evidence_limit']
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
    html = ('<!doctype html><html lang="en"><meta charset="utf-8"><title>EF3 ' + escape(producer) + '</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:2rem auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        '<h1>EF3 ' + escape(producer.replace('_', ' ')) + ' (contract fixture ' + escape(args.case) + ')</h1>'
        '<p>' + escape(fixture['description']) + '. Supplied-input adapter  ->  worker  ->  pure library  ->  separate DuckDB sink. '
        'Contract-literal match (value by acceptance rule, status, reasons, counts, witnesses and joint evidence), '
        'SDK readback, idempotent replay and close/reopen verified. Runs: ' + str(len(runs)) + '.</p>'
        '<p>Selected outputs across runs: ' + escape(outputs) + '.</p>'
        '<p>Task ' + escape(items[0].task.task_sha256) + '; content ' + escape(rows[0].output.receipt.content_sha256)
        + '.</p><h2>Complete canonical results (one line per run)</h2><pre>' + escape(sdk.decode('ascii')) + '</pre>'
        '<p>The worker orchestrates only; every median, MAD, quantile, rank, ratio, z-score and mean value is the library result. '
        'No remote assets or JavaScript.</p></html>')
    with args.html.open('x', encoding='utf-8', newline='\n') as target:
        target.write(html)
    print(json.dumps({'verified_readback': True, 'verified_reopen': True, 'contract_literal_match': True,
        'runs': len(runs), 'task_sha256': items[0].task.task_sha256,
        'content_sha256': rows[0].output.receipt.content_sha256}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
