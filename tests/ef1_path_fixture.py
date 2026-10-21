"""Literal reviewed synthetic populations; independent of I/O fixture factories."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration

ORACLE = json.loads(Path(__file__).with_name('ef1_path_oracle.json').read_text(encoding='utf-8'))
CASES = {case['id']: case for case in ORACLE['fixtures']}


def case_record(name):
    raw = CASES[name]
    result = dict(case_record(raw['based_on']), **raw) if 'based_on' in raw else dict(raw)
    # A derived no-evidence outcome replaces its parent's witness expectations.
    # Explicit per-case witness maps remain authoritative, including empty maps.
    if raw.get('evidence_count') == 0 and 'witnesses' not in raw:
        result['witnesses'] = {}
    return result


def fixture(name='G01', *, record=None, mode='known_at'):
    from equity_features.ef1 import build_ef1_config

    row = case_record(name) if record is None else record
    profile = ORACLE['profiles']
    gap = row['producer'] == 'path_gap'
    sampling = row.get('sampling', 'eligible_trade')
    calendar = profile['gap_calendar' if gap else 'drive_calendar']
    cutoff = row['C']
    selection = dict(sampling=sampling)
    if not gap:
        selection['duration_ns'] = row['duration_ns']
    timing = c.AvailabilitySpec(cutoff, cutoff, cutoff, mode,
        None if mode == 'known_at' else 'synthetic-reconstruction')
    config = build_ef1_config(dict(version='ef1-context1', producer=row['producer'],
        calendar=calendar, selection=selection),
        session=c.SessionSpec('SYN', 'S', 10, 30, 'UTC', scheduled_close_ns=30),
        window=c.WindowSpec(2 if gap else 1, 'S', ('P', 'S') if gap else ('S',), 'completed_eod'),
        availability=timing, price_unit=c.PriceUnit(0, 'USD'), adjustment=c.AdjustmentSpec())

    def batch(kind, columns, rows, source_key, coverage, scope=None):
        return c.CanonicalBatch(kind,
            tuple(c.Column(key, tuple(values)) for key, values in zip(columns, zip(*rows)))
            if rows else tuple(c.Column(key, ()) for key in columns),
            c.BatchMetadata('SYN', c.SourceBinding(**profile[source_key]), c.Coverage(*coverage),
                None if kind is c.DataKind.REFERENCE else c.PriceUnit(0, 'USD'), scope=scope))

    encoded = json.dumps(calendar, ensure_ascii=True, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode('ascii')
    proof_row = ['X', 'S', 'c1:' + hashlib.sha256(encoded).hexdigest(),
        'ef1.calendar.provenance', -10 if gap else 10, 30, 'supplied_schedule', -10]
    proof = batch(c.DataKind.REFERENCE, profile['calendar_columns'], [proof_row],
        'source_calendar', [1, 1, True])
    path = None
    if row.get('path', 'present') is not None:
        is_trade = sampling == 'eligible_trade'
        path = batch(c.DataKind.TRADE if is_trade else c.DataKind.BAR,
            row.get('columns', profile['trade_columns' if is_trade else 'bar_columns']),
            row['rows'], 'source_market', row['path_coverage'],
            c.InputScope(10, cutoff if gap else min(cutoff, 15), 'supplied_path'))
    prior = None
    if gap and (row.get('prior') is not None or 'prior_row' in row):
        prior = batch(c.DataKind.DAILY, profile['daily_columns'],
            [row.get('prior_row', profile.get(row.get('prior')))], 'source_prior', [1, 1, True],
            c.InputScope(-10, 0, 'supplied_path'))
    return config, (proof, path, prior) if gap else (proof, path)


def replace_column(batch, name, values):
    return replace(batch, columns=tuple(c.Column(column.name, tuple(values))
        if column.name == name else column for column in batch.columns))


def work(name='G01', *, config=None, slots=None, record=None, mode='known_at', identity=None):
    from equity_feature_workers.equity_path_gap import make_ef1_path_gap_work
    from equity_feature_workers.equity_path_drive import make_ef1_path_drive_work

    original_config, original_slots = fixture(name, record=record, mode=mode)
    config = original_config if config is None else config
    slots = original_slots if slots is None else slots
    gap = config.identity == 'ef1.path_gap'
    roles = ('calendar_provenance', 'path', 'prior_close') if gap else ('calendar_provenance', 'path')
    builder = make_ef1_path_gap_work if gap else make_ef1_path_drive_work
    key = (identity or name) + '-' + mode
    return builder(config, inputs=tuple(declaration(role, value) for role, value in zip(roles, slots)),
        batches=tuple(value for value in slots if value is not None), instrument_id='X',
        job_id='path-job-' + key, generation_id='path-generation-' + key,
        partition_id='X-S', destination_scope='ef1-path-independent')
