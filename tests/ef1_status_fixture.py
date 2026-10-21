"""Independent original S01-S03 rows and literal goldens; no I/O/helper oracle."""
import hashlib
import json
import pickle

import equity_feature_contracts as c


ROLES = ('calendar_provenance', 'status_states', 'status_prefix', 'status_validity')
NAMES = ('reference_id', 'fact_kind', 'effective_start_ns', 'effective_end_ns', 'text', 'known_at_ns')
S01 = (
    ('s1:t:ZQ:0000000000000000000:c2VlZA', 'ef1.status.trading', 8, None, 'active', 8),
    ('s1:t:ZQ:0000000000000000001:aGFsdA', 'ef1.status.trading', 12, None, 'halted', 12),
    ('s1:t:ZQ:0000000000000000002:cmVzdW1l', 'ef1.status.trading', 16, None, 'active', 16),
)
S02 = (('s1:s:ZQ:0000000000000000000:c2VlZA', 'ef1.status.ssr', 5, None, 'on', 5),)
S03 = (
    ('s1:s:ZQ:0000000000000000000:YQ', 'ef1.status.ssr', 5, None, 'on', 5),
    ('s1:s:ZQ:0000000000000000001:Yg', 'ef1.status.ssr', 5, None, 'off', 5),
)
# case,C,K,max_age,state,age,admission,reason,factual_state_quality,factual_age_quality
GOLDENS = (
    ('S01', 16, 16, None, 'halted', 4, 'ready', 'none', ('available', ()), ('available', ())),
    ('S01', 17, 17, None, 'active', 1, 'ready', 'none', ('available', ()), ('available', ())),
    ('S01', 17, 15, None, None, None, 'unavailable', 'future_knowledge',
        ('missing_input', ('future_knowledge',)), ('missing_input', ('future_knowledge',))),
    ('S02', 16, 16, None, 'on', 11, 'ready', 'none', ('available', ()), ('available', ())),
    ('S02', 16, 16, 10, None, 11, 'unavailable', 'stale_state',
        ('missing_input', ('ineligible',)), ('available', ())),
    ('S02', 15, 15, 10, 'on', 10, 'ready', 'none', ('available', ()), ('available', ())),
    ('S02', 17, 17, None, None, None, 'unavailable', 'incomplete_status_prefix',
        ('incomplete_coverage', ('governed_gap',)), ('incomplete_coverage', ('governed_gap',))),
    ('S03', 6, 6, None, 'off', 1, 'ready', 'none', ('available', ()), ('available', ())),
)


def batch(input_id, rows):
    columns = {'instrument_id': tuple('X' for _ in rows), 'session_id': tuple('S' for _ in rows)}
    columns.update({name: tuple(row[index] for row in rows) for index, name in enumerate(NAMES)})
    value = c.CanonicalBatch(c.DataKind.REFERENCE,
        tuple(c.Column(name, values) for name, values in columns.items()),
        c.BatchMetadata('SYN', c.SourceBinding('F', 'R1', 'ef1-reference-map1', input_id),
            c.Coverage(len(rows), len(rows), True), None))
    c.validate_batch(value)
    return value


def fixture(case='S01', *, cutoff=None, knowledge=None, max_age=None, missing=(), empty=(),
        prefix_end=None, validity_end=None, mode='known_at', seed=True):
    from equity_features.ef1 import build_ef1_config

    if case not in ('S01', 'S02', 'S03'):
        raise ValueError('closed independent case')
    dimension, code, rows = ('trading', 't', S01) if case == 'S01' else (
        'ssr', 's', S02 if case == 'S02' else S03)
    cutoff = (17 if case == 'S01' else 16 if case == 'S02' else 6) if cutoff is None else cutoff
    knowledge = cutoff if knowledge is None else knowledge
    calendar = dict(calendar_id='C01', revision='R1', timezone='UTC', target_date='2026-01-02',
        sessions=[['S', '2026-01-02', 10, 30]],
        segments=[['premarket', 0, 10], ['regular', 10, 30], ['postmarket', 30, 40]],
        scheduled_close_ns=40, early_close=True)
    config = build_ef1_config(dict(version='ef1-context1', producer='status', calendar=calendar,
        selection=dict(dimension=dimension, epoch='e', seed_reference_id=rows[0][0] if seed else None,
            max_age_ns=max_age)),
        session=c.SessionSpec('SYN', 'S', 10, 30, 'UTC', scheduled_close_ns=40, early_close=True),
        window=c.WindowSpec(1, 'S', ('S',), 'completed_eod'),
        availability=c.AvailabilitySpec(cutoff, knowledge, 20, mode,
            None if mode == 'known_at' else 'independent reconstruction'),
        price_unit=None, adjustment=c.AdjustmentSpec())
    raw = json.dumps(calendar, sort_keys=True, separators=(',', ':'), ensure_ascii=True,
        allow_nan=False).encode('ascii')
    proof = batch('calendar-proof', (('c1:' + hashlib.sha256(raw).hexdigest(),
        'ef1.calendar.provenance', 0, 40, 'supplied_schedule', 5),))
    start = rows[0][2]
    prefix_end = (16 if case == 'S02' else 20) if prefix_end is None else prefix_end
    validity_end = (17 if case == 'S02' else 21) if validity_end is None else validity_end
    slots = [proof, batch('st', rows),
        batch('prefix', (('p1:' + code + ':ZQ:c3Q:cHJlZg', 'ef1.status.prefix_complete',
            start, prefix_end, code, start),)),
        batch('validity', (('p1:' + code + ':ZQ:c3Q:dmFsaWQ', 'ef1.status.validity',
            start, validity_end, code, start),))]
    for index in empty:
        original = slots[index]
        slots[index] = c.CanonicalBatch(original.kind,
            tuple(c.Column(column.name, ()) for column in original.columns),
            c.BatchMetadata(original.metadata.namespace, original.metadata.source, c.Coverage(0, 0, True), None))
    for index in missing:
        slots[index] = None
    return config, tuple(slots)


def declarations(slots):
    from equity_feature_workers import InputManifest

    return tuple(InputManifest(role,
        hashlib.sha256(('missing-' + role).encode('ascii') if value is None
            else pickle.dumps(value, protocol=5)).hexdigest(),
        'R1' if value is None else value.metadata.source.snapshot_id,
        'missing' if value is None else 'partial' if not value.metadata.coverage.complete
            else 'empty' if value.row_count == 0 else 'complete',
        None if value is None else c.InputBinding(role, value.kind, value.metadata))
        for role, value in zip(ROLES, slots))


def make(config, slots, *, name='independent'):
    from equity_feature_workers.equity_status import make_ef1_status_work

    return make_ef1_status_work(config, inputs=declarations(slots),
        batches=tuple(value for value in slots if value is not None), instrument_id='X',
        job_id='status-job-' + name, generation_id='status-generation-' + name,
        partition_id='X-S', destination_scope='ef1-status-independent')


def work(case='S01', *, name='independent', **options):
    return make(*fixture(case, **options), name=name)
