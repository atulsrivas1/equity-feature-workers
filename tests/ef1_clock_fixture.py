"""Independent hand-authored C01 calendar/proof; no I/O fixture/oracle reuse."""
import hashlib
import json
import pickle

import equity_feature_contracts as c


def fixture(*, cutoff=20, proof_known=5, missing=False):
    from equity_features.ef1 import build_ef1_config

    calendar = dict(calendar_id='C01', revision='R1', timezone='UTC', target_date='2026-01-02',
        sessions=[['S', '2026-01-02', 10, 30]],
        segments=[['premarket', 0, 10], ['regular', 10, 30], ['postmarket', 30, 40]],
        scheduled_close_ns=40, early_close=True)
    config = build_ef1_config(dict(version='ef1-context1', producer='clock',
        calendar=calendar, selection={}),
        session=c.SessionSpec('SYN', 'S', 10, 30, 'UTC', scheduled_close_ns=40, early_close=True),
        window=c.WindowSpec(1, 'S', ('S',), 'completed_eod'), availability=c.AvailabilitySpec(cutoff, cutoff, 40),
        price_unit=None, adjustment=c.AdjustmentSpec())
    if missing:
        return config, None
    encoded = json.dumps(calendar, ensure_ascii=True, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode('ascii')
    data = dict(instrument_id=('X',), session_id=('S',),
        reference_id=('c1:' + hashlib.sha256(encoded).hexdigest(),),
        fact_kind=('ef1.calendar.provenance',), text=('supplied_schedule',),
        effective_start_ns=(0,), effective_end_ns=(40,), known_at_ns=(proof_known,))
    proof = c.CanonicalBatch(c.DataKind.REFERENCE, tuple(c.Column(name, values) for name, values in data.items()),
        c.BatchMetadata('SYN', c.SourceBinding('F', 'R1', 'ef1-reference-map1', 'C01-proof'),
            c.Coverage(1, 1, True), None))
    c.validate_batch(proof)
    return config, proof


def declaration(proof):
    from equity_feature_workers import InputManifest

    if proof is None:
        return InputManifest('calendar_provenance', hashlib.sha256(b'calendar-missing').hexdigest(),
            'R1', 'missing', None)
    coverage = proof.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest('calendar_provenance', hashlib.sha256(pickle.dumps(proof, protocol=5)).hexdigest(),
        proof.metadata.source.snapshot_id, state,
        c.InputBinding('calendar_provenance', proof.kind, proof.metadata))


def work(*, name='C01', **kwargs):
    from equity_feature_workers.equity_clock import make_ef1_clock_work

    config, proof = fixture(**kwargs)
    return make_ef1_clock_work(config, inputs=(declaration(proof),),
        batches=() if proof is None else (proof,), instrument_id='X',
        job_id='clock-job-' + name, generation_id='clock-generation-' + name,
        partition_id='X-S', destination_scope='ef1-clock-independent')
