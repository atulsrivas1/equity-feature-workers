"""Independent toy anchor population; no I/O fixture factory or worker oracle."""
import hashlib
import json
import pickle

import equity_feature_contracts as c


DEFINITION = dict(anchor_id='A', reset_id='R', start_ns=10,
                  selection_policy_id='supplied', revision='revision1')


def fixture(mode='trades', *, empty=False, notional=True, proof_known=10):
    from equity_features.ef1 import build_ef1_config

    context = dict(version='ef1-context1', producer='anchor', calendar=None,
        selection=dict(mode=mode, definition=DEFINITION.copy()))
    config = build_ef1_config(context, session=c.SessionSpec('SYN', 'S', 0, 30, 'supplied'),
        window=c.WindowSpec(1, 'S', ('S',), 'completed_eod'), availability=c.AvailabilitySpec(20, 20, 20),
        price_unit=c.PriceUnit(2, 'USD'), adjustment=c.AdjustmentSpec())
    if mode == 'trades':
        # 10.00*2 + 13.00*1 = 33.00; V=3; exact VWAP11.00.
        columns = dict(event_ns=(10, 15), order_key=(0, 1), event_id=('e1', 'e2'),
            eligible=(True, True), price=(1000, 1300), size=(2, 1), known_at_ns=(10, 15))
        kind = c.DataKind.TRADE
        count = 2
    else:
        columns = dict(start_ns=(10,), end_ns=(20,), open=(1000,), high=(1300,),
            low=(1000,), close=(1300,), volume=(3,),
            actual_notional=(3300 if notional else None,), known_at_ns=(20,))
        kind = c.DataKind.BAR
        count = 1
    columns = dict(instrument_id=('X',) * count, session_id=('S',) * count, **columns)
    if empty:
        columns = {key: () for key in columns}
        count = 0
    market = c.CanonicalBatch(kind, tuple(c.Column(key, value) for key, value in columns.items()),
        c.BatchMetadata('SYN', c.SourceBinding('fixture', 'revision1', 'ef1-reference-map1', 'market-' + mode),
            c.Coverage(count, count, True), c.PriceUnit(2, 'USD'), c.AdjustmentSpec(),
            scope=c.InputScope(10, 20, 'synthetic-explicit-eligibility')))
    definition = json.dumps(DEFINITION, ensure_ascii=True, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode('ascii')
    proof_columns = dict(instrument_id=('X',), session_id=('S',),
        reference_id=('a1:' + hashlib.sha256(definition).hexdigest(),),
        fact_kind=('ef1.anchor.provenance',), effective_start_ns=(10,),
        effective_end_ns=(None,), text=('supplied_anchor',), known_at_ns=(proof_known,))
    proof = c.CanonicalBatch(c.DataKind.REFERENCE,
        tuple(c.Column(key, value) for key, value in proof_columns.items()),
        c.BatchMetadata('SYN', c.SourceBinding('fixture-anchor', 'revision1', 'ef1-reference-map1', 'proof'),
            c.Coverage(1, 1, True), None))
    c.validate_batch(market)
    c.validate_batch(proof)
    return config, market, proof


def declaration(role, batch):
    from equity_feature_workers import InputManifest

    if batch is None:
        return InputManifest(role, hashlib.sha256(('absent:' + role).encode()).hexdigest(),
            'revision1', 'missing', None)
    coverage = batch.metadata.coverage
    state = 'partial' if not coverage.complete else 'empty' if coverage.observed == 0 else 'complete'
    return InputManifest(role, hashlib.sha256(pickle.dumps(batch, protocol=5)).hexdigest(),
        batch.metadata.source.snapshot_id, state, c.InputBinding(role, batch.kind, batch.metadata))


def work(mode='trades', *, name='one', market_missing=False, proof_missing=False, **kwargs):
    from equity_feature_workers.equity_sessions import make_ef1_anchor_work

    config, market, proof = fixture(mode, **kwargs)
    slots = (('trades' if mode == 'trades' else 'bars', None if market_missing else market),
             ('anchor_provenance', None if proof_missing else proof))
    item = make_ef1_anchor_work(config, inputs=tuple(declaration(role, batch) for role, batch in slots),
        batches=tuple(batch for _, batch in slots if batch is not None), instrument_id='X',
        job_id='job-' + name, generation_id='generation-' + name, partition_id='X-S',
        destination_scope='ef1-independent', max_input_bytes=1048576)
    return item
