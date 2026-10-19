"""Original six-slot facts; mathematical expectations are independently specified."""
from dataclasses import replace
from fractions import Fraction
import hashlib
import pickle

import equity_feature_contracts as c
from equity_features_intraday import IntradayContext, IntradayRun, build_intraday_config, describe_intraday
from equity_feature_workers import InputManifest, TaskManifest, WorkItem
from equity_feature_workers.intraday import compute_intraday_inputs, intraday_headers, intraday_history

ALL = tuple(entry.feature_id for entry in describe_intraday())


def facts(instrument='A', *, sessions=False, reset=False):
    governed = (c.SessionSpec('original', 'P', 0, 30, 'supplied'), c.SessionSpec('original', 'S', 30, 60, 'supplied')) if sessions else (c.SessionSpec('original', 'S', 0, 60, 'supplied'),)
    runs = (IntradayRun('P', 0, 10, 3), IntradayRun('S', 30, 10, 3)) if sessions else (IntradayRun('S', 0, 10, 6),)
    context = IntradayContext(instrument, 'original-calendar', '10ns-grid', 'revision1', governed, runs,
        3 if sessions and reset else 0, 5, 'reset' if reset or not sessions else 'carry')
    config = build_intraday_config(context, period=3, band_k=Fraction(2), availability=c.AvailabilitySpec(60, 60, 60),
        adjustment=c.AdjustmentSpec(), price_unit=c.PriceUnit(0, 'USD'), evidence_limit=100)
    batch = c.CanonicalBatch(c.DataKind.BAR, tuple(c.Column(k, v) for k, v in dict(
        instrument_id=(instrument,) * 6, session_id=('P',) * 3 + ('S',) * 3 if sessions else ('S',) * 6,
        start_ns=(0, 10, 20, 30, 40, 50), end_ns=(10, 20, 30, 40, 50, 60),
        known_at_ns=(10, 20, 30, 40, 50, 60), close=(10, 12, 11, 15, 14, 16),
        high=(12, 14, 13, 17, 16, 18), low=(8, 10, 9, 13, 12, 14)).items()),
        c.BatchMetadata('original', c.SourceBinding('synthetic', 'fixture1', 'map1', 'bars-' + instrument),
            c.Coverage(6, 6, True), c.PriceUnit(0, 'USD'), scope=c.InputScope(0, 60, 'original-v1'),
            interval_coverage=tuple(c.IntervalCoverage(f'slot:{n}', n * 10, (n + 1) * 10, c.Coverage(1, 1, True)) for n in range(6))))
    return context, config, batch


def work(context, config, batches, features=ALL, *, missing=False, name='original'):
    inputs = tuple(InputManifest(f'bars:{n:03}', hashlib.sha256(pickle.dumps(b, protocol=5)).hexdigest(), 'revision1',
        'empty' if b.metadata.coverage.complete and b.metadata.coverage.expected == 0 else 'complete' if b.metadata.coverage.complete else 'partial',
        c.InputBinding(f'bars:{n:03}', b.kind, b.metadata)) for n, b in enumerate(batches))
    if missing:
        inputs = (InputManifest('bars', hashlib.sha256(b'original-absent').hexdigest(), 'revision1', 'missing', None),)
    history = intraday_history(config, features)
    task = TaskManifest('original-intraday', 'generation1', name, 'intraday', (context.instrument_id,), config,
        intraday_headers(features), inputs, tuple(c.IntervalSpec(s.session_id, s.open_ns, s.close_ns) for s in context.sessions),
        'original-destination', 'serialized_destination', max(1, len(batches)), 1048576, history[0], history[1], history[2])
    return WorkItem(task, batches, compute_intraday_inputs)


def values(batch, **changes):
    return replace(batch, columns=tuple(c.Column(col.name, changes.get(col.name, col.values)) for col in batch.columns if changes.get(col.name, col.values) is not None))


def split(batch):
    return tuple(replace(batch, columns=tuple(c.Column(col.name, col.values[a:b]) for col in batch.columns),
        metadata=replace(batch.metadata, source=replace(batch.metadata.source, input_id=f'chunk-{a}-{batch.metadata.source.input_id}'),
            scope=replace(batch.metadata.scope, start_ns=a * 10, end_ns=b * 10), coverage=c.Coverage(3, 3, True),
            interval_coverage=batch.metadata.interval_coverage[a:b])) for a, b in ((0, 3), (3, 6)))
