"""Literal synthetic two-bar input and independent result oracle."""
import hashlib
import equity_feature_contracts as c
from equity_feature_contracts.specs import IntervalSpec
from equity_features.session import compute_bars
from equity_feature_io_sdk import descriptor, prepare_publication
from equity_feature_io_contracts import SinkRequirements
from equity_feature_workers import InputManifest, TaskManifest


def fixture():
    unit = c.PriceUnit(0, 'USD')
    config = c.ConfigSpec('synthetic-bars', 'v1', (c.Parameter('eligibility_policy', 'example-v1'),),
        c.SessionSpec('demo', 'S', 100, 200, 'caller-supplied'),
        c.WindowSpec(1, 'S', ('P', 'S')), c.AvailabilitySpec(200, 210, 210), price_unit=unit)
    facts = (('instrument_id', ('A', 'A')), ('session_id', ('S', 'S')),
        ('start_ns', (100, 150)), ('end_ns', (150, 200)), ('known_at_ns', (150, 210)),
        ('open', (100, 102)), ('high', (103, 104)), ('low', (99, 101)), ('close', (102, 103)),
        ('volume', (200, 300)), ('actual_notional', (20300, 30900)))
    batch = c.CanonicalBatch(c.DataKind.BAR, tuple(c.Column(n, v) for n, v in facts),
        c.BatchMetadata('demo', c.SourceBinding('synthetic', 'snapshot1', 'map1', 'bars1'),
        c.Coverage(2, 2, True), unit, scope=c.InputScope(100, 200, 'example-v1')))
    result = compute_bars(batch, config, entity=c.EntityKey('A', 'S'))
    values = {col.feature_id: col.values[0] for col in result.values}
    assert values['session.bar.volume'] == 500
    assert values['session.bar.notional'] == 51200
    assert values['session.bar.close_weighted_price'] == 102.6
    assert values['session.price.overnight_gap'] is None
    acquisition = hashlib.sha256(b'synthetic-bars-input-v1').hexdigest()
    task = TaskManifest('job1', 'generation1', 'A-S', 'bars', ('A',), config,
        descriptor(result).features,
        tuple(InputManifest(b.role, acquisition, 'revision1', 'complete', b) for b in result.metadata.inputs),
        (IntervalSpec('P', 0, 100), IntervalSpec('S', 100, 200)),
        'synthetic-output', 'serialized_destination', 4, 65536)
    envelope = prepare_publication((result,), destination_scope=task.destination_scope,
        generation_id=task.generation_id, job_id=task.job_id, partition_id=task.partition_id,
        limits=SinkRequirements(max_results=4, max_chunk_bytes=65536, max_total_bytes=262144,
                               max_result_cells=100, max_evidence_rows=100))
    return task, result, envelope
