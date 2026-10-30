"""Independent frozen canonical packets; no adapter-generated expected results."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration

ORACLE_BYTES = Path(__file__).with_name('ef1_reference_oracle.json').read_bytes()
if hashlib.sha256(ORACLE_BYTES).hexdigest() != '7eb97ec61eb1a945d39be9551c5937b3e86c7647d854b1aa507d0a8f32728a1d':
    raise AssertionError('Frozen reference fixture bytes differ')
ORACLE = json.loads(ORACLE_BYTES)
CASES = {packet['id']: packet for packet in ORACLE['canonical_literal_packets']}


def batch(record):
    if record is None:
        return None
    metadata = dict(record['metadata'])
    metadata['source'] = c.SourceBinding(**metadata['source'])
    metadata['coverage'] = c.Coverage(**metadata['coverage'])
    metadata['price_unit'] = None if metadata['price_unit'] is None else c.PriceUnit(**metadata['price_unit'])
    metadata['adjustment'] = c.AdjustmentSpec(**metadata['adjustment'])
    metadata['scope'] = None if metadata['scope'] is None else c.InputScope(**metadata['scope'])
    metadata['interval_coverage'] = tuple(c.IntervalCoverage(**dict(row,
        coverage=c.Coverage(**row['coverage']))) for row in metadata['interval_coverage'])
    return c.CanonicalBatch(c.DataKind(record['kind']),
        tuple(c.Column(column['name'], tuple(column['values'])) for column in record['columns']),
        c.BatchMetadata(**metadata))


def fixture(name='R01-CANONICAL'):
    packet = CASES[name]
    config = c.ConfigSpec.from_json(packet['canonical_config_json'])
    return config, tuple(batch(row['batch']) for row in packet['role_slots'])


def work(name='R01-CANONICAL', *, config=None, slots=None, identity=None):
    from equity_feature_workers.equity_reference_classification import make_ef1_reference_classification_work
    from equity_feature_workers.equity_reference_action import make_ef1_reference_action_work
    original_config, original_slots = fixture(name)
    config = original_config if config is None else config
    slots = original_slots if slots is None else slots
    roles = tuple(row['role'] for row in CASES[name]['role_slots'])
    builder = (make_ef1_reference_classification_work if config.identity == 'ef1.reference_classification'
        else make_ef1_reference_action_work)
    key = identity or name
    return builder(config, inputs=tuple(declaration(role, value) for role, value in zip(roles, slots)),
        batches=tuple(value for value in slots if value is not None), instrument_id='X',
        job_id='ref-job-' + key, generation_id='ref-generation-' + key,
        partition_id='X-S', destination_scope='ef1-reference-independent')


def replace_column(value, name, cells):
    return replace(value, columns=tuple(c.Column(name, tuple(cells)) if column.name == name else column
        for column in value.columns))


def calculate(item):
    return item.calculate(item.task, item.batches)[0]


def spawned(item):
    """Top-level callback resolved by an actual spawned child, no local closure."""
    from equity_feature_io_sdk import encode_result
    return encode_result(calculate(item))
