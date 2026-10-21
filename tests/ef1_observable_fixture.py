"""Independent frozen canonical packets; no adapter-generated expected results."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration

ORACLE_BYTES = Path(__file__).with_name('ef1_observable_oracle.json').read_bytes()
if hashlib.sha256(ORACLE_BYTES).hexdigest() != '646e402e321eb97a171a2f8f64ce3c1ee399f7bcb81906fd929be4e6477170a6':
    raise AssertionError('Frozen reference fixture bytes differ')
ORACLE = json.loads(ORACLE_BYTES)
CASES = {packet['id']: packet for packet in ORACLE['packets']}


def batch(record):
    if record is None:
        return None
    return c.CanonicalBatch(c.DataKind(record['kind']),
        tuple(c.Column(column['name'], tuple(column['values'])) for column in record['columns']),
        metadata(record['metadata']))


def metadata(raw):
    value = dict(raw)
    value['source'] = c.SourceBinding(**value['source'])
    value['coverage'] = c.Coverage(**value['coverage'])
    value['price_unit'] = None if value['price_unit'] is None else c.PriceUnit(**value['price_unit'])
    value['adjustment'] = c.AdjustmentSpec(**value['adjustment'])
    value['scope'] = None if value['scope'] is None else c.InputScope(**value['scope'])
    value['interval_coverage'] = tuple(c.IntervalCoverage(**dict(row,coverage=c.Coverage(**row['coverage']))) for row in value['interval_coverage'])
    return c.BatchMetadata(**value)


SUCCESS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is not None}
ERRORS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is None}


def fixture(name='O01-first'):
    packet = CASES[name]
    roles = ('calendar_provenance','history_events','history_prefix','history_seed','source_capability') if packet['producer']=='status_history' else ('calendar_provenance','market_prefix','source_capability','trades')
    explicit = {row['role']:row['batch'] for row in packet['batches']}
    return c.ConfigSpec.from_json(packet['config_canonical_json']), tuple(batch(explicit.get(role)) for role in roles)


def work(name='O01-first', *, config=None, slots=None, identity=None):
    import importlib
    original_config, original_slots = fixture(name)
    config = original_config if config is None else config
    slots = original_slots if slots is None else slots
    producer = CASES[name]['producer']
    module = importlib.import_module('equity_feature_workers.equity_' + producer)
    builder = getattr(module, 'make_ef1_' + producer + '_work')
    roles = ('calendar_provenance','history_events','history_prefix','history_seed','source_capability') if producer=='status_history' else ('calendar_provenance','market_prefix','source_capability','trades')
    key = identity or name
    return builder(config, inputs=tuple(declaration(role, value) for role, value in zip(roles, slots)),
        batches=tuple(value for value in slots if value is not None), instrument_id='X',
        job_id='observable-job-' + key, generation_id='observable-generation-' + key,
        partition_id='X-S', destination_scope='ef1-observable-independent')


def calculate(item):
    return item.calculate(item.task, item.batches)[0]


def spawned(item):
    from equity_feature_io_sdk import encode_result
    return encode_result(calculate(item))


def literal_result(raw):
    m = dict(raw['metadata'])
    m['availability'] = c.AvailabilitySpec(**m['availability'])
    m['inputs'] = tuple(c.InputBinding(row['role'],c.DataKind(row['kind']),metadata(row['metadata'])) for row in m['inputs'])
    return c.FeatureResult(
        tuple(c.FeatureColumn(row['feature_id'],row['algorithm_version'],c.ValueType(row['dtype']),row['unit'],tuple(c.EntityKey(**entity) for entity in row['entities']),tuple(row['values']),row['schema_version']) for row in raw['values']),
        tuple(c.QualityRow(c.EntityKey(**row['entity']),row['feature_id'],c.Status(row['status']),row['expected'],row['observed'],tuple(c.Reason(reason) for reason in row['reasons'])) for row in raw['quality']),
        c.ResultMetadata(**m),
        tuple(c.EvidenceRow(**dict(row,entity=c.EntityKey(**row['entity']),exclusion_reason=None if row['exclusion_reason'] is None else c.Reason(row['exclusion_reason']))) for row in raw['evidence']))


def literal_candidate_wire(raw):
    # Build the actual inert SDK representation, without constructing a forbidden
    # over-limit FeatureResult or changing any supplied candidate evidence row.
    from equity_feature_io_sdk.codec import canonical_json, to_wire
    skeleton = literal_result(dict(raw,evidence=[]))
    wire = to_wire(skeleton)
    evidence = tuple(c.EvidenceRow(**dict(row,entity=c.EntityKey(**row['entity']),exclusion_reason=None if row['exclusion_reason'] is None else c.Reason(row['exclusion_reason']))) for row in raw['evidence'])
    wire['fields']['evidence'] = to_wire(evidence)
    return canonical_json(wire)
