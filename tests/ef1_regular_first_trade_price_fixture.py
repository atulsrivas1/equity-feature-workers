"""Read-only independent synthetic regular first-trade literals and original DTOs."""
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration
from ef1_reference_fixture import batch

ORACLE_BYTES = Path(__file__).with_name('ef1_regular_first_trade_price_oracle.json').read_bytes()
assert hashlib.sha256(ORACLE_BYTES).hexdigest() == 'e4cd2ec2f97c43012884a7676d7bb6b81e3643ecbe88ebccb9ac251f9bcd6a44'
ORACLE = json.loads(ORACLE_BYTES)
CASES = {packet['id']: packet for packet in ORACLE['outcome_cases'] + ORACLE['refusal_cases']}
CONTROLS = {packet['id']: packet for packet in ORACLE['worker_result_cases']}
SUCCESS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is not None}
ERRORS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is None}


def fixture(name='R001'):
    packet = CASES[name]['request']
    return request_fixture(packet)


def work(name='R001'):
    from equity_feature_workers.equity_regular_first_trade_price import make_ef1_regular_first_trade_price_work
    config, slots = fixture(name)
    return make_ef1_regular_first_trade_price_work(config,
        inputs=tuple(declaration(role, value) for role, value in zip(('calendar_provenance', 'trades', 'population_proof'), slots)),
        batches=tuple(value for value in slots if value is not None),
        instrument_id=CASES[name]['request']['entity']['instrument_id'], job_id='first-trade-job-' + name,
        generation_id='first-trade-generation-' + name, partition_id='first-trade-partition',
        destination_scope='ef1-first-trade-synthetic')


def calculate(item):
    return item.calculate(item.task, item.batches)[0]


def metadata(raw):
    value = dict(raw)
    value['source'] = c.SourceBinding(**value['source'])
    value['coverage'] = c.Coverage(**value['coverage'])
    value['price_unit'] = None if value['price_unit'] is None else c.PriceUnit(**value['price_unit'])
    value['adjustment'] = c.AdjustmentSpec(**value['adjustment'])
    value['scope'] = None if value['scope'] is None else c.InputScope(**value['scope'])
    value['interval_coverage'] = tuple(c.IntervalCoverage(**dict(row,coverage=c.Coverage(**row['coverage']))) for row in value['interval_coverage'])
    return c.BatchMetadata(**value)




def literal_result(raw):
    m = dict(raw['metadata'])
    m['availability'] = c.AvailabilitySpec(**m['availability'])
    m['inputs'] = tuple(c.InputBinding(row['role'],c.DataKind(row['kind']),metadata(row['metadata'])) for row in m['inputs'])
    return c.FeatureResult(
        tuple(c.FeatureColumn(row['feature_id'],row['algorithm_version'],c.ValueType(row['dtype']),row['unit'],tuple(c.EntityKey(**entity) for entity in row['entities']),tuple(row['values']),row['schema_version']) for row in raw['values']),
        tuple(c.QualityRow(c.EntityKey(**row['entity']),row['feature_id'],c.Status(row['status']),row['expected'],row['observed'],tuple(c.Reason(reason) for reason in row['reasons'])) for row in raw['quality']),
        c.ResultMetadata(**m),
        tuple(c.EvidenceRow(**dict(row,entity=c.EntityKey(**row['entity']),exclusion_reason=None if row['exclusion_reason'] is None else c.Reason(row['exclusion_reason']))) for row in raw['evidence']))




def request_fixture(request, stage=None):
    # Record the real constructor reached, preserving constructor-before-pure refusals.
    marker = [] if stage is None else stage
    def at(label, operation):
        marker[:] = [label]
        return operation()
    config = at('ConfigSpec_constructor', lambda: c.ConfigSpec.from_json(json.dumps(
        request['config'], ensure_ascii=True, sort_keys=True, separators=(',', ':'))))
    slots = []
    for declaration in request['role_slots']:
        raw = declaration['batch']
        if raw is None:
            slots.append(None)
            continue
        if raw['schema_version'] != '1':
            marker[:] = ['transport_schema_admission']
            raise c.ContractError(c.ErrorCode.INCOMPATIBLE_VERSION, 'Unsupported literal batch schema')
        m = dict(raw['metadata'])
        m['source'] = c.SourceBinding(**m['source'])
        m['coverage'] = at('Coverage_constructor', lambda: c.Coverage(**m['coverage']))
        m['price_unit'] = None if m['price_unit'] is None else at('PriceUnit_constructor', lambda: c.PriceUnit(**m['price_unit']))
        m['adjustment'] = c.AdjustmentSpec(**m['adjustment'])
        m['scope'] = None if m['scope'] is None else c.InputScope(**m['scope'])
        m['interval_coverage'] = tuple(c.IntervalCoverage(**dict(row,
            coverage=c.Coverage(**row['coverage']))) for row in m['interval_coverage'])
        typed = at('BatchMetadata_constructor', lambda: c.BatchMetadata(**m))
        slots.append(at('CanonicalBatch_constructor', lambda: c.CanonicalBatch(c.DataKind(raw['kind']),
            tuple(c.Column(col['name'], tuple(col['values'])) for col in raw['columns']), typed)))
    return config, tuple(slots)
