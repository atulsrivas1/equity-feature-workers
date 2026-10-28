"""Literal independent period/phase packets, never producer-generated oracles."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration
from ef1_reference_fixture import batch as reference_batch

ORACLE_BYTES = Path(__file__).with_name('ef1_period_oracle.json').read_bytes()
if hashlib.sha256(ORACLE_BYTES).hexdigest() != '298851f79f26da042fada31163c8384eb497f9769d9c3b38b1f51128ec8f93be':
    raise AssertionError('Frozen period/phase fixture bytes differ')
ORACLE = json.loads(ORACLE_BYTES)
CASES = {packet['id']: packet for packet in
    ORACLE['canonical_fixture_packets'] + ORACLE['canonical_case_packets']}
SUCCESS = {name: packet for name, packet in CASES.items() if packet['expected_result'] is not None}
ERRORS = {name: packet for name, packet in CASES.items() if packet['expected_result'] is None}


def fixture(name='PERIOD_BASE'):
    packet = CASES[name]
    config = c.ConfigSpec.from_json(json.dumps(packet['config'], ensure_ascii=True,
        sort_keys=True, separators=(',', ':')))
    slots = []
    for row in packet['input_slots']:
        raw = row['batch']
        if raw is None:
            slots.append(None)
        else:
            # Rectangular literal values remain unchanged, including exact int ns.
            columns = [dict(name=key, values=[record[index] for record in raw['rows']])
                for index, key in enumerate(raw['column_order'])]
            slots.append(reference_batch(dict(kind=raw['kind'], columns=columns, metadata=raw['metadata'])))
    return config, tuple(slots)


def work(name='PERIOD_BASE', *, config=None, slots=None, identity=None):
    from equity_feature_workers.equity_period import make_ef1_period_work
    from equity_feature_workers.equity_extended import make_ef1_extended_work
    original_config, original_slots = fixture(name)
    config = original_config if config is None else config
    slots = original_slots if slots is None else slots
    roles = tuple(row['role'] for row in CASES[name]['input_slots'])
    builder = make_ef1_period_work if config.identity == 'ef1.period' else make_ef1_extended_work
    key = identity or name
    return builder(config, inputs=tuple(declaration(role, value) for role, value in zip(roles, slots)),
        batches=tuple(value for value in slots if value is not None), instrument_id='X',
        job_id='period-job-' + key, generation_id='period-generation-' + key,
        partition_id='X-S', destination_scope='ef1-period-independent')


def calculate(item):
    return item.calculate(item.task, item.batches)[0]


def spawned(item):
    from equity_feature_io_sdk import encode_result
    return encode_result(calculate(item))


def replace_column(value, name, cells):
    return replace(value, columns=tuple(c.Column(name, tuple(cells)) if column.name == name else column
        for column in value.columns))
