"""Frozen public synthetic packets, independent of the production calculation."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration
from ef1_reference_fixture import batch

ORACLE_BYTES = Path(__file__).with_name('ef1_daily_observations_oracle.json').read_bytes()
if hashlib.sha256(ORACLE_BYTES).hexdigest() != '50257a0d648809c7987ce49d710812afdd5f1ff6dd4ddf1c65c0fafb8c121f15':
    raise AssertionError('Frozen daily observation bytes differ')
ORACLE = json.loads(ORACLE_BYTES)
CASES = {packet['id']: packet for packet in ORACLE['packets']}
SUCCESS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is not None}
ERRORS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is None}


def fixture(name='V201-observed-not-scheduled'):
    packet = CASES[name]
    return c.ConfigSpec.from_json(json.dumps(packet['config'], ensure_ascii=True,
        sort_keys=True, separators=(',', ':'))), tuple(batch(row['batch']) for row in packet['role_slots'])


def work(name='V201-observed-not-scheduled', *, config=None, slots=None, identity=None):
    from equity_feature_workers.equity_daily_observations import make_ef1_daily_observations_work
    original_config, original_slots = fixture(name)
    config = original_config if config is None else config
    slots = original_slots if slots is None else slots
    roles = tuple(row['role'] for row in CASES[name]['role_slots'])
    entity = CASES[name]['entity']
    key = identity or name
    return make_ef1_daily_observations_work(config,
        inputs=tuple(declaration(role, value) for role, value in zip(roles, slots)),
        batches=tuple(value for value in slots if value is not None), instrument_id=entity['instrument_id'],
        job_id='daily-job-' + key, generation_id='daily-generation-' + key,
        partition_id='daily-partition', destination_scope='ef1-daily-synthetic')


def calculate(item):
    return item.calculate(item.task, item.batches)[0]


def spawned(item):
    from equity_feature_io_sdk import encode_result
    return encode_result(calculate(item))
