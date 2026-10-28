"""Read-only independent synthetic endpoint literals and real DTO construction."""
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration
from ef1_reference_fixture import batch

ORIGINAL_ORACLE_BYTES = Path(__file__).with_name('ef1_gap_endpoint_oracle.json').read_bytes()
assert hashlib.sha256(ORIGINAL_ORACLE_BYTES).hexdigest() == 'ca32e6c890697a09d1ba6450748085a4b03d2bff2eb8e711d8c79371289aede3'
ORACLE_BYTES = Path(__file__).with_name('ef1_gap_endpoint_oracle_causal_v4.json').read_bytes()
assert hashlib.sha256(ORACLE_BYTES).hexdigest() == 'ded050dc98f29a45d9f4bcbb47a3b3c0e415e93f381282500b3f1f8c72a1a0b5'
ORACLE = json.loads(ORACLE_BYTES)
CASES = {packet['id']: packet for packet in ORACLE['packets']}
SUCCESS = {name: packet for name, packet in CASES.items() if packet['expected_result'] is not None}
ERRORS = {name: packet for name, packet in CASES.items() if packet['expected_result'] is None}


def fixture(name='E01_up_reversal'):
    packet = CASES[name]
    return c.ConfigSpec.from_json(json.dumps(packet['config'], ensure_ascii=True,
        sort_keys=True, separators=(',', ':'))), tuple(batch(row['batch']) for row in packet['role_slots'])


def work(name='E01_up_reversal'):
    from equity_feature_workers.equity_gap_endpoint import make_ef1_gap_endpoint_work
    config, slots = fixture(name)
    return make_ef1_gap_endpoint_work(config,
        inputs=tuple(declaration(role, value) for role, value in zip(ORACLE['roles'], slots)),
        batches=tuple(value for value in slots if value is not None),
        instrument_id=CASES[name]['entity']['instrument_id'], job_id='endpoint-job-' + name,
        generation_id='endpoint-generation-' + name, partition_id='endpoint-partition',
        destination_scope='ef1-gap-endpoint-synthetic')


def calculate(item):
    return item.calculate(item.task, item.batches)[0]
