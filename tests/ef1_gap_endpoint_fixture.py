"""Read-only independent synthetic endpoint literals and real DTO construction."""
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration
from ef1_reference_fixture import batch

ORIGINAL_ORACLE_BYTES = Path(__file__).with_name('ef1_gap_endpoint_oracle.json').read_bytes()
assert hashlib.sha256(ORIGINAL_ORACLE_BYTES).hexdigest() == '751cd98fd7951aa7c2d4394d9a5fee87e184d9d484d9f43d4de4983b7333f40f'
ORACLE_BYTES = Path(__file__).with_name('ef1_gap_endpoint_oracle_causal_v4.json').read_bytes()
assert hashlib.sha256(ORACLE_BYTES).hexdigest() == '698994acf9d324d3c94334b02f307b2997ce320eb397d7f7cb5f8ea359d4f239'
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
