"""Read-only independent synthetic regular-BAR literals and original DTOs."""
import hashlib
import json
from pathlib import Path

import equity_feature_contracts as c
from ef1_anchor_fixture import declaration
from ef1_reference_fixture import batch

ORACLE_BYTES = Path(__file__).with_name('ef1_regular_bar_prices_oracle.json').read_bytes()
assert hashlib.sha256(ORACLE_BYTES).hexdigest() == '70fcaff8038d416f8cbc00340c7084fa106944c85a8e1ec4088d3b4b5149db96'
ORACLE = json.loads(ORACLE_BYTES)
CASES = {packet['id']: packet for packet in ORACLE['packets']}
SUCCESS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is not None}
ERRORS = {name: packet for name, packet in CASES.items() if packet.get('expected_result') is None}


def fixture(name='R01_two_independent_endpoints'):
    packet = CASES[name]
    return c.ConfigSpec.from_json(json.dumps(packet['config'], ensure_ascii=True,
        sort_keys=True, separators=(',', ':'))), tuple(batch(row['batch']) for row in packet['role_slots'])


def work(name='R01_two_independent_endpoints'):
    from equity_feature_workers.equity_regular_bar_prices import make_ef1_regular_bar_price_work
    config, slots = fixture(name)
    return make_ef1_regular_bar_price_work(config,
        inputs=tuple(declaration(role, value) for role, value in zip(ORACLE['roles'], slots)),
        batches=tuple(value for value in slots if value is not None),
        instrument_id=CASES[name]['entity']['instrument_id'], job_id='regular-bar-job-' + name,
        generation_id='regular-bar-generation-' + name, partition_id='regular-bar-partition',
        destination_scope='ef1-regular-bar-synthetic')


def calculate(item):
    return item.calculate(item.task, item.batches)[0]
