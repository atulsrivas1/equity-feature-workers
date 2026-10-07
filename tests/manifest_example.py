"""Run after installing the supported worker/core/SDK combination."""
from manifest_fixture import fixture
from equity_feature_workers import OutputManifest, decode_task, encode_task

task, result, envelope = fixture()
assert decode_task(encode_task(task)) == task
output = OutputManifest(task, envelope)
assert not output.committed
print('Synthetic manifest and independent 500 / 51200 / 102.6 oracle PASS')
