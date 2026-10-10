from pathlib import Path
import dataclasses
import hashlib
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'packages/client/src'))
from equity_feature_client import DatasetKey, Failure, FeatureRef, JobExpectation, Outcome, ProducerExpectation, RawExpectation, Request, ScopeKey
from equity_feature_client import _wire

FIXTURES = json.loads((ROOT / 'docs/EQ081_CLIENT_FIXTURES.json').read_text(encoding='utf-8'))


class Models(unittest.TestCase):
    def test_literal_source_and_exact_adjacent_scope(self):
        raw = FIXTURES['raw_cases'][-1]['expected_raw_payload']
        dataset = DatasetKey(**raw['dataset'])
        scope = ScopeKey('A', 'S', 9000000000000000110, 9000000000000000113)
        expected = RawExpectation(dataset, scope, 'trade', ['instrument_id', 'event_ns'])
        request = Request('1.0', 'owned-model', dict(operation='slice', dataset=dataset.payload_json(), scope=scope.payload_json(), columns=list(expected.columns), cursor=None))
        self.assertEqual(request.payload_json()['scope']['start_ns'], '9000000000000000110')
        self.assertEqual(request.payload_json()['scope']['end_ns'], '9000000000000000113')
        self.assertEqual(expected.columns, ('instrument_id', 'event_ns'))
        for values in [(True, 3), (0, 0), (2, 1), (-(2**63)-1, 0), (0, 2**63)]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                ScopeKey('A', 'S', *values)

    def test_original_producer_digest_and_owned_context(self):
        for case in FIXTURES['native_cases']:
            producer = case['complete_producer_payload']
            context = json.loads(json.dumps(producer['context']))
            # Scope is independently retained in complete native metadata.
            native = case['native_result']
            from equity_feature_io_sdk import decode_result
            accepted = decode_result(json.dumps(native, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('ascii'))
            input_scope = accepted.metadata.inputs[0].metadata.scope
            scope = ScopeKey(accepted.values[0].entities[0].instrument_id, accepted.metadata.session_id, input_scope.start_ns, input_scope.end_ns)
            refs = tuple(FeatureRef(c.feature_id, c.algorithm_version) for c in accepted.values)
            expected = ProducerExpectation(context, scope, refs, accepted.metadata.backend_id, accepted.metadata.backend_version)
            self.assertEqual(expected.command_digest, producer['command_digest'])
            before = expected.context_json()
            context['namespace'] = 'modified'
            expected.context_json()['namespace'] = 'also-modified'
            self.assertEqual(expected.context_json(), before)
            with self.assertRaises(dataclasses.FrozenInstanceError):
                expected.command_digest = '0' * 64

    def test_request_copies_nested_caller_input(self):
        dataset = DatasetKey(**FIXTURES['raw_cases'][0]['expected_raw_payload']['dataset'])
        payload = dict(operation='slice', dataset=dataset.payload_json(), scope=ScopeKey('A', 'S', 100, 200).payload_json(), columns=['event_ns'], cursor=None)
        request = Request('1.1', 'owned-request', payload)
        original = request.encoded
        payload['columns'].append('price')
        request.payload_json()['dataset']['revision'] = 'changed'
        self.assertEqual(request.encoded, original)
        self.assertEqual(request.payload_json()['columns'], ['event_ns'])

    def test_closed_failure_and_exact_outcome(self):
        value = Failure('credentials', 'provider_failed', 'provider')
        self.assertFalse(isinstance(value, BaseException))
        self.assertFalse(hasattr(value, '__traceback__'))
        self.assertFalse(hasattr(value, '__dict__'))
        self.assertFalse(Outcome(failure=value).ok)
        self.assertTrue(Outcome(view=b'owned').ok)
        for kwargs in ({}, {'view': b'owned', 'failure': value}, {'failure': 'caller-secret'}):
            with self.assertRaises(ValueError):
                Outcome(**kwargs)
        for args in [('caller-secret', 'provider_failed', 'provider'), ('credentials', 'caller-secret', 'provider'), ('credentials', 'provider_failed', 'caller-secret'), ('internal', 'remote_denial', 'read', True)]:
            with self.assertRaisesRegex(ValueError, '^invalid_configuration$'):
                Failure(*args)

    def test_invalid_request_and_precision_cells(self):
        for payload in [{'operation': 'discover', 'uploaded_code': 'no'}, {'operation': 'job_status', 'job_id': 'invalid space'}]:
            with self.assertRaises(ValueError):
                Request('1.1', 'owned', payload)
        for version in ('2.0', None, True):
            with self.assertRaises(ValueError):
                Request(version, 'owned', {'operation': 'discover'})
        for value in ('01', '-0', str(2**63), str(-(2**63)-1), 1, True):
            with self.assertRaises(ValueError):
                _wire.integer(value)
        for bits in ('7ff0000000000000', 'fff0000000000000', '7ff8000000000001'):
            with self.assertRaises(ValueError):
                _wire.canonical({'type': 'float64', 'bits': bits})
        self.assertIn(b'8000000000000000', _wire.canonical({'type': 'float64', 'bits': '8000000000000000'}))
        self.assertEqual(_wire.integer(str(-(2**63))), -(2**63))
        self.assertEqual(_wire.integer(str(2**63-1)), 2**63-1)

    def test_hostile_json_before_validation(self):
        for encoded in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":"\xff"}', b'[]', b'{"a":"\\ud800"}', b'{"a":' + b'[' * 33 + b'0' + b']' * 33 + b'}'):
            with self.subTest(encoded=encoded[:32]), self.assertRaises(ValueError):
                _wire.parse(encoded)
        with self.assertRaises(ValueError):
            _wire.parse(b'{"a":[' + b'0,' * 10000 + b'0]}')

    def test_exact_byte_limits_and_schema_pins(self):
        for maximum in (16384, 262144):
            body = b'{"value":"' + b'a' * (maximum - 12) + b'"}'
            self.assertEqual(len(body), maximum)
            self.assertEqual(len(_wire.canonical(_wire.parse(body, limit=maximum), limit=maximum)), maximum)
            with self.assertRaisesRegex(ValueError, '^bounds$'):
                _wire.parse(body + b' ', limit=maximum)
        pins = {'remote-v1.schema.json': '9d5e83f1d89bb34491d959158ae628e4b7b0590155244beb13945a3b94f362a0', 'remote-v1.1.schema.json': 'dda0f9d2e5968ff2ea3bf79f02740a18f9ae24cd93c492f7c38033883ed8a6b2'}
        for name, sha in pins.items():
            data = (ROOT / 'packages/client/src/equity_feature_client/schemas' / name).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), sha)


if __name__ == '__main__':
    unittest.main()
