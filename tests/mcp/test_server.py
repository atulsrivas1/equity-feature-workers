from pathlib import Path
import io
import json
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'packages/client/src'))
sys.path.insert(0, str(ROOT / 'packages/mcp/src'))
from equity_feature_client import FeatureRef, ProducerExpectation, ScopeKey, RemoteClient, Outcome, JobView, ResultView, Failure
from equity_feature_mcp import CommandRegistration, MCPProfile, StdioServer
from equity_feature_mcp import _wire


class Server(unittest.TestCase):
    def setUp(self):
        self.original = json.loads((ROOT / 'docs/EQ081_CLIENT_FIXTURES.json').read_bytes())['native_cases'][0]['complete_producer_payload']
        expected = ProducerExpectation(self.original['context'], ScopeKey('A', 'S', 100, 200),
            tuple(FeatureRef(**v) for v in self.original['executed_features']), self.original['backend_id'], self.original['backend_version'])
        self.profile = MCPProfile('owner', (), (CommandRegistration('cmd', expected),), 1)
        self.client = RemoteClient('http://127.0.0.1:1', lambda: 'A' * 43)
        self.server = StdioServer(self.client, self.profile)
        self.now = 0
        self.server._ledger._clock = lambda: self.now
        self.job = dict(job_id='job', command_digest=expected.command_digest, state='succeeded', result_id='result', error=None)

    def tearDown(self):
        self.server.close()

    def view(self, cls, kind, payload):
        return Outcome(view=cls(_wire.canonical(dict(schema='equity.remote', version='1.1', kind=kind, request_id='owned', payload=payload))))

    def request(self, method, params=None, identifier=1):
        return dict(jsonrpc='2.0', id=identifier, method=method, params={} if params is None else params)

    def initialize(self):
        initial = self.request('initialize', dict(protocolVersion='other', capabilities={}, clientInfo=dict(name='test', version='1')))
        self.assertEqual(self.server._dispatch(initial)['result']['protocolVersion'], '2025-11-25')
        self.server._dispatch(dict(jsonrpc='2.0', method='notifications/initialized'))

    def call(self, name, args):
        return self.server._dispatch(self.request('tools/call', dict(name=name, arguments=args)))['result']['structuredContent']

    def test_closed_lifecycle_ids_metadata_and_seven_tools(self):
        self.assertEqual(self.server._dispatch(self.request('tools/list'))['error']['code'], -32600)
        self.initialize()
        response = self.server._dispatch(self.request('tools/list', identifier='X' * 64))
        self.assertEqual(len(response['result']['tools']), 7)
        self.assertLessEqual(len(_wire.encode_frame(response)), 65536)
        for identifier in (True, None, -1, 2147483648, '\n', 'é'):
            self.assertIsNone(self.server._dispatch(self.request('ping', identifier=identifier))['id'])
        self.assertEqual(self.server._dispatch(self.request('ping', {'_meta': {'a': 'x' * 2048}}))['error']['code'], -32602)
        self.assertEqual(self.server._dispatch(self.request('initialize', dict(protocolVersion='1', capabilities={}, clientInfo=dict(name='test', version='1'))))['error']['code'], -32600)

    def test_exact_calculate_and_artifact_followup_never_downgrade(self):
        self.initialize()
        with patch.object(RemoteClient, 'calculate', return_value=self.view(JobView, 'job', self.job)) as calculate:
            result = self.call('equity_calculate', dict(profile='cmd', idempotency_key='key'))
            self.assertTrue(result['ok'])
            reference = result['payload']['reference']
            self.assertEqual(calculate.call_count, 1)
            self.assertEqual(calculate.call_args.kwargs['version'], '1.1')
        with patch.object(RemoteClient, 'artifact_read', return_value=self.view(ResultView, 'result', self.original)) as artifact, patch.object(RemoteClient, 'result_read') as ordinary:
            exported = self.call('equity_artifact_reference', dict(reference=reference))
            self.assertTrue(exported['ok'])
            followup = exported['payload']['reference']
            self.assertEqual(self.call('equity_result_summary', dict(reference=followup))['payload']['data'], self.original)
            self.assertEqual(self.call('equity_artifact_reference', dict(reference=reference))['payload']['reference'], followup)
            self.assertEqual(artifact.call_count, 3)
            ordinary.assert_not_called()

    def test_authority_denial_tombstones_and_no_resurrection_before_provider(self):
        self.initialize()
        with patch.object(RemoteClient, 'calculate', return_value=self.view(JobView, 'job', self.job)) as calculate:
            ref = self.call('equity_calculate', dict(profile='cmd', idempotency_key='key'))['payload']['reference']
            with patch.object(RemoteClient, 'job_status', return_value=Outcome(failure=Failure('authorization', 'remote_denial', 'read', 403))):
                self.assertFalse(self.call('equity_job_status', dict(reference=ref))['ok'])
            denied = self.call('equity_calculate', dict(profile='cmd', idempotency_key='key'))
            self.assertEqual(denied['failure']['code'], 'reference_expired')
            self.assertEqual(calculate.call_count, 1)

    def test_late_encoding_expiry_discards_success_before_first_write(self):
        initialize = self.request('initialize', dict(protocolVersion='2025-11-25', capabilities={}, clientInfo=dict(name='test', version='1')))
        frames = [initialize, dict(jsonrpc='2.0', method='notifications/initialized'), self.request('tools/call', dict(name='equity_calculate', arguments=dict(profile='cmd', idempotency_key='key')))]
        incoming = io.BytesIO(b''.join(_wire.encode_frame(v) for v in frames))
        outgoing = io.BytesIO()
        original_encoder = _wire.encode_frame
        def encode(value):
            encoded = original_encoder(value)
            if value.get('result', {}).get('structuredContent', {}).get('ok') is True:
                self.now = 1000000000
            return encoded
        with patch.object(RemoteClient, 'calculate', return_value=self.view(JobView, 'job', self.job)), patch('equity_feature_mcp.server._wire.encode_frame', side_effect=encode):
            self.server.run_stdio(incoming, outgoing)
        result = json.loads(outgoing.getvalue().splitlines()[-1])['result']
        self.assertTrue(result['isError'])
        self.assertEqual(result['structuredContent']['failure']['code'], 'reference_expired')
        self.assertEqual(json.loads(result['content'][0]['text']), result['structuredContent'])
        self.assertTrue(self.server._closed)

    def test_partial_frame_no_dispatch_and_short_write_no_replacement(self):
        with patch.object(RemoteClient, 'calculate') as calculate:
            output = io.BytesIO()
            self.server.run_stdio(io.BytesIO(b'{"jsonrpc":"2.0"}'), output)
            self.assertEqual(json.loads(output.getvalue())['error']['code'], -32700)
            calculate.assert_not_called()
        with self.assertRaises(ValueError):
            self.server.run_stdio(io.BytesIO(), io.BytesIO())
        class Short(io.BytesIO):
            writes = 0
            def write(self, data):
                self.writes += 1
                return super().write(data[:3])
        server = StdioServer(RemoteClient('http://127.0.0.1:1', lambda: 'A' * 43), self.profile)
        output = Short()
        server.run_stdio(io.BytesIO(_wire.encode_frame(self.request('ping'))), output)
        self.assertEqual(output.writes, 1)
        self.assertEqual(len(output.getvalue()), 3)
        self.assertTrue(server._closed)


if __name__ == '__main__':
    unittest.main()
