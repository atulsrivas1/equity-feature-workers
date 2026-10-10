"""Actual audited native HTTP through public bounded MCP stdio, no network mocks."""
from pathlib import Path
import io
import json
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'packages/client/src'))
sys.path.insert(0, str(ROOT / 'packages/mcp/src'))
sys.path.insert(0, str(ROOT / 'tests/client'))
from equity_feature_client import RemoteClient
from equity_feature_mcp import CommandRegistration, MCPProfile, StdioServer
from equity_feature_mcp import _wire
from test_client import producer, FIXTURES
import test_authority_http as authority


class NativeAuthority(unittest.TestCase):
    def setUp(self):
        _, self.expected = producer(FIXTURES['native_cases'][0])
        self.profile = MCPProfile('owner', (), (CommandRegistration('trades', self.expected),), 60)

    def request(self, method, params=None):
        return dict(jsonrpc='2.0', id='owned', method=method, params={} if params is None else params)

    def tool(self, name, **arguments):
        return self.request('tools/call', dict(name=name, arguments=arguments))

    def start(self):
        yield self.request('initialize', dict(protocolVersion='2025-11-25', capabilities={}, clientInfo=dict(name='owned', version='1')))
        yield dict(jsonrpc='2.0', method='notifications/initialized')

    def run_dialogue(self, client, commands):
        output = io.BytesIO()
        responses = []
        errors = []
        sequence = commands(responses)
        class Input:
            pending = b''
            last_count = 0
            finished = False
            def read(inner, count):
                self.assertEqual(count, 1)
                if not inner.pending:
                    frames = output.getvalue().splitlines()
                    responses.extend(json.loads(v) for v in frames[inner.last_count:])
                    inner.last_count = len(frames)
                    try: inner.pending = _wire.encode_frame(next(sequence))
                    except StopIteration:
                        inner.finished = True
                        return b''
                    except BaseException as error:
                        errors.append(error)
                        raise
                result, inner.pending = inner.pending[:1], inner.pending[1:]
                return result
        server = StdioServer(client, self.profile)
        input_stream = Input()
        server.run_stdio(input_stream, output)
        if errors: raise errors[0]
        self.assertTrue(input_stream.finished, 'product closed before complete owned dialogue')
        self.assertTrue(server._closed)
        self.assertTrue(client._closed)
        self.assertFalse(server._ledger._records)
        self.assertFalse(server._ledger._reservations)
        sequence.close()
        return responses

    def value(self, responses):
        result = responses[-1]['result']
        structured = result['structuredContent']
        self.assertEqual(json.loads(result['content'][0]['text']), structured)
        self.assertEqual(result['isError'], not structured['ok'])
        return structured

    def completed(self, responses):
        yield self.tool('equity_calculate', profile='trades', idempotency_key='owned-mcp-authority')
        admitted = self.value(responses); self.assertTrue(admitted['ok'], admitted)
        reference = admitted['payload']['reference']
        for _ in range(20):
            yield self.tool('equity_job_status', reference=reference)
            state = self.value(responses); self.assertTrue(state['ok'], state)
            if state['payload']['data']['state'] == 'succeeded': break
            import time
            time.sleep(.02)
        self.assertEqual(state['payload']['data']['state'], 'succeeded')
        yield self.tool('equity_result_summary', reference=reference)
        result = self.value(responses); self.assertTrue(result['ok'], result)
        self.assertEqual(result['payload']['data'], FIXTURES['native_cases'][0]['complete_producer_payload'])
        return reference

    def test_authenticated_second_principal_has_no_cross_process_reference_rights(self):
        fixture = authority.AuthorityHTTP('test_authenticated_foreign_owner_and_original_half_open_ttl')
        refs = []
        with fixture.native('ttl') as (owner, foreign, calls, action):
            def owner_commands(responses):
                yield from self.start()
                reference = yield from self.completed(responses)
                refs.append(reference)
            self.run_dialogue(owner, owner_commands)
            def foreign_commands(responses):
                yield from self.start()
                yield self.tool('equity_discover')
                self.assertTrue(self.value(responses)['ok'])
                self.assertEqual(len(calls), 1)
                for reference in (refs[0], 'Z' * 43):
                    for name in ('equity_job_status','equity_job_cancel','equity_result_summary','equity_artifact_reference'):
                        yield self.tool(name, reference=reference)
                        denied = self.value(responses)
                        self.assertEqual(denied, dict(ok=False, failure=dict(source='mcp',code='unknown_reference')))
                        self.assertEqual(len(calls), 1)
            self.run_dialogue(foreign, foreign_commands)
            self.assertEqual(action('ttl_before')['reads'], 1)

    def test_original_half_open_ttl_invalidates_export_and_same_key(self):
        fixture = authority.AuthorityHTTP('test_authenticated_foreign_owner_and_original_half_open_ttl')
        with fixture.native('ttl') as (owner, _, _, action):
            def commands(responses):
                yield from self.start()
                reference = yield from self.completed(responses)
                yield self.tool('equity_artifact_reference', reference=reference)
                exported = self.value(responses); self.assertTrue(exported['ok'], exported)
                export = exported['payload']['reference']
                self.assertEqual(action('ttl_before')['reads'], 1)
                yield self.tool('equity_result_summary', reference=export)
                self.assertTrue(self.value(responses)['ok'])
                self.assertEqual(action('ttl_exact')['reads'], 1)
                yield self.tool('equity_result_summary', reference=reference)
                self.assertEqual(self.value(responses)['failure']['status'], 403)
                yield self.tool('equity_result_summary', reference=export)
                self.assertEqual(self.value(responses)['failure']['code'], 'reference_expired')
                yield self.tool('equity_calculate', profile='trades', idempotency_key='owned-mcp-authority')
                self.assertEqual(self.value(responses)['failure']['code'], 'reference_expired')
            self.run_dialogue(owner, commands)

    def test_retirement_restore_and_grant_expiry_never_resurrect(self):
        for scenario, before, after, status in (('retire','retire','restore',401), ('grant','grant_before','grant_exact',403)):
            with self.subTest(scenario=scenario):
                fixture = authority.AuthorityHTTP('test_retired_credential_restore_cannot_resurrect_payload')
                with fixture.native(scenario) as (owner, foreign, _, action):
                    def commands(responses):
                        yield from self.start()
                        reference = yield from self.completed(responses)
                        self.assertEqual(action(before)['reads'], 1)
                        if scenario == 'grant':
                            yield self.tool('equity_result_summary', reference=reference)
                            self.assertTrue(self.value(responses)['ok'])
                            self.assertEqual(action(after)['reads'], 1)
                        yield self.tool('equity_result_summary', reference=reference)
                        self.assertEqual(self.value(responses)['failure']['status'], status)
                        if scenario == 'retire': self.assertEqual(action(after)['reads'], 1)
                        yield self.tool('equity_result_summary', reference=reference)
                        self.assertEqual(self.value(responses)['failure']['code'], 'reference_expired')
                        yield self.tool('equity_calculate', profile='trades', idempotency_key='owned-mcp-authority')
                        self.assertEqual(self.value(responses)['failure']['code'], 'reference_expired')
                        self.assertTrue(foreign.discover(request_id='unrelated').ok)
                    self.run_dialogue(owner, commands)

    def test_native_restart_and_explicit_new_owner_epoch_have_no_hidden_resubmit(self):
        fixture = authority.AuthorityHTTP('test_epoch_retirement_has_no_hidden_resubmit')
        origins = []
        original_client = RemoteClient
        def client(origin, provider, **kwargs):
            origins.append(origin)
            return original_client(origin, provider, **kwargs)
        refs = []
        with patch.object(authority, 'RemoteClient', side_effect=client), fixture.native('epoch') as (owner, _, _, action):
            def old_commands(responses):
                yield from self.start()
                reference = yield from self.completed(responses); refs.append(reference)
                self.assertEqual(action('restart')['reads'], 0)
                yield self.tool('equity_job_status', reference=reference)
                self.assertEqual(self.value(responses)['failure']['status'], 403)
                yield self.tool('equity_calculate', profile='trades', idempotency_key='owned-mcp-authority')
                self.assertEqual(self.value(responses)['failure']['code'], 'reference_expired')
            self.run_dialogue(owner, old_commands)
            fresh = RemoteClient(origins[0], lambda:'A'*43, attempts=1)
            def new_commands(responses):
                yield from self.start()
                yield self.tool('equity_job_status', reference=refs[0])
                self.assertEqual(self.value(responses)['failure']['code'], 'unknown_reference')
                new = yield from self.completed(responses)
                self.assertNotEqual(new, refs[0])
            self.run_dialogue(fresh, new_commands)


if __name__ == '__main__': unittest.main()
