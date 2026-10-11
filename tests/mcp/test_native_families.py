"""Real supervised native families; independent frozen producer bytes."""
from pathlib import Path
import contextlib
import hashlib
import json
import sys
import tempfile
import threading
import time
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'packages/client/src'))
sys.path.insert(0,str(ROOT/'packages/mcp/src'))
sys.path.insert(0,str(ROOT/'tests/client'))
from equity_feature_client import RemoteClient,DatasetKey,ScopeKey,RawExpectation,RawSliceView,ResultView
from equity_feature_mcp import MCPProfile,CommandRegistration,RawSliceRegistration,_wire
import test_client as fixtures
import test_authority_http as authority
import test_native_authority as dialogue

ENTRY_SHA='19a03bc41a70162e6998b140977e4b1b642aff5b287eed7e9608eaee01050e64'


class NativeFamilies(unittest.TestCase):
    run_dialogue=dialogue.NativeAuthority.run_dialogue
    request=dialogue.NativeAuthority.request
    tool=dialogue.NativeAuthority.tool
    start=dialogue.NativeAuthority.start
    value=dialogue.NativeAuthority.value

    @contextlib.contextmanager
    def native(self,family):
        from equity_feature_service._containment import OwnedEpochSupervisor
        from equity_feature_service._entry import OwnedEntryInventory,OwnedEntryProfile
        source=(ROOT/'docs/EQ082_NATIVE_FAMILY_ENTRY.txt').read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(),ENTRY_SHA)
        with tempfile.TemporaryDirectory() as directory:
            ready,stop=(Path(directory)/name for name in ('ready','stop'))
            profile=OwnedEntryProfile('eq082-family-'+family,source,ENTRY_SHA,(str(ROOT/'tests/service'),str(ready),str(stop),family),True)
            supervisor=OwnedEpochSupervisor(OwnedEntryInventory((profile,)))
            results=[];errors=[];client=None
            def launch():
                try:results.append(supervisor.run_admitted(profile.profile_id,wall_seconds=25))
                except BaseException as error:errors.append(type(error).__name__)
            thread=threading.Thread(target=launch);thread.start()
            try:
                deadline=time.monotonic()+4
                while True:
                    self.assertTrue(thread.is_alive(),errors)
                    raw=authority.completed_bytes(ready,128)
                    if raw is not None:break
                    self.assertLess(time.monotonic(),deadline)
                    time.sleep(.01)
                value=json.loads(raw);self.assertEqual(set(value),{'port'})
                port=value['port'];self.assertIs(type(port),int);self.assertTrue(1<=port<=65535)
                calls=[]
                client=RemoteClient('http://127.0.0.1:'+str(port),lambda:calls.append(1) or 'A'*43,attempts=3)
                yield client
                self.assertLessEqual(len(calls),25)
            finally:
                if client is not None:client.close()
                stop.write_bytes(b'1');thread.join(6)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertEqual(len(results),1)
            self.assertEqual((results[0].reason,results[0].exit_code),('exited',0))
            self.assertEqual(json.loads(results[0].output),dict(marker='owned-family-http-exited',family=family,reads=1))

    def test_four_real_families_full_raw_producer_export_and_native_parity(self):
        from equity_feature_client.native import convert_raw,convert_result
        from equity_feature_io_sdk import encode_result
        from equity_feature_contracts import ConfigSpec,EntityKey
        from equity_features.session.trades import compute_trades
        for index,case in enumerate(fixtures.FIXTURES['native_cases']):
            family='large_ns' if index==3 else case['family']
            raw_case=fixtures.FIXTURES['raw_cases'][-1 if index==3 else index]
            payload,expected=fixtures.producer(case)
            raw_payload=raw_case['expected_raw_payload'];scope=raw_payload['scope']
            raw_expected=RawExpectation(DatasetKey(**raw_payload['dataset']),ScopeKey(scope['instrument_id'],scope['session_id'],int(scope['start_ns']),int(scope['end_ns'])),raw_case['data_kind'],tuple(c['name'] for c in raw_payload['columns']))
            self.profile=MCPProfile('family-owner',(RawSliceRegistration('raw',raw_expected),),(CommandRegistration('cmd',expected),),60)
            with self.subTest(family=family):
                def commands(responses):
                    yield from self.start()
                    yield self.tool('equity_slice',profile='raw')
                    raw_value=self.value(responses);self.assertTrue(raw_value['ok'],raw_value)
                    self.assertEqual(raw_value['payload']['data'],raw_payload)
                    batch=convert_raw(RawSliceView(_wire.canonical(fixtures.envelope('slice',raw_payload))),raw_expected)
                    self.assertTrue(batch.ok,batch.failure)
                    if family in ('trades','large_ns'):
                        original=json.loads((ROOT/'tests/service/fixtures/jobs_native.json').read_text(encoding='utf-8'))
                        config_value=next(item for item in original['fixtures'] if item['family']=='trades')['native_spec']['config']
                        def shift(value,key=''):
                            if type(value) is dict:return {name:shift(item,name) for name,item in value.items()}
                            if type(value) is list:return [shift(item,key) for item in value]
                            return value+9000000000000000000 if family=='large_ns' and type(value) is int and key.endswith('_ns') else value
                        config=ConfigSpec.from_json(json.dumps(shift(config_value)))
                        pure=compute_trades(batch.view,config,entity=EntityKey('A','S'))
                        values={column.feature_id:column.values[0] for column in pure.values}
                        self.assertEqual([values['session.trade.'+name] for name in ('count','volume','notional')],[3,10,1011])
                    if family=='large_ns':
                        self.assertEqual(batch.view.column('event_ns').values,(9000000000000000110,9000000000000000111,9000000000000000112))
                    yield self.tool('equity_calculate',profile='cmd',idempotency_key='owned-family-key')
                    admitted=self.value(responses);self.assertTrue(admitted['ok'],admitted)
                    reference=admitted['payload']['reference']
                    for _ in range(20):
                        yield self.tool('equity_job_status',reference=reference)
                        state=self.value(responses);self.assertTrue(state['ok'],state)
                        if state['payload']['data']['state']=='succeeded':break
                        time.sleep(.02)
                    self.assertEqual(state['payload']['data']['state'],'succeeded')
                    yield self.tool('equity_result_summary',reference=reference)
                    result=self.value(responses);self.assertTrue(result['ok'],result)
                    original=result['payload']['data'];self.assertEqual(original,payload)
                    self.assertEqual(hashlib.sha256(_wire.canonical(original)).hexdigest(),case['producer_payload_sha256'])
                    native=convert_result(ResultView(_wire.canonical(fixtures.envelope('result',original))),expected)
                    self.assertTrue(native.ok,native.failure)
                    self.assertEqual(hashlib.sha256(encode_result(native.view)).hexdigest(),case['native_sha256'])
                    yield self.tool('equity_artifact_reference',reference=reference)
                    exported=self.value(responses);self.assertTrue(exported['ok'],exported)
                    self.assertEqual(exported['payload']['operation'],'artifact_read')
                    yield self.tool('equity_result_summary',reference=exported['payload']['reference'])
                    followup=self.value(responses);self.assertTrue(followup['ok'],followup)
                    self.assertEqual(followup['payload']['data'],payload)
                with self.native(family) as client:self.run_dialogue(client,commands)
