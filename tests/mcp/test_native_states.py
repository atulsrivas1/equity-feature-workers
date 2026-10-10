"""Frozen-entry actual native states; execute only after separate entry approval."""
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
from equity_feature_client import RemoteClient
from equity_feature_mcp import MCPProfile,CommandRegistration
import test_client as fixtures
import test_authority_http as authority
import test_native_authority as dialogue

ENTRY_SHA='8f7e757277b684f689cb32a8188adff257fa5b3d569ca2c4519ceff7004a8168'


class NativeStates(unittest.TestCase):
    request=dialogue.NativeAuthority.request
    tool=dialogue.NativeAuthority.tool
    start=dialogue.NativeAuthority.start
    run_dialogue=dialogue.NativeAuthority.run_dialogue
    value=dialogue.NativeAuthority.value

    def setUp(self):
        _,self.expected=fixtures.producer(fixtures.FIXTURES['native_cases'][0])
        self.profile=MCPProfile('states',(),(CommandRegistration('cmd',self.expected),),60)

    @contextlib.contextmanager
    def native(self,mode):
        from equity_feature_service._containment import OwnedEpochSupervisor
        from equity_feature_service._entry import OwnedEntryInventory,OwnedEntryProfile
        source=(ROOT/'docs/EQ082_NATIVE_STATE_ENTRY.txt').read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(),ENTRY_SHA)
        with tempfile.TemporaryDirectory() as directory:
            ready,stop,release=(Path(directory)/name for name in ('ready','stop','release'))
            profile=OwnedEntryProfile('eq082-state-'+mode,source,ENTRY_SHA,(str(ROOT/'tests/service'),str(ready),str(stop),str(release),mode),True)
            supervisor=OwnedEpochSupervisor(OwnedEntryInventory((profile,)));results=[];errors=[];client=None;calls=[]
            def launch():
                try:results.append(supervisor.run_admitted(profile.profile_id,wall_seconds=25))
                except BaseException as error:errors.append(type(error).__name__)
            thread=threading.Thread(target=launch);thread.start()
            try:
                deadline=time.monotonic()+4
                while True:
                    self.assertTrue(thread.is_alive(),errors);raw=authority.completed_bytes(ready,128)
                    if raw is not None:break
                    self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                value=json.loads(raw);self.assertEqual(set(value),{'port'});port=value['port']
                self.assertIs(type(port),int);self.assertTrue(1<=port<=65535)
                client=RemoteClient('http://127.0.0.1:'+str(port),lambda:calls.append(1) or 'A'*43,attempts=3)
                yield client,release.mkdir,calls
                self.assertLessEqual(len(calls),48)
            finally:
                if client is not None:client.close()
                stop.write_bytes(b'1');thread.join(6)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertEqual(len(results),1)
            self.assertEqual((results[0].reason,results[0].exit_code),('exited',0))
            self.assertEqual(json.loads(results[0].output),dict(marker='owned-state-http-exited',mode=mode,reads=1))

    def status(self,responses,reference,expected):
        for _ in range(20):
            yield self.tool('equity_job_status',reference=reference)
            value=self.value(responses);self.assertTrue(value['ok'],value)
            if value['payload']['data']['state']==expected:return value['payload']['data']
            time.sleep(.02)
        self.fail('owned native state not observed: '+expected)

    def test_actual_running_queued_owner_cancel_and_real_exit_cancelled(self):
        with self.native('cancel') as (client,release,calls):
            def commands(responses):
                yield from self.start()
                yield self.tool('equity_calculate',profile='cmd',idempotency_key='running-key')
                first=self.value(responses);self.assertTrue(first['ok'],first);running=first['payload']['reference']
                yield from self.status(responses,running,'running')
                yield self.tool('equity_calculate',profile='cmd',idempotency_key='queued-key')
                second=self.value(responses);self.assertTrue(second['ok'],second);queued=second['payload']['reference']
                self.assertEqual(second['payload']['data']['state'],'queued')
                yield self.tool('equity_job_cancel',reference=queued)
                cancelled=self.value(responses);self.assertTrue(cancelled['ok'],cancelled)
                self.assertEqual(cancelled['payload']['data']['state'],'cancelled');self.assertIsNone(cancelled['payload']['data']['result_id'])
                yield self.tool('equity_job_cancel',reference=running)
                requested=self.value(responses);self.assertTrue(requested['ok'],requested)
                self.assertEqual(requested['payload']['data']['state'],'cancel_requested')
                yield self.tool('equity_job_status',reference=running)
                self.assertEqual(self.value(responses)['payload']['data']['state'],'cancel_requested')
                release()
                terminal=yield from self.status(responses,running,'cancelled');self.assertIsNone(terminal['result_id'])
                before=len(calls)
                for name in ('equity_result_summary','equity_artifact_reference'):
                    yield self.tool(name,reference=running)
                    denied=self.value(responses);self.assertFalse(denied['ok']);self.assertEqual(denied['failure']['code'],'unknown_reference')
                self.assertEqual(len(calls),before)
            self.run_dialogue(client,commands)

    def test_actual_native_callback_failure_has_inert_state_and_no_result(self):
        with self.native('failure') as (client,_,calls):
            def commands(responses):
                yield from self.start()
                yield self.tool('equity_calculate',profile='cmd',idempotency_key='failure-key')
                admitted=self.value(responses);self.assertTrue(admitted['ok'],admitted)
                terminal=yield from self.status(responses,admitted['payload']['reference'],'failed')
                self.assertIsNone(terminal['result_id']);self.assertIsNotNone(terminal['error'])
                self.assertNotIn('owned private native callback text',json.dumps(responses))
                self.assertNotIn('RuntimeError',json.dumps(responses))
                before=len(calls)
                for name in ('equity_result_summary','equity_artifact_reference'):
                    yield self.tool(name,reference=admitted['payload']['reference'])
                    denied=self.value(responses);self.assertFalse(denied['ok']);self.assertEqual(denied['failure']['code'],'unknown_reference')
                self.assertEqual(len(calls),before)
            self.run_dialogue(client,commands)
