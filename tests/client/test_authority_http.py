"""Owned fixture definitions; execution follows separate frozen-entry review."""
from pathlib import Path
import contextlib
import hashlib
import json
import tempfile
import threading
import time
import unittest

from test_client import RemoteClient, JobExpectation, producer, FIXTURES
from test_owned_http import read_ready

ROOT=Path(__file__).resolve().parents[2]
ENTRY_SHA='6a2c1017277fd6586a3fcd9ea85602543a1828455a50befedd231fe0733eb956'


class AuthorityHTTP(unittest.TestCase):
    @contextlib.contextmanager
    def native(self,scenario):
        from equity_feature_service._containment import OwnedEpochSupervisor
        from equity_feature_service._entry import OwnedEntryInventory,OwnedEntryProfile
        source=(ROOT/'docs/EQ081_AUTHORITY_HTTP_ENTRY.txt').read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(),ENTRY_SHA)
        with tempfile.TemporaryDirectory() as directory:
            paths=[Path(directory)/name for name in ('ready','stop','control','ack')]
            ready,stop,control,ack=paths
            profile=OwnedEntryProfile('eq081-authority-'+scenario,source,ENTRY_SHA,(str(ROOT/'tests/service'),*[str(p) for p in paths],scenario),True)
            supervisor=OwnedEpochSupervisor(OwnedEntryInventory((profile,)));results=[];errors=[]
            def launch():
                try:results.append(supervisor.run_admitted(profile.profile_id,wall_seconds=25))
                except BaseException as error:errors.append(type(error).__name__)
            thread=threading.Thread(target=launch);thread.start()
            try:
                port=read_ready(ready,thread,errors)
                owner=RemoteClient('http://127.0.0.1:'+str(port),lambda:'A'*43,attempts=3)
                foreign_calls=[]
                foreign=RemoteClient('http://127.0.0.1:'+str(port),lambda:foreign_calls.append(1) or 'B'*43,attempts=3)
                sequence=[0]
                def action(name):
                    sequence[0]+=1;data=json.dumps({'sequence':sequence[0],'action':name},sort_keys=True,separators=(',',':')).encode('ascii')
                    self.assertLessEqual(len(data),128)
                    pending=control.with_suffix('.pending');pending.write_bytes(data);pending.replace(control)
                    deadline=time.monotonic()+4
                    while True:
                        self.assertTrue(thread.is_alive(),errors);self.assertLess(time.monotonic(),deadline)
                        try:
                            with ack.open('rb') as stream:raw=stream.read(129)
                        except FileNotFoundError:raw=b''
                        self.assertLessEqual(len(raw),128)
                        if raw:
                            result=json.loads(raw)
                            self.assertEqual(set(result),{'sequence','epoch','reads'})
                            if result['sequence']==sequence[0]:return result
                        time.sleep(.01)
                try:yield owner,foreign,foreign_calls,action
                finally:owner.close();foreign.close()
            finally:
                stop.write_bytes(b'1');thread.join(6)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertEqual(len(results),1)
            self.assertEqual((results[0].reason,results[0].exit_code),('exited',0))
            self.assertEqual(results[0].output.strip(),b'owned-authority-http-exited')

    def completed(self,client,key='owned-stable'):
        _,expected=producer(FIXTURES['native_cases'][0])
        outcome=client.calculate(expected,key,request_id='submit');self.assertTrue(outcome.ok,outcome.failure)
        job=JobExpectation(outcome.view.payload_json()['job_id'],expected.command_digest)
        for index in range(20):
            state=client.job_status(job,request_id='status');self.assertTrue(state.ok,state.failure)
            if state.view.payload_json()['state']=='succeeded':break
            time.sleep(.02)
        self.assertEqual(state.view.payload_json()['state'],'succeeded')
        return expected,job,state.view.payload_json()['result_id']

    def denied(self,outcome,status=403):
        self.assertFalse(outcome.ok);self.assertIsNone(outcome.view)
        self.assertEqual((outcome.failure.code,outcome.failure.status),('remote_denial',status))

    def test_authenticated_foreign_owner_and_original_half_open_ttl(self):
        with self.native('ttl') as (owner,foreign,calls,action):
            self.assertTrue(foreign.discover(request_id='authenticated').ok)
            expected,job,result_id=self.completed(owner)
            for identifier in (result_id,'unknown-result'):
                for method in ('result_read','artifact_read'):
                    before=len(calls);self.denied(getattr(foreign,method)(identifier,expected,request_id='foreign-result'))
                    self.assertEqual(len(calls),before+1)
            for identifier in (job.job_id,'unknown-job'):
                for method in ('job_status','job_cancel'):
                    before=len(calls);self.denied(getattr(foreign,method)(JobExpectation(identifier,job.command_digest),request_id='foreign-job'))
                    self.assertEqual(len(calls),before+1)
            before=action('ttl_before');self.assertEqual(before['reads'],1)
            self.assertTrue(owner.result_read(result_id,expected,request_id='before').ok)
            exact=action('ttl_exact');self.assertEqual(exact['reads'],1)
            self.denied(owner.result_read(result_id,expected,request_id='at'))
            self.denied(owner.artifact_read(result_id,expected,request_id='attachment-at'))
            state=owner.job_status(job,request_id='expired');self.assertTrue(state.ok,state.failure)
            self.assertEqual(state.view.payload_json()['state'],'expired')
            repeated=owner.calculate(expected,'owned-stable',request_id='manual-same');self.assertTrue(repeated.ok,repeated.failure)
            self.assertEqual((repeated.view.payload_json()['job_id'],repeated.view.payload_json()['state']),(job.job_id,'expired'))

    def test_retired_credential_restore_cannot_resurrect_payload(self):
        with self.native('retire') as (owner,foreign,calls,action):
            expected,job,result_id=self.completed(owner)
            self.assertTrue(owner.result_read(result_id,expected,request_id='initial').ok)
            self.assertEqual(action('retire')['reads'],1)
            self.denied(owner.result_read(result_id,expected,request_id='retired'),401)
            self.assertTrue(foreign.discover(request_id='unrelated').ok)
            self.assertEqual(action('restore')['reads'],1)
            self.denied(owner.result_read(result_id,expected,request_id='restore'))
            state=owner.job_status(job,request_id='tombstone');self.assertTrue(state.ok,state.failure)
            self.assertEqual(state.view.payload_json()['state'],'expired')
            repeated=owner.calculate(expected,'owned-stable',request_id='same');self.assertTrue(repeated.ok,repeated.failure)
            self.assertEqual(repeated.view.payload_json()['job_id'],job.job_id)

    def test_original_grant_end_caps_ttl_while_token_stays_valid(self):
        with self.native('grant') as (owner,foreign,calls,action):
            expected,job,result_id=self.completed(owner)
            self.assertEqual(action('grant_before')['reads'],1)
            self.assertTrue(owner.result_read(result_id,expected,request_id='before').ok)
            self.assertEqual(action('grant_exact')['reads'],1)
            self.denied(owner.result_read(result_id,expected,request_id='grant-at'))
            self.denied(owner.calculate(expected,'owned-stable',request_id='manual-denied'))
            self.assertTrue(foreign.discover(request_id='unrelated').ok)

    def test_epoch_retirement_has_no_hidden_resubmit(self):
        with self.native('epoch') as (owner,foreign,calls,action):
            expected,job,result_id=self.completed(owner)
            reset=action('restart');self.assertEqual(reset['reads'],0)
            self.denied(owner.job_status(job,request_id='old-job'))
            self.denied(owner.result_read(result_id,expected,request_id='old-result'))
            # Only this explicit new calculate may admit work in the new epoch.
            fresh_expected,fresh_job,fresh_result=self.completed(owner)
            self.assertNotEqual(fresh_job.job_id,job.job_id);self.assertNotEqual(fresh_result,result_id)
            self.assertTrue(owner.result_read(fresh_result,fresh_expected,request_id='manual-new').ok)


if __name__=='__main__':unittest.main()
