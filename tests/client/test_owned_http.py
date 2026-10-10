"""Actual admitted owned native service with independent client producer references."""
from pathlib import Path
import hashlib
import json
import sys
import tempfile
import threading
import time
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'packages/client/src'))
from equity_feature_client import DatasetKey, FeatureRef, JobExpectation, ProducerExpectation, RawExpectation, RemoteClient, ScopeKey


class OwnedHTTP(unittest.TestCase):
    def test_admitted_audited_native_client_operations(self):
        from equity_feature_service._containment import OwnedEpochSupervisor
        from equity_feature_service._entry import OwnedEntryInventory,OwnedEntryProfile
        fixture=json.loads((ROOT/'docs/EQ081_CLIENT_FIXTURES.json').read_text(encoding='utf-8'))
        payload=fixture['native_cases'][0]['complete_producer_payload']
        expected=ProducerExpectation(payload['context'],ScopeKey('A','S',100,200),tuple(FeatureRef(**f) for f in payload['executed_features']),payload['backend_id'],payload['backend_version'])
        raw=fixture['raw_cases'][0]['expected_raw_payload']
        raw_expected=RawExpectation(DatasetKey(**raw['dataset']),ScopeKey('A','S',100,200),'trade',tuple(c['name'] for c in raw['columns']))
        source=(ROOT/'docs/EQ081_OWNED_HTTP_ENTRY.txt').read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(),'e1cc8ccd0f1276ffc8d5c64d7ac9a312225586e611e01cd2f28ea8c6e2f124da')
        with tempfile.TemporaryDirectory() as directory:
            ready=Path(directory)/'ready.json';stop=Path(directory)/'stop'
            profile=OwnedEntryProfile('eq081-client-http',source,hashlib.sha256(source).hexdigest(),(str(ROOT/'tests/service'),str(ready),str(stop)),True)
            supervisor=OwnedEpochSupervisor(OwnedEntryInventory((profile,)));result=[];errors=[]
            def launch():
                try:result.append(supervisor.run_admitted(profile.profile_id,wall_seconds=25))
                except BaseException as error:errors.append(type(error).__name__)
            thread=threading.Thread(target=launch);thread.start()
            try:
                deadline=time.monotonic()+10
                while not ready.exists():
                    self.assertTrue(thread.is_alive(),errors);self.assertLess(time.monotonic(),deadline);time.sleep(.02)
                data=ready.read_bytes();self.assertLessEqual(len(data),64)
                port=json.loads(data)['port'];self.assertIs(type(port),int)
                with RemoteClient('http://127.0.0.1:'+str(port),lambda:'A'*43) as client:
                    discovery=client.discover(request_id='native-discover');self.assertTrue(discovery.ok,discovery.failure)
                    job=client.calculate(expected,'client-owned-stable',request_id='native-calculate',version='1.0');self.assertTrue(job.ok,job.failure)
                    job_expected=JobExpectation(job.view.payload_json()['job_id'],expected.command_digest)
                    for index in range(10):
                        status=client.job_status(job_expected,request_id='native-status',version='1.0');self.assertTrue(status.ok,status.failure)
                        if status.view.payload_json()['state']=='succeeded':break
                        time.sleep(.03)
                    self.assertEqual(status.view.payload_json()['state'],'succeeded')
                    result_id=status.view.payload_json()['result_id']
                    outcome=client.result_read(result_id,expected,request_id='native-result');self.assertTrue(outcome.ok,outcome.failure)
                    self.assertEqual(outcome.view.payload_json(),payload)
                    values={c['feature_id']:c['values'][0] for c in outcome.view.payload_json()['columns']}
                    self.assertEqual([values[f]['value'] for f in ('session.trade.count','session.trade.volume')],['3','10'])
                    self.assertEqual(values['session.trade.notional']['coefficient'],'1011')
                    artifact=client.artifact_read(result_id,expected,request_id='native-artifact');self.assertTrue(artifact.ok,artifact.failure)
                    self.assertEqual(artifact.view.payload_json(),payload)
                    self.assertEqual(artifact.view.filename,'result-'+hashlib.sha256(result_id.encode('ascii')).hexdigest()+'.json')
                    raw_outcome=client.slice_raw(raw_expected,request_id='native-raw',version='1.0');self.assertTrue(raw_outcome.ok,raw_outcome.failure)
                    self.assertEqual(raw_outcome.view.payload_json(),raw)
                    cancelled=client.job_cancel(job_expected,request_id='native-cancel');self.assertTrue(cancelled.ok,cancelled.failure)
                    self.assertEqual(cancelled.view.payload_json()['state'],'succeeded')
                with RemoteClient('http://127.0.0.1:'+str(port),lambda:'B'*43) as invalid:
                    denial=invalid.result_read(result_id,expected,request_id='native-foreign');self.assertEqual(denial.failure.code,'remote_denial');self.assertEqual(denial.failure.status,401)
            finally:
                stop.write_bytes(b'1');thread.join(6)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertEqual(len(result),1)
            self.assertEqual(result[0].exit_code,0);self.assertEqual(result[0].reason,'exited')
            self.assertEqual(result[0].output.strip(),b'owned-audited-native-http-exited')


if __name__=='__main__':unittest.main()
