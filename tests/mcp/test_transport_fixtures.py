"""Independent fixed producer/raw bytes over real HTTP; no native computation claim."""
from pathlib import Path
import hashlib
import json
import sys
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'packages/client/src'))
sys.path.insert(0,str(ROOT/'packages/mcp/src'))
sys.path.insert(0,str(ROOT/'tests/client'))
from equity_feature_client import RemoteClient,DatasetKey,ScopeKey,RawExpectation
from equity_feature_mcp import MCPProfile,CommandRegistration,RawSliceRegistration
from equity_feature_mcp import _wire
import test_client as fixtures
import test_native_authority as dialogue


class TransportFixtures(unittest.TestCase):
    run_dialogue=dialogue.NativeAuthority.run_dialogue
    request=dialogue.NativeAuthority.request
    tool=dialogue.NativeAuthority.tool
    start=dialogue.NativeAuthority.start

    def data(self,responses):
        result=responses[-1]['result']
        self.assertEqual(json.loads(result['content'][0]['text']),result['structuredContent'])
        self.assertFalse(result['isError'],result['structuredContent'])
        return result['structuredContent']['payload']

    def test_five_raw_fixtures_full_metadata_and_exact_scalars(self):
        for case in fixtures.FIXTURES['raw_cases']:
            with self.subTest(case=case['family']):
                payload=case['expected_raw_payload']
                scope=ScopeKey(**{**payload['scope'],'start_ns':int(payload['scope']['start_ns']),'end_ns':int(payload['scope']['end_ns'])})
                expected=RawExpectation(DatasetKey(**payload['dataset']),scope,case['data_kind'],tuple(c['name'] for c in payload['columns']))
                self.profile=MCPProfile('owned',(RawSliceRegistration('raw',expected),),())
                def commands(responses):
                    yield from self.start()
                    yield self.tool('equity_slice',profile='raw')
                    value=self.data(responses)
                    self.assertEqual(value['representation'],'raw_slice')
                    self.assertEqual(_wire.canonical(value['data']),_wire.canonical(payload))
                with fixtures.Peer(lambda r,_:(200,fixtures.envelope('slice',payload,r['request_id'],r['version']),b'')) as peer:
                    self.run_dialogue(RemoteClient(peer.origin,lambda:'A'*43),commands)
                    self.assertEqual(len(peer.requests),1)

    def test_four_original_producers_full_hashes_and_export_mode(self):
        for case in fixtures.FIXTURES['native_cases']:
            with self.subTest(case=case['family']):
                payload,expected=fixtures.producer(case)
                self.profile=MCPProfile('owned',(),(CommandRegistration('cmd',expected),))
                operations=[]
                def handler(request,index):
                    operations.append(request['payload']['operation'])
                    value=dict(job_id='owned-job',command_digest=expected.command_digest,state='succeeded',result_id='owned-result',error=None) if index==0 else payload
                    filename='result-'+hashlib.sha256(b'owned-result').hexdigest()+'.json'
                    headers=('Content-Disposition: attachment; filename="'+filename+'"\r\n').encode('ascii') if operations[-1]=='artifact_read' else b''
                    return 200,fixtures.envelope('job' if index==0 else 'result',value,request['request_id'],request['version']),headers
                def commands(responses):
                    yield from self.start()
                    yield self.tool('equity_calculate',profile='cmd',idempotency_key='owned')
                    reference=self.data(responses)['reference']
                    yield self.tool('equity_result_summary',reference=reference)
                    value=self.data(responses)
                    self.assertEqual(value['representation'],'complete_transported_producer')
                    self.assertEqual(hashlib.sha256(_wire.canonical(value['data'])).hexdigest(),case['producer_payload_sha256'])
                    yield self.tool('equity_artifact_reference',reference=reference)
                    exported=self.data(responses)
                    self.assertEqual(exported['operation'],'artifact_read')
                    yield self.tool('equity_result_summary',reference=exported['reference'])
                    self.assertEqual(_wire.canonical(self.data(responses)['data']),_wire.canonical(payload))
                with fixtures.Peer(handler,connections=4) as peer:
                    self.run_dialogue(RemoteClient(peer.origin,lambda:'A'*43),commands)
                self.assertEqual(operations,['calculate','result_read','artifact_read','artifact_read'])

    def test_real_postsend_disconnect_is_unknown_without_replay_or_history(self):
        _,expected=fixtures.producer(fixtures.FIXTURES['native_cases'][0])
        self.profile=MCPProfile('owned',(),(CommandRegistration('cmd',expected),))
        providers=[]
        def commands(responses):
            yield from self.start()
            yield self.tool('equity_calculate',profile='cmd',idempotency_key='manual-key')
            result=responses[-1]['result']
            self.assertTrue(result['isError'])
            failure=result['structuredContent']['failure']
            self.assertEqual(failure['code'],'outcome_unknown')
            self.assertEqual(failure['source'],'client')
            self.assertEqual(json.loads(result['content'][0]['text']),result['structuredContent'])
            self.assertNotIn('manual-key',json.dumps(result))
        with fixtures.Peer(lambda r,i:None) as peer:
            self.run_dialogue(RemoteClient(peer.origin,lambda:providers.append(1) or 'A'*43,attempts=3,retry_delay=0),commands)
            self.assertEqual(len(peer.requests),1)
            self.assertEqual(len(providers),1)


if __name__=='__main__':unittest.main()
