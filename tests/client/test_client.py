from pathlib import Path
import copy
import gc
import hashlib
import json
import socket
import sys
import threading
import time
import unittest
from unittest.mock import patch
import weakref

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'packages/client/src'))
from equity_feature_client import DatasetKey, Failure, FeatureRef, JobExpectation, ProducerExpectation, RawExpectation, RemoteClient, ScopeKey
from equity_feature_client import _wire
from equity_feature_client._response import response
from equity_feature_client._transport import Frame

FIXTURES=json.loads((ROOT/'docs/EQ081_CLIENT_FIXTURES.json').read_text(encoding='utf-8'))


def envelope(kind,payload,request_id='owned',version='1.1'):
    return {'schema':'equity.remote','version':version,'request_id':request_id,'kind':kind,'payload':payload}


def producer(case):
    value=case['complete_producer_payload'];md=value['metadata']['fields'];scope=md['inputs']['items'][0]['fields']['metadata']['fields']['scope']['fields']
    expected=ProducerExpectation(value['context'],ScopeKey('A','S',int(scope['start_ns']['value']),int(scope['end_ns']['value'])),tuple(FeatureRef(**f) for f in value['executed_features']),value['backend_id'],value['backend_version'])
    return copy.deepcopy(value),expected


class Peer:
    def __init__(self,handler,*,connections=1):
        self.handler=handler;self.connections=connections;self.requests=[];self.errors=[]
        self.listener=socket.socket();self.listener.bind(('127.0.0.1',0));self.listener.listen(4);self.listener.settimeout(3)
        self.origin='http://127.0.0.1:'+str(self.listener.getsockname()[1]);self.thread=threading.Thread(target=self.serve)
    def serve(self):
        try:
            for index in range(self.connections):
                connection,_=self.listener.accept()
                with connection:
                    connection.settimeout(3);data=b''
                    while b'\r\n\r\n' not in data:data+=connection.recv(4096)
                    headers,body=data.split(b'\r\n\r\n',1)
                    length=int(next(line.split(b':',1)[1] for line in headers.split(b'\r\n') if line.lower().startswith(b'content-length:')))
                    while len(body)<length:body+=connection.recv(4096)
                    request=json.loads(body);self.requests.append((body,headers))
                    result=self.handler(request,index)
                    if result is None:continue
                    status,value,extra=result
                    encoded=_wire.canonical(value,limit=262144)
                    connection.sendall(b'HTTP/1.0 '+str(status).encode()+b' Owned\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Length: '+str(len(encoded)).encode()+b'\r\n'+extra+b'\r\n'+encoded)
        except BaseException as error:self.errors.append(type(error).__name__)
    def __enter__(self):self.thread.start();return self
    def __exit__(self,*args):
        self.thread.join(4);self.listener.close()
        if self.thread.is_alive():raise AssertionError('owned peer thread did not exit')
        if self.errors:raise AssertionError(self.errors)


class Client(unittest.TestCase):
    def test_extreme_configuration_has_closed_validation_errors(self):
        for name in ('timeout','budget','retry_delay'):
            for value in (10**1000, -(10**1000), float('nan'), float('inf'), True):
                with self.subTest(name=name),self.assertRaisesRegex(ValueError,'^invalid_configuration$'):
                    RemoteClient('http://127.0.0.1:9',lambda:'A'*43,**{name:value})
    def test_complete_producer_coverage_source_and_evidence_denials(self):
        cases=[]
        payload,expected=producer(FIXTURES['native_cases'][0])
        missing=copy.deepcopy(payload);missing['quality']=[];cases.append((missing,expected))
        foreign=copy.deepcopy(payload);extra=copy.deepcopy(foreign['metadata']['fields']['inputs']['items'][0]);extra['fields']['metadata']['fields']['source']['fields']['input_id']['value']='foreign-input';foreign['metadata']['fields']['inputs']['items'].append(extra);cases.append((foreign,expected))
        quotes,quote_expected=producer(next(c for c in FIXTURES['native_cases'] if c['family']=='quotes'))
        self.assertTrue(quotes['evidence'])
        wrong=copy.deepcopy(quotes);wrong['evidence'][0]['fields']['entity']['fields']['instrument_id']['value']='FOREIGN';cases.append((wrong,quote_expected))
        wrong_input=copy.deepcopy(quotes);wrong_input['evidence'][0]['fields']['input_id']['value']='foreign-input';cases.append((wrong_input,quote_expected))
        for changed,pinned in cases:
            with Peer(lambda r,_:(200,envelope('result',changed,r['request_id']),b'')) as peer:
                outcome=RemoteClient(peer.origin,lambda:'A'*43).result_read('owned-result',pinned,request_id='owned')
                self.assertEqual(outcome.failure.code,'invalid_response')

    def test_unverifiable_mutation_reply_is_unknown_without_replay(self):
        _,expected=producer(FIXTURES['native_cases'][0]);job={'job_id':'owned-job','command_digest':'0'*64,'state':'queued','result_id':None,'error':None}
        for method in ('calculate','job_cancel'):
            with Peer(lambda r,_:(200,envelope('job',job,r['request_id']),b'')) as peer:
                client=RemoteClient(peer.origin,lambda:'A'*43,attempts=3)
                result=client.calculate(expected,'owned-key',request_id='owned') if method=='calculate' else client.job_cancel(JobExpectation('owned-job',expected.command_digest),request_id='owned')
                self.assertEqual(result.failure.code,'outcome_unknown');self.assertEqual(len(peer.requests),1)

    def test_discovery_correlated_immutable_and_closed(self):
        payload={'transport_versions':['1.0'],'registry_snapshot':'0'*64,'features':[]}
        with Peer(lambda request,_:(200,envelope('discovery',payload,request['request_id'],request['version']),b'')) as peer:
            with RemoteClient(peer.origin,lambda:'A'*43) as client:
                outcome=client.discover(request_id='owned',version='1.0')
                self.assertTrue(outcome.ok);self.assertEqual(outcome.view.payload_json(),payload)
                outcome.view.payload_json()['features'].append('changed')
                self.assertEqual(outcome.view.payload_json(),payload)
            self.assertEqual(client.discover(request_id='owned').failure.code,'client_closed')

    def test_full_raw5_and_producer4_identity(self):
        for case in FIXTURES['raw_cases']:
            payload=case['expected_raw_payload'];scope=ScopeKey(**{**payload['scope'],'start_ns':int(payload['scope']['start_ns']),'end_ns':int(payload['scope']['end_ns'])})
            expected=RawExpectation(DatasetKey(**payload['dataset']),scope,case['data_kind'],tuple(c['name'] for c in payload['columns']))
            with Peer(lambda r,_:(200,envelope('slice',payload,r['request_id'],r['version']),b'')) as peer:
                result=RemoteClient(peer.origin,lambda:'A'*43).slice_raw(expected,request_id='owned')
                self.assertTrue(result.ok,result.failure)
                self.assertEqual(_wire.canonical(result.view.payload_json(),limit=262144),_wire.canonical(payload,limit=262144))
        for case in FIXTURES['native_cases']:
            payload,expected=producer(case)
            with Peer(lambda r,_:(200,envelope('result',payload,r['request_id'],r['version']),b'')) as peer:
                result=RemoteClient(peer.origin,lambda:'A'*43).result_read('owned-result',expected,request_id='owned')
                self.assertTrue(result.ok,result.failure)
                self.assertEqual(hashlib.sha256(_wire.canonical(result.view.payload_json(),limit=262144)).hexdigest(),case['producer_payload_sha256'])

    def test_success_identity_and_status_denials(self):
        payload={'transport_versions':['1.1'],'registry_snapshot':'0'*64,'features':[]}
        for status,kind,rid,version in [(200,'discovery','foreign','1.1'),(200,'discovery','owned','1.0'),(302,'discovery','owned','1.1'),(500,'discovery','owned','1.1')]:
            with self.subTest(values=(status,rid,version)),Peer(lambda r,_:(status,envelope(kind,payload,rid,version),b'')) as peer:
                result=RemoteClient(peer.origin,lambda:'A'*43).discover(request_id='owned')
                self.assertEqual(result.failure.code,'invalid_response')

    def test_exact_null_error_table_and_no_server_retry(self):
        allowed=[(401,'authentication','unauthenticated'),(403,'authorization','not_permitted'),(429,'quota','quota_exceeded'),(500,'internal','internal_error')]+[(400,'transport',code) for code in ('invalid_schema','invalid_json','incompatible_version','bounds')]
        for status,category,code in allowed:
            with self.subTest(category=category,code=code),Peer(lambda r,_:(status,envelope('error',dict(category=category,code=code,retryable=False),None,'1.0'),b'')) as peer:
                result=RemoteClient(peer.origin,lambda:'A'*43,attempts=3).discover(request_id='owned')
                self.assertEqual(result.failure.code,'remote_denial');self.assertEqual(result.failure.status,status)
                self.assertEqual(len(peer.requests),1)
        for version,code,retryable,status in [('1.1','invalid_schema',False,400),('1.0','inconsistent_identity',False,400),('1.0','invalid_json',True,400),('1.0','invalid_json',False,500)]:
            with Peer(lambda r,_:(status,envelope('error',dict(category='transport',code=code,retryable=retryable),None,version),b'')) as peer:
                result=RemoteClient(peer.origin,lambda:'A'*43).discover(request_id='owned')
                self.assertEqual(result.failure.code,'invalid_response')

    def test_read_disconnect_retry_same_bytes_fresh_provider(self):
        payload={'transport_versions':['1.1'],'registry_snapshot':'0'*64,'features':[]};calls=[]
        def provider():calls.append(1);return ('A' if len(calls)==1 else 'B')*43
        with Peer(lambda r,index:None if index==0 else (200,envelope('discovery',payload,r['request_id']),b''),connections=2) as peer:
            outcome=RemoteClient(peer.origin,provider,attempts=3,retry_delay=0).discover(request_id='owned')
            self.assertTrue(outcome.ok,outcome.failure)
            self.assertEqual(len(calls),2);self.assertEqual(peer.requests[0][0],peer.requests[1][0])
            self.assertIn(b'A'*43,peer.requests[0][1]);self.assertIn(b'B'*43,peer.requests[1][1])

    def test_mutations_not_replayed_and_unknown_once_sent(self):
        payload,expected=producer(FIXTURES['native_cases'][0])
        for method in ('calculate','job_cancel'):
            with Peer(lambda r,index:None) as peer:
                client=RemoteClient(peer.origin,lambda:'A'*43,attempts=3,retry_delay=0)
                result=client.calculate(expected,'owned-key',request_id='owned') if method=='calculate' else client.job_cancel(JobExpectation('owned-job',expected.command_digest),request_id='owned')
                self.assertEqual(result.failure.code,'outcome_unknown');self.assertEqual(len(peer.requests),1)

    def test_job_digest_attachment_and_projection(self):
        payload,expected=producer(FIXTURES['native_cases'][0]);job={'job_id':'owned-job','command_digest':expected.command_digest,'state':'queued','result_id':None,'error':None}
        for method in ('calculate','job_status','job_cancel'):
            with Peer(lambda r,_:(200,envelope('job',job,r['request_id'],r['version']),b'')) as peer:
                client=RemoteClient(peer.origin,lambda:'A'*43)
                result=client.calculate(expected,'owned-key',request_id='owned',version='1.0') if method=='calculate' else getattr(client,method)(JobExpectation('owned-job',expected.command_digest),request_id='owned',version='1.0')
                self.assertTrue(result.ok,result.failure)
        filename='result-'+hashlib.sha256(b'owned-result').hexdigest()+'.json'
        with Peer(lambda r,_:(200,envelope('result',payload,r['request_id']),('Content-Disposition: attachment; filename="'+filename+'"\r\n').encode())) as peer:
            result=RemoteClient(peer.origin,lambda:'A'*43).artifact_read('owned-result',expected,request_id='owned')
            self.assertTrue(result.ok,result.failure);self.assertEqual(result.view.filename,filename)
        selected=expected.executed_features[:2];projected=copy.deepcopy(payload);projected['columns']=projected['columns'][:2];projected['quality']=projected['quality'][:2]
        projection={'dataset':payload['context']['dataset'],'scope':expected.scope.payload_json(),'selected_features':[f.payload_json() for f in selected],'feature_result':projected}
        with Peer(lambda r,_:(200,envelope('feature_slice',projection,r['request_id']),b'')) as peer:
            result=RemoteClient(peer.origin,lambda:'A'*43).slice_features(DatasetKey(**payload['context']['dataset']),expected,tuple(reversed(selected)),request_id='owned')
            self.assertTrue(result.ok,result.failure)

    def test_provider_secret_graph_not_retained(self):
        refs=[]
        class SecretError(Exception):pass
        def provider():
            try:
                original=SecretError('private-token-'+('A'*43));refs.append(weakref.ref(original));raise original
            except SecretError:
                error=SecretError('private-body');refs.append(weakref.ref(error));raise error
        result=RemoteClient('http://127.0.0.1:9',provider).discover(request_id='owned')
        gc.collect();self.assertEqual(result.failure.code,'provider_failed');self.assertTrue(all(r() is None for r in refs))
        self.assertFalse(hasattr(result.failure,'__cause__'));self.assertFalse(hasattr(result.failure,'__traceback__'));self.assertNotIn('private',repr(result))

    def test_bad_request_before_provider_and_late_provider_budget(self):
        calls=[];client=RemoteClient('http://127.0.0.1:9',lambda:calls.append(1) or 'A'*43)
        self.assertEqual(client.discover(request_id='invalid request space').failure.code,'invalid_request');self.assertEqual(calls,[])
        def slow():time.sleep(.03);return 'A'*43
        result=RemoteClient('http://127.0.0.1:9',slow,budget=.01).discover(request_id='owned')
        self.assertEqual(result.failure.code,'timeout')
        for token in ('A'*42,'A'*257,'A'*43+'\r\n',True):
            self.assertEqual(RemoteClient('http://127.0.0.1:9',lambda:token).discover(request_id='owned').failure.code,'token_invalid')

    def test_concurrent_busy_and_close_during_provider(self):
        entered=threading.Event();release=threading.Event();results=[]
        def provider():entered.set();release.wait(2);return 'A'*43
        client=RemoteClient('http://127.0.0.1:9',provider)
        thread=threading.Thread(target=lambda:results.append(client.discover(request_id='owned')));thread.start();self.assertTrue(entered.wait(1))
        self.assertEqual(client.discover(request_id='another').failure.code,'busy');client.close();release.set();thread.join(2)
        self.assertFalse(thread.is_alive());self.assertEqual(results[0].failure.code,'client_closed')


if __name__=='__main__':unittest.main()
