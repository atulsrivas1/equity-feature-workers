"""Public stdio adversaries with real client/provider/HTTP and physical pipes."""
from pathlib import Path
import copy
import io
import json
import os
import sys
import threading
import time
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'packages/client/src'))
sys.path.insert(0,str(ROOT/'packages/mcp/src'))
sys.path.insert(0,str(ROOT/'tests/client'))
from equity_feature_client import RemoteClient,DatasetKey,ScopeKey,RawExpectation
from equity_feature_mcp import MCPProfile,CommandRegistration,RawSliceRegistration,FeatureSliceRegistration,StdioServer,_wire
import test_client as fixtures
import test_native_authority as dialogue


class ProtocolBoundary(unittest.TestCase):
    request=dialogue.NativeAuthority.request
    tool=dialogue.NativeAuthority.tool
    start=dialogue.NativeAuthority.start
    run_dialogue=dialogue.NativeAuthority.run_dialogue
    value=dialogue.NativeAuthority.value

    def setUp(self):
        _,self.expected=fixtures.producer(fixtures.FIXTURES['native_cases'][0])
        self.profile=MCPProfile('boundary',(),(CommandRegistration('cmd',self.expected),),60)

    def exchange(self,client,commands,clock=None):
        server=StdioServer(client,self.profile)
        if clock is not None:server._ledger._clock=clock
        output=io.BytesIO()
        server.run_stdio(io.BytesIO(b''.join(_wire.encode_frame(v) for v in [*self.start(),*commands])),output)
        self.assertTrue(server._closed);self.assertTrue(client._closed)
        return [json.loads(v) for v in output.getvalue().splitlines()]

    def test_all_seven_closed_arguments_and_unknown_registration_before_provider(self):
        calls=[];client=RemoteClient('http://127.0.0.1:1',lambda:calls.append(1) or 'A'*43)
        valid={'equity_discover':{},'equity_slice':{'profile':'raw'},'equity_calculate':{'profile':'cmd','idempotency_key':'key'},'equity_job_status':{'reference':'opaque'},'equity_job_cancel':{'reference':'opaque'},'equity_result_summary':{'reference':'opaque'},'equity_artifact_reference':{'reference':'opaque'}}
        commands=[]
        for name,args in valid.items():
            for bad in (None,[],True,dict(args,origin='http://caller'),dict(args,token='secret'),dict(args,code='execute'),dict(args,path='private')):
                commands.append(self.request('tools/call',{'name':name,'arguments':bad}))
        for key in ('','x'*129,'not ascii é','line\n'):
            commands.append(self.tool('equity_calculate',profile='cmd',idempotency_key=key))
        results=self.exchange(client,commands)
        self.assertEqual(len(results),1+len(commands))
        self.assertTrue(all(v['error']['code']==-32602 for v in results[1:]));self.assertEqual(calls,[])
        calls=[];client=RemoteClient('http://127.0.0.1:1',lambda:calls.append(1) or 'A'*43)
        results=self.exchange(client,[self.tool('equity_calculate',profile='unknown',idempotency_key='key'),self.tool('equity_slice',profile='unknown')])
        self.assertTrue(all(v['result']['structuredContent']['failure']['code']=='unknown_registration' for v in results[1:]));self.assertEqual(calls,[])

    def test_unsupported_methods_notifications_and_thread_ownership(self):
        calls=[];client=RemoteClient('http://127.0.0.1:1',lambda:calls.append(1) or 'A'*43)
        names=('resources/list','resources/read','prompts/list','prompts/get','sampling/createMessage','tasks/get','logging/setLevel')
        frames=self.exchange(client,[*[self.request(n) for n in names],{'jsonrpc':'2.0','method':'notifications/cancelled','params':{'requestId':'owned'}},self.request('ping')])
        self.assertTrue(all(v['error']['code']==-32601 for v in frames[1:-1]));self.assertEqual(frames[-1]['result'],{});self.assertEqual(calls,[])
        client=RemoteClient('http://127.0.0.1:1',lambda:'A'*43);server=StdioServer(client,self.profile);errors=[]
        def other_thread():
            try:server.run_stdio(io.BytesIO(),io.BytesIO())
            except ValueError as error:errors.append(str(error))
        thread=threading.Thread(target=other_thread);thread.start();thread.join(1)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,['profile_invalid']);self.assertFalse(server._ran);server.close()

    def test_sixteen_lifetime_records_deny_seventeenth_before_provider(self):
        calls=[]
        def handler(request,index):
            job=dict(job_id='job-'+str(index),command_digest=self.expected.command_digest,state='queued',result_id=None,error=None)
            return 200,fixtures.envelope('job',job,request['request_id'],request['version']),b''
        def commands(responses):
            yield from self.start()
            refs=[]
            for index in range(16):
                yield self.tool('equity_calculate',profile='cmd',idempotency_key='key-'+str(index))
                admitted=self.value(responses);self.assertTrue(admitted['ok'],admitted);refs.append(admitted['payload']['reference'])
            self.assertEqual(len(set(refs)),16)
            yield self.tool('equity_calculate',profile='cmd',idempotency_key='seventeenth')
            denied=self.value(responses);self.assertFalse(denied['ok']);self.assertEqual(denied['failure']['code'],'reference_capacity')
            self.assertEqual(len(calls),16)
        with fixtures.Peer(handler,connections=16) as peer:
            self.run_dialogue(RemoteClient(peer.origin,lambda:calls.append(1) or 'A'*43),commands)
            self.assertEqual(len(peer.requests),16)

    def test_invalid_clock_before_provider_and_after_actual_response_closes_epoch(self):
        for bad in (True,-1,1.5,None):
            calls=[];client=RemoteClient('http://127.0.0.1:1',lambda:calls.append(1) or 'A'*43)
            frames=self.exchange(client,[self.tool('equity_discover')],lambda:bad)
            self.assertEqual(calls,[]);self.assertEqual(frames,[])
        for bad in (True,-1,1.5,None):
            now=[1];calls=[]
            def handler(request,index):
                now[0]=bad
                return 200,fixtures.envelope('discovery',{'transport_versions':['1.1'],'registry_snapshot':'0'*64,'features':[]},request['request_id'],request['version']),b''
            with fixtures.Peer(handler) as peer:
                frames=self.exchange(RemoteClient(peer.origin,lambda:calls.append(1) or 'A'*43),[self.tool('equity_discover')],lambda:now[0])
                self.assertEqual(len(calls),1);self.assertEqual(len(peer.requests),1);self.assertEqual(len(frames),1)

    def test_provider_exception_privacy_no_replay(self):
        for raised in (RuntimeError('private token/path detail'),KeyboardInterrupt('private interrupt')):
            calls=[]
            def provider():calls.append(1);raise raised
            frames=self.exchange(RemoteClient('http://127.0.0.1:1',provider,attempts=3),[self.tool('equity_discover')])
            self.assertEqual(len(calls),1);encoded=json.dumps(frames)
            self.assertNotIn('private',encoded);self.assertNotIn('RuntimeError',encoded);self.assertNotIn('KeyboardInterrupt',encoded)
            self.assertTrue(frames[-1]['result']['isError'])

    def test_active_provider_cannot_observe_physical_eof_until_return_and_interrupt_closes(self):
        for interrupt in (False,True):
            entered=threading.Event();release=threading.Event();finished=threading.Event();state={};errors=[]
            read_fd,write_fd=os.pipe()
            def provider():
                entered.set()
                if not release.wait(2):raise AssertionError('owned release timeout')
                if interrupt:raise KeyboardInterrupt('private active callback')
                raise RuntimeError('private active callback')
            def owner_thread():
                try:
                    client=RemoteClient('http://127.0.0.1:1',provider);server=StdioServer(client,self.profile);state.update(client=client,server=server)
                    with os.fdopen(read_fd,'rb',buffering=0) as incoming:
                        output=io.BytesIO();server.run_stdio(incoming,output);state['output']=output.getvalue()
                except BaseException as error:errors.append(type(error).__name__)
                finally:finished.set()
            thread=threading.Thread(target=owner_thread);thread.start()
            try:
                with os.fdopen(write_fd,'wb',buffering=0) as outgoing:
                    outgoing.write(b''.join(_wire.encode_frame(v) for v in [*self.start(),self.tool('equity_discover')]))
                    self.assertTrue(entered.wait(1))
                self.assertFalse(finished.wait(.05));self.assertTrue(thread.is_alive())
            finally:release.set();thread.join(3)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertTrue(state['client']._closed);self.assertTrue(state['server']._closed)
            self.assertNotIn(b'private',state['output']);self.assertEqual(len(state['output'].splitlines()),2)

    def test_exact_structured_16384_inclusive_16385_denial_without_truncation(self):
        base=fixtures.FIXTURES['raw_cases'][0]['expected_raw_payload'];scope=base['scope']
        expected=RawExpectation(DatasetKey(**base['dataset']),ScopeKey(scope['instrument_id'],scope['session_id'],int(scope['start_ns']),int(scope['end_ns'])),'trade',tuple(c['name'] for c in base['columns']))
        self.profile=MCPProfile('boundary',(RawSliceRegistration('raw',expected),),())
        for limit in (16384,16385):
            payload=copy.deepcopy(base);cells=next(c for c in payload['columns'] if c['name']=='event_id')['values']+[payload['metadata']['fields']['namespace']]
            structured={'ok':True,'payload':{'representation':'raw_slice','data':payload}}
            extra=limit-len(_wire.canonical(structured));self.assertGreater(extra,0)
            for cell in cells:
                amount=min(extra,4000);cell['value']+='x'*amount;extra-=amount
            self.assertEqual(extra,0);self.assertEqual(len(_wire.canonical(structured)),limit)
            def commands(responses):
                yield from self.start();yield self.tool('equity_slice',profile='raw')
                actual=self.value(responses)
                if limit==16384:self.assertEqual(actual,structured)
                else:self.assertEqual(actual,{'ok':False,'failure':{'source':'mcp','code':'summary_bounds'}})
            with fixtures.Peer(lambda r,_:(200,fixtures.envelope('slice',payload,r['request_id'],r['version']),b'')) as peer:
                self.run_dialogue(RemoteClient(peer.origin,lambda:'A'*43),commands)
                self.assertEqual(len(peer.requests),1)

    def test_owned_projection_preserves_complete_selected_metadata_and_denies_forgery(self):
        payload,expected=fixtures.producer(fixtures.FIXTURES['native_cases'][0])
        selected=expected.executed_features[:2]
        projected=copy.deepcopy(payload);projected['columns']=projected['columns'][:2];projected['quality']=projected['quality'][:2]
        projection=dict(dataset=payload['context']['dataset'],scope=expected.scope.payload_json(),selected_features=[v.payload_json() for v in selected],feature_result=projected)
        self.profile=MCPProfile('projection',(FeatureSliceRegistration('features',DatasetKey(**projection['dataset']),expected,tuple(reversed(selected))),),())
        for variant in ('valid','wrong_scope','wrong_selected','missing_quality'):
            sent=copy.deepcopy(projection)
            if variant=='wrong_scope':sent['scope']['start_ns']='101'
            elif variant=='wrong_selected':sent['selected_features']=sent['selected_features'][:1]
            elif variant=='missing_quality':sent['feature_result']['quality']=[]
            def commands(responses):
                yield from self.start();yield self.tool('equity_slice',profile='features')
                actual=self.value(responses)
                if variant=='valid':
                    self.assertTrue(actual['ok'],actual);self.assertEqual(actual['payload']['representation'],'projected_feature_slice')
                    self.assertEqual(actual['payload']['data'],projection)
                    self.assertEqual(actual['payload']['data']['feature_result']['metadata'],payload['metadata'])
                    self.assertEqual(actual['payload']['data']['feature_result']['context'],payload['context'])
                else:
                    self.assertFalse(actual['ok']);self.assertEqual(actual['failure']['source'],'client');self.assertNotIn('payload',actual)
            with fixtures.Peer(lambda r,_:(200,fixtures.envelope('feature_slice',sent,r['request_id'],r['version']),b'')) as peer:
                self.run_dialogue(RemoteClient(peer.origin,lambda:'A'*43),commands)
                self.assertEqual(len(peer.requests),1)

    def test_cancel_postsend_ambiguity_has_no_replay_and_only_explicit_followup(self):
        calls=[];operations=[]
        def handler(request,index):
            operations.append(request['payload']['operation'])
            if index==1:return None
            job=dict(job_id='owned-job',command_digest=self.expected.command_digest,state='queued' if index==0 else 'cancelled',result_id=None,error=None)
            return 200,fixtures.envelope('job',job,request['request_id'],request['version']),b''
        def commands(responses):
            yield from self.start();yield self.tool('equity_calculate',profile='cmd',idempotency_key='manual-stable-key')
            admitted=self.value(responses);self.assertTrue(admitted['ok'],admitted);reference=admitted['payload']['reference']
            yield self.tool('equity_job_cancel',reference=reference)
            uncertain=self.value(responses);self.assertFalse(uncertain['ok']);self.assertEqual(uncertain['failure']['code'],'outcome_unknown')
            self.assertEqual(operations,['calculate','job_cancel']);self.assertEqual(len(calls),2)
            yield self.tool('equity_job_status',reference=reference)
            followed=self.value(responses);self.assertTrue(followed['ok'],followed);self.assertEqual(followed['payload']['data']['state'],'cancelled')
        with fixtures.Peer(handler,connections=3) as peer:
            self.run_dialogue(RemoteClient(peer.origin,lambda:calls.append(1) or 'A'*43,attempts=3),commands)
            self.assertEqual(operations,['calculate','job_cancel','job_status']);self.assertEqual(len(peer.requests),3)

    def test_closed_output_after_mutation_drops_reference_without_replacement_or_replay(self):
        entered=threading.Event();release=threading.Event();state={};errors=[];calls=[]
        input_read,input_write=os.pipe();output_read,output_write=os.pipe()
        def provider():
            calls.append(1);entered.set()
            if not release.wait(2):raise AssertionError('owned output release timeout')
            return 'A'*43
        job=dict(job_id='owned-job',command_digest=self.expected.command_digest,state='queued',result_id=None,error=None)
        with fixtures.Peer(lambda r,_:(200,fixtures.envelope('job',job,r['request_id'],r['version']),b'')) as peer:
            def owner():
                try:
                    client=RemoteClient(peer.origin,provider,attempts=3);server=StdioServer(client,self.profile);state.update(client=client,server=server)
                    with os.fdopen(input_read,'rb',buffering=0) as incoming,os.fdopen(output_write,'wb',buffering=0) as outgoing:server.run_stdio(incoming,outgoing)
                except BaseException as error:errors.append(type(error).__name__)
            thread=threading.Thread(target=owner);thread.start()
            try:
                with os.fdopen(input_write,'wb',buffering=0) as incoming:
                    incoming.write(b''.join(_wire.encode_frame(v) for v in [*self.start(),self.tool('equity_calculate',profile='cmd',idempotency_key='manual-stable-key')]))
                    self.assertTrue(entered.wait(1))
                with os.fdopen(output_read,'rb',buffering=0) as outgoing:
                    initial=b''
                    while not initial.endswith(b'\n'):initial+=outgoing.read(1)
                    self.assertEqual(json.loads(initial)['result']['protocolVersion'],'2025-11-25')
            finally:release.set();thread.join(3)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[]);self.assertEqual(len(calls),1);self.assertEqual(len(peer.requests),1)
            self.assertEqual(peer.requests[0][0].count(b'manual-stable-key'),1)
            self.assertTrue(state['server']._closed);self.assertTrue(state['client']._closed)
            self.assertEqual(state['server']._ledger._records,{});self.assertEqual(state['server']._ledger._reservations,{})
