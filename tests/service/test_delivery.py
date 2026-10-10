"""Owned complete retained native delivery and expiry adversaries."""
from dataclasses import replace
import hashlib
import json
import struct
import secrets
import http.client
import threading
import time
from pathlib import Path
import unittest
from unittest.mock import patch

import test_jobs as native_helpers
from test_service import call,env
from equity_feature_service import codec,Credential,Limits,Service
from equity_feature_service.loopback import qualification_server

ACTIONS=('calculate','job_manage','retain','discover','derived_read','export')
FROZEN=json.loads((Path(__file__).parent/'fixtures/delivery_native.json').read_text(encoding='utf-8'))

class Delivery(unittest.TestCase):
    def make(self,**kwargs):
        scheduler,service,tokens,clock,audit=native_helpers.setup(actions=ACTIONS,**kwargs)
        self.addCleanup(lambda:scheduler.close(2))
        job=native_helpers.Jobs().wait_terminal(scheduler,native_helpers.Jobs().submit(service,tokens[0]))
        self.assertEqual(job.state,'succeeded')
        return scheduler,service,tokens,clock,audit,job
    def request(self,result_id,operation='result_read',version='1.1',cursor=None):
        return {'schema':'equity.remote','version':version,'kind':'request','request_id':'owned-delivery-reference',
                'payload':{'operation':operation,'result_id':result_id,'cursor':cursor}}
    def test_three_complete_producers_no_new_native_io(self):
        for family in ('trades','bars','quotes'):
            with self.subTest(family=family):
                scheduler,service,tokens,clock,audit,job=self.make(family=family)
                reference=next(c for c in FROZEN['cases'] if c['case']==family)
                for operation in ('result_read','artifact_read','result_read'):
                    status,body,*_=call(service,self.request(job.result_id,operation),tokens[0])
                    self.assertEqual(status,200,body)
                    self.assertEqual(body['payload'],reference['complete_producer_payload'])
                self.assertEqual(audit['reads'],1)
    def test_missing_export_preserves_result_only(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        service.ledger.grants=tuple(replace(g,actions=g.actions-{'export'}) for g in service.ledger.grants)
        self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],200)
        self.assertEqual(call(service,self.request(job.result_id,'artifact_read'),tokens[0])[0],403)
    def test_foreign_unknown_and_missing_derived(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        self.assertEqual(call(service,self.request(job.result_id),tokens[1])[0],403)
        self.assertEqual(call(service,self.request('unknown-result'),tokens[0])[0],403)
        service.ledger.grants=tuple(replace(g,actions=g.actions-{'derived_read'}) for g in service.ledger.grants)
        self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
    def test_version_cursor_closed(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        self.assertEqual(call(service,self.request(job.result_id,version='1.0'),tokens[0])[0],400)
        self.assertEqual(call(service,self.request(job.result_id,cursor='unsupported'),tokens[0])[0],400)
    def test_ttl_half_open_and_no_refresh(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        expiry=job.expires_ns;clock.n=expiry-1
        self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],200)
        self.assertEqual(job.expires_ns,expiry)
        clock.n=expiry
        self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
        self.assertEqual((job.native,job.wire,job.envelope,job.receipt,job.held),(b'',b'',b'',b'',0))
    def test_prepared_opaque_expiry_before_emission(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        observed=[]
        emission=service(env(self.request(job.result_id),tokens[0]),lambda status,headers:observed.append((status,headers)))
        self.assertNotIn('columns',json.dumps(emission.prepared.envelope))
        self.assertGreater(service.ledger._reserved,0)
        clock.n=job.expires_ns
        body=json.loads(next(iter(emission)))
        self.assertEqual(body['kind'],'error')
        self.assertTrue(observed[0][0].startswith('403'))
        self.assertEqual(service.ledger._reserved,0)
    def test_abandoned_release_once(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        emission=service(env(self.request(job.result_id),tokens[0]),lambda *args:None)
        self.assertGreater(service.ledger._reserved,0)
        emission.close();emission.close()
        self.assertEqual(service.ledger._reserved,0)
    def test_wire_native_receipt_tamper_denied(self):
        for field in ('native','wire','receipt','envelope'):
            with self.subTest(field=field):
                scheduler,service,tokens,clock,audit,job=self.make()
                with service.ledger.lock:setattr(job,field,getattr(job,field).replace(b'1011',b'2011') if b'1011' in getattr(job,field) else getattr(job,field)+b' ')
                self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
                self.assertEqual(audit['reads'],1)
    def test_actual_loopback_safe_attachment_and_foreign_denial(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        with qualification_server(service) as server:
            host,port=server.server_address
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            connection=http.client.HTTPConnection(host,port,timeout=3)
            try:
                for token,status,byte_range in [(tokens[0],200,None),(tokens[1],403,None),(tokens[0],400,'bytes=0-10')]:
                    headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
                    if byte_range is not None:headers['Range']=byte_range
                    connection.request('POST','/v1/request',json.dumps(self.request(job.result_id,'artifact_read')),
                        headers)
                    response=connection.getresponse();data=response.read()
                    self.assertEqual(response.status,status)
                    self.assertEqual(response.getheader('Cache-Control'),'no-store')
                    self.assertEqual(response.getheader('X-Content-Type-Options'),'nosniff')
                    self.assertEqual(int(response.getheader('Content-Length')),len(data))
                    expected='attachment; filename="result-'+hashlib.sha256(job.result_id.encode('ascii')).hexdigest()+'.json"'
                    self.assertEqual(response.getheader('Content-Disposition'),expected if status==200 else None)
                    self.assertNotIn(job.receipt.decode('ascii'),data.decode('ascii'))
                connection.close()
            finally:
                connection.close();server.shutdown();thread.join(2)
        self.assertEqual(audit['reads'],1)
    def test_exact_actual_transfer_and_one_byte_short(self):
        for delta in (0,-1):
            scheduler,service,tokens,clock,audit,job=self.make()
            size=len(codec.encode(scheduler.result_visible(service.ledger.credentials[0],job.result_id,'1.1','owned-delivery-reference','result_read')))
            ledger=service.ledger
            with ledger.lock:
                ledger._transfer=0;ledger._principal_transfer.clear()
                ledger.limits=Limits(60,size+delta,size+delta,60_000_000_000)
            self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],200 if delta==0 else 429)
            self.assertEqual(ledger._reserved,0)
    def test_prepare_then_revoke_denies_and_preserves_committed_history(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        observed=[]
        emission=service(env(self.request(job.result_id,'artifact_read'),tokens[0]),lambda *args:observed.append(args))
        service.ledger.revoke(job.grant_id)
        body=json.loads(next(emission))
        self.assertEqual(body['kind'],'error')
        self.assertTrue(observed[0][0].startswith('403'))
        self.assertFalse(any(h[0]=='Content-Disposition' for h in observed[0][1]))
        self.assertIsNotNone(job.committed_receipt_sha256)
        self.assertEqual(service.ledger._reserved,0)
    def test_emitted_iterator_retains_no_complete_payload(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        emission=service(env(self.request(job.result_id),tokens[0]),lambda *args:None)
        self.assertEqual(json.loads(next(emission))['kind'],'result')
        self.assertNotIn('columns',json.dumps(emission.prepared.envelope))
        self.assertIsNone(emission.prepared.finalize)
    def test_adjacent_large_ns_full_reference(self):
        scheduler,service,tokens,clock,audit,job=self.make(ns_offset=9_000_000_000_000_000_000,adjacent=True)
        reference=next(c for c in FROZEN['cases'] if c['case']=='trades-adjacent-large-ns')
        self.assertEqual(call(service,self.request(job.result_id),tokens[0])[1]['payload'],reference['complete_producer_payload'])
    def test_retired_original_token_no_resurrection_by_new_token(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        token=secrets.token_urlsafe(32)
        with service.ledger.lock:
            service.ledger.credentials=(Credential.provision('A',token,0,3_600_000_000_000),service.ledger.credentials[1])
        self.assertEqual(call(service,self.request(job.result_id),token)[0],403)
        self.assertEqual(job.held,0)
    def test_alternate_grant_does_not_authorize_original_result(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        with service.ledger.lock:
            service.ledger.grants=tuple(replace(g,grant_id='alternate') if g.principal=='A' else g for g in service.ledger.grants)
        self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
    def test_all_executed_features_remain_required(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        with service.ledger.lock:
            rule=scheduler.grants[job.grant_id]
            scheduler.grants=type(scheduler.grants)({job.grant_id:replace(rule,features=rule.features-{next(iter(rule.features))}),'grant-B':scheduler.grants['grant-B']})
        self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
    def test_invalid_result_operations_never_touch_source(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        for field,value in [('path','C:/private'),('sql','select *'),('destination','secret'),('range','0-10')]:
            request=self.request(job.result_id);request['payload'][field]=value
            self.assertEqual(call(service,request,tokens[0])[0],400)
        self.assertEqual(audit['reads'],1)
    def test_native_float_decimal_and_int64_codec_edges(self):
        for value in [-(2**63),2**63-1,9_000_000_000_000_000_110,9_000_000_000_000_000_111]:
            self.assertEqual(codec.cell(value),{'type':'int64','value':str(value)})
        for value in [-(10**38)+1,10**38-1,1011]:
            self.assertEqual(codec.cell(value,decimal=True),{'type':'decimal128','coefficient':str(value),'scale':0})
        for bits in ['0000000000000000','8000000000000000','0000000000000001','7fefffffffffffff','ffefffffffffffff']:
            self.assertEqual(codec.cell(struct.unpack('>d',bytes.fromhex(bits))[0]),{'type':'float64','bits':bits})
        for value,decimal in [(2**63,False),(-(2**63)-1,False),(10**38,True),(-(10**38),True),(float('inf'),False),(float('nan'),False)]:
            with self.assertRaises(codec.WireError):codec.cell(value,decimal=decimal)
    def test_token_and_grant_exact_expiry_distinct(self):
        for kind,status in [('credential',401),('grant',403)]:
            scheduler,service,tokens,clock,audit,job=self.make()
            if kind=='credential':
                service.ledger.credentials=tuple(replace(c,expires_at_ns=200) if c.principal=='A' else c for c in service.ledger.credentials)
            else:
                service.ledger.grants=tuple(replace(g,expires_at_ns=200) if g.principal=='A' else g for g in service.ledger.grants)
            clock.n=199
            self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],200)
            clock.n=200
            self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],status)
    def test_new_live_same_owner_token_does_not_refresh_ttl(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        expiry=job.expires_ns;token=secrets.token_urlsafe(32)
        with service.ledger.lock:
            service.ledger.credentials=service.ledger.credentials+(Credential.provision('A',token,0,3_600_000_000_000),)
        self.assertEqual(call(service,self.request(job.result_id),token)[0],200)
        self.assertEqual(job.expires_ns,expiry)
    def test_prepare_window_rollover_charges_current_frame(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        emission=service(env(self.request(job.result_id),tokens[0]),lambda *a:None)
        amount=emission.prepared.reserved_bytes
        clock.n=60_000_000_000
        data=next(emission)
        self.assertEqual(json.loads(data)['kind'],'result')
        self.assertEqual(len(data),amount)
        self.assertEqual(service.ledger._transfer,amount)
        self.assertEqual(service.ledger._reserved,0)
    def test_second_controller_shares_prepared_transfer_cap(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        size=len(codec.encode(scheduler.result_visible(service.ledger.credentials[0],job.result_id,'1.1','owned-delivery-reference','result_read')))
        with service.ledger.lock:
            service.ledger._transfer=0;service.ledger._principal_transfer.clear()
            service.ledger.limits=Limits(60,size,size,60_000_000_000)
        first=service(env(self.request(job.result_id),tokens[0]),lambda *a:None)
        second=Service(service.ledger,jobs=scheduler)
        prepared=second._prepare(env(self.request(job.result_id),tokens[0]))
        self.assertEqual(prepared.status,429)
        first.close()
        self.assertEqual(service.ledger._reserved,0)
    def test_no_poll_result_expiry_during_other_native_execution(self):
        gate=threading.Event();gate.set()
        scheduler,service,tokens,clock,audit,job=self.make(gate=gate,cooperative=False)
        self.addCleanup(gate.set)
        original=job.result_id
        gate.clear();audit['entered'].clear()
        running=native_helpers.Jobs().submit(service,tokens[0],key='other-native')
        self.assertTrue(audit['entered'].wait(1))
        clock.n=job.expires_ns
        deadline=time.monotonic()+1
        while job.held and time.monotonic()<deadline:time.sleep(.01)
        self.assertEqual(job.held,0)
        self.assertEqual(scheduler.jobs[running].state,'running')
        self.assertEqual(call(service,self.request(original),tokens[0])[0],403)
        gate.set()
    def test_restart_epoch_and_failed_state_ids_nonvisible(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        self.assertEqual(call(service,self.request('another-epoch-'+job.result_id.split('-')[-1]),tokens[0])[0],403)
        for state in ('failed','cancelled'):
            with service.ledger.lock:job.state=state
            self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
    def test_raw_calculate_management_only_not_derived(self):
        for actions in [{'calculate','job_manage','retain'},{'raw_read','retain'}]:
            scheduler,service,tokens,clock,audit,job=self.make()
            service.ledger.grants=tuple(replace(g,actions=frozenset(actions)) if g.principal=='A' else g for g in service.ledger.grants)
            self.assertEqual(call(service,self.request(job.result_id),tokens[0])[0],403)
    def test_original_policy_dataset_scope_registry_pinned_before_delivery(self):
        for mutation in ('policy','revision','scope','registry'):
            for staged in (False,True):
                with self.subTest(mutation=mutation,staged=staged):
                    scheduler,service,tokens,clock,audit,job=self.make()
                    observed=[]
                    emission=(service(env(self.request(job.result_id,'artifact_read'),tokens[0]),lambda *a:observed.append(a))
                              if staged else None)
                    with service.ledger.lock:
                        dataset=service.ledger.datasets[job.dataset_id]
                        if mutation=='policy':
                            service.ledger.policy_revision='policy-v2'
                            service.ledger.grants=tuple(replace(g,policy_revision='policy-v2') for g in service.ledger.grants)
                        elif mutation=='revision':
                            dataset.identity=replace(dataset.identity,revision='owned-v2')
                            service.ledger.grants=tuple(replace(g,dataset_revision='owned-v2') for g in service.ledger.grants)
                        elif mutation=='scope':
                            dataset.scope=replace(dataset.scope,end_ns=dataset.scope.end_ns+1)
                            service.ledger.grants=tuple(replace(g,scope=dataset.scope) for g in service.ledger.grants)
                        else:service.ledger.registry_snapshot='f'*64
                    if emission is None:
                        self.assertEqual(call(service,self.request(job.result_id,'artifact_read'),tokens[0])[0],403)
                    else:
                        self.assertEqual(json.loads(next(emission))['kind'],'error')
                        self.assertTrue(observed[0][0].startswith('403'))
                        self.assertFalse(any(h[0]=='Content-Disposition' for h in observed[0][1]))
                    self.assertEqual(service.ledger._reserved,0)
                    self.assertEqual(audit['reads'],1)
    def test_expiry_during_native_verification_denies_at_final_visibility(self):
        for kind,status in [('ttl',403),('credential',401),('grant',403)]:
            scheduler,service,tokens,clock,audit,job=self.make()
            if kind=='credential':
                service.ledger.credentials=tuple(replace(c,expires_at_ns=200) if c.principal=='A' else c for c in service.ledger.credentials)
                boundary=200
            elif kind=='grant':
                service.ledger.grants=tuple(replace(g,expires_at_ns=200) if g.principal=='A' else g for g in service.ledger.grants)
                boundary=200
            else:boundary=job.expires_ns
            observed=[]
            emission=service(env(self.request(job.result_id,'artifact_read'),tokens[0]),lambda *a:observed.append(a))
            from equity_feature_service import jobs as runtime
            verify=runtime.verify_content
            def after_verify(*args):
                verify(*args);clock.n=boundary
            with patch.object(runtime,'verify_content',after_verify):
                self.assertEqual(json.loads(next(emission))['kind'],'error')
            self.assertTrue(observed[0][0].startswith(str(status)),observed)
            self.assertFalse(any(h[0]=='Content-Disposition' for h in observed[0][1]))
            self.assertEqual(service.ledger._reserved,0)
    def test_expiry_during_encoding_denies_at_final_visibility(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        observed=[]
        emission=service(env(self.request(job.result_id),tokens[0]),lambda *a:observed.append(a))
        encode=codec.encode
        def after_encode(frame):
            data=encode(frame)
            if frame['kind']=='result':clock.n=job.expires_ns
            return data
        with patch.object(codec,'encode',after_encode):
            self.assertEqual(json.loads(next(emission))['kind'],'error')
        self.assertTrue(observed[0][0].startswith('403'))
    def test_http_range_rejected_before_attachment(self):
        scheduler,service,tokens,clock,audit,job=self.make()
        request_env=env(self.request(job.result_id,'artifact_read'),tokens[0]);request_env['HTTP_RANGE']='bytes=0-10'
        observed=[];body=json.loads(next(iter(service(request_env,lambda *a:observed.append(a)))))
        self.assertEqual(body['kind'],'error')
        self.assertTrue(observed[0][0].startswith('400'))
        self.assertFalse(any(h[0]=='Content-Disposition' for h in observed[0][1]))
        self.assertEqual(audit['reads'],1)
    def test_original_grant_end_caps_retained_payload_ttl(self):
        scheduler,service,tokens,clock,audit=native_helpers.setup(actions=ACTIONS)
        self.addCleanup(lambda:scheduler.close(2))
        service.ledger.grants=tuple(replace(g,expires_at_ns=200) for g in service.ledger.grants)
        job=native_helpers.Jobs().wait_terminal(scheduler,native_helpers.Jobs().submit(service,tokens[0]))
        self.assertEqual(job.state,'succeeded')
        self.assertEqual((job.grant_expires_ns,job.expires_ns),(200,200))
        result_id=job.result_id;clock.n=199
        self.assertEqual(call(service,self.request(result_id),tokens[0])[0],200)
        clock.n=200
        self.assertEqual(call(service,self.request(result_id),tokens[0])[0],403)
        self.assertEqual(job.held,0)

if __name__=='__main__':unittest.main()
