"""Actual native worker jobs with independent owned arithmetic and barriers."""
from dataclasses import replace
import json
import http.client
import hashlib
from pathlib import Path
import secrets
import threading
import time
import unittest
from unittest.mock import patch

from equity_feature_contracts import (CanonicalBatch, Column, DataKind, BatchMetadata, SourceBinding,
    Coverage, PriceUnit, InputScope, ConfigSpec)
from equity_feature_contracts.adapters import AdapterBatch, AdapterCapabilities, AcquisitionRequest
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_contracts.results import ValueType
from equity_feature_io_contracts import FeatureHeader
from equity_feature_io_contracts.publication import SinkError, SinkErrorCode, CompletionReceipt, ArtifactReference
from equity_feature_io_sdk import SourceRegistry, SinkRegistry, decode_result, decode_envelope, encode_receipt
from equity_feature_example_extensions import ExampleSink
from equity_feature_workers import SessionCommandSpec, SourceOffer
from equity_feature_service import Credential, Grant, Ledger, Limits, Service, RawDataset, RawRead, Scope
from equity_feature_service.jobs import JobRegistration, JobGrant, JobScheduler, OwnedReceiptProfile
from equity_feature_service.loopback import qualification_server
from equity_feature_service import codec
from test_service import Clock, call, env

FROZEN = json.loads((Path(__file__).parent/'fixtures/jobs_native.json').read_text(encoding='utf-8'))
TRADE = next(f for f in FROZEN['fixtures'] if f['family'] == 'trades')


class Factory:
    protocol_version = '1'
    def __init__(self, create): self.construct = create
    def validate_config(self, config):
        if config: raise ValueError('unexpected')
        return {}
    def create(self, config, credentials): return self.construct()


class Credentials:
    def get(self, name): raise AssertionError('no owned credential lookup')


def setup(gate=None, sink_create=ExampleSink, cooperative=True, transform=lambda value:value, monotonic=time.monotonic_ns,
          family='trades', registered_batch_transform=lambda value:value, audit_out=None, rows=None,
          max_input_bytes=None, ns_offset=0, adjacent=False):
    fixture = json.loads(json.dumps(next(f for f in FROZEN['fixtures'] if f['family'] == family)))
    if ns_offset:
        def shift(value):
            if type(value) is dict:
                return {k:(str(int(v)+ns_offset) if type(v) is str else v+ns_offset)
                    if k.endswith('_ns') and v is not None else shift(v) for k,v in value.items()}
            if type(value) is list: return [shift(v) for v in value]
            return value
        fixture = shift(fixture)
    native = fixture['native_spec']
    if rows is not None:
        if family != 'trades': raise ValueError('owned row fixture is trades')
        native['request']['max_rows'] = native['request']['max_batch_rows'] = rows
        fixture['request']['payload']['context']['dataset']['input_id'] = 'trades-'+str(rows)
    if max_input_bytes is not None:
        native['max_input_bytes'] = max_input_bytes
    config = ConfigSpec.from_json(json.dumps(native['config']))
    if rows is not None or ns_offset:
        payload = fixture['request']['payload']
        payload['context']['config']['digest'] = config.digest
        payload['command_digest'] = hashlib.sha256(json.dumps({k:v for k,v in payload.items() if k not in ('command_digest','idempotency_key')},
            sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')).hexdigest()
    request_fields = dict(native['request'])
    request_fields.update(kind=DataKind(request_fields['kind']),price_unit=config.price_unit,adjustment=config.adjustment,
                          availability=config.availability,instruments=tuple(request_fields['instruments']),sessions=tuple(request_fields['sessions']))
    request = AcquisitionRequest(**request_fields)
    spec = SessionCommandSpec(native['job_id'],native['generation_id'],native['partition_id'],family,request,config,
        tuple(FeatureHeader(**(f|{'dtype':ValueType(f['dtype'])})) for f in native['features']),tuple(IntervalSpec(**g) for g in native['governed_sessions']),
        native['revision_id'],native['destination_scope'],native['max_input_bytes'])
    facts = {
        'trades':{'instrument_id':('A',)*3,'session_id':('S',)*3,'event_ns':(110,130,160),'order_key':(1,2,3),
             'event_id':('t1','t2','t3'),'eligible':(True,)*3,'price':(100,102,101),'size':(2,3,5),'known_at_ns':(110,130,210)},
        'bars':{'instrument_id':('A','A'),'session_id':('S','S'),'start_ns':(100,150),'end_ns':(150,200),
             'known_at_ns':(150,210),'open':(100,102),'high':(103,104),'low':(99,101),'close':(102,103),
             'volume':(200,300),'actual_notional':(20300,30900)},
        'quotes':{'instrument_id':('A',)*4,'session_id':('S',)*4,'event_ns':(110,120,130,140),'order_key':(1,2,3,4),
             'event_id':('q1','q2','q3','q4'),'bid':(100,101,105,None),'ask':(102,101,104,102),'known_at_ns':(110,120,130,140)},
    }[family]
    if rows is not None:
        facts = {'instrument_id':('A',)*rows,'session_id':('S',)*rows,'event_ns':tuple(100+n for n in range(rows)),
            'order_key':tuple(range(rows)),'event_id':tuple('row-'+str(n) for n in range(rows)),'eligible':(True,)*rows,
            'price':(100,)*rows,'size':tuple(n+1 for n in range(rows)),'known_at_ns':tuple(100+n for n in range(rows))}
    if adjacent:
        facts['event_ns'] = (110,111,112)
        facts['known_at_ns'] = (110,111,112)
    if ns_offset:
        facts = {k:tuple(v+ns_offset for v in values) if k.endswith('_ns') else values for k,values in facts.items()}
    rows = len(facts['instrument_id'])
    batch = CanonicalBatch(request.kind,tuple(Column(k,v) for k,v in facts.items()),
        BatchMetadata('demo',SourceBinding('synthetic','snapshot1','map1',fixture['request']['payload']['context']['dataset']['input_id']),Coverage(rows,rows,True),PriceUnit(0,'USD'),
                      sampling=request.sampling,
                      scope=InputScope(request.start_ns,request.end_ns,'example-v1')))
    admitted_batch = batch
    batch = registered_batch_transform(batch)
    delivery = AdapterBatch(request.request_id,0,True,batch.metadata.source,batch.metadata.coverage,Coverage(rows,rows,True),batch)
    audit = {'reads':0,'source_threads':[],'sink_threads':[],'entered':threading.Event(),'batch':batch,'spec':spec}
    if audit_out is not None:
        audit_out.update(audit)
        audit = audit_out
    capabilities = AdapterCapabilities((request.kind,),('demo',),(PriceUnit(0,'USD'),),sampling=(request.sampling,),max_batch_rows=rows)
    class Source:
        def capabilities(self): return capabilities
        def iter_batches(self, observed, cancellation):
            audit['reads'] += 1
            audit['source_threads'].append(threading.get_ident())
            audit['entered'].set()
            if gate is not None:
                while not gate.wait(0.01):
                    if cooperative and cancellation.is_cancelled(): return
            if cancellation.is_cancelled(): return
            yield transform(delivery)
    sources,sinks = SourceRegistry(),SinkRegistry()
    sources.register('owned.source',Factory(Source))
    def construct_sink():
        audit['sink_threads'].append(threading.get_ident())
        return sink_create()
    sinks.register('owned.sink',Factory(construct_sink))
    registration = JobRegistration(fixture['request'],spec,delivery,sources=sources,source=SourceOffer('owned.source',capabilities,{}),
                                    sinks=sinks,sink_id='owned.sink',sink_config={},credentials=Credentials(),receipt_profile=OwnedReceiptProfile(),
                                    acquisition_receipt_fingerprint='a'*64)
    clock = Clock()
    scope = Scope('A','S',request.start_ns,request.end_ns)
    columns = tuple(facts)
    rights = frozenset(('calculate','job_manage','retain','discover'))
    dataset_id = 'owned.'+family
    dataset = RawDataset(dataset_id,'owned-v1',scope,RawRead(admitted_batch,'a'*64),lambda cancellation:RawRead(admitted_batch,'a'*64),
                         columns=columns,rights=rights,rights_owner='owned-test',rights_evidence='owned-native-fixture',valid_from_ns=0,expires_at_ns=3_600_000_000_000)
    tokens = (secrets.token_urlsafe(32),secrets.token_urlsafe(32))
    credentials = tuple(Credential.provision(p,t,0,3_600_000_000_000) for p,t in zip(('A','B'),tokens))
    grants = tuple(Grant('grant-'+p,p,dataset_id,'owned-v1','policy-v1',scope,frozenset(columns),rights,0,3_600_000_000_000) for p in ('A','B'))
    ledger = Ledger(clock=clock,limits=Limits(60,1_048_576,2_097_152,60_000_000_000),credentials=credentials,grants=grants,datasets=(dataset,),
                    policy_revision='policy-v1',registry_snapshot=fixture['request']['payload']['context']['registry_snapshot'])
    scheduler = JobScheduler(ledger,(registration,),tuple(JobGrant(g.grant_id,config.digest,registration.execution_features) for g in grants),monotonic=monotonic)
    return scheduler,Service(ledger,jobs=scheduler),tokens,clock,audit


class Jobs(unittest.TestCase):
    def make(self, **kwargs):
        scheduler,service,tokens,clock,audit = setup(**kwargs)
        self.addCleanup(lambda:scheduler.close(2))
        return scheduler,service,tokens,clock,audit
    def request(self, operation, **fields):
        return {'schema':'equity.remote','version':'1.0','kind':'request','request_id':'owned-request',
                'payload':{'operation':operation,**fields}}
    def submit(self, service, token, key='owned-key'):
        registration = next(iter(service.jobs.registrations.values()))
        payload = json.loads(registration.payload_bytes)
        payload.update(command_digest=registration.command_digest,idempotency_key=key)
        wire = self.request('calculate')
        wire['payload'] = payload
        status,body,*_ = call(service,wire,token)
        self.assertEqual(status,200,body)
        return body['payload']['job_id']
    def wait_terminal(self, scheduler, job_id):
        end = time.monotonic()+3
        with scheduler.condition:
            while scheduler.jobs[job_id].state in ('queued','running','cancel_requested'):
                remaining = end-time.monotonic()
                self.assertGreater(remaining,0,'native job did not exit')
                scheduler.condition.wait(remaining)
        return scheduler.jobs[job_id]
    def test_native_worker_completion_full_goldens_and_thread_ownership(self):
        scheduler,service,tokens,clock,audit = self.make()
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'succeeded',job.error)
        native = decode_result(job.native)
        values = {c.feature_id:c.values[0] for c in native.values}
        self.assertEqual(values['session.trade.count'],3)
        self.assertEqual(values['session.trade.volume'],10)
        self.assertEqual(values['session.trade.notional'],1011)
        self.assertTrue(job.receipt and job.wire)
        self.assertEqual(audit['reads'],1)
        self.assertEqual(audit['source_threads'],audit['sink_threads'])
        self.assertNotEqual(audit['source_threads'][0],threading.get_ident())
    def test_blocked_native_source_http_responsive_and_retry_once(self):
        gate = threading.Event()
        scheduler,service,tokens,clock,audit = self.make(gate=gate)
        job_id = self.submit(service,tokens[0])
        self.assertTrue(audit['entered'].wait(2))
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        status,body,*_ = call(service,self.request('job_status',job_id=job_id),tokens[0])
        self.assertEqual((status,body['payload']['state']),(200,'running'))
        self.assertEqual(scheduler._running,job_id)
        gate.set()
        self.assertEqual(self.wait_terminal(scheduler,job_id).state,'succeeded')
        self.assertEqual(audit['reads'],1)
    def test_running_cancel_is_cooperative_no_duplicate_retry(self):
        gate = threading.Event()
        scheduler,service,tokens,clock,audit = self.make(gate=gate)
        job_id = self.submit(service,tokens[0])
        self.assertTrue(audit['entered'].wait(2))
        status,body,*_ = call(service,self.request('job_cancel',job_id=job_id),tokens[0])
        self.assertEqual(status,200)
        self.assertIn(body['payload']['state'],('cancel_requested','cancelled'))
        self.assertEqual(self.wait_terminal(scheduler,job_id).state,'cancelled')
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)
    def test_foreign_status_cancel_denied_without_disclosure(self):
        scheduler,service,tokens,clock,audit = self.make()
        job_id = self.submit(service,tokens[0])
        for operation in ('job_status','job_cancel'):
            foreign = call(service,self.request(operation,job_id=job_id),tokens[1])
            unknown = call(service,self.request(operation,job_id='unknown'),tokens[1])
            self.assertEqual((foreign[0],foreign[1]['payload']),(unknown[0],unknown[1]['payload']))
            self.assertEqual(foreign[0],403)
    def test_payload_expiry_keeps_idempotency_tombstone(self):
        scheduler,service,tokens,clock,audit = self.make()
        job_id = self.submit(service,tokens[0])
        self.wait_terminal(scheduler,job_id)
        clock.n += 300_000_000_000
        status,body,*_ = call(service,self.request('job_status',job_id=job_id),tokens[0])
        self.assertEqual((status,body['payload']['state']),(200,'expired'))
        job = scheduler.jobs[job_id]
        self.assertFalse(job.native or job.receipt or job.envelope or job.wire)
        self.assertEqual(job.held,0)
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)
    def test_revoked_job_management_has_no_visible_chunk(self):
        scheduler,service,tokens,clock,audit = self.make()
        job_id = self.submit(service,tokens[0])
        self.wait_terminal(scheduler,job_id)
        scheduler.ledger.revoke('grant-A')
        status,body,*_ = call(service,self.request('job_status',job_id=job_id),tokens[0])
        self.assertEqual(status,403)
        self.assertEqual(body['payload']['code'],'not_permitted')
    def test_second_controller_shares_exact_scheduler(self):
        scheduler,service,tokens,clock,audit = self.make()
        another = Service(scheduler.ledger,jobs=scheduler)
        job_id = self.submit(service,tokens[0])
        self.assertEqual(self.submit(another,tokens[0]),job_id)
        self.wait_terminal(scheduler,job_id)
        self.assertEqual(audit['reads'],1)

    def test_exact_queue_caps_and_queued_cancel_release_once(self):
        gate = threading.Event()
        scheduler,service,tokens,clock,audit = self.make(gate=gate)
        running = self.submit(service,tokens[0],'running')
        self.assertTrue(audit['entered'].wait(2))
        queued_a = self.submit(service,tokens[0],'queued-a')
        queued_b = self.submit(service,tokens[1],'queued-b')
        self.assertEqual(len(scheduler.queue),2)
        for token in tokens:
            wire = json.loads(json.dumps(TRADE['request']))
            wire['payload']['idempotency_key'] = 'excess'
            self.assertEqual(call(service,wire,token)[0],429)
        reserved = scheduler.total_retained
        for _ in range(2):
            status,body,*_ = call(service,self.request('job_cancel',job_id=queued_b),tokens[1])
            self.assertEqual((status,body['payload']['state']),(200,'cancelled'))
        self.assertEqual(scheduler.total_retained,reserved-262144)
        self.assertEqual(audit['reads'],1)
        gate.set()
        self.assertEqual(self.wait_terminal(scheduler,running).state,'succeeded')
        self.assertEqual(self.wait_terminal(scheduler,queued_a).state,'succeeded')
        self.assertEqual(audit['reads'],2)

    def test_revoked_queued_job_has_finite_tombstone_and_no_source_call(self):
        gate = threading.Event()
        scheduler,service,tokens,clock,audit = self.make(gate=gate)
        running = self.submit(service,tokens[0],'running')
        self.assertTrue(audit['entered'].wait(2))
        queued = self.submit(service,tokens[1],'queued')
        scheduler.ledger.revoke('grant-B')
        with scheduler.condition:
            scheduler._sweep()
        self.assertEqual(scheduler.jobs[queued].state,'cancelled')
        self.assertEqual(scheduler.jobs[queued].expires_ns,clock.n+300_000_000_000)
        self.assertEqual(scheduler.jobs[queued].held,0)
        clock.n += 300_000_000_000
        with scheduler.condition:
            scheduler._sweep()
        self.assertEqual(scheduler.jobs[queued].state,'expired')
        gate.set()
        self.wait_terminal(scheduler,running)
        self.assertEqual(audit['reads'],1)

    def test_cancelled_native_work_keeps_slot_until_real_exit(self):
        gate = threading.Event()
        self.addCleanup(gate.set)
        scheduler,service,tokens,clock,audit = self.make(gate=gate,cooperative=False)
        running = self.submit(service,tokens[0],'running')
        self.assertTrue(audit['entered'].wait(2))
        queued = self.submit(service,tokens[1],'queued')
        call(service,self.request('job_cancel',job_id=running),tokens[0])
        self.assertEqual(scheduler._running,running)
        self.assertEqual(scheduler.jobs[running].state,'cancel_requested')
        self.assertEqual(scheduler.jobs[running].held,262144)
        self.assertEqual(audit['reads'],1)
        gate.set()
        self.assertEqual(self.wait_terminal(scheduler,running).state,'cancelled')
        self.assertEqual(self.wait_terminal(scheduler,queued).state,'succeeded')

    def test_native_readback_failure_preserves_committed_receipt_and_retry(self):
        class ReadFailure(ExampleSink):
            def read(self, receipt): raise SinkError(SinkErrorCode.INVALID_CONTENT)
        scheduler,service,tokens,clock,audit = self.make(sink_create=ReadFailure)
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'failed')
        self.assertTrue(job.receipt and job.native)
        self.assertIsNone(job.result_id)
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)

    def test_native_cancel_after_commit_preserves_receipt_without_result(self):
        owner = {}
        class CommitCancel(ExampleSink):
            def commit(self, session):
                receipt = super().commit(session)
                scheduler = owner['scheduler']
                scheduler.jobs[scheduler._running].cancelled = True
                return receipt
        scheduler,service,tokens,clock,audit = self.make(sink_create=CommitCancel)
        owner['scheduler'] = scheduler
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'cancelled')
        self.assertTrue(job.receipt and job.native)
        self.assertIsNone(job.result_id)
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)

    def test_native_source_binding_mutation_fails_before_publication(self):
        scheduler,service,tokens,clock,audit = self.make(transform=lambda d:replace(d,source=replace(d.source,snapshot_id='foreign')))
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'failed')
        self.assertFalse(job.receipt or job.wire)
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)

    def test_deadline_cancels_without_releasing_blocked_native_slot(self):
        mono = [100]
        gate = threading.Event()
        self.addCleanup(gate.set)
        scheduler,service,tokens,clock,audit = self.make(gate=gate,cooperative=False,monotonic=lambda:mono[0])
        job_id = self.submit(service,tokens[0])
        self.assertTrue(audit['entered'].wait(2))
        mono[0] = 30_000_000_100
        self.assertEqual(scheduler._running,job_id)
        self.assertEqual(scheduler.jobs[job_id].held,262144)
        gate.set()
        self.assertEqual(self.wait_terminal(scheduler,job_id).state,'cancelled')

    def test_bad_deadline_clock_does_not_kill_shared_execution_thread(self):
        mono = [100]
        scheduler,service,tokens,clock,audit = self.make(monotonic=lambda:mono[0])
        with scheduler.condition:
            mono[0] = 99
            job_id = self.submit(service,tokens[0])
        self.assertEqual(self.wait_terminal(scheduler,job_id).state,'failed')
        self.assertTrue(scheduler.thread.is_alive())
        self.assertIsNone(scheduler._running)
        self.assertEqual(audit['reads'],0)
        mono[0] = 100
        next_id = self.submit(service,tokens[0],'fresh')
        self.assertEqual(self.wait_terminal(scheduler,next_id).state,'succeeded')

    def test_actual_loopback_two_principals_native_job_and_poll(self):
        scheduler,service,tokens,clock,audit = self.make()
        server = qualification_server(service)
        worker = threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.05})
        worker.start()
        try:
            host,port = server.server_address[:2]
            def post(request, token):
                connection = http.client.HTTPConnection(host,port,timeout=3)
                try:
                    connection.request('POST','/v1/request',codec.canonical(request),
                        {'Content-Type':'application/json','Authorization':'Bearer '+token})
                    response = connection.getresponse()
                    return response.status,json.loads(response.read())
                finally: connection.close()
            status,body = post(TRADE['request'],tokens[0])
            self.assertEqual(status,200,body)
            job_id = body['payload']['job_id']
            job = self.wait_terminal(scheduler,job_id)
            status,body = post(self.request('job_status',job_id=job_id),tokens[0])
            self.assertEqual((status,body['payload']['state']),(200,'succeeded'))
            self.assertEqual(body['payload']['result_id'],job.result_id)
            self.assertEqual(post(self.request('job_status',job_id=job_id),tokens[1])[0],403)
        finally:
            server.shutdown(); worker.join(3); server.server_close()

    def test_same_key_other_principal_independent_and_correlation_ignored(self):
        scheduler,service,tokens,clock,audit = self.make()
        first = self.submit(service,tokens[0])
        self.wait_terminal(scheduler,first)
        request = json.loads(json.dumps(TRADE['request']))
        request['payload']['idempotency_key'] = 'owned-key'
        request['request_id'] = 'different-correlation'
        status,body,*_ = call(service,request,tokens[0])
        self.assertEqual((status,body['payload']['job_id']),(200,first))
        second = self.submit(service,tokens[1])
        self.assertNotEqual(first,second)
        self.wait_terminal(scheduler,second)
        self.assertEqual(audit['reads'],2)
        self.assertEqual(scheduler.jobs[first].native,scheduler.jobs[second].native)

    def test_successful_cancel_is_idempotent_receipt_preserved(self):
        scheduler,service,tokens,clock,audit = self.make()
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        receipt,result_id = job.receipt,job.result_id
        for _ in range(2):
            status,body,*_ = call(service,self.request('job_cancel',job_id=job_id),tokens[0])
            self.assertEqual((status,body['payload']['state']),(200,'succeeded'))
        self.assertEqual((job.receipt,job.result_id),(receipt,result_id))

    def test_stale_digest_and_extra_code_rejected_before_source(self):
        scheduler,service,tokens,clock,audit = self.make()
        stale = json.loads(json.dumps(TRADE['request']))
        stale['payload']['command_digest'] = '0'*64
        self.assertEqual(call(service,stale,tokens[0])[0],422)
        extra = json.loads(json.dumps(TRADE['request']))
        extra['payload']['code'] = 'import os'
        self.assertEqual(call(service,extra,tokens[0])[0],400)
        self.assertEqual((audit['reads'],len(scheduler.jobs)),(0,0))

    def test_changed_config_same_key_conflicts_without_second_execution(self):
        scheduler,service,tokens,clock,audit = self.make()
        original = self.submit(service,tokens[0])
        self.wait_terminal(scheduler,original)
        changed = json.loads(json.dumps(TRADE['request']))
        changed['payload']['idempotency_key'] = 'owned-key'
        changed['payload']['context']['config']['digest'] = 'b'*64
        self.assertEqual(call(service,changed,tokens[0])[0],422)
        self.assertEqual(audit['reads'],1)

    def test_eight_live_tombstones_deny_ninth_without_eviction(self):
        scheduler,service,tokens,clock,audit = self.make()
        ids = []
        for n in range(8):
            job_id = self.submit(service,tokens[0],'record-'+str(n))
            self.wait_terminal(scheduler,job_id)
            ids.append(job_id)
        request = json.loads(json.dumps(TRADE['request']))
        request['payload']['idempotency_key'] = 'ninth'
        self.assertEqual(call(service,request,tokens[0])[0],429)
        clock.n += 300_000_000_000
        with scheduler.condition: scheduler._sweep()
        self.assertEqual(len(scheduler.jobs),8)
        self.assertEqual(scheduler.total_retained,0)
        self.assertEqual(call(service,request,tokens[0])[0],429)
        self.assertEqual(self.submit(service,tokens[0],'record-0'),ids[0])
        self.assertEqual(audit['reads'],8)

    def test_invalid_idle_wall_clock_preserves_executor_and_future_admission(self):
        scheduler,service,tokens,clock,audit = self.make()
        observed = threading.Event()
        def faulty_clock():
            if threading.current_thread() is scheduler.thread and clock.n < 0:
                observed.set()
            return clock.n
        scheduler.ledger.clock = faulty_clock
        clock.n = -1
        self.assertTrue(observed.wait(2),'scheduler did not observe invalid trusted time')
        with scheduler.condition:
            clock.n = 100
        job_id = self.submit(service,tokens[0])
        self.assertEqual(self.wait_terminal(scheduler,job_id).state,'succeeded')
        self.assertTrue(scheduler.thread.is_alive())

    def test_invalid_wall_clock_during_native_exit_releases_uncommitted_forms(self):
        gate = threading.Event()
        scheduler,service,tokens,clock,audit = self.make(gate=gate)
        job_id = self.submit(service,tokens[0])
        self.assertTrue(audit['entered'].wait(2))
        clock.n = -1
        gate.set()
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'cancelled')
        self.assertEqual(job.held,0)
        self.assertTrue(scheduler.thread.is_alive())
        clock.n = 100

    def test_failure_before_native_commit_clears_staged_payload_and_reservation(self):
        class NeverCommit(ExampleSink):
            def commit(self, session): raise SinkError(SinkErrorCode.UNAVAILABLE)
        scheduler,service,tokens,clock,audit = self.make(sink_create=NeverCommit)
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'failed')
        self.assertFalse(job.native or job.envelope or job.receipt or job.wire)
        self.assertEqual((job.held,scheduler.total_retained),(0,0))
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)

    def test_commit_unknown_recovery_retains_exact_receipt_before_success(self):
        class CommitUnknown(ExampleSink):
            def commit(self, session):
                super().commit(session)
                raise SinkError(SinkErrorCode.COMMIT_UNKNOWN)
        scheduler,service,tokens,clock,audit = self.make(sink_create=CommitUnknown)
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'succeeded',job.error)
        self.assertTrue(job.native and job.envelope and job.receipt and job.wire)
        self.assertIsNotNone(job.committed_receipt_sha256)
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)

    def test_out_of_profile_committed_receipt_denies_success_and_records_fact(self):
        class OversizedReceipt(ExampleSink):
            def commit(self, session):
                receipt = super().commit(session)
                return replace(receipt,artifacts=(replace(receipt.artifacts[0],artifact_id='x'*65000),))
        scheduler,service,tokens,clock,audit = self.make(sink_create=OversizedReceipt)
        job_id = self.submit(service,tokens[0])
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'failed')
        self.assertIsNone(job.result_id)
        self.assertTrue(job.receipt_profile_failed)
        self.assertEqual(job.error,{'category':'contract','code':'bounds','retryable':False})
        self.assertIsNotNone(job.committed_receipt_sha256)
        self.assertLessEqual(job.held,262144)
        self.assertLessEqual(len(job.receipt)+len(job.envelope),32768)
        self.assertEqual(self.submit(service,tokens[0]),job_id)
        self.assertEqual(audit['reads'],1)

    def test_invalid_idle_clock_cannot_prevent_empty_scheduler_shutdown(self):
        scheduler,service,tokens,clock,audit = self.make()
        clock.n = -1
        self.assertTrue(scheduler.close(2))
        self.assertFalse(scheduler.thread.is_alive())
        self.assertEqual((len(scheduler.queue),scheduler._running),(0,None))

    def test_worst_escaped_receipt_exact_precommit_boundary(self):
        scheduler,service,tokens,clock,audit = self.make()
        baseline = self.wait_terminal(scheduler,self.submit(service,tokens[0]))
        envelope = decode_envelope(baseline.envelope)
        worst = CompletionReceipt(envelope.identity,'f'*64,'f'*64,1,2**63-1,2**63-1,2**63-1,
            (ArtifactReference('"'*128,'f'*64,2**63-1),),-2**63)
        unescaped = replace(worst,artifacts=(replace(worst.artifacts[0],artifact_id='x'*128),))
        self.assertEqual(len(encode_receipt(worst))-len(encode_receipt(unescaped)),128)
        from equity_feature_service import jobs
        original = jobs.encode_envelope
        for excess,expected in ((0,'succeeded'),(1,'failed')):
            begins = []
            class CountBegin(ExampleSink):
                def begin(self, value):
                    begins.append(value)
                    return super().begin(value)
            native,controller,owned,_,facts = self.make(sink_create=CountBegin)
            target = 32768-len(encode_receipt(worst))+excess
            def padded(value):
                encoded = original(value)
                self.assertLessEqual(len(encoded),target)
                return encoded+b' '*(target-len(encoded))
            with patch.object(jobs,'encode_envelope',padded):
                job = self.wait_terminal(native,self.submit(controller,owned[0]))
            self.assertEqual(job.state,expected,job.error)
            self.assertEqual(len(begins),0 if excess else 1)
            if excess:
                self.assertFalse(job.committed_receipt_sha256)
                self.assertEqual(job.held,0)

    def test_three_native_families_match_independent_frozen_full_results(self):
        from equity_features.session import compute_bars, compute_trades, compute_quotes
        from equity_feature_contracts import EntityKey
        calculators = {'trades':compute_trades,'bars':compute_bars,'quotes':compute_quotes}
        for family in calculators:
            with self.subTest(family=family):
                scheduler,service,tokens,clock,audit = self.make(family=family)
                job = self.wait_terminal(scheduler,self.submit(service,tokens[0]))
                self.assertEqual(job.state,'succeeded',job.error)
                expected = next(r for r in FROZEN['records'] if r.get('family') == family)
                self.assertEqual(hashlib.sha256(job.native).hexdigest(),expected['native_result_sha256'])
                native = decode_result(job.native)
                direct = calculators[family](audit['batch'],audit['spec'].config,entity=EntityKey('A','S'))
                self.assertEqual(native.values,direct.values)
                self.assertEqual(native.quality,direct.quality)
                self.assertEqual(native.evidence,direct.evidence)
                payload = json.loads(job.wire)['feature_result']
                self.assertEqual(payload['metadata'],codec.cell(native.metadata))
                self.assertEqual([q['status'] for q in payload['quality']],[q.status.value for q in native.quality])
                values = {v.feature_id:v.values[0] for v in native.values}
                if family == 'bars':
                    self.assertEqual((values['session.bar.volume'],values['session.bar.notional']),(500,51200))
                    self.assertIsNone(values['session.price.overnight_gap'])
                    quality = next(q for q in native.quality if q.feature_id == 'session.price.overnight_gap')
                    self.assertNotEqual(quality.status.value,'available')
                elif family == 'quotes':
                    self.assertEqual(values['session.quote.sampled_spread'].mean_spread,1.0)
                    self.assertEqual(len(native.evidence),4)
                else:
                    self.assertEqual((values['session.trade.count'],values['session.trade.volume'],values['session.trade.notional']),(3,10,1011))

    def test_native_registration_and_nested_config_remain_immutable(self):
        scheduler,service,tokens,clock,audit = self.make()
        registration = next(iter(scheduler.registrations.values()))
        original = registration.registration_digest
        with self.assertRaises(AttributeError):
            registration.spec = replace(registration.spec,revision_id='unapproved')
        with self.assertRaises(AttributeError):
            del registration._sealed
        with self.assertRaises(TypeError):
            scheduler.registrations['other'] = registration
        leaked = registration.sink_config
        leaked['unapproved'] = 'configuration'
        self.assertEqual(registration.sink_config,{})
        self.assertEqual(registration.registration_digest,original)
        self.assertEqual(self.wait_terminal(scheduler,self.submit(service,tokens[0])).state,'succeeded')

    def test_mutable_feature_permissions_rejected_at_trusted_startup(self):
        with self.assertRaisesRegex(ValueError,'invalid_job_permission'):
            JobGrant('grant-A','a'*64,{('session.trade.volume','v1')})

    def test_same_identity_different_admitted_content_denied_before_factory(self):
        audit = {}
        def changed_prices(batch):
            return replace(batch,columns=tuple(replace(c,values=(200,202,201)) if c.name == 'price' else c for c in batch.columns))
        with self.assertRaisesRegex(ValueError,'unadmitted_job_dataset'):
            setup(registered_batch_transform=changed_prices,audit_out=audit)
        self.assertEqual(audit['reads'],0)
        self.assertEqual(audit['source_threads'],[])
        self.assertEqual(audit['sink_threads'],[])

    def test_no_poll_physical_expiry_while_other_native_job_is_busy(self):
        gate = threading.Event()
        gate.set()
        scheduler,service,tokens,clock,audit = self.make(gate=gate)
        first_id = self.submit(service,tokens[0],'first')
        first = self.wait_terminal(scheduler,first_id)
        self.assertTrue(first.native and first.wire)
        gate.clear(); audit['entered'].clear()
        second_id = self.submit(service,tokens[1],'second')
        self.assertTrue(audit['entered'].wait(2))
        expired = threading.Event()
        clear = scheduler._clear
        def observe_clear(job):
            clear(job)
            if job is first: expired.set()
        with patch.object(scheduler,'_clear',observe_clear):
            clock.n = first.expires_ns
            self.assertTrue(expired.wait(2),'busy executor stalled no-poll expiry')
        with scheduler.condition:
            self.assertEqual(first.state,'expired')
            self.assertFalse(first.native or first.receipt or first.envelope or first.wire)
            self.assertEqual(first.held,0)
            self.assertEqual(scheduler._running,second_id)
            self.assertEqual(scheduler.jobs[second_id].held,262144)
        gate.set()
        self.assertEqual(self.wait_terminal(scheduler,second_id).state,'succeeded')

    def test_retention_right_revoked_during_acquisition_stops_publication(self):
        gate = threading.Event()
        scheduler,service,tokens,clock,audit = self.make(gate=gate)
        job_id = self.submit(service,tokens[0])
        self.assertTrue(audit['entered'].wait(2))
        with scheduler.ledger.lock:
            scheduler.ledger.grants = tuple(replace(g,actions=g.actions-{'retain'}) if g.principal == 'A' else g
                for g in scheduler.ledger.grants)
        gate.set()
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'cancelled')
        self.assertFalse(job.receipt or job.wire)
        self.assertEqual(job.held,0)

    def test_each_job_action_is_required_without_implicit_slice_rights(self):
        for action in ('calculate','job_manage','retain'):
            with self.subTest(action=action):
                scheduler,service,tokens,clock,audit = self.make()
                scheduler.ledger.grants = tuple(replace(g,actions=g.actions-{action}) for g in scheduler.ledger.grants)
                self.assertEqual(call(service,TRADE['request'],tokens[0])[0],403)
                self.assertEqual((len(scheduler.jobs),audit['reads']),(0,0))

    def test_token_and_grant_half_open_expiry_are_distinct_before_native_work(self):
        for field,status in (('credentials',401),('grants',403)):
            with self.subTest(field=field):
                scheduler,service,tokens,clock,audit = self.make()
                setattr(scheduler.ledger,field,tuple(replace(value,expires_at_ns=100)
                    for value in getattr(scheduler.ledger,field)))
                self.assertEqual(call(service,TRADE['request'],tokens[0])[0],status)
                self.assertEqual((len(scheduler.jobs),audit['reads']),(0,0))

    def test_job_http_first_chunk_reauthorizes_after_preparation(self):
        scheduler,service,tokens,clock,audit = self.make()
        job_id = self.submit(service,tokens[0])
        self.wait_terminal(scheduler,job_id)
        statuses = []
        emission = service(env(self.request('job_status',job_id=job_id),tokens[0]),lambda status,headers:statuses.append(status))
        scheduler.ledger.revoke('grant-A')
        chunks = tuple(emission)
        self.assertEqual(len(chunks),1)
        self.assertTrue(statuses[0].startswith('403'))
        self.assertNotIn(job_id.encode(),chunks[0])
        self.assertEqual(json.loads(chunks[0])['payload']['code'],'not_permitted')

    def test_revocation_before_commit_prevents_native_publication(self):
        owner,commits = {},[]
        gate = threading.Event()
        class RevokeAfterWrite(ExampleSink):
            def write(self, session, ordinal, result):
                super().write(session,ordinal,result)
                owner['scheduler'].ledger.revoke('grant-A')
            def commit(self, session):
                commits.append(session)
                return super().commit(session)
        scheduler,service,tokens,clock,audit = self.make(sink_create=RevokeAfterWrite,gate=gate)
        owner['scheduler'] = scheduler
        job_id = self.submit(service,tokens[0])
        gate.set()
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'cancelled')
        self.assertEqual(commits,[])
        self.assertIsNone(job.committed_receipt_sha256)
        self.assertEqual(job.held,0)

    def test_revocation_after_commit_denies_success_preserves_only_history_marker(self):
        owner = {}
        gate = threading.Event()
        class RevokeAfterCommit(ExampleSink):
            def commit(self, session):
                receipt = super().commit(session)
                owner['scheduler'].ledger.revoke('grant-A')
                return receipt
        scheduler,service,tokens,clock,audit = self.make(sink_create=RevokeAfterCommit,gate=gate)
        owner['scheduler'] = scheduler
        job_id = self.submit(service,tokens[0])
        gate.set()
        job = self.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'cancelled')
        self.assertIsNotNone(job.committed_receipt_sha256)
        self.assertIsNone(job.result_id)
        self.assertFalse(job.native or job.envelope or job.receipt or job.wire)
        self.assertEqual(job.held,0)

    def test_native_hundred_row_allow_and_hundred_one_deny_before_execution(self):
        scheduler,service,tokens,clock,audit = self.make(rows=100)
        job = self.wait_terminal(scheduler,self.submit(service,tokens[0]))
        self.assertEqual(job.state,'succeeded',job.error)
        values = {v.feature_id:v.values[0] for v in decode_result(job.native).values}
        self.assertEqual((values['session.trade.count'],values['session.trade.volume'],values['session.trade.notional']),(100,5050,505000))
        self.assertEqual(audit['reads'],1)
        with self.assertRaisesRegex(ValueError,'invalid_job_registration'):
            setup(rows=101)

    def test_declared_native_input_limit_one_mebibyte_inclusive(self):
        scheduler,service,tokens,clock,audit = self.make(max_input_bytes=1048576)
        self.assertEqual(self.wait_terminal(scheduler,self.submit(service,tokens[0])).state,'succeeded')
        with self.assertRaisesRegex(ValueError,'invalid_job_registration'):
            setup(max_input_bytes=1048577)

    def test_adjacent_large_int64_ns_native_and_wire_precision(self):
        from equity_features.session import compute_trades
        from equity_feature_contracts import EntityKey
        offset = 9_000_000_000_000_000_000
        scheduler,service,tokens,clock,audit = self.make(ns_offset=offset,adjacent=True)
        job = self.wait_terminal(scheduler,self.submit(service,tokens[0]))
        self.assertEqual(job.state,'succeeded',job.error)
        native = decode_result(job.native)
        direct = compute_trades(audit['batch'],audit['spec'].config,entity=EntityKey('A','S'))
        self.assertEqual(native.values,direct.values)
        self.assertEqual(native.quality,direct.quality)
        self.assertEqual(native.evidence,direct.evidence)
        actual = audit['batch'].column('event_ns').values
        self.assertEqual(actual,(offset+110,offset+111,offset+112))
        self.assertNotEqual(actual[0],actual[1])
        payload = json.loads(job.wire)['feature_result']
        self.assertEqual(payload['metadata'],codec.cell(native.metadata))
        self.assertIn(str(offset+100).encode(),job.wire)
        self.assertIn(str(offset+200).encode(),job.wire)
        self.assertEqual(clock.n,100,'numerical time must not become authorization time')


if __name__ == '__main__': unittest.main()
