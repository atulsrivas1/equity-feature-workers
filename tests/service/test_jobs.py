"""Actual native worker jobs with independent owned arithmetic and barriers."""
from dataclasses import replace
import json
import http.client
from pathlib import Path
import secrets
import threading
import time
import unittest

from equity_feature_contracts import (CanonicalBatch, Column, DataKind, BatchMetadata, SourceBinding,
    Coverage, PriceUnit, InputScope, ConfigSpec)
from equity_feature_contracts.adapters import AdapterBatch, AdapterCapabilities, AcquisitionRequest
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_contracts.results import ValueType
from equity_feature_io_contracts import FeatureHeader
from equity_feature_io_contracts.publication import SinkError, SinkErrorCode
from equity_feature_io_sdk import SourceRegistry, SinkRegistry, decode_result
from equity_feature_example_extensions import ExampleSink
from equity_feature_workers import SessionCommandSpec, SourceOffer
from equity_feature_service import Credential, Grant, Ledger, Limits, Service, RawDataset, RawRead, Scope
from equity_feature_service.jobs import JobRegistration, JobGrant, JobScheduler
from equity_feature_service.loopback import qualification_server
from equity_feature_service import codec
from test_service import Clock, call

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


def setup(gate=None, sink_create=ExampleSink, cooperative=True, transform=lambda value:value, monotonic=time.monotonic_ns):
    native = TRADE['native_spec']
    config = ConfigSpec.from_json(json.dumps(native['config']))
    request_fields = dict(native['request'])
    request_fields.update(kind=DataKind.TRADE,price_unit=config.price_unit,adjustment=config.adjustment,
                          availability=config.availability,instruments=tuple(request_fields['instruments']),sessions=tuple(request_fields['sessions']))
    request = AcquisitionRequest(**request_fields)
    spec = SessionCommandSpec(native['job_id'],native['generation_id'],native['partition_id'],'trades',request,config,
        tuple(FeatureHeader(**(f|{'dtype':ValueType(f['dtype'])})) for f in native['features']),tuple(IntervalSpec(**g) for g in native['governed_sessions']),
        native['revision_id'],native['destination_scope'],native['max_input_bytes'])
    facts = {'instrument_id':('A',)*3,'session_id':('S',)*3,'event_ns':(110,130,160),'order_key':(1,2,3),
             'event_id':('t1','t2','t3'),'eligible':(True,)*3,'price':(100,102,101),'size':(2,3,5),'known_at_ns':(110,130,210)}
    batch = CanonicalBatch(DataKind.TRADE,tuple(Column(k,v) for k,v in facts.items()),
        BatchMetadata('demo',SourceBinding('synthetic','snapshot1','map1','trades'),Coverage(3,3,True),PriceUnit(0,'USD'),
                      scope=InputScope(100,200,'example-v1')))
    delivery = AdapterBatch(request.request_id,0,True,batch.metadata.source,batch.metadata.coverage,Coverage(3,3,True),batch)
    audit = {'reads':0,'source_threads':[],'sink_threads':[],'entered':threading.Event()}
    capabilities = AdapterCapabilities((DataKind.TRADE,),('demo',),(PriceUnit(0,'USD'),),sampling=('none',),max_batch_rows=3)
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
    registration = JobRegistration(TRADE['request'],spec,delivery,sources=sources,source=SourceOffer('owned.source',capabilities,{}),
                                    sinks=sinks,sink_id='owned.sink',sink_config={},credentials=Credentials())
    clock = Clock()
    scope = Scope('A','S',100,200)
    columns = tuple(facts)
    rights = frozenset(('calculate','job_manage','retain','discover'))
    dataset = RawDataset('owned.trades','owned-v1',scope,RawRead(batch,'a'*64),lambda cancellation:RawRead(batch,'a'*64),
                         columns=columns,rights=rights,rights_owner='owned-test',rights_evidence='owned-native-fixture',valid_from_ns=0,expires_at_ns=3_600_000_000_000)
    tokens = (secrets.token_urlsafe(32),secrets.token_urlsafe(32))
    credentials = tuple(Credential.provision(p,t,0,3_600_000_000_000) for p,t in zip(('A','B'),tokens))
    grants = tuple(Grant('grant-'+p,p,'owned.trades','owned-v1','policy-v1',scope,frozenset(columns),rights,0,3_600_000_000_000) for p in ('A','B'))
    ledger = Ledger(clock=clock,limits=Limits(60,1_048_576,2_097_152,60_000_000_000),credentials=credentials,grants=grants,datasets=(dataset,),
                    policy_revision='policy-v1',registry_snapshot=TRADE['request']['payload']['context']['registry_snapshot'])
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
        wire = json.loads(json.dumps(TRADE['request']))
        wire['payload']['idempotency_key'] = key
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


if __name__ == '__main__': unittest.main()
