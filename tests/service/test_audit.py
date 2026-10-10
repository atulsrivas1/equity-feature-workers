"""Independent finite-store arithmetic and actual HTTP/native audit paths."""
import io
import json
import secrets
import threading
import time
import unittest
from unittest.mock import patch

from equity_feature_service._audit import OwnedAudit, AuditDenied, AuditReadPermit
from equity_feature_service import Service
import test_service as http
import test_jobs as native

ZERO = dict(request_used=0,transfer_used=0,transfer_reserved=0,retained_used=0,cache_used=0)


class Audit(unittest.TestCase):
    def store(self):
        return OwnedAudit(owned_synthetic=True,valid_from_ns=0,expires_at_ns=300_000_000_000)

    def fixture(self):
        fixture = http.ServiceVectors('runTest')
        fixture.setUp()
        fixture.ledger = fixture.make_ledger(audit=self.store())
        fixture.service = Service(fixture.ledger)
        return fixture

    def records(self, store, n=100):
        return json.loads(store.snapshot(store.permit,n))['records']

    def test_explicit_owned_reader_identity_epoch_action_expiry_revocation(self):
        store = self.store()
        for permit in (AuditReadPermit(0,300_000_000_000),self.store().permit):
            with self.assertRaises(AuditDenied): store.snapshot(permit,100)
        self.assertEqual(self.records(store),[])
        with self.assertRaises(AuditDenied): store.snapshot(store.permit,300_000_000_000)
        store = self.store(); store.revoke_reader()
        with self.assertRaises(AuditDenied): store.snapshot(store.permit,100)
        for fields in (dict(owned_synthetic=False,valid_from_ns=0,expires_at_ns=10),
                       dict(owned_synthetic=True,valid_from_ns=0,expires_at_ns=300_000_000_001)):
            with self.assertRaises(ValueError): OwnedAudit(**fields)

    def test_128_slots_inclusive_and_no_eviction_or_recursive_overflow(self):
        store = self.store()
        tokens = [store.reserve(100) for _ in range(64)]
        self.assertEqual(store.reserved_count,128)
        with self.assertRaises(AuditDenied): store.reserve(100)
        self.assertEqual((store.overflow,store.reserved_count),(1,128))
        for token in tokens:
            store.settle(token,100,'attempt','admitted',ZERO)
            store.settle(token,100,'finish','emitted',ZERO)
        snapshot = json.loads(store.snapshot(store.permit,100))
        self.assertEqual((len(snapshot['records']),snapshot['reserved_records']),(128,0))
        self.assertLessEqual(snapshot['record_bytes'],65536)
        self.assertLessEqual(len(store.snapshot(store.permit,100)),131072)
        with self.assertRaises(AuditDenied): store.reserve(100)
        self.assertEqual(len(self.records(store)),128)

    def test_literal_worst_record_497_bytes_and_redacted_domain_separation(self):
        store = self.store()
        token = store.reserve(0,actor='canary-secret',command='canary-secret',grant='canary-secret',policy='canary-secret',action='artifact_read')
        store._sequence = 2**63-2
        totals = dict.fromkeys(ZERO,2**63-1)
        body = store.prepare_settlement(token,2**63-1,'native_finish','historical_commit',totals)
        self.assertEqual(len(body),497)
        record = json.loads(body)
        self.assertEqual(len({record[k] for k in ('actor','command','grant','policy')}),4)
        self.assertNotIn(b'canary-secret',body)
        self.assertEqual(set(record),set(ZERO)|{'sequence','time_ns','stage','actor','command','grant','policy','action','decision'})

    def test_expiry_is_half_open_and_never_releases_active_reservations(self):
        store = self.store()
        token = store.reserve(100)
        store.settle(token,100,'attempt','admitted',ZERO)
        store._now(300_000_000_099)
        self.assertEqual((len(store._records),store.reserved_count),(1,1))
        store._now(300_000_000_100)
        self.assertEqual((len(store._records),store.reserved_count),(0,1))
        store.settle(token,300_000_000_100,'finish','abandoned',ZERO)
        self.assertEqual(store.reserved_count,0)

    def test_invalid_clock_and_codes_hold_active_pair_until_real_exit(self):
        store = self.store(); token = store.reserve(100)
        for n in (99,True,-1,2**63):
            with self.assertRaises(AuditDenied): store.settle(token,n,'finish','emitted',ZERO)
            self.assertEqual(token.remaining,2)
        with self.assertRaises(AuditDenied): store.settle(token,100,'finish','caller-exception',ZERO)
        store.abandon_after_exit(token)
        self.assertEqual(store.reserved_count,0)
        with self.assertRaises(AuditDenied): store.abandon_after_exit(token)

    def test_store_full_denies_before_http_body_read_and_source_call(self):
        fixture = self.fixture(); store = fixture.ledger.audit
        for _ in range(64): store.reserve(100)
        class Unreadable(io.BytesIO):
            def read(self,*args): raise AssertionError('caller body was read')
        status,body,*_ = http.call(fixture.service,fixture.request(),fixture.token,**{'wsgi.input':Unreadable()})
        self.assertEqual(status,429)
        self.assertEqual((fixture.source.calls,store.reserved_count,store.overflow),(0,128,1))
        self.assertEqual(fixture.ledger._requests,1)

    def test_http_real_source_and_redaction_and_exact_transfer(self):
        fixture = self.fixture(); store = fixture.ledger.audit
        request = fixture.request(); request['request_id'] = 'caller-canary'
        status,_,data,_ = http.call(fixture.service,request,fixture.token)
        self.assertEqual(status,200)
        rows = self.records(store)
        self.assertEqual([(r['stage'],r['decision']) for r in rows],[('attempt','admitted'),('finish','emitted')])
        self.assertEqual((rows[-1]['action'],rows[-1]['transfer_used']),('slice',len(data)))
        self.assertEqual(store.reserved_count,0)
        encoded = store.snapshot(store.permit,100)
        for text in (fixture.token,'caller-canary','principal-a','policy-v1','synthetic.raw'):
            self.assertNotIn(text.encode(),encoded)

    def test_http_invalid_body_unauthenticated_and_rate_denial_settle_once(self):
        fixture = self.fixture(); store = fixture.ledger.audit
        self.assertEqual(http.call(fixture.service,b'private-body-path-SQL',fixture.token)[0],400)
        self.assertEqual(http.call(fixture.service,fixture.request(),secrets.token_urlsafe(32))[0],401)
        fixture.ledger._requests = 60
        self.assertEqual(http.call(fixture.service,fixture.request(),fixture.token)[0],429)
        self.assertEqual((len(self.records(store)),store.reserved_count,fixture.source.calls),(6,0,0))
        self.assertNotIn(b'private-body-path-SQL',store.snapshot(store.permit,100))

    def test_close_before_next_twice_abandons_and_drops_retained_forms(self):
        fixture = self.fixture(); store = fixture.ledger.audit
        emission = fixture.service(http.env(fixture.request(),fixture.token),lambda *args:self.fail('unexpected headers'))
        self.assertEqual(store.reserved_count,1)
        emission.close(); emission.close()
        self.assertEqual((store.reserved_count,fixture.ledger._reserved),(0,0))
        self.assertEqual(self.records(store)[-1]['decision'],'abandoned')
        self.assertIsNone(emission.prepared.finalize)
        self.assertEqual(list(emission),[])

    def test_start_response_failure_records_abandoned_not_emitted(self):
        fixture = self.fixture(); store = fixture.ledger.audit
        def fail(*args): raise RuntimeError('private-header-callback')
        emission = fixture.service(http.env(fixture.request(),fixture.token),fail)
        with self.assertRaisesRegex(RuntimeError,'private-header-callback'): next(emission)
        emission.close()
        self.assertEqual([(r['stage'],r['decision']) for r in self.records(store)],[('attempt','admitted'),('finish','abandoned')])
        self.assertEqual(store.reserved_count,0)

    def test_clock_failure_after_prepare_releases_only_closed_http_pair(self):
        fixture = self.fixture(); store = fixture.ledger.audit
        statuses = []
        emission = fixture.service(http.env(fixture.request(),fixture.token),lambda s,h:statuses.append(s))
        fixture.clock.n = -1
        self.assertEqual(next(emission),b'')
        self.assertTrue(statuses[0].startswith('500 '))
        self.assertEqual((store.reserved_count,fixture.ledger._reserved),(0,0))
        self.assertGreater(store.failures,0)

    def native_fixture(self, **kwargs):
        scheduler,service,tokens,clock,observed = native.setup(audit_store=self.store(),**kwargs)
        self.addCleanup(lambda:scheduler.close(3))
        helper = native.Jobs('runTest')
        return helper,scheduler,service,tokens,clock,observed

    def test_native_retry_has_one_pair_and_full_store_keeps_reserved_exit(self):
        gate = threading.Event()
        helper,scheduler,service,tokens,clock,observed = self.native_fixture(gate=gate)
        self.addCleanup(gate.set)
        job_id = helper.submit(service,tokens[0]); store = scheduler.ledger.audit
        deadline = time.monotonic()+2
        while not observed['source_threads'] and time.monotonic()<deadline: time.sleep(.01)
        self.assertEqual(helper.submit(service,tokens[0]),job_id)
        with scheduler.ledger.lock:
            while len(store._records)+store.reserved_count+2 <= 128: store.reserve(100)
            held = store.reserved_count
        gate.set(); job = helper.wait_terminal(scheduler,job_id)
        self.assertEqual(job.state,'succeeded')
        rows = self.records(store)
        self.assertEqual([(r['stage'],r['decision']) for r in rows if r['stage'].startswith('native')],
                         [('native_start','started'),('native_finish','succeeded')])
        self.assertEqual(store.reserved_count,held-1)

    def test_native_reservation_denial_has_no_queue_key_or_output_mutation(self):
        helper,scheduler,service,tokens,clock,observed = self.native_fixture()
        store = scheduler.ledger.audit
        with scheduler.ledger.lock:
            for _ in range(63): store.reserve(100)
        registration = next(iter(scheduler.registrations.values()))
        payload = json.loads(registration.payload_bytes)
        payload.update(command_digest=registration.command_digest,idempotency_key='denied')
        request = helper.request('calculate'); request['payload'] = payload
        self.assertEqual(http.call(service,request,tokens[0])[0],429)
        self.assertEqual((len(scheduler.jobs),len(scheduler.keys),len(scheduler.queue),scheduler.total_retained),(0,0,0,0))
        self.assertEqual(observed['source_threads'],[])

    def test_queued_cancel_sweep_and_close_settle_without_fake_execution(self):
        for path in ('cancel','sweep','close'):
            with self.subTest(path=path):
                gate = threading.Event()
                helper,scheduler,service,tokens,clock,observed = self.native_fixture(gate=gate)
                self.addCleanup(gate.set)
                first = helper.submit(service,tokens[0])
                deadline = time.monotonic()+2
                while not observed['source_threads'] and time.monotonic()<deadline: time.sleep(.01)
                second = helper.submit(service,tokens[1],key='second')
                if path == 'cancel':
                    self.assertEqual(http.call(service,helper.request('job_cancel',job_id=second),tokens[1])[0],200)
                elif path == 'sweep':
                    scheduler.ledger.revoke('grant-B')
                    with scheduler.ledger.lock: scheduler._sweep()
                else: scheduler.close(0)
                self.assertIsNone(scheduler.jobs[second].audit_reservation)
                rows = self.records(scheduler.ledger.audit)
                self.assertEqual(sum(r['stage']=='native_start' and r['decision']=='cancelled' for r in rows),1)
                self.assertEqual(len(observed['source_threads']),1)
                gate.set(); helper.wait_terminal(scheduler,first); scheduler.close(3)

    def test_invalid_clock_at_real_native_exit_does_not_kill_executor_or_hold_pair(self):
        gate = threading.Event()
        helper,scheduler,service,tokens,clock,observed = self.native_fixture(gate=gate)
        self.addCleanup(gate.set)
        job_id = helper.submit(service,tokens[0]); store = scheduler.ledger.audit
        deadline = time.monotonic()+2
        while not observed['source_threads'] and time.monotonic()<deadline: time.sleep(.01)
        clock.n = -1; gate.set(); helper.wait_terminal(scheduler,job_id)
        self.assertEqual(store.reserved_count,0)
        self.assertTrue(scheduler.thread.is_alive())
        self.assertIsNone(scheduler._running)
        self.assertGreater(store.failures,0)
        clock.n = 100

    def test_native_start_encoding_fault_denies_factories_and_releases_after_exit(self):
        helper,scheduler,service,tokens,clock,observed = self.native_fixture()
        store = scheduler.ledger.audit
        original = store.prepare_settlement
        def fault(reservation,n,stage,decision,totals):
            if stage == 'native_start': raise AuditDenied('owned-encoding-fault')
            return original(reservation,n,stage,decision,totals)
        with patch.object(store,'prepare_settlement',side_effect=fault):
            job_id = helper.submit(service,tokens[0])
            job = helper.wait_terminal(scheduler,job_id)
        self.assertEqual((job.state,job.result_id,observed['source_threads'],store.reserved_count),('failed',None,[],0))
        self.assertEqual(scheduler.total_retained,0)

    def test_native_finish_encoding_fault_preserves_historical_digest_but_no_result(self):
        helper,scheduler,service,tokens,clock,observed = self.native_fixture()
        store = scheduler.ledger.audit
        original = store.prepare_settlement
        def fault(reservation,n,stage,decision,totals):
            if stage == 'native_finish': raise AuditDenied('owned-encoding-fault')
            return original(reservation,n,stage,decision,totals)
        with patch.object(store,'prepare_settlement',side_effect=fault):
            job_id = helper.submit(service,tokens[0])
            job = helper.wait_terminal(scheduler,job_id)
        self.assertEqual((job.state,job.result_id,job.native,store.reserved_count),('failed',None,b'',0))
        self.assertIsNotNone(job.committed_receipt_sha256)
        self.assertEqual(len(observed['sink_threads']),1)
        self.assertEqual(scheduler.total_retained,0)

    def test_no_poll_audit_expiry_while_native_owner_is_blocked(self):
        gate = threading.Event()
        helper,scheduler,service,tokens,clock,observed = self.native_fixture(gate=gate)
        self.addCleanup(gate.set)
        job_id = helper.submit(service,tokens[0]); store = scheduler.ledger.audit
        deadline = time.monotonic()+2
        while not observed['source_threads'] and time.monotonic()<deadline: time.sleep(.01)
        self.assertTrue(store._records)
        clock.n = 300_000_000_100
        deadline = time.monotonic()+1
        while store._records and time.monotonic()<deadline: time.sleep(.01)
        self.assertEqual((len(store._records),store.reserved_count),(0,1))
        self.assertEqual(scheduler._running,job_id)
        gate.set(); helper.wait_terminal(scheduler,job_id)
        self.assertEqual(store.reserved_count,0)

    def test_window_rollover_preserves_pending_audit_and_reader_cannot_be_reused(self):
        fixture = self.fixture(); store = fixture.ledger.audit
        emission = fixture.service(http.env(fixture.request(),fixture.token),lambda *args:None)
        fixture.clock.n = 60_000_000_100
        fixture.ledger.now()
        self.assertEqual(store.reserved_count,1)
        emission.close()
        self.assertEqual(store.reserved_count,0)
        with self.assertRaisesRegex(ValueError,'shared_audit_ledger_required'):
            fixture.make_ledger(audit=store)
        with self.assertRaises(AttributeError): fixture.ledger.audit = None


if __name__ == '__main__': unittest.main()
