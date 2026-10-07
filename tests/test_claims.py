"""Frozen independent transitions, real storage, fresh-process crash recovery."""
from dataclasses import replace
import json
import multiprocessing
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from equity_feature_io_contracts import SinkError, SinkErrorCode, PublicationStatus, PublicationState
from equity_feature_io_sdk import encode_result
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_workers.claims import ClaimLimits, ClaimStore
from test_publication import sink_for, Proxy, work

ORACLE=json.loads(Path(__file__).with_name('claims_oracle.json').read_text(encoding='utf-8'))

def store(root,limits=None):return ClaimStore(Path(root)/'claims',limits=limits or ClaimLimits(),requirements=LIMITS)
def acquire(s,t,owner='A',start=100,end=200,**kw):
    return s.acquire(t,owner_id=owner,attempt_id='attempt-'+owner,issued_at_ns=start,expires_at_ns=end,now_ns=start,**kw)

def child_owner(root,task,ready,release):
    with acquire(store(root),task):
        ready.set();release.wait(20)

def quota_child(root,task,ready,results):
    ready.wait(15)
    try:
        with acquire(store(root,ClaimLimits(max_tasks=1)),task):pass
        results.put('ACQUIRED')
    except SinkError as error:results.put(error.code.value)

def committed_child(root,kind,ready):
    import os
    command=work()[0]
    with acquire(store(root),command.output.task) as handle,sink_for(kind,Path(root)) as inner:
        class Crash(Proxy):
            def commit(self,session):
                self.inner.commit(session);ready.set();os._exit(23)
        handle.run(lambda:command.results,Crash(inner),now_ns=101)
        raise AssertionError('crash was not exercised')


class Claims(unittest.TestCase):
    def rejected(self,code,call):
        with self.assertRaises(SinkError) as caught:call()
        self.assertEqual(caught.exception.code,code)
        self.assertNotIn('secret',str(caught.exception))

    def test_frozen_live_expiry_does_not_steal_and_closed_handle_fenced(self):
        task=work()[0].output.task
        with tempfile.TemporaryDirectory() as root:
            s=store(root);a=acquire(s,task)
            for t in ORACLE['live_duplicate_times']:
                self.rejected(SinkErrorCode.BUSY,lambda:acquire(store(root),task,'B',t,t+100))
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:a.run(lambda:(),None,now_ns=200))
            a.close();a.close()
            self.rejected(SinkErrorCode.INVALID_SESSION,lambda:a._record())
            with acquire(store(root),task,'B',*ORACLE['owner_b_after_release']) as b:
                self.assertEqual(b.identity.owner_id,'B')

    def test_spawn_contention_crash_releases_lock_and_original_lock_bytes_preserved(self):
        task=work()[0].output.task;ctx=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            ready,release=ctx.Event(),ctx.Event();p=ctx.Process(target=child_owner,args=(root,task,ready,release))
            p.start()
            try:
                self.assertTrue(ready.wait(15))
                path=Path(root)/'claims'/'.efworker-claims1'/(task.task_sha256+'.lock');before=b'\0'
                self.rejected(SinkErrorCode.BUSY,lambda:acquire(store(root),task,'B',201,300))
                p.terminate();p.join(10);self.assertFalse(p.is_alive())
                with acquire(store(root),task,'B',201,300):pass
                self.assertEqual(path.read_bytes(),before)
            finally:
                if p.is_alive():p.terminate()
                p.join(10)

    def test_real_sinks_crash_after_commit_fresh_owner_recovery_no_second_begin(self):
        ctx=multiprocessing.get_context('spawn');command=work()[0]
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root:
                ready=ctx.Event();p=ctx.Process(target=committed_child,args=(root,kind,ready));p.start();p.join(20)
                if p.is_alive():p.terminate();p.join(10)
                self.assertEqual(p.exitcode,23);self.assertTrue(ready.is_set())
                s=store(root);s.request_cancel(command.output.task)
                with acquire(s,command.output.task,'B',201,300) as handle,sink_for(kind,Path(root)) as inner:
                    proxy=Proxy(inner);calls=[]
                    outcome=handle.run(lambda:calls.append(1),proxy,now_ns=202)
                    self.assertTrue(outcome.committed,outcome);self.assertEqual(calls,[]);self.assertEqual(proxy.begins,0)
                    self.assertEqual(outcome.attempts,1)
                    self.assertEqual(tuple(map(encode_result,outcome.results)),tuple(map(encode_result,command.results)))
                    self.assertEqual(outcome.results[0].values[0].values[0],.2)

    def test_retry_exhaustion_before_callback_and_explicit_cancellation_resume(self):
        command=work()[0]
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
            s=store(root);calls=[];proxy=Proxy(inner)
            def fail():calls.append(1);raise RuntimeError('secret')
            for i in ORACLE['allowed_compute_attempts']:
                with acquire(s,command.output.task,str(i)) as h:
                    result=h.run(fail,proxy,now_ns=101);self.assertEqual(result.attempts,i);self.assertEqual(result.reason,'UNAVAILABLE')
            with acquire(s,command.output.task,'fourth') as h:
                self.assertEqual(h.run(fail,proxy,now_ns=101).reason,'RETRY_EXHAUSTED')
            self.assertEqual(len(calls),3);self.assertEqual(proxy.begins,0)

    def test_cancellation_before_and_during_calculation_prevents_begin_then_clear(self):
        command=work()[0]
        for during in (False,True):
            with self.subTest(during=during),tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
                s=store(root);calls=[];proxy=Proxy(inner)
                with acquire(s,command.output.task) as h:
                    if not during:s.request_cancel(command.output.task)
                    def calculate():calls.append(1);s.request_cancel(command.output.task);return command.results
                    outcome=h.run(calculate,proxy,now_ns=101)
                    self.assertEqual(outcome.reason,'CANCELLED');self.assertEqual(len(calls),int(during));self.assertEqual(proxy.begins,0)
                with acquire(s,command.output.task,'B',clear_cancel=True) as h:
                    self.assertTrue(h.run(lambda:command.results,proxy,now_ns=101).committed)

    def test_intent_before_begin_unknown_waits_then_real_commit_recovered(self):
        command=work()[0]
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as inner:
                s=store(root);calls=[]
                class Fault(Proxy):
                    def begin(self,envelope):
                        record=s._read(command.output.task)
                        assert record['state']=='INTENT' and record['intent'] is not None
                        raise RuntimeError('secret')
                with acquire(s,command.output.task) as h:
                    self.assertFalse(h.run(lambda:command.results,Fault(inner),now_ns=101).committed)
                class Unknown(Proxy):
                    def lookup(self,key):return PublicationStatus(PublicationState.UNKNOWN,None)
                with acquire(s,command.output.task,'B') as h:
                    proxy=Unknown(inner);outcome=h.run(lambda:calls.append(1),proxy,now_ns=101)
                    self.assertEqual(outcome.reason,'UNKNOWN');self.assertEqual(calls,[]);self.assertEqual(proxy.begins,0)
                with acquire(s,command.output.task,'C') as h:
                    proxy=Proxy(inner);outcome=h.run(lambda:command.results,proxy,now_ns=101)
                    self.assertTrue(outcome.committed);self.assertEqual(proxy.begins,1)
                    # Committed restart never repeats callback, even at exhausted attempt cap.
                    h._update(attempts=3)
                with acquire(s,command.output.task,'D') as h:
                    s.request_cancel(command.output.task);proxy=Proxy(inner)
                    self.assertTrue(h.run(lambda:calls.append(1),proxy,now_ns=101).committed)
                    self.assertEqual(calls,[]);self.assertEqual(proxy.begins,0)

    def test_recomputed_conflicting_result_preserves_original_intent(self):
        command=work()[0]
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
            s=store(root)
            class Fault(Proxy):
                def begin(self,envelope):raise RuntimeError('secret')
            with acquire(s,command.output.task) as h:
                h.run(lambda:command.results,Fault(inner),now_ns=101);intent=h._record()['intent']
            wrong=tuple(replace(r,values=tuple(replace(col,values=(-.2,)) for col in r.values)) for r in command.results)
            with acquire(s,command.output.task,'B') as h:
                proxy=Proxy(inner);outcome=h.run(lambda:wrong,proxy,now_ns=101)
                self.assertEqual(outcome.reason,'CONFLICT');self.assertEqual(proxy.begins,0)
                self.assertEqual(h._record()['intent'],intent)

    def test_record_count_byte_limits_and_unrelated_work_preserved(self):
        a,b=work()
        with tempfile.TemporaryDirectory() as root:
            s=store(root,ClaimLimits(max_tasks=1))
            with acquire(s,a.output.task):
                self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:acquire(s,b.output.task))
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:acquire(store(root,ClaimLimits(max_record_bytes=1)),a.output.task))
            with acquire(s,a.output.task,'B'):pass

    def test_corrupt_unknown_metadata_and_stage_files_preserved(self):
        task=work()[0].output.task
        with tempfile.TemporaryDirectory() as root:
            s=store(root)
            with acquire(s,task):pass
            path=s._root/(task.task_sha256+'.json');path.write_bytes(b'private secret')
            self.rejected(SinkErrorCode.CORRUPTION,lambda:acquire(s,task,'B'));self.assertEqual(path.read_bytes(),b'private secret')
            path.unlink();alien=s._root/'foreign';alien.write_bytes(b'secret')
            self.rejected(SinkErrorCode.CORRUPTION,lambda:acquire(s,task,'B'));self.assertEqual(alien.read_bytes(),b'secret')

    def test_owner_thread_reentry_and_close_during_callback_reject(self):
        command=work()[0]
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
            s=store(root)
            with acquire(s,command.output.task) as h:
                errors=[]
                def foreign():
                    try:h.close()
                    except SinkError as e:errors.append(e.code)
                t=threading.Thread(target=foreign);t.start();t.join(5);self.assertEqual(errors,[SinkErrorCode.INVALID_SESSION])
                def calculate():
                    self.rejected(SinkErrorCode.BUSY,h.close)
                    self.rejected(SinkErrorCode.BUSY,lambda:h.run(lambda:command.results,inner,now_ns=101))
                    return command.results
                self.assertTrue(h.run(calculate,inner,now_ns=101).committed)

    def test_protected_source_paths_bool_overflow_invalid_platform(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root);source=path/'source';source.write_bytes(b'original')
            for target in (path,source,source/'child'):
                self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:ClaimStore(target,limits=ClaimLimits(),requirements=LIMITS,protected_sources=(source,)))
            self.assertEqual(source.read_bytes(),b'original')
            for limits in ({'max_tasks':True},{'max_attempts':0},{'max_record_bytes':2**63}):
                self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:ClaimLimits(**limits))
            with patch('equity_feature_workers.claims.sys.platform','unsupported'):
                self.rejected(SinkErrorCode.UNSUPPORTED_CAPABILITY,lambda:store(root))

    def test_interrupted_replace_retains_stage_and_original_valid_record(self):
        task=work()[0].output.task
        with tempfile.TemporaryDirectory() as root:
            s=store(root)
            with acquire(s,task) as h:
                before=(s._root/(task.task_sha256+'.json')).read_bytes()
                with patch('equity_feature_workers.claims.os.replace',side_effect=OSError('secret')):
                    self.rejected(SinkErrorCode.UNAVAILABLE,lambda:s.request_cancel(task))
                self.assertEqual((s._root/(task.task_sha256+'.json')).read_bytes(),before)
                self.assertEqual(len(list(s._root.glob('stage-*.tmp'))),1)
                self.assertFalse(h._record()['cancelled'])

    def test_two_spawn_processes_share_last_task_slot_without_quota_overrun(self):
        a,b=work();ctx=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            ready=ctx.Event();results=ctx.Queue()
            processes=[ctx.Process(target=quota_child,args=(root,c.output.task,ready,results)) for c in (a,b)]
            for p in processes:p.start()
            ready.set()
            for p in processes:
                p.join(20)
                if p.is_alive():p.terminate();p.join(10)
                self.assertEqual(p.exitcode,0)
            outcomes=[results.get(timeout=5) for _ in processes]
            self.assertEqual(outcomes.count('ACQUIRED'),1)
            self.assertIn(next(x for x in outcomes if x!='ACQUIRED'),('BUSY','RESOURCE_LIMIT'))
            self.assertEqual(len(list((Path(root)/'claims'/'.efworker-claims1').glob('*.json'))),1)
            results.close();results.join_thread()

    def test_failed_shard_does_not_block_unrelated_exact_task(self):
        a,b=work()
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
            s=store(root)
            with acquire(s,a.output.task) as h:
                def fail():raise RuntimeError('secret')
                self.assertEqual(h.run(fail,inner,now_ns=101).reason,'UNAVAILABLE')
            with acquire(s,b.output.task) as h:
                self.assertTrue(h.run(lambda:b.results,inner,now_ns=101).committed)
            with acquire(s,a.output.task,'B') as h:
                self.assertTrue(h.run(lambda:a.results,inner,now_ns=101).committed)

    def test_cancel_after_sink_commit_preserves_intent_then_verified_completion(self):
        command=work()[0]
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as inner:
                s=store(root)
                class Cancel(Proxy):
                    def commit(self,session):
                        receipt=self.inner.commit(session);s.request_cancel(command.output.task);return receipt
                with acquire(s,command.output.task) as h:
                    proxy=Cancel(inner);outcome=h.run(lambda:command.results,proxy,now_ns=101)
                    self.assertFalse(outcome.committed);self.assertEqual(h._record()['state'],'INTENT')
                with acquire(s,command.output.task,'B') as h:
                    calls=[];proxy=Proxy(inner)
                    self.assertTrue(h.run(lambda:calls.append(1),proxy,now_ns=101).committed)
                    self.assertEqual(calls,[]);self.assertEqual(proxy.begins,0)

    def test_aborted_publication_retried_only_with_same_intent(self):
        command=work()[0]
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as inner:
                s=store(root)
                class FailedWrite(Proxy):
                    def write(self,*args):raise SinkError(SinkErrorCode.UNAVAILABLE)
                with acquire(s,command.output.task) as h:
                    self.assertFalse(h.run(lambda:command.results,FailedWrite(inner),now_ns=101).committed)
                    from equity_feature_workers import decode_output
                    from equity_feature_io_sdk import idempotency_key
                    output=decode_output(h._record()['intent'].encode('ascii'))
                    self.assertEqual(inner.lookup(idempotency_key(output.envelope.identity)).state,PublicationState.ABORTED)
                with acquire(s,command.output.task,'B') as h:
                    self.assertTrue(h.run(lambda:command.results,inner,now_ns=101).committed)

    def test_corrupt_committed_readback_never_callbacks_or_new_begin(self):
        command=work()[0]
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
            s=store(root)
            with acquire(s,command.output.task) as h:self.assertTrue(h.run(lambda:command.results,inner,now_ns=101).committed)
            class Corrupt(Proxy):
                def read(self,receipt):return ()
            with acquire(s,command.output.task,'B') as h:
                calls=[];proxy=Corrupt(inner);outcome=h.run(lambda:calls.append(1),proxy,now_ns=101)
                self.assertFalse(outcome.committed);self.assertEqual(outcome.reason,'READBACK_FAILED')
                self.assertEqual(calls,[]);self.assertEqual(proxy.begins,0)

    def test_aggregate_record_bytes_and_admission_before_calculation(self):
        a,b=work()
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
            s=store(root)
            with acquire(s,a.output.task):pass
            amount=(s._root/(a.output.task.task_sha256+'.json')).stat().st_size
            tight=store(root,ClaimLimits(max_total_bytes=amount))
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:acquire(tight,b.output.task))
            with acquire(s,a.output.task,'B') as h:
                calls=[]
                class Unsupported(Proxy):
                    def capabilities(self):return replace(self.inner.capabilities(),supports_lookup=False)
                self.rejected(SinkErrorCode.UNSUPPORTED_CAPABILITY,lambda:h.run(lambda:calls.append(1),Unsupported(inner),now_ns=101))
                self.assertEqual(calls,[]);self.assertEqual(h._record()['attempts'],0)

    def test_malformed_cancellation_and_filesystem_error_are_redacted(self):
        command=work()[0]
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as inner:
            s=store(root)
            class Bad:
                def is_cancelled(self):raise RuntimeError('secret')
            with acquire(s,command.output.task) as h:
                calls=[]
                self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:h.run(lambda:calls.append(1),inner,now_ns=101,cancellation=Bad()))
                self.assertEqual(calls,[]);self.assertEqual(h._record()['attempts'],0)
                with patch('equity_feature_workers.claims.os.scandir',side_effect=OSError('secret')):
                    self.rejected(SinkErrorCode.UNAVAILABLE,h._record)

    def test_failed_handle_close_redacts_fences_and_allows_explicit_cleanup_retry(self):
        task=work()[0].output.task
        with tempfile.TemporaryDirectory() as root:
            s=store(root);h=acquire(s,task)
            with patch.object(h._lock,'close',side_effect=OSError('secret release detail')):
                self.rejected(SinkErrorCode.UNAVAILABLE,h.close)
                self.rejected(SinkErrorCode.INVALID_SESSION,h._record)
                self.rejected(SinkErrorCode.BUSY,lambda:acquire(store(root),task,'B'))
            h.close();h.close()
            with acquire(store(root),task,'B'):pass

    def test_metadata_release_failure_after_durable_cancel_is_redacted_and_recoverable(self):
        from equity_feature_workers.generations import _StoreLock
        task=work()[0].output.task
        with tempfile.TemporaryDirectory() as root:
            s=store(root)
            with acquire(s,task) as h:
                original=_StoreLock.close;faults=[]
                def fail_once(lock):
                    # fdopen gives an integer name: identify the metadata lock by the live task lock object.
                    is_metadata=lock is not h._lock
                    original(lock)
                    if is_metadata and not faults:faults.append(1);raise OSError('secret release detail')
                with patch.object(_StoreLock,'close',fail_once):
                    self.rejected(SinkErrorCode.UNAVAILABLE,lambda:s.request_cancel(task))
                self.assertEqual(faults,[1]);self.assertTrue(h._record()['cancelled'])

    def test_owned_task_cleanup_failure_still_releases_metadata_writer_lock(self):
        from equity_feature_workers.generations import _StoreLock
        task=work()[0].output.task
        with tempfile.TemporaryDirectory() as root:
            s=store(root);original=_StoreLock.close;released=[]
            def fail_first(lock):
                original(lock);released.append(1)
                if len(released)==1:raise OSError('secret owned close')
            with patch.object(s,'_read',side_effect=SinkError(SinkErrorCode.CORRUPTION)),patch.object(_StoreLock,'close',fail_first):
                self.rejected(SinkErrorCode.UNAVAILABLE,lambda:acquire(s,task))
            self.assertEqual(len(released),2)
            with acquire(store(root),task,'B'):pass

    def test_committed_metadata_release_failure_recovers_without_callback_or_begin(self):
        from equity_feature_workers.generations import _StoreLock
        command=work()[0]
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as inner:
                s=store(root);original=_StoreLock.close;faults=[]
                with acquire(s,command.output.task) as h:
                    def fail_committed_once(lock):
                        is_metadata=lock is not h._lock
                        original(lock)
                        record=s._read(command.output.task)
                        if is_metadata and record['state']=='COMMITTED' and not faults:
                            faults.append(1);raise OSError('secret after committed metadata')
                    with patch.object(_StoreLock,'close',fail_committed_once):
                        outcome=h.run(lambda:command.results,inner,now_ns=101)
                    self.assertEqual(faults,[1]);self.assertFalse(outcome.committed)
                    self.assertEqual(outcome.reason,'UNAVAILABLE');self.assertEqual(outcome.results,command.results)
                with acquire(store(root),command.output.task,'B') as h:
                    calls=[];proxy=Proxy(inner);outcome=h.run(lambda:calls.append(1),proxy,now_ns=101)
                    self.assertTrue(outcome.committed);self.assertEqual(calls,[]);self.assertEqual(proxy.begins,0)

    def test_successful_acquisition_final_release_failure_does_not_strand_task_lock(self):
        from equity_feature_workers.generations import _StoreLock
        task=work()[0].output.task
        with tempfile.TemporaryDirectory() as root:
            s=store(root);original=_StoreLock.close;released=[]
            def fail_first(lock):
                original(lock);released.append(1)
                if len(released)==1:raise OSError('secret after successful acquisition')
            with patch.object(_StoreLock,'close',fail_first):
                self.rejected(SinkErrorCode.UNAVAILABLE,lambda:acquire(s,task))
            self.assertEqual(len(released),2)
            with acquire(store(root),task,'B') as h:self.assertEqual(h._record()['attempts'],0)

if __name__=='__main__':unittest.main()
