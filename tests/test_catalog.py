"""Independent catalog transitions, real receipts, old readers and controlled death."""
from dataclasses import replace
from functools import lru_cache
import json
import multiprocessing
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from equity_feature_io_contracts import SinkError, SinkErrorCode, PublicationState, PublicationStatus
from equity_feature_io_sdk import encode_result
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_workers import BarrierLimits, Dependency, GenerationSpec, GenerationStore, SerialPublisher, PublicationLimits
from equity_feature_workers.catalog import CatalogLimits, CatalogStore, CatalogEntry, CatalogSnapshot, _key
from equity_feature_workers.generations import _StoreLock
from test_publication import sink_for, Proxy
from test_barriers import produce

ORACLE=json.loads(Path(__file__).with_name('catalog_oracle.json').read_text(encoding='utf-8'))


@lru_cache(maxsize=1)
def work():return tuple(produce(member)[1].command for member in ('A','B','C'))


def stores(root,limits=None):
    root=Path(root)
    return (CatalogStore(root/'catalog',limits=limits or CatalogLimits(),requirements=LIMITS),
            GenerationStore(root/'generations',limits=BarrierLimits(),requirements=LIMITS))


def publish(root,sink,name,job='job',complete=True):
    commands=work();tasks=tuple(replace(c.output.task,generation_id=name,job_id=job) for c in commands)
    publisher=SerialPublisher(sink,'synthetic-conformance',limits=PublicationLimits(),requirements=LIMITS)
    for task,command in zip(tasks,commands):publisher.submit(task,command.results)
    results=publisher.drain();assert all(r.committed for r in results),results
    deps=tuple(Dependency(r.task_sha256,r.output.task,r.output,sink) for r in results)
    spec=GenerationSpec('demo',job,name,tasks)
    if complete:assert stores(root)[1].publish(spec,deps).complete
    return spec,deps,commands


def dependencies(snapshot,sink):
    return tuple(Dependency(o.task.task_sha256,o.task,o,sink) for o in snapshot.selected.outputs)


def held_writer(root,ready):
    path=Path(root)/'catalog'/'.efworker-catalog1';path.mkdir(parents=True,exist_ok=True)
    lock=_StoreLock(path/'writer.lock');ready.set()
    threading.Event().wait(30)
    lock.close();raise AssertionError('parent did not terminate owner')


def death_writer(root,kind,phase,ready):
    with sink_for(kind,Path(root)) as sink:
        catalog,generations=stores(root);spec,deps,_=publish(root,sink,'B')
        original=os.replace
        def replacing(source,target):
            if phase=='after':original(source,target)
            ready.set();threading.Event().wait(30)
            raise AssertionError('parent did not terminate selector')
        with patch('equity_feature_workers.catalog.os.replace',replacing):
            catalog.select(spec,generations,deps,expected_sequence=1)


def racing_selector(root,name,ready,results):
    with sink_for('parquet',Path(root)) as sink:
        catalog,generations=stores(root)
        tasks=tuple(replace(c.output.task,generation_id=name) for c in work())
        spec=GenerationSpec('demo','job',name,tasks)
        outputs=generations._load(spec);assert outputs is not None
        deps=tuple(Dependency(o.task.task_sha256,o.task,o,sink) for o in outputs)
        ready.wait(20)
        try:
            result=catalog.select(spec,generations,deps,expected_sequence=1)
            results.put((name,'ACCEPTED' if result.accepted else 'WAITING'))
        except SinkError as error:results.put((name,error.code.value))


class Catalog(unittest.TestCase):
    def rejected(self,code,call):
        with self.assertRaises(SinkError) as caught:call()
        self.assertEqual(caught.exception.code,code)
        self.assertNotIn('secret',str(caught.exception))

    def test_constructor_missing_snapshot_and_invalid_inputs_make_no_files(self):
        with tempfile.TemporaryDirectory() as root:
            c,g=stores(root);self.assertIsNone(c.snapshot('demo','job'))
            self.assertEqual(list(Path(root).iterdir()),[])
            for value in (True,-1,1.5,'0'):
                self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:c.select(GenerationSpec('demo','job','A',(replace(work()[0].output.task,generation_id='A'),)),g,(),expected_sequence=value))
            spec=GenerationSpec('demo','job','A',(replace(work()[0].output.task,generation_id='A'),))
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:c.select(spec,None,(),expected_sequence=0))
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:c.select(spec,g,(),expected_sequence=0))
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:c.snapshot('','job'))
            self.assertEqual(list(Path(root).iterdir()),[])

    def test_limits_exact_positive_counts_and_protected_source_separation(self):
        for value in (True,0,-1,1.5,'1'):
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:CatalogLimits(max_history=value))
        self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:CatalogLimits(max_catalogs=4097))
        self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:CatalogLimits(max_record_bytes=16777217))
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)
            for source in (p,p/'nested',p.parent):
                self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:CatalogStore(p,limits=CatalogLimits(),requirements=LIMITS,protected_sources=(source,)))
            self.assertEqual(list(p.iterdir()),[])

    def test_frozen_transition_history_stale_conflict_and_current_replay(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');b,bd,_=publish(root,sink,'B')
            outcomes=[c.select(a,g,ad,expected_sequence=0),c.select(a,g,ad,expected_sequence=0),c.select(b,g,bd,expected_sequence=1)]
            for outcome,expected in zip(outcomes,ORACLE['transitions']):
                self.assertTrue(outcome.accepted);self.assertEqual(outcome.snapshot.sequence,expected['sequence'])
                self.assertEqual([e.spec.generation_id for e in outcome.snapshot.history],expected['history'])
            self.assertEqual(outcomes[0].snapshot,outcomes[1].snapshot)
            for expected in ORACLE['transitions'][3:]:
                self.rejected(SinkErrorCode.CONFLICT,lambda:c.select(a,g,ad,expected_sequence=expected['expected_sequence']))
            self.assertEqual(c.snapshot('demo','job'),outcomes[-1].snapshot)

    def test_both_real_sinks_old_reader_and_original_full_results_zero_begin(self):
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as sink:
                c,g=stores(root);a,ad,commands=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot
                b,bd,_=publish(root,sink,'B');new=c.select(b,g,bd,expected_sequence=1).snapshot
                proxy=Proxy(sink)
                for snapshot,history in ((old,ORACLE['old_reader_history']),(new,ORACLE['new_reader_history'])):
                    outcome=c.verify(snapshot,g,dependencies(snapshot,proxy))
                    self.assertTrue(outcome.accepted);self.assertEqual([e.spec.generation_id for e in snapshot.history],history)
                    actual=tuple(v.command.results for v in outcome.generation.barrier.verified)
                    self.assertEqual([r[0].values[0].values[0] for r in actual],ORACLE['independent_returns'])
                    self.assertEqual([tuple(map(encode_result,r)) for r in actual],[tuple(map(encode_result,x.results)) for x in commands])
                self.assertEqual(proxy.begins,ORACLE['sink_begin_count'])

    def test_snapshot_and_verification_are_read_only_after_selection(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,deps,_=publish(root,sink,'A');original=c.select(a,g,deps,expected_sequence=0).snapshot
            before={p.name:p.read_bytes() for p in c._root.iterdir()}
            with patch('equity_feature_workers.catalog._StoreLock',side_effect=AssertionError('secret')),patch.object(Path,'mkdir',side_effect=AssertionError('secret')):
                self.assertEqual(c.snapshot('demo','job'),original);self.assertTrue(c.verify(original,g,deps).accepted)
            self.assertEqual(before,{p.name:p.read_bytes() for p in c._root.iterdir()})

    def test_forged_or_truncated_snapshot_lineage_cannot_be_verified(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot
            b,bd,_=publish(root,sink,'B');fake=CatalogSnapshot('demo','job',(CatalogEntry(b,tuple(d.output for d in bd)),))
            self.rejected(SinkErrorCode.CONFLICT,lambda:c.verify(fake,g,bd))
            new=c.select(b,g,bd,expected_sequence=1).snapshot
            self.rejected(SinkErrorCode.CONFLICT,lambda:c.verify(CatalogSnapshot('demo','job',(new.selected,)),g,bd))
            self.assertTrue(c.verify(old,g,ad).accepted)

    def test_incomplete_generation_preserves_prior_selection(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot
            b,bd,_=publish(root,sink,'B',complete=False)
            self.assertFalse(c.select(b,g,bd,expected_sequence=1).accepted);self.assertEqual(c.snapshot('demo','job'),old)
            self.assertTrue(g.publish(b,bd).complete);self.assertTrue(c.select(b,g,bd,expected_sequence=1).accepted)

    def test_unknown_or_corrupt_live_readback_cannot_accept_or_replace(self):
        class Unknown(Proxy):
            def lookup(self,key):return PublicationStatus(PublicationState.UNKNOWN,None)
        class Empty(Proxy):
            def read(self,receipt):return ()
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot
            b,bd,_=publish(root,sink,'B')
            for proxy in (Unknown(sink),Empty(sink)):
                self.assertFalse(c.verify(old,g,dependencies(old,proxy)).accepted)
                broken=tuple(replace(d,sink=proxy) for d in bd)
                self.assertFalse(c.select(b,g,broken,expected_sequence=1).accepted)
                self.assertEqual(c.snapshot('demo','job'),old);self.assertEqual(proxy.begins,0)

    def test_corrupt_immutable_generation_record_cannot_select(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot
            b,bd,_=publish(root,sink,'B');p=g._path(b);data=p.read_bytes();p.write_bytes(b'{}')
            self.rejected(SinkErrorCode.CORRUPTION,lambda:c.select(b,g,bd,expected_sequence=1))
            self.assertEqual(c.snapshot('demo','job'),old);p.write_bytes(data)

    def test_cancel_before_replace_preserves_old_and_after_replace_cannot_retract(self):
        class Token:
            cancelled=False
            def is_cancelled(self):return self.cancelled
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot
            b,bd,_=publish(root,sink,'B');token=Token();fsync=os.fsync
            def syncing(fd):fsync(fd);token.cancelled=True
            with patch('equity_feature_workers.catalog.os.fsync',syncing):
                self.rejected(SinkErrorCode.CANCELLED,lambda:c.select(b,g,bd,expected_sequence=1,cancellation=token))
            self.assertEqual(c.snapshot('demo','job'),old);self.assertTrue(list(c._root.glob('stage-*.tmp')))
            token=Token();replacing=os.replace
            def replaced(source,target):replacing(source,target);token.cancelled=True
            with patch('equity_feature_workers.catalog.os.replace',replaced):
                self.assertTrue(c.select(b,g,bd,expected_sequence=1,cancellation=token).accepted)
            self.assertEqual(c.snapshot('demo','job').sequence,2)

    def test_active_history_catalog_record_and_aggregate_quotas(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root,CatalogLimits(max_catalogs=1,max_history=1));a,ad,_=publish(root,sink,'A')
            old=c.select(a,g,ad,expected_sequence=0).snapshot;b,bd,_=publish(root,sink,'B')
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:c.select(b,g,bd,expected_sequence=1))
            other,od,_=publish(root,sink,'A',job='other')
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:c.select(other,g,od,expected_sequence=0))
            self.assertEqual(c.snapshot('demo','job'),old)
            length=c._path('demo','job').stat().st_size
            for limits in (CatalogLimits(max_record_bytes=length),CatalogLimits(max_total_bytes=length)):
                bounded,_=stores(root,limits)
                self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:bounded.select(b,g,bd,expected_sequence=1))
                self.assertEqual(bounded.snapshot('demo','job'),old)

    def test_corrupt_closed_codec_and_foreign_files_are_preserved(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');c.select(a,g,ad,expected_sequence=0);p=c._path('demo','job');data=p.read_bytes()
            value=json.loads(data)
            broken=[b'{}',data+b' ',data.replace(b'"sequence":1',b'"sequence":true')]
            unknown=dict(value);unknown['secret']='private';broken.append(json.dumps(unknown).encode())
            bad=dict(value);bad['history']=value['history']*2;bad['sequence']=2;broken.append(json.dumps(bad).encode())
            for wire in broken:
                p.write_bytes(wire);self.rejected(SinkErrorCode.CORRUPTION,lambda:c.snapshot('demo','job'));self.assertEqual(p.read_bytes(),wire)
            p.write_bytes(data);foreign=c._root/'foreign-secret';foreign.write_bytes(b'private')
            self.rejected(SinkErrorCode.CORRUPTION,lambda:c.snapshot('demo','job'));self.assertEqual(foreign.read_bytes(),b'private');self.assertEqual(p.read_bytes(),data)

    def test_interrupted_stage_retained_invisible_and_source_unchanged(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            source=Path(root)/'original';source.mkdir();p=source/'rows';p.write_bytes(b'synthetic original')
            c=CatalogStore(Path(root)/'catalog',limits=CatalogLimits(),requirements=LIMITS,protected_sources=(source,));g=stores(root)[1]
            a,ad,_=publish(root,sink,'A');original=c.select(a,g,ad,expected_sequence=0).snapshot
            stage=c._root/('stage-'+'f'*32+'.tmp');stage.write_bytes(b'interrupted private metadata')
            self.assertEqual(c.snapshot('demo','job'),original);self.assertTrue(c.verify(original,g,ad).accepted)
            self.assertEqual(stage.read_bytes(),b'interrupted private metadata');self.assertEqual(p.read_bytes(),b'synthetic original')

    def test_actual_process_writer_contention_and_death_release(self):
        ctx=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');ready=ctx.Event();p=ctx.Process(target=held_writer,args=(root,ready));p.start()
            try:
                self.assertTrue(ready.wait(15));self.rejected(SinkErrorCode.BUSY,lambda:c.select(a,g,ad,expected_sequence=0))
                self.assertIsNone(c.snapshot('demo','job'));p.terminate();p.join(10);self.assertFalse(p.is_alive());self.assertEqual(p.exitcode,-signal.SIGTERM)
                self.assertTrue(c.select(a,g,ad,expected_sequence=0).accepted)
            finally:
                if p.is_alive():p.terminate();p.join(10)

    def test_both_sinks_controlled_death_before_and_after_atomic_selection(self):
        ctx=multiprocessing.get_context('spawn')
        for kind in ('parquet','duckdb'):
            for phase in ('before','after'):
                with self.subTest(kind=kind,phase=phase),tempfile.TemporaryDirectory() as root:
                    with sink_for(kind,Path(root)) as sink:
                        c,g=stores(root);a,ad,_=publish(root,sink,'A');c.select(a,g,ad,expected_sequence=0)
                    ready=ctx.Event();p=ctx.Process(target=death_writer,args=(root,kind,phase,ready));p.start()
                    try:
                        self.assertTrue(ready.wait(20));self.assertTrue(p.is_alive())
                        snapshot=c.snapshot('demo','job');self.assertEqual(snapshot.sequence,1 if phase=='before' else 2)
                        p.terminate();p.join(10);self.assertFalse(p.is_alive());self.assertEqual(p.exitcode,-signal.SIGTERM)
                    finally:
                        if p.is_alive():p.terminate();p.join(10)
                    with sink_for(kind,Path(root)) as sink:
                        outcome=c.verify(snapshot,g,dependencies(snapshot,sink));self.assertTrue(outcome.accepted)
                        self.assertEqual([v.command.results[0].values[0].values[0] for v in outcome.generation.barrier.verified],ORACLE['independent_returns'])
                        if phase=='before':self.assertTrue(list(c._root.glob('stage-*.tmp')))
                        self.assertEqual(c.snapshot('demo','job'),snapshot)

    def test_actual_two_selector_race_never_loses_or_forks_history(self):
        ctx=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            with sink_for('parquet',Path(root)) as sink:
                c,g=stores(root);a,ad,_=publish(root,sink,'A');c.select(a,g,ad,expected_sequence=0)
                publish(root,sink,'B');publish(root,sink,'C')
            ready=ctx.Event();results=ctx.Queue();processes=[ctx.Process(target=racing_selector,args=(root,name,ready,results)) for name in ('B','C')]
            for process in processes:process.start()
            ready.set()
            try:
                responses=[results.get(timeout=30) for _ in processes]
                for process in processes:process.join(10);self.assertFalse(process.is_alive());self.assertEqual(process.exitcode,0)
                self.assertEqual(sum(status=='ACCEPTED' for _,status in responses),1)
                self.assertTrue(all(status in ('ACCEPTED',SinkErrorCode.BUSY.value,SinkErrorCode.CONFLICT.value) for _,status in responses))
                snapshot=c.snapshot('demo','job');self.assertEqual(snapshot.sequence,2);self.assertEqual(snapshot.history[0].spec.generation_id,'A')
                winner=next(name for name,status in responses if status=='ACCEPTED');self.assertEqual(snapshot.selected.spec.generation_id,winner)
                loser=next(name for name in ('B','C') if name!=winner)
                with sink_for('parquet',Path(root)) as sink:
                    self.assertTrue(c.verify(snapshot,g,dependencies(snapshot,sink)).accepted)
                    spec=GenerationSpec('demo','job',loser,tuple(replace(x.output.task,generation_id=loser) for x in work()))
                    outputs=g._load(spec);deps=tuple(Dependency(o.task.task_sha256,o.task,o,sink) for o in outputs)
                    self.rejected(SinkErrorCode.CONFLICT,lambda:c.select(spec,g,deps,expected_sequence=1))
            finally:
                for process in processes:
                    if process.is_alive():process.terminate();process.join(10)
                results.close()

    def test_native_open_reader_replacement_preserves_whole_old_or_new(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot
            b,bd,_=publish(root,sink,'B');path=c._path('demo','job');before=path.read_bytes()
            with path.open('rb') as reader:
                if sys.platform=='win32':
                    self.rejected(SinkErrorCode.BUSY,lambda:c.select(b,g,bd,expected_sequence=1));self.assertEqual(c.snapshot('demo','job'),old)
                else:self.assertTrue(c.select(b,g,bd,expected_sequence=1).accepted)
                self.assertEqual(reader.read(),before)
            self.assertTrue(c.select(b,g,bd,expected_sequence=1).accepted)
            self.assertTrue(c.verify(old,g,ad).accepted);self.assertEqual(c.snapshot('demo','job').sequence,2)

    def test_writer_release_fault_after_durable_selection_is_redacted_and_recoverable(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');original=_StoreLock.close
            def fail(lock):original(lock);raise OSError('secret release path')
            with patch.object(_StoreLock,'close',fail):
                self.rejected(SinkErrorCode.UNAVAILABLE,lambda:c.select(a,g,ad,expected_sequence=0))
            current=c.snapshot('demo','job');self.assertEqual(current.sequence,1);self.assertTrue(c.verify(current,g,ad).accepted)
            self.assertEqual(c.select(a,g,ad,expected_sequence=0).snapshot,current)

    def test_replace_failure_retains_old_selection_and_complete_stage(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');old=c.select(a,g,ad,expected_sequence=0).snapshot;b,bd,_=publish(root,sink,'B')
            with patch('equity_feature_workers.catalog.os.replace',side_effect=OSError('secret replacement')):
                self.rejected(SinkErrorCode.UNAVAILABLE,lambda:c.select(b,g,bd,expected_sequence=1))
            self.assertEqual(c.snapshot('demo','job'),old);self.assertTrue(list(c._root.glob('stage-*.tmp')))
            self.assertTrue(c.select(b,g,bd,expected_sequence=1).accepted)

    def test_creating_thread_reentry_and_typed_token_error(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');caught=[]
            def attempt():
                try:c.select(a,g,ad,expected_sequence=0)
                except SinkError as e:caught.append(e.code)
            t=threading.Thread(target=attempt);t.start();t.join();self.assertEqual(caught,[SinkErrorCode.INVALID_SESSION]);self.assertFalse(c._root.exists())
            read=g.read
            def reenter(*args,**kw):
                self.rejected(SinkErrorCode.BUSY,lambda:c.select(a,g,ad,expected_sequence=0));return read(*args,**kw)
            with patch.object(g,'read',reenter):self.assertTrue(c.select(a,g,ad,expected_sequence=0).accepted)
            case=self
            class ReentrantToken:
                def is_cancelled(self):
                    case.rejected(SinkErrorCode.BUSY,lambda:c.select(a,g,ad,expected_sequence=0))
                    return False
            self.assertTrue(c.select(a,g,ad,expected_sequence=0,cancellation=ReentrantToken()).accepted)
            admitted=g._dependencies
            def admitting(*args,**kw):
                self.rejected(SinkErrorCode.BUSY,lambda:c.select(a,g,ad,expected_sequence=0))
                return admitted(*args,**kw)
            with patch.object(g,'_dependencies',admitting):self.assertTrue(c.select(a,g,ad,expected_sequence=0).accepted)
            class Bad:
                def is_cancelled(self):raise ValueError('secret token')
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:c.select(a,g,ad,expected_sequence=0,cancellation=Bad()))
            class Wrong:
                def is_cancelled(self):return 1
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:c.select(a,g,ad,expected_sequence=0,cancellation=Wrong()))

    def test_changed_namespace_job_identity_is_isolated(self):
        self.assertNotEqual(_key('demo','job'),_key('other','job'));self.assertNotEqual(_key('demo','job'),_key('demo','other'))
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            c,g=stores(root);a,ad,_=publish(root,sink,'A');b,bd,_=publish(root,sink,'A',job='other')
            one=c.select(a,g,ad,expected_sequence=0);two=c.select(b,g,bd,expected_sequence=0)
            self.assertTrue(one.accepted and two.accepted);self.assertNotEqual(one.snapshot.identity,two.snapshot.identity)
            self.assertEqual(c.snapshot('demo','job').sequence,1);self.assertEqual(c.snapshot('demo','other').sequence,1)


if __name__=='__main__':unittest.main()
