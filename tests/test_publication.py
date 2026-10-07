"""Real optional sink ownership, immutable generation visibility and literal parity."""
from dataclasses import replace
import hashlib
import json
import multiprocessing
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from equity_feature_io_contracts import SinkError, SinkErrorCode
from equity_feature_io_sdk import SinkRegistry, encode_result
from equity_feature_parquet import ParquetSink, ParquetSinkFactory
from equity_feature_duckdb_sink import DuckDBSink, DuckDBSinkFactory
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_workers import (
    BarrierLimits, CommandError, Dependency, GenerationSpec, GenerationStore,
    PublicationLimits, SerialPublisher,
)
from equity_feature_workers.generations import _StoreLock
from publication_fixture import ORACLE, source
from test_barriers import produce

SCOPE='synthetic-conformance'
BOUNDS=BarrierLimits(64,8388608)


def sink_for(kind,root):
    return ParquetSink(root/'output',SCOPE,limits=LIMITS) if kind=='parquet' else DuckDBSink(root/'output.duckdb',SCOPE,limits=LIMITS)


def work():
    return tuple(produce(m,batch=source(m))[1].command for m in ORACLE['ordered_members'])


def publish_all(sink,commands):
    publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
    for command in commands:publisher.submit(command.output.task,command.results)
    outcomes=publisher.drain()
    assert all(o.committed for o in outcomes),outcomes
    dependencies=tuple(Dependency(o.task_sha256,o.output.task,o.output,sink) for o in outcomes)
    return publisher,dependencies


def generation(commands):return GenerationSpec('demo','job','generation',tuple(c.output.task for c in commands))


def hold_lock(root,ready,release):
    root=Path(root)/'.efworker-generations1';root.mkdir(parents=True,exist_ok=True)
    lock=_StoreLock(root/'writer.lock')
    ready.set()
    try:release.wait(15)
    finally:lock.close()


class Proxy:
    def __init__(self,inner):self.inner=inner;self.begins=0
    def __getattr__(self,key):return getattr(self.inner,key)
    def begin(self,envelope):self.begins+=1;return self.inner.begin(envelope)


class Publication(unittest.TestCase):
    def rejected(self,code,call):
        with self.assertRaises(SinkError) as caught:call()
        self.assertEqual(caught.exception.code,code)
        self.assertNotIn('secret',str(caught.exception))

    def test_real_both_sinks_exact_contents_and_original_receipt_replay(self):
        commands=work()
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as sink:
                publisher,deps=publish_all(sink,commands)
                self.assertEqual(publisher.pending,())
                for member,command,dep in zip(ORACLE['ordered_members'],commands,deps):
                    read=sink.read(dep.output.receipt)
                    self.assertEqual(read[0].values[0].values[0],ORACLE['returns'][member])
                    self.assertEqual(tuple(map(encode_result,read)),tuple(map(encode_result,command.results)))
                    publisher.submit(command.output.task,command.results)
                    self.assertEqual(publisher.drain()[0].output,dep.output)

    def test_queue_count_bytes_and_conflicting_duplicate_before_begin(self):
        a,b=work()
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as original:
            sink=Proxy(original)
            publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(1,8388608),requirements=LIMITS)
            publisher.submit(a.output.task,a.results)
            self.assertEqual(publisher.submit(a.output.task,a.results),a.output.task.task_sha256)
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:publisher.submit(b.output.task,b.results))
            result=a.results[0];wrong=replace(result,values=tuple(replace(col,values=(-0.2,)) for col in result.values))
            self.rejected(SinkErrorCode.CONFLICT,lambda:publisher.submit(a.output.task,(wrong,)))
            tiny=SerialPublisher(sink,SCOPE,limits=PublicationLimits(2,1),requirements=LIMITS)
            with self.assertRaises(CommandError):tiny.submit(a.output.task,a.results)
            self.assertEqual(sink.begins,0)

    def test_wrong_scope_and_entity_reject_without_publication(self):
        a=work()[0]
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as original:
            sink=Proxy(original);publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:publisher.submit(replace(a.output.task,destination_scope='other'),a.results))
            result=a.results[0]
            wrong=replace(result,values=tuple(replace(col,entities=(replace(col.entities[0],instrument_id='alien'),)) for col in result.values),
                quality=tuple(replace(q,entity=replace(q.entity,instrument_id='alien')) for q in result.quality),
                evidence=tuple(replace(e,entity=replace(e.entity,instrument_id='alien')) for e in result.evidence))
            with self.assertRaises(CommandError):publisher.submit(a.output.task,(wrong,))
            self.assertEqual(sink.begins,0)

    def test_concurrent_producers_and_owner_thread_only(self):
        commands=work()
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(2,8388608),requirements=LIMITS)
            start=threading.Barrier(6);errors=[]
            def submit(index):
                start.wait()
                for _ in range(50):
                    try:
                        c=commands[index%2];publisher.submit(c.output.task,c.results);return
                    except SinkError as error:
                        if error.code is not SinkErrorCode.BUSY:errors.append(error);return
                        threading.Event().wait(.001)
                errors.append('bounded submit attempts exhausted')
            threads=[threading.Thread(target=submit,args=(i,)) for i in range(6)]
            for t in threads:t.start()
            for t in threads:t.join(5);self.assertFalse(t.is_alive())
            self.assertEqual(errors,[]);self.assertEqual(set(publisher.pending),{c.output.task.task_sha256 for c in commands})
            def foreign():
                try:publisher.drain()
                except SinkError as error:errors.append(error.code)
            t=threading.Thread(target=foreign);t.start();t.join(5)
            self.assertEqual(errors,[SinkErrorCode.INVALID_SESSION])
            self.assertTrue(all(o.committed for o in publisher.drain()))
            # A publisher cannot transfer to a successor after its creating thread exits.
            holder={}
            def create():holder['publisher']=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
            creator=threading.Thread(target=create);creator.start();creator.join(5)
            self.assertFalse(creator.is_alive())
            def successor():
                try:holder['publisher'].drain()
                except SinkError as error:errors.append(error.code)
            other=threading.Thread(target=successor);other.start();other.join(5)
            self.assertFalse(other.is_alive());self.assertEqual(errors,[SinkErrorCode.INVALID_SESSION]*2)

    def test_same_root_parquet_distinct_partitions_still_contend(self):
        a,b=work()
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as owner,sink_for('parquet',Path(root)) as foreign:
            handle=foreign.begin(a.output.envelope)
            try:
                publisher=SerialPublisher(owner,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
                publisher.submit(b.output.task,b.results)
                outcome=publisher.drain()[0]
                self.assertFalse(outcome.committed);self.assertEqual(outcome.reason,'BUSY')
                self.assertEqual(publisher.pending,(b.output.task.task_sha256,))
            finally:foreign.abort(handle)
            self.assertTrue(publisher.drain()[0].committed)

    def test_duckdb_foreign_writer_waits_then_serialized_retry(self):
        a,b=work()
        with tempfile.TemporaryDirectory() as root,sink_for('duckdb',Path(root)) as owner,sink_for('duckdb',Path(root)) as foreign:
            handle=foreign.begin(a.output.envelope)
            try:
                publisher=SerialPublisher(owner,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
                publisher.submit(b.output.task,b.results)
                outcome=publisher.drain()[0]
                self.assertFalse(outcome.committed)
                self.assertIn(outcome.reason,('BUSY','READBACK_FAILED'))
                self.assertEqual(publisher.pending,(b.output.task.task_sha256,))
            finally:foreign.abort(handle)
            self.assertTrue(publisher.drain()[0].committed)

    def test_cancelled_before_begin_retains_work_and_no_generation(self):
        a=work()[0];token=type('Stop',(),{'is_cancelled':lambda self:True})()
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as original:
            sink=Proxy(original);publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
            publisher.submit(a.output.task,a.results)
            self.assertEqual(publisher.drain(cancellation=token)[0].reason,'CANCELLED')
            self.assertEqual(sink.begins,0);self.assertEqual(len(publisher.pending),1)
            store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
            d=Dependency('A',a.output.task,a.output,sink)
            self.rejected(SinkErrorCode.CANCELLED,lambda:store.publish(generation((a,)),(d,),cancellation=token))
            self.assertFalse((Path(root)/'generations').exists())
            self.assertTrue(publisher.drain()[0].committed)

    def test_cancel_during_write_aborts_and_retries_without_drop(self):
        a=work()[0]
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as original:
                token=type('Token',(),{'stop':False,'is_cancelled':lambda self:self.stop})()
                class CancelWrite(Proxy):
                    def write(self,session,ordinal,result):self.inner.write(session,ordinal,result);token.stop=True
                sink=CancelWrite(original);publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
                publisher.submit(a.output.task,a.results)
                self.assertFalse(publisher.drain(cancellation=token)[0].committed)
                self.assertEqual(len(publisher.pending),1)
                token.stop=False
                self.assertTrue(publisher.drain()[0].committed)

    def test_uncertain_commit_looked_up_before_retry_no_duplicate_begin(self):
        a=work()[0]
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as original:
                class Uncertain(Proxy):
                    hidden=False
                    def commit(self,session):
                        self.inner.commit(session);self.hidden=True;raise SinkError(SinkErrorCode.COMMIT_UNKNOWN)
                    def lookup(self,key):
                        if self.hidden:raise SinkError(SinkErrorCode.UNAVAILABLE)
                        return self.inner.lookup(key)
                sink=Uncertain(original);publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
                publisher.submit(a.output.task,a.results)
                first=publisher.drain()[0]
                self.assertFalse(first.committed);self.assertEqual(first.reason,'COMMIT_UNKNOWN');self.assertEqual(sink.begins,1)
                sink.hidden=False
                second=publisher.drain()[0]
                self.assertTrue(second.committed);self.assertEqual(sink.begins,1);self.assertEqual(publisher.pending,())

    def test_corrupt_readback_retained_with_redacted_reason(self):
        a=work()[0]
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as original:
            class Corrupt(Proxy):
                broken=True
                def read(self,receipt):
                    if self.broken:raise RuntimeError('secret backend path')
                    return self.inner.read(receipt)
            sink=Corrupt(original);publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=LIMITS)
            publisher.submit(a.output.task,a.results)
            out=publisher.drain()[0]
            self.assertFalse(out.committed);self.assertEqual(out.reason,'READBACK_FAILED');self.assertNotIn('secret',repr(out))
            self.assertEqual(len(publisher.pending),1)
            sink.broken=False;self.assertTrue(publisher.drain()[0].committed);self.assertEqual(sink.begins,1)

    def test_missing_expected_task_prevents_generation_completion(self):
        commands=work();spec=generation(commands)
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as sink:
                publisher,deps=publish_all(sink,commands[:1]);b=commands[1]
                missing=Dependency('B',b.output.task,b.output,sink)
                store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
                out=store.publish(spec,deps+(missing,))
                self.assertEqual(out.complete,ORACLE['one_committed_complete']);self.assertFalse(store.read(spec,deps+(missing,)).complete)
                publisher.submit(b.output.task,b.results);resolved=publisher.drain()[0]
                actual=Dependency('B',b.output.task,resolved.output,sink)
                complete=store.publish(spec,deps+(actual,))
                self.assertEqual(complete.complete,ORACLE['both_committed_complete'])
                self.assertEqual(tuple(o.task.task_sha256 for o in complete.outputs),tuple(t.task_sha256 for t in spec.tasks))
                self.assertTrue(store.read(spec,deps+(actual,)).complete)

    def test_generation_declared_order_independent_of_arrival(self):
        commands=work();spec=generation(commands)
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            _,deps=publish_all(sink,commands[::-1])
            store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
            out=store.publish(spec,deps)
            self.assertEqual(tuple(o.task.partition_id for o in out.outputs),tuple(t.partition_id for t in spec.tasks))

    def test_immutable_generation_replay_conflict_and_receipt_mismatch(self):
        commands=work();spec=generation(commands)
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            _,deps=publish_all(sink,commands);store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
            first=store.publish(spec,deps);path=Path(root)/'generations'/'.efworker-generations1'/(spec.key+'.complete.json');before=path.read_bytes()
            self.assertEqual(first,store.publish(spec,deps));self.assertEqual(before,path.read_bytes())
            self.rejected(SinkErrorCode.CONFLICT,lambda:store.publish(replace(spec,tasks=spec.tasks[::-1]),deps))
            self.assertEqual(before,path.read_bytes())
            bad=replace(deps[0],output=replace(deps[0].output,receipt=replace(deps[0].output.receipt,caller_committed_at_ns=1)))
            self.assertFalse(store.read(spec,(bad,)+deps[1:]).complete)
            self.assertEqual(before,path.read_bytes())

    def test_interrupted_private_stage_not_visible_and_retry_complete(self):
        commands=work();spec=generation(commands)
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            _,deps=publish_all(sink,commands);store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
            with patch('equity_feature_workers.generations.os.replace',side_effect=OSError('secret interruption')):
                self.rejected(SinkErrorCode.UNAVAILABLE,lambda:store.publish(spec,deps))
            self.assertTrue(tuple((Path(root)/'generations'/'.efworker-generations1').glob('stage-*.json')))
            self.assertFalse(store.read(spec,deps).complete)
            self.assertTrue(store.publish(spec,deps).complete)

    def test_cancel_after_stage_before_completion_no_visibility(self):
        commands=work();spec=generation(commands)
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            _,deps=publish_all(sink,commands);store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
            stage_root=Path(root)/'generations'/'.efworker-generations1'
            token=type('StopAfterStage',(),{'is_cancelled':lambda self:stage_root.exists() and bool(tuple(stage_root.glob('stage-*.json')))})()
            self.rejected(SinkErrorCode.CANCELLED,lambda:store.publish(spec,deps,cancellation=token))
            self.assertFalse(store.read(spec,deps).complete)
            self.assertTrue(store.publish(spec,deps).complete)

    def test_corrupt_complete_metadata_never_becomes_absence(self):
        commands=work();spec=generation(commands)
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            _,deps=publish_all(sink,commands);store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS);store.publish(spec,deps)
            path=Path(root)/'generations'/'.efworker-generations1'/(spec.key+'.complete.json');original=path.read_bytes()
            for wire in (b'{',b'{"protocol":"alien"}',original.replace(b'efworker-generation1',b'efworker-generation9')):
                path.write_bytes(wire)
                self.rejected(SinkErrorCode.CORRUPTION,lambda:store.read(spec,deps))
                self.rejected(SinkErrorCode.CORRUPTION,lambda:store.publish(spec,deps))
                self.assertEqual(path.read_bytes(),wire)

    def test_generation_counts_wrong_population_and_optional_reject(self):
        commands=work();spec=generation(commands)
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            _,deps=publish_all(sink,commands);store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
            for candidate in (deps[:1],(replace(deps[0],required=False),)+deps[1:]):
                self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:store.publish(spec,candidate))
            with self.assertRaises(CommandError):store.publish(spec,(deps[0],deps[0]))
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:GenerationSpec('other','job','generation',spec.tasks))
            tiny=GenerationStore(Path(root)/'tiny',limits=BarrierLimits(1,1),requirements=LIMITS)
            with self.assertRaises(CommandError):tiny.publish(spec,deps)
            self.assertFalse((Path(root)/'tiny').exists())

    def test_cross_process_store_lock_busy_then_release(self):
        commands=work();spec=generation(commands);ctx=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            _,deps=publish_all(sink,commands);store_root=Path(root)/'generations';store=GenerationStore(store_root,limits=BOUNDS,requirements=LIMITS)
            ready,release=ctx.Event(),ctx.Event();child=ctx.Process(target=hold_lock,args=(str(store_root),ready,release));child.start()
            try:
                self.assertTrue(ready.wait(10));self.rejected(SinkErrorCode.BUSY,lambda:store.publish(spec,deps))
                self.assertFalse(store.read(spec,deps).complete)
            finally:release.set();child.join(10)
            self.assertFalse(child.is_alive());self.assertEqual(child.exitcode,0);self.assertTrue(store.publish(spec,deps).complete)

    def test_protected_source_files_and_rows_preserved(self):
        import duckdb
        commands=work();spec=generation(commands)
        with tempfile.TemporaryDirectory() as root,sink_for('duckdb',Path(root)) as sink:
            original=Path(root)/'original.duckdb'
            with duckdb.connect(str(original)) as connection:connection.execute('CREATE TABLE original AS SELECT 100::BIGINT AS close')
            before=hashlib.sha256(original.read_bytes()).digest();rows=tuple(encode_result(c.results[0]) for c in commands)
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:GenerationStore(Path(root),limits=BOUNDS,requirements=LIMITS,protected_sources=(original,)))
            _,deps=publish_all(sink,commands);store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS,protected_sources=(original,))
            self.assertTrue(store.publish(spec,deps).complete)
            self.assertEqual(hashlib.sha256(original.read_bytes()).digest(),before)
            self.assertEqual(tuple(encode_result(c.results[0]) for c in commands),rows)
            self.assertEqual(ORACLE['original_sources_changed'],False)

    def test_explicit_qualified_sink_factories_use_same_publication(self):
        commands=work()
        class Credentials:
            def get(self,key):raise AssertionError('credentials not requested')
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root:
                registry=SinkRegistry();registry.register('qualified',ParquetSinkFactory() if kind=='parquet' else DuckDBSinkFactory())
                config={'root':str(Path(root)/'output'),'destination_scope':SCOPE} if kind=='parquet' else {'output_path':str(Path(root)/'output.duckdb'),'destination_scope':SCOPE}
                sink=registry.resolve('qualified',config,Credentials(),LIMITS)
                try:
                    _,deps=publish_all(sink,commands);store=GenerationStore(Path(root)/'generations',limits=BOUNDS,requirements=LIMITS)
                    self.assertTrue(store.publish(generation(commands),deps).complete)
                finally:sink.close()
