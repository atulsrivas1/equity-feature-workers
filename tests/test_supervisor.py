"""Independent resource/row/scope/ownership tests and real publication integration."""
from dataclasses import replace
import hashlib
import multiprocessing
import os
import pickle
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_example_extensions import ExampleSink
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_io_contracts import SinkError, SinkErrorCode
from equity_feature_io_sdk import encode_result
from equity_feature_workers import (
    BoundedSupervisor, ResourceBudget, WorkItem, InputReuseCache, ResultSpill,
    partition_tasks, prepare_session, compute_session_inputs, SerialPublisher, PublicationLimits,
)
from supervisor_fixture import ORACLE, facts
from test_commands import inputs, Source
from test_publication import sink_for, Proxy

CALLS=[]
STARTED=threading.Event()

def counted(task,batches):
    CALLS.append(task.task_sha256)
    return compute_session_inputs(task,batches)

def delayed(task,batches):
    STARTED.set()
    time.sleep(.08)
    return compute_session_inputs(task,batches)

def failed(task,batches):
    raise ValueError('secret private backend error')

def wrong(task,batches):
    return list(compute_session_inputs(task,batches))

def history_inputs(task,batches):
    from equity_feature_contracts.history import HistoryContext
    from equity_features.history import compute_history
    from required_fixture import SESSIONS
    CALLS.append(task.config.session.session_id)
    sessions=tuple(s for s in SESSIONS if s.session_id in task.config.window.governed_sessions)
    context=HistoryContext(c.EntityKey(task.instruments[0],task.config.session.session_id),'grid-v1',
                           sessions,(c.Coverage(1,1,True),)*len(sessions))
    return (compute_history(batches[0],task.config,context=context,
                           feature_ids=tuple(h.feature_id for h in task.features)),)

def work(member='A',calculator=compute_session_inputs):
    spec,_,_=inputs()
    config,batch=facts(member)
    requested=replace(spec.request,instruments=(member,))
    spec=replace(spec,request=requested,config=config,partition_id=member+'-S')
    source=Source(batch,requested)
    prepared=prepare_session(spec,source)
    return WorkItem(prepared.task,(prepared.batch,),calculator),source

def all_work(calculator=compute_session_inputs):
    return tuple(work(m,calculator)[0] for m in ORACLE['instruments'])

def budget(workers=1,**changes):
    return replace(ResourceBudget(workers=workers,max_cpu_slots=workers+1,max_in_flight=workers),**changes)

class Supervisor(unittest.TestCase):
    def setUp(self):CALLS.clear();STARTED.clear()
    def rejected(self,code,call):
        with self.assertRaises(SinkError) as caught:call()
        self.assertEqual(caught.exception.code,code)
        self.assertNotIn('secret',str(caught.exception))

    def test_sequential_literal_rows_and_full_result_wire(self):
        items=all_work();out=BoundedSupervisor().run(items)
        self.assertEqual(len(out.tasks),4);self.assertEqual(out.distinct_reuse_rows,8)
        self.assertEqual(sorted(rows for _,rows in out.row_ledger),[2]*4)
        for execution in out.tasks:
            values={col.feature_id:col.values[0] for col in execution.results[0].values}
            self.assertTrue(all(values[k]==v for k,v in ORACLE['per_instrument'].items()))
            item=next(i for i in items if i.task==execution.task)
            self.assertEqual(tuple(map(encode_result,execution.results)),tuple(map(encode_result,item.calculate(item.task,item.batches))))

    def test_thread_and_spawn_process_parity(self):
        items=all_work();reference=BoundedSupervisor().run(items)
        workers=2 if (os.cpu_count() or 1)>=3 else 1
        for mode in ('thread','process'):
            out=BoundedSupervisor(mode=mode,budget=budget(workers)).run(tuple(reversed(items)))
            self.assertEqual(out.row_ledger,reference.row_ledger)
            self.assertEqual(tuple(tuple(map(encode_result,e.results)) for e in out.tasks),tuple(tuple(map(encode_result,e.results)) for e in reference.tasks))
            self.assertEqual(out.distinct_reuse_rows,8)

    def test_deterministic_partition_instrument_components_and_task_population(self):
        items=all_work();a=items[0]
        later=replace(a,task=replace(a.task,partition_id='A-other',job_id='other-job'))
        left=partition_tasks(items+(later,),2);right=partition_tasks(tuple(reversed(items+(later,))),2)
        self.assertEqual(left,right)
        self.assertEqual(len([i for p in left for i in p.items]),5)
        self.assertEqual(len({i.task.task_sha256 for p in left for i in p.items}),5)
        self.assertEqual(len([p for p in left if any('A' in i.task.instruments for i in p.items)]),1)

    def test_duplicate_tasks_and_unsupported_partition_modes(self):
        item,_=work()
        self.rejected(SinkErrorCode.CONFLICT,lambda:partition_tasks((item,item),2))
        self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:partition_tasks((item,),True))
        from equity_feature_workers import ManifestError
        with self.assertRaises(ManifestError):replace(item.task,merge_policy='continuous_quote')

    def test_overlapping_instrument_shard_components_cannot_split(self):
        a,_=work();b,_=work('B');citem,_=work('C')
        bridge=replace(a,task=replace(a.task,instruments=('A','B'),partition_id='AB'))
        partitions=partition_tasks((citem,b,bridge,a),3)
        self.assertEqual(len(partitions),2)
        self.assertEqual(sorted(len(p.items) for p in partitions),[1,3])
        self.assertEqual(partitions,partition_tasks((a,bridge,b,citem),3))

    def test_ordered_history_warmup_and_future_rows_preserved(self):
        from test_barriers import produce
        from required_fixture import daily,SESSIONS
        from equity_feature_contracts.specs import IntervalSpec
        from equity_feature_contracts.history import HistoryContext
        from required_fixture import request,LiteralSource
        from equity_feature_workers import RequiredCommandSpec,run_required
        command=produce('A',batch=daily())[1].command
        later=command.output.task
        cfg=replace(later.config,session=SESSIONS[1],window=c.WindowSpec(3,'S2',('S1','S2'),'completed_eod'),
                    availability=c.AvailabilitySpec(200,210,210))
        batch=daily()
        batch=replace(batch,columns=tuple(c.Column(col.name,col.values[:2]) for col in batch.columns),
            metadata=replace(batch.metadata,source=replace(batch.metadata.source,input_id='daily-A-S2'),
                coverage=c.Coverage(2,2,True),scope=c.InputScope(0,200,'fixture-v1')))
        requested=replace(request(),sessions=('S1','S2'),end_ns=200,availability=cfg.availability,max_rows=2,max_batch_rows=2)
        context=HistoryContext(c.EntityKey('A','S2'),'grid-v1',SESSIONS[:2],(c.Coverage(1,1,True),)*2)
        spec=RequiredCommandSpec(later.job_id,later.generation_id,'A-S2','history',cfg,
            (IntervalSpec('S1',0,100),IntervalSpec('S2',100,200)),'revision1','synthetic-conformance',65536,
            request=requested,context=context,feature_ids=('history.return',))
        earlier=run_required(spec,LiteralSource(batch,requested),ExampleSink(),requirements=LIMITS).command.output.task
        self.rejected(SinkErrorCode.INVALID_CONTENT,lambda:WorkItem(earlier,(daily(),),history_inputs))
        items=(WorkItem(later,(daily(),),history_inputs),WorkItem(earlier,(batch,),history_inputs))
        ordered=partition_tasks(items,2)[0].items
        self.assertEqual([i.task.config.session.session_id for i in ordered],['S2','S3'])
        out=BoundedSupervisor().run(items)
        self.assertEqual(CALLS,['S2','S3'])
        by_session={e.task.config.session.session_id:e for e in out.tasks}
        self.assertIsNone(by_session['S2'].results[0].values[0].values[0])
        self.assertAlmostEqual(by_session['S3'].results[0].values[0].values[0],.2)
        self.assertEqual(later.warmup_sessions,('S1','S2'))

    def test_input_reuse_has_one_acquisition_and_same_objects(self):
        item,source=work();cache=InputReuseCache()
        cache.put(item.task,item.batches)
        compatible=replace(item.task,job_id='other-job',partition_id='other-partition')
        self.assertIs(cache.get(compatible),item.batches)
        out=BoundedSupervisor().run((item,replace(item,task=compatible)))
        self.assertEqual(out.distinct_reuse_rows,2);self.assertEqual(sum(r for _,r in out.row_ledger),4)
        self.assertEqual(source.called,1)

    def test_changed_revision_config_and_cutoff_miss_reuse(self):
        item,_=work();cache=InputReuseCache();cache.put(item.task,item.batches)
        revision=replace(item.task,inputs=tuple(replace(i,revision_id='revision2') for i in item.task.inputs))
        changedcfg=replace(item.task.config,identity='other-config')
        cutoffcfg=replace(item.task.config,availability=replace(item.task.config.availability,knowledge_cutoff_ns=211,evaluation_ns=211))
        for task in (revision,replace(item.task,config=changedcfg),replace(item.task,config=cutoffcfg)):
            self.assertIsNone(cache.get(task))
        cache.clear();self.assertIsNone(cache.get(item.task))

    def test_cache_count_bytes_scope_conflict_before_replacement(self):
        a,_=work();b,_=work('B');cache=InputReuseCache(max_entries=1)
        cache.put(a.task,a.batches)
        self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:cache.put(b.task,b.batches))
        self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:InputReuseCache(max_bytes=1).put(a.task,a.batches))
        mutated=replace(a.batches[0],columns=tuple(c.Column(x.name,(201,300)) if x.name=='volume' else x for x in a.batches[0].columns))
        self.rejected(SinkErrorCode.CONFLICT,lambda:cache.put(a.task,(mutated,)))
        self.assertIs(cache.get(a.task),a.batches)

    def test_same_reuse_scope_different_rows_rejected_before_callbacks(self):
        a,_=work(calculator=counted)
        changed=replace(a.batches[0],columns=tuple(c.Column(x.name,(201,300)) if x.name=='volume' else x for x in a.batches[0].columns))
        other=WorkItem(replace(a.task,partition_id='other'),(changed,),counted)
        self.rejected(SinkErrorCode.CONFLICT,lambda:BoundedSupervisor().run((a,other)))
        self.assertEqual(CALLS,[])

    def test_foreign_binding_and_input_byte_limit(self):
        a,_=work();b,_=work('B')
        self.rejected(SinkErrorCode.INVALID_CONTENT,lambda:WorkItem(a.task,b.batches))
        self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:WorkItem(replace(a.task,max_input_bytes=1),a.batches))

    def test_cpu_backend_worker_and_io_budget_admission(self):
        for changes in ({'workers':2},{'backend_threads':2},{'compute_threads':2},{'max_in_flight':2},{'io_slots':2},{'workers':True}):
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda changes=changes:replace(ResourceBudget(),**changes))
        self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:BoundedSupervisor(mode='sequential',budget=budget(2)))
        with patch('equity_feature_workers.supervisor.os.cpu_count',return_value=1):
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:BoundedSupervisor())

    def test_count_input_transport_resident_and_returned_result_admission(self):
        items=all_work(counted)
        for changes in ({'max_tasks':1},{'max_input_bytes':1},{'max_resident_bytes':1},
                        {'max_total_result_bytes':262144}):
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda changes=changes:BoundedSupervisor(budget=budget(**changes)).run(items))
        self.assertEqual(CALLS,[])
        # The same frozen work requires an extra actual pickle transport reservation in process mode.
        seq=BoundedSupervisor().run(items)
        process=BoundedSupervisor(mode='process').run(items)
        self.assertGreater(process.reserved_resident_bytes,seq.reserved_resident_bytes)

    def test_callable_closure_and_non_importable_payload_rejected(self):
        item,_=work()
        self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:replace(item,calculate=lambda task,batches:()))

    def test_calculation_failure_and_invalid_results_fixed_no_false_success(self):
        for calculator,diagnosis in ((failed,'CALCULATION_FAILED'),(wrong,'INVALID_CONTENT')):
            out=BoundedSupervisor().run((work(calculator=calculator)[0],))
            self.assertIsNone(out.tasks[0].results);self.assertEqual(out.tasks[0].reason,diagnosis)
            self.assertIsNone(out.tasks[0].output);self.assertNotIn('secret',repr(out))

    def test_result_codec_cap_is_applied_to_actual_computation(self):
        out=BoundedSupervisor(budget=budget(max_task_result_bytes=1)).run((work()[0],))
        self.assertEqual(out.tasks[0].reason,'RESOURCE_LIMIT');self.assertIsNone(out.tasks[0].results)

    def test_owner_thread_and_cache_lifetime(self):
        item,_=work();supervisor=BoundedSupervisor();cache=InputReuseCache();errors=[]
        def foreign():
            for call in (lambda:supervisor.run((item,)),lambda:cache.put(item.task,item.batches)):
                try:call()
                except SinkError as error:errors.append(error.code)
        thread=threading.Thread(target=foreign);thread.start();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[SinkErrorCode.INVALID_SESSION]*2)
        self.assertEqual(supervisor.run((item,)).tasks[0].reason,None)

    def test_cancel_before_start_no_callbacks_or_spill_creation(self):
        class Stop:
            def is_cancelled(self):return True
        with tempfile.TemporaryDirectory() as root:
            spill=ResultSpill(Path(root)/'spill')
            out=BoundedSupervisor().run(all_work(counted),cancellation=Stop(),spill=spill)
            self.assertTrue(out.cancelled);self.assertEqual(CALLS,[])
            self.assertTrue(all(e.reason=='CANCELLED' for e in out.tasks));self.assertFalse(spill.root.exists())

    def test_running_thread_cancel_joins_and_retains_completed_computation(self):
        event=threading.Event()
        class Token:
            def is_cancelled(self):return event.is_set()
        items=all_work(delayed)
        def stop_started():
            STARTED.wait(3);event.set()
        timer=threading.Thread(target=stop_started);timer.start()
        try:
            out=BoundedSupervisor(mode='thread',budget=budget(2 if (os.cpu_count() or 1)>=3 else 1,max_in_flight=1)).run(items,cancellation=Token())
        finally:timer.join(2)
        self.assertTrue(out.cancelled)
        self.assertTrue(any(e.results is not None for e in out.tasks))
        self.assertTrue(all(e.output is None for e in out.tasks))
        self.assertFalse(any(t.name.startswith('ThreadPoolExecutor') for t in threading.enumerate()))

    def test_spawn_cancel_stops_unsubmitted_coarse_shard_and_joins(self):
        if (os.cpu_count() or 1)<3:self.skipTest('Two compute workers plus coordinator require three declared CPU slots')
        class Token:
            calls=0
            def is_cancelled(self):
                self.calls+=1;return self.calls>1
        before={p.pid for p in multiprocessing.active_children()}
        out=BoundedSupervisor(mode='process',budget=budget(2,max_in_flight=1)).run(all_work(),cancellation=Token())
        self.assertTrue(out.cancelled)
        self.assertEqual(sum(e.results is not None for e in out.tasks),2)
        self.assertEqual(sum(e.results is None for e in out.tasks),2)
        self.assertEqual({p.pid for p in multiprocessing.active_children()},before)

    def test_isolated_spill_exact_existing_codec_readback(self):
        items=all_work()
        with tempfile.TemporaryDirectory() as root:
            spill=ResultSpill(Path(root)/'spill');out=BoundedSupervisor().run(items,spill=spill)
            for execution in out.tasks:
                self.assertIsNone(execution.results);self.assertIsNotNone(execution.spill)
                task,results=spill.read(execution.spill,budget=budget())
                self.assertEqual(task,execution.task)
                item=next(i for i in items if i.task==task)
                self.assertEqual(tuple(map(encode_result,results)),tuple(map(encode_result,item.calculate(task,item.batches))))
            other=ResultSpill(Path(root)/'spill')
            self.rejected(SinkErrorCode.INVALID_CONTENT,lambda:other.read(out.tasks[0].spill,budget=budget()))

    def test_spill_admission_before_calculation_and_root_mutation(self):
        with tempfile.TemporaryDirectory() as root:
            spill=ResultSpill(Path(root)/'spill',max_bytes=1)
            self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:BoundedSupervisor().run(all_work(counted),spill=spill))
            self.assertEqual(CALLS,[]);self.assertFalse(spill.root.exists())

    def test_spill_corruption_and_forged_reference_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            spill=ResultSpill(Path(root)/'spill');execution=BoundedSupervisor().run((work()[0],),spill=spill).tasks[0]
            reference=execution.spill
            self.rejected(SinkErrorCode.INVALID_CONTENT,lambda:spill.read(replace(reference,files=reference.files[::-1]),budget=budget()))
            path=spill.directory/reference.files[-1][0];path.write_bytes(b'corrupt secret content')
            self.rejected(SinkErrorCode.CORRUPTION,lambda:spill.read(reference,budget=budget()))

    def test_spill_partial_failure_reserved_results_retained_and_owned_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            spill=ResultSpill(Path(root)/'spill')
            with patch('equity_feature_workers.supervisor.os.fsync',side_effect=OSError('secret')):
                out=BoundedSupervisor().run((work()[0],),spill=spill)
            self.assertEqual(out.tasks[0].reason,'UNAVAILABLE');self.assertIsNotNone(out.tasks[0].results)
            self.assertLess(spill.remaining_bytes,spill.max_bytes)
            foreign=spill.directory/'unrelated.txt';foreign.write_bytes(b'preserve')
            spill.close(remove=True);self.assertEqual(foreign.read_bytes(),b'preserve')
            self.assertEqual([p.name for p in spill.directory.iterdir()],['unrelated.txt'])

    def test_spill_protected_original_source_bytes_and_paths(self):
        with tempfile.TemporaryDirectory() as root:
            base=Path(root);source=base/'original.db';source.write_bytes(b'synthetic original unchanged')
            before=hashlib.sha256(source.read_bytes()).hexdigest()
            self.rejected(SinkErrorCode.INVALID_CONFIG,lambda:ResultSpill(base,protected_sources=(source,)))
            spill=ResultSpill(base/'spill',protected_sources=(source,))
            BoundedSupervisor().run(all_work(),spill=spill);spill.close(remove=True)
            self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(),before)
            self.assertFalse(spill.directory.exists())

    def test_spill_file_collision_never_owns_or_deletes_unrelated_file(self):
        from types import SimpleNamespace
        item,_=work()
        with tempfile.TemporaryDirectory() as root:
            spill=ResultSpill(Path(root)/'spill')
            BoundedSupervisor().run((item,),spill=spill)
            foreign=spill.directory/(item.task.task_sha256+'-'+'f'*32+'-task.json')
            foreign.write_bytes(b'preserve unrelated collision')
            with patch('equity_feature_workers.supervisor.uuid.uuid4',return_value=SimpleNamespace(hex='f'*32)):
                out=BoundedSupervisor().run((item,),spill=spill)
            self.assertEqual(out.tasks[0].reason,'UNAVAILABLE');self.assertIsNotNone(out.tasks[0].results)
            spill.close(remove=True)
            self.assertEqual(foreign.read_bytes(),b'preserve unrelated collision')

    def test_spill_empty_directory_collision_preserved_after_failed_creation(self):
        with tempfile.TemporaryDirectory() as root:
            spill=ResultSpill(Path(root)/'spill')
            spill.directory.mkdir(parents=True)
            out=BoundedSupervisor().run((work()[0],),spill=spill)
            self.assertEqual(out.tasks[0].reason,'UNAVAILABLE')
            self.assertIsNotNone(out.tasks[0].results)
            self.assertFalse(spill._created);self.assertEqual(spill._owned,{})
            spill.close(remove=True)
            self.assertTrue(spill.directory.is_dir())
            self.assertEqual(list(spill.directory.iterdir()),[])

    def test_terminal_foreign_publisher_owner_rejected_before_calculation(self):
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            holders=[]
            def create():
                holders.append(SerialPublisher(sink,'synthetic-conformance',limits=PublicationLimits(),requirements=LIMITS))
            owner=threading.Thread(target=create);owner.start();owner.join()
            publisher=holders[0]
            self.rejected(SinkErrorCode.INVALID_SESSION,
                lambda:BoundedSupervisor().run((work(calculator=counted)[0],),publisher=publisher))
            self.assertEqual(CALLS,[]);self.assertEqual(publisher.pending,())

    def test_process_transport_task_metadata_admitted_before_callbacks(self):
        item,_=work(calculator=counted)
        self.rejected(SinkErrorCode.RESOURCE_LIMIT,
            lambda:BoundedSupervisor(mode='process',budget=budget(max_task_transport_bytes=1)).run((item,)))
        self.assertEqual(CALLS,[])

    def test_process_known_copy_reservations_and_resident_boundary(self):
        items=all_work();limits=budget()
        seq=BoundedSupervisor(budget=limits).run(items)
        out=BoundedSupervisor(mode='process',budget=limits).run(items)
        partitions=partition_tasks(items,1)
        additional=2*len(pickle.dumps(partitions[0],protocol=5))
        additional+=3*(len(items)*limits.max_task_transport_bytes+len(pickle.dumps((None,)*len(items),protocol=5)))
        self.assertEqual(out.reserved_resident_bytes-seq.reserved_resident_bytes,additional)
        counted_items=all_work(counted)
        counted_bound=BoundedSupervisor(mode='process',budget=limits).run(counted_items).reserved_resident_bytes
        self.rejected(SinkErrorCode.RESOURCE_LIMIT,lambda:BoundedSupervisor(mode='process',
            budget=budget(max_resident_bytes=counted_bound-1)).run(counted_items))
        self.assertEqual(CALLS,[])

    def test_process_result_transport_bound_enforced_in_child(self):
        from equity_feature_workers.supervisor import TaskExecution
        from equity_feature_workers.commands import CommandErrorCode
        item,_=work()
        empty=max(len(pickle.dumps(TaskExecution(item.task,None,None,None,r),protocol=5))
                  for r in tuple(code.value for code in SinkErrorCode)+tuple(code.value for code in CommandErrorCode))
        results=compute_session_inputs(item.task,item.batches)
        self.assertGreater(len(pickle.dumps(TaskExecution(item.task,results,None,None,None),protocol=5)),empty)
        out=BoundedSupervisor(mode='process',budget=budget(max_task_transport_bytes=empty)).run((item,))
        self.assertEqual(out.tasks[0].reason,'RESOURCE_LIMIT');self.assertIsNone(out.tasks[0].results)

    def test_physical_publisher_both_sinks_owner_and_exact_wire(self):
        items=all_work()
        for kind in ('parquet','duckdb'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root,sink_for(kind,Path(root)) as sink:
                publisher=SerialPublisher(sink,'synthetic-conformance',limits=PublicationLimits(),requirements=LIMITS)
                out=BoundedSupervisor().run(items,publisher=publisher)
                self.assertEqual(publisher.pending,())
                for e in out.tasks:
                    self.assertIsNotNone(e.output);self.assertIsNone(e.reason)
                    self.assertEqual(tuple(map(encode_result,sink.read(e.output.receipt))),tuple(map(encode_result,e.results)))

    def test_publisher_existing_pending_is_busy_before_compute(self):
        item,_=work(calculator=counted)
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            publisher=SerialPublisher(sink,'synthetic-conformance',limits=PublicationLimits(),requirements=LIMITS)
            publisher.submit(item.task,compute_session_inputs(item.task,item.batches))
            self.rejected(SinkErrorCode.BUSY,lambda:BoundedSupervisor().run((item,),publisher=publisher))
            self.assertEqual(CALLS,[]);self.assertEqual(len(publisher.pending),1)

    def test_publisher_fault_backpressure_retains_every_computed_result(self):
        class Broken(Proxy):
            def lookup(self,key):raise ValueError('secret writer busy')
        with tempfile.TemporaryDirectory() as root,sink_for('parquet',Path(root)) as sink:
            publisher=SerialPublisher(Broken(sink),'synthetic-conformance',limits=PublicationLimits(1,8388608),requirements=LIMITS)
            out=BoundedSupervisor().run(all_work(),publisher=publisher)
            self.assertTrue(all(e.results is not None and e.output is None and e.reason for e in out.tasks))
            self.assertEqual(len(publisher.pending),1)
            self.assertNotIn('secret',repr(out))

    def test_compatible_public_prepare_missing_empty_partial_inputs(self):
        spec,batch,_=inputs()
        for batch in (None,replace(batch,columns=tuple(c.Column(x.name,()) for x in batch.columns),
                                  metadata=replace(batch.metadata,coverage=c.Coverage(0,0,True))),
                      replace(batch,metadata=replace(batch.metadata,coverage=c.Coverage(3,2,False)))):
            source=Source(batch,spec.request);prepared=prepare_session(spec,source)
            item=WorkItem(prepared.task,() if prepared.batch is None else (prepared.batch,))
            execution=BoundedSupervisor().run((item,)).tasks[0]
            self.assertIsNotNone(execution.results);self.assertEqual(source.called,1)

if __name__=='__main__':unittest.main()
