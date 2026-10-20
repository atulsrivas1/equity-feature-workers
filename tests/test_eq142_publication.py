"""Actual commit/outcome separation; independent fixed boundary arithmetic."""
from copy import deepcopy
from dataclasses import replace
import inspect
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY,qualification,bound_inventory,load,require_cpu_slots
from eq142_publication import PublicationLedger,PublicationHookSession,replay_sizes,OBSERVER_RESERVATION
from eq142_admission import budget_for,original_reservation,delegate_reservation
from eq142_delegate import make_plan,patched_delegate
from eq142_causal import CausalLedger
from eq142_trace import Collector
from eq142_evidence import ProductStore
from equity_feature_workers import BoundedSupervisor,ProgressRecorder,encode_output
if HAVE_INTRADAY:
    from eq142_workloads import items_for
    from benchmark_supplied_inputs import publisher


class FixedAdmissionArithmetic(unittest.TestCase):
    def test_exact_limit_rejection_leaves_counter_for_later_candidate(self):
        # Hand-specified arithmetic: 80+12+8=100; rejected9 does not consume8.
        self.assertEqual(replay_sizes([12,9,8],80,100),[
            {'candidate_bytes':12,'before_bytes':0,'after_bytes':12,'accepted':True},
            {'candidate_bytes':9,'before_bytes':12,'after_bytes':12,'accepted':False},
            {'candidate_bytes':8,'before_bytes':12,'after_bytes':20,'accepted':True}])
        self.assertEqual(replay_sizes([21],80,100)[0]['accepted'],False)
        for args in (([True],0,10),([0],0,10),([1048577],0,2000000),([1]*17,0,100),
                ([1],False,10),([1],11,10),([1],0,True)):
            with self.subTest(args=args),self.assertRaises(ValueError):replay_sizes(*args)
        self.assertEqual(OBSERVER_RESERVATION,69793792)


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ActualPublicationObservation(unittest.TestCase):
    def setup_run(self,root,mode='sequential',degree=1,*,publishing=True,items=None,budget=None):
        items=items_for('family_missing_hl') if items is None else items
        budget=budget or budget_for(degree,require_cpu_slots(self,degree+3))
        sink,pub=publisher() if publishing else (None,None)
        original=original_reservation(items,mode=mode,budget=budget,pending_bytes=8388608 if publishing else 0)
        bound=qualification();inventory=bound_inventory(bound)
        trace=root/'trace';trace.mkdir()
        plan,_=make_plan(items,mode=mode,budget=budget,expected_inventory=inventory,output_dir=trace,
            operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
        collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=plan['operations'])
        collector.bind_labels(tuple(i.task for i in items));causal=CausalLedger(plan,items,collector)
        observation=PublicationLedger(bound,publisher=pub,sink=sink,initial=original['initial_reserved_resident_bytes'],
            maximum=budget.max_resident_bytes,combined=original['initial_reserved_resident_bytes'])
        session=PublicationHookSession(bound,inventory['hooks'],collector,ledger=causal,publication=observation)
        return items,budget,sink,pub,plan,collector,observation,session,original

    def test_actual_seq_thread_spawn_commits_encodes_and_no_publication(self):
        from equity_feature_io_sdk import encode_result
        for mode,degree in (('sequential',1),('thread',2),('process',2)):
            for publishing in (False,True):
                with self.subTest(mode=mode,publishing=publishing),tempfile.TemporaryDirectory() as d:
                    root=Path(d)
                    items,budget,sink,pub,plan,collector,observed,session,base=self.setup_run(root,mode,degree,publishing=publishing)
                    expected={i.task.task_sha256:tuple(encode_result(r) for r in i.calculate(i.task,i.batches)) for i in items}
                    with session,patched_delegate(plan,collector):
                        outcome=BoundedSupervisor(mode=mode,budget=budget).run(items,publisher=pub,progress=ProgressRecorder(max_tasks=16))
                    self.assertEqual({e.task.task_sha256:tuple(encode_result(r) for r in e.results) for e in outcome.tasks},expected)
                    store=ProductStore(root/'objects');record=observed.snapshot(store)
                    checked=observed.verify(record,outcome,store)
                    self.assertEqual(len(record['commits']),3 if publishing else 0)
                    self.assertEqual(len(record['candidates']),3 if publishing else 0)
                    self.assertEqual(checked['cumulative_accepted_output_metadata_bytes'],
                        sum(len(encode_output(e.output)) for e in outcome.tasks if e.output is not None))
                    self.assertFalse(checked['counter_directly_observed']);self.assertTrue(collector.snapshot()['complete'])
                    if publishing:
                        self.assertEqual(sink.calls.count('commit'),3)
                        # Publisher/barrier encoding is present but excluded from
                        # the three direct _run coordinator candidates.
                        count=sum(c['returned'] for c in collector.snapshot()['counters'] if c['operation']=='manifest.encode_output')
                        self.assertGreater(count,3)
                        self.assertTrue(all(len(c['commit_events'])==1 for c in record['candidates']))
                    admitted=delegate_reservation(plan,items,budget,original=base,publication_observation=True)
                    self.assertEqual(admitted['publication_observer_logical_reservation_bytes'],69793792)
                    with self.assertRaises(ValueError):delegate_reservation(plan,items,
                        replace(budget,max_resident_bytes=admitted['combined_logical_resident_bytes']-1),
                        original=base,publication_observation=True)

    def test_actual_commit_then_original_metadata_refusal_is_not_rollback(self):
        # Component fault control, not selected-phase admission: the deliberately
        # tiny original cap cannot admit the full B observer allowance.
        items=items_for('small');budget=budget_for(1,require_cpu_slots(self,4))
        sink,pub=publisher();plain=BoundedSupervisor(mode='sequential',budget=budget).run(items,publisher=pub,progress=ProgressRecorder(max_tasks=16))
        base=original_reservation(items,mode='sequential',budget=budget,pending_bytes=8388608)
        size=len(encode_output(plain.tasks[0].output))
        for offset in (-1,0):
            with self.subTest(offset=offset),tempfile.TemporaryDirectory() as d:
                root=Path(d);limited=replace(budget,max_resident_bytes=base['initial_reserved_resident_bytes']+size+offset)
                items,limited,sink,pub,plan,collector,observed,session,_=self.setup_run(root,items=items,budget=limited)
                with session,patched_delegate(plan,collector):
                    outcome=BoundedSupervisor(mode='sequential',budget=limited).run(items,publisher=pub,progress=ProgressRecorder(max_tasks=16))
                store=ProductStore(root/'objects');record=observed.snapshot(store)
                checked=observed.verify(record,outcome,store,require_success=False)
                self.assertEqual(sink.calls.count('commit'),1)
                self.assertEqual(len(record['commits']),1);self.assertIsNotNone(record['commits'][0]['receipt'])
                self.assertEqual(checked['rows'][0]['accepted'],offset==0)
                self.assertEqual(outcome.tasks[0].reason,'RESOURCE_LIMIT' if offset<0 else None)
                self.assertEqual(outcome.tasks[0].output is not None,offset==0)

    def test_observer_failures_preserve_original_return_and_exception_once(self):
        for where in ('before','after'):
            with self.subTest(where=where),tempfile.TemporaryDirectory() as d:
                *_,collector,observation,session,base=self.setup_run(Path(d))
                calls=[];product=object();original_error=RuntimeError('original failure');cause=OSError('original cause')
                def original(*args):calls.append(args);return product
                def failed(*args):
                    calls.append(args)
                    raise original_error from cause
                for operation in (original,failed):
                    wrapped=session.wrapper(operation,'sink.commit')
                    with patch.object(observation,where,side_effect=ValueError('observer failed')):
                        if operation is original:self.assertIs(wrapped(observation.sink,object()),product)
                        else:
                            with self.assertRaises(RuntimeError) as caught:wrapped(observation.sink,object())
                            self.assertIs(caught.exception,original_error);self.assertIs(caught.exception.__cause__,cause)
                self.assertEqual(len(calls),2);self.assertEqual(observation.failure,'publication_observer_failed')
                self.assertEqual(collector.failure_reason,'collector_error')

    def test_actual_failed_commit_stays_unknown_even_when_sdk_recovers_output(self):
        for fault in ('before_commit','after_commit'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as d:
                root=Path(d)
                items,budget,sink,pub,plan,collector,observed,session,_=self.setup_run(root,items=items_for('small'))
                sink._inner.fault=fault
                with session,patched_delegate(plan,collector):
                    outcome=BoundedSupervisor(mode='sequential',budget=budget).run(items,publisher=pub,progress=ProgressRecorder(max_tasks=16))
                store=ProductStore(root/'objects');record=observed.snapshot(store)
                self.assertEqual(len(record['commits']),1)
                self.assertIsNone(record['commits'][0]['receipt'])
                self.assertEqual(record['events'][record['commits'][0]['event']]['status'],'rejected')
                # A throwing commit supplies no returned receipt to this observer.
                # SDK recovery after a real backend commit is a separate fact.
                self.assertEqual(outcome.tasks[0].output is not None,fault=='after_commit')
                observed.verify(record,outcome,store,require_success=False)
                with self.assertRaises(ValueError):observed.verify(record,outcome,store)

    def test_caller_descriptor_owner_thread_and_event_caps_refuse(self):
        with tempfile.TemporaryDirectory() as d:
            *_,observation,session,base=self.setup_run(Path(d))
            cls=observation.supervisor_class
            with patch.object(cls,'_run',lambda *args:None):
                with self.assertRaises(ValueError):observation.check()
            with self.assertRaises(ValueError):observation.before('sink.commit',(object(),),None)
            errors=[]
            def foreign():
                try:observation.before('sink.commit',(observation.sink,),None)
                except ValueError as error:errors.append(error)
            thread=threading.Thread(target=foreign);thread.start();thread.join()
            self.assertEqual(len(errors),1)
            observation.counts['sink.commit']=16
            with self.assertRaises(ValueError):observation.before('sink.commit',(observation.sink,),None)
            self.assertIsNone(observation.before('manifest.encode_output',(object(),),self.__class__.setup_run.__code__))

    def test_actual_candidate_order_missing_duplicate_and_wire_tamper_reject(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            items,budget,sink,pub,plan,collector,observed,session,_=self.setup_run(root)
            with session,patched_delegate(plan,collector):outcome=BoundedSupervisor(mode='sequential',budget=budget).run(items,publisher=pub,progress=ProgressRecorder(max_tasks=16))
            store=ProductStore(root/'objects');record=observed.snapshot(store);observed.verify(record,outcome,store)
            for change in ('missing','duplicate','order','commit','wire','false_commit','extra','timeline','drain_task'):
                altered=deepcopy(record)
                if change=='missing':altered['candidates'].pop()
                elif change=='duplicate':altered['candidates'].append(altered['candidates'][0])
                elif change=='order':altered['candidates'].reverse()
                elif change=='commit':altered['candidates'][0]['commit_events']=[]
                elif change=='wire':altered['candidates'][0]['output']['bytes']+=1
                elif change=='false_commit':altered['candidates'][0]['commit_events']=[altered['candidates'][0]['drain_event']]
                elif change=='extra':altered['commits'][0]['invented']=True
                elif change=='timeline':altered['events'][0]['timeline_id']='invented'
                else:altered['drains'][0]['rows'][0]['task_sha256']='0'*64
                observed.retained=altered
                with self.subTest(change=change),self.assertRaises(ValueError):observed.verify(altered,outcome,store)

    def test_later_observer_loss_or_proof_failure_retains_partial_commit_events_once(self):
        from equity_feature_io_sdk import encode_receipt
        for fault in ('drain_loss','receipt_store','source_check'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as d:
                root=Path(d)
                items,budget,sink,pub,plan,collector,observed,session,_=self.setup_run(root,items=items_for('small'))
                with session,patched_delegate(plan,collector):
                    outcome=BoundedSupervisor(mode='sequential',budget=budget).run(items,publisher=pub,progress=ProgressRecorder(max_tasks=16))
                self.assertIsNotNone(outcome.tasks[0].output);self.assertEqual(sink.calls.count('commit'),1)
                store=ProductStore(root/'objects');original_put=store.put
                if fault=='drain_loss':
                    # Synthetic observation loss after an actual successful run;
                    # original calculation, backend and outcome stay unchanged.
                    next(e for e in observed.events if e['operation']=='publisher.inner_drain')['result']=()
                    record=observed.snapshot(store)
                elif fault=='receipt_store':
                    def fail_receipt(codec,body):
                        if codec=='encode_receipt':raise OSError('independent receipt storage failure')
                        return original_put(codec,body)
                    with patch.object(store,'put',side_effect=fail_receipt):record=observed.snapshot(store)
                else:
                    with (patch.object(observed,'check',side_effect=OSError('independent source check')),
                            patch('equity_feature_io_sdk.encode_receipt',wraps=encode_receipt) as codec):
                        record=observed.snapshot(store)
                        self.assertEqual(codec.call_count,0)
                self.assertFalse(record['complete']);self.assertTrue(record['retention_failures'])
                self.assertFalse(record['retention_failures_truncated']);self.assertEqual(len(record['events']),3)
                self.assertEqual(len(record['commits']),1);self.assertEqual(len(record['candidates']),1)
                self.assertEqual(store.verify(record['candidates'][0]['output']),encode_output(outcome.tasks[0].output))
                if fault=='drain_loss':
                    self.assertEqual(store.verify(record['commits'][0]['receipt']),encode_receipt(outcome.tasks[0].output.receipt))
                    self.assertIsNone(record['candidates'][0]['task_sha256'])
                    self.assertIsNone(record['candidates'][0]['drain_event'])
                # Repeat returns the retained incomplete observation, with no
                # codec calls or proof writes attempted again.
                with patch.object(store,'put',side_effect=AssertionError('no retry')):
                    self.assertIs(observed.snapshot(store),record)
                with self.assertRaises(ValueError):observed.verify(record,outcome,store)

    def test_runner_keeps_publication_refs_and_original_storage_error(self):
        import json
        from benchmark_execution_baseline import prepare_reference,run_control,SERIES
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);reference=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,4),profiles=('small',))
            error=OSError('independent original proof storage failure');local=root/'run'
            with patch('benchmark_execution_baseline.record_products',side_effect=error):
                with self.assertRaises(OSError) as caught:
                    run_control(root/'reference/reference.json',reference['sha256'],directory=local,profile='small',
                        mode='sequential',degree=1,control='instrumented_B',series=SERIES[1])
            self.assertIs(caught.exception,error)
            runner=json.loads((local/'runner.json').read_bytes())
            self.assertTrue(runner['original_outcome_returned']);self.assertFalse(runner['full_sample_accepted'])
            self.assertEqual(runner['failure_identity']['stage'],'retain_products')
            ref=runner['publication_observation'];observed=json.loads((local/ref['retained_name']).read_bytes())
            self.assertEqual(len(observed['commits']),1);self.assertIsNotNone(observed['commits'][0]['receipt'])
