"""Actual waited shutdown, zero-submit cancellation and manual Future evidence."""
from copy import deepcopy
from concurrent.futures import Future,ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_code_counts import CodeCounts,validate_profile,for_original_run
from eq142_fixture import HAVE_INTRADAY,qualification,bound_inventory,load,require_cpu_slots
from eq142_graph import GraphSpecificationError
from eq142_causal import CausalLedger,CausalHookSession
from eq142_delegate import make_plan,patched_delegate
from eq142_trace import Collector,TraceSpecificationError
from eq142_admission import budget_for
from equity_feature_workers import BoundedSupervisor
from equity_feature_io_sdk import encode_result
if HAVE_INTRADAY:from supplied_intraday_fixture import facts,work


class CodeIdentityCardinality(unittest.TestCase):
    def test_42_hook_and_delegate_identities_admit_43_reject(self):
        codes=[compile(f'def selected_{i}(): return {i}',f'<synthetic-code-{i}>','exec').co_consts[0] for i in range(43)]
        operations={code:f'operation:{i:03d}' for i,code in enumerate(codes[:42])}
        observer=CodeCounts(operations,root_codes=(codes[0],));record=observer.snapshot()
        record['counters']=[{'operation':name,'call_entries':0,'return_events':0,'typed_byte_returns':0,
            'typed_returned_bytes':0} for name in sorted(operations.values())]
        validate_profile(record)
        with self.assertRaises(ValueError):CodeCounts({**operations,codes[42]:'operation:042'},root_codes=(codes[0],))
        changed=deepcopy(record);changed['counters'].append({**record['counters'][0],'operation':'operation:042'})
        with self.assertRaises(ValueError):validate_profile(changed)


class AlwaysCancelled:
    def is_cancelled(self):return True


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ExecutorObservations(unittest.TestCase):
    def setup_run(self,root,mode,degree):
        budget=budget_for(degree,require_cpu_slots(self,degree+3));items=[]
        for symbol in ('A','B','C'):
            context,config,batch=facts(symbol);items.append(work(context,config,(batch,),name='executor-'+symbol))
        items=tuple(items);bound=qualification();inventory=bound_inventory(bound)
        plan,_=make_plan(items,mode=mode,budget=budget,expected_inventory=inventory,output_dir=root,
            operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
        collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=plan['operations'])
        collector.bind_labels(tuple(item.task for item in items));ledger=CausalLedger(plan,items,collector)
        return items,budget,bound,inventory,plan,collector,ledger

    def test_actual_three_modes_waited_shutdown_and_terminal_manual_queries(self):
        require_cpu_slots(self,5)
        for mode,degree in (('sequential',1),('thread',2),('process',2)):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                items,budget,bound,inventory,plan,collector,ledger=self.setup_run(Path(directory),mode,degree)
                profile=for_original_run(bound,inventory['hooks'])
                self.assertEqual(len(profile.operations),42)
                self.assertFalse(any(op.startswith('future_terminal.') for op in profile.operations.values()))
                expected={i.task.task_sha256:tuple(encode_result(r) for r in i.calculate(i.task,i.batches)) for i in items}
                with CausalHookSession(bound,inventory['hooks'],collector,ledger=ledger),patched_delegate(plan,collector):
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items)
                self.assertEqual({e.task.task_sha256:tuple(encode_result(r) for r in e.results) for e in outcome.tasks},expected)
                rows=ledger.terminal_snapshot();ledger.require_waited_shutdown(rows)
                self.assertEqual(ledger.terminal_snapshot(),rows) # read retained observation, no re-query
                shutdown=ledger.shutdown_snapshot()
                if mode=='sequential':self.assertEqual(rows,[]);self.assertEqual(shutdown,[])
                else:
                    self.assertEqual(len(shutdown),1);self.assertEqual(shutdown[0]['operation'],f'pool.{mode}_shutdown')
                    self.assertTrue(shutdown[0]['wait']);self.assertTrue(shutdown[0]['cancel_futures'])
                    self.assertEqual(shutdown[0]['status'],'returned');self.assertIsNone(shutdown[0]['error_type'])
                    self.assertEqual(len(rows),len(plan['reservations']))
                    self.assertTrue(all(r['done'] and not r['cancelled'] and not r['running'] for r in rows))
                    with self.assertRaises(TraceSpecificationError):ledger.require_waited_shutdown([])
                    self.assertGreaterEqual(int(shutdown[0]['parent_end_ns']),int(shutdown[0]['parent_start_ns']))
                self.assertTrue(collector.snapshot()['complete']);bound.check()

    def test_zero_submit_cancellation_observes_pool_without_inventing_future(self):
        require_cpu_slots(self,5)
        for mode,degree in (('sequential',1),('thread',2),('process',2)):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                items,budget,bound,inventory,plan,collector,ledger=self.setup_run(Path(directory),mode,degree)
                with CausalHookSession(bound,inventory['hooks'],collector,ledger=ledger),patched_delegate(plan,collector):
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items,cancellation=AlwaysCancelled())
                self.assertTrue(outcome.cancelled);self.assertTrue(all(e.reason=='CANCELLED' and e.results is None for e in outcome.tasks))
                self.assertEqual(ledger.futures,{})
                rows=ledger.terminal_snapshot();self.assertEqual(rows,[]);ledger.require_waited_shutdown(rows)
                self.assertEqual(len(ledger.shutdown_snapshot()),0 if mode=='sequential' else 1)
                self.assertTrue(all(not (Path(directory)/r['file']).exists() for r in plan['reservations']))

    def test_manual_descriptor_interference_rejects_before_or_after_binding(self):
        with patch.object(Future,'done',lambda self:True):
            with self.assertRaises(GraphSpecificationError):qualification()
        with tempfile.TemporaryDirectory() as directory:
            items,budget,bound,inventory,plan,collector,ledger=self.setup_run(Path(directory),'thread',2)
            with CausalHookSession(bound,inventory['hooks'],collector,ledger=ledger),patched_delegate(plan,collector):
                outcome=BoundedSupervisor(mode='thread',budget=budget).run(items)
            self.assertTrue(all(e.reason is None for e in outcome.tasks))
            with patch.object(Future,'done',lambda self:False):
                with self.assertRaises(GraphSpecificationError):ledger.terminal_snapshot()
            bound.check()

    def test_shutdown_observer_failure_preserves_normal_original_products(self):
        with tempfile.TemporaryDirectory() as directory:
            items,budget,bound,inventory,plan,collector,ledger=self.setup_run(Path(directory),'thread',2)
            original=ledger.before
            def observed(operation,*args,**kwargs):
                if operation.endswith('_shutdown'):raise RuntimeError('synthetic observer refusal')
                return original(operation,*args,**kwargs)
            with patch.object(ledger,'before',side_effect=observed),CausalHookSession(
                    bound,inventory['hooks'],collector,ledger=ledger),patched_delegate(plan,collector):
                outcome=BoundedSupervisor(mode='thread',budget=budget).run(items)
            self.assertTrue(all(e.reason is None and e.results for e in outcome.tasks))
            self.assertEqual(ledger.shutdown_snapshot(),[]);self.assertFalse(collector.snapshot()['complete'])
            with self.assertRaises(TraceSpecificationError):ledger.terminal_snapshot()
            self.assertTrue(ledger.retained_terminal_rows())

    def test_actual_shutdown_exception_identity_is_not_repaired_by_done_futures(self):
        with tempfile.TemporaryDirectory() as directory:
            items,budget,bound,inventory,plan,collector,ledger=self.setup_run(Path(directory),'thread',2)
            before=ledger.before;error=RuntimeError('injected join error inside actual shutdown');changed=[]
            def observed(operation,args,*rest,**kwargs):
                value=before(operation,args,*rest,**kwargs)
                if operation=='pool.thread_shutdown':
                    thread=next(iter(args[0]._threads));original_join=thread.join
                    def joined(*a,**k):original_join(*a,**k);raise error
                    thread.join=joined;changed.append(thread)
                return value
            try:
                with patch.object(ledger,'before',side_effect=observed),CausalHookSession(
                        bound,inventory['hooks'],collector,ledger=ledger),patched_delegate(plan,collector):
                    with self.assertRaises(RuntimeError) as caught:BoundedSupervisor(mode='thread',budget=budget).run(items)
                self.assertIs(caught.exception,error)
            finally:
                for thread in changed:del thread.join
                if ledger.executor_owner is not None:
                    for thread in ledger.executor_owner._threads:
                        thread.join(timeout=2);self.assertFalse(thread.is_alive())
            rows=ledger.terminal_snapshot();self.assertTrue(all(r['done'] for r in rows))
            shutdown=ledger.shutdown_snapshot();self.assertEqual(len(shutdown),1)
            self.assertEqual(shutdown[0]['status'],'rejected');self.assertEqual(shutdown[0]['error_type'],'builtins.RuntimeError')
            with self.assertRaises(TraceSpecificationError):ledger.require_waited_shutdown(rows)


if __name__=='__main__':unittest.main()
