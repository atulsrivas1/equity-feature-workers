"""Actual Future owners bind dispatch/wait/result to exact original partitions."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY,qualification,bound_inventory,load
from eq142_causal import CausalLedger,CausalHookSession
from eq142_delegate import make_plan,patched_delegate
from eq142_trace import Collector
from eq142_evidence import combine_fragments
from equity_feature_workers import BoundedSupervisor,ResourceBudget
from equity_feature_io_sdk import encode_result
if HAVE_INTRADAY:
    from supplied_intraday_fixture import facts,work


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ActualFutureJoins(unittest.TestCase):
    def test_actual_modes_and_observer_failure_preserve_product(self):
        items=[]
        for name in ('A','B'):
            context,config,batch=facts(name);items.append(work(context,config,(batch,),name='causal-'+name))
        items=tuple(items)
        expected={item.task.task_sha256:tuple(encode_result(r) for r in item.calculate(item.task,item.batches)) for item in items}
        for mode,degree in (('sequential',1),('thread',2),('process',2)):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                bound=qualification();inventory=bound_inventory(bound)
                budget=ResourceBudget(workers=degree,max_cpu_slots=4,max_in_flight=degree,max_resident_bytes=134217728)
                plan,_=make_plan(items,mode=mode,budget=budget,expected_inventory=inventory,
                    output_dir=directory,operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
                collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=plan['operations'])
                collector.bind_labels(tuple(i.task for i in items));ledger=CausalLedger(plan,items,collector)
                with collector.phase('stage:supervisor'),CausalHookSession(bound,inventory['hooks'],collector,ledger=ledger),patched_delegate(plan,collector):
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items)
                self.assertEqual({e.task.task_sha256:tuple(encode_result(r) for r in e.results) for e in outcome.tasks},expected)
                collector.persist(Path(directory)/'parent.json')
                joined=combine_fragments(directory,[{'id':'parent','file':'parent.json','max_spans':16384,'max_bytes':8388608},*plan['reservations']])
                rows=ledger.snapshot((directory,joined))
                if mode=='sequential':self.assertEqual(rows,[])
                else:
                    self.assertEqual(len(ledger.futures),2)
                    for kind in ('dispatch','result_join'):
                        selected=[r for r in rows if r['kind']==kind]
                        self.assertEqual(len(selected),2)
                        self.assertEqual({r['invocation_id'] for r in selected},{r['id'] for r in plan['reservations']})
                    self.assertTrue(any(r['kind']=='parent_wait' for r in rows))
                    for row in rows:
                        self.assertFalse(row['cross_process_interval_subtraction'])
                        self.assertEqual(row['status'],'returned');self.assertTrue(row['child_timeline_ids'])
                        self.assertGreaterEqual(int(row['parent_end_ns']),int(row['parent_start_ns']))
        # Inject observer failure before the actual Executor.submit operation.
        with tempfile.TemporaryDirectory() as directory:
            bound=qualification();inventory=bound_inventory(bound);budget=ResourceBudget(workers=2,max_cpu_slots=4,max_in_flight=2,max_resident_bytes=134217728)
            plan,_=make_plan(items,mode='thread',budget=budget,expected_inventory=inventory,output_dir=directory,
                operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
            collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=plan['operations'])
            collector.bind_labels(tuple(i.task for i in items));ledger=CausalLedger(plan,items,collector)
            with (patch.object(ledger,'before',side_effect=RuntimeError('observer only')),
                    CausalHookSession(bound,inventory['hooks'],collector,ledger=ledger),patched_delegate(plan,collector)):
                outcome=BoundedSupervisor(mode='thread',budget=budget).run(items)
            self.assertEqual({e.task.task_sha256:tuple(encode_result(r) for r in e.results) for e in outcome.tasks},expected)
            self.assertFalse(collector.snapshot()['complete'])


if __name__=='__main__':unittest.main()
