"""Actual sequential/thread/spawn delegate parity and separate reservations."""
import inspect
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from equity_feature_workers import BoundedSupervisor,ResourceBudget
from equity_feature_io_sdk import encode_result
import equity_feature_workers.supervisor as supervisor
from eq142_delegate import RoutingHookSession,make_plan,patched_delegate
from eq142_trace import Collector,validate_fragment
from eq142_fixture import qualification,load,HAVE_INTRADAY,bound_inventory
if HAVE_INTRADAY:
    from supplied_intraday_fixture import facts,work


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class PartitionDelegation(unittest.TestCase):
    def items(self):
        result=[]
        for name in ('A','B'):
            context,config,batch=facts(name)
            result.append(work(context,config,(batch,),name='delegate-'+name))
        return tuple(result)

    def test_actual_modes_original_callback_and_full_sdk_parity(self):
        items=self.items()
        expected={item.task.task_sha256:tuple(encode_result(r) for r in item.calculate(item.task,item.batches)) for item in items}
        original_target=supervisor._compute_partition
        operations=load('RECORD_CONTRACTS.json')['trace']['operations']
        for mode,degree in [('sequential',1),('thread',2),('process',2)]:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                bound=qualification();inventory=bound_inventory(bound)
                budget=ResourceBudget(workers=degree,max_cpu_slots=4,max_in_flight=degree,max_resident_bytes=134217728)
                plan,metadata_bytes=make_plan(items,mode=mode,budget=budget,expected_inventory=inventory,
                    output_dir=directory,operations=operations)
                self.assertLessEqual(metadata_bytes,1048576)
                self.assertEqual(len(plan['reservations']),2)
                self.assertLessEqual(sum(r['max_spans'] for r in plan['reservations'])+16384,131072)
                self.assertLessEqual(sum(r['max_bytes'] for r in plan['reservations'])+8388608,67108864)
                collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=operations)
                collector.bind_labels(tuple(item.task for item in items))
                with collector.phase('stage:supervisor'),RoutingHookSession(bound,inventory['hooks'],collector),patched_delegate(plan,collector):
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items)
                self.assertIs(supervisor._compute_partition,original_target)
                bound.check()
                self.assertFalse(outcome.cancelled)
                self.assertEqual({t.task.task_sha256:tuple(encode_result(r) for r in t.results) for t in outcome.tasks},expected)
                self.assertTrue(all(t.reason is None and t.output is None and t.spill is None for t in outcome.tasks))
                parent=collector.snapshot();validate_fragment(parent)
                for reservation in plan['reservations']:
                    path=Path(directory)/reservation['file'];self.assertTrue(path.is_file())
                    self.assertLessEqual(path.stat().st_size,reservation['max_bytes'])
                    fragment=json.loads(path.read_bytes());validate_fragment(fragment)
                    if mode=='process':self.assertNotEqual(fragment['process_binding']['pid'],os.getpid())
                    else:self.assertEqual(fragment['process_binding']['pid'],os.getpid())
                    calls=[s for s in fragment['spans'] if s['operation']=='delegate.original_partition']
                    self.assertEqual(len(calls),1)
                    self.assertEqual(sum(c['attempts'] for c in fragment['counters'] if c['operation']=='supplied.admit'),1)
                self.assertEqual(len(list(Path(directory).iterdir())),2)


if __name__=='__main__':unittest.main()
