"""Actual original admission joins, including instrumented specification work."""
from dataclasses import replace
from pathlib import Path
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY,qualification,bound_inventory,load
from eq142_admission import budget_for,original_reservation,delegate_reservation,confirm_original
from equity_feature_workers import BoundedSupervisor,ProgressRecorder,encode_output
if HAVE_INTRADAY:
    from eq142_workloads import items_for
    from benchmark_supplied_inputs import publisher
    from eq142_delegate import make_plan


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ActualAdmission(unittest.TestCase):
    def test_actual_original_six_mode_publication_admission_joins(self):
        items=items_for('family_missing_hl')
        for mode,degree in (('sequential',1),('thread',2),('process',2)):
            for publishing in (False,True):
                with self.subTest(mode=mode,publishing=publishing):
                    budget=budget_for(degree,8);progress=ProgressRecorder(max_tasks=16)
                    sink,pub=publisher() if publishing else (None,None)
                    proposed=original_reservation(items,mode=mode,budget=budget,pending_bytes=8388608 if publishing else 0)
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items,publisher=pub,progress=progress)
                    self.assertTrue(all(t.reason is None for t in outcome.tasks))
                    confirm_original(outcome,proposed)
                    self.assertEqual(progress.reserved_bytes,proposed['diagnostic_reserved_bytes'])
                    outputs=sum(len(encode_output(t.output)) for t in outcome.tasks if t.output is not None)
                    self.assertLessEqual(proposed['initial_reserved_resident_bytes']+outputs,budget.max_resident_bytes)
                    with self.assertRaises(ValueError):confirm_original(outcome,{**proposed,'input_bytes':0})

    def test_actual_delegate_spec_extras_and_rejection_before_dispatch(self):
        items=items_for('family_complete');bound=qualification();inventory=bound_inventory(bound)
        for mode,degree in (('sequential',1),('thread',2),('process',2)):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                budget=budget_for(degree,8)
                base=original_reservation(items,mode=mode,budget=budget)
                plain=delegate_reservation(None,items,budget,original=base)
                self.assertEqual(plain['logical_spec_resident_reservation_bytes'],0)
                plan,size=make_plan(items,mode=mode,budget=budget,expected_inventory=inventory,
                    output_dir=directory,operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
                admitted=delegate_reservation(plan,items,budget,original=base)
                self.assertEqual(admitted['plan_pickle_bytes'],size)
                self.assertEqual(len(admitted['per_invocation_spec_extra_pickle_bytes']),len(plan['reservations']))
                self.assertTrue(all(v>0 for v in admitted['per_invocation_spec_extra_pickle_bytes']))
                self.assertIsNone(admitted['native_ipc_encoded_bytes'])
                self.assertGreater(admitted['combined_logical_resident_bytes'],plain['combined_logical_resident_bytes'])
                with self.assertRaises(ValueError):delegate_reservation(plan,items,
                    replace(budget,max_resident_bytes=admitted['combined_logical_resident_bytes']-1),original=base)
                self.assertEqual(list(Path(directory).iterdir()),[])
        for degree,cpus in ((True,4),(2,3),(2,4),(3,8)):
            with self.assertRaises(ValueError):budget_for(degree,cpus)


if __name__=='__main__':unittest.main()
