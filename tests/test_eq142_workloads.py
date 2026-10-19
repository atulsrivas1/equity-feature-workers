"""Frozen layouts, independent quality and hostile Decimal-context controls."""
from copy import deepcopy
from decimal import getcontext,setcontext,Inexact
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY
if HAVE_INTRADAY:
    from eq142_workloads import items_for,independent_expectations,verify_result,PROFILES


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class WorkloadExpectations(unittest.TestCase):
    def test_original_and_focused_layouts_with_independent_readiness(self):
        expected_rows={'small':[256],'balanced':[1024]*16,'skewed':[256]*15+[1024],
            'family_complete':[256]*3,'family_missing_hl':[256]*3,'family_old_close_gap':[255]*3}
        for profile in PROFILES:
            with self.subTest(profile=profile):
                items=items_for(profile)
                self.assertEqual(sorted(i.rows for i in items),sorted(expected_rows[profile]))
                if profile.startswith('family_'):
                    self.assertEqual([len(i.task.features) for i in items],[2,1,5])
                    self.assertTrue(all(i.batches is items[0].batches for i in items))
                    self.assertTrue(all(i.task.instruments==('SYNTHETIC0',) for i in items))
        for profile in ('small','family_complete','family_missing_hl','family_old_close_gap'):
            items=items_for(profile);expected=independent_expectations(profile,items)
            for item in items:
                (result,)=item.calculate(item.task,item.batches)
                verify_result(item.task,result,expected[item.task.task_sha256])
                corrupted=deepcopy(expected[item.task.task_sha256])
                first=next(iter(corrupted['cells']));corrupted['cells'][first]['observed']+=1
                with self.assertRaises(AssertionError):verify_result(item.task,result,corrupted)
        items=items_for('family_old_close_gap');expected=independent_expectations('family_old_close_gap',items)
        atr=expected[items[1].task.task_sha256]['cells']['intraday.atr']
        self.assertEqual((atr['status'],atr['expected'],atr['observed'],atr['value']),('incomplete_coverage',256,255,None))

    def test_oracle_context_does_not_inherit_hostile_process_defaults(self):
        items=items_for('small');ordinary=independent_expectations('small',items)
        saved=getcontext().copy()
        try:
            getcontext().prec=2;getcontext().rounding='ROUND_DOWN';getcontext().traps[Inexact]=True
            hostile=independent_expectations('small',items)
            self.assertEqual(hostile,ordinary)
            self.assertEqual(getcontext().prec,2);self.assertTrue(getcontext().traps[Inexact])
        finally:setcontext(saved)


if __name__=='__main__':unittest.main()
