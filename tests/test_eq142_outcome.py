"""Actual admission/row ledgers and failure retention; no study acceptance."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY,require_cpu_slots
from eq142_outcome import snapshot_original,validate_summary,verify_original
from eq142_evidence import ProductStore,record_products,persist_record
from eq142_admission import original_reservation
from equity_feature_workers import BoundedSupervisor,ResourceBudget,ProgressRecorder
if HAVE_INTRADAY:
    from supplied_intraday_fixture import facts,work


class SummaryContract(unittest.TestCase):
    def test_closed_summary_none_zero_empty_and_corruption(self):
        record={'schema':'eq142-supervisor-outcome:1','evidence':'actual_original_supervisor_return',
            'mode':'sequential','cancelled':False,'row_ledger':[['a'*64,6]],'distinct_reuse_rows':6,
            'admitted_input_bytes':100,'reserved_resident_bytes':1000,'result_bytes':0,
            'tasks':[{'task_sha256':'a'*64,'reason':None,'result_count':0,
                'output_present':False,'spill':None,'timings':[{'stage':'queue_wait','elapsed_ns':'0','probes':0},
                    {'stage':'calculation','elapsed_ns':None,'probes':0}]}]}
        validate_summary(record)
        none=deepcopy(record);none['tasks'][0]['result_count']=None;validate_summary(none)
        self.assertNotEqual(none,record)
        changes=[{**record,'cancelled':0},{**record,'extra':0},{**record,'distinct_reuse_rows':True},
            {**record,'row_ledger':[['a'*64,-1]]},{**record,'row_ledger':[['c'*64,6]]}]
        for field,value in (('result_count',65),('reason','INVENTED'),('spill',{'run_id':'outside-study'})):
            changed=deepcopy(record);changed['tasks'][0][field]=value;changes.append(changed)
        for field,value in (('elapsed_ns','00'),('elapsed_ns','9223372036854775808'),('probes',True),('stage','invented')):
            changed=deepcopy(record);changed['tasks'][0]['timings'][0][field]=value;changes.append(changed)
        for changed in changes:
            with self.subTest(changed=changed),self.assertRaises(ValueError):validate_summary(changed)


class AlreadyCancelled:
    def is_cancelled(self):return True


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ActualOutcomeLedger(unittest.TestCase):
    def test_actual_three_modes_duplicate_reuse_and_original_cancellation(self):
        # Component controls use the original two-slot budget, not a frozen
        # study host or the full runner's additional host reserve.
        require_cpu_slots(self,2)
        context,config,batch=facts('LEDGER');first=work(context,config,(batch,),name='ledger-first')
        second=replace(first,task=replace(first.task,partition_id='ledger-second'))
        items=(first,second)
        for mode in ('sequential','thread','process'):
            for cancelled in (False,True):
                with self.subTest(mode=mode,cancelled=cancelled),tempfile.TemporaryDirectory() as directory:
                    budget=ResourceBudget(max_resident_bytes=134217728);progress=ProgressRecorder(max_tasks=16)
                    proposed=original_reservation(items,mode=mode,budget=budget)
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items,progress=progress,
                        cancellation=AlreadyCancelled() if cancelled else None)
                    actual=snapshot_original(outcome)
                    self.assertEqual(actual['row_ledger'],[[key,6] for key in sorted(i.task.task_sha256 for i in items)])
                    self.assertEqual(actual['distinct_reuse_rows'],6)
                    self.assertEqual(actual['cancelled'],cancelled)
                    self.assertEqual([t['result_count'] for t in actual['tasks']],[None,None] if cancelled else [1,1])
                    self.assertEqual([t['reason'] for t in actual['tasks']],['CANCELLED']*2 if cancelled else [None]*2)
                    store=ProductStore(Path(directory)/'objects')
                    products=record_products(outcome,progress.snapshot(),store,directory)
                    checked=verify_original(actual,items,products,proposed,store,expected_mode=mode)
                    self.assertEqual(checked['accepted_output_metadata_bytes'],0);self.assertFalse(checked['full_sample_accepted'])
                    for field in ('admitted_input_bytes','reserved_resident_bytes','distinct_reuse_rows','result_bytes'):
                        changed=deepcopy(actual);changed[field]+=1
                        with self.assertRaises(ValueError):verify_original(changed,items,products,proposed,store,expected_mode=mode)
                    path=Path(directory)/'supervisor.json';persist_record(path,actual,max_bytes=65536)
                    self.assertEqual(json.loads(path.read_bytes()),actual)


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ActualOutcomeFailureRetention(unittest.TestCase):
    def test_runner_preserves_actual_metadata_before_proof_or_verification_failure(self):
        from benchmark_execution_baseline import prepare_reference,run_control,SERIES
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);reference=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,4),
                profiles=('family_missing_hl',))
            for operation in ('record_products','verify_original'):
                with self.subTest(operation=operation):
                    local=root/operation;error=OSError('independent evidence failure');observed=[]
                    def rejected(*args,**kwargs):
                        observed.append((local/'supervisor.json').read_bytes());raise error
                    with patch('benchmark_execution_baseline.'+operation,side_effect=rejected):
                        with self.assertRaises(OSError) as caught:
                            run_control(root/'reference/reference.json',reference['sha256'],directory=local,
                                profile='family_missing_hl',mode='sequential',degree=1,
                                control='uninstrumented_A',series=SERIES[0])
                    self.assertIs(caught.exception,error);self.assertEqual(len(observed),1)
                    raw=(local/'supervisor.json').read_bytes();self.assertEqual(raw,observed[0])
                    actual=json.loads(raw);validate_summary(actual)
                    self.assertEqual([r[1] for r in actual['row_ledger']],[256]*3)
                    self.assertEqual([t['result_count'] for t in actual['tasks']],[1]*3)
                    report=json.loads((local/'runner.json').read_bytes())
                    self.assertTrue(report['original_outcome_returned']);self.assertFalse(report['full_sample_accepted'])
                    self.assertEqual(report['status'],'failed_development_control')
                    self.assertEqual(report['actual_supervisor_outcome']['sha256'],hashlib.sha256(raw).hexdigest())
                    self.assertEqual((local/'products.json').exists(),operation=='verify_original')


if __name__=='__main__':unittest.main()
