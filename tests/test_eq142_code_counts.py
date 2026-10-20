"""Separate Python event counts; fixed fixtures do not inspect tracer totals."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_code_counts import CodeCounts,for_original_run,compare_aggregate,validate_profile,guarded_activation
from eq142_fixture import HAVE_INTRADAY,qualification,bound_inventory,load,require_cpu_slots
if HAVE_INTRADAY:
    from supplied_intraday_fixture import facts,work


def byte_leaf(value):return value
def direct_root(values):return tuple(byte_leaf(v) for v in values)
def thread_root(values):
    with ThreadPoolExecutor(max_workers=2) as pool:return tuple(pool.map(byte_leaf,values))
def failing_leaf(error):raise error
def failing_root(error):return failing_leaf(error)


class IndependentCodeEvents(unittest.TestCase):
    def test_partial_install_cleanup_preserves_product_and_foreign_profiles(self):
        def other(frame,event,arg):pass
        for failing_module in (threading,sys):
            for disposition in ('before_install','after_install','foreign_profile'):
                with self.subTest(module=failing_module.__name__,disposition=disposition):
                    observer=CodeCounts({byte_leaf.__code__:'sdk.encode_result'},root_codes=(direct_root.__code__,))
                    original_set=failing_module.setprofile
                    error=RuntimeError('profile installation rejected')
                    errors=[]
                    def reject(callback):
                        if callback is observer.callback:
                            if disposition=='after_install':original_set(callback)
                            elif disposition=='foreign_profile':original_set(other)
                            raise error
                        return original_set(callback)
                    try:
                        with patch.object(failing_module,'setprofile',reject):
                            with guarded_activation(observer,lambda:errors.append('observer_error')):
                                self.assertEqual(direct_root((b'a',b'b')),(b'a',b'b'))
                        self.assertEqual(errors,['observer_error'])
                        self.assertFalse(observer.running);self.assertFalse(observer.snapshot()['complete'])
                        self.assertEqual(observer.failure_reason,'observer_error')
                        for module in (sys,threading):
                            expected=other if module is failing_module and disposition=='foreign_profile' else None
                            self.assertIs(module.getprofile(),expected)
                    finally:
                        sys.setprofile(None);threading.setprofile(None)

                    # Direct users must also get an incomplete snapshot, even
                    # when a setter mutates its hook and then raises.
                    observer=CodeCounts({byte_leaf.__code__:'sdk.encode_result'},root_codes=(direct_root.__code__,))
                    try:
                        with patch.object(failing_module,'setprofile',reject):
                            with self.assertRaises(RuntimeError) as caught:
                                with observer.activate():self.fail('activation must not enter')
                        self.assertIs(caught.exception,error)
                        self.assertFalse(observer.running);self.assertFalse(observer.snapshot()['complete'])
                        self.assertEqual(observer.failure_reason,'observer_error')
                        for module in (sys,threading):
                            expected=other if module is failing_module and disposition=='foreign_profile' else None
                            self.assertIs(module.getprofile(),expected)
                    finally:
                        sys.setprofile(None);threading.setprofile(None)

    def test_fixed_direct_and_actual_future_thread_counts_and_bytes(self):
        for root in (direct_root,thread_root):
            with self.subTest(root=root.__name__):
                observer=CodeCounts({byte_leaf.__code__:'sdk.encode_result'},
                    root_codes=(root.__code__,byte_leaf.__code__))
                with observer.activate():actual=root((b'a',b'bbb'))
                self.assertEqual(actual,(b'a',b'bbb'))
                record=observer.snapshot();self.assertTrue(record['complete'])
                validate_profile(record)
                self.assertEqual(record['counters'],[{'operation':'sdk.encode_result','call_entries':2,
                    'return_events':2,'typed_byte_returns':2,'typed_returned_bytes':4}])
                comparison=compare_aggregate([record],[{'operation':'sdk.encode_result','attempts':2,
                    'returned':2,'byte_observation':'returned_encoded_bytes','returned_observed_bytes':4}])
                self.assertEqual(comparison['typed_returned_bytes'],4)
                with self.assertRaises(ValueError):compare_aggregate([record],[])
                self.assertIsNone(sys.getprofile());self.assertIsNone(threading.getprofile())
                for changed in ({**record,'complete':1},{**record,'extra':True},
                        {**record,'counters':[record['counters'][0]]*2}):
                    with self.assertRaises(ValueError):validate_profile(changed)
                changed=deepcopy(record);changed['counters'][0]['call_entries']=True
                with self.assertRaises(ValueError):validate_profile(changed)

    def test_original_exception_identity_limits_and_existing_profile(self):
        error=RuntimeError('original fixture object')
        observer=CodeCounts({failing_leaf.__code__:'fixture.throw'},root_codes=(failing_root.__code__,))
        with self.assertRaises(RuntimeError) as actual:
            with observer.activate():failing_root(error)
        self.assertIs(actual.exception,error)
        record=observer.snapshot();self.assertTrue(record['complete'])
        self.assertEqual(record['completion_classification'],'return_events_do_not_classify_exception')
        self.assertEqual(record['counters'][0]['return_events'],1)
        self.assertNotIn('rejected',record['counters'][0])
        bounded=CodeCounts({byte_leaf.__code__:'sdk.encode_result'},root_codes=(direct_root.__code__,),max_entries=1)
        with bounded.activate():self.assertEqual(direct_root((b'a',b'b',b'c')),(b'a',b'b',b'c'))
        self.assertFalse(bounded.snapshot()['complete']);self.assertEqual(bounded.snapshot()['failure_reason'],'entry_limit')
        def other(frame,event,arg):pass
        sys.setprofile(other)
        try:
            another=CodeCounts({byte_leaf.__code__:'sdk.encode_result'},root_codes=(direct_root.__code__,))
            with self.assertRaises(ValueError):
                with another.activate():pass
            self.assertIs(sys.getprofile(),other)
        finally:sys.setprofile(None)


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ActualOriginalCountParity(unittest.TestCase):
    def test_actual_harness_thread_and_spawn_profile_counts(self):
        import json
        from benchmark_execution_baseline import prepare_reference,run_control,SERIES
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);reference=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,5),profiles=('family_missing_hl',))
            for mode in ('thread','process'):
                with self.subTest(mode=mode):
                    local=root/mode
                    result=run_control(root/'reference/reference.json',reference['sha256'],directory=local,
                        profile='family_missing_hl',mode=mode,degree=2,control='instrumented_B',series=SERIES[1],qualify_code_counts=True)
                    record=json.loads((local/result['retained_name']).read_bytes())
                    self.assertEqual(record['status'],'development_product_and_local_trace_checks_passed')
                    self.assertFalse(record['full_sample_accepted']);self.assertIsNone(record['selected_phase_wall_ns'])
                    self.assertEqual(record['selected_phase_scope'],'qualification_profiled_no_timing_authority')
                    self.assertFalse(record['qualification_code_counts']['timing_authority'])
                    self.assertGreater(record['qualification_code_counts']['comparison']['call_entries'],0)
                    self.assertEqual(len(record['qualification_code_counts']['profiles']),1 if mode=='thread' else 2)

    def test_actual_original_sequential_code_events_match_aggregate_hooks(self):
        from eq142_delegate import RoutingHookSession,make_plan,patched_delegate
        from eq142_trace import Collector
        from eq142_evidence import combine_fragments
        from equity_feature_workers import BoundedSupervisor,ResourceBudget,ProgressRecorder
        from benchmark_supplied_inputs import publisher
        from equity_feature_io_sdk import encode_result
        context,config,batch=facts('COUNTS');items=(work(context,config,(batch,),name='code-count-fixture'),)
        expected=tuple(encode_result(r) for r in items[0].calculate(items[0].task,items[0].batches))
        bound=qualification();inventory=bound_inventory(bound);observer=for_original_run(bound,inventory['hooks'])
        with tempfile.TemporaryDirectory() as directory:
            budget=ResourceBudget(max_resident_bytes=134217728)
            plan,_=make_plan(items,mode='sequential',budget=budget,expected_inventory=inventory,output_dir=directory,
                operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
            collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=plan['operations'])
            collector.bind_labels(tuple(i.task for i in items));sink,pub=publisher()
            with (collector.phase('stage:supervisor'),RoutingHookSession(bound,inventory['hooks'],collector),
                    patched_delegate(plan,collector),observer.activate()):
                result=BoundedSupervisor(budget=budget).run(items,publisher=pub,progress=ProgressRecorder(max_tasks=16))
            self.assertEqual(tuple(encode_result(r) for r in result.tasks[0].results),expected)
            collector.persist(Path(directory)/'parent.json')
            joined=combine_fragments(directory,[{'id':'parent','file':'parent.json','max_spans':16384,'max_bytes':8388608},*plan['reservations']])
            comparison=compare_aggregate([observer.snapshot()],joined['counters'],
                manual_operations=('trace.bind_labels','trace.install','trace.restore'))
            self.assertGreater(comparison['operations'],20)
            self.assertGreater(comparison['typed_returned_bytes'],0)


if __name__=='__main__':unittest.main()
