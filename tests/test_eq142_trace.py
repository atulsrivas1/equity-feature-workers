"""Independent known call/byte/exception/alias/interval/bounds controls."""
from copy import deepcopy
import inspect
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_graph import resolve
from eq142_trace import Collector,HookSession,TraceSpecificationError,validate_fragment
from eq142_fixture import qualification,load,canonical_cases


def collector(name='parent',max_spans=16384,max_bytes=8388608):
    return Collector(name,max_spans=max_spans,max_bytes=max_bytes,
        operations=load('RECORD_CONTRACTS.json')['trace']['operations'])


class LocalTracing(unittest.TestCase):
    def test_known_leaf_calls_bytes_exceptions_and_no_digest_length_claim(self):
        c=collector();bound=qualification();session=HookSession(bound,load('HOOK_INVENTORY.json'),c)
        marker=ValueError('private exception text must not escape')
        def leaf(value):
            if value is marker:raise marker
            return value
        traced=session.wrapper(leaf,'manifest.task_digest')
        self.assertEqual(traced('a'*64),'a'*64)
        with self.assertRaises(ValueError) as error:traced(marker)
        self.assertIs(error.exception,marker)
        encode=session.wrapper(lambda value:value,'sdk.encode_result')
        self.assertIs(encode(b'abc'),b'abc')
        fragment=c.snapshot();validate_fragment(fragment)
        counters={x['operation']:x for x in fragment['counters']}
        self.assertEqual((counters['manifest.task_digest']['attempts'],counters['manifest.task_digest']['returned'],counters['manifest.task_digest']['rejected']),(2,1,1))
        self.assertIsNone(counters['manifest.task_digest']['returned_observed_bytes'])
        self.assertEqual(counters['sdk.encode_result']['returned_observed_bytes'],3)
        self.assertNotIn('private exception text',str(fragment))

    def test_actual_installed_aliases_properties_full_product_and_restoration(self):
        from equity_feature_io_sdk import encode_result
        from equity_feature_workers.intraday import compute_intraday_inputs
        case,task,batches=next(canonical_cases());bound=qualification()
        expected=compute_intraday_inputs(task,batches);expected_bytes=encode_result(expected[0])
        hooks=load('HOOK_INVENTORY.json');originals=[]
        for record in hooks['records']:
            for site in record['patch_sites']:
                owner,name=resolve(site);originals.append((owner,name,inspect.getattr_static(owner,name)))
        c=collector();c.bind_labels((task,));session=HookSession(bound,hooks,c)
        with c.phase('stage:calculation'),session:
            actual=compute_intraday_inputs(task,batches)
            # Imported local aliases are deliberately outside the closed hook
            # inventory; call the inspected module export for this byte count.
            import equity_feature_io_sdk as sdk
            actual_bytes=sdk.encode_result(actual[0])
        self.assertEqual(actual,expected);self.assertEqual(actual_bytes,expected_bytes)
        for owner,name,original in originals:self.assertIs(inspect.getattr_static(owner,name),original)
        bound.check();fragment=c.snapshot();validate_fragment(fragment)
        counts={op:sum(x['attempts'] for x in fragment['counters'] if x['operation']==op) for op in ['supplied.admit','intraday.compute','intraday.indicator','sdk.encode_result']}
        self.assertEqual(counts,{'supplied.admit':1,'intraday.compute':1,'intraday.indicator':1,'sdk.encode_result':1})
        self.assertTrue(all(s['task_sha256']==task.task_sha256 for s in fragment['spans'] if s['operation'].startswith('intraday.')))
        self.assertEqual(sum(x['returned_observed_bytes'] or 0 for x in fragment['counters'] if x['operation']=='sdk.encode_result'),len(expected_bytes))

    def test_observer_failure_does_not_mask_original_outcome(self):
        c=collector();session=HookSession(qualification(),load('HOOK_INVENTORY.json'),c)
        marker=RuntimeError('original marker')
        def original(fail=False):
            if fail:raise marker
            return 123
        wrapped=session.wrapper(original,'trace.bookkeeping')
        with patch.object(c,'begin',side_effect=MemoryError):
            self.assertEqual(wrapped(),123)
            with self.assertRaises(RuntimeError) as error:wrapped(True)
            self.assertIs(error.exception,marker)
        self.assertFalse(c.snapshot()['complete'])
        self.assertEqual(c.snapshot()['failure_reason'],'collector_error')

    def test_independent_interval_union_validation_rejects_corruption(self):
        c=collector();session=HookSession(qualification(),load('HOOK_INVENTORY.json'),c)
        inner=session.wrapper(lambda:7,'trace.bookkeeping')
        outer=session.wrapper(lambda:inner()+inner(),'delegate.original_partition')
        self.assertEqual(outer(),14)
        fragment=c.snapshot();validate_fragment(fragment)
        outer_span=next(s for s in fragment['spans'] if s['operation']=='delegate.original_partition')
        self.assertEqual(sum(s['parent_id']==outer_span['id'] for s in fragment['spans']),2)
        broken=deepcopy(fragment);next(s for s in broken['spans'] if s['operation']=='delegate.original_partition')['exclusive_ns']=outer_span['inclusive_ns']
        with self.assertRaises(TraceSpecificationError):validate_fragment(broken)

    def test_bounds_partial_evidence_and_duplicate_files(self):
        c=collector(max_spans=2);session=HookSession(qualification(),load('HOOK_INVENTORY.json'),c)
        called=[];wrapped=session.wrapper(lambda:called.append(1),'trace.bookkeeping')
        for _ in range(3):wrapped()
        self.assertEqual(len(called),3);self.assertFalse(c.snapshot()['complete'])
        self.assertEqual(c.snapshot()['failure_reason'],'event_limit')
        other=collector('persist');session=HookSession(qualification(),load('HOOK_INVENTORY.json'),other)
        session.wrapper(lambda:5,'trace.bookkeeping')()
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'trace.json';size=other.persist(path)
            self.assertEqual(size,path.stat().st_size);self.assertLessEqual(size,other.max_bytes)
            with self.assertRaises(TraceSpecificationError):other.persist(path)

    def test_actual_thread_timeline_stable_across_collector_invocations(self):
        first=collector('first');second=collector('second')
        for c in (first,second):
            frame=c.begin('trace.bookkeeping',());c.finish(frame,result=1)
        self.assertEqual(first.snapshot()['spans'][0]['timeline_id'],second.snapshot()['spans'][0]['timeline_id'])
        thread_collector=collector('thread')
        def work():
            frame=thread_collector.begin('trace.bookkeeping',());thread_collector.finish(frame,result=1)
        thread=threading.Thread(target=work);thread.start();thread.join()
        self.assertNotEqual(first.snapshot()['spans'][0]['timeline_id'],thread_collector.snapshot()['spans'][0]['timeline_id'])

    def test_closed_empty_fragment_and_rejected_error_type(self):
        empty=collector('empty').snapshot();validate_fragment(empty)
        for field,value in [('reservation_id',{}),('reservation_id',''),('counters',{})]:
            bad=deepcopy(empty);bad[field]=value
            with self.assertRaises(TraceSpecificationError):validate_fragment(bad)
        c=collector();session=HookSession(qualification(),load('HOOK_INVENTORY.json'),c)
        def reject():raise ValueError('synthetic')
        with self.assertRaises(ValueError):session.wrapper(reject,'trace.bookkeeping')()
        valid=c.snapshot();validate_fragment(valid)
        for value in ({'bad':True},'x'*1000):
            bad=deepcopy(valid);bad['spans'][0]['error_type']=value
            with self.assertRaises(TraceSpecificationError):validate_fragment(bad)
        self.assertEqual(valid['exclusive_cost_disposition'],'exclusive_of_observed_spans_includes_unseparated_hook_bookkeeping')


if __name__=='__main__':unittest.main()
