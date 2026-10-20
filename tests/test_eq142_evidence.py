"""Full-body proof retention and actual original publication witnesses."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from io import BytesIO

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY
from eq142_evidence import ProductStore,bounded_read,persist_record,combine_fragments,original_reason,record_products,stable_products
from eq142_trace import Collector,TraceSpecificationError
from equity_feature_workers import BoundedSupervisor,ProgressRecorder,encode_task,encode_output
from equity_feature_io_sdk import encode_result,encode_receipt
if HAVE_INTRADAY:
    from eq142_workloads import items_for
    from benchmark_supplied_inputs import publisher


class RetainedBodies(unittest.TestCase):
    def test_exclusive_bounded_record_writer_retains_failed_partial(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'native.json'
            reference=persist_record(path,{'samples':[]},max_bytes=64)
            self.assertEqual(reference['bytes'],len(path.read_bytes()))
            self.assertEqual(path.read_bytes(),b'{"samples":[]}\n')
            with self.assertRaises(ValueError):persist_record(path,{},max_bytes=64)
            failed=Path(directory)/'failed.json'
            with self.assertRaises(ValueError):persist_record(failed,{'samples':[1]*100},max_bytes=16)
            self.assertFalse(failed.exists())
            self.assertLessEqual((Path(directory)/'failed.json.partial').stat().st_size,16)
            with self.assertRaises(ValueError):persist_record(failed,{},max_bytes=64)

    def test_bounded_reader_and_incremental_reopen_admission(self):
        # The file may grow after a successful stat. Read only limit+1 bytes.
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'growing.json';path.write_bytes(b'{}')
            stream=BytesIO(b'x'*100)
            with patch.object(Path,'open',return_value=stream):
                with self.assertRaises(ValueError):bounded_read(path,4)
                self.assertTrue(stream.closed)
            # Reopening rejects before reading any member after object capacity.
            store=ProductStore(Path(directory)/'objects')
            store.put('encode_task',b'{}');store.put('encode_task',b'[]')
            import eq142_evidence as evidence
            original=evidence.bounded_read;read_sizes=[]
            def observe(path,maximum):
                read_sizes.append(maximum);return original(path,maximum)
            with patch.object(evidence,'bounded_read',side_effect=observe):
                with self.assertRaises(ValueError):ProductStore(store.root,max_objects=1)
            self.assertEqual(read_sizes,[1048576])
            read_sizes.clear()
            with patch.object(evidence,'bounded_read',side_effect=observe):
                with self.assertRaises(ValueError):ProductStore(store.root,max_bytes=3)
            self.assertEqual(read_sizes,[3,1])

    def test_exact_bodies_reopen_corruption_and_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            store=ProductStore(directory,max_bytes=4,max_objects=1)
            reference=store.put('encode_task',b'{}\n')
            self.assertEqual(store.put('encode_task',b'{}\n'),reference)
            self.assertEqual(ProductStore(directory).verify(reference),b'{}\n')
            for changed in ({**reference,'bytes':True},{**reference,'bytes':2},
                    {**reference,'retained_name':'../elsewhere.json'}, {**reference,'extra':1}):
                with self.assertRaises(ValueError):store.verify(changed)
            with self.assertRaises(ValueError):store.put('encode_task',b'[]\n')
            self.assertEqual(len(list(Path(directory).iterdir())),1)
            (Path(directory)/reference['retained_name']).write_bytes(b'bad')
            with self.assertRaises(ValueError):store.verify(reference)
            with self.assertRaises(ValueError):ProductStore(directory)
        for kwargs in ({'max_bytes':True},{'max_objects':0},{'max_objects':513}):
            with tempfile.TemporaryDirectory() as directory,self.assertRaises(ValueError):ProductStore(directory,**kwargs)

    def test_retained_fragment_joins_and_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            reservations=[]
            for name in ('first','second'):
                collector=Collector(name,max_spans=8,max_bytes=16384,operations=('trace.bookkeeping',))
                frame=collector.begin('trace.bookkeeping',());collector.finish(frame,result=1)
                collector.persist(Path(directory)/(name+'.json'))
                reservations.append({'id':name,'file':name+'.json','max_spans':8,'max_bytes':16384})
            joined=combine_fragments(directory,reservations)
            self.assertEqual(joined['total_span_count'],2)
            self.assertEqual(len(joined['processes']),1);self.assertEqual(len(joined['timelines']),1)
            self.assertEqual(sum(c['attempts'] for c in joined['counters']),2)
            self.assertIsNone(joined['global_numeric_critical_path_ns'])
            self.assertIsNone(joined['native_ipc_encoded_bytes'])
            import eq142_evidence as evidence
            for oversized in ([{**r,'max_spans':131072} for r in reservations],
                    [{**reservations[0],'max_bytes':16777216,'id':'r'+str(n),'file':'r'+str(n)+'.json'} for n in range(5)]):
                with patch.object(evidence,'bounded_read',side_effect=AssertionError('must admit before reading')):
                    with self.assertRaises(TraceSpecificationError):combine_fragments(directory,oversized)
            for invalid in ([],[reservations[0],reservations[0]],
                    [{**reservations[0],'file':'../first.json'}],
                    [{**reservations[0],'id':'other'}],
                    [{**reservations[0],'max_spans':True}]):
                with self.assertRaises(TraceSpecificationError):combine_fragments(directory,invalid)
            changed=json.loads((Path(directory)/'second.json').read_bytes())
            changed['spans'][0]['id']=json.loads((Path(directory)/'first.json').read_bytes())['spans'][0]['id']
            (Path(directory)/'second.json').write_text(json.dumps(changed),encoding='utf-8')
            with self.assertRaises(TraceSpecificationError):combine_fragments(directory,reservations)

    def test_reason_taxonomy_is_closed_not_exception_origin(self):
        self.assertIsNone(original_reason(None))
        self.assertEqual(original_reason('CANCELLED'),{'domain':'supervisor','value':'CANCELLED'})
        self.assertEqual(original_reason('RESOURCE_LIMIT'),{'domain':'sink','value':'RESOURCE_LIMIT'})
        with self.assertRaises(ValueError):original_reason('invented')


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class OriginalProductWitnesses(unittest.TestCase):
    def run_original(self):
        items=items_for('family_missing_hl');sink,pub=publisher();progress=ProgressRecorder(max_tasks=16)
        outcome=BoundedSupervisor().run(items,publisher=pub,progress=progress)
        self.assertTrue(all(t.reason is None and t.output is not None for t in outcome.tasks))
        return outcome,progress.snapshot(),sink

    def record(self,outcome,snapshot,sink,directory):
        root=Path(directory);local=root/'sample';local.mkdir()
        store=ProductStore(root/'objects')
        return record_products(outcome,snapshot,store,local,sink=sink),store,local

    def test_actual_original_full_codec_receipts_and_missing_feature(self):
        outcome,snapshot,sink=self.run_original()
        with tempfile.TemporaryDirectory() as directory:
            records,store,local=self.record(outcome,snapshot,sink,directory)
            self.assertEqual(len(records),3)
            for actual,execution in zip(records,outcome.tasks):
                self.assertEqual(store.verify(actual['task_wire']),encode_task(execution.task))
                self.assertEqual(store.verify(actual['output']),encode_output(execution.output))
                self.assertEqual(store.verify(actual['receipt']),encode_receipt(execution.output.receipt))
                self.assertEqual([store.verify(ref) for ref in actual['results']],
                    [encode_result(r) for r in execution.results])
                self.assertEqual(actual['readback_results'],actual['results'])
                self.assertEqual(actual['committed_lookup'],'committed_matching_receipt')
                self.assertEqual(actual['disposition']['commit_state'],'committed')
            self.assertEqual((local/'diagnostics.json').read_bytes(),snapshot.encode())
            changed=deepcopy(records)
            changed[0]['raw_diagnostics']['sha256']='0'*64
            self.assertEqual(stable_products(changed),stable_products(records))
            changed[0]['results'][0]['sha256']='0'*64
            self.assertNotEqual(stable_products(changed),stable_products(records))

    def test_postcommit_verification_failure_and_unknown_commit_preserve_outcome(self):
        outcome,snapshot,sink=self.run_original()
        class FailedRead:
            def lookup(self,key):return sink.lookup(key)
            def read(self,receipt):raise RuntimeError('post-outcome verifier only')
        class MalformedRead(FailedRead):
            def read(self,receipt):return (object(),)
        class WrongShapeRead(FailedRead):
            def read(self,receipt):return iter(())
        for failing in (FailedRead(),MalformedRead(),WrongShapeRead()):
            with tempfile.TemporaryDirectory() as directory:
                records,_,_=self.record(outcome,snapshot,failing,directory)
                for actual in records:
                    self.assertEqual(actual['committed_lookup'],'original_failure')
                    self.assertEqual(actual['readback_results'],[])
                    self.assertEqual(actual['disposition']['commit_state'],'committed')
                    self.assertIsNone(actual['disposition']['reason'])
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);local=root/'sample';local.mkdir();store=ProductStore(root/'objects')
            original_put=store.put;result_writes=0
            def failing_storage(codec,body):
                nonlocal result_writes
                if codec=='encode_result':
                    result_writes+=1
                    if result_writes==2:raise OSError('owned proof storage unavailable')
                return original_put(codec,body)
            with patch.object(store,'put',side_effect=failing_storage):
                with self.assertRaises(OSError):record_products(outcome,snapshot,store,local,sink=sink)
            self.assertTrue(all(t.reason is None and t.output.receipt is not None for t in outcome.tasks))
        partial=replace(outcome,tasks=(replace(outcome.tasks[0],output=None,reason='RESOURCE_LIMIT'),))
        with tempfile.TemporaryDirectory() as directory:
            records,store,_=self.record(partial,snapshot,sink,directory)
            actual=records[0]
            self.assertEqual(actual['disposition']['commit_state'],'unknown')
            self.assertEqual(actual['disposition']['reason']['value'],'RESOURCE_LIMIT')
            self.assertTrue(actual['results']);self.assertIsNone(actual['receipt'])
            self.assertEqual(store.verify(actual['results'][0]),encode_result(partial.tasks[0].results[0]))


if __name__=='__main__':unittest.main()
