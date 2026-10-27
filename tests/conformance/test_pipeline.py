"""Fresh actual adapter/producer/provenance/store conformance, no fake completion."""
from pathlib import Path
import tempfile
import unittest
from equity_feature_evidence import canonical_content,encode_receipt,verify_receipt_content,fingerprint_result
from equity_feature_provenance import bind_supplied_provenance,verify_live_generation
from equity_feature_duckdb_sink import DuckDBSink
from equity_feature_workers import GenerationSpec
from equity_feature_conformance import compare_content,summarize
from native_composition import create,acquire,population,diagnose,LIMITS,store,publish,deps


class Pipeline(unittest.TestCase):
    def test_actual_adapter_goldens_unknown_quality_and_full_receipt_parity(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)/'inputs';original=create(root)
            for K in (210,200):
                value=acquire(root,K=K);batch=value.acquired[0].batch
                before=(canonical_content(value.result),encode_receipt(value.receipt),canonical_content(batch))
                anchor=fingerprint_result(value.result);out=diagnose(value,K)
                self.assertTrue(out.matched,out.mismatch_fields)
                self.assertEqual(out.observed.admission,'admitted' if K==210 else 'denied')
                if K==210:
                    self.assertEqual(tuple(col.values[0] for col in value.result.values),(3,10,1011,101.1,10/3))
                    self.assertTrue(all(q.status.value=='available' for q in value.result.quality))
                else:self.assertTrue(all(q.status.value=='missing_input' and q.reasons==('future_knowledge',) for q in value.result.quality))
                supplied=bind_supplied_provenance(value)
                self.assertEqual((supplied.generation_status,supplied.operational_readback),('not_checked',False))
                self.assertTrue(verify_receipt_content(value.receipt,value.expected_task.config,(('trades',batch),),value.result))
                self.assertEqual(before,(canonical_content(value.result),encode_receipt(value.receipt),canonical_content(batch)))
                self.assertTrue(compare_content('original-result-K'+str(K),anchor,fingerprint_result(value.result),'equal').matched)
            self.assertEqual(population(root),original)

    def test_actual_wait_reopen_complete_does_not_admit_late_quality(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);original=create(root/'inputs')
            a,b=(acquire(root/'inputs',instrument=i,K=200) for i in ('A','B'))
            anchors=tuple((canonical_content(v.result),encode_receipt(v.receipt)) for v in (a,b))
            spec=GenerationSpec('demo','job','G',(a.expected_task,b.expected_task));generation=store(root/'generations')
            with DuckDBSink(root/'sink.duckdb','synthetic-output',limits=LIMITS) as sink:
                a=publish(sink,a);values=(a,b);dependencies=deps(values,sink)
                self.assertFalse(generation.publish(spec,dependencies).complete)
                waiting=verify_live_generation(store(root/'generations'),spec,dependencies,values)
                self.assertEqual((waiting.generation_status,waiting.operational_readback),('waiting',False))
                b=publish(sink,b);values=(a,b);dependencies=deps(values,sink)
                self.assertTrue(generation.publish(spec,dependencies).complete)
                live=verify_live_generation(store(root/'generations'),spec,dependencies,values)
                self.assertEqual((live.generation_status,live.operational_readback),('complete',True))
                outcomes=tuple(diagnose(v,200) for v in values)
                self.assertTrue(summarize(outcomes).all_matched)
                self.assertTrue(all(o.observed.admission=='denied' for o in outcomes))
                self.assertFalse(live.source_authenticated)
                self.assertEqual(anchors,tuple((canonical_content(v.result),encode_receipt(v.receipt)) for v in values))
            self.assertEqual(population(root/'inputs'),original)


if __name__=='__main__':unittest.main()
