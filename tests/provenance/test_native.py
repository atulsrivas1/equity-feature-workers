"""Actual DuckDB acquisition, publication and reopened-generation evidence."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from equity_feature_contracts import EntityKey
from equity_feature_contracts.adapters import SourceError
from equity_feature_diagnostics import (DecisionContext, DiagnosticPolicy, KnowledgeCheck,evaluate_diagnostics)
from equity_feature_evidence import canonical_content,encode_receipt,fingerprint_batch
from equity_feature_duckdb_sink import DuckDBSink
from equity_feature_io_contracts import SinkRequirements
from equity_feature_workers import (BarrierLimits,GenerationSpec,GenerationStore,Dependency,SerialPublisher,PublicationLimits)
from equity_feature_provenance import bind_supplied_provenance,verify_live_generation,ProvenanceError
from native_fixture import create,acquire,population

LIMITS=SinkRequirements(max_results=1,max_chunk_bytes=1048576,max_total_bytes=1048576,max_result_cells=8192,max_evidence_rows=512)


def store(root):return GenerationStore(root,limits=BarrierLimits(),requirements=LIMITS)


def publish(sink,value):
    publisher=SerialPublisher(sink,'synthetic-output',limits=PublicationLimits(),requirements=LIMITS)
    publisher.submit(value.expected_task,(value.result,))
    outcome=publisher.drain()[0]
    assert outcome.committed and outcome.output is not None
    return replace(value,output=outcome.output)


def deps(values,sink):
    return tuple(Dependency(v.expected_task.task_sha256,v.expected_task,v.output,sink if v.output is not None else None) for v in values)


class NativeTests(unittest.TestCase):
    def test_actual_acquisition_quality_diagnostics_and_original_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)/'inputs';before=create(root)
            for K in (210,200):
                value=acquire(root,K=K);saved=(canonical_content(value.result),encode_receipt(value.receipt))
                supplied=bind_supplied_provenance(value)
                self.assertFalse(supplied.operational_readback)
                if K==210:
                    self.assertEqual(tuple(c.values[0] for c in value.result.values),(3,10,1011,101.1,10/3))
                    self.assertTrue(all(q.status.value=='available' for q in value.result.quality))
                else:
                    self.assertTrue(all(q.status.value=='missing_input' and q.reasons==('future_knowledge',) for q in value.result.quality))
                batch=value.acquired[0].batch
                report=evaluate_diagnostics(DecisionContext('demo',EntityKey('A','S'),value.expected_task.config.availability),
                    DiagnosticPolicy('selected-rows','strict','deny'),tuple(KnowledgeCheck('row'+str(n),'trades:'+str(n),t,
                    content=fingerprint_batch(batch)) for n,t in enumerate(batch.column('known_at_ns').values)))
                self.assertEqual(tuple(f.status for f in report.findings),('passed','passed','passed') if K==210 else ('passed','passed','failed'))
                self.assertEqual(report.admission,'admitted' if K==210 else 'denied')
                self.assertEqual(saved,(canonical_content(value.result),encode_receipt(value.receipt)))
            self.assertEqual(population(root),before)

    def test_live_wait_reopen_complete_repeat_and_supplied_not_live(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);before=create(root/'inputs')
            a,b=(acquire(root/'inputs',instrument=i,K=200) for i in ('A','B'))
            original=tuple((canonical_content(v.result),encode_receipt(v.receipt)) for v in (a,b))
            spec=GenerationSpec('demo','job','G',(a.expected_task,b.expected_task))
            with DuckDBSink(root/'sink.duckdb','synthetic-output',limits=LIMITS) as sink:
                a=publish(sink,a);values=(a,b);dependencies=deps(values,sink);generation=store(root/'generations')
                self.assertFalse(generation.publish(spec,dependencies).complete)
                for opened in (generation,store(root/'generations')):
                    report=verify_live_generation(opened,spec,dependencies,values)
                    self.assertEqual((report.verification_mode,report.generation_status,report.operational_readback),('live','waiting',False))
                b=publish(sink,b);values=(a,b);dependencies=deps(values,sink)
                self.assertTrue(generation.publish(spec,dependencies).complete)
                for opened in (generation,store(root/'generations')):
                    report=verify_live_generation(opened,spec,dependencies,values)
                    self.assertEqual((report.generation_status,report.operational_readback),('complete',True))
                    self.assertTrue(all(r.generation_status=='not_checked' and not r.operational_readback for r in report.task_reports))
                    self.assertFalse(report.source_authenticated)
                self.assertFalse(bind_supplied_provenance(a).operational_readback)
                self.assertEqual(original,tuple((canonical_content(v.result),encode_receipt(v.receipt)) for v in values))
                with self.assertRaises(ProvenanceError) as caught:
                    verify_live_generation(generation,spec,dependencies[::-1],values)
                self.assertEqual(caught.exception.code,'inconsistent_binding')
                class Cancelled:
                    def is_cancelled(self):return True
                with self.assertRaises(ProvenanceError) as caught:
                    verify_live_generation(generation,spec,dependencies,values,cancellation=Cancelled())
                self.assertEqual(caught.exception.code,'cancelled')
                complete=root/'generations'/'.efworker-generations1'/(spec.key+'.complete.json')
                complete.write_bytes(b'{corrupt')
                with self.assertRaises(ProvenanceError) as caught:
                    verify_live_generation(generation,spec,dependencies,values)
                self.assertEqual(caught.exception.code,'readback_failed')
            self.assertEqual(population(root/'inputs'),before)

    def test_changed_physical_file_refused_by_original_pin_before_binding(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)/'inputs';before=create(root)
            value=acquire(root);self.assertFalse(bind_supplied_provenance(value).operational_readback)
            path=root/'original.parquet';raw=path.read_bytes();path.write_bytes(raw[:-1]+bytes((raw[-1]^1,)))
            with self.assertRaises(SourceError):acquire(root,pin=before['original.parquet'])
            # Supplied-only has no I/O and cannot observe this external change.
            self.assertFalse(bind_supplied_provenance(value).operational_readback)

    def test_actual_native_output_corruption_never_waiting_or_complete(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);create(root/'inputs');value=acquire(root/'inputs')
            spec=GenerationSpec('demo','job','G',(value.expected_task,))
            generation=store(root/'generations');path=root/'sink.duckdb'
            with DuckDBSink(path,'synthetic-output',limits=LIMITS) as sink:
                value=publish(sink,value);dependencies=deps((value,),sink)
                self.assertTrue(generation.publish(spec,dependencies).complete)
                self.assertTrue(verify_live_generation(generation,spec,dependencies,(value,)).operational_readback)
            # Owned fault injection after the sink closes. This uses the actual
            # pinned storage layout; no fake adapter/store completion is supplied.
            import duckdb
            with duckdb.connect(str(path)) as connection:
                connection.execute('UPDATE efio_results SET record=?',[b'corrupt-original-result'])
            with DuckDBSink(path,'synthetic-output',limits=LIMITS) as sink:
                with self.assertRaises(ProvenanceError) as caught:
                    verify_live_generation(generation,spec,deps((value,),sink),(value,))
                self.assertEqual(caught.exception.code,'readback_failed')


if __name__=='__main__':unittest.main()
