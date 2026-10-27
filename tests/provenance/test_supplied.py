"""Independent supplied consistency, mutation/refusal and preflight controls."""
from dataclasses import replace, FrozenInstanceError
import unittest
from unittest.mock import patch, Mock
from equity_feature_contracts import Column, DataKind
from equity_feature_duckdb import AcquisitionReceipt, MappingReport, PriceConversion, FileEvidence, ResolvedPartition
from equity_feature_evidence import canonical_content, encode_receipt
from equity_feature_workers import GenerationOutcome, GenerationSpec
from equity_feature_provenance import (TaskEvidence, AcquiredInput, ProvenanceError,
    bind_supplied_provenance, verify_live_generation)
from provenance_fixture import fixture


class SuppliedTests(unittest.TestCase):
    def reject(self, code, call):
        with self.assertRaises(ProvenanceError) as caught: call()
        self.assertEqual(caught.exception.code,code)
        self.assertEqual(str(caught.exception),'Provenance rejected: '+code)
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)

    def test_supplied_consistency_is_not_live_completion(self):
        value=fixture();before=(canonical_content(value.result),encode_receipt(value.receipt))
        report=bind_supplied_provenance(value)
        self.assertEqual((report.verification_mode,report.generation_status,report.operational_readback),('supplied','not_checked',False))
        self.assertEqual(report.input_links[0].content,value.receipt.inputs[0].content)
        self.assertEqual(report.task_sha256,value.expected_task.task_sha256)
        self.assertFalse(report.source_authenticated);self.assertFalse(report.historical_acquisition_verified)
        self.assertEqual((canonical_content(value.result),encode_receipt(value.receipt)),before)
        with self.assertRaises(FrozenInstanceError):report.operational_readback=True

    def test_explicit_missing_empty_partial_are_distinct(self):
        for state in ('missing','empty','partial'):
            with self.subTest(state=state):
                value=fixture(state=state);report=bind_supplied_provenance(value)
                self.assertEqual(report.input_links[0].state,state)
                self.assertEqual(report.input_links[0].content is None,state=='missing')
                self.assertEqual(bool(report.input_links[0].missing_sessions),state in ('missing','partial'))
                self.assertFalse(report.operational_readback)

    def test_unchanged_metadata_changed_cells_refuses_original_anchor(self):
        value=fixture();batch=value.acquired[0].batch
        changed=replace(batch,columns=tuple(replace(c,values=(2,3,6)) if c.name=='size' else c for c in batch.columns))
        self.assertEqual(changed.metadata,batch.metadata)
        candidate=replace(value,acquired=(replace(value.acquired[0],batch=changed),))
        self.reject('inconsistent_binding',lambda:bind_supplied_provenance(candidate))

    def test_metadata_context_source_revision_task_mismatch(self):
        value=fixture()
        other=fixture(instrument='B')
        self.reject('inconsistent_binding',lambda:bind_supplied_provenance(replace(value,result=other.result)))
        task=replace(value.expected_task,inputs=(replace(value.expected_task.inputs[0],acquisition_sha256='0'*64),))
        self.reject('inconsistent_binding',lambda:bind_supplied_provenance(replace(value,expected_task=task)))
        task=replace(value.expected_task,config=replace(value.expected_task.config,identity='another'))
        self.reject('inconsistent_binding',lambda:bind_supplied_provenance(replace(value,expected_task=task)))
        self.reject('inconsistent_binding',lambda:bind_supplied_provenance(replace(value,receipt=other.receipt)))

    def test_original_revision_snapshot_coverage_headers_and_content_anchor(self):
        value=fixture();acquired=value.acquired[0];task=value.expected_task
        for receipt in (replace(acquired.receipt,source=replace(acquired.receipt.source,snapshot_id='wrong')),
                        replace(acquired.receipt,coverage=replace(acquired.receipt.coverage,complete=False)),
                        replace(acquired.receipt,availability=replace(acquired.receipt.availability,evaluation_ns=211))):
            self.reject('inconsistent_binding',lambda:bind_supplied_provenance(replace(value,
                acquired=(replace(acquired,receipt=receipt),))))
        revised=replace(task,inputs=(replace(task.inputs[0],revision_id='other-revision'),))
        self.reject('inconsistent_binding',lambda:bind_supplied_provenance(replace(value,output=replace(value.output,task=revised))))
        fingerprint=replace(value.receipt.inputs[0].content,digest='0'*64)
        retained=replace(value.receipt,inputs=(replace(value.receipt.inputs[0],content=fingerprint),))
        self.reject('inconsistent_binding',lambda:bind_supplied_provenance(replace(value,receipt=retained)))
        header=replace(task.features[0],unit='wrong')
        self.reject('unsupported_version',lambda:bind_supplied_provenance(replace(value,expected_task=replace(task,features=(header,)+task.features[1:]))))

    def test_missing_provenance_and_forged_outcome_not_store(self):
        value=fixture()
        self.reject('missing_provenance',lambda:replace(value,acquired=()))
        self.reject('missing_provenance',lambda:AcquiredInput('trades',None,value.acquired[0].batch))
        spec=GenerationSpec('demo','job','G',(value.expected_task,))
        self.reject('invalid_schema',lambda:verify_live_generation(GenerationOutcome(True,None,()),spec,(),(value,)))

    def test_duplicate_acquisition_roles_are_invalid_not_missing(self):
        value=fixture();acquired=value.acquired[0]
        self.reject('invalid_schema',lambda:replace(value,acquired=(acquired,acquired)))
        self.reject('missing_provenance',lambda:replace(value,acquired=()))

    def test_borrowed_list_detachment(self):
        value=fixture();original=value.acquired[0].receipt
        conversions=[PriceConversion('price','integer','none',0)]
        unmapped=['caller_extra']
        borrowed=replace(original,normalization=replace(original.normalization,price_conversions=conversions,unmapped_fields=unmapped))
        acquired=AcquiredInput('trades',borrowed,value.acquired[0].batch)
        conversions.clear();unmapped.clear()
        self.assertEqual(len(acquired.receipt.normalization.price_conversions),1)
        self.assertEqual(acquired.receipt.normalization.unmapped_fields,('caller_extra',))
        values=[acquired];snapshot=replace(value,acquired=values);values.clear()
        self.assertEqual(len(snapshot.acquired),1)

    def test_files_and_path_limits_before_identity(self):
        value=fixture();original=value.acquired[0].receipt
        def file(n,path='/synthetic'):
            p=ResolvedPartition('2026-01-01',path+'/'+str(n),path+'/optimized'+str(n),1,1,3,'a'*64,'synthetic','synthetic',None,None,None,None)
            return FileEvidence(p,'b'*64,None)
        allowed=replace(original,files=tuple(file(n) for n in range(32)))
        acquired=AcquiredInput('trades',allowed,value.acquired[0].batch)
        self.assertEqual(len(acquired.receipt.files),32)
        excessive=replace(original,files=tuple(file(n) for n in range(33)))
        identity=Mock(side_effect=AssertionError('Identity must not execute'))
        with patch.object(AcquisitionReceipt,'identity_digest',property(identity)):
            self.reject('limit_exceeded',lambda:AcquiredInput('trades',excessive,value.acquired[0].batch))
        identity.assert_not_called()
        too_long=replace(original,files=(file(0,'/'+('x'*4096)),))
        self.reject('limit_exceeded',lambda:AcquiredInput('trades',too_long,value.acquired[0].batch))
        duplicate=replace(original,files=(file(0),file(0)))
        self.reject('invalid_schema',lambda:AcquiredInput('trades',duplicate,value.acquired[0].batch))

    def test_normalization_limits_and_nonfinite(self):
        value=fixture();original=value.acquired[0].receipt
        for changed in (replace(original.normalization,price_conversions=tuple(PriceConversion('price','integer','none',0) for _ in range(17))),
                        replace(original.normalization,unmapped_fields=tuple('f'+str(n) for n in range(65)))):
            self.reject('limit_exceeded',lambda:AcquiredInput('trades',replace(original,normalization=changed),value.acquired[0].batch))
        bad=fixture();object.__setattr__(bad.expected_task.config.parameters[0],'value',float('inf'))
        self.reject('invalid_schema',lambda:bind_supplied_provenance(bad))

    def test_unsupported_type_and_profile(self):
        class Custom(AcquiredInput): pass
        value=fixture()
        subclass=Custom('trades',value.acquired[0].receipt,value.acquired[0].batch)
        self.reject('invalid_schema',lambda:replace(value,acquired=(subclass,)))
        self.reject('unsupported_version',lambda:replace(value,expected_task=replace(value.expected_task,family='quotes')))


if __name__=='__main__':unittest.main()
