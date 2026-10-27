"""Reachable public limits and the independent closed traversal boundaries."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock,patch
from equity_feature_contracts import Column
from equity_feature_duckdb import AcquisitionReceipt,FileEvidence,ResolvedPartition
from equity_feature_workers import GenerationSpec,GenerationStore,BarrierLimits,Dependency
from equity_feature_io_contracts import SinkRequirements
from equity_feature_provenance import AcquiredInput,TaskEvidence,ProvenanceError,bind_supplied_provenance,verify_live_generation
from equity_feature_provenance._preflight import _Budget
from provenance_fixture import fixture


class BoundsTests(unittest.TestCase):
    def test_current_adapter_identity_is_exact_and_old_future_versions_refuse(self):
        value = fixture()
        acquired = value.acquired[0]
        self.assertEqual(acquired.receipt.adapter_version, '0.1.0a9')
        self.assertEqual(AcquiredInput('trades', acquired.receipt, acquired.batch).receipt,
                         acquired.receipt)
        for version in ('0.1.0a8', '0.1.0a10', 'unknown'):
            # Deliberately forged version must fail before hashing/encoding. The
            # immutable original remains unchanged, including its real a9 stamp.
            candidate = replace(acquired.receipt)
            object.__setattr__(candidate, 'adapter_version', version)
            self.refusal('unsupported_version',
                         lambda: AcquiredInput('trades', candidate, acquired.batch))
        self.assertEqual(acquired.receipt.adapter_version, '0.1.0a9')

    def refusal(self,code,call):
        with self.assertRaises(ProvenanceError) as caught:call()
        self.assertEqual(caught.exception.code,code)
        self.assertIsNone(caught.exception.__context__)

    def test_rows512_and513_before_identity_or_encoder(self):
        value=fixture();batch=value.acquired[0].batch
        def observed(n):
            cols=tuple(replace(c,values=(c.values[0],)*n) for c in batch.columns)
            return replace(batch,columns=cols,metadata=replace(batch.metadata,coverage=replace(batch.metadata.coverage,expected=n,observed=n)))
        allowed=AcquiredInput('trades',value.acquired[0].receipt,observed(512))
        self.assertEqual(allowed.batch.row_count,512)
        identity=Mock(side_effect=AssertionError('No identity before bounds'))
        encoder=Mock(side_effect=AssertionError('No encoding before bounds'))
        with patch.object(AcquisitionReceipt,'identity_digest',property(identity)),patch('equity_feature_provenance._bind.encode_task',encoder):
            self.refusal('limit_exceeded',lambda:AcquiredInput('trades',value.acquired[0].receipt,observed(513)))
        identity.assert_not_called();encoder.assert_not_called()

    def test_path_boundary_4096_codepoints_16384_bytes(self):
        value=fixture();receipt=value.acquired[0].receipt
        def candidate(n):
            p=ResolvedPartition('2026-01-01','\U00010000'*n,'/synthetic',1,1,3,'a'*64,'fixture','fixture',None,None,None,None)
            return replace(receipt,files=(FileEvidence(p,'b'*64,None),))
        observed=AcquiredInput('trades',candidate(4096),value.acquired[0].batch)
        self.assertEqual(len(observed.receipt.files[0].partition.original_path.encode('utf-8')),16384)
        self.refusal('limit_exceeded',lambda:AcquiredInput('trades',candidate(4097),value.acquired[0].batch))

    def test_conversion_unmapped_and_missing_session_caps(self):
        value=fixture();receipt=value.acquired[0].receipt
        from equity_feature_duckdb import PriceConversion
        norm=replace(receipt.normalization,price_conversions=tuple(PriceConversion('f'+str(i),'integer','none',0) for i in range(16)),
            unmapped_fields=tuple('f'+str(i) for i in range(64)))
        self.assertEqual(len(AcquiredInput('trades',replace(receipt,normalization=norm),value.acquired[0].batch).receipt.normalization.unmapped_fields),64)
        from datetime import date,timedelta
        sessions=tuple((date(2026,1,1)+timedelta(days=n)).isoformat() for n in range(129))
        self.assertEqual(len(AcquiredInput('trades',replace(receipt,missing_sessions=sessions[:128]),value.acquired[0].batch).receipt.missing_sessions),128)
        self.refusal('limit_exceeded',lambda:AcquiredInput('trades',replace(receipt,missing_sessions=sessions),value.acquired[0].batch))

    def test_generator_bool_unknown_type_and_nested_version(self):
        value=fixture()
        self.refusal('invalid_schema',lambda:replace(value,acquired=(x for x in value.acquired)))
        bad=fixture();object.__setattr__(bad.acquired[0].receipt,'rows',True)
        self.refusal('invalid_schema',lambda:bind_supplied_provenance(bad))
        bad=fixture();object.__setattr__(bad.acquired[0].receipt,'adapter_version','future')
        self.refusal('unsupported_version',lambda:bind_supplied_provenance(bad))
        bad=fixture();object.__setattr__(bad.acquired[0].receipt.normalization,'unmapped_fields',(object(),))
        self.refusal('invalid_schema',lambda:bind_supplied_provenance(bad))

    def test_traversal_depth_sequence_and_work_estimate(self):
        # These are traversal-unit controls, not claims of a valid public graph
        # combining every maximum; stronger schema bounds remain conjunctive.
        nested=None
        for _ in range(16):nested=(nested,)
        _Budget().walk(nested)
        self.refusal('limit_exceeded',lambda:_Budget().walk((nested,)))
        _Budget().walk((None,)*4096)
        self.refusal('limit_exceeded',lambda:_Budget().walk((None,)*4097))
        # 64 units per visit, then 12 per character. Force the exact remaining
        # estimate boundary after independently valid preceding visits.
        budget=_Budget();budget.units=1048576-64-12;budget.walk('x')
        self.assertEqual(budget.units,1048576)
        self.refusal('limit_exceeded',lambda:budget.walk(None))

    def test_task16_waiting_task17_and_custom_sink_before_store(self):
        values=tuple(fixture(instrument='A'+str(n)) for n in range(16))
        values=tuple(replace(v,output=None) for v in values)
        spec=GenerationSpec('demo','job','G',tuple(v.expected_task for v in values))
        dependencies=tuple(Dependency('i'+str(n),v.expected_task,None,None) for n,v in enumerate(values))
        with tempfile.TemporaryDirectory() as folder:
            store=GenerationStore(Path(folder),limits=BarrierLimits(),requirements=SinkRequirements())
            self.assertFalse(verify_live_generation(store,spec,dependencies,values).operational_readback)
            extra=fixture(instrument='last');spec17=replace(spec,tasks=spec.tasks+(extra.expected_task,))
            self.refusal('limit_exceeded',lambda:verify_live_generation(store,spec17,dependencies+(Dependency('last',extra.expected_task,None,None),),values+(extra,)))
            declared=fixture();custom=Mock()
            dependency=Dependency('custom',declared.expected_task,declared.output,custom)
            spy=Mock(side_effect=AssertionError('Store must not execute'))
            with patch.object(GenerationStore,'read',spy):
                self.refusal('unsupported_version',lambda:verify_live_generation(store,replace(spec,tasks=(declared.expected_task,)),(dependency,),(declared,)))
            spy.assert_not_called();self.assertEqual(custom.mock_calls,[])

    def test_duplicate_partition_and_entity_before_store(self):
        a=replace(fixture(K=210),output=None)
        b=replace(fixture(K=200),output=None)
        # Distinct task hashes do not make duplicate entity/partition admissible.
        self.assertNotEqual(a.expected_task.task_sha256,b.expected_task.task_sha256)
        partition_other=replace(b,expected_task=replace(b.expected_task,partition_id='other'))
        entity_other=replace(fixture(instrument='B'),output=None)
        entity_other=replace(entity_other,expected_task=replace(entity_other.expected_task,partition_id='A-S'))
        with tempfile.TemporaryDirectory() as folder:
            store=GenerationStore(Path(folder),limits=BarrierLimits(),requirements=SinkRequirements())
            spy=Mock(side_effect=AssertionError('Store must not execute'))
            for second in (b,partition_other,entity_other):
                values=(a,second)
                spec=GenerationSpec('demo','job','G',tuple(v.expected_task for v in values))
                dependencies=tuple(Dependency('d'+str(n),v.expected_task,None,None) for n,v in enumerate(values))
                with patch.object(GenerationStore,'read',spy):
                    self.refusal('invalid_schema',lambda:verify_live_generation(store,spec,dependencies,values))
            spy.assert_not_called()


if __name__=='__main__':unittest.main()
