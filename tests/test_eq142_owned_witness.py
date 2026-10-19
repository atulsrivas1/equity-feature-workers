"""Writer-bound proofs, admission and error-preserving owned controls."""
from contextlib import nullcontext
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from test_eq142_identity import witness,traced,JOB
from eq142_identity import retain_owned_witness,read_owned_witnesses,validate_reference
from eq142_fixture import HAVE_INTRADAY,qualification,bound_inventory,load,require_cpu_slots
from eq142_admission import original_reservation,delegate_reservation
from eq142_delegate import make_plan,partition_entry
from equity_feature_workers import ResourceBudget
from equity_feature_workers.supervisor import Partition
if HAVE_INTRADAY:
    from supplied_intraday_fixture import facts,work
    from benchmark_execution_baseline import prepare_reference,run_control,SERIES


class WriterBoundProofs(unittest.TestCase):
    def test_exact_written_reference_hash_and_proof_population(self):
        with tempfile.TemporaryDirectory() as directory:
            record=witness()
            with patch('eq142_identity.capture_owned_process',return_value=record):
                reference=retain_owned_witness(directory,'parent.json',JOB)
                with self.assertRaises(ValueError):retain_owned_witness(directory,'parent.json',JOB)
            checked=read_owned_witnesses(directory,[reference],job_name=JOB)
            self.assertEqual(checked['scope'],'runner_process_only');self.assertFalse(checked['full_sample_accepted'])
            checked=read_owned_witnesses(directory,[reference],job_name=JOB,processes=traced(record))
            self.assertEqual(checked['scope'],'actual_validated_trace_processes_only')
            path=Path(directory)/'parent.json';original=path.read_bytes()
            path.write_bytes(original.replace(b'10000000',b'10000001'))
            with self.assertRaises(ValueError):read_owned_witnesses(directory,[reference],job_name=JOB)
            path.write_bytes(original)
            for name in ('other.json','parent.json.partial'):
                extra=Path(directory)/name;extra.write_bytes(b'{}')
                with self.assertRaises(ValueError):read_owned_witnesses(directory,[reference],job_name=JOB)
                extra.unlink()
            for changed in ({**reference,'bytes':reference['bytes']-1},{**reference,'sha256':'0'*64}):
                with self.assertRaises(ValueError):read_owned_witnesses(directory,[changed],job_name=JOB)
            with self.assertRaises(ValueError):read_owned_witnesses(directory,[reference,reference],job_name=JOB)
            with patch.object(Path,'is_symlink',return_value=True):
                with self.assertRaises(ValueError):read_owned_witnesses(directory,[reference],job_name=JOB)

    def test_closed_references_and_parent_only_scope(self):
        reference={'codec':'eq142_owned_process_witness','sha256':'a'*64,'bytes':1,'retained_name':'parent.json'}
        validate_reference(reference)
        for field,value in [('codec','encode_result'),('sha256','A'*64),('bytes',True),('bytes',4097),
                ('retained_name','../parent.json'),('retained_name','delegate-016.json')]:
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):validate_reference({**reference,field:value})
        with self.assertRaises(ValueError):validate_reference({**reference,'extra':None})
        with tempfile.TemporaryDirectory() as directory,patch('eq142_identity.capture_owned_process',return_value=witness()):
            first=retain_owned_witness(directory,'parent.json',JOB)
            second=retain_owned_witness(directory,'delegate-000.json',JOB)
            with self.assertRaises(ValueError):read_owned_witnesses(directory,[first,second],job_name=JOB)
            joined=read_owned_witnesses(directory,[first,second],job_name=JOB,processes=traced(witness()))
            self.assertEqual(len(joined['processes']),1);self.assertEqual(len(joined['references']),2)


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class OwnedWitnessIntegration(unittest.TestCase):
    def planned(self,root):
        context,config,batch=facts('OWNED');item=work(context,config,(batch,),name='owned-witness')
        budget=ResourceBudget(max_resident_bytes=134217728,max_cpu_slots=2)
        trace=root/'trace';trace.mkdir();proof=root/'owned-processes';proof.mkdir()
        plan,_=make_plan((item,),mode='sequential',budget=budget,expected_inventory=bound_inventory(qualification()),
            output_dir=trace,operations=load('RECORD_CONTRACTS.json')['trace']['operations'],
            owned_process_witness={'job_name':JOB,'output_dir':str(proof)})
        return item,budget,plan

    def test_actual_plan_encoded_admission_at_and_beyond_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            item,budget,plan=self.planned(Path(directory));base=original_reservation((item,),mode='sequential',budget=budget)
            record=delegate_reservation(plan,(item,),budget,original=base)
            self.assertEqual(record['owned_witness_population'],2)
            self.assertEqual(record['owned_witness_logical_reservation_bytes'],24576)
            exact=replace(budget,max_resident_bytes=record['combined_logical_resident_bytes'])
            self.assertEqual(delegate_reservation(plan,(item,),exact,original=base),record)
            with self.assertRaises(ValueError):delegate_reservation(plan,(item,),replace(exact,
                max_resident_bytes=exact.max_resident_bytes-1),original=base)
            for count in (0,1,3,True):
                with self.assertRaises(ValueError):delegate_reservation(plan,(item,),budget,original=base,owned_witness_count=count)
            parent=delegate_reservation(None,(item,),budget,original=base,owned_witness_count=1)
            self.assertEqual(parent['owned_witness_logical_reservation_bytes'],12288)
            self.assertGreater(record['plan_pickle_bytes'],0)

    def test_delegate_observer_failure_preserves_original_once_and_exception_identity(self):
        for original_fails in (False,True):
            with self.subTest(original_fails=original_fails),tempfile.TemporaryDirectory() as directory:
                item,budget,plan=self.planned(Path(directory));value=object();original_error=RuntimeError('original callback failure')
                with patch('eq142_identity.retain_owned_witness',side_effect=OSError('observer failure')),patch(
                        'eq142_delegate._ORIGINAL_PARTITION',side_effect=original_error if original_fails else None,
                        return_value=value) as callback:
                    if original_fails:
                        with self.assertRaises(RuntimeError) as caught:partition_entry(Partition(0,(item,)),budget,plan=plan)
                        self.assertIs(caught.exception,original_error)
                    else:self.assertIs(partition_entry(Partition(0,(item,)),budget,plan=plan),value)
                self.assertEqual(callback.call_count,1)
                fragment=json.loads((Path(plan['output_dir'])/'delegate-000.json').read_bytes())
                self.assertEqual(fragment['schema'],'eq142-local-trace-fragment:2')
                self.assertFalse(fragment['complete']);self.assertIsNone(fragment['owned_process_witness'])

    def test_parent_failure_retains_actual_products_and_independent_storage_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ref=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,4),profiles=('family_missing_hl',))
            for fault in ('witness','storage','original'):
                with self.subTest(fault=fault):
                    local=root/fault;observer=OSError('independent observer');storage=OSError('independent store')
                    original=RuntimeError('original supervisor')
                    target='benchmark_execution_baseline.record_products' if fault=='storage' else 'benchmark_execution_baseline.BoundedSupervisor.run'
                    optional=patch(target,side_effect=storage if fault=='storage' else original) if fault!='witness' else nullcontext()
                    with patch('supplied_native.counters',return_value={'user':0,'kernel':0}),patch(
                            'benchmark_execution_baseline.retain_owned_witness',side_effect=observer),optional:
                        with self.assertRaises(OSError if fault!='original' else RuntimeError) as caught:
                            run_control(root/'reference/reference.json',ref['sha256'],directory=local,
                                profile='family_missing_hl',mode='sequential',degree=1,
                                control='uninstrumented_A',series=SERIES[0],job_name=JOB)
                    self.assertIs(caught.exception,observer if fault=='witness' else storage if fault=='storage' else original)
                    report=json.loads((local/'runner.json').read_bytes());self.assertFalse(report['full_sample_accepted'])
                    self.assertEqual(report['owned_witness_failure_identity']['error_type'],'builtins.OSError')
                    self.assertEqual(report['failure_identity']['stage'],{
                        'witness':'deferred_parent_witness_failure','storage':'retain_products','original':'original_supervisor'}[fault])
                    self.assertEqual(report['original_outcome_returned'],fault!='original')
                    if fault=='witness':self.assertTrue((local/'products.json').is_file())
                    if fault=='storage':
                        self.assertTrue((local/'supervisor.json').is_file());self.assertTrue(storage.__notes__)
                        self.assertIsNone(storage.__cause__)


@unittest.skipUnless(HAVE_INTRADAY and os.name=='nt','Optional installed intraday plus Windows owned Job required')
class NativeOwnedWitnessIntegration(unittest.TestCase):
    def test_actual_owned_parent_and_delegate_proofs_all_three_modes(self):
        from eq142_native import run_owned
        slots=require_cpu_slots(self,5)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ref=prepare_reference(root/'reference',cpu_slots=slots,profiles=('family_missing_hl',))
            for mode,degree in (('sequential',1),('thread',2),('process',2)):
                for control in ('uninstrumented_A','instrumented_B'):
                    with self.subTest(mode=mode,control=control):
                        local=root/(mode+'-'+control)
                        native=run_owned([sys.executable,str(ROOT/'tools/benchmark_execution_baseline.py'),
                            'qualification-runner','--directory',str(local),'--reference',str(root/'reference/reference.json'),
                            '--reference-sha256',ref['sha256'],'--profile','family_missing_hl','--mode',mode,
                            '--degree',str(degree),'--control',control,'--series',SERIES[0],'--job','{job_name}'],cwd=ROOT,timeout_s=60)
                        self.assertEqual(native['exit_code'],0);self.assertEqual(native['probe_errors'],[])
                        self.assertEqual(native['final_accounting']['active_processes'],0)
                        report=json.loads((local/'runner.json').read_bytes());self.assertFalse(report['full_sample_accepted'])
                        proof=report['owned_process_witnesses'];population=report['delegate_admission']['owned_witness_population']
                        self.assertEqual(len(proof['references']),population)
                        self.assertEqual(report['delegate_admission']['owned_witness_logical_reservation_bytes'],population*12288)
                        parent=json.loads((local/'owned-processes/parent.json').read_bytes())
                        self.assertLessEqual(int(report['runner_control_phase_start_ns']),int(parent['capture_start_ns']))
                        self.assertLessEqual(int(parent['capture_end_ns']),int(report['runner_control_phase_end_ns']))
                        self.assertTrue(report['native_control_scope']['common_metadata_may_dilute_ratios'])
                        if control=='uninstrumented_A':
                            self.assertEqual(population,1);self.assertIsNone(report['retained_spans'])
                        else:
                            self.assertEqual({r['process_startup_nonce'] for r in proof['processes']},set(report['retained_spans']['processes']))
                            self.assertGreater(population,1)


if __name__=='__main__':unittest.main()
