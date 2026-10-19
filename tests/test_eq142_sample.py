"""Development sample assembly; explicitly not frozen benchmark acceptance."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY,require_cpu_slots
if HAVE_INTRADAY:
    from benchmark_execution_baseline import prepare_reference,run_control,source_state,verify_result,SERIES


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class DevelopmentSample(unittest.TestCase):
    def test_original_controls_actual_modes_and_incomplete_full_sample_label(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ref=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,4),profiles=('family_missing_hl',))
            reference=root/'reference/reference.json'
            for mode,degree in (('sequential',1),('thread',2),('process',2)):
                for control in ('uninstrumented_A','instrumented_B'):
                    with self.subTest(mode=mode,control=control):
                        require_cpu_slots(self,degree+3)
                        local=root/(mode+'-'+control)
                        verified=[]
                        def observed_verify(*args):
                            result=verify_result(*args);verified.append(time.perf_counter_ns());return result
                        with patch('benchmark_execution_baseline.verify_result',side_effect=observed_verify):
                            result=run_control(reference,ref['sha256'],directory=local,profile='family_missing_hl',
                                mode=mode,degree=degree,control=control,series=SERIES[1])
                        report=json.loads((local/result['retained_name']).read_bytes())
                        self.assertEqual(report['status'],'development_product_and_local_trace_checks_passed')
                        self.assertFalse(report['full_sample_accepted'])
                        self.assertIsNone(report['selected_phase_wall_ns'])
                        self.assertEqual(report['selected_phase_scope'],'external_owned_runner_final_exit_required')
                        self.assertEqual(len(verified),3)
                        self.assertTrue(all(int(report['runner_control_phase_start_ns'])<=t<=int(report['runner_control_phase_end_ns']) for t in verified))
                        self.assertEqual(len(report['product_outcomes']),3)
                        self.assertEqual(report['original_admission']['evidence'],'source_derived_preflight')
                        self.assertTrue((local/'products.json').is_file())
                        if control=='instrumented_B':
                            self.assertGreater(report['retained_spans']['total_span_count'],0)
                            self.assertGreater(report['delegate_admission']['logical_spec_resident_reservation_bytes'],0)
                            if mode!='sequential':self.assertTrue(report['parent_relations'])
                        else:self.assertIsNone(report['retained_spans'])

    def test_source_reference_rejection_is_retained_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ref=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,4),profiles=('family_missing_hl',))
            reference=root/'reference/reference.json';local=root/'rejected'
            options=dict(directory=local,profile='family_missing_hl',mode='sequential',degree=1,
                control='uninstrumented_A',series=SERIES[0])
            with self.assertRaises(ValueError):run_control(reference,'0'*64,**options)
            body=(local/'runner.json').read_bytes();record=json.loads(body)
            self.assertEqual(record['status'],'failed_before_phase');self.assertFalse(record['full_sample_accepted'])
            with self.assertRaises(FileExistsError):run_control(reference,ref['sha256'],**options)
            self.assertEqual((local/'runner.json').read_bytes(),body)
            # Change the observed helper digest without modifying a shared file.
            actual=source_state()
            self.assertIn('tools/build_supplied_inputs.py',actual['files'])
            actual['files']['tools/build_supplied_inputs.py']='0'*64
            with patch('benchmark_execution_baseline.source_state',return_value=actual):
                with self.assertRaises(ValueError):run_control(reference,ref['sha256'],
                    **{**options,'directory':root/'changed-helper'})
            rejected=json.loads((root/'changed-helper/runner.json').read_bytes())
            self.assertEqual(rejected['status'],'failed_before_phase')


@unittest.skipUnless(HAVE_INTRADAY and os.name=='nt','Optional installed intraday plus Windows owned Job required')
class NativeOwnedDevelopmentSample(unittest.TestCase):
    def test_actual_owned_spawn_counter_and_bounded_native_receipt(self):
        from eq142_native import run_owned
        from eq142_evidence import persist_record
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);slots=require_cpu_slots(self,5)
            ref=prepare_reference(root/'reference',cpu_slots=slots,profiles=('family_missing_hl',))
            local=root/'owned-runner'
            native=run_owned([sys.executable,str(ROOT/'tools/benchmark_execution_baseline.py'),
                'qualification-runner','--directory',str(local),
                '--reference',str(root/'reference/reference.json'),'--reference-sha256',ref['sha256'],
                '--profile','family_missing_hl','--mode','process','--degree','2',
                '--control','instrumented_B','--series',SERIES[1],'--job','{job_name}'],cwd=ROOT,timeout_s=60)
            self.assertEqual(native['exit_code'],0);self.assertEqual(native['probe_errors'],[])
            self.assertEqual(native['final_accounting']['active_processes'],0)
            actual=json.loads((local/'runner.json').read_bytes())
            self.assertEqual(actual['status'],'development_product_and_local_trace_checks_passed')
            self.assertTrue(actual['native_job_supplied']);self.assertFalse(actual['full_sample_accepted'])
            for field in ('user','kernel'):
                self.assertGreaterEqual(actual['after_phase'][field],actual['before_phase'][field])
            traced={record['pid'] for record in actual['retained_spans']['processes'].values()}
            self.assertTrue(traced<=set(native['pids_observed']))
            self.assertGreaterEqual(len(traced),2)
            reference=persist_record(root/'native.json',native,max_bytes=16777216)
            self.assertEqual(reference['bytes'],len((root/'native.json').read_bytes()))


if __name__=='__main__':unittest.main()
