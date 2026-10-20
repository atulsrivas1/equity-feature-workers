"""Closed actual-return binding and independent native parent joins."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_owned_runner import (validate_return,decode_return,validate_options,
    run_owned_control,PROFILES,SERIES)
from eq142_graph import wire
from eq142_fixture import HAVE_INTRADAY,require_cpu_slots

JOB='Local\\equity-feature-supplied-'+'a'*32


def record():
    return {'schema':'eq142-owned-runner-return:1','sample_id':'sample-1','purpose':'development_candidate',
        'runner_reference':{'codec':'eq142_development_runner','sha256':'b'*64,'bytes':73,'retained_name':'runner.json'},
        'process_binding':{'pid':42,'process_startup_nonce':'c'*32}}


def options():
    return dict(sample_id='sample-1',purpose='development_candidate',profile='family_missing_hl',
        mode='sequential',degree=1,control='uninstrumented_A',series=SERIES[0],job_name=JOB)


class PortableWriter(unittest.TestCase):
    def test_closed_canonical_record_and_cross_codec_types(self):
        valid=record();validate_return(valid);self.assertEqual(decode_return(wire(valid)),valid)
        for field,value in [('schema','eq142-owned-runner-return:2'),('sample_id','../sample-1'),
                ('sample_id','A'),('purpose','study_accepted'),('extra',False)]:
            bad=deepcopy(valid);bad[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):validate_return(bad)
        for field,value in [('codec','encode_result'),('sha256','B'*64),('bytes',True),('bytes',0),
                ('bytes',1048577),('retained_name','../runner.json'),('extra',None)]:
            bad=deepcopy(valid);bad['runner_reference'][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):validate_return(bad)
        for field,value in [('pid',True),('pid',0),('pid',4294967296),('process_startup_nonce','A'*32),('extra',0)]:
            bad=deepcopy(valid);bad['process_binding'][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):validate_return(bad)
        body=wire(valid)
        for malformed in (body+b' ',body.replace(b'{',b'{"schema":"duplicate",',1),b'{}',b' '*4097,b'\xff',b'[]'):
            with self.assertRaises(ValueError):decode_return(malformed)

    def test_selector_refusals_before_original_import(self):
        valid=options();validate_options(reference_sha256='a'*64,**valid)
        for field,value in [('sample_id','../sample-1'),('purpose','official'),('profile','unknown'),
                ('mode','pool'),('degree',True),('degree',2),('control','A'),('series','unbounded'),
                ('job_name',None),('purpose','qualification_counts')]:
            bad={**valid,field:value}
            with self.subTest(field=field),self.assertRaises(ValueError):
                run_owned_control('unused','a'*64,directory='unused',**bad)

    def test_actual_return_forwarded_once_and_exclusive_outer_namespace(self):
        from eq142_evidence import persist_record
        fake=types.ModuleType('benchmark_execution_baseline')
        original_ref={}
        def original(reference_path,reference_sha256,*,directory,**kwargs):
            self.assertEqual(directory.name,'runner');self.assertFalse(directory.exists())
            directory.mkdir(exist_ok=False)
            original_ref.update(codec='eq142_development_runner',**persist_record(directory/'runner.json',
                {'schema':'synthetic-original-return'},max_bytes=1048576))
            self.assertFalse(kwargs['qualify_code_counts'])
            return original_ref
        fake.run_control=Mock(side_effect=original)
        with tempfile.TemporaryDirectory() as directory,patch.dict(sys.modules,{'benchmark_execution_baseline':fake}):
            sample=Path(directory)/'sample-1';sample.mkdir()
            (sample/'capacity-before.json').write_bytes(b'{}\n')
            returned=run_owned_control('unused','a'*64,directory=sample,**options())
            body=(sample/'runner-reference.json').read_bytes();value=decode_return(body)
            self.assertEqual(value['runner_reference'],original_ref)
            self.assertEqual(returned,{'codec':'eq142_owned_runner_return','sha256':hashlib.sha256(body).hexdigest(),
                'bytes':len(body),'retained_name':'runner-reference.json'})
            self.assertEqual(value['process_binding']['pid'],os.getpid());self.assertEqual(fake.run_control.call_count,1)
            with self.assertRaises(ValueError):run_owned_control('unused','a'*64,directory=sample,**options())
            self.assertEqual(fake.run_control.call_count,1)
        for name in ('runner','runner-reference.json','runner-reference.json.partial','foreign.json'):
            with tempfile.TemporaryDirectory() as directory,patch.dict(sys.modules,{'benchmark_execution_baseline':fake}):
                sample=Path(directory)/'sample-1';sample.mkdir();(sample/name).write_bytes(b'preserved')
                with self.assertRaises(ValueError):run_owned_control('unused','a'*64,directory=sample,**options())
                self.assertEqual((sample/name).read_bytes(),b'preserved')
        with tempfile.TemporaryDirectory() as directory,patch('eq142_owned_runner.Path.is_junction',return_value=True):
            sample=Path(directory)/'sample-1';sample.mkdir()
            with self.assertRaises(ValueError):run_owned_control('unused','a'*64,directory=sample,**options())
        self.assertEqual(fake.run_control.call_count,1)

    def test_original_failure_keeps_identity_cause_and_failed_bytes_without_success(self):
        from supplied_native import NativeFailure
        error=NativeFailure('SYNTHETIC_ORIGINAL_FAILURE');cause=OSError('synthetic original cause')
        error.__cause__=cause
        fake=types.ModuleType('benchmark_execution_baseline')
        def original(*args,**kwargs):
            target=kwargs['directory'];target.mkdir();(target/'runner.json').write_bytes(b'original failed bytes\n')
            raise error
        fake.run_control=Mock(side_effect=original)
        with tempfile.TemporaryDirectory() as directory,patch.dict(sys.modules,{'benchmark_execution_baseline':fake}):
            sample=Path(directory)/'sample-1';sample.mkdir()
            with self.assertRaises(NativeFailure) as caught:run_owned_control('unused','a'*64,directory=sample,**options())
            self.assertIs(caught.exception,error);self.assertIs(error.__cause__,cause)
            self.assertEqual(error.code,'SYNTHETIC_ORIGINAL_FAILURE');self.assertEqual(fake.run_control.call_count,1)
            self.assertEqual((sample/'runner/runner.json').read_bytes(),b'original failed bytes\n')
            self.assertFalse((sample/'runner-reference.json').exists())
            self.assertFalse((sample/'runner-reference.json.partial').exists())


@unittest.skipUnless(HAVE_INTRADAY and os.name=='nt','Optional installed intraday plus Windows owned Job required')
class ActualOwnedRunner(unittest.TestCase):
    def test_actual_owned_a_b_sequential_process_and_profile_purpose(self):
        from benchmark_execution_baseline import prepare_reference,PROFILES as ACTUAL_PROFILES
        from eq142_native import run_owned
        self.assertEqual(PROFILES,ACTUAL_PROFILES)
        slots=require_cpu_slots(self,5)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ref=prepare_reference(root/'reference',cpu_slots=slots,profiles=('family_missing_hl',))
            for mode,degree,control,purpose in (
                    ('sequential',1,'uninstrumented_A','development_candidate'),
                    ('sequential',1,'instrumented_B','development_candidate'),
                    ('process',2,'uninstrumented_A','development_candidate'),
                    ('process',2,'instrumented_B','development_candidate'),
                    ('process',2,'instrumented_B','qualification_counts')):
                name=mode+'-'+control.lower()+'-'+purpose
                with self.subTest(name=name):
                    sample=root/name;sample.mkdir()
                    native=run_owned([sys.executable,str(ROOT/'tools/eq142_owned_runner.py'),
                        '--directory',str(sample),'--sample-id',name,'--purpose',purpose,
                        '--reference',str(root/'reference/reference.json'),'--reference-sha256',ref['sha256'],
                        '--profile','family_missing_hl','--mode',mode,'--degree',str(degree),
                        '--control',control,'--series',SERIES[0],'--job','{job_name}'],cwd=ROOT,timeout_s=90)
                    self.assertEqual(native['exit_code'],0);self.assertEqual(native['probe_errors'],[])
                    self.assertEqual(native['final_accounting']['active_processes'],0)
                    returned=decode_return((sample/'runner-reference.json').read_bytes())
                    runner_body=(sample/'runner/runner.json').read_bytes();runner=json.loads(runner_body)
                    self.assertEqual(returned['runner_reference'],{'codec':'eq142_development_runner',
                        'sha256':hashlib.sha256(runner_body).hexdigest(),'bytes':len(runner_body),'retained_name':'runner.json'})
                    parent=json.loads((sample/'runner/owned-processes/parent.json').read_bytes())
                    self.assertEqual(returned['process_binding'],{k:parent[k] for k in ('pid','process_startup_nonce')})
                    self.assertEqual(runner['schema'],'eq142-development-runner:2');self.assertFalse(runner['full_sample_accepted'])
                    self.assertEqual(runner['status'],'development_product_and_local_trace_checks_passed')
                    self.assertIn('expected_accepted_output_metadata_bytes',runner['delegate_admission'])
                    self.assertNotIn('expected_committed_output_metadata_bytes',runner['delegate_admission'])
                    self.assertTrue((sample/'runner/diagnostics.json').is_file());self.assertFalse((sample/'diagnostics.json').exists())
                    self.assertTrue((root/'reference/objects').is_dir());self.assertFalse((sample/'objects').exists())
                    self.assertEqual('qualification_code_counts' in runner,purpose=='qualification_counts')
                    if purpose=='qualification_counts':
                        self.assertIsNone(runner['selected_phase_wall_ns'])
                        self.assertTrue((sample/'runner/code-counts/parent.json').is_file())
                    if control=='instrumented_B':
                        self.assertIn(returned['process_binding']['process_startup_nonce'],runner['retained_spans']['processes'])


if __name__=='__main__':unittest.main()
