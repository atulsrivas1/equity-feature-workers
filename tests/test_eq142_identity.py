"""Closed lifetime joins and direct Windows membership/handle controls."""
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from eq142_identity import capture_owned_process,validate_witness,join_owned_processes,WitnessKernel,NativeFailure
from supplied_native import run_owned

JOB='Local\\equity-feature-supplied-'+'a'*32


def witness():
    return {'schema':'eq142-owned-process-witness:1','job_name':JOB,'pid':123,
        'process_startup_nonce':'b'*32,'creation_filetime_100ns_since_1601_utc':'18446744073709551615',
        'is_member':True,'capture_start_ns':'0','capture_end_ns':'9223372036854775807',
        'clock':{'name':'perf_counter','implementation':'QueryPerformanceCounter()',
            'monotonic':True,'adjustable':False,'reported_resolution_seconds':'0.0000001',
            'qpc_frequency_hz':10000000,'cross_process_raw_ticks_qualified':False}}


def traced(record,*,unqueried=True):
    clock=deepcopy(record['clock'])
    if unqueried:clock['qpc_frequency_hz']=None
    return {record['process_startup_nonce']:{'pid':record['pid'],'clock':clock}}


class LifetimeJoinControls(unittest.TestCase):
    def test_exact_native_integer_clock_and_closed_record_bounds(self):
        record=witness();validate_witness(record,expected_job=JOB)
        mutations=[('pid',True),('pid',0),('pid',4294967296),('is_member',1),
            ('job_name',JOB+'x'),('process_startup_nonce','g'*32),('schema','other'),
            ('creation_filetime_100ns_since_1601_utc','0'),
            ('creation_filetime_100ns_since_1601_utc','18446744073709551616'),
            ('creation_filetime_100ns_since_1601_utc','01'),('capture_start_ns','-1'),
            ('capture_end_ns','9223372036854775808'),('capture_end_ns',1)]
        for field,value in mutations:
            with self.subTest(field=field,value=value):
                changed=deepcopy(record);changed[field]=value
                with self.assertRaises(ValueError):validate_witness(changed,expected_job=JOB)
        for field,values in {'qpc_frequency_hz':[None,0,True,9007199254740992],
                'reported_resolution_seconds':['0','NaN','1e-7','9'*65],
                'monotonic':[1,False],'cross_process_raw_ticks_qualified':[True],
                'implementation':['clock_gettime(CLOCK_MONOTONIC)']}.items():
            for value in values:
                with self.subTest(field=field,value=value):
                    changed=deepcopy(record);changed['clock'][field]=value
                    with self.assertRaises(ValueError):validate_witness(changed,expected_job=JOB)
        for target in ('record','clock'):
            changed=deepcopy(record);(changed if target=='record' else changed['clock'])['extra']=None
            with self.assertRaises(ValueError):validate_witness(changed,expected_job=JOB)
        changed=deepcopy(record);changed.update(capture_start_ns='2',capture_end_ns='1')
        with self.assertRaises(ValueError):validate_witness(changed,expected_job=JOB)

    def test_bidirectional_lifetime_identity_and_nullable_trace_clock(self):
        first=witness();processes=traced(first)
        duplicate=deepcopy(first);duplicate.update(capture_start_ns='1',capture_end_ns='2')
        joined=join_owned_processes([first,duplicate],processes,job_name=JOB)
        self.assertEqual(len(joined),1);self.assertEqual(joined[0]['clock']['qpc_frequency_hz'],10000000)
        self.assertIsNone(processes['b'*32]['clock']['qpc_frequency_hz'])
        # PID reuse is a new lifetime, while each native lifetime has one nonce.
        reused=deepcopy(first);reused['process_startup_nonce']='c'*32
        reused['creation_filetime_100ns_since_1601_utc']='123456789012345678'
        processes.update(traced(reused,unqueried=False))
        self.assertEqual(len(join_owned_processes([first,reused],processes,job_name=JOB)),2)
        drift=deepcopy(first);drift['creation_filetime_100ns_since_1601_utc']='1'
        with self.assertRaises(ValueError):join_owned_processes([first,drift],traced(first),job_name=JOB)
        collision=deepcopy(first);collision['process_startup_nonce']='c'*32
        both=traced(first);both.update(traced(collision))
        with self.assertRaises(ValueError):join_owned_processes([first,collision],both,job_name=JOB)
        for field,value in [('qpc_frequency_hz',3),('reported_resolution_seconds','0.001'),('pid',124)]:
            changed=traced(first)
            if field=='pid':changed['b'*32][field]=value
            else:changed['b'*32]['clock'][field]=value
            with self.assertRaises(ValueError):join_owned_processes([first],changed,job_name=JOB)
        for witnesses,actual in [([],traced(first)),([first],{}),([first],processes),
                ([first]*18,traced(first)),([reused],traced(first))]:
            with self.assertRaises(ValueError):join_owned_processes(witnesses,actual,job_name=JOB)


@unittest.skipUnless(os.name=='nt','Windows owned process witness; no Linux native measurement claim')
class WindowsOwnedWitness(unittest.TestCase):
    def test_actual_owned_child_and_exact_bounded_witness(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'witness.json'
            evidence=run_owned([sys.executable,str(ROOT/'tests/eq142_identity_child.py'),
                '--job','{job_name}','--output',str(output)],cwd=ROOT,timeout_s=15)
            self.assertEqual(evidence['exit_code'],0)
            self.assertEqual(evidence['final_accounting']['active_processes'],0)
            raw=output.read_bytes();self.assertLessEqual(len(raw),4096)
            record=json.loads(raw);validate_witness(record,expected_job=evidence['job_name'])
            self.assertTrue(record['is_member']);self.assertNotEqual(record['pid'],os.getpid())
            self.assertEqual(len(join_owned_processes([record],traced(record),job_name=evidence['job_name'])),1)

    def test_actual_nonmember_and_missing_job_fail_closed(self):
        kernel=WitnessKernel();name='Local\\equity-feature-supplied-'+uuid.uuid4().hex
        handle=kernel.CreateJobObjectW(None,name);self.assertTrue(handle)
        try:
            with self.assertRaises(NativeFailure) as caught:capture_owned_process(name)
            self.assertEqual(caught.exception.code,'WITNESS_NOT_MEMBER')
        finally:self.assertTrue(kernel.CloseHandle(handle))
        with self.assertRaises(NativeFailure) as caught:capture_owned_process(name)
        self.assertEqual(caught.exception.code,'WITNESS_OPEN_JOB')

    def fake(self,*,fault=None,close_failure=False):
        kernel=object.__new__(WitnessKernel);closed=[]
        kernel.OpenJobObjectW=lambda *args:0 if fault=='open' else 101
        kernel.GetCurrentProcess=lambda:-1
        kernel.GetProcessId=lambda handle:0 if fault=='pid' else os.getpid()
        def member(process,job,pointer):
            pointer._obj.value=fault!='foreign';return fault!='membership'
        def creation(process,created,*unused):
            created._obj.dwLowDateTime=123;created._obj.dwHighDateTime=456
            if fault=='zero':created._obj.dwLowDateTime=0;created._obj.dwHighDateTime=0
            return fault!='creation'
        def frequency(pointer):pointer._obj.value=0 if fault=='qpf_zero' else 10000000;return fault!='qpf'
        kernel.IsProcessInJob=member;kernel.GetProcessTimes=creation;kernel.QueryPerformanceFrequency=frequency
        def close(handle):closed.append(handle);return not close_failure
        kernel.CloseHandle=close
        return kernel,closed

    def test_native_api_faults_close_only_opened_job_and_preserve_original_error(self):
        expected={'open':'WITNESS_OPEN_JOB','pid':'WITNESS_PROCESS_ID','foreign':'WITNESS_NOT_MEMBER',
            'membership':'WITNESS_MEMBERSHIP_API','creation':'WITNESS_CREATION_API',
            'zero':'WITNESS_CREATION_UNKNOWN','qpf':'WITNESS_QPF_API'}
        for fault,code in expected.items():
            for close_failure in (False,True):
                with self.subTest(fault=fault,close_failure=close_failure):
                    kernel,closed=self.fake(fault=fault,close_failure=close_failure)
                    with patch('eq142_identity.WitnessKernel',return_value=kernel):
                        with self.assertRaises(NativeFailure) as caught:capture_owned_process(JOB)
                    self.assertEqual(caught.exception.code,code)
                    self.assertEqual(closed,[] if fault=='open' else [101])
                    if close_failure and fault!='open':self.assertTrue(caught.exception.evidence['job_handle_close_failed'])
        for fault in ('qpf_zero',None):
            kernel,closed=self.fake(fault=fault,close_failure=True)
            with patch('eq142_identity.WitnessKernel',return_value=kernel):
                with self.assertRaises(ValueError if fault else NativeFailure) as caught:capture_owned_process(JOB)
            self.assertEqual(closed,[101])
            if not fault:self.assertEqual(caught.exception.code,'WITNESS_CLOSE_JOB')
        original=OSError('original trace observation failure');kernel,closed=self.fake(close_failure=True)
        with patch('eq142_identity.WitnessKernel',return_value=kernel),patch(
                'eq142_trace.fragment_clock_and_timelines',side_effect=original):
            with self.assertRaises(OSError) as caught:capture_owned_process(JOB)
        self.assertIs(caught.exception,original);self.assertEqual(closed,[101])
        self.assertTrue(any('close failed' in note for note in original.__notes__))


if __name__=='__main__':unittest.main()
