"""Windows actual thread joins plus inherited whole-Job/cleanup controls."""
import ctypes as ct
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_native import Kernel,ProcessEntry,NativeFailure,run_owned,NativeEvidenceBuffer
import test_supplied_native as original


@unittest.skipUnless(os.name=='nt','Windows Toolhelp32/Job qualification; no Linux native measurement claim')
class NativeThreadControls(unittest.TestCase):
    def test_evidence_bound_rejection_preserves_rows_and_cleans_owned_runner(self):
        evidence={'samples':[],'probe_errors':[]};buffer=NativeEvidenceBuffer(evidence)
        buffer.append('samples',{'known':1})
        before=buffer.encoded_bytes
        with patch('eq142_native.NATIVE_ROWS_MAX_BYTES',before):
            with self.assertRaises(NativeFailure) as error:buffer.append('samples',{'known':2})
        self.assertEqual(error.exception.code,'NATIVE_EVIDENCE_LIMIT')
        self.assertEqual(evidence['samples'],[{'known':1}]);self.assertEqual(buffer.encoded_bytes,before)
        with tempfile.TemporaryDirectory() as directory,patch('eq142_native.NATIVE_ROWS_MAX_BYTES',4):
            with self.assertRaises(NativeFailure) as error:
                run_owned([sys.executable,str(ROOT/'tests/eq142_native_child.py'),'--output',str(Path(directory)/'threads.json')],cwd=ROOT,timeout_s=10)
        self.assertEqual(error.exception.code,'NATIVE_EVIDENCE_LIMIT')
        actual=error.exception.evidence
        self.assertEqual(actual['samples'],[])
        self.assertTrue(actual['assigned_to_owned_job'])
        self.assertEqual(actual['cleanup_accounting']['active_processes'],0)
        self.assertNotIn('cleanup_failed',actual)

    def test_native_struct_and_actual_owned_member_thread_counts(self):
        self.assertEqual(ct.sizeof(ProcessEntry),568 if ct.sizeof(ct.c_void_p)==8 else 556)
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'threads.json'
            evidence=run_owned([sys.executable,str(ROOT/'tests/eq142_native_child.py'),'--output',str(output)],cwd=ROOT,timeout_s=10)
            self.assertEqual(evidence['exit_code'],0);self.assertEqual(evidence['final_accounting']['active_processes'],0)
            actual=json.loads(output.read_bytes());self.assertEqual(actual['python_threads'],4)
            rows=[row for sample in evidence['samples'] for row in sample['processes'] if row['pid']==actual['pid']]
            self.assertTrue(any(row['native_threads'] is not None and row['native_threads']>=actual['python_threads'] for row in rows))
            for sample in evidence['samples']:
                if all(row['native_threads'] is not None for row in sample['processes']):
                    self.assertEqual(sample['native_thread_count'],sum(row['native_threads'] for row in sample['processes']))
            self.assertGreaterEqual(evidence['aggregate_sampled_peak_native_threads'],4)
            self.assertEqual(evidence['probe_errors'],[])

    def fake(self,*,fault=None):
        kernel=object.__new__(Kernel);closed=[]
        kernel.OpenProcess=lambda *a:None if fault=='open' else 101
        def membership(handle,job,pointer):
            pointer._obj.value=fault!='foreign';return True
        kernel.IsProcessInJob=membership
        kernel.WaitForSingleObject=lambda *a:0 if fault=='exited' else 0xFFFFFFFF if fault=='wait' else 258
        kernel.CreateToolhelp32Snapshot=lambda *a:ct.c_void_p(-1).value if fault=='snapshot' else 202
        def first(handle,pointer):
            if fault=='first':return False
            self.assertEqual(pointer._obj.dwSize,ct.sizeof(ProcessEntry))
            pointer._obj.th32ProcessID=999 if fault=='missing' else 123
            pointer._obj.cntThreads=4
            return True
        kernel.Process32FirstW=first
        def next(handle,pointer):
            ct.set_last_error(5 if fault=='next' else 18);return False
        kernel.Process32NextW=next
        kernel.CloseHandle=lambda handle:closed.append(handle)
        return kernel,closed

    def test_snapshot_errors_membership_races_and_handle_cleanup(self):
        expected={'open':'OPEN_PROCESS','foreign':'MEMBERSHIP_RACE','wait':'THREAD_PROBE',
            'snapshot':'SNAPSHOT','first':'PROCESS_ENUMERATION','next':'PROCESS_ENUMERATION','missing':'MISSING_OWNED_PID'}
        for fault,code in expected.items():
            with self.subTest(fault=fault):
                kernel,closed=self.fake(fault=fault)
                with self.assertRaises(NativeFailure) as error:kernel.threads_for_members(1,[123])
                self.assertEqual(error.exception.code,code)
                if fault!='open':self.assertIn(101,closed)
                if fault in ('first','next','missing'):self.assertIn(202,closed)
                self.assertNotIn(ct.c_void_p(-1).value,closed)
        kernel,closed=self.fake();self.assertEqual(kernel.threads_for_members(1,[123]),{123:4})
        self.assertEqual(sorted(closed),[101,202])
        kernel,closed=self.fake(fault='exited')
        self.assertEqual(kernel.threads_for_members(1,[123]),{123:0})
        self.assertEqual(kernel.last_thread_dispositions[123],{'state':'observed_terminated','terminal_wait':0,'snapshot_threads':4})
        self.assertEqual(sorted(closed),[101,202])


@unittest.skipUnless(os.name=='nt','Windows owned Job; no Linux native measurement claim')
class ExtendedOwnedJob(original.SuppliedNative):
    def setUp(self):
        self.use_extension=patch.object(original,'run_owned',run_owned)
        self.use_extension.start();self.addCleanup(self.use_extension.stop)


if __name__=='__main__':unittest.main()
