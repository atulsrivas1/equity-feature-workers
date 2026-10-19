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
        def creation(handle,created,*unused):
            created._obj.dwLowDateTime=123;created._obj.dwHighDateTime=456;return True
        kernel.GetProcessTimes=creation
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
        def close(handle):closed.append(handle);return True
        kernel.CloseHandle=close
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
        self.assertEqual(kernel.last_thread_dispositions[123],{'state':'observed_terminated','terminal_wait':0,
            'snapshot_threads':4,'creation_filetime_100ns_since_1601_utc':str((456<<32)|123)})
        self.assertEqual(sorted(closed),[101,202])

    def test_creation_api_unknown_and_lifetime_drift_reject_without_handle_leaks(self):
        original=str((456<<32)|123)
        for fault in ('api','zero','drift','expected'):
            with self.subTest(fault=fault):
                kernel,closed=self.fake();calls=[]
                def creation(handle,created,*unused):
                    calls.append(handle);created._obj.dwLowDateTime=0 if fault=='zero' else 123
                    created._obj.dwHighDateTime=0 if fault=='zero' else 456
                    if fault=='drift' and len(calls)==2:created._obj.dwLowDateTime+=1
                    return fault!='api'
                kernel.GetProcessTimes=creation
                expected='PROCESS_CREATION_API' if fault=='api' else 'PROCESS_CREATION_UNKNOWN' if fault=='zero' else 'PROCESS_LIFETIME_RACE'
                with self.assertRaises(NativeFailure) as caught:
                    kernel.threads_for_members(1,[123],{123:'1' if fault=='expected' else original})
                self.assertEqual(caught.exception.code,expected);self.assertIn(101,closed)
                self.assertEqual(closed,[202,101] if fault=='drift' else [101])
        kernel,closed=self.fake()
        def high(handle,created,*unused):
            created._obj.dwLowDateTime=4294967295;created._obj.dwHighDateTime=4294967295;return True
        kernel.GetProcessTimes=high
        self.assertEqual(kernel.creation_identity(101),'18446744073709551615')
        for proposed in ({},{123:'0'},{123:'18446744073709551616'},{123:True},{124:original}):
            with self.assertRaises(NativeFailure):kernel.threads_for_members(1,[123],proposed)
        for pids in ([True],[0],[4294967296],[123,123],list(range(1,258))):
            with self.assertRaises(NativeFailure):kernel.threads_for_members(1,pids)
        with self.assertRaises(NativeFailure):kernel.threads_for_members(1,[1],{True:original})
        self.assertEqual(closed,[])

    def test_actual_held_child_witness_matches_same_handle_rss_and_thread_lifetime(self):
        original=Kernel.threads_for_members;matches=[]
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'witness.json';release=Path(directory)/'release'
            def observed(kernel,job,pids,expected_lifetimes=None):
                counts=original(kernel,job,pids,expected_lifetimes)
                if output.exists() and not release.exists():
                    record=json.loads(output.read_bytes());pid=record['pid']
                    if pid in pids:
                        creation=record['creation_filetime_100ns_since_1601_utc']
                        self.assertEqual(expected_lifetimes[pid],creation)
                        self.assertEqual(kernel.last_thread_dispositions[pid]['creation_filetime_100ns_since_1601_utc'],creation)
                        self.assertEqual(kernel.last_thread_dispositions[pid]['state'],'observed_live')
                        self.assertGreater(counts[pid],0);matches.append((pid,creation));release.write_bytes(b'')
                return counts
            with patch.object(Kernel,'threads_for_members',observed):
                evidence=run_owned([sys.executable,str(ROOT/'tests/eq142_identity_child.py'),
                    '--job','{job_name}','--output',str(output),'--release',str(release)],cwd=ROOT,timeout_s=15)
            self.assertEqual(evidence['exit_code'],0);self.assertEqual(evidence['probe_errors'],[])
            self.assertEqual(evidence['final_accounting']['active_processes'],0)
            self.assertEqual(len(matches),1);pid,creation=matches[0]
            rows=[row for sample in evidence['samples'] for row in sample['processes'] if row['pid']==pid]
            self.assertTrue(rows)
            self.assertTrue(all(row['creation_filetime_100ns_since_1601_utc']==creation for row in rows))
            self.assertIn({'pid':pid,'creation_filetime_100ns_since_1601_utc':creation},evidence['observed_lifetimes'])


class ObservedLifetimeBounds(unittest.TestCase):
    def test_distinct_reused_pid_lifetimes_bound_and_exact_metadata(self):
        evidence={'samples':[],'probe_errors':[]};buffer=NativeEvidenceBuffer(evidence)
        for value in range(1,257):buffer.observe_lifetime(123,str(value))
        before=json.dumps(evidence,sort_keys=True);self.assertEqual(len(evidence['observed_lifetimes']),256)
        buffer.observe_lifetime(123,'1');self.assertEqual(json.dumps(evidence,sort_keys=True),before)
        with self.assertRaises(NativeFailure) as caught:buffer.observe_lifetime(123,'257')
        self.assertEqual(caught.exception.code,'PROCESS_LIFETIME_LIMIT')
        self.assertEqual(json.dumps(evidence,sort_keys=True),before)
        self.assertLessEqual(len(json.dumps(evidence['observed_lifetimes']).encode()),65536)
        for pid,creation in ((True,'1'),(123,'0'),(123,'01'),(123,'18446744073709551616')):
            with self.assertRaises(NativeFailure):buffer.observe_lifetime(pid,creation)


@unittest.skipUnless(os.name=='nt','Windows owned Job; no Linux native measurement claim')
class ExtendedOwnedJob(original.SuppliedNative):
    def setUp(self):
        self.use_extension=patch.object(original,'run_owned',run_owned)
        self.use_extension.start();self.addCleanup(self.use_extension.stop)


if __name__=='__main__':unittest.main()
