"""Independent frozen arithmetic, native resident allocation and physical parity."""
from fractions import Fraction
import json
import os
import queue
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

from pilot_fixture import ORACLE, counts, make_fixture, isolated_sample
from pilot_memory import NativeMemory, admission, quantile, simultaneous_peak


class Pilot(unittest.TestCase):
    def test_frozen_independent_oracle_and_quantiles(self):
        pair=ORACLE['pair']
        self.assertEqual(sum(v['price']*v['size'] for v in pair),506)
        self.assertEqual(sum(v['size'] for v in pair),5)
        self.assertEqual(Fraction(506,5),Fraction(ORACLE['per_pair']['vwap_numerator'],ORACLE['per_pair']['vwap_denominator']))
        for workload in ('small','large','skew'):
            self.assertEqual(sum(counts(workload)),ORACLE['workloads'][workload]['total_rows'])
        self.assertEqual(quantile([10,20,30,40],.5),25)
        self.assertAlmostEqual(quantile([10,20,30,40],.95),38.5)
        self.assertEqual(simultaneous_peak([{'a':100,'b':50},{'a':80,'b':100}]),180)
        self.assertNotEqual(simultaneous_peak([{'a':100,'b':50},{'a':80,'b':100}]),200)

    def test_admission_unavailable_ram_and_cpu_are_distinct(self):
        self.assertIn('RAM observation unavailable',admission({'admitted_cpu_capacity':9,'available_physical_bytes':None},8))
        self.assertIn('slot',admission({'admitted_cpu_capacity':2,'available_physical_bytes':10**10},2))
        self.assertIn('heuristic',admission({'admitted_cpu_capacity':9,'available_physical_bytes':10**9},8))
        self.assertIsNone(admission({'admitted_cpu_capacity':9,'available_physical_bytes':3221225472},8))

    def test_native_counter_independent_child_allocation_handshake(self):
        # A separate stdlib child has no source/native engine imports or forced exit.
        code="""import os,sys
print('before '+str(os.getpid()),flush=True);sys.stdin.readline()
resident=bytearray(64*1024*1024)
for offset in range(0,len(resident),4096):resident[offset]=1
print('after '+str(os.getpid()),flush=True);sys.stdin.readline()
"""
        child=subprocess.Popen([sys.executable,'-I','-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,text=True,encoding='utf-8',creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
        try:
            replies=queue.Queue()
            def read():
                for line in child.stdout:replies.put(line.strip())
            reader=threading.Thread(target=read,daemon=True);reader.start()
            before=replies.get(timeout=15).split();self.assertEqual(before[0],'before')
            # Windows venv python.exe can be a launcher. Measure the actual allocator,
            # not its constant-size launcher; the pipeline sampler includes both.
            allocator=int(before[1]);native=NativeMemory();creation,pre,_=native.read(allocator)
            child.stdin.write('allocate\n');child.stdin.flush()
            after=replies.get(timeout=15).split();self.assertEqual(after,['after',str(allocator)])
            same,post,high=native.read(allocator,creation)
            self.assertEqual(creation,same);self.assertGreaterEqual(post-pre,48*1024*1024)
            self.assertGreaterEqual(high,post)
            child.stdin.write('exit\n');child.stdin.flush();child.communicate(timeout=15)
            self.assertEqual(child.returncode,0)
        finally:
            if child.poll() is None:child.terminate();child.communicate(timeout=15)

    def test_actual_source_both_sinks_all_modes_full_encoded_parity(self):
        import equity_feature_workers
        installed='site-packages' in Path(equity_feature_workers.__file__).resolve().parts
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);initial=make_fixture(root/'fixture','small');baseline=None
            for backend in ('parquet','duckdb'):
                for mode in ('sequential','thread','process'):
                    sample=isolated_sample(root/'fixture',root/(backend+'-'+mode),'small',backend,mode,1,installed)
                    if baseline is None:baseline=sample['content_sha256']
                    self.assertEqual(sample['content_sha256'],baseline)
                    self.assertEqual(sample['input_population'],initial)
                    self.assertEqual(sample['rows'],16)
                    self.assertEqual(sample['reporter_attempts'],16)
                    self.assertEqual(sample['reporter_reserved_bytes'],53760)
                    self.assertEqual(len(sample['task_latency_ns']),4)
                    self.assertGreater(sample['memory']['sampled_job_peak_bytes'],0)
                    self.assertGreater(sample['memory']['root_samples'],0)
                    self.assertTrue(sample['independent_five_trade_goldens'])


if __name__=='__main__':unittest.main()
