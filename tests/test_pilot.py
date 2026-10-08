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
import time
import importlib.util
from unittest.mock import patch
import unittest

from pilot_fixture import ORACLE, counts, make_fixture, isolated_sample
from pilot_memory import NativeMemory, MemorySampler, admission, quantile, simultaneous_peak
from pilot_process import OwnedProcess, communicate_owned


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
        owner=OwnedProcess()
        child=owner.start([sys.executable,'-I','-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
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
            owner.close(failed=child.poll() is None)
            child.communicate(timeout=5)

    def test_owned_timeout_and_terminal_root_cleanup_preserves_unrelated(self):
        flags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0
        sentinel_owner=OwnedProcess()
        sentinel=sentinel_owner.start([sys.executable,'-I','-c','import sys;sys.stdin.read()'],
            stdin=subprocess.PIPE,creationflags=flags)
        try:
            for terminal_root in (False,True):
                with self.subTest(terminal_root=terminal_root), tempfile.TemporaryDirectory() as temporary:
                    marker=Path(temporary)/'child.json'
                    child_code="import os,time,json;from pathlib import Path;Path("+repr(str(marker))+ ").write_text(json.dumps({'pid':os.getpid()}));time.sleep(60)"
                    root_code="import sys,subprocess,time;sys.stdin.read();subprocess.Popen([sys.executable,'-I','-c',"+repr(child_code)+"]);"+('time.sleep(.4)' if terminal_root else 'time.sleep(60)')
                    samplers=[]
                    def factory(process):
                        sampler=MemorySampler(process);samplers.append(sampler);return sampler
                    with self.assertRaises(subprocess.TimeoutExpired):
                        communicate_owned([sys.executable,'-I','-c',root_code],'go',timeout=2,
                            sampler_factory=factory,creationflags=flags)
                    self.assertTrue(marker.exists(),'owned child handshake did not execute')
                    descendant=json.loads(marker.read_text(encoding='utf-8'))['pid']
                    deadline=time.monotonic()+5
                    while descendant in NativeMemory().inventory() and time.monotonic()<deadline:time.sleep(.02)
                    self.assertNotIn(descendant,NativeMemory().inventory(),'owned child survived cleanup')
                    self.assertTrue(samplers[0].stop_event.is_set());self.assertFalse(samplers[0].thread.is_alive())
                    self.assertIsNone(sentinel.poll(),'unrelated process was terminated')
        finally:
            sentinel_owner.close(failed=True);sentinel.communicate(timeout=5)

    def test_owned_sampler_initialization_and_start_failures(self):
        flags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0
        for phase in ('constructor','start','interrupt'):
            at_start=phase!='constructor'
            with self.subTest(phase=phase):
                processes=[];samplers=[]
                class Failure(RuntimeError):pass
                def factory(process):
                    processes.append(process)
                    if not at_start:raise Failure('constructor failure')
                    sampler=MemorySampler(process);samplers.append(sampler)
                    def fail():
                        if phase=='interrupt':
                            sampler.thread.start();raise KeyboardInterrupt('interrupted initialization')
                        raise Failure('start failure')
                    sampler.start=fail;return sampler
                expected=KeyboardInterrupt if phase=='interrupt' else Failure
                with self.assertRaisesRegex(expected,'interrupted initialization' if phase=='interrupt' else ('start failure' if at_start else 'constructor failure')):
                    communicate_owned([sys.executable,'-I','-c','import sys;sys.stdin.read()'],'go',
                        timeout=2,sampler_factory=factory,creationflags=flags)
                self.assertIsNotNone(processes[0].returncode)
                for sampler in samplers:
                    self.assertTrue(sampler.stop_event.is_set());self.assertFalse(sampler.thread.is_alive())

    def test_existing_report_and_overlapping_paths_preserve_evidence(self):
        spec=importlib.util.spec_from_file_location('benchmark_pipeline',Path(__file__).parents[1]/'tools'/'benchmark_pipeline.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);report=root/'retained.json';report.write_bytes(b'retained evidence')
            with self.assertRaisesRegex(ValueError,'must be new'):
                module.evidence_locations(root/'fixture',root/'output',report)
            self.assertEqual(report.read_bytes(),b'retained evidence');self.assertFalse((root/'fixture').exists())
            with self.assertRaisesRegex(ValueError,'separate'):
                module.evidence_locations(root/'fixture',root/'fixture'/'outputs',root/'new.json')
            with self.assertRaisesRegex(ValueError,'tracked/source'):
                module.evidence_locations(root/'fixture',root/'outputs',Path(__file__).parents[1]/'tests'/'new-report.json')

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
