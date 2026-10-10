"""Actual owned Service/native execution and noncooperative epoch limits."""
import ctypes
import errno
import hashlib
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from equity_feature_service import _containment
from equity_feature_service._containment import OwnedEpochSupervisor
from equity_feature_service._entry import OwnedEntryInventory, OwnedEntryProfile, startup_frame


class Containment(unittest.TestCase):
    def native_entry(self, callback):
        """Trusted owned fixture; callback is never HTTP-selected code."""
        source = str(Path(__file__).resolve().parent)
        return ('import sys,os,time,json\nsys.path.insert(0,'+repr(source)+')\n'
            'from test_jobs import setup,Jobs\n'
            'from equity_feature_service._audit import OwnedAudit\n'
            'def callback(value):\n'+callback+'\n'
            'audit=OwnedAudit(owned_synthetic=True,valid_from_ns=0,expires_at_ns=300_000_000_000)\n'
            'scheduler,service,tokens,clock,facts=setup(transform=callback,audit_store=audit)\n'
            'job_id=Jobs().submit(service,tokens[0])\n'
            'while scheduler.jobs[job_id].state in ("queued","running","cancel_requested"):\n time.sleep(.01)\n'
            'job=scheduler.jobs[job_id]\n'
            'print(json.dumps({"state":job.state,"native_bytes":len(job.native),"wire_bytes":len(job.wire),'
            '"receipt":job.committed_receipt_sha256 is not None,"reads":facts["reads"]}),flush=True)\n'
            'assert scheduler.close(2)\n')

    def launch(self, code, **kwargs):
        source = code.encode('utf-8')
        profile = OwnedEntryProfile('native-fixture',source,hashlib.sha256(source).hexdigest(),(),True)
        return OwnedEpochSupervisor(OwnedEntryInventory((profile,))).run_admitted('native-fixture',**kwargs)

    def test_actual_service_native_three_families_and_owner_thread_goldens(self):
        # The trusted qualification entry is a frozen source file, not an HTTP
        # selector. Test helpers are imported only after child OS containment.
        source = str(Path(__file__).resolve().parent)
        code = "import sys,unittest\nsys.path.insert(0,"+repr(source)+")\n"
        code += "from test_jobs import Jobs\nsuite=unittest.TestSuite([Jobs('test_three_native_families_match_independent_frozen_full_results'),Jobs('test_native_worker_completion_full_goldens_and_thread_ownership')])\n"
        code += "result=unittest.TextTestRunner().run(suite)\nraise SystemExit(0 if result.wasSuccessful() else 1)\n"
        result = self.launch(code)
        self.assertEqual((result.exit_code,result.reason),(0,'exited'),result.output)
        self.assertIn(b'Ran 2 tests',result.output)

    def test_wall_deadline_kills_noncooperative_child_then_reuses_slot(self):
        supervisor = OwnedEpochSupervisor()
        with tempfile.TemporaryDirectory() as temporary:
            entry = Path(temporary)/'owned.py'
            entry.write_text(self.native_entry(' os.write(1,b"native-callback-entered\\n")\n while True: pass'),encoding='utf-8')
            source=entry.read_bytes()
            registered=OwnedEpochSupervisor(OwnedEntryInventory((OwnedEntryProfile('wall-native',source,hashlib.sha256(source).hexdigest(),(),True),)))
            result = registered.run_admitted('wall-native',wall_seconds=3)
            self.assertEqual(result.reason,'deadline')
            self.assertNotEqual(result.exit_code,0)
            self.assertIn(b'native-callback-entered',result.output)
            self.assertLess(result.elapsed_seconds,6)
            entry.write_text('import os\nos.write(1,b"next-epoch-explicit\\n")',encoding='utf-8')
            self.assertEqual(supervisor.run(entry).output,b'next-epoch-explicit\n')

    def test_combined_capture_has_exact_inclusive_boundary(self):
        profile = OwnedEntryProfile('native-fixture',b'pass',hashlib.sha256(b'pass').hexdigest(),(),True)
        startup_bytes = len(startup_frame('a'*32,'a'*32,profile))
        maximum = 65536-startup_bytes
        result = self.launch('import os\nos.write(1,b"a"*'+str(maximum-1)+')\nos.write(2,b"b")')
        self.assertEqual((result.reason,len(result.output)),('exited',maximum))
        result = self.launch('import os\nos.write(1,b"a"*'+str(maximum)+')\nos.write(2,b"b")\nwhile True: pass')
        self.assertEqual((result.reason,len(result.output)),('output_limit',maximum))

    def test_process_creation_denied_but_threads_work(self):
        code = self.native_entry(' import subprocess,threading\n t=threading.Thread(target=lambda:None)\n t.start();t.join()\n'
            ' try:\n  p=subprocess.Popen([sys.executable,"-I","-c","print(123)"]);p.wait(timeout=2)\n'
            ' except OSError:\n  os.write(1,b"denied\\n")\n else:\n  raise RuntimeError("escaped")\n return value')
        result = self.launch(code)
        self.assertEqual((result.exit_code,result.reason),(0,'exited'),result)
        self.assertTrue(result.output.startswith(b'denied\n'),result.output)
        self.assertIn(b'"state": "succeeded"',result.output)
        self.assertIn(b'"reads": 1',result.output)

    def test_actual_native_memory_allocation_denied_without_visible_forms(self):
        result = self.launch(self.native_entry(' value=bytearray(5_000_000_000)\n return value'))
        # The OS denies the native callback allocation; accepted worker failure
        # handling may keep this epoch alive, but must expose no producer forms.
        self.assertEqual((result.exit_code,result.reason),(0,'exited'),result.output)
        self.assertIn(b'"state": "failed"',result.output)
        self.assertIn(b'"native_bytes": 0',result.output)
        self.assertIn(b'"wire_bytes": 0',result.output)
        self.assertIn(b'"receipt": false',result.output)
        self.assertIn(b'"reads": 1',result.output)

    def test_cpu_budget_terminates_without_cooperation(self):
        result = self.launch(self.native_entry(' os.write(1,b"native-cpu-entered\\n")\n while True: pass'),wall_seconds=25)
        self.assertEqual(result.reason,'child_failed',result)
        self.assertNotEqual(result.exit_code,0)
        self.assertIn(b'native-cpu-entered',result.output)
        self.assertLess(result.elapsed_seconds,23)

    def test_actual_native_commit_witness_survives_epoch_death_without_visible_result_or_reexecution(self):
        from equity_feature_io_sdk import decode_receipt, encode_receipt
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root/'historical-receipt.json'
            code = self.native_entry(' return value')
            owned_sink = ('from pathlib import Path\n'
                'from equity_feature_example_extensions import ExampleSink\n'
                'from equity_feature_io_sdk import encode_receipt\n'
                'class AfterCommit(ExampleSink):\n'
                ' def commit(self,session):\n'
                '  receipt=super().commit(session)\n'
                '  with Path('+repr(str(marker))+').open("xb") as witness:witness.write(encode_receipt(receipt))\n'
                '  os.write(1,b"native-committed-witnessed\\n")\n'
                '  while True:time.sleep(.01)\n')
            code = code.replace('audit=OwnedAudit',owned_sink+'audit=OwnedAudit')
            code = code.replace('setup(transform=callback,audit_store=audit)',
                'setup(transform=callback,audit_store=audit,sink_create=AfterCommit)')
            result = self.launch(code,wall_seconds=5)
            self.assertEqual(result.reason,'deadline',result.output)
            self.assertIn(b'native-committed-witnessed',result.output)
            self.assertNotIn(b'"state"',result.output,'blocked commit must not return a successful service result')
            receipt_bytes = marker.read_bytes()
            receipt = decode_receipt(receipt_bytes)
            self.assertEqual(receipt.result_count,1)
            self.assertGreater(receipt.content_bytes,0)
            self.assertEqual(encode_receipt(receipt),receipt_bytes)
            time.sleep(.1)
            self.assertEqual(marker.read_bytes(),receipt_bytes)
            self.assertFalse(OwnedEpochSupervisor()._capacity.locked())
            # This owned external witness is the returned native receipt, not a
            # durable native payload or recovery store. ExampleSink's actual
            # in-memory committed artifact dies with the epoch; no rollback,
            # persistence/restart or post-death result-download claim follows.

    def test_running_epoch_holds_capacity_until_real_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            entry = Path(temporary)/'owned.py'
            entry.write_text('import time\ntime.sleep(2)',encoding='utf-8')
            supervisor = OwnedEpochSupervisor()
            outcome = []
            thread = threading.Thread(target=lambda:outcome.append(supervisor.run(entry,wall_seconds=0.5)))
            thread.start()
            deadline = time.monotonic()+2
            while not supervisor._capacity.locked() and time.monotonic()<deadline:
                time.sleep(0.01)
            with self.assertRaisesRegex(RuntimeError,'epoch_capacity'):
                OwnedEpochSupervisor().run(entry)
            thread.join(4)
            self.assertFalse(thread.is_alive())
            self.assertEqual(outcome[0].reason,'deadline')

    def test_invalid_profiles_fail_before_launch(self):
        for fields in ({'wall_seconds':31},{'wall_seconds':True},{'output_limit':65537},{'output_limit':True}):
            with self.subTest(fields=fields),self.assertRaises(ValueError):
                self.launch('raise AssertionError("executed")',**fields)

    def test_output_reader_start_failure_closes_capture_and_releases_dead_epoch(self):
        original = _containment._WindowsChild if sys.platform == 'win32' else subprocess.Popen
        streams = []
        def observe(*args,**kwargs):
            child = original(*args,**kwargs)
            streams.append(child.output if sys.platform == 'win32' else child.stdout)
            return child
        target = 'equity_feature_service._containment._WindowsChild' if sys.platform == 'win32' else 'subprocess.Popen'
        with patch(target,side_effect=observe),patch('threading.Thread.start',side_effect=RuntimeError('owned-reader-start-fault')):
            with self.assertRaisesRegex(RuntimeError,'owned-reader-start-fault'):
                self.launch('while True: pass')
        self.assertEqual(len(streams),1)
        self.assertTrue(streams[0].closed)
        self.assertFalse(OwnedEpochSupervisor()._capacity.locked())
        result = self.launch('import os\nos.write(1,b"new-explicit-epoch")')
        self.assertEqual((result.reason,result.output),('exited',b'new-explicit-epoch'))

    @unittest.skipUnless(sys.platform == 'win32','Windows HANDLE-to-CRT ownership boundary')
    def test_capture_stream_wrapper_failure_closes_transferred_crt_descriptor(self):
        descriptors = []
        def fail(fd,*args,**kwargs):
            descriptors.append(fd)
            raise RuntimeError('owned-capture-wrapper-fault')
        with patch('os.fdopen',side_effect=fail):
            with self.assertRaisesRegex(RuntimeError,'owned-capture-wrapper-fault'):
                self.launch('while True: pass')
        self.assertEqual(len(descriptors),1)
        with self.assertRaises(OSError) as caught:
            os.fstat(descriptors[0])
        self.assertEqual(caught.exception.errno,errno.EBADF)
        self.assertFalse(OwnedEpochSupervisor()._capacity.locked())
        self.assertEqual(self.launch('import os\nos.write(1,b"next")').output,b'next')

    def test_original_supervisor_death_kills_blocked_child(self):
        # Owned marker proves child startup, not stdout forwarding/persistence.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker, entry, parent = root/'pid',root/'owned.py',root/'parent.py'
            entry.write_text('import os,time\nfrom pathlib import Path\nPath('+repr(str(marker))+').write_text(str(os.getpid()))\nwhile True: time.sleep(.01)',encoding='utf-8')
            parent.write_text('from pathlib import Path\nfrom equity_feature_service._containment import OwnedEpochSupervisor\nOwnedEpochSupervisor().run(Path('+repr(str(entry))+'))',encoding='utf-8')
            outer = subprocess.Popen([sys.executable,'-I',str(parent)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            handle = None
            child_pid = None
            try:
                deadline = time.monotonic()+5
                while not marker.exists() and time.monotonic()<deadline:
                    self.assertIsNone(outer.poll(),'supervisor exited before child startup')
                    time.sleep(.01)
                self.assertTrue(marker.exists())
                child_pid = int(marker.read_text())
                if sys.platform == 'win32':
                    from ctypes import wintypes as w
                    kernel = ctypes.WinDLL('kernel32',use_last_error=True)
                    kernel.OpenProcess.argtypes = [w.DWORD,w.BOOL,w.DWORD]
                    kernel.OpenProcess.restype = w.HANDLE
                    kernel.WaitForSingleObject.argtypes = [w.HANDLE,w.DWORD]
                    kernel.WaitForSingleObject.restype = w.DWORD
                    kernel.CloseHandle.argtypes = [w.HANDLE]
                    handle = kernel.OpenProcess(0x100000,False,child_pid)
                    self.assertTrue(handle)
                outer.kill()
                outer.wait(timeout=3)
                if sys.platform == 'win32':
                    self.assertEqual(kernel.WaitForSingleObject(handle,3000),0)
                else:
                    deadline = time.monotonic()+3
                    while time.monotonic()<deadline:
                        stat = Path('/proc')/str(child_pid)/'stat'
                        if not stat.exists() or stat.read_text().split(') ',1)[1].split()[0] == 'Z':
                            break
                        time.sleep(.01)
                    else: self.fail('original-parent death did not terminate child')
            finally:
                if outer.poll() is None: outer.kill()
                outer.wait(timeout=3)
                if handle: kernel.CloseHandle(handle)
                if child_pid is not None and sys.platform == 'linux':
                    try: os.kill(child_pid,9)
                    except ProcessLookupError: pass


if __name__ == '__main__': unittest.main()
