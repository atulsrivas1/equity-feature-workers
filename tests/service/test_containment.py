"""Actual owned Service/native execution and noncooperative epoch limits."""
import ctypes
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


class Containment(unittest.TestCase):
    def launch(self, code, **kwargs):
        with tempfile.TemporaryDirectory() as temporary:
            entry = Path(temporary)/'owned.py'
            entry.write_text(code,encoding='utf-8')
            return OwnedEpochSupervisor().run(entry,**kwargs)

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
            entry.write_text('while True: pass',encoding='utf-8')
            result = supervisor.run(entry,wall_seconds=0.3)
            self.assertEqual(result.reason,'deadline')
            self.assertNotEqual(result.exit_code,0)
            self.assertLess(result.elapsed_seconds,3.3)
            entry.write_text('import os\nos.write(1,b"next-epoch-explicit\\n")',encoding='utf-8')
            self.assertEqual(supervisor.run(entry).output,b'next-epoch-explicit\n')

    def test_combined_capture_has_exact_inclusive_boundary(self):
        result = self.launch('import os\nos.write(1,b"a"*32768)\nos.write(2,b"b"*32768)')
        self.assertEqual((result.reason,len(result.output)),('exited',65536))
        result = self.launch('import os\nos.write(1,b"a"*65536)\nos.write(2,b"b")\nwhile True: pass')
        self.assertEqual((result.reason,len(result.output)),('output_limit',65536))

    def test_process_creation_denied_but_threads_work(self):
        code = 'import subprocess,sys,threading\nt=threading.Thread(target=lambda:None)\nt.start();t.join()\n'
        code += 'try:\n p=subprocess.Popen([sys.executable,"-I","-c","print(123)"]);p.wait(timeout=2)\nexcept OSError:\n import os;os.write(1,b"denied\\n")\nelse:\n raise RuntimeError("escaped")\n'
        result = self.launch(code)
        self.assertEqual((result.reason,result.output),('exited',b'denied\n'),result)

    def test_memory_exhaustion_is_child_failure_without_parent_growth(self):
        result = self.launch('value=bytearray(5_000_000_000)')
        self.assertNotEqual(result.exit_code,0)
        self.assertEqual(result.reason,'child_failed')
        self.assertIn(b'MemoryError',result.output)

    def test_cpu_budget_terminates_without_cooperation(self):
        result = self.launch('while True: pass',wall_seconds=25)
        self.assertEqual(result.reason,'child_failed',result)
        self.assertNotEqual(result.exit_code,0)
        self.assertLess(result.elapsed_seconds,23)

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
