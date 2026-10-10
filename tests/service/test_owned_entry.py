"""Independent closed-startup admission and resource ownership adversaries."""
from dataclasses import FrozenInstanceError
import ctypes
import errno
import hashlib
import subprocess
import sys
import time
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from equity_feature_service import _containment
from equity_feature_service._containment import OwnedEpochSupervisor
from equity_feature_service._entry import (OwnedEntryInventory, OwnedEntryProfile,
    canonical, startup_ack, startup_frame, validate_startup)


def profile(source=b'print("native-started",flush=True)', profile_id='owned', arguments=()):
    return OwnedEntryProfile(profile_id,source,hashlib.sha256(source).hexdigest(),arguments,True)


class OwnedEntry(unittest.TestCase):
    def supervisor(self, source=b'print("native-started",flush=True)'):
        return OwnedEpochSupervisor(OwnedEntryInventory((profile(source),)))

    def test_literal_source_argument_inventory_bounds_and_immutability(self):
        source=b'#'+b'a'*8191
        self.assertEqual(len(profile(source).source),8192)
        for value in (b'#'+b'a'*8192,b'',b'\xff',b'if:',b'\x00'):
            with self.assertRaises(ValueError): profile(value)
        self.assertEqual(len(canonical(('a'*4092,))),4096)
        self.assertEqual(profile(arguments=('a'*4092,)).arguments,('a'*4092,))
        with self.assertRaises(ValueError):profile(arguments=('a'*4093,))
        valid=profile()
        with self.assertRaises(ValueError):OwnedEntryProfile('owned',valid.source,'0'*64,(),True)
        for owned in (False,1,'yes'):
            with self.assertRaises(ValueError):OwnedEntryProfile('owned',valid.source,valid.source_sha256,(),owned)
        for name in ('','a'*65,'foreign/path','nonascii-\u03b1'):
            with self.assertRaises(ValueError):profile(profile_id=name)
        four=tuple(profile(profile_id=str(n)) for n in range(4))
        inventory=OwnedEntryInventory(four)
        with self.assertRaises(FrozenInstanceError):valid.source=b'other'
        with self.assertRaises(FrozenInstanceError):inventory.profiles=()
        for items in ((),four+(profile(profile_id='4'),),(valid,valid),list(four)):
            with self.assertRaises(ValueError):OwnedEntryInventory(items)
        for identifier in ('missing',None,1):
            with self.assertRaises(ValueError):inventory.select(identifier)

    def test_complete_frame_and_ack_literal_shape_bounds(self):
        p=profile()
        expected=(b'{"challenge":"'+b'c'*32+b'","epoch":"'+b'e'*32+
            b'","profile":"owned","schema":"owned.startup.v1","source_sha256":"'+
            hashlib.sha256(p.source).hexdigest().encode()+b'"}\n')
        self.assertEqual(startup_frame('e'*32,'c'*32,p),expected)
        self.assertEqual(startup_ack('c'*32),b'ACK:'+b'c'*32+b'\n')
        validate_startup(expected,expected)
        validate_startup(b'a'*512,b'a'*512)
        for frame in (b'',expected+b'\n',expected+expected,b'a'*513):
            with self.assertRaises(ValueError):validate_startup(frame,expected)

    def test_registered_legacy_bypass_and_frozen_file_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'owned.py'
            path.write_bytes(b'import sys\nprint(sys.argv[1],flush=True)')
            registration=profile(path.read_bytes(),arguments=('original',))
            supervisor=OwnedEpochSupervisor(OwnedEntryInventory((registration,)))
            path.write_bytes(b'print("replacement")')
            with self.assertRaises(ValueError):supervisor.run(path)
            with self.assertRaises(ValueError):supervisor.run_admitted('missing')
            result=supervisor.run_admitted('owned')
            self.assertEqual((result.exit_code,result.reason),(0,'exited'),result.output)
            self.assertEqual(result.output.strip(),b'original')
            with self.assertRaises(AttributeError):supervisor.inventory=None

    def test_wrong_duplicate_empty_oversize_and_unclosed_startup_no_factory(self):
        original=_containment._ADMITTED_BOOTSTRAP
        for mutation in ("frame=b''", "frame=b'wrong\\n'", 'frame=frame+frame',
                         "frame=b'x'*513"):
            with self.subTest(mutation=mutation):
                bootstrap=original.replace("fd=m['startup_endpoint']",mutation+"\nfd=m['startup_endpoint']")
                with patch.object(_containment,'_ADMITTED_BOOTSTRAP',bootstrap):
                    result=self.supervisor().run_admitted('owned',wall_seconds=2)
                self.assertEqual(result.reason,'startup_failure',result)
                self.assertNotIn(b'native-started',result.output)
                self.assertEqual(self.supervisor().run_admitted('owned').exit_code,0)
        # A valid first frame without EOF must not be ACKed or run factories.
        bootstrap=original.replace('finally: os.close(fd)','finally:\n while True: pass')
        with patch.object(_containment,'_ADMITTED_BOOTSTRAP',bootstrap):
            result=self.supervisor().run_admitted('owned',wall_seconds=1)
        self.assertEqual(result.reason,'deadline')
        self.assertNotIn(b'native-started',result.output)
        self.assertEqual(self.supervisor().run_admitted('owned').exit_code,0)

    def test_wrong_closed_ack_and_source_digest_drift_prevent_factory(self):
        for ack in (b'',b'ACK:wrong\n',b'x'*66):
            with patch.object(_containment,'startup_ack',return_value=ack):
                result=self.supervisor().run_admitted('owned')
            self.assertNotEqual(result.exit_code,0)
            self.assertNotIn(b'native-started',result.output)
        original=_containment._ADMITTED_BOOTSTRAP
        with patch.object(_containment,'_ADMITTED_BOOTSTRAP',original.replace(
                "source=base64.b64decode(m['source'],validate=True)","source=b'print(123)'") ):
            result=self.supervisor().run_admitted('owned')
        self.assertEqual(result.reason,'startup_failure')
        self.assertNotIn(b'native-started',result.output)

    def test_aggregate_output_charges_startup_and_later_diagnostics(self):
        with patch.object(_containment.secrets,'token_hex',return_value='a'*32):
            p=profile(b'import os\nos.write(1,b"x"*100)')
            size=len(startup_frame('a'*32,'a'*32,p))
            supervisor=OwnedEpochSupervisor(OwnedEntryInventory((p,)))
            result=supervisor.run_admitted('owned',output_limit=size+100)
            self.assertEqual((result.reason,len(result.output)),('exited',100),result)
            result=supervisor.run_admitted('owned',output_limit=size+99)
            self.assertEqual((result.reason,len(result.output)),('output_limit',99),result)
        result=self.supervisor().run_admitted('owned',output_limit=1)
        self.assertEqual(result.reason,'output_limit')
        self.assertNotIn(b'native-started',result.output)

    def test_late_startup_diagnostic_is_postadmission_fault(self):
        source=b'import os,time\nos.write(1,b\'{"schema":"owned.startup.v1"}\')\ntime.sleep(2)'
        result=self.supervisor(source).run_admitted('owned')
        self.assertEqual(result.reason,'startup_failure')
        self.assertIn(b'owned.startup.v1',result.output)

    def test_startup_reader_start_failure_observes_exit_closes_then_next_epoch(self):
        actual=threading.Thread.start
        def fail(reader):
            if reader.name=='owned-epoch-startup':raise RuntimeError('injected-startup-reader')
            return actual(reader)
        with patch.object(threading.Thread,'start',fail):
            with self.assertRaisesRegex(RuntimeError,'injected-startup-reader'):
                self.supervisor().run_admitted('owned')
        self.assertFalse(_containment._EPOCH_CAPACITY.locked())
        self.assertEqual(self.supervisor().run_admitted('owned').exit_code,0)

    def test_launch_metadata_and_windows_command_line_bounded_before_factory(self):
        self.assertEqual(len(_containment._launch_metadata({'x':'a'*16376})),16384)
        with self.assertRaises(ValueError):_containment._launch_metadata({'x':'a'*16377})
        with patch.object(_containment.sys,'platform','unsupported'):
            with self.assertRaisesRegex(RuntimeError,'unsupported_host'):self.supervisor().run_admitted('owned')
        self.assertFalse(_containment._EPOCH_CAPACITY.locked())

    def test_actual_owned_audited_three_family_inventory_golden_results(self):
        helper=str(Path(__file__).resolve().parent)
        entries=[]
        for family in ('trades','bars','quotes'):
            source=('import sys,hashlib\nsys.path.insert(0,'+repr(helper)+')\n'
                'from test_jobs import setup,Jobs,FROZEN\n'
                'from equity_feature_service._audit import OwnedAudit\n'
                'audit=OwnedAudit(owned_synthetic=True,valid_from_ns=0,expires_at_ns=300_000_000_000)\n'
                'scheduler,service,tokens,clock,facts=setup(family='+repr(family)+',audit_store=audit)\n'
                'try:\n job=Jobs().wait_terminal(scheduler,Jobs().submit(service,tokens[0]))\n'
                ' assert service.ledger.audit is audit\n assert job.state=="succeeded",job.error\n'
                ' expected=next(r for r in FROZEN["records"] if r.get("family")=='+repr(family)+')\n'
                ' assert hashlib.sha256(job.native).hexdigest()==expected["native_result_sha256"]\n'
                ' print('+repr(family+'-full-golden')+',flush=True)\n'
                'finally: assert scheduler.close(2)\n').encode()
            entries.append(profile(source,profile_id=family))
        supervisor=OwnedEpochSupervisor(OwnedEntryInventory(tuple(entries)))
        for family in ('trades','bars','quotes'):
            result=supervisor.run_admitted(family)
            self.assertEqual((result.reason,result.exit_code),('exited',0),result.output)
            self.assertEqual(result.output.strip(),(family+'-full-golden').encode())

    def test_new_epoch_does_not_resurrect_job_cache_or_audit_records(self):
        helper=str(Path(__file__).resolve().parent)
        source=('import sys,json\nsys.path.insert(0,'+repr(helper)+')\n'
            'import test_delivery as d\nfrom test_jobs import Jobs\n'
            'from test_service import call\nfrom equity_feature_service._audit import OwnedAudit\n'
            'audit=OwnedAudit(owned_synthetic=True,valid_from_ns=0,expires_at_ns=300_000_000_000)\n'
            'owner=d.Delivery()\n scheduler,service,tokens,clock,facts,job=owner.make(audit_store=audit)\n').replace('\n scheduler','\nscheduler')
        source+=('try:\n incoming=sys.argv[1]\n'
            ' if incoming!="none":\n'
            '  status,body,*_=call(service,owner.request(incoming),tokens[0])\n'
            '  assert status==403,body\n  assert not scheduler.cache.entries\n'
            ' job_id=job.job_id\n'
            ' status,body,*_=call(service,owner.request(job.result_id),tokens[0])\n'
            ' assert status==200,body\n assert len(scheduler.cache.entries)==1\n'
            ' print(json.dumps(dict(job_id=job.result_id,sequence=audit._sequence)),flush=True)\n'
            'finally: assert scheduler.close(2)\n')
        first=OwnedEpochSupervisor(OwnedEntryInventory((profile(source.encode(),arguments=('none',)),))).run_admitted('owned')
        self.assertEqual(first.exit_code,0,first.output)
        import json
        original=json.loads(first.output)
        second=OwnedEpochSupervisor(OwnedEntryInventory((profile(source.encode(),arguments=(original['job_id'],)),))).run_admitted('owned')
        self.assertEqual(second.exit_code,0,second.output)
        replacement=json.loads(second.output)
        self.assertNotEqual(original['job_id'],replacement['job_id'])
        self.assertGreater(original['sequence'],0)
        self.assertGreater(replacement['sequence'],0)

    def test_ipc_wrapper_fault_closes_every_transferred_descriptor(self):
        actual=os.fdopen
        for fail_at in ((2,3) if sys.platform=='win32' else (1,)):
            descriptors=[]
            def wrap(fd,*args,**kwargs):
                descriptors.append(fd)
                if len(descriptors)==fail_at:raise RuntimeError('ipc-wrapper-fault')
                return actual(fd,*args,**kwargs)
            with patch.object(os,'fdopen',side_effect=wrap):
                with self.assertRaisesRegex(RuntimeError,'ipc-wrapper-fault'):
                    self.supervisor().run_admitted('owned')
            for descriptor in descriptors:
                with self.assertRaises(OSError) as caught:os.fstat(descriptor)
                self.assertEqual(caught.exception.errno,errno.EBADF)
            self.assertFalse(_containment._EPOCH_CAPACITY.locked())
            self.assertEqual(self.supervisor().run_admitted('owned').exit_code,0)

    def test_parent_death_before_ack_prevents_program_or_factory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            marker,factory,parent=root/'child-pid',root/'factory',root/'parent.py'
            # Parent read thread sees the pre-ACK frame and deliberately pauses.
            # Child writes only a PID witness after guards, before its ACK read.
            replacement="Path("+repr(str(marker))+ ").write_text(str(os.getpid()))\nfd=m['startup_endpoint']"
            code=('import hashlib,time\nfrom pathlib import Path\n'
                'from equity_feature_service import _containment as c\n'
                'from equity_feature_service._entry import OwnedEntryProfile,OwnedEntryInventory\n'
                'source='+repr(('from pathlib import Path\nPath('+repr(str(factory))+').write_text("started")').encode())+'\n'
                'c._ADMITTED_BOOTSTRAP=c._ADMITTED_BOOTSTRAP.replace('+repr("fd=m['startup_endpoint']")+','+repr("assert 'site' not in sys.modules and 'equity_feature_service' not in sys.modules\nfrom pathlib import Path\n"+replacement)+')\n'
                'def block(*args):\n while True: time.sleep(.01)\n'
                'c.validate_startup=block\n'
                'p=OwnedEntryProfile("owned",source,hashlib.sha256(source).hexdigest(),(),True)\n'
                'print(c.OwnedEpochSupervisor(OwnedEntryInventory((p,))).run_admitted("owned",wall_seconds=3),flush=True)\n')
            parent.write_text(code,encoding='utf-8')
            outer=subprocess.Popen([sys.executable,'-I',str(parent)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            handle=None
            try:
                deadline=time.monotonic()+5
                while not marker.exists() and time.monotonic()<deadline:
                    if outer.poll() is not None:
                        self.fail(str(outer.communicate(timeout=1)))
                    time.sleep(.01)
                self.assertTrue(marker.exists())
                self.assertFalse(factory.exists())
                child_pid=int(marker.read_text())
                if sys.platform=='win32':
                    from ctypes import wintypes as w
                    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
                    kernel.OpenProcess.argtypes=[w.DWORD,w.BOOL,w.DWORD]
                    kernel.OpenProcess.restype=w.HANDLE
                    kernel.WaitForSingleObject.argtypes=[w.HANDLE,w.DWORD]
                    kernel.WaitForSingleObject.restype=w.DWORD
                    kernel.CloseHandle.argtypes=[w.HANDLE]
                    handle=kernel.OpenProcess(0x100000,False,child_pid)
                    self.assertTrue(handle)
                outer.kill();outer.wait(timeout=3)
                if sys.platform=='win32':self.assertEqual(kernel.WaitForSingleObject(handle,3000),0)
                else:
                    deadline=time.monotonic()+3
                    while time.monotonic()<deadline:
                        stat=Path('/proc')/str(child_pid)/'stat'
                        if not stat.exists() or stat.read_text().split(') ',1)[1].split()[0]=='Z':break
                        time.sleep(.01)
                    else:self.fail('child alive after original parent death')
                self.assertFalse(factory.exists())
            finally:
                if outer.poll() is None:outer.kill()
                outer.communicate(timeout=3)
                if handle:kernel.CloseHandle(handle)

    def test_invalid_initial_or_backwards_clock_never_strands_capacity(self):
        for clock in (float('nan'),float('inf'),-1,True):
            with patch.object(_containment.time,'monotonic',return_value=clock):
                with self.assertRaisesRegex(RuntimeError,'owned_epoch_clock'):
                    self.supervisor().run_admitted('owned')
            self.assertFalse(_containment._EPOCH_CAPACITY.locked())
        calls=[]
        def backwards():
            calls.append(1)
            return 100.0 if len(calls)==1 else 99.0
        with patch.object(_containment.time,'monotonic',side_effect=backwards):
            with self.assertRaisesRegex(RuntimeError,'owned_epoch_clock'):
                self.supervisor(b'import time\ntime.sleep(2)').run_admitted('owned')
        self.assertFalse(_containment._EPOCH_CAPACITY.locked())
        self.assertEqual(self.supervisor().run_admitted('owned').exit_code,0)
