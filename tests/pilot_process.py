"""Owned synthetic coordinator lifetime; no public worker/calculation API."""
from __future__ import annotations
import ctypes
import os
from pathlib import Path
import signal
import subprocess
from pilot_memory import NativeMemory


class OwnedProcess:
    """Windows noninheritable kill-on-close job; Linux owned session/group."""
    def __init__(self):
        self.process=None; self.handles={}; self.identities={}; self.job=None
        if os.name!='nt': return
        from ctypes import wintypes as w
        self.native=NativeMemory(); self.kernel=self.native.kernel
        class Basic(ctypes.Structure):
            _fields_=[('process_time',ctypes.c_longlong),('job_time',ctypes.c_longlong),
                ('flags',w.DWORD),('min_ws',ctypes.c_size_t),('max_ws',ctypes.c_size_t),
                ('active',w.DWORD),('affinity',ctypes.c_size_t),('priority',w.DWORD),('scheduling',w.DWORD)]
        class IO(ctypes.Structure):
            _fields_=[(name,ctypes.c_ulonglong) for name in ('read_ops','write_ops','other_ops','read_bytes','write_bytes','other_bytes')]
        class Extended(ctypes.Structure):
            _fields_=[('basic',Basic),('io',IO)]+[(name,ctypes.c_size_t) for name in ('process_memory','job_memory','peak_process','peak_job')]
        for name,args,restype in (
            ('CreateJobObjectW',[ctypes.c_void_p,w.LPCWSTR],w.HANDLE),
            ('SetInformationJobObject',[w.HANDLE,ctypes.c_int,ctypes.c_void_p,w.DWORD],w.BOOL),
            ('AssignProcessToJobObject',[w.HANDLE,w.HANDLE],w.BOOL),
            ('IsProcessInJob',[w.HANDLE,w.HANDLE,ctypes.POINTER(w.BOOL)],w.BOOL),
            ('TerminateProcess',[w.HANDLE,w.UINT],w.BOOL)):
            fn=getattr(self.kernel,name);fn.argtypes=args;fn.restype=restype
        self.job=self.kernel.CreateJobObjectW(None,None)  # null security: noninheritable
        if not self.job: raise OSError('owned job creation failed')
        limits=Extended();limits.basic.flags=0x2000  # KILL_ON_JOB_CLOSE; no breakaway/RSS/UI flags
        if not self.kernel.SetInformationJobObject(self.job,9,ctypes.byref(limits),ctypes.sizeof(limits)):
            self.kernel.CloseHandle(self.job);self.job=None
            raise OSError('owned job kill-on-close configuration failed')

    def _hold(self,pid,expected=None):
        from ctypes import wintypes as w
        handle=self.kernel.OpenProcess(0x1101,False,pid)  # query limited, set quota, terminate
        if not handle: raise OSError('owned process handle unavailable')
        times=[w.FILETIME() for _ in range(4)]
        try:
            if not self.kernel.GetProcessTimes(handle,*(ctypes.byref(t) for t in times)):
                raise OSError('owned process creation unavailable')
            creation=times[0].dwLowDateTime|(times[0].dwHighDateTime<<32)
            if expected not in (None,creation):raise OSError('owned process identity changed')
        except BaseException:
            self.kernel.CloseHandle(handle);raise
        self.handles[pid]=handle;self.identities[pid]=creation
        return handle

    def _assign(self,handle):
        from ctypes import wintypes as w
        present=w.BOOL()
        if not self.kernel.IsProcessInJob(handle,self.job,ctypes.byref(present)):
            raise OSError('owned job membership unavailable')
        if not present.value and not self.kernel.AssignProcessToJobObject(self.job,handle):
            raise OSError('owned job assignment rejected')
        if not self.kernel.IsProcessInJob(handle,self.job,ctypes.byref(present)) or not present.value:
            raise OSError('owned job membership unverified')

    def _capture_windows(self):
        # Root handle is retained. Trusted sample cannot spawn a pool before stdin.
        inventory=self.native.inventory();pending=dict(inventory)
        while True:
            added=False
            for pid,(parent,_) in list(pending.items()):
                if pid in self.handles or parent not in self.identities:continue
                try:
                    handle=self._hold(pid)
                    if self.identities[pid]<self.identities[parent]:
                        self.kernel.CloseHandle(handle);del self.handles[pid];del self.identities[pid]
                        continue  # reused snapshot parent PID is not owned ancestry
                except OSError:continue  # exited before obtaining a held identity
                added=True;pending.pop(pid)
            if not added:break

    @staticmethod
    def _linux_row(pid):
        fields=(Path('/proc')/str(pid)/'stat').read_text(encoding='ascii').rsplit(')',1)[1].split()
        return int(fields[19]),int(fields[2]),int(fields[3])  # start, group, session

    def start(self,command,**kwargs):
        try:
            self.process=subprocess.Popen(command,**kwargs,start_new_session=os.name!='nt')
            if os.name=='nt':
                root=self._hold(self.process.pid)
                try:self._assign(root)
                finally:self._capture_windows()  # hold preexisting launcher children even if assignment rejects
                for handle in self.handles.values():self._assign(handle)
            else:
                creation,group,session=self._linux_row(self.process.pid)
                if group!=self.process.pid or session!=self.process.pid:raise OSError('owned session unavailable')
                self.identities[self.process.pid]=creation
            return self.process
        except BaseException as error:
            try:self.close(failed=True)
            except Exception as cleanup:error.add_note('owned startup cleanup failed: '+type(cleanup).__name__)
            raise

    def close(self,failed=False,sampler=None):
        if os.name=='nt':
            if self.job:
                job=self.job;self.job=None
                if not self.kernel.CloseHandle(job):raise OSError('owned job close failed')
            # Held identity handles also cover rejected assignment, never arbitrary reused PIDs.
            for handle in self.handles.values():
                self.kernel.TerminateProcess(handle,1)
                self.kernel.CloseHandle(handle)
            self.handles.clear()
            return
        if not failed or self.process is None:return
        known=dict(self.identities)
        if sampler is not None:
            known.update({p['pid']:p['creation'] for p in sampler.peaks.values()})
        anchored=False
        for pid,creation in known.items():
            try:
                current,group,session=self._linux_row(pid)
                anchored|=current==creation and group==self.process.pid and session==self.process.pid
            except (OSError,ValueError,IndexError):pass
        if anchored:
            try:os.killpg(self.process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        elif self.process.returncode is None:
            # Do not signal a numerically reused group without a retained creation anchor.
            self.process.terminate()
            raise OSError('owned group identity unavailable; group termination not assumed')


def communicate_owned(command,settings,timeout=180,sampler_factory=None,**kwargs):
    from pilot_memory import MemorySampler
    owner=OwnedProcess();sampler=None;process=None;failure=None
    try:
        process=owner.start(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            text=True,encoding='utf-8',**kwargs)
        sampler=(sampler_factory or MemorySampler)(process);sampler.start()
        stdout,stderr=process.communicate(settings,timeout=timeout)
        memory=sampler.stop()
        return stdout,stderr,memory,process.returncode
    except BaseException as error:
        failure=error;raise
    finally:
        try:
            owner.close(failed=failure is not None,sampler=sampler)
            if failure is not None and process is not None:
                try:process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    for stream in (process.stdin,process.stdout,process.stderr):
                        if stream is not None:stream.close()
                    failure.add_note('owned pipe drain exceeded five seconds')
        except Exception as cleanup:
            if failure is None:raise
            failure.add_note('owned cleanup failed: '+type(cleanup).__name__)
        finally:
            if sampler is not None:
                sampler.stop_event.set()
                if sampler.thread.ident is not None:sampler.thread.join(5)
                if sampler.thread.is_alive():
                    if failure is None:raise RuntimeError('owned sampler did not stop')
                    failure.add_note('owned sampler did not stop within five seconds')
