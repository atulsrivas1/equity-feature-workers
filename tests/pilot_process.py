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
            ('TerminateProcess',[w.HANDLE,w.UINT],w.BOOL),
            ('OpenThread',[w.DWORD,w.BOOL,w.DWORD],w.HANDLE),
            ('GetProcessIdOfThread',[w.HANDLE],w.DWORD),
            ('GetExitCodeProcess',[w.HANDLE,ctypes.POINTER(w.DWORD)],w.BOOL),
            ('ResumeThread',[w.HANDLE],w.DWORD)):
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

    def _resume_windows(self):
        from ctypes import wintypes as w
        class ThreadEntry(ctypes.Structure):
            _fields_=[('size',w.DWORD),('usage',w.DWORD),('tid',w.DWORD),
                ('owner',w.DWORD),('base_priority',w.LONG),('delta_priority',w.LONG),('flags',w.DWORD)]
        for name in ('Thread32First','Thread32Next'):
            fn=getattr(self.kernel,name);fn.argtypes=[w.HANDLE,ctypes.POINTER(ThreadEntry)];fn.restype=w.BOOL
        snapshot=self.kernel.CreateToolhelp32Snapshot(4,0)
        if snapshot==ctypes.c_void_p(-1).value:raise OSError('owned primary thread inventory unavailable')
        tids=[]
        try:
            entry=ThreadEntry();entry.size=ctypes.sizeof(entry)
            present=self.kernel.Thread32First(snapshot,ctypes.byref(entry))
            while present:
                if entry.owner==self.process.pid:tids.append(int(entry.tid))
                present=self.kernel.Thread32Next(snapshot,ctypes.byref(entry))
            if ctypes.get_last_error()!=18:raise OSError('owned primary thread inventory incomplete')
        finally:
            if not self.kernel.CloseHandle(snapshot):raise OSError('owned thread snapshot close failed')
        if len(tids)!=1:raise OSError('owned single suspended primary thread unavailable')
        thread=self.kernel.OpenThread(0x802,False,tids[0])  # suspend/resume plus query limited
        if not thread:raise OSError('owned primary thread handle unavailable')
        try:
            live=w.DWORD()
            if self.kernel.GetProcessIdOfThread(thread)!=self.process.pid:
                raise OSError('owned primary thread identity changed')
            if not self.kernel.GetExitCodeProcess(self.handles[self.process.pid],ctypes.byref(live)) or live.value!=259:
                raise OSError('original suspended process no longer live')
            if self.kernel.ResumeThread(thread)!=1:raise OSError('owned primary thread resume failed')
        finally:
            if not self.kernel.CloseHandle(thread):raise OSError('owned primary thread close failed')

    @staticmethod
    def _linux_row(pid):
        fields=(Path('/proc')/str(pid)/'stat').read_text(encoding='ascii').rsplit(')',1)[1].split()
        return int(fields[19]),int(fields[2]),int(fields[3])  # start, group, session

    def start(self,command,**kwargs):
        try:
            if os.name=='nt':kwargs['creationflags']=kwargs.get('creationflags',0)|4  # CREATE_SUSPENDED
            self.process=subprocess.Popen(command,**kwargs,start_new_session=os.name!='nt')
            if os.name=='nt':
                root=self._hold(self.process.pid)
                self._assign(root)
                self._resume_windows()  # no launcher/helper can run before verified job membership
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
            errors=[]
            if self.job:
                if self.kernel.CloseHandle(self.job):self.job=None
                else:errors.append('owned job close failed')  # retain identity for bounded retry
            # Every retained process identity gets fallback cleanup even if job close failed.
            for pid,handle in list(self.handles.items()):
                try:self.kernel.TerminateProcess(handle,1)
                except Exception:errors.append('held process termination failed')
                try:
                    if self.kernel.CloseHandle(handle):del self.handles[pid]
                    else:errors.append('held process handle close failed')
                except Exception:errors.append('held process handle close failed')
            if self.job:
                if self.kernel.CloseHandle(self.job):self.job=None
                else:errors.append('owned job close retry failed')
            if self.process is not None and (failed or errors):
                try:self.process.terminate()  # Popen retains its original Windows process handle
                except Exception:errors.append('original process handle cleanup failed')
            if errors:raise OSError('; '.join(errors))
            return
        if not failed or self.process is None:return
        known=dict(self.identities)
        if sampler is not None:
            known.update({p['pid']:p['creation'] for p in list(sampler.peaks.values())})
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
        process=process or owner.process  # startup rejection still owns its pipes
        cleanup_errors=[]
        def finalize_sampler():
            if sampler is None:return
            try:
                sampler.stop_event.set()
                if sampler.thread.ident is not None:sampler.thread.join(5)
                if sampler.thread.is_alive():cleanup_errors.append('owned sampler did not stop within five seconds')
            except Exception as error:cleanup_errors.append('sampler finalization failed: '+type(error).__name__)
        finalize_sampler()  # stop mutation before copying Linux retained identities
        try:owner.close(failed=failure is not None,sampler=sampler)
        except Exception as error:cleanup_errors.append('owned cleanup failed: '+str(error))
        try:
            if failure is not None and process is not None:
                try:process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    # Closing buffered output under a blocked reader can itself block.
                    # Do not pretend unavailable inherited pipes were drained.
                    cleanup_errors.append('owned pipe drain exceeded five seconds')
        except Exception as error:cleanup_errors.append('owned pipe drain failed: '+type(error).__name__)
        finally:finalize_sampler()
        if cleanup_errors:
            if failure is not None:
                for note in cleanup_errors:failure.add_note(note)
            else:raise RuntimeError('; '.join(cleanup_errors))
