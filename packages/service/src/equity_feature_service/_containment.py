"""Internal owned epoch boundary. No HTTP-selected code, paths or recovery."""
from __future__ import annotations

import ctypes
from dataclasses import dataclass
import errno
from importlib import import_module
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import sysconfig
import threading
import time
from typing import Any, BinaryIO


@dataclass(frozen=True)
class EpochResult:
    exit_code: int
    reason: str
    output: bytes
    elapsed_seconds: float


def _linux_limits(expected_parent: int) -> None:
    """Run in the initial child thread before installation/native imports."""
    resource: Any = import_module('resource')
    if (platform.machine() != 'x86_64' or ctypes.sizeof(ctypes.c_void_p) != 8 or getattr(os, 'geteuid')() == 0
            or len(os.listdir('/proc/self/task')) != 1):
        raise RuntimeError('unsupported_host')
    with open('/proc/self/status', encoding='ascii') as stream:
        capabilities = next(row for row in stream if row.startswith('CapEff:'))
    if capabilities.split()[1] != '0000000000000000' or os.getppid() != expected_parent or expected_parent <= 1:
        raise RuntimeError('unadmitted_parent_or_capabilities')
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_AS, (4_294_967_296, 4_294_967_296))
    resource.setrlimit(resource.RLIMIT_CPU, (10, 10))
    library: Any = ctypes.CDLL(None, use_errno=True)
    library.prctl.restype = ctypes.c_int
    if library.prctl(1, 9, 0, 0, 0) != 0:
        raise RuntimeError('parent_death_setup')
    death = ctypes.c_int()
    if library.prctl(2, ctypes.byref(death), 0, 0, 0) != 0 or death.value != 9 or os.getppid() != expected_parent:
        raise RuntimeError('parent_death_setup')
    class Instruction(ctypes.Structure):
        _fields_ = [('code', ctypes.c_ushort), ('jt', ctypes.c_ubyte), ('jf', ctypes.c_ubyte), ('k', ctypes.c_uint)]
    class Program(ctypes.Structure):
        _fields_ = [('length', ctypes.c_ushort), ('filter', ctypes.POINTER(Instruction))]
    rows = [
        (0x20,0,0,4), (0x15,1,0,0xc000003e), (0x06,0,0,0x80000000),
        (0x20,0,0,0), (0x45,0,1,0x40000000), (0x06,0,0,0x80000000),
        (0x15,0,1,57), (0x06,0,0,0x00050000 | errno.EPERM),
        (0x15,0,1,58), (0x06,0,0,0x00050000 | errno.EPERM),
        (0x15,0,1,435), (0x06,0,0,0x00050000 | errno.ENOSYS),
        (0x15,1,0,56), (0x06,0,0,0x7fff0000),
        (0x20,0,0,16), (0x45,0,1,0x7e0240ff), (0x06,0,0,0x00050000 | errno.EPERM),
        (0x54,0,0,0x10900), (0x15,1,0,0x10900),
        (0x06,0,0,0x00050000 | errno.EPERM), (0x06,0,0,0x7fff0000)]
    instructions = (Instruction * len(rows))(*(Instruction(*row) for row in rows))
    program = Program(len(rows), instructions)
    if library.prctl(38,1,0,0,0) != 0 or library.prctl(22,2,ctypes.byref(program),0,0) != 0:
        raise RuntimeError('process_filter_setup')
    if library.prctl(21,0,0,0,0) != 2:
        raise RuntimeError('process_filter_setup')


class _WindowsChild:
    def __init__(self, argv: list[str], environment: dict[str, str]) -> None:
        from ctypes import wintypes as w
        if ctypes.sizeof(ctypes.c_void_p) != 8:
            raise RuntimeError('unsupported_host')
        kernel: Any = ctypes.WinDLL('kernel32', use_last_error=True)
        self.kernel = kernel
        self.job: Any = None
        self.process: Any = None
        self.thread: Any = None
        self.output: BinaryIO
        class Basic(ctypes.Structure):
            _fields_ = [('process_time',ctypes.c_longlong),('job_time',ctypes.c_longlong),('flags',w.DWORD),
                ('min_ws',ctypes.c_size_t),('max_ws',ctypes.c_size_t),('active',w.DWORD),
                ('affinity',ctypes.c_size_t),('priority',w.DWORD),('scheduling',w.DWORD)]
        class IO(ctypes.Structure):
            _fields_ = [(name,ctypes.c_ulonglong) for name in ('read_op','write_op','other_op','read_bytes','write_bytes','other_bytes')]
        class Extended(ctypes.Structure):
            _fields_ = [('basic',Basic),('io',IO),('process_memory',ctypes.c_size_t),
                ('job_memory',ctypes.c_size_t),('peak_process_memory',ctypes.c_size_t),('peak_job_memory',ctypes.c_size_t)]
        class Startup(ctypes.Structure):
            _fields_ = [('cb',w.DWORD),('reserved',w.LPWSTR),('desktop',w.LPWSTR),('title',w.LPWSTR),
                ('x',w.DWORD),('y',w.DWORD),('xs',w.DWORD),('ys',w.DWORD),('xc',w.DWORD),('yc',w.DWORD),
                ('fill',w.DWORD),('flags',w.DWORD),('show',w.WORD),('cb_reserved',w.WORD),
                ('reserved2',ctypes.POINTER(ctypes.c_byte)),('stdin',w.HANDLE),('stdout',w.HANDLE),('stderr',w.HANDLE)]
        class StartupExtended(ctypes.Structure):
            _fields_ = [('startup',Startup),('attributes',ctypes.c_void_p)]
        class Process(ctypes.Structure):
            _fields_ = [('process',w.HANDLE),('thread',w.HANDLE),('pid',w.DWORD),('tid',w.DWORD)]
        class Security(ctypes.Structure):
            _fields_ = [('length',w.DWORD),('descriptor',ctypes.c_void_p),('inherit',w.BOOL)]
        signatures = {
            'CreateJobObjectW':([ctypes.c_void_p,w.LPCWSTR],w.HANDLE),
            'SetInformationJobObject':([w.HANDLE,ctypes.c_int,ctypes.c_void_p,w.DWORD],w.BOOL),
            'QueryInformationJobObject':([w.HANDLE,ctypes.c_int,ctypes.c_void_p,w.DWORD,ctypes.c_void_p],w.BOOL),
            'CreatePipe':([ctypes.POINTER(w.HANDLE),ctypes.POINTER(w.HANDLE),ctypes.c_void_p,w.DWORD],w.BOOL),
            'SetHandleInformation':([w.HANDLE,w.DWORD,w.DWORD],w.BOOL),
            'CreateFileW':([w.LPCWSTR,w.DWORD,w.DWORD,ctypes.c_void_p,w.DWORD,w.DWORD,w.HANDLE],w.HANDLE),
            'InitializeProcThreadAttributeList':([ctypes.c_void_p,w.DWORD,w.DWORD,ctypes.POINTER(ctypes.c_size_t)],w.BOOL),
            'UpdateProcThreadAttribute':([ctypes.c_void_p,w.DWORD,ctypes.c_size_t,ctypes.c_void_p,ctypes.c_size_t,ctypes.c_void_p,ctypes.c_void_p],w.BOOL),
            'DeleteProcThreadAttributeList':([ctypes.c_void_p],None),
            'CreateProcessW':([w.LPCWSTR,w.LPWSTR,ctypes.c_void_p,ctypes.c_void_p,w.BOOL,w.DWORD,
                ctypes.c_void_p,w.LPCWSTR,ctypes.POINTER(Startup),ctypes.POINTER(Process)],w.BOOL),
            'IsProcessInJob':([w.HANDLE,w.HANDLE,ctypes.POINTER(w.BOOL)],w.BOOL),
            'ResumeThread':([w.HANDLE],w.DWORD), 'WaitForSingleObject':([w.HANDLE,w.DWORD],w.DWORD),
            'GetExitCodeProcess':([w.HANDLE,ctypes.POINTER(w.DWORD)],w.BOOL),
            'TerminateJobObject':([w.HANDLE,w.UINT],w.BOOL), 'CloseHandle':([w.HANDLE],w.BOOL)}
        for name, (args, result) in signatures.items():
            function = getattr(kernel, name)
            function.argtypes, function.restype = args, result
        def check(value: Any) -> None:
            if not value:
                raise OSError(ctypes.get_last_error(), 'owned_epoch_setup')
        read, write = w.HANDLE(), w.HANDLE()
        nul: Any = None
        attributes: Any = None
        initialized = False
        child = Process()
        try:
            self.job = kernel.CreateJobObjectW(None, None)
            check(self.job)
            limits = Extended()
            limits.basic.flags = 0x4 | 0x8 | 0x200 | 0x2000
            limits.basic.job_time, limits.basic.active, limits.job_memory = 100_000_000, 1, 1_073_741_824
            check(kernel.SetInformationJobObject(self.job,9,ctypes.byref(limits),ctypes.sizeof(limits)))
            observed = Extended()
            check(kernel.QueryInformationJobObject(self.job,9,ctypes.byref(observed),ctypes.sizeof(observed),None))
            if (observed.basic.flags, observed.basic.job_time, observed.basic.active, observed.job_memory) != (
                    limits.basic.flags, limits.basic.job_time, 1, limits.job_memory):
                raise RuntimeError('unverified_limits')
            security = Security(ctypes.sizeof(Security),None,True)
            check(kernel.CreatePipe(ctypes.byref(read),ctypes.byref(write),ctypes.byref(security),4096))
            check(kernel.SetHandleInformation(read,1,0))
            nul = kernel.CreateFileW('NUL',0x80000000,3,ctypes.byref(security),3,0,None)
            if nul == ctypes.c_void_p(-1).value:
                nul = None
                raise OSError('owned_epoch_stdin')
            size = ctypes.c_size_t()
            kernel.InitializeProcThreadAttributeList(None,2,0,ctypes.byref(size))
            if not size.value:
                raise OSError('owned_epoch_attributes')
            attributes = ctypes.create_string_buffer(size.value)
            check(kernel.InitializeProcThreadAttributeList(attributes,2,0,ctypes.byref(size)))
            initialized = True
            jobs = (w.HANDLE * 1)(self.job)
            handles = (w.HANDLE * 2)(write.value,nul)
            check(kernel.UpdateProcThreadAttribute(attributes,0,0x2000d,jobs,ctypes.sizeof(jobs),None,None))
            check(kernel.UpdateProcThreadAttribute(attributes,0,0x20002,handles,ctypes.sizeof(handles),None,None))
            startup = StartupExtended()
            startup.startup.cb, startup.startup.flags = ctypes.sizeof(startup), 0x100
            startup.startup.stdin, startup.startup.stdout, startup.startup.stderr = nul, write.value, write.value
            startup.attributes = ctypes.cast(attributes,ctypes.c_void_p)
            command = ctypes.create_unicode_buffer(subprocess.list2cmdline(argv))
            block = ctypes.create_unicode_buffer(''.join(k+'='+v+'\0' for k,v in sorted(environment.items()))+'\0')
            check(kernel.CreateProcessW(argv[0],command,None,None,True,4|0x80000|0x400,block,
                                        None,ctypes.byref(startup.startup),ctypes.byref(child)))
            self.process, self.thread = child.process, child.thread
            contained = w.BOOL()
            check(kernel.IsProcessInJob(self.process,self.job,ctypes.byref(contained)))
            if not contained.value:
                raise RuntimeError('uncontained_child')
            import msvcrt
            if read.value is None:
                raise OSError('owned_epoch_pipe')
            fd = msvcrt.open_osfhandle(read.value,os.O_RDONLY | os.O_BINARY)
            read = w.HANDLE()
            self.output = os.fdopen(fd,'rb',buffering=0)
            if kernel.ResumeThread(self.thread) == 0xffffffff:
                raise OSError('owned_epoch_resume')
        except BaseException:
            self.close()
            if hasattr(self,'output'):
                self.output.close()
            raise
        finally:
            if write.value: kernel.CloseHandle(write)
            if read.value: kernel.CloseHandle(read)
            if nul: kernel.CloseHandle(nul)
            if initialized: kernel.DeleteProcThreadAttributeList(attributes)

    def poll(self) -> int | None:
        from ctypes import wintypes as w
        state = self.kernel.WaitForSingleObject(self.process,0)
        if state == 258:
            return None
        if state != 0:
            raise OSError('owned_epoch_wait')
        code = w.DWORD()
        if not self.kernel.GetExitCodeProcess(self.process,ctypes.byref(code)):
            raise OSError('owned_epoch_exit')
        return int(code.value)

    def kill(self) -> None:
        if self.job and not self.kernel.TerminateJobObject(self.job,99):
            raise OSError('owned_epoch_terminate')

    def close(self) -> None:
        if self.job:
            self.kernel.TerminateJobObject(self.job,99)
        if self.process:
            # Never hand capacity to a new epoch without observing real exit.
            if self.kernel.WaitForSingleObject(self.process,3000) != 0:
                raise RuntimeError('owned_epoch_exit_not_observed')
            self.kernel.CloseHandle(self.process)
            self.process = None
        if self.thread:
            self.kernel.CloseHandle(self.thread)
            self.thread = None
        if self.job:
            self.kernel.CloseHandle(self.job)
            self.job = None


_BOOTSTRAP = """import json,os,runpy,sys
policy,installation,entry,args,parent=json.loads(sys.argv[1])
boundary=runpy.run_path(policy)
if sys.platform=='linux': boundary['_linux_limits'](parent)
elif sys.platform!='win32': raise RuntimeError('unsupported_host')
import site
site.addsitedir(installation)
sys.argv=[entry]+args
runpy.run_path(entry,run_name='__main__')
"""

_EPOCH_CAPACITY = threading.Lock()


class OwnedEpochSupervisor:
    """Trusted installation/entry registration only; one epoch per supervisor.

    A child owns the whole Service and all native objects. Killing it ends that
    ephemeral epoch; this class offers neither rollback nor restart/reexecution.
    This internal candidate is not an HTTP code-execution interface.
    """
    def __init__(self) -> None:
        self._capacity = _EPOCH_CAPACITY

    def run(self, entry: Path, arguments: tuple[str, ...] = (), *, wall_seconds: float = 30,
            output_limit: int = 65536) -> EpochResult:
        if (type(wall_seconds) not in (float,int) or not 0 < wall_seconds <= 30
                or type(output_limit) is not int or not 1 <= output_limit <= 65536
                or type(arguments) is not tuple or len(arguments) > 32
                or any(type(arg) is not str or len(arg.encode('utf-8')) > 4096 for arg in arguments)):
            raise ValueError('unadmitted_epoch_profile')
        entry = entry.resolve(strict=True)
        if not entry.is_file() or entry.suffix != '.py':
            raise ValueError('unadmitted_epoch_entry')
        if not self._capacity.acquire(blocking=False):
            raise RuntimeError('epoch_capacity')
        start = time.monotonic()
        child: Any = None
        reader: threading.Thread | None = None
        captured = bytearray()
        overflow, read_failure = threading.Event(), threading.Event()
        safe_release = True
        try:
            installation = sysconfig.get_path('purelib')
            argv = [str(Path(sys.base_prefix)/'python.exe') if sys.platform == 'win32' else sys.executable,
                    '-I','-S','-c',_BOOTSTRAP,json.dumps([str(Path(__file__).resolve()),installation,str(entry),list(arguments),os.getpid()])]
            environment = {key:os.environ[key] for key in ('SystemRoot','WINDIR','TEMP','TMP') if key in os.environ}
            if sys.platform == 'win32':
                child = _WindowsChild(argv,environment)
                output = child.output
            elif sys.platform == 'linux':
                child = subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT,env=environment,close_fds=True)
                output = child.stdout
                if output is None: raise RuntimeError('owned_epoch_pipe')
            else:
                raise RuntimeError('unsupported_host')
            def drain() -> None:
                try:
                    while True:
                        block = os.read(output.fileno(),4096)
                        if not block: return
                        remaining = output_limit-len(captured)
                        captured.extend(block[:remaining])
                        if len(block) > remaining:
                            overflow.set()
                            return
                except OSError:
                    read_failure.set()
            reader = threading.Thread(target=drain,name='owned-epoch-output',daemon=True)
            reader.start()
            reason = 'exited'
            while child.poll() is None:
                if overflow.is_set() or read_failure.is_set() or time.monotonic()-start >= wall_seconds:
                    reason = 'output_limit' if overflow.is_set() else 'capture_failure' if read_failure.is_set() else 'deadline'
                    child.kill()
                    break
                time.sleep(0.01)
            deadline = time.monotonic()+3
            while child.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            code = child.poll()
            if code is None: raise RuntimeError('owned_epoch_exit_not_observed')
            reader.join(3)
            if reader.is_alive(): raise RuntimeError('owned_epoch_capture_not_closed')
            if overflow.is_set(): reason = 'output_limit'
            elif read_failure.is_set(): reason = 'capture_failure'
            elif reason == 'exited' and code != 0: reason = 'child_failed'
            return EpochResult(code,reason,bytes(captured),time.monotonic()-start)
        except RuntimeError as error:
            if str(error) in ('owned_epoch_exit_not_observed', 'owned_epoch_capture_not_closed'):
                safe_release = False
            raise
        finally:
            # Failed termination deliberately keeps capacity occupied.
            if child is not None:
                if sys.platform == 'win32': child.close()
                else:
                    if child.poll() is None: child.kill()
                    child.wait(timeout=3)
                if reader is not None: reader.join(3)
                if reader is not None and reader.is_alive():
                    raise RuntimeError('owned_epoch_capture_not_closed')
                output.close()
            if safe_release:
                self._capacity.release()
