"""Internal owned epoch boundary. No HTTP-selected code, paths or recovery."""
from __future__ import annotations

import base64
import ctypes
import hashlib
import secrets
from dataclasses import dataclass
import errno
from importlib import import_module
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import sysconfig
import threading
import time
from typing import Any, BinaryIO

if __package__:
    from ._entry import OwnedEntryInventory, OwnedEntryProfile, canonical, startup_ack, startup_frame, validate_startup


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
    def __init__(self, argv: list[str], environment: dict[str, str], *, admitted: bool = False) -> None:
        from ctypes import wintypes as w
        if ctypes.sizeof(ctypes.c_void_p) != 8:
            raise RuntimeError('unsupported_host')
        kernel: Any = getattr(ctypes, 'WinDLL')('kernel32', use_last_error=True)
        self.kernel = kernel
        self.job: Any = None
        self.process: Any = None
        self.thread: Any = None
        self.output: BinaryIO
        self.startup: BinaryIO | None = None
        self.ack: BinaryIO | None = None
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
                raise OSError(getattr(ctypes, 'get_last_error')(), 'owned_epoch_setup')
        read, write = w.HANDLE(), w.HANDLE()
        startup_read, startup_write = w.HANDLE(), w.HANDLE()
        ack_read, ack_write = w.HANDLE(), w.HANDLE()
        nul: Any = None
        attributes: Any = None
        initialized = False
        capture_fd: int | None = None
        ipc_fd: int | None = None
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
            if admitted:
                check(kernel.CreatePipe(ctypes.byref(startup_read),ctypes.byref(startup_write),ctypes.byref(security),4096))
                check(kernel.CreatePipe(ctypes.byref(ack_read),ctypes.byref(ack_write),ctypes.byref(security),4096))
                check(kernel.SetHandleInformation(startup_read,1,0))
                check(kernel.SetHandleInformation(ack_write,1,0))
                metadata = json.loads(argv[-1])
                metadata['startup_endpoint'] = startup_write.value
                argv = argv[:-1]+[_launch_metadata(metadata)]
            else:
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
            handles = ((w.HANDLE * 3)(write.value,ack_read.value,startup_write.value) if admitted
                       else (w.HANDLE * 2)(write.value,nul))
            check(kernel.UpdateProcThreadAttribute(attributes,0,0x2000d,jobs,ctypes.sizeof(jobs),None,None))
            check(kernel.UpdateProcThreadAttribute(attributes,0,0x20002,handles,ctypes.sizeof(handles),None,None))
            startup = StartupExtended()
            startup.startup.cb, startup.startup.flags = ctypes.sizeof(startup), 0x100
            startup.startup.stdin, startup.startup.stdout, startup.startup.stderr = (ack_read.value if admitted else nul), write.value, write.value
            startup.attributes = ctypes.cast(attributes,ctypes.c_void_p)
            command_text = subprocess.list2cmdline(argv)
            if len(command_text.encode('utf-16-le'))//2+1 > 32760:
                raise ValueError('owned_epoch_command_line')
            command = ctypes.create_unicode_buffer(command_text)
            block = ctypes.create_unicode_buffer(''.join(k+'='+v+'\0' for k,v in sorted(environment.items()))+'\0')
            check(kernel.CreateProcessW(argv[0],command,None,None,True,4|0x80000|0x400,block,
                                        None,ctypes.byref(startup.startup),ctypes.byref(child)))
            self.process, self.thread = child.process, child.thread
            # Parent must not retain child-only endpoints: EOF is admission.
            for handle in (startup_write,ack_read,write):
                if handle.value:
                    kernel.CloseHandle(handle)
                    handle.value = None
            contained = w.BOOL()
            check(kernel.IsProcessInJob(self.process,self.job,ctypes.byref(contained)))
            if not contained.value:
                raise RuntimeError('uncontained_child')
            msvcrt: Any = import_module('msvcrt')
            if read.value is None:
                raise OSError('owned_epoch_pipe')
            capture_fd = int(msvcrt.open_osfhandle(read.value,os.O_RDONLY | getattr(os, 'O_BINARY')))
            read = w.HANDLE()
            self.output = os.fdopen(capture_fd,'rb',buffering=0)
            capture_fd = None
            if admitted:
                if startup_read.value is None or ack_write.value is None:
                    raise OSError('owned_startup_pipe')
                ipc_fd = int(msvcrt.open_osfhandle(startup_read.value,os.O_RDONLY | getattr(os, 'O_BINARY')))
                startup_read = w.HANDLE()
                self.startup = os.fdopen(ipc_fd,'rb',buffering=0)
                ipc_fd = None
                ipc_fd = int(msvcrt.open_osfhandle(ack_write.value,os.O_WRONLY | getattr(os, 'O_BINARY')))
                ack_write = w.HANDLE()
                self.ack = os.fdopen(ipc_fd,'wb',buffering=0)
                ipc_fd = None
            if kernel.ResumeThread(self.thread) == 0xffffffff:
                raise OSError('owned_epoch_resume')
        except BaseException:
            self.close()
            if hasattr(self,'output'):
                self.output.close()
            for stream in (self.startup,self.ack):
                if stream is not None: stream.close()
            raise
        finally:
            if capture_fd is not None: os.close(capture_fd)
            if ipc_fd is not None: os.close(ipc_fd)
            for handle in (startup_read,startup_write,ack_read,ack_write):
                if handle.value: kernel.CloseHandle(handle)
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

_ADMITTED_BOOTSTRAP = r"""import base64,hashlib,json,os,runpy,sys
m=json.loads(sys.argv[1])
boundary=runpy.run_path(m['policy'])
if sys.platform=='linux': boundary['_linux_limits'](m['parent'])
elif sys.platform!='win32': raise RuntimeError('unsupported_host')
source=base64.b64decode(m['source'],validate=True)
if not 1<=len(source)<=8192 or hashlib.sha256(source).hexdigest()!=m['source_sha256']:
 raise RuntimeError('owned_source_integrity')
code=compile(source.decode('utf-8'),'<owned:'+m['profile']+'>','exec')
frame=json.dumps(dict(schema='owned.startup.v1',epoch=m['epoch'],challenge=m['challenge'],profile=m['profile'],source_sha256=m['source_sha256']),sort_keys=True,separators=(',',':'),ensure_ascii=True).encode('ascii')+b'\n'
if len(frame)>512: raise RuntimeError('owned_startup_bounds')
fd=m['startup_endpoint']
if sys.platform=='win32':
 import msvcrt
 fd=msvcrt.open_osfhandle(fd,os.O_WRONLY|os.O_BINARY)
try:
 position=0
 while position<len(frame):
  written=os.write(fd,frame[position:])
  if written<=0: raise RuntimeError('owned_startup_write')
  position+=written
finally: os.close(fd)
ack=b''
while True:
 block=os.read(0,66-len(ack))
 if not block: break
 ack+=block
 if len(ack)>65: raise RuntimeError('owned_ack_bounds')
if ack!=('ACK:'+m['challenge']+'\n').encode('ascii'):
 raise RuntimeError('owned_ack_identity')
import site
site.addsitedir(m['installation'])
sys.argv=['<owned:'+m['profile']+'>']+m['arguments']
exec(code,dict(__name__='__main__',__file__=sys.argv[0]))
"""


def _launch_metadata(metadata: dict[str, Any]) -> str:
    encoded = canonical(metadata)
    if len(encoded)>16384:
        raise ValueError('owned_launch_metadata')
    return encoded.decode('ascii')


def _epoch_clock(start: float | None = None) -> float:
    value = time.monotonic()
    if type(value) not in (int,float) or not math.isfinite(value) or value < 0 or start is not None and value < start:
        raise RuntimeError('owned_epoch_clock')
    return float(value)


_EPOCH_CAPACITY = threading.Lock()


class OwnedEpochSupervisor:
    """An immutable trusted inventory, or a development-only path supervisor.

    This does not accept HTTP code/selectors and never restarts a killed epoch.
    A registered inventory disables the development run(Path,args) bypass.
    """
    def __init__(self, inventory: OwnedEntryInventory | None = None) -> None:
        if inventory is not None and type(inventory) is not OwnedEntryInventory:
            raise ValueError('unadmitted_owned_inventory')
        self._capacity = _EPOCH_CAPACITY
        self._inventory = inventory

    @property
    def inventory(self) -> OwnedEntryInventory | None:
        return self._inventory

    def run(self, entry: Path, arguments: tuple[str, ...] = (), *, wall_seconds: float = 30,
            output_limit: int = 65536) -> EpochResult:
        if self._inventory is not None:
            raise ValueError('registered_entry_bypass')
        entry = entry.resolve(strict=True)
        if not entry.is_file() or entry.suffix != '.py':
            raise ValueError('unadmitted_epoch_entry')
        return self._run(entry,arguments,None,wall_seconds,output_limit)

    def run_admitted(self, profile_id: str, *, wall_seconds: float = 30,
                     output_limit: int = 65536) -> EpochResult:
        if self._inventory is None:
            raise ValueError('unadmitted_owned_inventory')
        profile = self._inventory.select(profile_id)
        return self._run(None,profile.arguments,profile,wall_seconds,output_limit)

    def _run(self, entry: Path | None, arguments: tuple[str, ...], profile: OwnedEntryProfile | None,
             wall_seconds: float, output_limit: int) -> EpochResult:
        if (type(wall_seconds) not in (float,int) or not 0 < wall_seconds <= 30
                or type(output_limit) is not int or not 1 <= output_limit <= 65536
                or type(arguments) is not tuple or profile is None and len(arguments) > 32
                or any(type(arg) is not str or len(arg.encode('utf-8')) > 4096 for arg in arguments)):
            raise ValueError('unadmitted_epoch_profile')
        if sys.platform not in ('win32','linux'):
            raise RuntimeError('unsupported_host')
        if not self._capacity.acquire(blocking=False):
            raise RuntimeError('epoch_capacity')
        start = 0.0
        child: Any = None
        readers: list[threading.Thread] = []
        streams: list[BinaryIO] = []
        raw_fds: set[int] = set()
        captured = bytearray()
        capture_lock = threading.Lock()
        used = 0
        overflow, read_failure = threading.Event(), threading.Event()
        startup_failure, admitted = threading.Event(), threading.Event()
        expected = b''
        ack_bytes = b''
        safe_release = True
        try:
            start = _epoch_clock()
            installation = sysconfig.get_path('purelib')
            executable = str(Path(sys.base_prefix)/'python.exe') if sys.platform=='win32' else sys.executable
            startup_reader: BinaryIO | None = None
            ack_writer: BinaryIO | None = None
            if profile is None:
                argv = [executable,'-I','-S','-c',_BOOTSTRAP,
                        json.dumps([str(Path(__file__).resolve()),installation,str(entry),list(arguments),os.getpid()])]
            else:
                epoch,challenge = secrets.token_hex(16),secrets.token_hex(16)
                expected,ack_bytes = startup_frame(epoch,challenge,profile),startup_ack(challenge)
                endpoint = 0
                if sys.platform=='linux':
                    startup_read,startup_write = os.pipe()
                    raw_fds.update((startup_read,startup_write))
                    endpoint = startup_write
                metadata = dict(policy=str(Path(__file__).resolve()),installation=installation,
                    parent=os.getpid(),source=base64.b64encode(profile.source).decode('ascii'),
                    source_sha256=profile.source_sha256,profile=profile.profile_id,arguments=list(arguments),
                    epoch=epoch,challenge=challenge,startup_endpoint=endpoint)
                argv = [executable,'-I','-S','-c',_ADMITTED_BOOTSTRAP,_launch_metadata(metadata)]
            environment = {key:os.environ[key] for key in ('SystemRoot','WINDIR','TEMP','TMP') if key in os.environ}
            if sys.platform=='win32':
                child = _WindowsChild(argv,environment,admitted=profile is not None)
                output = child.output
                if profile is not None:
                    startup_reader,ack_writer = child.startup,child.ack
                    if startup_reader is None or ack_writer is None:
                        raise RuntimeError('owned_startup_pipe')
                    streams.extend((startup_reader,ack_writer))
            else:
                if profile is None:
                    child = subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                                             stderr=subprocess.STDOUT,env=environment,close_fds=True)
                else:
                    child = subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,env=environment,close_fds=True,pass_fds=(endpoint,))
                    if child.stdout is not None: streams.append(child.stdout)
                    if child.stdin is not None: streams.append(child.stdin)
                    os.close(startup_write)
                    raw_fds.remove(startup_write)
                    startup_reader = os.fdopen(startup_read,'rb',buffering=0)
                    raw_fds.remove(startup_read)
                    streams.append(startup_reader)
                    ack_writer = child.stdin
                    if ack_writer is None: raise RuntimeError('owned_startup_pipe')
                output = child.stdout
                if output is None: raise RuntimeError('owned_epoch_pipe')
            if output not in streams: streams.append(output)

            def charge(block: bytes, *, diagnostic: bool) -> bool:
                nonlocal used
                with capture_lock:
                    remaining = output_limit-used
                    used += min(len(block),remaining)
                    if diagnostic: captured.extend(block[:remaining])
                    if len(block)>remaining:
                        overflow.set()
                        return False
                    return True

            def drain() -> None:
                try:
                    while True:
                        block = os.read(output.fileno(),4096)
                        if not block: return
                        if not charge(block,diagnostic=True): return
                        # A later startup-shaped diagnostic is post-admission
                        # failure only; it cannot undo earlier native work.
                        if profile is not None and b'"schema":"owned.startup.v1"' in captured:
                            startup_failure.set()
                            return
                except OSError:
                    read_failure.set()

            def admit() -> None:
                assert startup_reader is not None and ack_writer is not None
                frame = bytearray()
                try:
                    while True:
                        block = os.read(startup_reader.fileno(),513-len(frame))
                        if not block: break
                        frame.extend(block)
                        if not charge(block,diagnostic=False) or len(frame)>512:
                            startup_failure.set()
                            return
                    validate_startup(bytes(frame),expected)
                    if overflow.is_set() or read_failure.is_set() or startup_failure.is_set():
                        startup_failure.set()
                        return
                    position = 0
                    while position<len(ack_bytes):
                        written = os.write(ack_writer.fileno(),ack_bytes[position:])
                        if written<=0: raise OSError('owned_ack_write')
                        position += written
                    ack_writer.close()
                    admitted.set()
                except (OSError,ValueError):
                    startup_failure.set()

            reader = threading.Thread(target=drain,name='owned-epoch-output',daemon=True)
            reader.start()
            readers.append(reader)
            if profile is not None:
                reader = threading.Thread(target=admit,name='owned-epoch-startup',daemon=True)
                reader.start()
                readers.append(reader)
            reason = 'exited'
            while child.poll() is None:
                if (overflow.is_set() or read_failure.is_set() or startup_failure.is_set()
                        or _epoch_clock(start)-start>=wall_seconds):
                    reason = ('output_limit' if overflow.is_set() else 'capture_failure' if read_failure.is_set()
                              else 'startup_failure' if startup_failure.is_set() else 'deadline')
                    child.kill()
                    break
                time.sleep(0.01)
            deadline = _epoch_clock(start)+3
            while child.poll() is None and _epoch_clock(start)<deadline:
                time.sleep(0.01)
            code = child.poll()
            if code is None: raise RuntimeError('owned_epoch_exit_not_observed')
            for reader in readers:
                reader.join(3)
                if reader.is_alive(): raise RuntimeError('owned_epoch_capture_not_closed')
            if overflow.is_set(): reason='output_limit'
            elif read_failure.is_set(): reason='capture_failure'
            elif reason!='deadline' and (startup_failure.is_set() or profile is not None and not admitted.is_set()): reason='startup_failure'
            elif reason=='exited' and code!=0: reason='child_failed'
            return EpochResult(code,reason,bytes(captured),_epoch_clock(start)-start)
        except RuntimeError as error:
            if str(error) in ('owned_epoch_exit_not_observed','owned_epoch_capture_not_closed'):
                safe_release=False
            raise
        finally:
            # No release after an unobserved process or open capture reader.
            try:
                if child is not None:
                    if sys.platform=='win32': child.close()
                    else:
                        if child.poll() is None: child.kill()
                        child.wait(timeout=3)
                    for reader in readers:
                        reader.join(3)
                        if reader.is_alive(): raise RuntimeError('owned_epoch_capture_not_closed')
                for stream in streams: stream.close()
                for descriptor in raw_fds: os.close(descriptor)
            except BaseException:
                safe_release=False
                raise
            finally:
                if safe_release: self._capacity.release()
