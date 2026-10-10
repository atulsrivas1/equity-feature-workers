"""Disposable pre-code OS API probe, not Service containment qualification."""
import ctypes
import errno
import json
import os
import platform
from pathlib import Path
import subprocess
import sys
import threading


def linux():
    import resource
    assert platform.machine() == 'x86_64' and os.geteuid() != 0
    status = open('/proc/self/status', encoding='ascii').read().splitlines()
    assert next(line for line in status if line.startswith('CapEff:')).split()[1] == '0000000000000000'
    assert len(os.listdir('/proc/self/task')) == 1
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_AS, (4294967296, 4294967296))
    resource.setrlimit(resource.RLIMIT_CPU, (10, 10))
    library = ctypes.CDLL(None, use_errno=True)
    library.prctl.restype = ctypes.c_int
    library.syscall.restype = ctypes.c_long
    parent = os.getppid()
    assert parent > 1
    assert library.prctl(1,9,0,0,0) == 0, ctypes.get_errno()
    death_signal = ctypes.c_int()
    assert library.prctl(2,ctypes.byref(death_signal),0,0,0) == 0 and death_signal.value == 9
    assert os.getppid() == parent
    class Instruction(ctypes.Structure):
        _fields_ = [('code',ctypes.c_ushort),('jt',ctypes.c_ubyte),('jf',ctypes.c_ubyte),('k',ctypes.c_uint)]
    class Program(ctypes.Structure):
        _fields_ = [('length',ctypes.c_ushort),('filter',ctypes.POINTER(Instruction))]
    # Native x86_64 arch, deny x32 and process creation; allow checked thread clone.
    rows = [
        (0x20,0,0,4), (0x15,1,0,0xc000003e), (0x06,0,0,0x80000000),
        (0x20,0,0,0), (0x45,0,1,0x40000000), (0x06,0,0,0x80000000),
        (0x15,0,1,57), (0x06,0,0,0x00050000|errno.EPERM),
        (0x15,0,1,58), (0x06,0,0,0x00050000|errno.EPERM),
        (0x15,0,1,435), (0x06,0,0,0x00050000|errno.ENOSYS),
        (0x15,1,0,56), (0x06,0,0,0x7fff0000),
        (0x20,0,0,16), (0x45,0,1,0x7e0240ff), (0x06,0,0,0x00050000|errno.EPERM),
        (0x54,0,0,0x10900), (0x15,1,0,0x10900),
        (0x06,0,0,0x00050000|errno.EPERM), (0x06,0,0,0x7fff0000)]
    instructions = (Instruction * len(rows))(*(Instruction(*r) for r in rows))
    program = Program(len(rows), instructions)
    assert library.prctl(38,1,0,0,0) == 0, ctypes.get_errno()
    assert library.prctl(22,2,ctypes.byref(program),0,0) == 0, ctypes.get_errno()
    assert library.prctl(21,0,0,0,0) == 2
    results = []
    def denied():
        for number, expected, args in ((57,errno.EPERM,()),(58,errno.EPERM,()),
            (56,errno.EPERM,(17,0,0,0,0)),(435,errno.ENOSYS,(0,0))):
            ctypes.set_errno(0)
            value = library.syscall(number,*args)
            if value == 0:
                os._exit(99)
            if value > 0:
                os.waitpid(value,0)
            assert value == -1 and ctypes.get_errno() == expected,(number,value,ctypes.get_errno())
        results.append('denied')
    denied()
    thread = threading.Thread(target=denied)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and results == ['denied','denied']
    raised = False
    try:
        resource.setrlimit(resource.RLIMIT_CPU,(11,11))
        raised = True
    except (ValueError,OSError):
        pass
    assert not raised
    return {'os':'linux','architecture':'x86_64','hard_address_space_bytes':4294967296,
        'hard_cpu_seconds':10,'no_new_privs':True,'seccomp_mode':2,'thread_clone_works':True,
        'initial_and_thread_process_creation_denied':True,'clone3_enosys':True,'hard_limit_raise_denied':True,
        'parent_death_signal_api':True,'parent_death_exit_qualified':False}


def windows():
    from ctypes import wintypes as w
    assert ctypes.sizeof(ctypes.c_void_p) == 8
    kernel = ctypes.WinDLL('kernel32',use_last_error=True)
    class Basic(ctypes.Structure):
        _fields_ = [('process_time',ctypes.c_longlong),('job_time',ctypes.c_longlong),('flags',w.DWORD),
            ('min_ws',ctypes.c_size_t),('max_ws',ctypes.c_size_t),('active',w.DWORD),
            ('affinity',ctypes.c_size_t),('priority',w.DWORD),('scheduling',w.DWORD)]
    class IO(ctypes.Structure):
        _fields_ = [(n,ctypes.c_ulonglong) for n in ('read_op','write_op','other_op','read_bytes','write_bytes','other_bytes')]
    class Extended(ctypes.Structure):
        _fields_ = [('basic',Basic),('io',IO),('process_memory',ctypes.c_size_t),
            ('job_memory',ctypes.c_size_t),('peak_process_memory',ctypes.c_size_t),('peak_job_memory',ctypes.c_size_t)]
    class Startup(ctypes.Structure):
        _fields_ = [('cb',w.DWORD),('reserved',w.LPWSTR),('desktop',w.LPWSTR),('title',w.LPWSTR),
            ('x',w.DWORD),('y',w.DWORD),('xs',w.DWORD),('ys',w.DWORD),('xc',w.DWORD),('yc',w.DWORD),
            ('fill',w.DWORD),('flags',w.DWORD),('show',w.WORD),('cb_reserved',w.WORD),('reserved2',ctypes.POINTER(ctypes.c_byte)),
            ('stdin',w.HANDLE),('stdout',w.HANDLE),('stderr',w.HANDLE)]
    class Process(ctypes.Structure):
        _fields_ = [('process',w.HANDLE),('thread',w.HANDLE),('pid',w.DWORD),('tid',w.DWORD)]
    class StartupExtended(ctypes.Structure):
        _fields_ = [('startup',Startup),('attributes',ctypes.c_void_p)]
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p,w.LPCWSTR]
    kernel.CreateJobObjectW.restype = w.HANDLE
    kernel.SetInformationJobObject.argtypes = [w.HANDLE,ctypes.c_int,ctypes.c_void_p,w.DWORD]
    kernel.SetInformationJobObject.restype = w.BOOL
    kernel.QueryInformationJobObject.argtypes = [w.HANDLE,ctypes.c_int,ctypes.c_void_p,w.DWORD,ctypes.c_void_p]
    kernel.QueryInformationJobObject.restype = w.BOOL
    kernel.CreateProcessW.argtypes = [w.LPCWSTR,w.LPWSTR,ctypes.c_void_p,ctypes.c_void_p,w.BOOL,w.DWORD,
        ctypes.c_void_p,w.LPCWSTR,ctypes.POINTER(Startup),ctypes.POINTER(Process)]
    kernel.CreateProcessW.restype = w.BOOL
    kernel.AssignProcessToJobObject.argtypes = [w.HANDLE,w.HANDLE]
    kernel.AssignProcessToJobObject.restype = w.BOOL
    kernel.InitializeProcThreadAttributeList.argtypes = [ctypes.c_void_p,w.DWORD,w.DWORD,ctypes.POINTER(ctypes.c_size_t)]
    kernel.InitializeProcThreadAttributeList.restype = w.BOOL
    kernel.UpdateProcThreadAttribute.argtypes = [ctypes.c_void_p,w.DWORD,ctypes.c_size_t,ctypes.c_void_p,ctypes.c_size_t,ctypes.c_void_p,ctypes.c_void_p]
    kernel.UpdateProcThreadAttribute.restype = w.BOOL
    kernel.DeleteProcThreadAttributeList.argtypes = [ctypes.c_void_p]
    kernel.IsProcessInJob.argtypes = [w.HANDLE,w.HANDLE,ctypes.POINTER(w.BOOL)]
    kernel.IsProcessInJob.restype = w.BOOL
    kernel.ResumeThread.argtypes = [w.HANDLE]
    kernel.ResumeThread.restype = w.DWORD
    kernel.WaitForSingleObject.argtypes = [w.HANDLE,w.DWORD]
    kernel.WaitForSingleObject.restype = w.DWORD
    kernel.GetExitCodeProcess.argtypes = [w.HANDLE,ctypes.POINTER(w.DWORD)]
    kernel.GetExitCodeProcess.restype = w.BOOL
    kernel.TerminateJobObject.argtypes = [w.HANDLE,w.UINT]
    kernel.CloseHandle.argtypes = [w.HANDLE]
    job = kernel.CreateJobObjectW(None,None)
    assert job, ctypes.get_last_error()
    child = Process()
    attributes = None
    attributes_initialized = False
    try:
        limits = Extended()
        limits.basic.flags = 0x4 | 0x8 | 0x200 | 0x2000
        limits.basic.job_time = 100_000_000
        limits.basic.active = 1
        limits.job_memory = 1073741824
        assert kernel.SetInformationJobObject(job,9,ctypes.byref(limits),ctypes.sizeof(limits)),ctypes.get_last_error()
        queried = Extended()
        assert kernel.QueryInformationJobObject(job,9,ctypes.byref(queried),ctypes.sizeof(queried),None)
        assert (queried.basic.flags == limits.basic.flags and queried.job_memory == limits.job_memory
            and queried.basic.active == 1 and queried.basic.job_time == limits.basic.job_time)
        size = ctypes.c_size_t()
        assert not kernel.InitializeProcThreadAttributeList(None,1,0,ctypes.byref(size)) and size.value > 0
        attributes = ctypes.create_string_buffer(size.value)
        assert kernel.InitializeProcThreadAttributeList(attributes,1,0,ctypes.byref(size))
        attributes_initialized = True
        jobs = (w.HANDLE * 1)(job)
        assert kernel.UpdateProcThreadAttribute(attributes,0,0x2000d,jobs,ctypes.sizeof(jobs),None,None),ctypes.get_last_error()
        startup = StartupExtended()
        startup.startup.cb = ctypes.sizeof(startup)
        startup.attributes = ctypes.cast(attributes,ctypes.c_void_p)
        code = 'import os; import threading; t=threading.Thread(target=lambda: None); t.start(); t.join(); print("owned-bootstrap-probe-pass")'
        # Windows venv python.exe is a redirector and creates a second process.
        # The owned bootstrap invokes the actual interpreter, not that launcher.
        interpreter = str(Path(sys.base_prefix)/'python.exe')
        assert Path(interpreter).is_file()
        command = ctypes.create_unicode_buffer(subprocess.list2cmdline([interpreter,'-I','-c',code]))
        assert kernel.CreateProcessW(interpreter,command,None,None,False,4|0x80000,None,None,ctypes.byref(startup.startup),ctypes.byref(child)),ctypes.get_last_error()
        contained = w.BOOL()
        assert kernel.IsProcessInJob(child.process,job,ctypes.byref(contained)) and contained.value
        assert kernel.ResumeThread(child.thread) != 0xffffffff
        assert kernel.WaitForSingleObject(child.process,5000) == 0
        exit_code = w.DWORD()
        assert kernel.GetExitCodeProcess(child.process,ctypes.byref(exit_code)) and exit_code.value == 0
        return {'os':'windows','architecture':'64bit','aggregate_commit_bytes':1073741824,
            'job_user_cpu_seconds':10,'active_processes':1,'suspended_assigned_before_resume':True,
            'atomic_job_list_creation':True,
            'queried_limits_match':True,'owned_bootstrap_thread_works':True,'actual_exit_observed':True}
    finally:
        kernel.TerminateJobObject(job,99)
        if child.process:
            kernel.WaitForSingleObject(child.process,5000)
            kernel.CloseHandle(child.process)
        if child.thread:
            kernel.CloseHandle(child.thread)
        if attributes_initialized:
            kernel.DeleteProcThreadAttributeList(attributes)
        kernel.CloseHandle(job)


if __name__ == '__main__':
    result = windows() if sys.platform == 'win32' else linux() if sys.platform == 'linux' else None
    assert result is not None,'unsupported_host'
    result.update({'kind':'disposable-pre-code-os-api-probe','service_containment_qualified':False,
        'native_under_limits_qualified':False,'cpu_memory_exhaustion_qualified':False})
    print(json.dumps(result,sort_keys=True))
