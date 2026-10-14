"""Identity-bound observation for the owned Windows qualification fixture only."""
import ctypes
from ctypes import wintypes as w
import math
import time


def _note(primary, label, error):
    try:
        primary.add_note(label + type(error).__name__)
    except BaseException:
        pass  # Diagnostic failure must never replace the retained primary error.


def _creation(kernel, handle):
    times = [w.FILETIME() for _ in range(4)]
    if not kernel.GetProcessTimes(handle, *(ctypes.byref(value) for value in times)):
        raise OSError('owned entry creation unavailable')
    creation = times[0].dwLowDateTime | (times[0].dwHighDateTime << 32)
    if creation <= 0:
        raise OSError('owned entry creation invalid')
    return creation


def retain_entry(kernel, job, pid):
    if type(pid) is not int or not 0 < pid <= 0xffffffff:
        raise ValueError('owned entry PID invalid')
    if type(job) is not int or job <= 0:
        raise ValueError('owned entry original job unavailable')
    kernel.GetProcessId.argtypes = [w.HANDLE]
    kernel.GetProcessId.restype = w.DWORD
    kernel.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    kernel.WaitForSingleObject.restype = w.DWORD
    handle = kernel.OpenProcess(0x100000 | 0x1000, False, pid)
    if not handle:
        raise OSError('owned entry observer unavailable')
    try:
        if kernel.GetProcessId(handle) != pid:
            raise OSError('owned entry PID changed')
        present = w.BOOL()
        if not kernel.IsProcessInJob(handle, job, ctypes.byref(present)) or not present.value:
            raise OSError('owned entry original job unverified')
        creation = _creation(kernel, handle)
        status = w.DWORD()
        if not kernel.GetExitCodeProcess(handle, ctypes.byref(status)) or status.value != 259:
            raise OSError('owned entry not live before controlled fault')
        return handle, pid, creation
    except BaseException as primary:
        try:
            if not kernel.CloseHandle(handle):
                raise OSError('owned entry observer close failed')
        except BaseException as cleanup:
            _note(primary, 'observer acquisition cleanup: ', cleanup)
        raise


def settle_entry(kernel, entry, deadline, clock=time.monotonic):
    handle, pid, creation = entry
    before = clock()
    if type(before) not in (int, float) or type(deadline) not in (int, float) or not math.isfinite(before) or not math.isfinite(deadline) or not 0 <= deadline - before <= 5:
        raise OSError('owned entry settle deadline expired')
    remaining_ms = int((deadline - before) * 1000)
    waited = kernel.WaitForSingleObject(handle, remaining_ms)
    if type(waited) is not int or waited != 0:
        raise OSError('owned entry did not signal within settle budget')
    if kernel.GetProcessId(handle) != pid or _creation(kernel, handle) != creation:
        raise OSError('retained owned entry identity changed')
    status = w.DWORD()
    if not kernel.GetExitCodeProcess(handle, ctypes.byref(status)) or status.value == 259:
        raise OSError('retained owned entry exit unavailable')
    after = clock()
    if type(after) not in (int, float) or not math.isfinite(after) or not before <= after <= deadline:
        raise OSError('owned entry settle deadline expired')
    return int(status.value)


def finish_entry_observation(owner, process, entry, primary):
    """Attempt each owned cleanup independently without replacing the first failure."""
    errors = []
    operations = [lambda: owner.close(failed=True)]
    process = process or owner.process
    if process is not None:
        operations.append(lambda: process.communicate(timeout=5))
    if entry is not None:
        def close_observer():
            if not owner.kernel.CloseHandle(entry[0]):
                raise OSError('owned entry observer close failed')
        operations.append(close_observer)
    for operation in operations:
        try:
            operation()
        except BaseException as error:
            errors.append(error)
    if primary is not None:
        for error in errors:
            _note(primary, 'owned qualification cleanup: ', error)
    elif errors:
        for error in errors[1:]:
            _note(errors[0], 'additional owned qualification cleanup: ', error)
        raise errors[0]
