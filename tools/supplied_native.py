"""Owned Windows benchmark accounting, outside calculations and worker scheduling.

One fresh Job contains one suspended-then-admitted runner and its descendants.
CPU includes terminated members. RSS is sampled, not a physical memory limit.
"""
from __future__ import annotations

import ctypes as ct
from ctypes import wintypes as w
import os
from pathlib import Path
import subprocess
import time
import uuid


class NativeFailure(RuntimeError):
    def __init__(self, code, evidence=None):
        self.code = code
        self.evidence = evidence
        super().__init__('Owned benchmark accounting failed: ' + code)


class Accounting(ct.Structure):
    _fields_ = [(name, ct.c_longlong) for name in ('user', 'kernel', 'period_user', 'period_kernel')] + [
        (name, w.DWORD) for name in ('page_faults', 'total_processes', 'active_processes', 'terminated_by_limit')]


class IOCounts(ct.Structure):
    _fields_ = [(name, ct.c_ulonglong) for name in ('read_operations', 'write_operations', 'other_operations', 'read_bytes', 'write_bytes', 'other_bytes')]


class Limits(ct.Structure):
    _fields_ = [('process_time', ct.c_longlong), ('job_time', ct.c_longlong), ('flags', w.DWORD),
        ('minimum_working_set', ct.c_size_t), ('maximum_working_set', ct.c_size_t), ('active_limit', w.DWORD),
        ('affinity', ct.c_size_t), ('priority', w.DWORD), ('scheduling', w.DWORD)]


class ExtendedLimits(ct.Structure):
    _fields_ = [('basic', Limits), ('io', IOCounts)] + [(name, ct.c_size_t) for name in (
        'process_memory_limit', 'job_memory_limit', 'peak_process_private_commit', 'peak_job_private_commit')]


class Memory(ct.Structure):
    _fields_ = [('cb', w.DWORD), ('page_faults', w.DWORD)] + [(name, ct.c_size_t) for name in (
        'peak_working_set', 'working_set', 'peak_paged_pool', 'paged_pool', 'peak_nonpaged_pool', 'nonpaged_pool', 'pagefile', 'peak_pagefile')]


class Startup(ct.Structure):
    _fields_ = [('cb', w.DWORD), ('reserved', w.LPWSTR), ('desktop', w.LPWSTR), ('title', w.LPWSTR)] + [
        (name, w.DWORD) for name in ('x', 'y', 'x_size', 'y_size', 'x_chars', 'y_chars', 'fill', 'flags')] + [
        ('show', w.WORD), ('reserved_count', w.WORD), ('reserved_bytes', ct.POINTER(ct.c_ubyte)),
        ('stdin', w.HANDLE), ('stdout', w.HANDLE), ('stderr', w.HANDLE)]


class Process(ct.Structure):
    _fields_ = [('process', w.HANDLE), ('thread', w.HANDLE), ('pid', w.DWORD), ('tid', w.DWORD)]


class Pids(ct.Structure):
    _fields_ = [('assigned', w.DWORD), ('count', w.DWORD), ('pids', ct.c_size_t * 256)]


class Kernel:
    def __init__(self):
        if os.name != 'nt':
            raise NativeFailure('WINDOWS_ONLY')
        self.library = ct.WinDLL('kernel32', use_last_error=True)
        self.psapi = ct.WinDLL('psapi', use_last_error=True)
        signatures = {
            'CreateJobObjectW': ([ct.c_void_p, w.LPCWSTR], w.HANDLE),
            'OpenJobObjectW': ([w.DWORD, w.BOOL, w.LPCWSTR], w.HANDLE),
            'AssignProcessToJobObject': ([w.HANDLE, w.HANDLE], w.BOOL),
            'SetInformationJobObject': ([w.HANDLE, ct.c_int, ct.c_void_p, w.DWORD], w.BOOL),
            'QueryInformationJobObject': ([w.HANDLE, ct.c_int, ct.c_void_p, w.DWORD, ct.POINTER(w.DWORD)], w.BOOL),
            'CreateProcessW': ([w.LPCWSTR, w.LPWSTR, ct.c_void_p, ct.c_void_p, w.BOOL, w.DWORD,
                ct.c_void_p, w.LPCWSTR, ct.POINTER(Startup), ct.POINTER(Process)], w.BOOL),
            'ResumeThread': ([w.HANDLE], w.DWORD),
            'WaitForSingleObject': ([w.HANDLE, w.DWORD], w.DWORD),
            'GetExitCodeProcess': ([w.HANDLE, ct.POINTER(w.DWORD)], w.BOOL),
            'TerminateJobObject': ([w.HANDLE, w.UINT], w.BOOL),
            'TerminateProcess': ([w.HANDLE, w.UINT], w.BOOL),
            'OpenProcess': ([w.DWORD, w.BOOL, w.DWORD], w.HANDLE),
            'IsProcessInJob': ([w.HANDLE, w.HANDLE, ct.POINTER(w.BOOL)], w.BOOL),
            'CloseHandle': ([w.HANDLE], w.BOOL),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.library, name)
            function.argtypes = arguments; function.restype = result
            setattr(self, name, function)
        self.memory = self.psapi.GetProcessMemoryInfo
        self.memory.argtypes = [w.HANDLE, ct.POINTER(Memory), w.DWORD]; self.memory.restype = w.BOOL

    def query(self, job, kind, record):
        if not self.QueryInformationJobObject(job, kind, ct.byref(record), ct.sizeof(record), None):
            raise NativeFailure('QUERY_' + str(kind))
        return record

    def accounting(self, job):
        record = self.query(job, 1, Accounting())
        return {name: int(getattr(record, name)) for name, _ in Accounting._fields_}


def counters(job_name):
    """A computing runner queries only its explicitly named owned benchmark Job."""
    kernel = Kernel(); job = kernel.OpenJobObjectW(0x0004, False, job_name)
    if not job:
        raise NativeFailure('OPEN_OWNED_JOB')
    try:
        return kernel.accounting(job)
    finally:
        kernel.CloseHandle(job)


def run_owned(command, *, cwd: Path, timeout_s=120, interval_s=0.01, reject_assignment=False, reject_resume=False):
    """Run argv with {job_name} substitution; never invoke a shell or visible window.

    Fault switches are qualification controls only. Child report paths belong to
    the caller. Returned evidence contains no argv/full local source paths.
    """
    if type(timeout_s) not in (int, float) or not 0 < timeout_s <= 600 or not 0 < interval_s <= 0.1:
        raise NativeFailure('INVALID_LIMITS')
    kernel = Kernel(); name = 'Local\\equity-feature-supplied-' + uuid.uuid4().hex
    ct.set_last_error(0)
    job = kernel.CreateJobObjectW(None, name)
    if not job or ct.get_last_error() == 183:
        if job: kernel.CloseHandle(job)
        raise NativeFailure('CREATE_UNIQUE_JOB')
    process = Process(); assigned = False; exited = False; exit_code = None; seen = set()
    evidence = {'job_name': name, 'samples': [], 'probe_errors': [], 'sample_interval_s': interval_s,
        'observer_clock_nominal_resolution_s': time.get_clock_info('process_time').resolution,
        'wall_clock_nominal_resolution_s': time.get_clock_info('perf_counter').resolution,
        'containment': 'owned job; kill on close; no breakaway', 'hard_memory_limit': False,
        'rss_limits': 'sampled simultaneous working sets; transient peaks/short-lived members may be missed',
        'cpu_limits': 'whole Job user/kernel100ns counters including terminated descendants; observed accounting is quantized, including possible zero observer/short-phase CPU; nominal units are not empirical resolution'}
    outer_cpu = time.process_time_ns(); started = time.perf_counter_ns(); deadline = time.monotonic() + timeout_s
    try:
        limits = ExtendedLimits(); limits.basic.flags = 0x2000
        if not kernel.SetInformationJobObject(job, 9, ct.byref(limits), ct.sizeof(limits)):
            raise NativeFailure('SET_OWNED_CONTAINMENT')
        argv = [str(part).replace('{job_name}', name) for part in command]
        if not argv:
            raise NativeFailure('EMPTY_COMMAND')
        startup = Startup(); startup.cb = ct.sizeof(startup); startup.flags = 1; startup.show = 0
        line = ct.create_unicode_buffer(subprocess.list2cmdline(argv))
        if not kernel.CreateProcessW(argv[0], line, None, None, False, 0x08000004, None,
                                     str(cwd), ct.byref(startup), ct.byref(process)):
            raise NativeFailure('CREATE_SUSPENDED_RUNNER')
        evidence['runner_pid'] = int(process.pid)
        if reject_assignment or not kernel.AssignProcessToJobObject(job, process.process):
            raise NativeFailure('ASSIGN_SUSPENDED_RUNNER')
        assigned = True
        before = kernel.accounting(job)
        if before['total_processes'] != 1 or before['active_processes'] != 1:
            raise NativeFailure('SUSPENDED_MEMBERSHIP')
        evidence['suspended_accounting'] = before
        if reject_resume or kernel.ResumeThread(process.thread) != 1:
            raise NativeFailure('RESUME_ADMITTED_RUNNER')
        kernel.CloseHandle(process.thread); process.thread = None
        while True:
            accounting = kernel.accounting(job)
            pids = kernel.query(job, 3, Pids())
            if pids.count > 256 or pids.assigned > 256:
                raise NativeFailure('PROCESS_INVENTORY_LIMIT')
            rows = []
            for raw in pids.pids[:pids.count]:
                pid = int(raw); seen.add(pid)
                handle = kernel.OpenProcess(0x0410, False, pid)
                if not handle:
                    evidence['probe_errors'].append({'pid': pid, 'code': 'OPEN_PROCESS', 'winerror': ct.get_last_error()})
                    continue
                try:
                    membership = w.BOOL()
                    if not kernel.IsProcessInJob(handle, job, ct.byref(membership)) or not membership.value:
                        evidence['probe_errors'].append({'pid': pid, 'code': 'MEMBERSHIP_RACE'})
                        continue
                    memory = Memory(); memory.cb = ct.sizeof(memory)
                    if not kernel.memory(handle, ct.byref(memory), memory.cb):
                        evidence['probe_errors'].append({'pid': pid, 'code': 'RSS_PROBE', 'winerror': ct.get_last_error()})
                        continue
                    rows.append({'pid': pid, 'rss_bytes': int(memory.working_set)})
                finally:
                    kernel.CloseHandle(handle)
            evidence['samples'].append({'elapsed_ns': time.perf_counter_ns() - started,
                'active_processes': accounting['active_processes'], 'total_processes': accounting['total_processes'],
                'rss_bytes': sum(row['rss_bytes'] for row in rows), 'processes': rows})
            if process.process and kernel.WaitForSingleObject(process.process, 0) == 0:
                value = w.DWORD()
                if not kernel.GetExitCodeProcess(process.process, ct.byref(value)):
                    raise NativeFailure('RUNNER_EXIT_CODE')
                exit_code = int(value.value); exited = True
                kernel.CloseHandle(process.process); process.process = None
            if exited and accounting['active_processes'] == 0:
                break
            if time.monotonic() >= deadline:
                raise NativeFailure('OWNED_DEADLINE')
            time.sleep(interval_s)
        final = kernel.accounting(job)
        extended = kernel.query(job, 9, ExtendedLimits())
        evidence.update(exit_code=exit_code, wall_ns=time.perf_counter_ns() - started,
            cpu_user_100ns=final['user'], cpu_kernel_100ns=final['kernel'],
            cpu_seconds=(final['user'] + final['kernel']) / 10000000,
            final_accounting=final, pids_observed=sorted(seen),
            aggregate_sampled_peak_rss_bytes=max((sample['rss_bytes'] for sample in evidence['samples']), default=0),
            peak_job_private_commit_bytes=int(extended.peak_job_private_commit),
            external_observer_cpu_ns=time.process_time_ns() - outer_cpu)
        if final['active_processes'] or final['terminated_by_limit']:
            raise NativeFailure('FINAL_MEMBERSHIP')
        return evidence
    except NativeFailure as error:
        evidence['failure'] = error.code
        evidence['runner_exited_before_failure'] = exited
        evidence['runner_exit_code_before_failure'] = exit_code
        evidence['assigned_to_owned_job'] = assigned
        raise NativeFailure(error.code, evidence) from None
    finally:
        # Only this created runner/job is ever terminated. No PID-based deletion.
        if process.process and not assigned:
            evidence['cleanup_runner_termination_issued'] = bool(kernel.TerminateProcess(process.process, 1))
            evidence['cleanup_runner_reaped'] = kernel.WaitForSingleObject(process.process, 5000) == 0
            if not evidence['cleanup_runner_reaped']:
                evidence['cleanup_failed'] = True
        if assigned:
            remaining = kernel.accounting(job)
            if remaining['active_processes']:
                evidence['cleanup_job_termination_issued'] = bool(kernel.TerminateJobObject(job, 1))
                stop = time.monotonic() + 5
                while kernel.accounting(job)['active_processes'] and time.monotonic() < stop:
                    time.sleep(0.01)
                evidence['cleanup_accounting'] = kernel.accounting(job)
                if evidence['cleanup_accounting']['active_processes']:
                    evidence['cleanup_failed'] = True
        if process.thread: kernel.CloseHandle(process.thread)
        if process.process: kernel.CloseHandle(process.process)
        kernel.CloseHandle(job)
