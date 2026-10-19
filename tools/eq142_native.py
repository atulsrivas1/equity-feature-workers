"""EQ142 development owned-Job sampler with actual sampled native threads.

Preserves supplied_native containment/accounting/cleanup policy; separate code is
qualified separately. Observer CPU/RSS is outside the owned Job. No peak guarantee.
Sources: Microsoft PROCESSENTRY32W/CreateToolhelp32Snapshot/Process32FirstW docs.
"""
from __future__ import annotations
import ctypes as ct
from ctypes import wintypes as w
import json
import math
import os
from pathlib import Path
import subprocess
import time
import uuid
from supplied_native import (Kernel as BaseKernel, NativeFailure, ExtendedLimits,
    Startup, Process, Pids, Memory)
from eq142_identity import decimal_integer

NATIVE_RECEIPT_MAX_BYTES=16777216
NATIVE_ROWS_MAX_BYTES=15728640


class NativeEvidenceBuffer:
    """Bound encoded retained rows, not physical Python heap or sample peaks.

    The external observer has a separate host reserve. A rejected row makes the
    receipt incomplete; it never silently replaces or drops an accepted row.
    """
    def __init__(self,evidence):
        self.evidence=evidence;self.encoded_bytes=4;self.lifetimes=set()
        evidence['observed_lifetimes']=[]
        evidence['evidence_bounds']={'receipt_max_bytes':NATIVE_RECEIPT_MAX_BYTES,
            'rows_max_bytes':NATIVE_ROWS_MAX_BYTES,'max_samples':60001,'max_probe_errors':1024,
            'max_observed_lifetimes':256,
            'physical_memory_enforced':False}

    def observe_lifetime(self,pid,creation):
        if type(pid) is not int or not 1<=pid<=4294967295:raise NativeFailure('PROCESS_CREATION_UNKNOWN')
        try:decimal_integer(creation,18446744073709551615,positive=True)
        except ValueError:raise NativeFailure('PROCESS_CREATION_UNKNOWN') from None
        pair=(pid,creation)
        if pair in self.lifetimes:return
        if len(self.lifetimes)>=256:raise NativeFailure('PROCESS_LIFETIME_LIMIT')
        self.lifetimes.add(pair)
        self.evidence['observed_lifetimes']=[{'pid':p,'creation_filetime_100ns_since_1601_utc':c}
            for p,c in sorted(self.lifetimes,key=lambda row:(row[0],int(row[1])))]

    def append(self,kind,row):
        maximum={'samples':60001,'probe_errors':1024}[kind]
        size=len(json.dumps(row,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode())+1
        if len(self.evidence[kind])>=maximum or self.encoded_bytes+size>NATIVE_ROWS_MAX_BYTES:
            self.evidence['rejected_evidence']={'kind':kind,'row_bytes':size,
                'retained_rows_encoded_bytes':self.encoded_bytes,'retained_count':len(self.evidence[kind])}
            raise NativeFailure('NATIVE_EVIDENCE_LIMIT')
        self.evidence[kind].append(row);self.encoded_bytes+=size


class ProcessEntry(ct.Structure):
    _fields_=[('dwSize',w.DWORD),('cntUsage',w.DWORD),('th32ProcessID',w.DWORD),
        ('th32DefaultHeapID',ct.c_size_t),('th32ModuleID',w.DWORD),('cntThreads',w.DWORD),
        ('th32ParentProcessID',w.DWORD),('pcPriClassBase',w.LONG),('dwFlags',w.DWORD),
        ('szExeFile',w.WCHAR*260)]


class Kernel(BaseKernel):
    def __init__(self):
        super().__init__()
        for name,args,result in [
            ('CreateToolhelp32Snapshot',[w.DWORD,w.DWORD],w.HANDLE),
            ('Process32FirstW',[w.HANDLE,ct.POINTER(ProcessEntry)],w.BOOL),
            ('Process32NextW',[w.HANDLE,ct.POINTER(ProcessEntry)],w.BOOL),
            ('GetProcessTimes',[w.HANDLE]+[ct.POINTER(w.FILETIME)]*4,w.BOOL)]:
            function=getattr(self.library,name);function.argtypes=args;function.restype=result
            setattr(self,name,function)

    def creation_identity(self,handle):
        created=w.FILETIME();exited=w.FILETIME();system=w.FILETIME();user=w.FILETIME()
        if not self.GetProcessTimes(handle,ct.byref(created),ct.byref(exited),ct.byref(system),ct.byref(user)):
            raise NativeFailure('PROCESS_CREATION_API',{'winerror':ct.get_last_error()})
        value=(int(created.dwHighDateTime)<<32)|int(created.dwLowDateTime)
        if value<=0:raise NativeFailure('PROCESS_CREATION_UNKNOWN')
        return str(value)

    def threads_for_members(self,job,pids,expected_lifetimes=None):
        if type(pids) is not list or len(pids)>256 or any(type(pid) is not int or not 1<=pid<=4294967295 for pid in pids) or len(set(pids))!=len(pids):
            raise NativeFailure('THREAD_PROBE')
        if expected_lifetimes is not None:
            if (type(expected_lifetimes) is not dict or any(type(pid) is not int for pid in expected_lifetimes)
                    or set(expected_lifetimes)!=set(pids)):raise NativeFailure('THREAD_PROBE')
            try:
                for creation in expected_lifetimes.values():decimal_integer(creation,18446744073709551615,positive=True)
            except ValueError:raise NativeFailure('THREAD_PROBE') from None
        self.last_thread_dispositions={}
        if not pids:return {}
        handles=[];snapshot=None;creations={}
        try:
            for pid in pids:
                handle=self.OpenProcess(0x100410,False,pid)
                if not handle:raise NativeFailure('OPEN_PROCESS')
                handles.append(handle)
                member=w.BOOL()
                if not self.IsProcessInJob(handle,job,ct.byref(member)) or not member.value:
                    raise NativeFailure('MEMBERSHIP_RACE')
                creation=self.creation_identity(handle);creations[pid]=creation
                if expected_lifetimes is not None and creation!=expected_lifetimes[pid]:
                    raise NativeFailure('PROCESS_LIFETIME_RACE')
            snapshot=self.CreateToolhelp32Snapshot(0x00000002,0)
            if not snapshot or snapshot==ct.c_void_p(-1).value:
                snapshot=None;raise NativeFailure('SNAPSHOT')
            entry=ProcessEntry();entry.dwSize=ct.sizeof(entry)
            ct.set_last_error(0)
            if not self.Process32FirstW(snapshot,ct.byref(entry)):raise NativeFailure('PROCESS_ENUMERATION')
            counts={};wanted=set(pids)
            while True:
                if int(entry.th32ProcessID) in wanted:
                    counts[int(entry.th32ProcessID)]=int(entry.cntThreads)
                ct.set_last_error(0)
                if not self.Process32NextW(snapshot,ct.byref(entry)):
                    if ct.get_last_error()!=18:raise NativeFailure('PROCESS_ENUMERATION')
                    break
            for pid,handle in zip(pids,handles):
                member=w.BOOL()
                if not self.IsProcessInJob(handle,job,ct.byref(member)) or not member.value:
                    raise NativeFailure('MEMBERSHIP_RACE')
                if self.creation_identity(handle)!=creations[pid]:raise NativeFailure('PROCESS_LIFETIME_RACE')
                state=self.WaitForSingleObject(handle,0)
                if state==0:
                    self.last_thread_dispositions[pid]={'state':'observed_terminated','terminal_wait':0,
                        'snapshot_threads':counts.get(pid),'creation_filetime_100ns_since_1601_utc':creations[pid]}
                    counts[pid]=0
                elif state==258:
                    if pid not in counts or counts[pid]<=0:raise NativeFailure('MISSING_OWNED_PID')
                    self.last_thread_dispositions[pid]={'state':'observed_live','terminal_wait':258,
                        'snapshot_threads':counts[pid],'creation_filetime_100ns_since_1601_utc':creations[pid]}
                else:raise NativeFailure('THREAD_PROBE')
            return counts
        finally:
            if snapshot:self.CloseHandle(snapshot)
            for handle in handles:self.CloseHandle(handle)


def run_owned(command, *, cwd: Path, timeout_s=120, interval_s=0.01, reject_assignment=False, reject_resume=False):
    """Run argv with {job_name} substitution; never invoke a shell or visible window.

    Fault switches are qualification controls only. Child report paths belong to
    the caller. Returned evidence contains no argv/full local source paths.
    """
    if (type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 600
            or type(interval_s) not in (int,float) or not math.isfinite(interval_s) or not 0 < interval_s <= 0.1):
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
    buffer=NativeEvidenceBuffer(evidence)
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
                pid = int(raw)
                if pid not in seen and len(seen)>=256:raise NativeFailure('PROCESS_INVENTORY_LIMIT')
                seen.add(pid)
                handle = kernel.OpenProcess(0x0410, False, pid)
                if not handle:
                    buffer.append('probe_errors',{'pid': pid, 'code': 'OPEN_PROCESS', 'winerror': ct.get_last_error()})
                    continue
                try:
                    membership = w.BOOL()
                    if not kernel.IsProcessInJob(handle, job, ct.byref(membership)) or not membership.value:
                        buffer.append('probe_errors',{'pid': pid, 'code': 'MEMBERSHIP_RACE'})
                        continue
                    creation=kernel.creation_identity(handle)
                    memory = Memory(); memory.cb = ct.sizeof(memory)
                    if not kernel.memory(handle, ct.byref(memory), memory.cb):
                        buffer.append('probe_errors',{'pid': pid, 'code': 'RSS_PROBE', 'winerror': ct.get_last_error()})
                        continue
                    if kernel.creation_identity(handle)!=creation:raise NativeFailure('PROCESS_LIFETIME_RACE')
                    buffer.observe_lifetime(pid,creation)
                    rows.append({'pid': pid, 'rss_bytes': int(memory.working_set),
                        'creation_filetime_100ns_since_1601_utc':creation})
                finally:
                    kernel.CloseHandle(handle)
            thread_counts = {}
            try:
                thread_counts = kernel.threads_for_members(job, [row['pid'] for row in rows],
                    {row['pid']:row['creation_filetime_100ns_since_1601_utc'] for row in rows})
            except NativeFailure as issue:
                buffer.append('probe_errors',{'code': issue.code, 'winerror': ct.get_last_error()})
            for row in rows:
                row['native_threads'] = thread_counts.get(row['pid'])
                row['thread_disposition'] = kernel.last_thread_dispositions.get(row['pid'])
            buffer.append('samples',{'native_thread_count': sum(thread_counts.values()) if len(thread_counts) == len(rows) else None,
                'elapsed_ns': time.perf_counter_ns() - started,
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
            aggregate_sampled_peak_native_threads=max((sample['native_thread_count'] for sample in evidence['samples'] if sample['native_thread_count'] is not None), default=None),
            thread_limits='Toolhelp32 sampled counts joined to verified owned Job members; transient peaks can be missed; membership/snapshot are not atomic; observer is outside owned Job',
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
