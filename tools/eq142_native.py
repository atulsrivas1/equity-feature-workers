"""EQ142 development owned-Job sampler with actual sampled native threads.

Preserves supplied_native containment/accounting/cleanup policy; separate code is
qualified separately. Observer CPU/RSS is outside the owned Job. No peak guarantee.
Sources: Microsoft PROCESSENTRY32W/CreateToolhelp32Snapshot/Process32FirstW docs.
"""
from __future__ import annotations
import ctypes as ct
from ctypes import wintypes as w
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import subprocess
import time
import uuid
from supplied_native import (Kernel as BaseKernel, NativeFailure, ExtendedLimits,
    Startup, Process, Pids, Memory)
from eq142_identity import decimal_integer,clock_valid

NATIVE_RECEIPT_MAX_BYTES=16777216
NATIVE_ROWS_MAX_BYTES=15728640
BOUNDARIES=('observer_start_ns','runner_created_ns','runner_assigned_ns',
    'runner_resume_before_ns','runner_resume_after_ns','runner_exit_observed_ns',
    'job_empty_observed_ns','final_accounting_end_ns','native_measurement_end_ns')


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


def cleanup_owned(kernel,process,job,*,assigned,evidence):
    """Attempt each owned cleanup stage; never replace the primary exception."""
    failures=[];cleanup_stop=time.monotonic()+5
    def failed(stage,error):
        failures.append({'stage':stage,'error_type':type(error).__module__+'.'+type(error).__qualname__,
            'code':error.code if isinstance(error,NativeFailure) else None})
    def attempt(stage,operation,*,truth=False):
        try:
            value=operation()
            if truth and not value:raise NativeFailure('CLEANUP_API_FALSE')
            return value
        except BaseException as error:
            failed(stage,error);return None
    def query(stage):
        value=attempt(stage,lambda:kernel.accounting(job))
        if value is None:return None
        if (type(value) is not dict or type(value.get('active_processes')) is not int
                or not 0<=value['active_processes']<=4294967295):
            failed(stage,NativeFailure('CLEANUP_ACCOUNTING_INVALID'));return None
        return value
    def reap(stage,handle):
        remaining_ms=max(0,min(5000,int((cleanup_stop-time.monotonic())*1000)))
        return attempt(stage,lambda:kernel.WaitForSingleObject(handle,remaining_ms))
    if process.process and not assigned:
        evidence['cleanup_runner_termination_issued']=bool(attempt('terminate_unassigned_runner',
            lambda:kernel.TerminateProcess(process.process,1),truth=True))
        state=reap('reap_unassigned_runner',process.process)
        evidence['cleanup_runner_reaped']=state==0
        if state!=0:failed('reap_unassigned_runner',NativeFailure('CLEANUP_RUNNER_NOT_REAPED'))
    if assigned:
        remaining=query('query_owned_job')
        if remaining is None or remaining['active_processes']:
            evidence['cleanup_job_termination_issued']=bool(attempt('terminate_owned_job',
                lambda:kernel.TerminateJobObject(job,1),truth=True))
            while True:
                remaining=query('reap_owned_job')
                if remaining is None or remaining['active_processes']==0:break
                if time.monotonic()>=cleanup_stop:
                    failed('reap_owned_job',NativeFailure('CLEANUP_JOB_NOT_REAPED'));break
                time.sleep(0.01)
            evidence['cleanup_accounting']=remaining
            if process.process:
                state=reap('reap_owned_runner',process.process)
                evidence['cleanup_runner_reaped']=state==0
                if state!=0:failed('reap_owned_runner',NativeFailure('CLEANUP_RUNNER_NOT_REAPED'))
    # Each close is independent of preceding queries/termination/other closes.
    for attribute,stage in (('thread','close_runner_thread'),('process','close_runner_process')):
        handle=getattr(process,attribute)
        if handle:
            closed=attempt(stage,lambda h=handle:kernel.CloseHandle(h),truth=True)
            if closed:setattr(process,attribute,None)
    if job:attempt('close_owned_job',lambda:kernel.CloseHandle(job),truth=True)
    if failures:
        evidence['cleanup_failed']=True;evidence['cleanup_failures']=failures
    return failures


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
        for name,args,result in (
            ('GetCurrentProcess',[],w.HANDLE),('GetProcessId',[w.HANDLE],w.DWORD),
            ('QueryPerformanceFrequency',[ct.POINTER(ct.c_longlong)],w.BOOL)):
            function=getattr(self.library,name);function.argtypes=args;function.restype=result
            setattr(self,name,function)

    def observer_identity(self,job):
        current=self.GetCurrentProcess();pid=int(self.GetProcessId(current))
        if not pid or pid!=os.getpid():raise NativeFailure('OBSERVER_PROCESS_ID')
        member=w.BOOL()
        if not self.IsProcessInJob(current,job,ct.byref(member)):raise NativeFailure('OBSERVER_MEMBERSHIP_API')
        if member.value:raise NativeFailure('OBSERVER_IN_OWNED_JOB')
        frequency=ct.c_longlong()
        if not self.QueryPerformanceFrequency(ct.byref(frequency)):raise NativeFailure('OBSERVER_QPF_API')
        info=time.get_clock_info('perf_counter')
        clock={'name':'perf_counter','implementation':info.implementation,'monotonic':info.monotonic,
            'adjustable':info.adjustable,'reported_resolution_seconds':format(Decimal(str(info.resolution)),'f'),
            'qpc_frequency_hz':int(frequency.value),'cross_process_raw_ticks_qualified':False}
        try:clock_valid(clock,require_frequency=True)
        except ValueError:raise NativeFailure('OBSERVER_CLOCK') from None
        return {'pid':pid,'creation_filetime_100ns_since_1601_utc':self.creation_identity(current),
            'is_member':False,'clock':clock}

    def runner_identity(self,handle,job,pid):
        if int(self.GetProcessId(handle))!=pid:raise NativeFailure('RUNNER_PROCESS_ID')
        member=w.BOOL()
        if not self.IsProcessInJob(handle,job,ct.byref(member)) or not member.value:
            raise NativeFailure('RUNNER_MEMBERSHIP')
        return {'pid':pid,'creation_filetime_100ns_since_1601_utc':self.creation_identity(handle),
            'is_member':True,'terminal_wait':None,'terminal_creation_filetime_100ns_since_1601_utc':None}

    def runner_wait(self,handle):
        state=self.WaitForSingleObject(handle,0)
        if state not in (0,258):raise NativeFailure('RUNNER_WAIT')
        return state

    def creation_identity(self,handle):
        created=w.FILETIME();exited=w.FILETIME();system=w.FILETIME();user=w.FILETIME()
        if not self.GetProcessTimes(handle,ct.byref(created),ct.byref(exited),ct.byref(system),ct.byref(user)):
            raise NativeFailure('PROCESS_CREATION_API',{'winerror':ct.get_last_error()})
        value=(int(created.dwHighDateTime)<<32)|int(created.dwLowDateTime)
        if value<=0:raise NativeFailure('PROCESS_CREATION_UNKNOWN')
        return str(value)

    def close_probe_handles(self,handles,primary=None):
        """Close all actual probe handles; retain primary errors and close faults."""
        failures=[]
        for ordinal,(kind,handle) in enumerate(handles):
            try:
                if not self.CloseHandle(handle):raise NativeFailure('PROBE_CLOSE_API_FALSE')
            except BaseException as error:
                failures.append({'ordinal':ordinal,'kind':kind,
                    'error_type':type(error).__module__+'.'+type(error).__qualname__,
                    'code':error.code if isinstance(error,NativeFailure) else None})
        if failures:
            if primary is not None:
                if isinstance(primary,NativeFailure):
                    primary.evidence={**(primary.evidence or {}),'probe_handle_close_failures':failures}
                primary.add_note('Independent owned probe handle close failures retained')
            else:raise NativeFailure('PROBE_HANDLE_CLEANUP',{'probe_handle_close_failures':failures})

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
        handles=[];snapshot=None;creations={};primary=None
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
        except BaseException as error:
            primary=error;raise
        finally:
            self.close_probe_handles(([('snapshot',snapshot)] if snapshot else [])+
                [('member',handle) for handle in handles],primary)


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
        # The returned reference may name an existing Job. Only close this
        # caller's reference; never configure, assign to or terminate that Job.
        evidence={'failure':'CREATE_UNIQUE_JOB','stage':'unique_job_refusal',
            'primary_failure_identity':{'error_type':NativeFailure.__module__+'.'+NativeFailure.__qualname__,
                'code':'CREATE_UNIQUE_JOB'},'reference_opened':bool(job),
            'reference_close_attempted':bool(job),'reference_close_succeeded':None}
        primary=NativeFailure('CREATE_UNIQUE_JOB',evidence)
        if job:
            try:
                if not kernel.CloseHandle(job):
                    evidence['reference_close_succeeded']=False
                    raise NativeFailure('CLOSE_COLLIDING_JOB_REFERENCE')
                evidence['reference_close_succeeded']=True
            except BaseException as error:
                evidence['cleanup_failed']=True
                evidence['cleanup_failures']=[{'stage':'close_colliding_job_reference',
                    'error_type':type(error).__module__+'.'+type(error).__qualname__,
                    'code':error.code if isinstance(error,NativeFailure) else None}]
                primary.add_note('Independent colliding Job reference close failure retained')
        raise primary
    process = Process(); assigned = False; exited = False; exit_code = None; seen = set();primary=None
    evidence = {'job_name': name, 'samples': [], 'probe_errors': [], 'sample_interval_s': interval_s,
        'observer_clock_nominal_resolution_s': time.get_clock_info('process_time').resolution,
        'wall_clock_nominal_resolution_s': time.get_clock_info('perf_counter').resolution,
        'containment': 'owned job; kill on close; no breakaway', 'hard_memory_limit': False,
        'rss_limits': 'sampled simultaneous working sets; transient peaks/short-lived members may be missed',
        'cpu_limits': 'whole Job user/kernel100ns counters including terminated descendants; observed accounting is quantized, including possible zero observer/short-phase CPU; nominal units are not empirical resolution'}
    buffer=NativeEvidenceBuffer(evidence)
    evidence['schema']='eq142-development-native-receipt:2'
    closure={'schema':'eq142-native-local-closure:1','observer':None,'runner':None,
        'boundaries':dict.fromkeys(BOUNDARIES),'job_empty_accounting':None,
        'scope':'native_local_not_selected_complete_e2e'}
    evidence['native_closure']=closure;boundaries=closure['boundaries']
    outer_cpu = time.process_time_ns(); started = time.perf_counter_ns(); deadline = time.monotonic() + timeout_s
    boundaries['observer_start_ns']=str(started)
    try:
        closure['observer']=kernel.observer_identity(job)
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
        boundaries['runner_created_ns']=str(time.perf_counter_ns())
        if reject_assignment or not kernel.AssignProcessToJobObject(job, process.process):
            raise NativeFailure('ASSIGN_SUSPENDED_RUNNER')
        assigned = True
        closure['runner']=kernel.runner_identity(process.process,job,int(process.pid))
        boundaries['runner_assigned_ns']=str(time.perf_counter_ns())
        before = kernel.accounting(job)
        if before['total_processes'] != 1 or before['active_processes'] != 1:
            raise NativeFailure('SUSPENDED_MEMBERSHIP')
        evidence['suspended_accounting'] = before
        boundaries['runner_resume_before_ns']=str(time.perf_counter_ns())
        if reject_resume or kernel.ResumeThread(process.thread) != 1:
            raise NativeFailure('RESUME_ADMITTED_RUNNER')
        boundaries['runner_resume_after_ns']=str(time.perf_counter_ns())
        if not kernel.CloseHandle(process.thread):raise NativeFailure('CLOSE_RUNNER_THREAD')
        process.thread = None
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
                probe_primary=None
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
                except BaseException as error:
                    probe_primary=error;raise
                finally:
                    kernel.close_probe_handles([('rss_member',handle)],probe_primary)
            thread_counts = {}
            try:
                thread_counts = kernel.threads_for_members(job, [row['pid'] for row in rows],
                    {row['pid']:row['creation_filetime_100ns_since_1601_utc'] for row in rows})
            except NativeFailure as issue:
                if issue.evidence is not None and 'probe_handle_close_failures' in issue.evidence:raise
                buffer.append('probe_errors',{'code': issue.code, 'winerror': ct.get_last_error()})
            for row in rows:
                row['native_threads'] = thread_counts.get(row['pid'])
                row['thread_disposition'] = kernel.last_thread_dispositions.get(row['pid'])
            buffer.append('samples',{'native_thread_count': sum(thread_counts.values()) if len(thread_counts) == len(rows) else None,
                'elapsed_ns': time.perf_counter_ns() - started,
                'active_processes': accounting['active_processes'], 'total_processes': accounting['total_processes'],
                'rss_bytes': sum(row['rss_bytes'] for row in rows), 'processes': rows})
            if process.process and kernel.runner_wait(process.process) == 0:
                boundaries['runner_exit_observed_ns']=str(time.perf_counter_ns())
                closure['runner']['terminal_wait']=0
                created=kernel.creation_identity(process.process)
                closure['runner']['terminal_creation_filetime_100ns_since_1601_utc']=created
                if created!=closure['runner']['creation_filetime_100ns_since_1601_utc']:
                    raise NativeFailure('RUNNER_LIFETIME_RACE')
                value = w.DWORD()
                if not kernel.GetExitCodeProcess(process.process, ct.byref(value)):
                    raise NativeFailure('RUNNER_EXIT_CODE')
                exit_code = int(value.value); exited = True
                if not kernel.CloseHandle(process.process):raise NativeFailure('CLOSE_RUNNER_PROCESS')
                process.process = None
            if exited:
                # A fresh query follows the actual positive runner wait. Earlier
                # sampled active0 is never re-timestamped as a later observation.
                after_runner=kernel.accounting(job)
                if after_runner['active_processes']==0:
                    closure['job_empty_accounting']=after_runner
                    boundaries['job_empty_observed_ns']=str(time.perf_counter_ns())
                    break
            if time.monotonic() >= deadline:
                raise NativeFailure('OWNED_DEADLINE')
            time.sleep(interval_s)
        final = kernel.accounting(job)
        extended = kernel.query(job, 9, ExtendedLimits())
        boundaries['final_accounting_end_ns']=str(time.perf_counter_ns())
        evidence.update(exit_code=exit_code,
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
        finished=time.perf_counter_ns()
        boundaries['native_measurement_end_ns']=str(finished)
        evidence['wall_ns']=finished-started
        return evidence
    except BaseException as error:
        primary=error
        evidence['failure']=error.code if isinstance(error,NativeFailure) else 'UNEXPECTED_NATIVE_CONTROL'
        evidence['primary_failure_identity']={'error_type':type(error).__module__+'.'+type(error).__qualname__,
            'code':error.code if isinstance(error,NativeFailure) else None}
        evidence['runner_exited_before_failure'] = exited
        evidence['runner_exit_code_before_failure'] = exit_code
        evidence['assigned_to_owned_job'] = assigned
        if isinstance(error,NativeFailure):
            if error.evidence is not None and error.evidence is not evidence:evidence['primary_failure_detail']=error.evidence
            error.evidence=evidence
        raise
    finally:
        failures=cleanup_owned(kernel,process,job,assigned=assigned,evidence=evidence)
        if failures:
            if primary is not None:primary.add_note('Independent owned cleanup failures are retained in native evidence')
            else:
                evidence['failure']='OWNED_CLEANUP_FAILED'
                raise NativeFailure('OWNED_CLEANUP_FAILED',evidence)
