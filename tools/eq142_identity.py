"""Positive current-process owned Job/lifetime witness; no sampled-resource proof.

FILETIME creation identity is never used as elapsed time. Direct QPF enriches
an unqueried trace clock without qualifying cross-process raw counter alignment.
"""
import ctypes as ct
from ctypes import wintypes as w
from decimal import Decimal
import os
import re
import time

from supplied_native import Kernel as BaseKernel,NativeFailure

JOB_PATTERN=r'Local\\equity-feature-supplied-[0-9a-f]{32}'
CLOCK_FIELDS=frozenset(('name','implementation','monotonic','adjustable','reported_resolution_seconds',
    'qpc_frequency_hz','cross_process_raw_ticks_qualified'))
FIELDS=frozenset(('schema','job_name','pid','process_startup_nonce','creation_filetime_100ns_since_1601_utc',
    'is_member','capture_start_ns','capture_end_ns','clock'))


class WitnessKernel(BaseKernel):
    def __init__(self):
        if os.name!='nt':raise NativeFailure('WITNESS_WINDOWS_ONLY')
        if (ct.sizeof(w.DWORD)!=4 or ct.sizeof(w.BOOL)!=4 or ct.sizeof(w.FILETIME)!=8
                or w.FILETIME.dwHighDateTime.offset!=4 or w.FILETIME.dwLowDateTime.offset!=0):
            raise NativeFailure('WITNESS_NATIVE_ABI')
        super().__init__()
        for name,args,result in (
            ('GetCurrentProcess',[],w.HANDLE),('GetProcessId',[w.HANDLE],w.DWORD),
            ('GetProcessTimes',[w.HANDLE]+[ct.POINTER(w.FILETIME)]*4,w.BOOL),
            ('QueryPerformanceFrequency',[ct.POINTER(ct.c_longlong)],w.BOOL)):
            function=getattr(self.library,name);function.argtypes=args;function.restype=result
            setattr(self,name,function)


def api_failure(code):return NativeFailure(code,{'winerror':ct.get_last_error()})


def capture_owned_process(job_name):
    if type(job_name) is not str or re.fullmatch(JOB_PATTERN,job_name) is None:
        raise NativeFailure('WITNESS_JOB_NAME')
    started=time.perf_counter_ns();kernel=WitnessKernel();job=None;failure=None
    try:
        job=kernel.OpenJobObjectW(0x0004,False,job_name)
        if not job:raise api_failure('WITNESS_OPEN_JOB')
        current=kernel.GetCurrentProcess();pid=int(kernel.GetProcessId(current))
        if not pid:raise api_failure('WITNESS_PROCESS_ID')
        member=w.BOOL()
        if not kernel.IsProcessInJob(current,job,ct.byref(member)):raise api_failure('WITNESS_MEMBERSHIP_API')
        if not member.value:raise NativeFailure('WITNESS_NOT_MEMBER')
        created=w.FILETIME();exited=w.FILETIME();user=w.FILETIME();system=w.FILETIME()
        if not kernel.GetProcessTimes(current,ct.byref(created),ct.byref(exited),ct.byref(system),ct.byref(user)):
            raise api_failure('WITNESS_CREATION_API')
        creation=(int(created.dwHighDateTime)<<32)|int(created.dwLowDateTime)
        if creation<=0:raise NativeFailure('WITNESS_CREATION_UNKNOWN')
        frequency=ct.c_longlong()
        if not kernel.QueryPerformanceFrequency(ct.byref(frequency)):raise api_failure('WITNESS_QPF_API')
        from eq142_trace import fragment_clock_and_timelines
        process,clock,_=fragment_clock_and_timelines(set())
        if process['pid']!=pid or os.getpid()!=pid:raise NativeFailure('WITNESS_PROCESS_ID')
        clock['qpc_frequency_hz']=int(frequency.value)
        record={'schema':'eq142-owned-process-witness:1','job_name':job_name,'pid':pid,
            'process_startup_nonce':process['process_startup_nonce'],
            'creation_filetime_100ns_since_1601_utc':str(creation),'is_member':True,
            'capture_start_ns':str(started),'capture_end_ns':str(time.perf_counter_ns()),'clock':clock}
        validate_witness(record,expected_job=job_name)
        return record
    except BaseException as issue:
        failure=issue;raise
    finally:
        if job:
            try:closed=bool(kernel.CloseHandle(job))
            except BaseException:closed=False
            if not closed:
                if failure is None:raise NativeFailure('WITNESS_CLOSE_JOB')
                failure.add_note('Owned witness Job handle close failed')
                if isinstance(failure,NativeFailure):
                    failure.evidence={**(failure.evidence or {}),'job_handle_close_failed':True}


def clock_valid(clock,*,require_frequency):
    if (type(clock) is not dict or set(clock)!=CLOCK_FIELDS or clock['name']!='perf_counter'
            or clock['implementation']!='QueryPerformanceCounter()' or clock['monotonic'] is not True
            or clock['adjustable'] is not False or clock['cross_process_raw_ticks_qualified'] is not False):
        raise ValueError('invalid owned local clock')
    resolution=clock['reported_resolution_seconds']
    if (type(resolution) is not str or len(resolution)>64 or re.fullmatch('[0-9]+(?:[.][0-9]+)?',resolution) is None
            or not Decimal(resolution).is_finite() or Decimal(resolution)<=0):raise ValueError('invalid nominal clock resolution')
    frequency=clock['qpc_frequency_hz']
    if frequency is None and not require_frequency:return
    if type(frequency) is not int or not 1<=frequency<=9007199254740991:raise ValueError('invalid direct QPF')


def decimal_integer(value,maximum,*,positive=False):
    if (type(value) is not str or re.fullmatch('0|[1-9][0-9]{0,19}',value) is None
            or not int(positive)<=int(value)<=maximum):raise ValueError('invalid exact native integer')
    return int(value)


def validate_witness(record,*,expected_job):
    if (type(record) is not dict or set(record)!=FIELDS or record['schema']!='eq142-owned-process-witness:1'
            or type(record['job_name']) is not str or re.fullmatch(JOB_PATTERN,record['job_name']) is None
            or record['job_name']!=expected_job or type(record['pid']) is not int or not 1<=record['pid']<=4294967295
            or type(record['process_startup_nonce']) is not str
            or re.fullmatch('[0-9a-f]{32}',record['process_startup_nonce']) is None or record['is_member'] is not True):
        raise ValueError('invalid owned process witness')
    decimal_integer(record['creation_filetime_100ns_since_1601_utc'],18446744073709551615,positive=True)
    start=decimal_integer(record['capture_start_ns'],9223372036854775807)
    end=decimal_integer(record['capture_end_ns'],9223372036854775807)
    if end<start:raise ValueError('reversed local capture')
    clock_valid(record['clock'],require_frequency=True)


def join_owned_processes(witnesses,processes,*,job_name):
    """Join traced processes only; no all-descendant/sample-resource inference."""
    if (type(witnesses) is not list or not 1<=len(witnesses)<=17 or type(processes) is not dict
            or not 1<=len(processes)<=17):raise ValueError('invalid owned witness population')
    nonces={};lifetimes={}
    for record in witnesses:
        validate_witness(record,expected_job=job_name)
        nonce=record['process_startup_nonce'];pid=record['pid'];created=record['creation_filetime_100ns_since_1601_utc']
        if nonce not in processes:raise ValueError('untraced witness process')
        actual=processes[nonce]
        if type(actual) is not dict or set(actual)!={'pid','clock'} or type(actual['pid']) is not int or actual['pid']!=pid:
            raise ValueError('trace/native process differs')
        clock_valid(actual['clock'],require_frequency=False)
        for field in CLOCK_FIELDS-{'qpc_frequency_hz'}:
            if record['clock'][field]!=actual['clock'][field]:raise ValueError('trace/native clock differs')
        if actual['clock']['qpc_frequency_hz'] not in (None,record['clock']['qpc_frequency_hz']):
            raise ValueError('trace/native QPF differs')
        value={'pid':pid,'creation_filetime_100ns_since_1601_utc':created,'clock':record['clock']}
        if nonce in nonces and nonces[nonce]!=value:raise ValueError('nonce lifetime drift')
        pair=(pid,created)
        if pair in lifetimes and lifetimes[pair]!=nonce:raise ValueError('lifetime nonce drift')
        nonces[nonce]=value;lifetimes[pair]=nonce
    if set(nonces)!=set(processes):raise ValueError('missing owned witness')
    return [{'process_startup_nonce':nonce,**nonces[nonce]} for nonce in sorted(nonces)]
