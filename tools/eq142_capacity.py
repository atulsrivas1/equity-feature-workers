"""Development capacity observations, separate from launch admission.

The caller establishes owned-namespace byte accounting and source provenance.
Raw receipts containing the actual opaque device ID stay in private evidence.
This module never pins workers, allocates the stated reserves or runs a study.
"""
from __future__ import annotations
import ctypes as ct
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import shutil
import sys
import threading
import time

GIB=1073741824
STUDY_BYTES=72*GIB
HOST_DISK_BYTES=8*GIB
HOST_MEMORY_BYTES=2*GIB
COUNT_MAX=9007199254740991
UINT64_MAX=18446744073709551615
SOURCE_FIELDS=frozenset(('baseline_machine_module_sha256','baseline_machine_definition_line',
    'python_runtime_version','shutil_module_sha256','shutil_windows_definition_line','native_contract_version'))
FIELDS=frozenset('''schema position capture_start_ns capture_end_ns active_processor_groups_before
active_processor_groups_after process_groups_before process_groups_after primary_thread_group_before
primary_thread_group_after primary_thread_mask_before primary_thread_mask_after primary_system_mask word_bits
allowed_affinity_mask allowed_cpu_ids allowed_cpu_domain allowed_cpu_slots free_memory_bytes storage_device
storage_total_bytes storage_used_bytes free_storage_bytes study_encoded_bytes_already_retained
remaining_study_allowance_bytes study_storage_reserve_bytes additional_storage_reserve_bytes
owned_job_sampled_rss_cap_bytes additional_host_memory_reserve_bytes raw_machine_observation
non_atomic_observation physical_enforcement probe_sources capacity_disposition placement_enforced'''.split())
RAW_FIELDS=frozenset(('allowed_affinity_mask','allowed_cpu_slots','initial_free_physical_bytes',
    'memory_cap_bytes','degrees','omitted_degrees','backend_slots'))


class CapacityUnavailable(ValueError):
    def __init__(self,code):
        self.code=code
        super().__init__('EQ142 capacity unavailable: '+code)


def count(value,maximum=COUNT_MAX,minimum=0):
    if type(value) is not int or not minimum<=value<=maximum:raise ValueError('capacity count')
    return value


def decimal(value,maximum):
    if (type(value) is not str or re.fullmatch('0|[1-9][0-9]{0,19}',value) is None
            or int(value)>maximum):raise ValueError('capacity decimal identifier/time')
    return int(value)


def mask(value):
    if type(value) is not str or re.fullmatch('0x[1-9a-f][0-9a-f]{0,15}',value) is None:
        raise ValueError('capacity mask')
    return int(value,16)


def validate_capacity(record,expected_probe_sources):
    """Pure structural/source-fact checks, not a namespace certificate."""
    if type(record) is not dict or set(record)!=FIELDS:raise ValueError('capacity closed record')
    if type(expected_probe_sources) is not dict or set(expected_probe_sources)!=SOURCE_FIELDS:
        raise ValueError('capacity source context')
    source=record['probe_sources']
    if type(source) is not dict or set(source)!=SOURCE_FIELDS or source!=expected_probe_sources:
        raise ValueError('capacity source join')
    for key in ('baseline_machine_module_sha256','shutil_module_sha256'):
        if type(source[key]) is not str or re.fullmatch('[0-9a-f]{64}',source[key]) is None:
            raise ValueError('capacity source hash')
    if (source['python_runtime_version'] not in ('3.12.10','3.12.15')
            or type(source['baseline_machine_definition_line']) is not int
            or source['baseline_machine_definition_line']!=325
            or type(source['shutil_windows_definition_line']) is not int
            or source['shutil_windows_definition_line']!={'3.12.10':1379,'3.12.15':1361}[source['python_runtime_version']]
            or source['native_contract_version']!='win32-capacity-api2'):
        raise ValueError('capacity source definition')
    if (record['schema']!='eq142-capacity-receipt:1' or record['position'] not in ('before','after')
            or record['capacity_disposition']!='conservative_observed_primary_group_budget_not_global_eligibility'
            or record['non_atomic_observation'] is not True or record['physical_enforcement'] is not False
            or record['placement_enforced'] is not False):raise ValueError('capacity scope')
    if decimal(record['capture_start_ns'],9223372036854775807)>decimal(record['capture_end_ns'],9223372036854775807):
        raise ValueError('capacity capture order')
    active=count(record['active_processor_groups_before'],64,1)
    if count(record['active_processor_groups_after'],64,1)!=active:raise ValueError('capacity group stability')
    groups=record['process_groups_before']
    if type(groups) is not list or not 1<=len(groups)<=active:raise ValueError('capacity process groups')
    for group in groups:count(group,active-1)
    if groups!=sorted(set(groups)):raise ValueError('capacity group population')
    other=record['process_groups_after']
    if type(other) is not list or not 1<=len(other)<=active:raise ValueError('capacity process groups')
    for group in other:count(group,active-1)
    if other!=groups:raise ValueError('capacity process groups changed')
    primary=count(record['primary_thread_group_before'],active-1)
    if primary not in groups or count(record['primary_thread_group_after'],active-1)!=primary:
        raise ValueError('capacity primary group')
    thread=mask(record['primary_thread_mask_before'])
    if mask(record['primary_thread_mask_after'])!=thread:raise ValueError('capacity primary mask changed')
    allowed=mask(record['allowed_affinity_mask']);system=mask(record['primary_system_mask'])
    if allowed&~thread or allowed&~system:raise ValueError('capacity mask subset')
    if type(record['word_bits']) is not int or record['word_bits']!=64:raise ValueError('capacity word width')
    ids=record['allowed_cpu_ids']
    if type(ids) is not list or not 1<=len(ids)<=64:raise ValueError('capacity CPU ids')
    for value in ids:count(value,63)
    if ids!=[i for i in range(64) if allowed>>i&1]:raise ValueError('capacity mask ids')
    slots=count(record['allowed_cpu_slots'],64,1)
    if slots!=len(ids) or record['allowed_cpu_domain']!='windows_primary_processor_group:'+str(primary):
        raise ValueError('capacity CPU domain')
    for key in ('free_memory_bytes','storage_total_bytes','storage_used_bytes','free_storage_bytes'):
        count(record[key])
    decimal(record['storage_device'],UINT64_MAX)
    if record['storage_used_bytes']+record['free_storage_bytes']!=record['storage_total_bytes']:
        raise ValueError('capacity disk sum')
    retained=count(record['study_encoded_bytes_already_retained'],STUDY_BYTES)
    if count(record['remaining_study_allowance_bytes'],STUDY_BYTES)!=STUDY_BYTES-retained:
        raise ValueError('capacity remaining allowance')
    for key,expected in (('study_storage_reserve_bytes',STUDY_BYTES),
            ('additional_storage_reserve_bytes',HOST_DISK_BYTES),('owned_job_sampled_rss_cap_bytes',GIB),
            ('additional_host_memory_reserve_bytes',HOST_MEMORY_BYTES)):
        if type(record[key]) is not int or record[key]!=expected:raise ValueError('capacity fixed allowance')
    raw=record['raw_machine_observation']
    if type(raw) is not dict or set(raw)!=RAW_FIELDS:raise ValueError('capacity original machine shape')
    if (mask(raw['allowed_affinity_mask'])!=allowed or count(raw['allowed_cpu_slots'],64,2)!=slots
            or count(raw['initial_free_physical_bytes'])!=record['free_memory_bytes']
            or count(raw['memory_cap_bytes'],GIB)!=min(GIB,record['free_memory_bytes']//4)
            or type(raw['backend_slots']) is not int or raw['backend_slots']!=1):
        raise ValueError('capacity original machine join')
    for key in ('degrees','omitted_degrees'):
        if type(raw[key]) is not list or len(raw[key])>4:raise ValueError('capacity original degree observation')
        for degree in raw[key]:count(degree,8,1)
    if (raw['degrees']!=[n for n in (1,2,4,8) if n<=slots-1]
            or raw['omitted_degrees']!=[n for n in (1,2,4,8) if n>slots-1]):
        raise ValueError('capacity original degree observation')
    if len((json.dumps(record,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)+'\n').encode())>16384:
        raise ValueError('capacity record bound')


def admit_capacity(record,expected_probe_sources,*,degree,expected_device,expected_retained_bytes):
    """Pure launch/root gate; caller supplies independently joined context."""
    validate_capacity(record,expected_probe_sources)
    decimal(expected_device,UINT64_MAX);count(expected_retained_bytes,STUDY_BYTES)
    if record['storage_device']!=expected_device or record['study_encoded_bytes_already_retained']!=expected_retained_bytes:
        raise ValueError('capacity owned context')
    if type(degree) is not int or degree not in (1,2,4,8):raise ValueError('capacity degree')
    if degree+3>record['allowed_cpu_slots']:raise CapacityUnavailable('cpu_capacity')
    required_memory=HOST_MEMORY_BYTES+(GIB if record['position']=='before' else 0)
    if record['free_memory_bytes']<required_memory:raise CapacityUnavailable('memory_capacity')
    if record['free_storage_bytes']<record['remaining_study_allowance_bytes']+HOST_DISK_BYTES:
        raise CapacityUnavailable('storage_capacity')
    return {'evidence':'observed_primary_group_and_host_capacity_with_caller_joined_namespace',
        'degree':degree,'max_cpu_slots':record['allowed_cpu_slots']-2,
        'physical_enforcement':False,'worker_placement_observed':False,'full_sample_accepted':False}


WORD=ct.c_uint16
BOOL=ct.c_int32
HANDLE=ct.c_void_p
class GroupAffinity(ct.Structure):
    _fields_=[('Mask',ct.c_uint64),('Group',WORD),('Reserved',WORD*3)]


class CapacityProbe:
    """Actual Windows observations; no assumed namespace-byte provenance."""
    def __init__(self,study_directory,approved_sources):
        if os.name!='nt' or ct.sizeof(ct.c_void_p)!=8 or sys.implementation.name!='cpython':
            raise CapacityUnavailable('unsupported_platform')
        import benchmark_supplied_inputs as baseline
        import nt
        self.baseline=baseline;self.nt=nt;self.approved=approved_sources
        self.version='.'.join(map(str,sys.version_info[:3]))
        if self.version not in approved_sources['versions']:raise CapacityUnavailable('unsupported_runtime')
        self.owner=threading.get_ident()
        requested=Path(study_directory).absolute()
        self._no_indirections(requested)
        self.directory=requested.resolve(strict=True)
        self.machine=inspect.getattr_static(baseline,'machine');self.machine_code=self.machine.__code__
        self.disk=inspect.getattr_static(shutil,'disk_usage');self.disk_code=self.disk.__code__
        self.disk_export=nt._getdiskusage;self.stat_export=os.stat
        self.directory_device=self._directory_device()
        self.kernel=ct.WinDLL('kernel32',use_last_error=True)
        if (ct.sizeof(WORD),ct.sizeof(BOOL),ct.sizeof(GroupAffinity),GroupAffinity.Group.offset,GroupAffinity.Reserved.offset)!=(2,4,16,8,10):
            raise CapacityUnavailable('native_abi')
        k=self.kernel
        for name in ('GetCurrentProcess','GetCurrentThread'):
            function=getattr(k,name);function.argtypes=[];function.restype=HANDLE
        k.GetActiveProcessorGroupCount.argtypes=[];k.GetActiveProcessorGroupCount.restype=WORD
        k.GetProcessGroupAffinity.argtypes=[HANDLE,ct.POINTER(WORD),ct.POINTER(WORD)];k.GetProcessGroupAffinity.restype=BOOL
        k.GetThreadGroupAffinity.argtypes=[HANDLE,ct.POINTER(GroupAffinity)];k.GetThreadGroupAffinity.restype=BOOL
        k.GetProcessAffinityMask.argtypes=[HANDLE,ct.POINTER(ct.c_uint64),ct.POINTER(ct.c_uint64)];k.GetProcessAffinityMask.restype=BOOL
        # Both are pseudo-handles. This probe owns no native closeable handle.
        self.process=k.GetCurrentProcess();self.thread=k.GetCurrentThread()
        self.probe_sources=self._check_source()

    @staticmethod
    def _no_indirections(path):
        for parent in (path,*path.parents):
            if parent.is_symlink() or parent.is_junction():raise ValueError('capacity directory indirection')

    def _directory_device(self):
        path=self.directory
        if not path.is_dir() or path.resolve(strict=True)!=path:raise ValueError('capacity owned directory')
        self._no_indirections(path)
        value=os.stat(path).st_dev
        count(value,UINT64_MAX)
        return str(value)

    @staticmethod
    def _file_hash(module):
        path=Path(module.__file__)
        if path.is_symlink() or path.stat().st_size>1048576:raise ValueError('capacity source file')
        with path.open('rb') as source:body=source.read(1048577)
        if len(body)>1048576:raise ValueError('capacity source file')
        return hashlib.sha256(body).hexdigest()

    def _check_source(self):
        approved=self.approved['versions'][self.version];original=self.approved['original_machine']
        if (threading.get_ident()!=self.owner
                or inspect.getattr_static(self.baseline,'machine') is not self.machine or self.machine.__code__ is not self.machine_code
                or self.machine.__module__!='benchmark_supplied_inputs' or self.machine.__qualname__!='machine'
                or self.machine_code.co_firstlineno!=325
                or inspect.getattr_static(shutil,'disk_usage') is not self.disk or self.disk.__code__ is not self.disk_code
                or self.disk.__module__!='shutil' or self.disk.__qualname__!='disk_usage'
                or self.disk_code.co_firstlineno!=approved['windows_definition_line']
                or shutil.nt is not self.nt or self.nt._getdiskusage is not self.disk_export
                or os.stat is not self.stat_export or self.nt.stat is not self.stat_export):
            raise ValueError('capacity source identity')
        machine_hash=self._file_hash(self.baseline);disk_hash=self._file_hash(shutil)
        if machine_hash not in original['raw_sha256_variants'].values() or disk_hash not in approved['raw_sha256_variants'].values():
            raise ValueError('capacity source bytes')
        return {'baseline_machine_module_sha256':machine_hash,'baseline_machine_definition_line':325,
            'python_runtime_version':self.version,'shutil_module_sha256':disk_hash,
            'shutil_windows_definition_line':self.disk_code.co_firstlineno,'native_contract_version':'win32-capacity-api2'}

    def _topology(self):
        k=self.kernel;active=int(k.GetActiveProcessorGroupCount())
        if not 1<=active<=64:raise CapacityUnavailable('active_groups')
        groups=(WORD*active)();size=WORD(active)
        if not k.GetProcessGroupAffinity(self.process,ct.byref(size),groups) or not 1<=size.value<=active:
            raise CapacityUnavailable('process_groups')
        ids=sorted(int(groups[i]) for i in range(size.value))
        if len(set(ids))!=len(ids) or any(i>=active for i in ids):raise CapacityUnavailable('process_groups')
        group=GroupAffinity()
        if not k.GetThreadGroupAffinity(self.thread,ct.byref(group)) or list(group.Reserved)!=[0,0,0]:
            raise CapacityUnavailable('thread_group')
        process_mask=ct.c_uint64();system_mask=ct.c_uint64()
        if not k.GetProcessAffinityMask(self.process,ct.byref(process_mask),ct.byref(system_mask)):
            raise CapacityUnavailable('primary_masks')
        if (group.Group>=active or group.Group not in ids or not group.Mask or not process_mask.value
                or not system_mask.value or process_mask.value&~group.Mask or process_mask.value&~system_mask.value):
            raise CapacityUnavailable('primary_masks')
        return {'active':active,'groups':ids,'primary':int(group.Group),'thread_mask':hex(group.Mask),
            'process_mask':hex(process_mask.value),'system_mask':hex(system_mask.value)}

    def capture(self,position,*,encoded_bytes_already_retained):
        if position not in ('before','after'):raise ValueError('capacity position')
        count(encoded_bytes_already_retained,STUDY_BYTES)
        start=time.perf_counter_ns();sources=self._check_source()
        if sources!=self.probe_sources or self._directory_device()!=self.directory_device:
            raise ValueError('capacity source/device changed')
        before=self._topology();raw=self.machine();after=self._topology()
        if before!=after or raw['allowed_affinity_mask']!=before['process_mask']:
            raise CapacityUnavailable('topology_changed')
        usage=self.disk(self.directory)
        if self._directory_device()!=self.directory_device or self._check_source()!=sources:
            raise ValueError('capacity source/device changed')
        end=time.perf_counter_ns();allowed=int(before['process_mask'],16)
        result={'schema':'eq142-capacity-receipt:1','position':position,'capture_start_ns':str(start),'capture_end_ns':str(end),
            'active_processor_groups_before':before['active'],'active_processor_groups_after':after['active'],
            'process_groups_before':before['groups'],'process_groups_after':after['groups'],
            'primary_thread_group_before':before['primary'],'primary_thread_group_after':after['primary'],
            'primary_thread_mask_before':before['thread_mask'],'primary_thread_mask_after':after['thread_mask'],
            'primary_system_mask':before['system_mask'],'word_bits':64,'allowed_affinity_mask':before['process_mask'],
            'allowed_cpu_ids':[i for i in range(64) if allowed>>i&1],
            'allowed_cpu_domain':'windows_primary_processor_group:'+str(before['primary']),
            'allowed_cpu_slots':allowed.bit_count(),'free_memory_bytes':raw['initial_free_physical_bytes'],
            'storage_device':self.directory_device,'storage_total_bytes':usage.total,'storage_used_bytes':usage.used,
            'free_storage_bytes':usage.free,'study_encoded_bytes_already_retained':encoded_bytes_already_retained,
            'remaining_study_allowance_bytes':STUDY_BYTES-encoded_bytes_already_retained,
            'study_storage_reserve_bytes':STUDY_BYTES,'additional_storage_reserve_bytes':HOST_DISK_BYTES,
            'owned_job_sampled_rss_cap_bytes':GIB,'additional_host_memory_reserve_bytes':HOST_MEMORY_BYTES,
            'raw_machine_observation':raw,'non_atomic_observation':True,'physical_enforcement':False,
            'probe_sources':sources,'capacity_disposition':'conservative_observed_primary_group_budget_not_global_eligibility',
            'placement_enforced':False}
        validate_capacity(result,sources)
        return result
