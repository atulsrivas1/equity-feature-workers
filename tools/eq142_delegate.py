"""Importable development partition delegate; original callbacks stay exported.

Each invocation has an explicit reservation. Spawn children bind their own source
and hooks; thread/sequential delegates route the parent's one installed hook set.
No inherited process globals, extra scheduling, or native IPC byte claim.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from functools import partial
import inspect
import json
from pathlib import Path
import pickle
import threading

from equity_feature_workers import TaskManifest
import equity_feature_workers.supervisor as supervisor
from eq142_graph import bind_source, digest, wire
from eq142_trace import Collector, HookSession, TraceSpecificationError

_ORIGINAL_PARTITION=supervisor._compute_partition
_ORIGINAL_PARTITION_CODE=_ORIGINAL_PARTITION.__code__
_ORIGINAL_TASK_HASH=inspect.getattr_static(TaskManifest,'task_sha256').fget
_ROUTE=threading.local()
_ACTIVE_PARENT=None


class RoutingHookSession(HookSession):
    @property
    def collector(self):
        return getattr(_ROUTE,'collector',None) or self._parent_collector

    @collector.setter
    def collector(self,value):
        self._parent_collector=value


@contextmanager
def route(collector):
    before=getattr(_ROUTE,'collector',None);_ROUTE.collector=collector
    try:yield
    finally:
        if before is None:_ROUTE.__dict__.pop('collector',None)
        else:_ROUTE.collector=before


def invocation_key(hashes):
    return digest(b'eq142-invocation1\0'+wire(list(hashes)))


def make_plan(items,*,mode,budget,expected_inventory,output_dir,operations,qualification_profile_dir=None):
    """Prepare explicit labels/reservations before installing hooks.

    Returned pickle length describes this Python object only, never pool-native
    IPC bytes. Full harness must reserve actual delegate/spec overhead separately.
    """
    if mode not in ('sequential','thread','process') or not 1<=len(items)<=16:
        raise TraceSpecificationError('unknown_invocation')
    root=Path(output_dir).resolve()
    if not root.is_dir() or any(root.iterdir()):raise TraceSpecificationError('duplicate_sidecar')
    profile_root=None
    if qualification_profile_dir is not None:
        profile_root=Path(qualification_profile_dir).resolve()
        if profile_root==root or profile_root.parent!=root.parent or not profile_root.is_dir() or any(profile_root.iterdir()):
            raise TraceSpecificationError('duplicate_sidecar')
    bind_source(expected_inventory).check()
    partitions=supervisor.partition_tasks(items,budget.workers)
    if mode=='sequential':groups=[(item,) for item in partitions[0].items]
    else:groups=[partition.items for partition in partitions]
    count=len(groups)
    max_spans=114688//count;max_bytes=min(16777216,58720256//count)
    reservations=[]
    for ordinal,group in enumerate(groups):
        hashes=[_ORIGINAL_TASK_HASH(item.task) for item in group]
        reservations.append({'id':'delegate:%03d'%ordinal,'key':invocation_key(hashes),
            'task_hashes':hashes,'max_spans':max_spans,'max_bytes':max_bytes,
            'file':'delegate-%03d.json'%ordinal})
    if len({r['key'] for r in reservations})!=count:raise TraceSpecificationError('unknown_invocation')
    plan={'schema':'eq142-delegate-plan:1','mode':mode,'expected_inventory':json.loads(wire(expected_inventory)),
        'output_dir':str(root),'operations':list(operations),'reservations':reservations,
        'qualification_profile_dir':None if profile_root is None else str(profile_root)}
    # This is observed metadata serialization only. The final harness admission
    # must close its multiplication/copy and overall trace/resident reservations.
    observed=len(pickle.dumps(plan,protocol=5))
    if observed>1048576:raise TraceSpecificationError('byte_limit')
    return plan,observed


@contextmanager
def patched_delegate(plan,parent_collector):
    """One owner patches the partition target, restored before hook restoration."""
    global _ACTIVE_PARENT
    if _ACTIVE_PARENT is not None or supervisor._compute_partition is not _ORIGINAL_PARTITION or _ORIGINAL_PARTITION.__code__ is not _ORIGINAL_PARTITION_CODE:
        raise TraceSpecificationError('source_mismatch')
    _ACTIVE_PARENT=parent_collector
    replacement=partial(partition_entry,plan=plan)
    supervisor._compute_partition=replacement
    try:yield
    finally:
        if supervisor._compute_partition is replacement:supervisor._compute_partition=_ORIGINAL_PARTITION
        else:parent_collector.failure('restoration_failure')
        _ACTIVE_PARENT=None


def partition_entry(partition,budget,process_transport=False,telemetry=False,submitted_ns=None,*,plan):
    """Top-level spawn-pickleable delegate; invoke original partition exactly once."""
    # Explicit label binding is bookkeeping, not a new task/body seal or count
    # of product hashes. Parent hooks are suppressed while deriving primitive
    # task IDs; fresh spawn children have no inherited installed hooks.
    parent=_ACTIVE_PARENT
    with parent.suppress() if parent is not None else nullcontext():
        hashes=[_ORIGINAL_TASK_HASH(item.task) for item in partition.items]
    key=invocation_key(hashes)
    matches=[r for r in plan['reservations'] if r['key']==key]
    if len(matches)!=1:raise TraceSpecificationError('unknown_invocation')
    reservation=matches[0]
    collector=Collector(reservation['id'],max_spans=reservation['max_spans'],
        max_bytes=reservation['max_bytes'],operations=plan['operations'])
    fresh_process=plan['mode']=='process'
    session=None;profile=None
    with route(collector),collector.phase('stage:delegate'):
        label_frame=collector.begin('trace.bind_labels',())
        try:collector.bind_labels(tuple(i.task for i in partition.items),_ORIGINAL_TASK_HASH)
        finally:collector.finish(label_frame)
        if fresh_process:
            bound=bind_source(plan['expected_inventory'])
            session=RoutingHookSession(bound,plan['expected_inventory']['hooks'],collector)
            if plan.get('qualification_profile_dir') is not None:
                try:
                    from eq142_code_counts import for_original_run
                    profile=for_original_run(bound,plan['expected_inventory']['hooks'])
                except BaseException:collector.failure('collector_error')
        error=None;result=None
        try:
            if session is not None:
                install_frame=collector.begin('trace.install',())
                try:session.install()
                finally:collector.finish(install_frame)
            frame=collector.begin('delegate.original_partition',())
            try:
                if profile is not None:
                    from eq142_code_counts import guarded_activation
                    with guarded_activation(profile,lambda:collector.failure('collector_error')):
                        result=_ORIGINAL_PARTITION(partition,budget,process_transport,telemetry,submitted_ns)
                else:result=_ORIGINAL_PARTITION(partition,budget,process_transport,telemetry,submitted_ns)
            except BaseException as issue:
                error=issue;collector.finish(frame,error=issue)
            else:collector.finish(frame,result=result)
        finally:
            if session is not None:
                restore_frame=collector.begin('trace.restore',())
                try:session.restore()
                finally:collector.finish(restore_frame)
            if profile is not None:
                try:
                    from eq142_evidence import persist_record
                    persist_record(Path(plan['qualification_profile_dir'])/reservation['file'],profile.snapshot(),max_bytes=65536)
                except BaseException:collector.failure('collector_error')
            try:collector.persist(Path(plan['output_dir'])/reservation['file'])
            except BaseException:
                # Original product result/error stays intact. Missing/partial or
                # incomplete sidecars reject trace qualification in the harness.
                collector.failure('collector_error')
        if error is not None:raise error
        return result
