"""Independent qualification counts from Python code events, never trace data.

No timing authority. Python return events do not distinguish exception unwind
from legitimate None returns; this observer never invents completion status.
"""
from contextlib import contextmanager
import inspect
import re
import sys
import threading
from types import CodeType

BYTE_RETURNS=frozenset(('manifest.canonical','manifest.encode_task','manifest.encode_output',
    'sdk.encode_result','sdk.encode_receipt'))
ASCII_RETURNS=frozenset(('config.to_json','context.to_json'))


class CodeCounts:
    def __init__(self,operations,*,root_codes,transparent_codes=(),max_entries=131072,max_frames=4096):
        if (not operations or len(operations)>40 or any(type(code) is not CodeType or type(op) is not str for code,op in operations.items())
                or not root_codes or any(type(code) is not CodeType for code in root_codes)
                or type(max_entries) is not int or not 1<=max_entries<=131072
                or type(max_frames) is not int or not 1<=max_frames<=4096):
            raise ValueError('invalid code count specification')
        self.code_owners=tuple(operations)+tuple(root_codes)+tuple(transparent_codes)
        self.operations={id(code):op for code,op in operations.items()}
        self.roots=frozenset(map(id,root_codes));self.transparent=frozenset(map(id,transparent_codes))
        self.max_entries=max_entries;self.max_frames=max_frames;self.entries=0
        self.frames={};self.counters={};self.failure_reason=None;self.lock=threading.RLock()
        self.callback=self.observe;self.running=False;self.used=False

    def fail(self,reason):
        with self.lock:
            if self.failure_reason is None:self.failure_reason=reason

    def original_scope(self,frame):
        depth=0
        while frame is not None:
            if id(frame.f_code) in self.roots:return True
            if depth>=128:
                self.fail('frame_limit');return False
            module=frame.f_globals.get('__name__','')
            if module.startswith('eq142_') and id(frame.f_code) not in self.transparent:return False
            frame=frame.f_back;depth+=1
        return False

    def observe(self,frame,event,value):
        if not self.running or self.failure_reason is not None or event not in ('call','return'):return
        operation=self.operations.get(id(frame.f_code))
        if operation is None:return
        try:
            if event=='call':
                if not self.original_scope(frame):return
                with self.lock:
                    if self.entries>=self.max_entries:
                        self.fail('entry_limit');return
                    if len(self.frames)>=self.max_frames:
                        self.fail('frame_limit');return
                    key=id(frame)
                    if key in self.frames:
                        self.fail('unbalanced_events');return
                    self.frames[key]=operation;self.entries+=1
                    counts=self.counters.setdefault(operation,[0,0,0,0]);counts[0]+=1
            else:
                with self.lock:
                    entered=self.frames.pop(id(frame),None)
                    if entered is None:return
                    counts=self.counters[entered];counts[1]+=1
                    observed=None
                    if entered in BYTE_RETURNS and type(value) is bytes:observed=len(value)
                    elif entered in ASCII_RETURNS and type(value) is str and value.isascii():observed=len(value)
                    elif entered=='supervisor.pickle_size' and type(value) is int and value>=0:observed=value
                    if observed is not None:
                        if counts[3]+observed>9007199254740991:
                            self.fail('observer_error');return
                        counts[2]+=1;counts[3]+=observed
        except BaseException:self.fail('observer_error')

    @contextmanager
    def activate(self):
        if self.used or sys.getprofile() is not None or threading.getprofile() is not None:
            raise ValueError('existing/reused profile observer')
        self.used=True;self.running=True;installation_complete=False
        try:
            threading.setprofile(self.callback);sys.setprofile(self.callback)
            installation_complete=True
            yield self
        except BaseException:
            # Includes partial installation. Product exceptions still propagate
            # unchanged; the observer only records its own installation failure.
            if not installation_complete or sys.getprofile() is not self.callback or threading.getprofile() is not self.callback:
                self.fail('observer_error')
            raise
        finally:
            # Restore owned hooks only; never overwrite another observer.
            self.running=False
            for getter,setter in ((sys.getprofile,sys.setprofile),(threading.getprofile,threading.setprofile)):
                try:
                    current=getter()
                    if current is self.callback:setter(None)
                    elif installation_complete or current is not None:self.fail('profiler_interference')
                except BaseException:self.fail('observer_error')
            if self.frames:self.fail('unbalanced_events')

    def snapshot(self):
        with self.lock:
            return {'schema':'eq142-code-count-qualification:1',
                'complete':self.failure_reason is None and not self.running and not self.frames,
                'failure_reason':self.failure_reason,
                'counters':[{'operation':op,'call_entries':v[0],'return_events':v[1],
                    'typed_byte_returns':v[2],'typed_returned_bytes':v[3]} for op,v in sorted(self.counters.items())],
                'completion_classification':'return_events_do_not_classify_exception',
                'elapsed_cost_disposition':'qualification_only_no_timing_authority'}


def for_original_run(bound,inventory):
    """Build before alias hooks are installed, using approved original objects."""
    from eq142_graph import resolve,function_of,digest,wire
    from eq142_trace import HookSession
    from eq142_causal import CausalHookSession
    from eq142_delegate import _ORIGINAL_PARTITION,_ORIGINAL_PARTITION_CODE
    from equity_feature_workers import BoundedSupervisor
    bound.check()
    if digest(wire(inventory))!=bound.hook_inventory_sha256:raise ValueError('changed profile inventory')
    operations={}
    for record in inventory['records']:
        owner,name=resolve(record['definition']);function=function_of(inspect.getattr_static(owner,name))
        if function.__code__ in operations:raise ValueError('ambiguous code operation')
        operations[function.__code__]=record['operation']
    if _ORIGINAL_PARTITION.__code__ is not _ORIGINAL_PARTITION_CODE:raise ValueError('changed original partition')
    operations[_ORIGINAL_PARTITION_CODE]='delegate.original_partition'
    transparent=[code for function in (HookSession.wrapper,CausalHookSession.wrapper) for code in function.__code__.co_consts
        if type(code) is CodeType and code.co_name in ('traced','observed')]
    return CodeCounts(operations,root_codes=(BoundedSupervisor.run.__code__,_ORIGINAL_PARTITION_CODE),
        transparent_codes=transparent)


@contextmanager
def guarded_activation(observer,on_error):
    """Observer admission/restoration failures cannot replace a product outcome."""
    context=observer.activate();entered=False
    try:
        context.__enter__();entered=True
    except BaseException:
        observer.fail('observer_error')
        try:on_error()
        except BaseException:pass
    try:yield observer
    finally:
        if entered:
            try:context.__exit__(None,None,None)
            except BaseException:
                observer.fail('observer_error')
                try:on_error()
                except BaseException:pass


def compare_aggregate(profile_records,trace_counters,*,manual_operations=()):
    """Exact aggregate entries and actual typed returned byte observations.

    Manual trace-only bookkeeping has independently source-defined expectations;
    it is excluded explicitly. No returned/rejected inference from profile None.
    """
    expected={};actual={}
    for record in profile_records:
        validate_profile(record)
        if record['complete'] is not True:raise ValueError('incomplete count observer')
        for row in record['counters']:
            values=expected.setdefault(row['operation'],[0,0,0])
            if row['call_entries']!=row['return_events']:raise ValueError('unbalanced code events')
            for n,field in enumerate(('call_entries','typed_byte_returns','typed_returned_bytes')):values[n]+=row[field]
    for row in trace_counters:
        if row['operation'] in manual_operations:continue
        values=actual.setdefault(row['operation'],[0,0,0]);values[0]+=row['attempts']
        if row['byte_observation']!='unavailable':
            values[1]+=row['returned'];values[2]+=row['returned_observed_bytes']
    if expected!=actual:raise ValueError('independent code/hook aggregate counts differ')
    return {'operations':len(expected),'call_entries':sum(v[0] for v in expected.values()),
        'typed_byte_returns':sum(v[1] for v in expected.values()),'typed_returned_bytes':sum(v[2] for v in expected.values()),
        'scope':'qualification aggregate only; no timing or completion-status authority'}


def validate_profile(record):
    fields={'schema','complete','failure_reason','counters','completion_classification','elapsed_cost_disposition'}
    if type(record) is not dict or set(record)!=fields or record['schema']!='eq142-code-count-qualification:1':
        raise ValueError('invalid code count record')
    if (type(record['complete']) is not bool or record['failure_reason'] not in
            (None,'observer_error','entry_limit','frame_limit','profiler_interference','unbalanced_events')
            or record['complete'] and record['failure_reason'] is not None
            or record['completion_classification']!='return_events_do_not_classify_exception'
            or record['elapsed_cost_disposition']!='qualification_only_no_timing_authority'
            or type(record['counters']) is not list or len(record['counters'])>40):
        raise ValueError('invalid code count record')
    operations=[];entries=0
    for row in record['counters']:
        if type(row) is not dict or set(row)!={'operation','call_entries','return_events','typed_byte_returns','typed_returned_bytes'}:
            raise ValueError('invalid code count row')
        if type(row['operation']) is not str or re.fullmatch('[a-z][a-z0-9_.:-]{0,127}',row['operation']) is None:
            raise ValueError('invalid code count operation')
        for name,limit in (('call_entries',131072),('return_events',131072),('typed_byte_returns',131072),
                ('typed_returned_bytes',9007199254740991)):
            if type(row[name]) is not int or not 0<=row[name]<=limit:raise ValueError('invalid code count value')
        if not row['typed_byte_returns']<=row['return_events']<=row['call_entries']:
            raise ValueError('invalid code event closure')
        if record['complete'] and row['call_entries']!=row['return_events']:raise ValueError('unbalanced code event record')
        if row['typed_byte_returns']==0 and row['typed_returned_bytes']!=0:raise ValueError('invalid code byte closure')
        operations.append(row['operation']);entries+=row['call_entries']
    if operations!=sorted(set(operations)) or entries>131072:raise ValueError('invalid code count closure')
