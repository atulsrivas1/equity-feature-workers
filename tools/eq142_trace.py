"""Opt-in development hooks. Preserve original return/exception and restore aliases.

This collector is not a scheduler, source certificate or cross-process clock.
Callers must persist actual product outcomes before rejecting incomplete traces.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
import functools
import inspect
import json
import os
from pathlib import Path
import re
import threading
import time
import uuid

from eq142_graph import BoundSource, resolve, function_of, wire, digest

OPERATIONS=frozenset(('supervisor.inputs','supervisor.pickle_size','supervisor.partition',
    'supervisor.validate_results','supervisor.admit_entities','manifest.task_digest','manifest.reuse_digest',
    'manifest.canonical','manifest.encode_task','manifest.encode_output','supplied.admit','supplied.result_identity',
    'worker.intraday_context','intraday.validate_config','intraday.admit_facts','intraday.prepare',
    'intraday.prepare_operand','intraday.compute','intraday.indicator','intraday.band_values',
    'config.to_json','config.digest','context.to_json','context.digest','sdk.prepare_publication',
    'sdk.encode_result','sdk.decode_result','sdk.encode_receipt','sdk.publish','publisher.submit',
    'publisher.inner_submit','publisher.drain','publisher.inner_drain','sink.lookup','sink.read',
    'pool.thread_submit','pool.process_submit','future.result','parent.wait','delegate.original_partition',
    'trace.bind_labels','trace.install','trace.restore','trace.bookkeeping'))
OBSERVATIONS=frozenset(('returned_encoded_bytes','observed_pickle_size_check',
    'source_guaranteed_ascii_json_length','unavailable'))
EXCLUSIVE_DISPOSITION='exclusive_of_observed_spans_includes_unseparated_hook_bookkeeping'
ERROR_TYPE_PATTERN=r'[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+'


class TraceSpecificationError(ValueError):
    def __init__(self, code):
        self.code=code
        super().__init__('Trace specification failed: '+code)


_IDENTITY_LOCK=threading.Lock()
_PROCESS_NONCE=uuid.uuid4().hex
_PROCESS_PID=os.getpid()
_THREADS={}


def timeline():
    """Stable actual thread identity across reused delegate invocations.

    Retain thread objects so recycled numeric thread IDs cannot acquire old spans.
    Spawn/fork processes bind their own identity; no inherited-state assumption.
    """
    global _PROCESS_NONCE,_PROCESS_PID,_THREADS
    with _IDENTITY_LOCK:
        if os.getpid()!=_PROCESS_PID:
            _PROCESS_NONCE=uuid.uuid4().hex;_PROCESS_PID=os.getpid();_THREADS={}
        current=threading.current_thread()
        if current not in _THREADS:
            if len(_THREADS)>=64:raise TraceSpecificationError('map_limit')
            _THREADS[current]=(len(_THREADS),uuid.uuid4().hex,threading.get_native_id())
        ordinal,nonce,_=_THREADS[current]
        return 'p'+_PROCESS_NONCE+':t'+str(ordinal)+':'+nonce


def fragment_clock_and_timelines(ids):
    """Actual local clock/process/thread bindings; no global clock ordinal yet."""
    info=time.get_clock_info('perf_counter')
    if info.monotonic is not True or info.adjustable is not False:raise TraceSpecificationError('invalid_clock')
    clock={'name':'perf_counter','implementation':info.implementation,'monotonic':True,'adjustable':False,
        'reported_resolution_seconds':format(Decimal(str(info.resolution)),'f'),
        'qpc_frequency_hz':None,'cross_process_raw_ticks_qualified':False}
    with _IDENTITY_LOCK:
        bindings=[]
        for ordinal,nonce,native_id in _THREADS.values():
            tid='p'+_PROCESS_NONCE+':t'+str(ordinal)+':'+nonce
            if tid in ids:
                bindings.append({'id':tid,'thread_ordinal':ordinal,'thread_startup_nonce':nonce,
                    'native_thread_id':native_id})
        if {t['id'] for t in bindings}!=set(ids):raise TraceSpecificationError('unknown_timeline')
        return {'pid':os.getpid(),'process_startup_nonce':_PROCESS_NONCE},clock,bindings


@dataclass
class _Frame:
    id: str
    parent_id: str|None
    timeline_id: str
    operation: str
    scope_id: str
    task_sha256: str|None
    started: int
    child_ns: int=0
    last_child_end: int|None=None


class Collector:
    """Bounded encoded event buffer and per-thread stacks, no per-row hooks.

    Byte bounds refer to actual serialized sidecars, not Python physical heap.
    Actual whole-Job sampled RSS remains a separate mandatory acceptance gate.
    """
    def __init__(self, reservation_id, *, max_spans, max_bytes, operations):
        if type(reservation_id) is not str or not re.fullmatch('[a-z][a-z0-9_.:-]{0,63}',reservation_id):raise TraceSpecificationError('unknown_invocation')
        if type(max_spans) is not int or not 1<=max_spans<=131072 or type(max_bytes) is not int or not 4096<=max_bytes<=16777216:raise TraceSpecificationError('byte_limit')
        self.reservation_id=reservation_id;self.max_spans=max_spans;self.max_bytes=max_bytes
        self.operations=frozenset(operations)
        if not self.operations<=OPERATIONS:raise TraceSpecificationError('collector_error')
        self.lock=threading.RLock();self.local=threading.local()
        self.frames=[];self.encoded_bytes=0;self.attempts=0;self.thread_attempts={};self.counters={}
        self.failure_reason=None;self.labels={};self.label_owners=();self.active=0
        self.default_scope='stage:unbound'

    def failure(self,code):
        with self.lock:
            if self.failure_reason is None:self.failure_reason=code

    @contextmanager
    def suppress(self):
        before=getattr(self.local,'suppressed',False);self.local.suppressed=True
        try:yield
        finally:self.local.suppressed=before

    def bind_labels(self, tasks, original_task_getter=None):
        """Bind primitive labels before hooks; no hooked hash getter in labels."""
        with self.suppress():
            if type(tasks) not in (tuple,list) or len(tasks)>16:raise TraceSpecificationError('map_limit')
            labels={}
            for task in tasks:
                value=original_task_getter(task) if original_task_getter else task.task_sha256
                if type(value) is not str or not re.fullmatch('[0-9a-f]{64}',value):raise TraceSpecificationError('unknown_invocation')
                labels[id(task)]=('task:'+value,value)
            if len(labels)>16:raise TraceSpecificationError('map_limit')
            self.labels=labels;self.label_owners=tuple(tasks)

    @contextmanager
    def phase(self, scope_id):
        if type(scope_id) is not str or not re.fullmatch('[a-z][a-z0-9_.:-]{0,127}',scope_id):raise TraceSpecificationError('unknown_invocation')
        before=getattr(self.local,'current_label',None);self.local.current_label=(scope_id,None)
        try:yield
        finally:
            if before is None:self.local.__dict__.pop('current_label',None)
            else:self.local.current_label=before

    def label(self,args):
        for arg in args:
            label=self.labels.get(id(arg))
            if label is not None:return label
        return getattr(self.local,'current_label',(self.default_scope,None))

    def begin(self,operation,args):
        if getattr(self.local,'suppressed',False):return None
        try:
            if operation not in self.operations:raise TraceSpecificationError('collector_error')
            with self.suppress():
                tid=timeline();stack=getattr(self.local,'stack',None)
                if stack is None:stack=[];self.local.stack=stack
                if len(stack)>=64:raise TraceSpecificationError('event_limit')
                scope,task=self.label(args)
                with self.lock:
                    if self.attempts>=self.max_spans or self.thread_attempts.get(tid,0)>=32768:raise TraceSpecificationError('event_limit')
                    index=self.attempts;self.attempts+=1;self.active+=1
                    self.thread_attempts[tid]=self.thread_attempts.get(tid,0)+1
                frame=_Frame(self.reservation_id+':e'+str(index),stack[-1].id if stack else None,
                    tid,operation,scope,task,time.perf_counter_ns())
                stack.append(frame)
                return frame
        except BaseException as error:
            self.failure(error.code if type(error) is TraceSpecificationError else 'collector_error')
            return None

    def finish(self,frame,*,result=None,error=None):
        if frame is None:return
        try:
            ended=time.perf_counter_ns()
            with self.suppress():
                stack=self.local.stack
                if not stack or stack[-1] is not frame:raise TraceSpecificationError('invalid_nesting')
                stack.pop()
                if not 0<=frame.started<=ended<=9223372036854775807:raise TraceSpecificationError('invalid_clock')
                inclusive=ended-frame.started
                if frame.child_ns>inclusive:raise TraceSpecificationError('invalid_nesting')
                if stack:
                    parent=stack[-1]
                    if frame.started<parent.started or parent.last_child_end is not None and frame.started<parent.last_child_end:raise TraceSpecificationError('invalid_nesting')
                    parent.child_ns+=inclusive;parent.last_child_end=ended
                observation='unavailable';observed=None
                if error is None:
                    if frame.operation in ('manifest.canonical','manifest.encode_task','manifest.encode_output','sdk.encode_result','sdk.encode_receipt') and type(result) is bytes:
                        observation='returned_encoded_bytes';observed=len(result)
                    elif frame.operation=='supervisor.pickle_size' and type(result) is int and result>=0:
                        observation='observed_pickle_size_check';observed=result
                    elif frame.operation in ('config.to_json','context.to_json') and type(result) is str and result.isascii():
                        observation='source_guaranteed_ascii_json_length';observed=len(result)
                error_type=None
                if error is not None:
                    cls=type(error);name=cls.__module__+'.'+cls.__qualname__
                    error_type=name if len(name)<=128 and re.fullmatch(ERROR_TYPE_PATTERN,name) else 'builtins.BaseException'
                event={'id':frame.id,'parent_id':frame.parent_id,'reservation_id':self.reservation_id,
                    'timeline_id':frame.timeline_id,'operation':frame.operation,'scope_id':frame.scope_id,
                    'task_sha256':frame.task_sha256,'start_ns':str(frame.started),'end_ns':str(ended),
                    'inclusive_ns':str(inclusive),'exclusive_ns':str(inclusive-frame.child_ns),
                    'status':'returned' if error is None else 'rejected','error_type':error_type,
                    'byte_observation':observation,'observed_bytes':observed}
                encoded=wire(event)
                key=(frame.operation,frame.scope_id,observation)
                with self.lock:
                    self.active-=1
                    if self.encoded_bytes+len(encoded)+4096>self.max_bytes:raise TraceSpecificationError('byte_limit')
                    if key not in self.counters and len(self.counters)>=1024:raise TraceSpecificationError('map_limit')
                    self.frames.append(encoded);self.encoded_bytes+=len(encoded)
                    counter=self.counters.setdefault(key,[0,0,0,0])
                    counter[0]+=1;counter[1 if error is None else 2]+=1
                    counter[3]+=observed or 0
        except BaseException as issue:
            self.failure(issue.code if type(issue) is TraceSpecificationError else 'collector_error')

    def snapshot(self):
        """Local fragment, not the complete source/product/native trace envelope."""
        with self.lock:
            if self.active:self.failure('unterminated_span')
            spans=[json.loads(raw) for raw in self.frames]
            counters=[{'operation':op,'scope_id':scope,'attempts':v[0],'returned':v[1],
                'rejected':v[2],'byte_observation':obs,'returned_observed_bytes':None if obs=='unavailable' else v[3],
                'unique_physical_allocation_bytes':None} for (op,scope,obs),v in sorted(self.counters.items())]
            if self.failure_reason is None and (len(spans)!=self.attempts or sum(c['attempts'] for c in counters)!=self.attempts):self.failure('count_mismatch')
            process,clock,timelines=fragment_clock_and_timelines({s['timeline_id'] for s in spans})
            return {'schema':'eq142-local-trace-fragment:1','reservation_id':self.reservation_id,'complete':self.failure_reason is None,
                'failure_reason':self.failure_reason,'spans':spans,'counters':counters,
                'exclusive_cost_disposition':EXCLUSIVE_DISPOSITION,'process_binding':process,
                'clock':clock,'timeline_bindings':timelines}

    def persist(self,path):
        """Bounded incremental exclusive creation; never overwrite prior evidence."""
        path=Path(path);temporary=path.with_name(path.name+'.partial')
        if path.exists() or temporary.exists():raise TraceSpecificationError('duplicate_sidecar')
        snapshot=self.snapshot();encoder=json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
        count=0
        with temporary.open('xb') as out:
            for chunk in encoder.iterencode(snapshot):
                body=chunk.encode('ascii');count+=len(body)
                if count+1>self.max_bytes:
                    self.failure('byte_limit');raise TraceSpecificationError('byte_limit')
                out.write(body)
            out.write(b'\n');count+=1
        # Exclusive hard-link publication cannot replace a concurrent final file.
        # Both names count toward the declared temporary/final file allowance.
        os.link(temporary,path);temporary.unlink()
        return count


class HookSession:
    """Patch each inspected alias once, retaining original descriptors/objects."""
    def __init__(self,qualification,inventory,collector):
        if type(qualification) is not BoundSource or type(collector) is not Collector:raise TraceSpecificationError('source_mismatch')
        if digest(wire(inventory))!=qualification.hook_inventory_sha256:raise TraceSpecificationError('hook_alias_mismatch')
        self.qualification=qualification;self.inventory=json.loads(wire(inventory));self.collector=collector;self.patches=[]

    def wrapper(self,original,operation):
        @functools.wraps(original)
        def traced(*args,**kwargs):
            # Actual task arguments advance the invocation's primitive label.
            # Keep it across sibling kernel calls after supplied.admit returns.
            # Delegate/phase ownership resets this TLS at the enclosing boundary.
            frame=None;before=None
            try:
                for arg in (*args,*kwargs.values()):
                    label=self.collector.labels.get(id(arg))
                    if label is not None:
                        self.collector.local.current_label=label
                        break
                frame=self.collector.begin(operation,args)
                before=getattr(self.collector.local,'current_label',None)
                if frame is not None:self.collector.local.current_label=(frame.scope_id,frame.task_sha256)
            except BaseException:self.collector.failure_reason='collector_error'
            try:
                result=original(*args,**kwargs)
            except BaseException as error:
                try:self.collector.finish(frame,error=error)
                except BaseException:self.collector.failure_reason='collector_error'
                raise
            else:
                try:self.collector.finish(frame,result=result)
                except BaseException:self.collector.failure_reason='collector_error'
                return result
            finally:
                try:
                    if before is None:self.collector.local.__dict__.pop('current_label',None)
                    else:self.collector.local.current_label=before
                except BaseException:self.collector.failure_reason='collector_error'
        return traced

    def install(self):
        if self.patches:raise TraceSpecificationError('hook_alias_mismatch')
        self.qualification.check()
        planned=[];sites=set()
        for record in self.inventory['records']:
            owner,name=resolve(record['definition']);original=inspect.getattr_static(owner,name)
            wrapped=self.wrapper(function_of(original),record['operation'])
            replacement=property(wrapped,original.fset,original.fdel,original.__doc__) if type(original) is property else wrapped
            for site in record['patch_sites']:
                target,attribute=resolve(site);key=(id(target),attribute)
                if key in sites or inspect.getattr_static(target,attribute) is not original:raise TraceSpecificationError('hook_alias_mismatch')
                sites.add(key);planned.append((target,attribute,original,replacement))
        try:
            for target,attribute,original,replacement in planned:
                setattr(target,attribute,replacement);self.patches.append((target,attribute,original,replacement))
        except BaseException:
            self.restore();raise

    def restore(self):
        for target,attribute,original,replacement in reversed(self.patches):
            try:
                if inspect.getattr_static(target,attribute) is not replacement:
                    self.collector.failure('restoration_failure')
                    continue
                setattr(target,attribute,original)
            except BaseException:self.collector.failure('restoration_failure')
        self.patches=[]
        try:self.qualification.check()
        except BaseException:self.collector.failure('restoration_failure')

    def __enter__(self):
        self.install();return self

    def __exit__(self,*exc):
        self.restore()
        return False


def validate_fragment(fragment):
    """Recompute same-timeline interval unions and count/byte closures.

    Completeness of a fragment does not prove whole sample/native/product parity.
    """
    from eq142_graph import closed
    try:
        closed(fragment,'schema reservation_id complete failure_reason spans counters exclusive_cost_disposition process_binding clock timeline_bindings')
        if fragment['schema']!='eq142-local-trace-fragment:1' or fragment['complete'] is not True or fragment['failure_reason'] is not None:raise TraceSpecificationError('collector_error')
        if type(fragment['reservation_id']) is not str or re.fullmatch('[a-z][a-z0-9_.:-]{0,63}',fragment['reservation_id']) is None:raise TraceSpecificationError('unknown_invocation')
        if fragment['exclusive_cost_disposition']!=EXCLUSIVE_DISPOSITION:raise TraceSpecificationError('collector_error')
        if type(fragment['counters']) is not list or len(fragment['counters'])>1024:raise TraceSpecificationError('map_limit')
        process=fragment['process_binding'];closed(process,'pid process_startup_nonce')
        if type(process['pid']) is not int or process['pid']<=0 or type(process['process_startup_nonce']) is not str or re.fullmatch('[0-9a-f]{32}',process['process_startup_nonce']) is None:raise TraceSpecificationError('unknown_timeline')
        clock=fragment['clock'];closed(clock,'name implementation monotonic adjustable reported_resolution_seconds qpc_frequency_hz cross_process_raw_ticks_qualified')
        if clock['name']!='perf_counter' or type(clock['implementation']) is not str or not 1<=len(clock['implementation'])<=256 or clock['monotonic'] is not True or clock['adjustable'] is not False or clock['cross_process_raw_ticks_qualified'] is not False:raise TraceSpecificationError('invalid_clock')
        resolution=clock['reported_resolution_seconds']
        if type(resolution) is not str or re.fullmatch(r'0\.[0-9]{1,32}|[1-9][0-9]{0,8}(?:\.[0-9]{1,32})?',resolution) is None or Decimal(resolution)<=0:raise TraceSpecificationError('invalid_clock')
        if clock['qpc_frequency_hz'] is not None and (type(clock['qpc_frequency_hz']) is not int or clock['qpc_frequency_hz']<=0):raise TraceSpecificationError('invalid_clock')
        bindings=fragment['timeline_bindings']
        if type(bindings) is not list or len(bindings)>64:raise TraceSpecificationError('map_limit')
        timelines={}
        for binding in bindings:
            closed(binding,'id thread_ordinal thread_startup_nonce native_thread_id')
            if type(binding['thread_ordinal']) is not int or not 0<=binding['thread_ordinal']<64 or type(binding['native_thread_id']) is not int or binding['native_thread_id']<=0 or type(binding['thread_startup_nonce']) is not str or re.fullmatch('[0-9a-f]{32}',binding['thread_startup_nonce']) is None:raise TraceSpecificationError('unknown_timeline')
            expected='p'+process['process_startup_nonce']+':t'+str(binding['thread_ordinal'])+':'+binding['thread_startup_nonce']
            if binding['id']!=expected or expected in timelines:raise TraceSpecificationError('unknown_timeline')
            timelines[expected]=binding
        spans=fragment['spans']
        if type(spans) is not list or len(spans)>131072:raise TraceSpecificationError('event_limit')
        byid={s['id']:s for s in spans}
        if len(byid)!=len(spans):raise TraceSpecificationError('invalid_nesting')
        if {s['timeline_id'] for s in spans}!=set(timelines):raise TraceSpecificationError('unknown_timeline')
        sums={};children={key:[] for key in byid}
        for s in spans:
            closed(s,'id parent_id reservation_id timeline_id operation scope_id task_sha256 start_ns end_ns inclusive_ns exclusive_ns status error_type byte_observation observed_bytes')
            if s['reservation_id']!=fragment['reservation_id']:raise TraceSpecificationError('unknown_invocation')
            for field in ['id','reservation_id','timeline_id','scope_id']:
                if type(s[field]) is not str or not re.fullmatch('[a-z][a-z0-9_.:-]{0,127}',s[field]):raise TraceSpecificationError('unknown_invocation')
            if s['operation'] not in OPERATIONS or s['byte_observation'] not in OBSERVATIONS:raise TraceSpecificationError('collector_error')
            if s['task_sha256'] is not None and (type(s['task_sha256']) is not str or not re.fullmatch('[0-9a-f]{64}',s['task_sha256'])):raise TraceSpecificationError('unknown_invocation')
            if s['observed_bytes'] is not None and (type(s['observed_bytes']) is not int or not 0<=s['observed_bytes']<=9007199254740991):raise TraceSpecificationError('count_mismatch')
            if (s['byte_observation']=='unavailable')!=(s['observed_bytes'] is None):raise TraceSpecificationError('count_mismatch')
            for key in ['start_ns','end_ns','inclusive_ns','exclusive_ns']:
                value=s[key]
                if type(value) is not str or re.fullmatch('0|[1-9][0-9]{0,18}',value) is None or int(value)>9223372036854775807:raise TraceSpecificationError('invalid_clock')
            a,b=int(s['start_ns']),int(s['end_ns'])
            if b<a or int(s['inclusive_ns'])!=b-a:raise TraceSpecificationError('invalid_clock')
            parent=s['parent_id']
            if parent is not None:
                if parent not in byid:raise TraceSpecificationError('invalid_nesting')
                p=byid[parent]
                if p['timeline_id']!=s['timeline_id'] or not int(p['start_ns'])<=a<=b<=int(p['end_ns']):raise TraceSpecificationError('invalid_nesting')
                children[parent].append((a,b))
                chain={s['id']};cursor=parent
                while cursor is not None:
                    if cursor in chain or len(chain)>64:raise TraceSpecificationError('invalid_nesting')
                    chain.add(cursor);cursor=byid[cursor]['parent_id']
            if s['status'] not in ('returned','rejected') or (s['status']=='returned')!=(s['error_type'] is None):raise TraceSpecificationError('count_mismatch')
            if s['error_type'] is not None and (type(s['error_type']) is not str or len(s['error_type'])>128 or re.fullmatch(ERROR_TYPE_PATTERN,s['error_type']) is None):raise TraceSpecificationError('collector_error')
            if s['status']=='rejected' and s['byte_observation']!='unavailable':raise TraceSpecificationError('count_mismatch')
            key=(s['operation'],s['scope_id'],s['byte_observation']);v=sums.setdefault(key,[0,0,0,0])
            v[0]+=1;v[1 if s['status']=='returned' else 2]+=1;v[3]+=s['observed_bytes'] or 0
        for sid,intervals in children.items():
            total=0;end=None
            for a,b in sorted(intervals):
                if end is None or a>=end:total+=b-a
                elif b>end:total+=b-end
                end=b if end is None else max(end,b)
            if int(byid[sid]['exclusive_ns'])!=int(byid[sid]['inclusive_ns'])-total:raise TraceSpecificationError('invalid_nesting')
        actual={}
        for c in fragment['counters']:
            closed(c,'operation scope_id attempts returned rejected byte_observation returned_observed_bytes unique_physical_allocation_bytes')
            key=(c['operation'],c['scope_id'],c['byte_observation'])
            if key in actual or c['unique_physical_allocation_bytes'] is not None:raise TraceSpecificationError('count_mismatch')
            if c['operation'] not in OPERATIONS or c['byte_observation'] not in OBSERVATIONS:raise TraceSpecificationError('count_mismatch')
            if c['returned_observed_bytes'] is not None and (type(c['returned_observed_bytes']) is not int or not 0<=c['returned_observed_bytes']<=9007199254740991):raise TraceSpecificationError('count_mismatch')
            if (c['byte_observation']=='unavailable')!=(c['returned_observed_bytes'] is None):raise TraceSpecificationError('count_mismatch')
            for field in ['attempts','returned','rejected']:
                if type(c[field]) is not int or not 0<=c[field]<=131072:raise TraceSpecificationError('count_mismatch')
            actual[key]=[c['attempts'],c['returned'],c['rejected'],c['returned_observed_bytes'] or 0]
        if actual!=sums:raise TraceSpecificationError('count_mismatch')
        return None
    except TraceSpecificationError:raise
    except Exception:raise TraceSpecificationError('invalid_nesting') from None
