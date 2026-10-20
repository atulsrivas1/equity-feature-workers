"""Actual parent Future/partition joins; never align cross-process raw clocks.

The ledger retains bounded object owners only within one development run. Its
bookkeeping is instrumentation cost outside selected original hook intervals.
"""
import functools
import inspect
import threading
import time

from eq142_delegate import RoutingHookSession,invocation_key
from eq142_trace import TraceSpecificationError,timeline


class CausalLedger:
    def __init__(self,plan,items,collector):
        self.collector=collector;self.mode=plan['mode'];self.reservations={r['key']:r for r in plan['reservations']}
        self.labels={id(item.task):h for item in items for h in [item.task.task_sha256]}
        self.task_owners=tuple(item.task for item in items)
        self.futures={};self.rows=[];self.relation_count=0;self.lock=threading.RLock()
        self.shutdowns=[];self.executor_owner=None
        self.terminal_rows=[];self.terminal_attempted=False;self.terminal_complete=False
        from eq142_graph import bind_source,resolve,function_of
        self.source=bind_source(plan['expected_inventory'])
        self.terminal_functions={record['operation'].split('.')[-1]:function_of(inspect.getattr_static(*resolve(record['definition'])))
            for record in plan['expected_inventory']['hooks']['terminal_observations']}
        from concurrent.futures import Future,ThreadPoolExecutor,ProcessPoolExecutor
        self.future_type=Future
        self.executor_type={'thread':ThreadPoolExecutor,'process':ProcessPoolExecutor}.get(self.mode)

    def bind_executor(self,owner):
        if self.executor_type is None or type(owner) is not self.executor_type:raise TraceSpecificationError('unknown_invocation')
        if self.executor_owner is not None and self.executor_owner is not owner:raise TraceSpecificationError('unknown_invocation')
        self.executor_owner=owner

    def tasks_for_partition(self,partition):
        hashes=[self.labels[id(item.task)] for item in partition.items]
        reservation=self.reservations.get(invocation_key(hashes))
        if reservation is None:raise TraceSpecificationError('unknown_invocation')
        return reservation

    def before(self,operation,args,kwargs=None,signature=None):
        if operation in ('pool.thread_shutdown','pool.process_shutdown'):
            if operation!=f'pool.{self.mode}_shutdown' or signature is None:raise TraceSpecificationError('unknown_invocation')
            bound=signature.bind(*args,**(kwargs or {}));bound.apply_defaults()
            self.bind_executor(bound.arguments['self'])
            waited=bound.arguments['wait'];cancel=bound.arguments['cancel_futures']
            if type(waited) is not bool or type(cancel) is not bool:raise TraceSpecificationError('collector_error')
            return {'operation':operation,'executor_id':'owned-executor:0','wait':waited,'cancel_futures':cancel,
                'parent_timeline_id':timeline(),'parent_start_ns':str(time.perf_counter_ns())}
        if operation in ('pool.thread_submit','pool.process_submit'):
            # Bound inspected Executor.submit(self, fn, partition, ...) source.
            if len(args)<3:raise TraceSpecificationError('unknown_invocation')
            self.bind_executor(args[0])
            related=[self.tasks_for_partition(args[2])]
        elif operation=='future.result':
            record=self.futures.get(id(args[0]))
            if record is None or record['owner'] is not args[0]:raise TraceSpecificationError('unknown_invocation')
            related=[record['reservation']]
        elif operation=='parent.wait':
            related=[]
            for future in args[0]:
                record=self.futures.get(id(future))
                if record is None or record['owner'] is not future:raise TraceSpecificationError('unknown_invocation')
                related.append(record['reservation'])
            if not 1<=len(related)<=16:raise TraceSpecificationError('map_limit')
        else:return None
        return {'operation':operation,'related':related,'parent_timeline_id':timeline(),
            'parent_start_ns':str(time.perf_counter_ns())}

    def after(self,before,*,result=None,error=None):
        if before is None:return
        end=str(time.perf_counter_ns());operation=before['operation']
        with self.lock:
            if operation in ('pool.thread_shutdown','pool.process_shutdown'):
                if self.shutdowns:raise TraceSpecificationError('event_limit')
                self.shutdowns.append({**before,'parent_end_ns':end,'status':'rejected' if error is not None else 'returned',
                    'error_type':None if error is None else type(error).__module__+'.'+type(error).__qualname__})
                return
            if self.relation_count+len(before['related'])>4096:raise TraceSpecificationError('event_limit')
            if operation.startswith('pool.') and error is None:
                if type(result) is not self.future_type:raise TraceSpecificationError('unknown_invocation')
                if id(result) in self.futures or len(self.futures)>=16:raise TraceSpecificationError('map_limit')
                self.futures[id(result)]={'owner':result,'reservation':before['related'][0]}
            self.rows.append({**before,'parent_end_ns':end,'status':'rejected' if error is not None else 'returned'})
            self.relation_count+=len(before['related'])

    def shutdown_snapshot(self):
        with self.lock:return [dict(row) for row in self.shutdowns]

    def terminal_snapshot(self):
        """Manual non-atomic public queries after hook/profile restoration only."""
        self.source.check()
        if self.terminal_attempted:
            if not self.terminal_complete:raise TraceSpecificationError('collector_error')
            return self.retained_terminal_rows()
        self.terminal_attempted=True
        with self.lock:owners=tuple(self.futures.values())
        for record in sorted(owners,key=lambda r:r['reservation']['id']):
            started=time.perf_counter_ns();values={name:function(record['owner']) for name,function in self.terminal_functions.items()}
            if any(type(value) is not bool for value in values.values()):raise TraceSpecificationError('collector_error')
            self.terminal_rows.append({'invocation_id':record['reservation']['id'],'task_hashes':list(record['reservation']['task_hashes']),
                'parent_timeline_id':timeline(),'capture_start_ns':str(started),'capture_end_ns':str(time.perf_counter_ns()),**values})
        self.source.check()
        if self.collector.failure_reason is not None:raise TraceSpecificationError('collector_error')
        self.terminal_complete=True
        return self.retained_terminal_rows()

    def retained_terminal_rows(self):
        return [{**row,'task_hashes':list(row['task_hashes'])} for row in self.terminal_rows]

    def require_waited_shutdown(self,terminal_rows):
        if not self.terminal_complete or self.collector.failure_reason is not None:raise TraceSpecificationError('collector_error')
        ids=[row['invocation_id'] for row in terminal_rows]
        if (len(ids)!=len(self.futures) or len(set(ids))!=len(ids)
                or set(ids)!={record['reservation']['id'] for record in self.futures.values()}):
            raise TraceSpecificationError('unknown_invocation')
        if self.mode=='sequential':
            if self.shutdowns or self.futures or self.executor_owner is not None:raise TraceSpecificationError('unknown_invocation')
            return
        if len(self.shutdowns)!=1:raise TraceSpecificationError('missing_sidecar')
        row=self.shutdowns[0]
        if row['status']!='returned' or row['wait'] is not True or row['cancel_futures'] is not True:
            raise TraceSpecificationError('collector_error')
        if any(r['done'] is not True or r['running'] is not False for r in terminal_rows):
            raise TraceSpecificationError('collector_error')

    def snapshot(self,joined):
        """Resolve children only from actual validated retained fragment records."""
        import json
        from pathlib import Path
        from eq142_evidence import bounded_read
        from eq142_graph import digest
        directory,fragments=joined
        by_invocation={}
        for ref in fragments['fragments']:
            raw=bounded_read(Path(directory)/ref['retained_name'],ref['bytes'])
            if len(raw)!=ref['bytes'] or digest(raw)!=ref['sha256']:raise TraceSpecificationError('source_mismatch')
            body=json.loads(raw)
            by_invocation[ref['reservation_id']]=[t['id'] for t in body['timeline_bindings']]
        rows=[]
        for raw in self.rows:
            for reservation in raw['related']:
                children=by_invocation.get(reservation['id'])
                if children is None:raise TraceSpecificationError('missing_sidecar')
                rows.append({'id':'causal:%04d'%len(rows),
                    'kind':'dispatch' if raw['operation'].startswith('pool.') else 'result_join' if raw['operation']=='future.result' else 'parent_wait',
                    'parent_timeline_id':raw['parent_timeline_id'],'invocation_id':reservation['id'],
                    'task_hashes':reservation['task_hashes'],
                    'parent_start_ns':raw['parent_start_ns'],'parent_end_ns':raw['parent_end_ns'],
                    'child_timeline_ids':children,'cross_process_interval_subtraction':False,
                    'status':raw['status']})
                if len(rows)>4096:raise TraceSpecificationError('event_limit')
        return rows


class CausalHookSession(RoutingHookSession):
    def __init__(self,*args,ledger,**kwargs):
        super().__init__(*args,**kwargs);self.ledger=ledger

    def wrapper(self,original,operation):
        traced=super().wrapper(original,operation)
        shutdown=operation in ('pool.thread_shutdown','pool.process_shutdown')
        signature=inspect.signature(original) if shutdown else None
        if operation not in ('pool.thread_submit','pool.process_submit','future.result','parent.wait',
                'pool.thread_shutdown','pool.process_shutdown'):return traced
        @functools.wraps(original)
        def observed(*args,**kwargs):
            before=None
            try:before=self.ledger.before(operation,args,kwargs,signature)
            except BaseException:self.collector.failure('collector_error')
            try:result=traced(*args,**kwargs)
            except BaseException as error:
                try:self.ledger.after(before,error=error)
                except BaseException:self.collector.failure('collector_error')
                raise
            else:
                try:self.ledger.after(before,result=result)
                except BaseException:self.collector.failure('collector_error')
                return result
        return observed
