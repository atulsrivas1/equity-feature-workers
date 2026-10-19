"""Actual parent Future/partition joins; never align cross-process raw clocks.

The ledger retains bounded object owners only within one development run. Its
bookkeeping is instrumentation cost outside selected original hook intervals.
"""
import functools
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

    def tasks_for_partition(self,partition):
        hashes=[self.labels[id(item.task)] for item in partition.items]
        reservation=self.reservations.get(invocation_key(hashes))
        if reservation is None:raise TraceSpecificationError('unknown_invocation')
        return reservation

    def before(self,operation,args):
        if operation in ('pool.thread_submit','pool.process_submit'):
            # Bound inspected Executor.submit(self, fn, partition, ...) source.
            if len(args)<3:raise TraceSpecificationError('unknown_invocation')
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
            if self.relation_count+len(before['related'])>4096:raise TraceSpecificationError('event_limit')
            if operation.startswith('pool.') and error is None:
                if id(result) in self.futures or len(self.futures)>=16:raise TraceSpecificationError('map_limit')
                self.futures[id(result)]={'owner':result,'reservation':before['related'][0]}
            self.rows.append({**before,'parent_end_ns':end,'status':'rejected' if error is not None else 'returned'})
            self.relation_count+=len(before['related'])

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
        if operation not in ('pool.thread_submit','pool.process_submit','future.result','parent.wait'):return traced
        @functools.wraps(original)
        def observed(*args,**kwargs):
            before=None
            try:before=self.ledger.before(operation,args)
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
