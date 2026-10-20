"""B-only original publication observations and source-derived admission replay.

This tooling retains actual commits separately from accepted output metadata.
It does not inspect private frame locals or change a publication decision.
"""
import functools
import inspect
import re
import sys
import threading
import time

from eq142_causal import CausalHookSession
from eq142_trace import timeline

OBSERVER_RESERVATION=69793792
OPERATIONS=('sink.commit','publisher.inner_drain','manifest.encode_output')


def replay_sizes(sizes,initial,maximum):
    """Accepted-only counter, derived from the frozen original source branch."""
    if (type(sizes) is not list or len(sizes)>16 or type(initial) is not int
            or type(maximum) is not int or not 0<=initial<=maximum<=1073741824):
        raise ValueError('publication replay bound')
    rows=[];used=0
    for size in sizes:
        if type(size) is not int or not 0<size<=1048576:raise ValueError('publication wire bound')
        before=used;accepted=initial+used+size<=maximum
        if accepted:used+=size
        rows.append({'candidate_bytes':size,'before_bytes':before,'after_bytes':used,'accepted':accepted})
    return rows


def verify_publication(record,facts,proof_bodies,*,initial,maximum,combined,publishing,require_success=True):
    """Independent arithmetic replay cross-checks actual original outcomes."""
    from equity_feature_workers import decode_output
    from equity_feature_io_sdk import encode_receipt
    from eq142_graph import wire
    from eq142_evidence import original_reason
    # These inputs are facts, not source/provenance certificates. The live
    # adapter and retained root reader establish their respective provenance.
    from equity_feature_workers import TaskManifest,OutputManifest
    from equity_feature_io_sdk import decode_receipt
    from eq142_graph import digest
    if type(publishing) is not bool or type(require_success) is not bool:
        raise ValueError('publication boolean policy')
    replay_sizes([],initial,maximum)
    if type(combined) is not int or not initial<=combined<=maximum:
        raise ValueError('publication combined reservation')
    if type(facts) is not dict or set(facts)!={'reserved_resident_bytes','tasks'}:
        raise ValueError('publication outcome facts')
    if (type(facts['reserved_resident_bytes']) is not int
            or facts['reserved_resident_bytes']!=initial
            or type(facts['tasks']) is not list or len(facts['tasks'])>16):
        raise ValueError('publication outcome facts')
    actual_facts={}
    for row in facts['tasks']:
        if type(row) is not dict or set(row)!={'task','output','reason'}:
            raise ValueError('publication task facts')
        if (type(row['task']) is not TaskManifest
                or row['output'] is not None and type(row['output']) is not OutputManifest
                or row['reason'] is not None and type(row['reason']) is not str):
            raise ValueError('publication task fact type')
        original_reason(row['reason'])
        if row['output'] is not None and (row['output'].task!=row['task'] or row['reason'] is not None):
            raise ValueError('publication task fact output')
        task=row['task'].task_sha256
        if task in actual_facts:raise ValueError('publication duplicate task facts')
        actual_facts[task]=row
    if type(proof_bodies) is not dict or len(proof_bodies)>32:
        raise ValueError('publication proof population')
    for sha,body in proof_bodies.items():
        if (type(sha) is not str or re.fullmatch('[0-9a-f]{64}',sha) is None
                or type(body) is not bytes or not 0<len(body)<=1048576
                or digest(body)!=sha):raise ValueError('publication proof body')
    used_proofs=set()
    def proof(reference):
        if (type(reference) is not dict or set(reference)!={'codec','sha256','bytes','retained_name'}
                or type(reference['codec']) is not str or reference['codec'] not in ('encode_output','encode_receipt')
                or type(reference['sha256']) is not str or reference['sha256'] not in proof_bodies
                or type(reference['bytes']) is not int or reference['retained_name']!=reference['sha256']+'.json'):
            raise ValueError('publication proof reference')
        body=proof_bodies[reference['sha256']]
        if len(body)!=reference['bytes']:raise ValueError('publication proof length')
        if reference['codec']=='encode_receipt':
            if len(body)>400000:raise ValueError('publication receipt wire bound')
            decode_receipt(body)
        else:decode_output(body)
        used_proofs.add(reference['sha256'])
        return body
    def closed(value,keys):
        if type(value) is not dict or set(value)!=set(keys.split()):raise ValueError('publication closed record')
    def index(value):
        if type(value) is not int or not 0<=value<len(record['events']):raise ValueError('publication event reference')
        return value
    def identity(value):
        if type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None:raise ValueError('publication task identity')
    closed(record,'schema evidence events commits drains candidates complete failure_reason declared_logical_observer_allowance_bytes retention_failures retention_failures_truncated physical_enforcement full_sample_accepted')
    if (record['schema']!='eq142-publication-observation:1' or record['evidence']!='actual_original_B_calls'
            or record['declared_logical_observer_allowance_bytes']!=OBSERVER_RESERVATION
            or type(record['declared_logical_observer_allowance_bytes']) is not int
            or record['physical_enforcement'] is not False or record['full_sample_accepted'] is not False
            or len(wire(record))>65536):raise ValueError('publication record scope/bound')
    if (record['complete'] is not True
            or record['failure_reason'] is not None or record['retention_failures']!=[]
            or type(record['retention_failures']) is not list
            or record['retention_failures_truncated'] is not False):raise ValueError('incomplete publication observation')
    events=record['events'];candidates=record['candidates']
    for key,limit in (('events',64),('candidates',16),('commits',16),('drains',16)):
        if type(record[key]) is not list or len(record[key])>limit:raise ValueError('publication record population')
    for event in events:
        closed(event,'operation start_ns end_ns timeline_id status error_type sequence')
    if len(events)>64 or any(event['sequence']!=i for i,event in enumerate(events)):
        raise ValueError('publication event sequence')
    prior_end=0;timelines=set()
    for event in events:
        closed(event,'operation start_ns end_ns timeline_id status error_type sequence')
        index(event['sequence'])
        if (event['operation'] not in OPERATIONS or event['status'] not in ('returned','rejected')
                or type(event['timeline_id']) is not str or not 1<=len(event['timeline_id'])<=128
                or (event['status']=='returned')!=(event['error_type'] is None)):
            raise ValueError('publication event scope')
        timelines.add(event['timeline_id'])
        if event['error_type'] is not None and (type(event['error_type']) is not str or not 1<=len(event['error_type'])<=128):
            raise ValueError('publication error identity')
        for key in ('start_ns','end_ns'):
            if (type(event[key]) is not str or re.fullmatch('0|[1-9][0-9]{0,18}',event[key]) is None
                    or int(event[key])>9223372036854775807):raise ValueError('publication interval type')
        if int(event['start_ns'])>int(event['end_ns']) or int(event['end_ns'])<prior_end:
            raise ValueError('publication local interval')
        prior_end=int(event['end_ns'])
    if len(timelines)>1:raise ValueError('publication coordinator timeline')
    represented=set();commit_bodies={};progress_rows=0
    for key,operation,fields in (('commits','sink.commit','event receipt'),
            ('drains','publisher.inner_drain','event rows'),
            ('candidates','manifest.encode_output','event output task_sha256 drain_event drain_row commit_events')):
        for row in record[key]:
            closed(row,fields);eid=index(row['event'])
            if eid in represented or events[eid]['operation']!=operation:raise ValueError('publication operation join')
            represented.add(eid)
            if key=='commits':
                if (row['receipt'] is not None)!=(events[eid]['status']=='returned'):raise ValueError('publication commit status')
                if row['receipt'] is not None:
                    if row['receipt']['codec']!='encode_receipt':raise ValueError('publication receipt codec')
                    body=proof(row['receipt'])
                    if len(body)>400000:raise ValueError('publication receipt wire bound')
                    commit_bodies[eid]=body
            elif key=='drains':
                if type(row['rows']) is not list or len(row['rows'])>16:raise ValueError('publication progress rows')
                progress_rows+=len(row['rows'])
                if events[eid]['status']=='rejected' and row['rows']:raise ValueError('publication rejected drain rows')
                for progress in row['rows']:
                    closed(progress,'task_sha256 output_present reason');identity(progress['task_sha256'])
                    if type(progress['output_present']) is not bool:raise ValueError('publication progress state')
                    original_reason(progress['reason'])
                    if progress['output_present'] and progress['reason'] is not None:raise ValueError('publication progress reason')
    if represented!=set(range(len(events))) or progress_rows>16:raise ValueError('publication event closure')
    for commit in record['commits']:
        c=events[commit['event']]
        enclosing=[d for d in record['drains'] if int(events[d['event']]['start_ns'])<=int(c['start_ns'])
            <=int(c['end_ns'])<=int(events[d['event']]['end_ns'])]
        if len(enclosing)!=1:raise ValueError('publication commit drain ownership')
    if any(c['output'] is None for c in candidates):raise ValueError('publication original encode failed')
    sizes=[len(proof(c['output'])) for c in candidates]
    replay=replay_sizes(sizes,initial,maximum)
    actual=actual_facts
    if facts['reserved_resident_bytes']!=initial:raise ValueError('publication actual initial admission')
    seen=set();used_commits=set();rows=[]
    expected_progress=[(d['event'],i) for d in record['drains'] for i,r in enumerate(d['rows']) if r['output_present']]
    if [(c['drain_event'],c['drain_row']) for c in candidates]!=expected_progress:
        raise ValueError('publication progress candidate order/population')
    for candidate,derived in zip(candidates,replay):
        task=candidate['task_sha256'];execution=actual.get(task)
        identity(task)
        if task in seen or execution is None:raise ValueError('publication duplicate/foreign task')
        seen.add(task);output=decode_output(proof(candidate['output']))
        if output.task!=execution['task'] or output.task.task_sha256!=task:raise ValueError('publication candidate task')
        if candidate['output']['codec']!='encode_output':raise ValueError('publication output codec')
        drain_event=index(candidate['drain_event']);event=events[candidate['event']];drain=events[drain_event]
        drain_rows=next((d['rows'] for d in record['drains'] if d['event']==drain_event),None)
        row_index=candidate['drain_row']
        if (type(row_index) is not int or drain_rows is None or not 0<=row_index<len(drain_rows)
                or drain_rows[row_index]['task_sha256']!=task):raise ValueError('publication progress task join')
        if (candidate['event']<=drain_event or int(event['start_ns'])<int(drain['end_ns'])
                or event['timeline_id']!=drain['timeline_id']):raise ValueError('publication drain order')
        matched=candidate['commit_events']
        receipt_body=encode_receipt(output.receipt) if output.receipt is not None else None
        expected_matches=[eid for eid,body in commit_bodies.items() if body==receipt_body]
        if type(matched) is not list or any(type(i) is not int for i in matched) or matched!=expected_matches:raise ValueError('publication receipt codec join')
        if require_success and len(matched)!=1:raise ValueError('publication commit population')
        for commit in matched:
            if commit in used_commits or commit>=drain_event:raise ValueError('publication commit join')
            used_commits.add(commit)
        if derived['accepted']:
            if execution['output']!=output or execution['reason'] is not None:raise ValueError('publication actual admission differs')
        elif execution['output'] is not None or execution['reason']!='RESOURCE_LIMIT':
            raise ValueError('publication actual refusal differs')
        combined_value=combined+derived['after_bytes']
        rows.append({**derived,'task_sha256':task,'candidate_event':candidate['event'],
            'combined_logical_resident_bytes':combined_value,'combined_within_cap':combined_value<=maximum})
    if require_success:
        if (any(e['status']!='returned' for e in events)
                or any(row['reason'] is not None for d in record['drains'] for row in d['rows'])
                or len(used_commits)!=len(record['commits'])
                or any(not r['accepted'] or not r['combined_within_cap'] for r in rows)
                or publishing and seen!=set(actual)
                or not publishing and events):raise ValueError('publication successful-path closure')
    if set(proof_bodies)!=used_proofs:raise ValueError('publication extra proof body')
    return {'evidence':'source_derived_per_candidate_admission_with_actual_outcome_cross_check',
        'rows':rows,'cumulative_accepted_output_metadata_bytes':sum(r['candidate_bytes'] for r in rows if r['accepted']),
        'counter_directly_observed':False,'physical_enforcement':False,'full_sample_accepted':False}


class PublicationLedger:
    def __init__(self,bound,*,publisher,sink,initial,maximum,combined):
        import equity_feature_workers as exports
        import equity_feature_workers.supervisor as supervisor
        from equity_feature_workers.publication import SerialPublisher,PublicationProgress
        from equity_feature_workers import OutputManifest
        from equity_feature_io_contracts import CompletionReceipt
        from supplied_memory import SerializedMemorySink
        bound.check()
        if (publisher is None)!=(sink is None):raise ValueError('publication owner pair')
        if publisher is not None and (type(publisher) is not SerialPublisher or type(sink) is not SerializedMemorySink):
            raise ValueError('publication owner type')
        replay_sizes([],initial,maximum)
        if type(combined) is not int or not initial<=combined<=maximum:raise ValueError('publication combined reservation')
        self.bound=bound;self.publisher=publisher;self.sink=sink;self.owner=threading.current_thread()
        self.initial=initial;self.maximum=maximum;self.combined=combined
        self.supervisor_module=supervisor;self.exports=exports;self.supervisor_class=supervisor.BoundedSupervisor
        self.caller=inspect.getattr_static(self.supervisor_class,'_run');self.caller_code=self.caller.__code__
        if (self.caller.__module__!='equity_feature_workers.supervisor'
                or self.caller.__qualname__!='BoundedSupervisor._run' or self.caller_code.co_firstlineno!=434):
            raise ValueError('publication caller source')
        self.progress_type=PublicationProgress;self.output_type=OutputManifest;self.receipt_type=CompletionReceipt
        self.events=[];self.counts={op:0 for op in OPERATIONS};self.progress_count=0;self.failure=None
        self.attempted=False;self.retained=None
        self.check()

    def check(self):
        self.bound.check()
        if (self.supervisor_module.BoundedSupervisor is not self.supervisor_class
                or self.exports.BoundedSupervisor is not self.supervisor_class
                or inspect.getattr_static(self.supervisor_class,'_run') is not self.caller
                or self.caller.__code__ is not self.caller_code):raise ValueError('publication caller changed')

    def fail(self):
        if self.failure is None:self.failure='publication_observer_failed'

    def before(self,operation,args,caller):
        if operation=='manifest.encode_output' and caller is not self.caller_code:return None
        if threading.current_thread() is not self.owner:raise ValueError('publication coordinator owner')
        if self.publisher is None:raise ValueError('unexpected publication')
        expected=self.sink if operation=='sink.commit' else self.publisher
        if not args or operation!='manifest.encode_output' and args[0] is not expected:
            raise ValueError('publication invocation owner')
        if operation=='manifest.encode_output' and type(args[0]) is not self.output_type:
            raise ValueError('publication candidate type')
        if self.counts[operation]>=16 or sum(self.counts.values())>=64:raise ValueError('publication event bound')
        self.counts[operation]+=1
        return {'operation':operation,'start_ns':str(time.perf_counter_ns()),'timeline_id':timeline(),
            'argument':args[0] if operation=='manifest.encode_output' else None}

    def after(self,before,result=None,error=None):
        if before is None:return
        operation=before['operation']
        if error is None:
            if operation=='sink.commit' and type(result) is not self.receipt_type:raise ValueError('publication receipt type')
            if operation=='manifest.encode_output' and (type(result) is not bytes or not 0<len(result)<=1048576):
                raise ValueError('publication output bytes')
            if operation=='publisher.inner_drain':
                if (type(result) is not tuple or self.progress_count+len(result)>16
                        or any(type(row) is not self.progress_type for row in result)):
                    raise ValueError('publication progress bound/type')
                self.progress_count+=len(result)
        self.events.append({**before,'end_ns':str(time.perf_counter_ns()),
            'status':'returned' if error is None else 'rejected',
            'error_type':None if error is None else type(error).__module__+'.'+type(error).__qualname__,
            'result':result if error is None else None})

    def snapshot(self,store):
        """Called after hooks restore; retain available proofs even on failure."""
        from equity_feature_io_sdk import encode_receipt
        if self.attempted:
            if self.retained is None:raise ValueError('publication retention previously failed')
            return self.retained
        self.attempted=True
        commits=[];drains=[];candidates=[];events=[];failures=[];truncated=False
        def failed(event,stage,error):
            nonlocal truncated
            self.fail()
            if len(failures)>=64:truncated=True;return
            name=type(error).__module__+'.'+type(error).__qualname__
            failures.append({'event':event,'stage':stage,'error_type':name[:128],
                'error_type_truncated':len(name)>128})
        source_ok=True
        try:self.check()
        except BaseException as error:
            source_ok=False;failed(None,'source_check',error)
        for index,event in enumerate(self.events):
            meta={k:event[k] for k in ('operation','start_ns','end_ns','timeline_id','status','error_type')}
            meta['sequence']=index;events.append(meta)
            if event['operation']=='sink.commit':
                ref=None
                if event['status']=='returned':
                    try:
                        if not source_ok:raise ValueError('unqualified receipt codec')
                        body=encode_receipt(event['result'])
                        if len(body)>400000:raise ValueError('publication receipt wire bound')
                        ref=store.put('encode_receipt',body)
                    except BaseException as error:failed(index,'commit_proof',error)
                commits.append({'event':index,'receipt':ref})
            elif event['operation']=='publisher.inner_drain':
                rows=[]
                try:
                    for row in event['result'] or ():
                        rows.append({'task_sha256':row.task_sha256,'output_present':row.output is not None,
                            'reason':row.reason,'output_owner':row.output})
                except BaseException as error:failed(index,'drain_copy',error)
                drains.append({'event':index,'rows':rows})
            else:
                output=event['argument'];ref=None
                if event['status']=='returned':
                    try:ref=store.put('encode_output',event['result'])
                    except BaseException as error:failed(index,'candidate_proof',error)
                candidates.append({'event':index,'output':ref,'output_owner':output,
                    'task_sha256':None,'drain_event':None,'drain_row':None,'commit_events':[]})
        # Resolve immutable original objects only after instrumentation restoration.
        for candidate in candidates:
            output=candidate.pop('output_owner')
            try:
                matches=[(d['event'],i,row) for d in drains for i,row in enumerate(d['rows']) if row['output_owner'] is output]
                if len(matches)!=1:raise ValueError('publication candidate progress identity')
                drain_event,row_index,row=matches[0]
                candidate.update(task_sha256=row['task_sha256'],drain_event=drain_event,drain_row=row_index)
                if not source_ok:raise ValueError('unqualified receipt codec')
                body=encode_receipt(output.receipt) if output.receipt is not None else None
                matches=[c['event'] for c in commits if c['receipt'] is not None and store.verify(c['receipt'])==body]
                candidate['commit_events']=matches
            except BaseException as error:failed(candidate['event'],'candidate_join',error)
        for drain in drains:
            for row in drain['rows']:row.pop('output_owner')
        try:self.check()
        except BaseException as error:failed(None,'source_check',error)
        self.retained={'schema':'eq142-publication-observation:1','evidence':'actual_original_B_calls',
            'events':events,'commits':commits,'drains':drains,'candidates':candidates,
            'complete':self.failure is None and sum(self.counts.values())==len(events),
            'failure_reason':self.failure,'declared_logical_observer_allowance_bytes':OBSERVER_RESERVATION,
            'retention_failures':failures,'retention_failures_truncated':truncated,
            'physical_enforcement':False,'full_sample_accepted':False}
        return self.retained

    def verify(self,record,outcome,store,*,require_success=True):
        """Keep live provenance; share pure retained-proof semantics."""
        self.check()
        if record is not self.retained:raise ValueError('publication live record identity')
        proof_bodies={}
        for key,field in (('commits','receipt'),('candidates','output')):
            for row in record[key]:
                reference=row[field]
                if reference is not None:
                    body=store.verify(reference)
                    proof_bodies[reference['sha256']]=body
        facts={'reserved_resident_bytes':outcome.reserved_resident_bytes,
            'tasks':[{'task':e.task,'output':e.output,'reason':e.reason} for e in outcome.tasks]}
        result=verify_publication(record,facts,proof_bodies,initial=self.initial,maximum=self.maximum,
            combined=self.combined,publishing=self.publisher is not None,require_success=require_success)
        self.check()
        return result


class PublicationHookSession(CausalHookSession):
    def __init__(self,*args,publication,**kwargs):
        super().__init__(*args,**kwargs);self.publication=publication

    def wrapper(self,original,operation):
        traced=super().wrapper(original,operation)
        if operation not in OPERATIONS:return traced
        @functools.wraps(original)
        def publication_observed(*args,**kwargs):
            before=None
            try:before=self.publication.before(operation,args,sys._getframe(1).f_code)
            except BaseException:
                self.publication.fail();self.collector.failure('collector_error')
            try:result=traced(*args,**kwargs)
            except BaseException as error:
                try:self.publication.after(before,error=error)
                except BaseException:
                    self.publication.fail();self.collector.failure('collector_error')
                raise
            else:
                try:self.publication.after(before,result=result)
                except BaseException:
                    self.publication.fail();self.collector.failure('collector_error')
                return result
        return publication_observed
