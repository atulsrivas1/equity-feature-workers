"""Pure retained execution/source invariants for successful original B controls.

The controller supplies its independently reconstructed original plan, selected
headers and mathematics. This layer does not establish their source provenance,
native membership, the prior qualification index or complete-root acceptance.
Only parent-local intervals are ordered. No child/parent tick subtraction occurs.
"""
from bisect import bisect_left
import re
from eq142_graph import wire
from eq142_identity import decimal_integer
from eq142_retained_trace import combine_retained_fragments,decode_fragment

RELATION_FIELDS=frozenset(('id','kind','parent_timeline_id','invocation_id','task_hashes',
    'parent_start_ns','parent_end_ns','child_timeline_ids','cross_process_interval_subtraction','status'))
SHUTDOWN_FIELDS=frozenset(('operation','executor_id','wait','cancel_futures','parent_timeline_id',
    'parent_start_ns','parent_end_ns','status','error_type'))
TERMINAL_FIELDS=frozenset(('invocation_id','task_hashes','parent_timeline_id','capture_start_ns',
    'capture_end_ns','done','cancelled','running'))
TASK_OPERATIONS=frozenset(('supervisor.validate_results','supplied.admit','supplied.result_identity',
    'worker.intraday_context','intraday.prepare','intraday.prepare_operand','intraday.compute',
    'intraday.indicator','intraday.band_values','publisher.inner_submit','publisher.inner_drain',
    'sdk.publish','sink.commit'))
FEATURES=('intraday.atr','intraday.band_lower','intraday.band_middle','intraday.band_position',
    'intraday.band_upper','intraday.band_width_ratio','intraday.ema','intraday.rsi')
SELECTIONS=(FEATURES,('intraday.ema','intraday.rsi'),('intraday.atr',),FEATURES[1:6])
PUBLICATION_OPERATIONS=frozenset(('publisher.inner_submit','publisher.inner_drain','sdk.publish','sink.commit'))


def closed(record,fields):
    if (type(record) is not dict or any(type(k) is not str for k in record)
            or set(record)!=fields):raise ValueError('execution closed record')


def same(actual,expected):
    # Canonical bytes distinguish bool/int and integer/float unlike dict equality.
    if wire(actual)!=wire(expected):raise ValueError('execution retained replay differs')


def ns(value):return decimal_integer(value,9223372036854775807)


def interval(record,start,end,*,lower,upper):
    a,b=ns(record[start]),ns(record[end])
    if not lower<=a<=b<=upper:raise ValueError('execution parent-local interval')
    return a,b


def fixed_counts(plan,task_features,mathematics,*,publishing):
    """Source-derived entries; other operations need separate code qualification.

    Readiness controls actual arithmetic calls: an unavailable anchored operand
    does not call its indicator, while independently available bands still run.
    These formulas describe only the six frozen supplied-input profiles.
    """
    if type(task_features) is not dict or not 1<=len(task_features)<=16 or type(mathematics) is not dict:
        raise ValueError('execution trusted task context')
    if set(task_features)!=set(mathematics) or type(publishing) is not bool:raise ValueError('execution math membership')
    operands=indicators=bands=0
    for task,features in task_features.items():
        if (type(task) is not str or re.fullmatch('[0-9a-f]{64}',task) is None
                or type(features) is not tuple or not 1<=len(features)<=8
                or any(type(feature) is not str for feature in features) or features not in SELECTIONS):
            raise ValueError('execution trusted headers')
        expected=mathematics[task]
        if (type(expected) is not dict or type(expected.get('selected_headers')) is not list
                or list(features)!=expected['selected_headers'] or type(expected.get('cells')) is not dict
                or set(expected['cells'])!=set(features)):raise ValueError('execution math headers')
        requirements=set();has_available_band=False
        for feature in features:
            if type(feature) is not str or feature not in (
                    'intraday.ema','intraday.rsi','intraday.atr','intraday.band_lower',
                    'intraday.band_middle','intraday.band_upper','intraday.band_position','intraday.band_width_ratio'):
                raise ValueError('execution frozen feature')
            band=feature.startswith('intraday.band_')
            requirements.add('price_moments' if band else 'true_ranges' if feature=='intraday.atr' else 'anchored_closes')
            cell=expected['cells'][feature]
            if (type(cell) is not dict or type(cell.get('status')) is not str
                    or cell['status'] not in ('available','missing_input','incomplete_coverage')):
                raise ValueError('execution frozen readiness')
            available=cell['status']=='available'
            if band:has_available_band|=available
            else:indicators+=int(available)
        operands+=len(requirements);bands+=int(has_available_band)
    count=len(task_features);delegates=len(plan['reservations']);pooled=plan['mode']!='sequential'
    result={'trace.bind_labels':1+delegates,'trace.install':1+(delegates if plan['mode']=='process' else 0),
        'trace.restore':1+(delegates if plan['mode']=='process' else 0),'delegate.original_partition':delegates,
        'supervisor.inputs':0,'supervisor.partition':1,'supervisor.validate_results':2*count,
        'supplied.admit':count,'supplied.result_identity':count,'worker.intraday_context':count,
        'intraday.prepare':count,'intraday.prepare_operand':operands,'intraday.compute':count,
        'intraday.indicator':indicators,'intraday.band_values':bands,'future.result':delegates if pooled else 0,
        'publisher.submit':0,'publisher.drain':0,'publisher.inner_submit':count if publishing else 0,
        'publisher.inner_drain':count if publishing else 0,'sdk.publish':count if publishing else 0,
        'sink.commit':count if publishing else 0}
    for mode in ('thread','process'):
        result['pool.'+mode+'_submit']=delegates if plan['mode']==mode else 0
        result['pool.'+mode+'_shutdown']=int(plan['mode']==mode)
    return result


def ancestor(span,required,byid):
    """Input nesting was already closed by validate_fragment's bounded chain."""
    cursor=span['parent_id']
    while cursor is not None:
        if cursor==required['id']:return True
        cursor=byid[cursor]['parent_id']
    return False


def verify_roles(fragments,plan,task_features,mathematics,*,publishing,fixed):
    """Global/per-task totals cannot substitute for actual fragment ownership."""
    by_task={task:fixed_counts(plan,{task:features},{task:mathematics[task]},publishing=publishing)
        for task,features in task_features.items()}
    reservations={r['id']:r for r in plan['reservations']}
    for name,fragment in fragments.items():
        parent=name=='parent'
        tasks=list(task_features) if parent else reservations[name]['task_hashes']
        expected=dict(fixed) if parent else {operation:0 for operation in fixed}
        expected.update({'trace.bind_labels':1,'trace.install':int(parent or plan['mode']=='process'),
            'trace.restore':int(parent or plan['mode']=='process'),'delegate.original_partition':int(not parent)})
        per_task={}
        for task in tasks:
            for operation in TASK_OPERATIONS:
                count=(1 if operation=='supervisor.validate_results' else
                    int(publishing) if operation in PUBLICATION_OPERATIONS and parent else
                    0 if operation in PUBLICATION_OPERATIONS or parent else by_task[task][operation])
                per_task[(operation,task)]=count
        for operation in TASK_OPERATIONS:expected[operation]=sum(per_task[(operation,task)] for task in tasks)
        counts={};actual_task={};byid={s['id']:s for s in fragment['spans']};anchors_by_task={}
        for span in fragment['spans']:
            anchors_by_task.setdefault((span['operation'],span['task_sha256']),[]).append(span)
        original=[s for s in fragment['spans'] if s['operation']=='delegate.original_partition']
        for span in fragment['spans']:
            operation=span['operation'];counts[operation]=counts.get(operation,0)+1
            if operation in TASK_OPERATIONS:
                key=(operation,span['task_sha256']);actual_task[key]=actual_task.get(key,0)+1
                if not parent:
                    if len(original)!=1 or not ancestor(span,original[0],byid):raise ValueError('execution delegate call ancestry')
                    required='intraday.prepare' if operation=='intraday.prepare_operand' else 'intraday.compute' if operation in ('intraday.indicator','intraday.band_values') else None
                    if required is not None:
                        anchors=anchors_by_task.get((required,span['task_sha256']),[])
                        if len(anchors)!=1 or not ancestor(span,anchors[0],byid):raise ValueError('execution operand/header ancestry')
        for operation,count in expected.items():
            if counts.get(operation,0)!=count:raise ValueError('execution fragment role count: '+operation)
        if any(actual_task.get(key,0)!=count for key,count in per_task.items()) or set(actual_task)-set(per_task):
            raise ValueError('execution fragment task role')


def verify_parent_barriers(fragments,plan,task_features,*,publishing,lower,upper,owner=None):
    """Original final validation/publication is serial on one parent timeline."""
    parent=fragments['parent'];spans=parent['spans'];byid={s['id']:s for s in spans}
    validations=sorted((s for s in spans if s['operation']=='supervisor.validate_results'),key=lambda s:ns(s['start_ns']))
    if [s['task_sha256'] for s in validations]!=sorted(task_features):raise ValueError('execution coordinator validation order')
    owner=validations[0]['timeline_id'] if owner is None else owner
    if plan['mode']=='sequential':
        for name,fragment in fragments.items():
            if name=='parent':continue
            same(fragment['process_binding'],parent['process_binding']);same(fragment['clock'],parent['clock'])
            if [t['id'] for t in fragment['timeline_bindings']]!=[owner]:raise ValueError('execution sequential thread differs')
            for span in fragment['spans']:
                interval(span,'start_ns','end_ns',lower=lower,upper=upper)
            lower=max(lower,max(ns(s['end_ns']) for s in fragment['spans']))
    selected={(s['operation'],s['task_sha256']):s for s in spans if s['operation'] in TASK_OPERATIONS}
    previous=lower
    for validation in validations:
        task=validation['task_sha256']
        if validation['timeline_id']!=owner:raise ValueError('execution coordinator timeline differs')
        _,previous=interval(validation,'start_ns','end_ns',lower=previous,upper=upper)
        if publishing:
            submit=selected[('publisher.inner_submit',task)];drain=selected[('publisher.inner_drain',task)]
            publish=selected[('sdk.publish',task)];commit=selected[('sink.commit',task)]
            if any(s['timeline_id']!=owner for s in (submit,drain,publish,commit)):raise ValueError('execution publication owner differs')
            _,submitted=interval(submit,'start_ns','end_ns',lower=previous,upper=upper)
            a,previous=interval(drain,'start_ns','end_ns',lower=submitted,upper=upper)
            published_start,published_end=interval(publish,'start_ns','end_ns',lower=a,upper=previous)
            interval(commit,'start_ns','end_ns',lower=published_start,upper=published_end)
            if not ancestor(publish,drain,byid) or not ancestor(commit,publish,byid):raise ValueError('execution publication nesting')
    restore=next(s for s in spans if s['operation']=='trace.restore')
    if restore['timeline_id']!=owner or ns(restore['start_ns'])<previous:raise ValueError('execution restoration before coordinator completion')
    return owner


def verify_execution(report,plan,bodies,*,task_features,mathematics,publishing,maximum_in_flight):
    """Recompute trace closure and match actual parent relations to raw hook spans.

    A returned wait's completed-Future set was not separately captured. The
    original source controls that decision; retained relations prove its actual
    pending population and successful result joins, not invented return members.
    """
    if (type(plan) is not dict or type(plan.get('mode')) is not str or plan['mode'] not in ('sequential','thread','process')
            or type(report) is not dict or type(maximum_in_flight) is not int
            or maximum_in_flight not in (1,2,4,8) or plan['mode']=='sequential' and maximum_in_flight!=1):
        raise ValueError('execution context')
    reservations=[{'id':'parent','file':'parent.json','max_spans':16384,'max_bytes':8388608},*plan['reservations']]
    joined=combine_retained_fragments(bodies,reservations)
    same(report['retained_spans'],joined)
    fragments={r['id']:decode_fragment(bodies[r['file']]) for r in reservations}
    expected={r['id']:r for r in plan['reservations']}
    expected_tasks=[task for r in expected.values() for task in r['task_hashes']]
    if len(set(expected_tasks))!=len(expected_tasks) or set(expected_tasks)!=set(task_features):
        raise ValueError('execution plan task membership')
    if report['executor_observation_attempted'] is not True or report['executor_terminal_observation_complete'] is not True:
        raise ValueError('execution manual observation incomplete')
    start,end=ns(report['runner_control_phase_start_ns']),ns(report['runner_control_phase_end_ns'])
    if end<=start:raise ValueError('execution parent phase')
    parent=fragments['parent'];timelines={t['id'] for t in parent['timeline_bindings']}
    counters={};task_counters={};by_operation={}
    for name,fragment in fragments.items():
        membership=set(task_features) if name=='parent' else set(expected[name]['task_hashes'])
        for span in fragment['spans']:
            if span['status']!='returned':raise ValueError('execution original hook rejection')
            task=span['task_sha256']
            if task is not None:
                if task not in membership or span['scope_id']!='task:'+task:raise ValueError('execution foreign task label')
            elif span['scope_id'] not in (('stage:unbound','stage:supervisor') if name=='parent' else ('stage:delegate',)):
                raise ValueError('execution foreign stage label')
            counters[span['operation']]=counters.get(span['operation'],0)+1
            if span['operation'] in TASK_OPERATIONS:
                if task is None:raise ValueError('execution missing task label')
                key=(span['operation'],task);task_counters[key]=task_counters.get(key,0)+1
            if name=='parent':
                interval(span,'start_ns','end_ns',lower=start,upper=end)
                by_operation.setdefault((span['operation'],span['timeline_id']),[]).append(span)
        if name!='parent' and sum(s['operation']=='delegate.original_partition' for s in fragment['spans'])!=1:
            raise ValueError('execution delegate original call population')
    fixed=fixed_counts(plan,task_features,mathematics,publishing=publishing)
    for operation,count in fixed.items():
        if counters.get(operation,0)!=count:raise ValueError('execution fixed source count: '+operation)
    for task,features in task_features.items():
        per_task=fixed_counts(plan,{task:features},{task:mathematics[task]},publishing=publishing)
        for operation in TASK_OPERATIONS:
            if task_counters.get((operation,task),0)!=per_task[operation]:
                raise ValueError('execution task/header source count: '+operation)
    verify_roles(fragments,plan,task_features,mathematics,publishing=publishing,fixed=fixed)
    for key,values in by_operation.items():values.sort(key=lambda s:ns(s['start_ns']))
    starts={key:[ns(s['start_ns']) for s in values] for key,values in by_operation.items()}
    used_spans=set()
    def hook(operation,timeline,a,b):
        key=(operation,timeline);values=by_operation.get(key,[])
        index=bisect_left(starts.get(key,[]),a)
        if index>=len(values):raise ValueError('execution missing causal hook')
        span=values[index]
        if not a<=ns(span['start_ns'])<=ns(span['end_ns'])<=b:raise ValueError('execution causal hook interval')
        if (index+1<len(values) and ns(values[index+1]['start_ns'])<=b
                and ns(values[index+1]['end_ns'])<=b):
            raise ValueError('execution ambiguous causal hook')
        if span['id'] in used_spans:raise ValueError('execution repeated causal hook')
        used_spans.add(span['id']);return span
    relations=report['parent_relations'];shutdowns=report['executor_shutdowns'];terminal=report['future_terminal_observations']
    if type(relations) is not list or len(relations)>4096 or type(shutdowns) is not list or type(terminal) is not list:
        raise ValueError('execution relation population')
    if plan['mode']=='sequential':
        if relations or shutdowns or terminal or counters.get('parent.wait',0):raise ValueError('execution sequential has executor')
        verify_parent_barriers(fragments,plan,task_features,publishing=publishing,lower=start,upper=end)
        return {'fixed_source_counts':fixed,'actual_wait_groups':0,'scope':'local_trace_and_source_invariants',
            'full_sample_accepted':False}
    if len(shutdowns)!=1 or len(terminal)!=len(expected):raise ValueError('execution executor population')
    groups=[];seen_groups=set()
    for index,row in enumerate(relations):
        closed(row,RELATION_FIELDS)
        if (row['id']!='causal:%04d'%index or type(row['kind']) is not str
                or row['kind'] not in ('dispatch','result_join','parent_wait')
                or type(row['parent_timeline_id']) is not str or row['parent_timeline_id'] not in timelines
                or type(row['invocation_id']) is not str or row['invocation_id'] not in expected
                or row['status']!='returned' or row['cross_process_interval_subtraction'] is not False):
            raise ValueError('execution causal relation')
        invocation=row['invocation_id'];reservation=expected[invocation]
        same(row['task_hashes'],reservation['task_hashes'])
        same(row['child_timeline_ids'],[t['id'] for t in fragments[invocation]['timeline_bindings']])
        a,b=interval(row,'parent_start_ns','parent_end_ns',lower=start,upper=end)
        key=(row['kind'],row['parent_timeline_id'],a,b)
        if not groups or key!=groups[-1][0]:
            if key in seen_groups:raise ValueError('execution noncontiguous causal group')
            seen_groups.add(key);groups.append((key,[]))
        members=groups[-1][1]
        if invocation in members:raise ValueError('execution duplicate causal member')
        members.append(invocation)
    pending=set();dispatched=set();completed=set();waited=set();wait_groups=0;owner=None;previous_end=start
    for (kind,timeline,a,b),members in groups:
        if a<previous_end or owner is not None and owner!=timeline:raise ValueError('execution parent causal order/owner')
        owner=timeline;previous_end=b
        operation={'dispatch':'pool.'+plan['mode']+'_submit','result_join':'future.result','parent_wait':'parent.wait'}[kind]
        hook(operation,timeline,a,b)
        if kind=='dispatch':
            if len(members)!=1 or members[0] in dispatched:raise ValueError('execution duplicate dispatch')
            dispatched.add(members[0]);pending.add(members[0])
            if len(pending)>maximum_in_flight:raise ValueError('execution in-flight bound')
        elif kind=='parent_wait':
            if not pending or set(members)!=pending:raise ValueError('execution wait pending population')
            waited.update(members);wait_groups+=1
        else:
            if len(members)!=1 or members[0] not in pending or members[0] not in waited:
                raise ValueError('execution result without dispatch/wait')
            pending.remove(members[0]);completed.add(members[0])
    if pending or dispatched!=set(expected) or completed!=set(expected) or wait_groups!=counters.get('parent.wait',0):
        raise ValueError('execution incomplete causal coverage')
    shutdown=shutdowns[0];closed(shutdown,SHUTDOWN_FIELDS)
    if (shutdown['operation']!='pool.'+plan['mode']+'_shutdown' or shutdown['executor_id']!='owned-executor:0'
            or shutdown['wait'] is not True or shutdown['cancel_futures'] is not True
            or shutdown['parent_timeline_id']!=owner or shutdown['status']!='returned' or shutdown['error_type'] is not None):
        raise ValueError('execution waited shutdown')
    a,b=interval(shutdown,'parent_start_ns','parent_end_ns',lower=previous_end,upper=end)
    hook(shutdown['operation'],owner,a,b)
    verify_parent_barriers(fragments,plan,task_features,publishing=publishing,lower=b,upper=end,owner=owner)
    causal_operations={'pool.thread_submit','pool.process_submit','pool.thread_shutdown','pool.process_shutdown','future.result','parent.wait'}
    if used_spans!={s['id'] for values in by_operation.values() for s in values if s['operation'] in causal_operations}:
        raise ValueError('execution uncovered causal hook')
    restoration=[ns(s['end_ns']) for values in by_operation.values() for s in values if s['operation']=='trace.restore']
    lower=max([b,*restoration])
    if [r.get('invocation_id') if type(r) is dict else None for r in terminal]!=sorted(expected):
        raise ValueError('execution terminal order/population')
    for row in terminal:
        closed(row,TERMINAL_FIELDS)
        if (row['parent_timeline_id']!=owner or row['done'] is not True
                or row['cancelled'] is not False or row['running'] is not False):raise ValueError('execution terminal state')
        same(row['task_hashes'],expected[row['invocation_id']]['task_hashes'])
        _,lower=interval(row,'capture_start_ns','capture_end_ns',lower=lower,upper=end)
    return {'fixed_source_counts':fixed,'actual_wait_groups':wait_groups,
        'scope':'local_trace_and_source_invariants','wait_completed_membership':'original_source_required_not_separately_captured',
        'full_sample_accepted':False}
