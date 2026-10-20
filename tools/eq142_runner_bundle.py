"""Pure normal runner/wrapper/native joins, not a complete root validator.

The caller supplies independently regenerated source/job/admission expectations
and separately verified products. Fixed namespace, source qualification, trace,
publication, count index, capacity and outer-envelope authority remain separate.
No filesystem access, native probes, calculation callbacks or study command.
"""
from eq142_graph import wire
from eq142_identity import decimal_integer,join_owned_processes
from eq142_native_closure import accounting,ACCOUNTING,validate_native_closure
from eq142_owned_runner import decode_return,validate_options
from eq142_proofs import check_body,decode_record,validate_reference

NORMAL_FIELDS=set('schema stage profile mode degree control series reference_sha256 source descriptive_job original_admission status native_job_supplied native_control_scope executor_observation_attempted executor_shutdowns future_terminal_observations executor_terminal_observation_complete full_sample_accepted delegate_admission parent_owned_process_witness actual_supervisor_outcome product_proof product_outcomes publication_observation actual_admission_verification retained_spans parent_relations owned_process_witnesses runner_control_phase_start_ns runner_control_phase_end_ns runner_control_phase_wall_ns selected_phase_wall_ns selected_phase_scope before_phase after_phase'.split())


def closed(value,fields):
    if type(value) is not dict or set(value)!=set(fields):raise ValueError('runner closed record')


def same(actual,expected):
    if wire(actual)!=wire(expected):raise ValueError('runner expected binding differs')


def phase_accounting(report,native,parent):
    """Same-parent phase interval; Job counters retain their own 100ns units."""
    start=decimal_integer(report['runner_control_phase_start_ns'],9223372036854775807)
    end=decimal_integer(report['runner_control_phase_end_ns'],9223372036854775807)
    wall=decimal_integer(report['runner_control_phase_wall_ns'],9223372036854775807,positive=True)
    if end<=start or end-start!=wall:raise ValueError('runner phase interval')
    a=decimal_integer(parent['capture_start_ns'],9223372036854775807)
    b=decimal_integer(parent['capture_end_ns'],9223372036854775807)
    if not start<=a<=b<=end:raise ValueError('runner parent witness outside phase')
    before,after=report['before_phase'],report['after_phase']
    accounting(before);accounting(after)
    suspended,final=native['suspended_accounting'],native['final_accounting']
    for key in ACCOUNTING-{'active_processes'}:
        if not suspended[key]<=before[key]<=after[key]<=final[key]:
            raise ValueError('runner Job phase accounting regression')
    if (before['active_processes']<1 or after['active_processes']<1
            or before['terminated_by_limit']!=0 or after['terminated_by_limit']!=0):
        raise ValueError('runner live phase disposition')
    return {'wall_ns':str(wall),'user_ticks_100ns':str(after['user']-before['user']),
        'kernel_ticks_100ns':str(after['kernel']-before['kernel'])}


def verify_runner_bundle(runner_body,returned_body,*,sample_id,purpose,selector,control,
        reference_sha256,source,descriptive_job,original_admission,delegate_admission,
        product_records,product_admission,witness_bodies,native,plan=None):
    """Reopen the wrapper-selected runner and bind original parent ownership.

    Expected maps are trusted caller context, not self-authenticating source or
    product certificates. This component never emits complete:true or a root.
    A has no invented observer evidence. B's raw execution/publication semantics
    and independent profiler/count-index qualification must still be checked.
    """
    closed(selector,('profile','series','mode','degree'))
    returned=decode_return(returned_body)
    validate_options(sample_id=sample_id,purpose=purpose,reference_sha256=reference_sha256,
        control=control,job_name=native['job_name'],**selector)
    if selector['profile'].startswith('family_') and (selector['mode'],selector['degree']) not in (
            ('sequential',1),('thread',2),('process',2)):
        raise ValueError('runner focused frozen matrix')
    same(returned['sample_id'],sample_id);same(returned['purpose'],purpose)
    body=check_body(runner_body,returned['runner_reference'],scope='runner',name='runner.json')
    report=decode_record(body,maximum=1048576)
    traced=control=='instrumented_B';profiled=purpose=='qualification_counts'
    fields=NORMAL_FIELDS|({'publication_admission_replay'} if traced else set())|(
        {'qualification_code_counts'} if profiled else set())
    closed(report,fields)
    for key,value in {**selector,'control':control,'reference_sha256':reference_sha256,
            'source':source,'descriptive_job':descriptive_job,'original_admission':original_admission,
            'delegate_admission':delegate_admission,'product_outcomes':product_records,
            'actual_admission_verification':product_admission}.items():same(report[key],value)
    if (report['schema']!='eq142-development-runner:2'
            or report['stage']!='qualification_only_not_study_acceptance'
            or report['status']!='development_product_and_local_trace_checks_passed'
            or report['native_job_supplied'] is not True or report['full_sample_accepted'] is not False):
        raise ValueError('runner successful development scope')
    same(report['native_control_scope'],{'parent_ownership_metadata_in_selected_phase':True,
        'original_supervisor_without_trace_hooks':not traced,'common_metadata_may_dilute_ratios':True,
        'observer_cost_subtraction':False})
    for key,name,codec in (('actual_supervisor_outcome','supervisor.json','eq142_supervisor_outcome'),
            ('product_proof','products.json','eq142_product_projection')):
        validate_reference(report[key],scope='runner',name=name)
        if report[key]['codec']!=codec:raise ValueError('runner record codec')
    if traced:
        if type(plan) is not dict or plan.get('mode')!=selector['mode']:raise ValueError('runner plan context')
        if report['executor_observation_attempted'] is not True or report['executor_terminal_observation_complete'] is not True:
            raise ValueError('runner executor observations incomplete')
        validate_reference(report['publication_observation'],scope='runner',name='publication.json')
        if report['publication_observation']['codec']!='eq142_publication_observation':raise ValueError('runner publication codec')
        if type(report['retained_spans']) is not dict:raise ValueError('runner traced records absent')
        fragments=report['retained_spans']['fragments'];processes=report['retained_spans']['processes']
        expected_names=['parent.json',*(r['file'] for r in plan['reservations'])]
        if (type(fragments) is not list or len(fragments)!=len(expected_names)
                or [f['retained_name'] for f in fragments]!=expected_names):
            raise ValueError('runner fragment witness population/order')
        expected_refs=[f['owned_process_witness'] for f in fragments]
        scope='actual_validated_trace_processes_only'
    else:
        if plan is not None:raise ValueError('runner untraced plan')
        same(report['retained_spans'],None);same(report['parent_relations'],[])
        same(report['publication_observation'],None)
        for key in ('executor_shutdowns','future_terminal_observations','executor_terminal_observation_complete'):
            same(report[key],None)
        if report['executor_observation_attempted'] is not False:raise ValueError('runner untraced observer')
        expected_names=['parent.json'];expected_refs=[report['parent_owned_process_witness']]
        processes=None;scope='runner_process_only'
    if type(witness_bodies) is not dict or set(witness_bodies)!=set(expected_names) or len(expected_names)>17:
        raise ValueError('runner selected witness bodies')
    witnesses=[]
    for name,reference in zip(expected_names,expected_refs):
        witness=decode_record(check_body(witness_bodies[name],reference,scope='owned_processes',name=name),maximum=4096)
        witnesses.append(witness)
    same(expected_refs[0],report['parent_owned_process_witness'])
    parent=witnesses[0]
    same(returned['process_binding'],{k:parent[k] for k in ('pid','process_startup_nonce')})
    if processes is None:
        processes={parent['process_startup_nonce']:{'pid':parent['pid'],'clock':parent['clock']}}
    joined=join_owned_processes(witnesses,processes,job_name=native['job_name'])
    same(report['owned_process_witnesses'],{'references':expected_refs,'processes':joined,'scope':scope,
        'cross_process_raw_ticks_qualified':False,'full_sample_accepted':False})
    native_checked=validate_native_closure(native,witnesses,parent_nonce=parent['process_startup_nonce'],
        traced_processes=processes if traced else None)
    phase=phase_accounting(report,native,parent)
    want_scope='qualification_profiled_no_timing_authority' if profiled else (
        'admitted_control' if selector['series']=='eq142_admitted_control' else 'external_owned_runner_final_exit_required')
    same(report['selected_phase_scope'],want_scope)
    same(report['selected_phase_wall_ns'],phase['wall_ns'] if want_scope=='admitted_control' else None)
    return {'report':report,'witnesses':witnesses,'native_validation':native_checked,'phase':phase,
        'scope':'normal_runner_parent_native_joins_only','full_sample_accepted':False}
