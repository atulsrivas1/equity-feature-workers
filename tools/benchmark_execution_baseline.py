"""EQ142 development sample assembly. No frozen study command is enabled yet.

Qualification runs retain original product/source/trace evidence. Their durations
are control observations, not official A/A samples or performance acceptance.
"""
from __future__ import annotations
import argparse
from contextlib import nullcontext
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tests'))
from eq142_graph import bind_source,digest,wire,describe_job
from eq142_admission import budget_for,original_reservation,delegate_reservation,confirm_original
from eq142_causal import CausalLedger,CausalHookSession
from eq142_publication import PublicationLedger,PublicationHookSession
from eq142_delegate import make_plan,patched_delegate
from eq142_evidence import (ProductStore,bounded_read,persist_record,record_products,
    stable_products,combine_fragments)
from eq142_trace import Collector
from eq142_workloads import items_for,independent_expectations,verify_result,PROFILES
from eq142_outcome import snapshot_original,verify_original
from eq142_identity import retain_owned_witness,read_owned_witnesses
from eq142_fixture import qualification,bound_inventory,load
from benchmark_supplied_inputs import publisher
from equity_feature_workers import BoundedSupervisor,ProgressRecorder

SERIES=('eq142_admitted_control','eq142_owned_runner_e2e')


def failure_identity(error,stage):
    from supplied_native import NativeFailure
    return {'stage':stage,'error_type':type(error).__module__+'.'+type(error).__qualname__,
        'native_code':error.code if isinstance(error,NativeFailure) else None}


def retain_executor_observations(report,ledger,*,require_waited):
    report['executor_observation_attempted']=True
    report['executor_shutdowns']=ledger.shutdown_snapshot()
    report['executor_terminal_observation_complete']=False
    try:terminal=ledger.terminal_snapshot()
    finally:
        report['future_terminal_observations']=ledger.retained_terminal_rows()
        report['executor_terminal_observation_complete']=ledger.terminal_complete
    if require_waited:ledger.require_waited_shutdown(terminal)


def installed_snapshot():
    """Actual installed origins/complete namespaces, including absent metadata.

    Name-based offline wheel installation may legitimately omit direct_url.json.
    Absence is recorded explicitly, never fabricated as a checked origin. Actual
    package payload/build/archive qualification is a separate mandatory gate.
    """
    from build_supplied_inputs import PACKAGES
    result={};prefix=Path(sys.prefix).resolve();total_bytes=0
    for _,(_,_,module,version) in PACKAGES.items():
        dist=importlib.metadata.distribution(module.replace('_','-'))
        location=Path(importlib.import_module(module).__file__).resolve()
        if dist.version!=version or not location.is_relative_to(prefix) or 'site-packages' not in location.parts:
            raise ValueError('unexpected installed version/origin')
        if dist.files is None or dist.read_text('RECORD') is None:raise ValueError('missing installed RECORD')
        files={}
        for declared in dist.files:
            name=str(declared).replace('\\','/')
            if '__pycache__' in name or name.endswith(('.pyc','.pyo')):continue
            path=Path(dist.locate_file(declared))
            if path.is_symlink() or not path.resolve().is_relative_to(prefix):raise ValueError('unowned installed member')
            if len(files)>=2048:raise ValueError('installed inventory bound')
            body=bounded_read(path,16777216);total_bytes+=len(body)
            if total_bytes>67108864:raise ValueError('installed payload bound')
            files[name]={'sha256':digest(body),'bytes':len(body)}
        site=Path(dist.locate_file('')).resolve();info=module+'-'+version+'.dist-info'
        for namespace in (module,info):
            actual=set()
            for path in (site/namespace).rglob('*'):
                if path.is_symlink():raise ValueError('installed symlink')
                if path.is_file() and '__pycache__' not in path.parts:
                    actual.add(path.relative_to(site).as_posix())
                    if len(actual)>2048:raise ValueError('installed namespace bound')
            if actual!={n for n in files if n.startswith(namespace+'/')}:raise ValueError('unexpected installed namespace member')
        direct=dist.read_text('direct_url.json')
        disposition='not_emitted'
        if direct is not None:
            if json.loads(direct).get('dir_info',{}).get('editable') is True:raise ValueError('editable install')
            disposition='present_noneditable'
        result[module]={'version':version,'origin':location.relative_to(prefix).as_posix(),
            'files':files,'direct_url_disposition':disposition}
    return result


def source_state():
    names={p.relative_to(ROOT).as_posix() for pattern in ('eq142_*.py','benchmark_execution_baseline.py',
        'benchmark_supplied_inputs.py','supplied_native.py','build_supplied_inputs.py') for p in (ROOT/'tools').glob(pattern)}
    names.update(p.relative_to(ROOT).as_posix() for p in (ROOT/'tests/fixtures/eq142_graph').glob('*.json'))
    names.update(('tests/eq142_fixture.py','tests/supplied_memory.py','tests/supplied_memory_delegate.py'))
    return {'files':{name:digest((ROOT/name).read_bytes()) for name in sorted(names)},
        'installed_runtime':installed_snapshot()}


def sample_budget(degree,cpu_slots):return budget_for(degree,cpu_slots)


def prepare_reference(directory,*,cpu_slots,profiles=PROFILES):
    """Independent mathematical expectations plus full original SDK witnesses."""
    directory=Path(directory);directory.mkdir(exist_ok=False)
    if not profiles or len(set(profiles))!=len(profiles) or any(p not in PROFILES for p in profiles):
        raise ValueError('unsupported qualification profiles')
    before=source_state();bound=qualification();inventory=bound_inventory(bound)
    inventory['source']['tool_sha256']={Path(n).name:h for n,h in before['files'].items() if n.startswith('tools/')}
    bind_source(inventory).check()
    reference={'schema':'eq142-development-reference:1','stage':'qualification_only_not_frozen_study',
        'state':before,'source_inventory':inventory,'cpu_slots':cpu_slots,'profiles':{}}
    store=ProductStore(directory/'objects');budget=sample_budget(1,cpu_slots)
    for profile in profiles:
        items=items_for(profile);mathematics=independent_expectations(profile,items)
        expected={'mathematics':mathematics,'products':{}}
        for series in SERIES:
            local=directory/(profile+'-'+series);local.mkdir()
            sink,pub=publisher() if series=='eq142_owned_runner_e2e' else (None,None)
            progress=ProgressRecorder(max_tasks=16)
            preflight=original_reservation(items,mode='sequential',budget=budget,pending_bytes=8388608 if pub else 0)
            outcome=BoundedSupervisor(mode='sequential',budget=budget).run(items,publisher=pub,progress=progress)
            records=record_products(outcome,progress.snapshot(),store,local,sink=sink)
            persist_record(local/'products.json',records,max_bytes=65536)
            confirm_original(outcome,preflight)
            for execution in outcome.tasks:
                if execution.reason is not None or execution.results is None or len(execution.results)!=1:
                    raise ValueError('original preflight rejected')
                verify_result(execution.task,execution.results[0],mathematics[execution.task.task_sha256])
            expected['products'][series]=stable_products(records)
        reference['profiles'][profile]=expected
    if source_state()!=before:raise ValueError('source/runtime changed during reference preparation')
    ref=persist_record(directory/'reference.json',reference,max_bytes=16777216)
    return ref


def read_reference(path,expected_sha256):
    path=Path(path);body=bounded_read(path,16777216)
    if digest(body)!=expected_sha256:raise ValueError('reference hash changed')
    reference=json.loads(body)
    if reference['schema']!='eq142-development-reference:1' or reference['stage']!='qualification_only_not_frozen_study':
        raise ValueError('unsupported reference stage')
    if source_state()!=reference['state']:raise ValueError('source/runtime changed before phase')
    bind_source(reference['source_inventory']).check()
    return reference


def _run_control(reference_path,reference_sha256,*,directory,profile,mode,degree,control,series,job_name=None,qualify_code_counts=False):
    """Assemble a real original-run control, preserving products before checks.

    This is a development record, not the accepted full execution-trace schema.
    Native owned-PID/barrier/clock joins and independent B-count qualification
    remain final sample gates. Never relabel returned records as study samples.
    """
    if control not in ('uninstrumented_A','instrumented_B') or series not in SERIES:
        raise ValueError('unsupported control/class')
    if type(qualify_code_counts) is not bool or qualify_code_counts and control!='instrumented_B':
        raise ValueError('count observer requires explicit instrumented qualification')
    reference_path=Path(reference_path);directory=Path(directory)
    reference=read_reference(reference_path,reference_sha256)
    expected=reference['profiles'][profile]
    items=items_for(profile);budget=sample_budget(degree,reference['cpu_slots'])
    bound=bind_source(reference['source_inventory'])
    job=describe_job(items,mode=mode,budget=budget,qualification=bound)
    sink,pub=publisher() if series=='eq142_owned_runner_e2e' else (None,None)
    progress=ProgressRecorder(max_tasks=16)
    original=original_reservation(items,mode=mode,budget=budget,pending_bytes=8388608 if pub else 0)
    expected_output=sum(p['output']['bytes'] for p in expected['products'][series] if p['output'] is not None)
    trace_dir=directory/'trace';trace_dir.mkdir()
    report={'schema':'eq142-development-runner:1','stage':'qualification_only_not_study_acceptance',
        'profile':profile,'mode':mode,'degree':degree,'control':control,'series':series,
        'reference_sha256':reference_sha256,'source':bound.source,'descriptive_job':job,
        'original_admission':original,'status':'started','native_job_supplied':job_name is not None,
        'native_control_scope':{'parent_ownership_metadata_in_selected_phase':job_name is not None,
            'original_supervisor_without_trace_hooks':control=='uninstrumented_A',
            'common_metadata_may_dilute_ratios':job_name is not None,'observer_cost_subtraction':False},
        'executor_observation_attempted':False,'executor_shutdowns':None,'future_terminal_observations':None,
        'executor_terminal_observation_complete':None,
        'full_sample_accepted':False}
    plan=None;collector=None;ledger=None;outcome=None;count_observer=None;profile_dir=None;publication=None
    cpu_before=None;cpu_after=None
    witness_directory=directory/'owned-processes';witness_setting=None;witness_failure=None;parent_witness=None
    phase_operation='prepare_instrumentation'
    if job_name is not None:
        from supplied_native import counters
        cpu_before=counters(job_name)
    phase_start=time.perf_counter_ns()
    try:
        if job_name is not None:
            try:
                witness_directory.mkdir()
                witness_setting={'job_name':job_name,'output_dir':str(witness_directory)}
            except BaseException as error:witness_failure=error
        if control=='instrumented_B':
            # All construction, hashes, copied plan and representation admission
            # occur after selected-phase start, not in excluded pre-admission.
            if qualify_code_counts:
                from eq142_code_counts import for_original_run
                profile_dir=directory/'code-counts';profile_dir.mkdir()
                count_observer=for_original_run(bound,reference['source_inventory']['hooks'])
            plan,_=make_plan(items,mode=mode,budget=budget,expected_inventory=reference['source_inventory'],
                output_dir=trace_dir,operations=load('RECORD_CONTRACTS.json')['trace']['operations'],
                qualification_profile_dir=profile_dir,owned_process_witness=witness_setting)
            collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=plan['operations'])
            label_frame=collector.begin('trace.bind_labels',())
            try:collector.bind_labels(tuple(item.task for item in items))
            finally:collector.finish(label_frame)
            ledger=CausalLedger(plan,items,collector)
        report['delegate_admission']=delegate_reservation(plan,items,budget,original=original,
            expected_output_bytes=expected_output,publication_observation=collector is not None,owned_witness_count=(
                1+len(plan['reservations']) if plan is not None and witness_setting is not None else
                1 if job_name is not None else 0))
        if collector is not None:
            publication=PublicationLedger(bound,publisher=pub,sink=sink,
                initial=original['initial_reserved_resident_bytes'],maximum=budget.max_resident_bytes,
                combined=report['delegate_admission']['combined_logical_resident_bytes']-expected_output)
        phase_operation='capture_parent_witness'
        if job_name is not None and witness_failure is None:
            try:parent_witness=retain_owned_witness(witness_directory,'parent.json',job_name)
            except BaseException as error:witness_failure=error
        if collector is not None:
            collector.owned_process_witness=parent_witness
            if witness_failure is not None:collector.failure('collector_error')
        report['parent_owned_process_witness']=parent_witness
        phase_operation='original_supervisor'
        session=None
        with collector.phase('stage:supervisor') if collector is not None else nullcontext():
            try:
                if collector is not None:
                    session=PublicationHookSession(bound,reference['source_inventory']['hooks'],collector,
                        ledger=ledger,publication=publication)
                    frame=collector.begin('trace.install',())
                    try:session.install()
                    finally:collector.finish(frame)
                count_context=nullcontext()
                if count_observer is not None:
                    from eq142_code_counts import guarded_activation
                    count_context=guarded_activation(count_observer,lambda:collector.failure('collector_error'))
                with (patched_delegate(plan,collector) if collector is not None else nullcontext(),count_context):
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items,publisher=pub,progress=progress)
            finally:
                if session is not None:
                    frame=collector.begin('trace.restore',())
                    try:session.restore()
                    finally:collector.finish(frame)
        # Retain actual metadata first, then full codec proofs before gates.
        phase_operation='retain_supervisor_metadata'
        actual=snapshot_original(outcome)
        report['actual_supervisor_outcome']={'codec':'eq142_supervisor_outcome',
            **persist_record(directory/'supervisor.json',actual,max_bytes=65536)}
        store=ProductStore(reference_path.parent/'objects')
        phase_operation='retain_products'
        products=record_products(outcome,progress.snapshot(),store,directory,sink=sink)
        report['product_proof']={'codec':'eq142_product_projection',**persist_record(directory/'products.json',products,max_bytes=65536)}
        report['product_outcomes']=products
        report['publication_observation']=None
        if publication is not None:
            phase_operation='publication_observation'
            observed=publication.snapshot(store)
            report['publication_observation']={'codec':'eq142_publication_observation',
                **persist_record(directory/'publication.json',observed,max_bytes=65536)}
            report['publication_admission_replay']=publication.verify(observed,outcome,store)
        if witness_failure is not None:
            phase_operation='deferred_parent_witness_failure'
            raise witness_failure
        phase_operation='verify_products'
        report['actual_admission_verification']=verify_original(actual,items,products,original,store,expected_mode=mode)
        if ledger is not None:
            phase_operation='executor_observations'
            retain_executor_observations(report,ledger,require_waited=True)
        if actual['cancelled']:raise ValueError('unexpected original study cancellation')
        if collector is not None:
            phase_operation='trace_and_witness_joins'
            collector.persist(trace_dir/'parent.json')
            reservations=[{'id':'parent','file':'parent.json','max_spans':16384,'max_bytes':8388608},*plan['reservations']]
            report['retained_spans']=combine_fragments(trace_dir,reservations)
            report['parent_relations']=ledger.snapshot((trace_dir,report['retained_spans']))
            if count_observer is not None:
                from eq142_code_counts import compare_aggregate,validate_profile
                persist_record(profile_dir/'parent.json',count_observer.snapshot(),max_bytes=65536)
                expected_files=['parent.json']+([r['file'] for r in plan['reservations']] if mode=='process' else [])
                if {p.name for p in profile_dir.iterdir()}!=set(expected_files):raise ValueError('incomplete profile files')
                profiles=[];references=[]
                for name in expected_files:
                    body=bounded_read(profile_dir/name,65536);value=json.loads(body);validate_profile(value);profiles.append(value)
                    references.append({'codec':'eq142_code_counts','sha256':digest(body),'bytes':len(body),'retained_name':name})
                comparison=compare_aggregate(profiles,report['retained_spans']['counters'],
                    manual_operations=('trace.bind_labels','trace.install','trace.restore'))
                manual_expected={'trace.bind_labels':1+len(plan['reservations']),
                    'trace.install':1+(len(plan['reservations']) if mode=='process' else 0),
                    'trace.restore':1+(len(plan['reservations']) if mode=='process' else 0)}
                for operation,count in manual_expected.items():
                    rows=[r for r in report['retained_spans']['counters'] if r['operation']==operation]
                    if (sum(r['attempts'] for r in rows)!=count or sum(r['returned'] for r in rows)!=count
                            or any(r['rejected'] or r['byte_observation']!='unavailable' for r in rows)):
                        raise ValueError('manual trace source expectation differs')
                report['qualification_code_counts']={'profiles':references,'comparison':comparison,
                    'manual_source_expectations':manual_expected,'timing_authority':False}
        else:
            report['retained_spans']=None;report['parent_relations']=[]
        report['owned_process_witnesses']=None
        if job_name is not None:
            phase_operation='join_owned_witnesses'
            references=[parent_witness]
            processes=None
            if collector is not None:
                processes=report['retained_spans']['processes']
                for fragment in report['retained_spans']['fragments']:
                    witness=fragment['owned_process_witness']
                    if fragment['reservation_id']=='parent':
                        if witness!=parent_witness:raise ValueError('parent witness reference differs')
                    else:
                        if witness is None or witness['retained_name']!=fragment['retained_name']:
                            raise ValueError('delegate witness reference differs')
                        references.append(witness)
                if {ref['retained_name'] for ref in references}!={'parent.json',*(r['file'] for r in plan['reservations'])}:
                    raise ValueError('owned invocation proof population differs')
            report['owned_process_witnesses']=read_owned_witnesses(witness_directory,references,
                job_name=job_name,processes=processes)
        phase_operation='parity_and_math'
        confirm_original(outcome,original)
        if stable_products(products)!=expected['products'][series]:raise ValueError('full original product parity failed')
        for execution in outcome.tasks:
            if execution.reason is not None or execution.results is None or len(execution.results)!=1:
                raise ValueError('original task rejected')
            verify_result(execution.task,execution.results[0],expected['mathematics'][execution.task.task_sha256])
        if pub is not None and pub.pending:raise ValueError('original publication pending')
        phase_finish=time.perf_counter_ns()
        if job_name is not None:cpu_after=counters(job_name)
        report.update(runner_control_phase_start_ns=str(phase_start),runner_control_phase_end_ns=str(phase_finish),
            runner_control_phase_wall_ns=str(phase_finish-phase_start),
            selected_phase_wall_ns=str(phase_finish-phase_start) if series=='eq142_admitted_control' and not qualify_code_counts else None,
            selected_phase_scope='qualification_profiled_no_timing_authority' if qualify_code_counts else
                'admitted_control' if series=='eq142_admitted_control' else 'external_owned_runner_final_exit_required',
            before_phase=cpu_before,after_phase=cpu_after)
        phase_operation='source_closure'
        bound.check()
        if source_state()!=reference['state']:raise ValueError('source/runtime changed after phase')
        report['status']='development_product_and_local_trace_checks_passed'
        report['full_sample_accepted']=False
    except BaseException as error:
        report['status']='failed_development_control';report['failure_type']=type(error).__module__+'.'+type(error).__qualname__
        report['original_outcome_returned']=outcome is not None
        if publication is not None and report.get('publication_observation') is None:
            try:
                available=publication.snapshot(ProductStore(reference_path.parent/'objects'))
                report['publication_observation']={'codec':'eq142_publication_observation',
                    **persist_record(directory/'publication.json',available,max_bytes=65536)}
            except BaseException as observation_error:
                report['publication_observation_failure_identity']=failure_identity(observation_error,'retain_publication_on_failed_control')
        if ledger is not None and not report['executor_observation_attempted']:
            try:retain_executor_observations(report,ledger,require_waited=False)
            except BaseException as observation_error:
                report['executor_observation_failure_identity']=failure_identity(observation_error,'executor_observations_on_failed_control')
        report['failure_identity']=failure_identity(error,phase_operation)
        if witness_failure is not None:
            report['owned_witness_failure_identity']=failure_identity(witness_failure,'parent_owned_witness_capture_or_write')
            if error is not witness_failure:
                error.add_note('An earlier independent owned witness capture/write also failed; both typed identities are retained in runner.json')
        if collector is not None:report['local_trace_failure']=collector.failure_reason
        persist_record(directory/'runner.json',report,max_bytes=1048576)
        raise
    return {'codec':'eq142_development_runner',**persist_record(directory/'runner.json',report,max_bytes=1048576)}


def run_control(reference_path,reference_sha256,*,directory,**options):
    directory=Path(directory);directory.mkdir(exist_ok=False)
    try:return _run_control(reference_path,reference_sha256,directory=directory,**options)
    except BaseException as error:
        if not (directory/'runner.json').exists() and not (directory/'runner.json.partial').exists():
            persist_record(directory/'runner.json',{'schema':'eq142-development-runner:1',
                'stage':'qualification_only_not_study_acceptance','status':'failed_before_phase',
                'failure_type':type(error).__module__+'.'+type(error).__qualname__,
                'full_sample_accepted':False},max_bytes=1048576)
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('prepare-qualification','qualification-runner'))
    parser.add_argument('--directory',required=True,type=Path)
    parser.add_argument('--reference',type=Path);parser.add_argument('--reference-sha256')
    parser.add_argument('--cpu-slots',type=int);parser.add_argument('--profile',choices=PROFILES)
    parser.add_argument('--mode',choices=('sequential','thread','process'),default='sequential')
    parser.add_argument('--degree',type=int,default=1)
    parser.add_argument('--control',choices=('uninstrumented_A','instrumented_B'),default='uninstrumented_A')
    parser.add_argument('--series',choices=SERIES,default=SERIES[0]);parser.add_argument('--job')
    parser.add_argument('--qualify-code-counts',action='store_true')
    args=parser.parse_args()
    if args.command=='prepare-qualification':
        if args.cpu_slots is None:parser.error('--cpu-slots required for explicit qualification configuration')
        result=prepare_reference(args.directory,cpu_slots=args.cpu_slots,
            profiles=(args.profile,) if args.profile else PROFILES)
    else:
        if args.reference is None or args.reference_sha256 is None or args.profile is None:
            parser.error('--reference/--reference-sha256/--profile required')
        result=run_control(args.reference,args.reference_sha256,directory=args.directory,profile=args.profile,
            mode=args.mode,degree=args.degree,control=args.control,series=args.series,job_name=args.job,
            qualify_code_counts=args.qualify_code_counts)
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':main()
