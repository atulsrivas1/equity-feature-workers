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
from eq142_delegate import make_plan,patched_delegate
from eq142_evidence import (ProductStore,bounded_read,persist_record,record_products,
    stable_products,combine_fragments)
from eq142_trace import Collector
from eq142_workloads import items_for,independent_expectations,verify_result,PROFILES
from eq142_fixture import qualification,bound_inventory,load
from benchmark_supplied_inputs import publisher
from equity_feature_workers import BoundedSupervisor,ProgressRecorder

SERIES=('eq142_admitted_control','eq142_owned_runner_e2e')


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


def _run_control(reference_path,reference_sha256,*,directory,profile,mode,degree,control,series,job_name=None):
    """Assemble a real original-run control, preserving products before checks.

    This is a development record, not the accepted full execution-trace schema.
    Native owned-PID/barrier/clock joins and independent B-count qualification
    remain final sample gates. Never relabel returned records as study samples.
    """
    if control not in ('uninstrumented_A','instrumented_B') or series not in SERIES:
        raise ValueError('unsupported control/class')
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
        'original_admission':original,'status':'started','native_job_supplied':job_name is not None}
    plan=None;collector=None;ledger=None;outcome=None
    cpu_before=None;cpu_after=None
    if job_name is not None:
        from supplied_native import counters
        cpu_before=counters(job_name)
    phase_start=time.perf_counter_ns()
    try:
        if control=='instrumented_B':
            # All construction, hashes, copied plan and representation admission
            # occur after selected-phase start, not in excluded pre-admission.
            plan,_=make_plan(items,mode=mode,budget=budget,expected_inventory=reference['source_inventory'],
                output_dir=trace_dir,operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
            collector=Collector('parent',max_spans=16384,max_bytes=8388608,operations=plan['operations'])
            label_frame=collector.begin('trace.bind_labels',())
            try:collector.bind_labels(tuple(item.task for item in items))
            finally:collector.finish(label_frame)
            ledger=CausalLedger(plan,items,collector)
        report['delegate_admission']=delegate_reservation(plan,items,budget,original=original,
            expected_output_bytes=expected_output)
        session=None
        with collector.phase('stage:supervisor') if collector is not None else nullcontext():
            try:
                if collector is not None:
                    session=CausalHookSession(bound,reference['source_inventory']['hooks'],collector,ledger=ledger)
                    frame=collector.begin('trace.install',())
                    try:session.install()
                    finally:collector.finish(frame)
                with patched_delegate(plan,collector) if collector is not None else nullcontext():
                    outcome=BoundedSupervisor(mode=mode,budget=budget).run(items,publisher=pub,progress=progress)
            finally:
                if session is not None:
                    frame=collector.begin('trace.restore',())
                    try:session.restore()
                    finally:collector.finish(frame)
        # Persist actual full original outcomes before parity/source/trace gates.
        products=record_products(outcome,progress.snapshot(),ProductStore(reference_path.parent/'objects'),directory,sink=sink)
        report['product_proof']={'codec':'eq142_product_projection',**persist_record(directory/'products.json',products,max_bytes=65536)}
        report['product_outcomes']=products
        if collector is not None:
            collector.persist(trace_dir/'parent.json')
            reservations=[{'id':'parent','file':'parent.json','max_spans':16384,'max_bytes':8388608},*plan['reservations']]
            report['retained_spans']=combine_fragments(trace_dir,reservations)
            report['parent_relations']=ledger.snapshot((trace_dir,report['retained_spans']))
        else:
            report['retained_spans']=None;report['parent_relations']=[]
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
            selected_phase_wall_ns=str(phase_finish-phase_start) if series=='eq142_admitted_control' else None,
            selected_phase_scope='admitted_control' if series=='eq142_admitted_control' else 'external_owned_runner_final_exit_required',
            before_phase=cpu_before,after_phase=cpu_after)
        bound.check()
        if source_state()!=reference['state']:raise ValueError('source/runtime changed after phase')
        report['status']='development_product_and_local_trace_checks_passed'
        report['full_sample_accepted']=False
    except BaseException as error:
        report['status']='failed_development_control';report['failure_type']=type(error).__module__+'.'+type(error).__qualname__
        report['original_outcome_returned']=outcome is not None
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
    args=parser.parse_args()
    if args.command=='prepare-qualification':
        if args.cpu_slots is None:parser.error('--cpu-slots required for explicit qualification configuration')
        result=prepare_reference(args.directory,cpu_slots=args.cpu_slots,
            profiles=(args.profile,) if args.profile else PROFILES)
    else:
        if args.reference is None or args.reference_sha256 is None or args.profile is None:
            parser.error('--reference/--reference-sha256/--profile required')
        result=run_control(args.reference,args.reference_sha256,directory=args.directory,profile=args.profile,
            mode=args.mode,degree=args.degree,control=args.control,series=args.series,job_name=args.job)
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':main()
