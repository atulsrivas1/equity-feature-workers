"""Prospective development-run admission for original supplied supervisor work.

Encoded logical reservations are not physical allocation or native IPC sizes.
The actual original SupervisorOutcome must independently confirm admission.
"""
from dataclasses import asdict
from functools import partial
import pickle

from equity_feature_io_contracts import SinkErrorCode
from equity_feature_workers import ResourceBudget,encode_task
from equity_feature_workers.commands import CommandErrorCode
from equity_feature_workers.diagnostics import DiagnosticStage,StageTiming
from equity_feature_workers.supervisor import partition_tasks,TaskExecution
from eq142_delegate import partition_entry,_ORIGINAL_PARTITION

TRACE_RESERVATION=67108864
JOB_RSS_CAP=1073741824
PRODUCT_CAP=JOB_RSS_CAP-TRACE_RESERVATION


def size(value):return len(pickle.dumps(value,protocol=5))


def budget_for(degree,cpu_slots):
    if type(degree) is not int or degree not in (1,2,4,8):raise ValueError('unsupported frozen degree')
    # Product reserves degree compute slots plus its original backend slot;
    # the additional two declared host/coordinator slots stay outside that cap.
    if type(cpu_slots) is not int or degree+3>cpu_slots:raise ValueError('missing backend/host CPU reserve')
    return ResourceBudget(max_tasks=16,workers=degree,max_cpu_slots=cpu_slots-2,
        max_in_flight=degree,max_input_bytes=16777216,max_resident_bytes=PRODUCT_CAP,
        max_task_result_bytes=262144,max_task_transport_bytes=1048576,max_total_result_bytes=16777216)


def original_reservation(items,*,mode,budget,pending_bytes=0,diagnostics=True):
    """Exact source-derived formula, compared later with original actual outcome.

    Pins/installed source and accepted inventory must be bound by the caller.
    This does not run/admit a product or substitute for its validation/errors.
    """
    if mode not in ('sequential','thread','process') or type(items) is not tuple or not 1<=len(items)<=16:
        raise ValueError('unsupported frozen population')
    if mode=='sequential' and budget.workers!=1:raise ValueError('unsupported sequential degree')
    if type(pending_bytes) is not int or pending_bytes not in (0,8388608):raise ValueError('unsupported publisher reserve')
    partitions=partition_tasks(items,budget.workers)
    inputs=sum(size(item) for item in items)
    result_reservation=len(items)*budget.max_task_result_bytes
    copies=0;failure_record_max=0
    if mode=='process':
        timings=(StageTiming(DiagnosticStage.QUEUE_WAIT,(1<<63)-1),
            StageTiming(DiagnosticStage.CALCULATION,(1<<63)-1),
            StageTiming(DiagnosticStage.SERIALIZATION,(1<<63)-1)) if diagnostics else ()
        reasons=tuple(c.value for c in SinkErrorCode)+tuple(c.value for c in CommandErrorCode)
        failure_record_max=max(size(TaskExecution(item.task,None,None,None,reason,timings)) for item in items for reason in reasons)
        if failure_record_max>budget.max_task_transport_bytes:raise ValueError('original failure transport bound')
        copies=2*sum(sorted((size(p) for p in partitions),reverse=True)[:budget.max_in_flight])
        copies+=3*(len(items)*budget.max_task_transport_bytes+size((None,)*len(items)))
    diagnostic_bytes=0
    if diagnostics:
        spans=5+3*len(items) if pending_bytes else 3
        diagnostic_bytes=512+len(items)*(2048+spans*256)
    task_bytes=max(len(encode_task(item.task)) for item in items)
    resident=inputs+copies+result_reservation+pending_bytes+budget.max_task_result_bytes+task_bytes+diagnostic_bytes
    if inputs>budget.max_input_bytes or result_reservation>budget.max_total_result_bytes or resident>budget.max_resident_bytes:
        raise ValueError('original logical reservation bound')
    return {'evidence':'source_derived_preflight','budget':asdict(budget),
        'input_bytes':inputs,'process_copy_reservation_bytes':copies,'result_reservation_bytes':result_reservation,
        'pending_bytes':pending_bytes,'diagnostic_reserved_bytes':diagnostic_bytes,'max_task_wire_bytes':task_bytes,
        'failure_record_max_bytes':failure_record_max,'initial_reserved_resident_bytes':resident,
        'native_ipc_encoded_bytes':None,'physical_enforcement':False}


def delegate_reservation(plan,items,budget,*,original,expected_output_bytes=0,owned_witness_count=None,
        publication_observation=False):
    """Reserve actual new plan/spec serialization without calling it native IPC.

    Two plan representations and three per-invocation spec populations are a
    conservative declared logical allowance. Source/hash/plan construction and
    these measurements belong inside the instrumented selected phase.
    """
    if type(expected_output_bytes) is not int or expected_output_bytes<0:raise ValueError('output bound')
    if type(publication_observation) is not bool:raise ValueError('publication observer admission')
    from eq142_publication import OBSERVER_RESERVATION
    observer_reservation=OBSERVER_RESERVATION if publication_observation else 0
    planned_witnesses=1+len(plan['reservations']) if plan is not None and plan.get('owned_process_witness') is not None else 0
    if owned_witness_count is None:owned_witness_count=planned_witnesses
    if (type(owned_witness_count) is not int or not 0<=owned_witness_count<=17
            or planned_witnesses and owned_witness_count!=planned_witnesses
            or not planned_witnesses and owned_witness_count not in (0,1)):
        raise ValueError('owned witness population differs')
    if plan is None:plan_bytes=0;extra=[]
    else:
        plan_bytes=size(plan);mode=plan['mode']
        partitions=partition_tasks(items,budget.workers)
        if mode=='sequential':
            from equity_feature_workers.supervisor import Partition
            partitions=tuple(Partition(0,(item,)) for item in partitions[0].items)
        if len(partitions)!=len(plan['reservations']):raise ValueError('delegate population mismatch')
        extra=[]
        for partition in partitions:
            args=(partition,budget,mode=='process',True,(1<<63)-1)
            raw=size((_ORIGINAL_PARTITION,args))
            proposed=size((partial(partition_entry,plan=plan),args))
            if proposed>16777216 or plan_bytes>1048576:raise ValueError('delegate spec bound')
            extra.append(max(0,proposed-raw))
    reservation=2*plan_bytes+3*sum(extra)
    witness_reservation=3*4096*owned_witness_count
    combined=original['initial_reserved_resident_bytes']+reservation+expected_output_bytes+witness_reservation+observer_reservation
    if combined>budget.max_resident_bytes:raise ValueError('instrumented logical resident bound')
    return {'plan_pickle_bytes':plan_bytes,'per_invocation_spec_extra_pickle_bytes':extra,
        'owned_witness_population':owned_witness_count,'owned_witness_encoded_max_bytes':4096*owned_witness_count,
        'owned_witness_logical_reservation_bytes':witness_reservation,
        'logical_spec_resident_reservation_bytes':reservation,'expected_committed_output_metadata_bytes':expected_output_bytes,
        'publication_observer_logical_reservation_bytes':observer_reservation,
        'combined_logical_resident_bytes':combined,'trace_reserved_bytes':TRACE_RESERVATION,
        'whole_job_sampled_rss_cap_bytes':JOB_RSS_CAP,'native_ipc_encoded_bytes':None,
        'physical_enforcement':False}


def confirm_original(outcome,preflight):
    if (outcome.admitted_input_bytes!=preflight['input_bytes']
            or outcome.reserved_resident_bytes!=preflight['initial_reserved_resident_bytes']):
        raise ValueError('actual original admission differs from source preflight')
