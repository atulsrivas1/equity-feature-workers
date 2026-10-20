"""Actual supervisor metadata, retained before later evidence/acceptance gates.

This summary is not a substitute for full original result/output codec bodies,
physical memory accounting, a per-commit ledger or complete sample acceptance.
"""
from dataclasses import asdict
import re

from equity_feature_workers.supervisor import SupervisorOutcome
from eq142_evidence import original_reason

FIELDS=frozenset(('schema','evidence','mode','cancelled','row_ledger','distinct_reuse_rows',
    'admitted_input_bytes','reserved_resident_bytes','result_bytes','tasks'))
TASK_FIELDS=frozenset(('task_sha256','reason','result_count','output_present','spill','timings'))


def snapshot_original(outcome):
    """Copy actual fields without sorting, correcting or verifying expectations."""
    if type(outcome) is not SupervisorOutcome:raise ValueError('no original supervisor outcome')
    return {'schema':'eq142-supervisor-outcome:1','evidence':'actual_original_supervisor_return',
        'mode':outcome.mode,'cancelled':outcome.cancelled,
        'row_ledger':[[identity,rows] for identity,rows in outcome.row_ledger],
        'distinct_reuse_rows':outcome.distinct_reuse_rows,'admitted_input_bytes':outcome.admitted_input_bytes,
        'reserved_resident_bytes':outcome.reserved_resident_bytes,'result_bytes':outcome.result_bytes,
        'tasks':[{'task_sha256':execution.task.task_sha256,
            'reason':execution.reason,'result_count':None if execution.results is None else len(execution.results),
            'output_present':execution.output is not None,'spill':None if execution.spill is None else asdict(execution.spill),
            'timings':[{'stage':timing.stage.value,'elapsed_ns':None if timing.elapsed_ns is None else str(timing.elapsed_ns),
                'probes':timing.probes} for timing in execution.timings]} for execution in outcome.tasks]}


def count(value):
    if type(value) is not int or not 0<=value<=9007199254740991:raise ValueError('invalid outcome count')


def identity(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None:raise ValueError('invalid outcome identity')


def validate_summary(record):
    if (type(record) is not dict or set(record)!=FIELDS or record['schema']!='eq142-supervisor-outcome:1'
            or record['evidence']!='actual_original_supervisor_return' or type(record['mode']) is not str
            or record['mode'] not in ('sequential','thread','process')
            or type(record['cancelled']) is not bool):raise ValueError('invalid outcome summary')
    for field in ('distinct_reuse_rows','admitted_input_bytes','reserved_resident_bytes','result_bytes'):count(record[field])
    if type(record['tasks']) is not list or not 1<=len(record['tasks'])<=16:raise ValueError('invalid outcome tasks')
    if type(record['row_ledger']) is not list or len(record['row_ledger'])!=len(record['tasks']):raise ValueError('invalid row ledger')
    rows=[]
    for row in record['row_ledger']:
        if type(row) is not list or len(row)!=2:raise ValueError('invalid row entry')
        identity(row[0]);count(row[1]);rows.append(row[0])
    if rows!=sorted(set(rows)):raise ValueError('unordered/duplicate row ledger')
    tasks=[]
    for task in record['tasks']:
        if type(task) is not dict or set(task)!=TASK_FIELDS:raise ValueError('invalid outcome task')
        identity(task['task_sha256']);tasks.append(task['task_sha256'])
        if task['reason'] is not None:
            if type(task['reason']) is not str:raise ValueError('invalid outcome reason')
            original_reason(task['reason'])
        if task['result_count'] is not None:
            count(task['result_count'])
            if task['result_count']>64:raise ValueError('original result tuple bound')
        if type(task['output_present']) is not bool:raise ValueError('invalid output presence')
        if task['spill'] is not None:raise ValueError('spill outside frozen study; reference retained only')
        timings=task['timings']
        if type(timings) is not list or len(timings)>3:raise ValueError('invalid outcome timings')
        stages=[]
        for timing in timings:
            if type(timing) is not dict or set(timing)!={'stage','elapsed_ns','probes'}:raise ValueError('invalid outcome timing')
            stages.append(timing['stage']);count(timing['probes']);elapsed=timing['elapsed_ns']
            if elapsed is not None and (type(elapsed) is not str or re.fullmatch('0|[1-9][0-9]{0,18}',elapsed) is None
                    or int(elapsed)>9223372036854775807):raise ValueError('invalid original timing')
        if stages not in ([],['queue_wait','calculation'],['queue_wait','calculation','serialization']):
            raise ValueError('invalid original timing order')
    if tasks!=rows:raise ValueError('outcome task/row memberships differ')


def verify_original(record,items,products,preflight,store,*,expected_mode):
    """Input-derived row/reuse joins and exact reopened result/output byte sums."""
    validate_summary(record)
    if record['mode']!=expected_mode:raise ValueError('actual original mode differs')
    expected={item.task.task_sha256:item for item in items}
    if len(expected)!=len(items) or not expected:raise ValueError('invalid expected tasks')
    expected_rows=[[key,sum(batch.row_count for batch in expected[key].batches)] for key in sorted(expected)]
    if record['row_ledger']!=expected_rows:raise ValueError('actual row ledger differs from inputs')
    groups={}
    for key,rows in expected_rows:
        group=expected[key].task.reuse_sha256;groups.setdefault(group,set()).add(rows)
    if any(len(values)!=1 for values in groups.values()):raise ValueError('inconsistent reuse row inputs')
    if record['distinct_reuse_rows']!=sum(next(iter(values)) for values in groups.values()):
        raise ValueError('actual distinct reuse rows differ')
    if (record['admitted_input_bytes']!=preflight['input_bytes'] or
            record['reserved_resident_bytes']!=preflight['initial_reserved_resident_bytes']):
        raise ValueError('actual original admission differs')
    if type(products) is not list or [p['task_sha256'] for p in products]!=sorted(expected):
        raise ValueError('actual product membership differs')
    result_bytes=0;accepted_metadata=0
    for task,product in zip(record['tasks'],products):
        references=product['results'];expected_count=task['result_count']
        if len(references)!=(0 if expected_count is None else expected_count):raise ValueError('result tuple/proof count differs')
        for reference in references:
            if reference['codec']!='encode_result':raise ValueError('wrong result proof codec')
            result_bytes+=len(store.verify(reference))
        output=product['output']
        if task['output_present']!=(output is not None):raise ValueError('output presence differs')
        if output is not None:
            if output['codec']!='encode_output':raise ValueError('wrong output proof codec')
            accepted_metadata+=len(store.verify(output))
        if product['disposition']['reason']!=original_reason(task['reason']):raise ValueError('original reason differs')
        if product['disposition']['spill_wire'] is not None:raise ValueError('unexpected spill proof')
    if record['result_bytes']!=result_bytes:raise ValueError('actual result byte ledger differs')
    if record['reserved_resident_bytes']+accepted_metadata>preflight['budget']['max_resident_bytes']:
        raise ValueError('accepted output metadata exceeds original logical cap')
    return {'evidence':'actual_supervisor_admitted','accepted_output_metadata_bytes':accepted_metadata,
        'output_metadata_scope':'accepted_output_bodies_only_not_all_backend_commits',
        'physical_enforcement':False,'full_sample_accepted':False}
