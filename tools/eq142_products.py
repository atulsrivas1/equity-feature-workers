"""Pure full-body product joins for the frozen successful study path.

Trusted items, independent mathematics, reference products and input admission
must come from the separately source-qualified controller. Bytes/hash validity
does not establish their provenance, a physical commit, or sample acceptance.
No filesystem access, calculation callback, native probe or optional calculator
import occurs here. Original SDK codecs remain the product authority.
"""
from __future__ import annotations
import hashlib
import json
import math

from equity_feature_workers import (TaskManifest, WorkItem, OutputManifest,
    decode_task, encode_task, decode_output, encode_output)
from equity_feature_workers.diagnostics import (DiagnosticSnapshot, TaskDiagnostic,
    StageTiming, DiagnosticStage, DiagnosticStatus)
from equity_feature_io_sdk import (decode_result, encode_result, decode_receipt,
    encode_receipt, prepare_publication)
from equity_feature_io_contracts import SinkRequirements
from eq142_evidence import diagnostic_projection, stable_products
from eq142_outcome import verify_original, validate_summary
from eq142_proofs import check_body, validate_reference

PRODUCT_FIELDS=frozenset(('task_sha256','disposition','task_wire','results','output',
    'receipt','committed_lookup','readback_results','diagnostics','raw_diagnostics'))
DIAGNOSTIC_FIELDS=frozenset(('attempt','intent_sha256','task_sha256','family',
    'family_sha256','partition_sha256','status','reason','timings','elapsed_ns'))


def closed(value,fields):
    if (type(value) is not dict or any(type(k) is not str for k in value)
            or set(value)!=fields):raise ValueError('product closed record')


class RetainedProducts:
    """Bounded immutable bodies; a selected proof subset, not namespace authority."""
    def __init__(self,bodies):
        if type(bodies) is not dict or len(bodies)>512:raise ValueError('product body population')
        total=0
        for sha,body in bodies.items():
            reference={'codec':'encode_task','sha256':sha,'bytes':len(body) if type(body) is bytes else 0,
                'retained_name':sha+'.json' if type(sha) is str else ''}
            check_body(body,reference,scope='products',name=reference['retained_name'])
            total+=len(body)
            if total>268435456:raise ValueError('product body total')
        # A caller mutation of its mapping cannot change this selected snapshot.
        self._bodies=bodies.copy();self.used=set()

    def verify(self,reference):
        name=reference.get('retained_name') if type(reference) is dict else None
        validate_reference(reference,scope='products',name=name)
        if reference['sha256'] not in self._bodies:raise ValueError('missing product body')
        body=check_body(self._bodies[reference['sha256']],reference,scope='products',name=name)
        self.used.add(reference['sha256'])
        return body

    def decode(self,reference,codec,decoder,encoder):
        if type(reference) is not dict or reference.get('codec')!=codec:raise ValueError('product codec')
        body=self.verify(reference);value=decoder(body)
        if encoder(value)!=body:raise ValueError('product codec round trip')
        return value

    def complete(self):
        if self.used!=set(self._bodies):raise ValueError('extra selected product body')


def decode_diagnostics(body):
    """Reconstruct the original SDK dataclasses, with exact canonical encoding.

    The SDK exposes encode(), not a snapshot decoder. This adapter validates
    every field through those original constructors; development JSON/LF is not
    the diagnostic wire format. Timing observations retain their original scope.
    """
    if type(body) is not bytes or not 1<=len(body)<=1048576:raise ValueError('diagnostic body bound')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('diagnostic duplicate key')
            result[key]=value
        return result
    try:
        record=json.loads(body,object_pairs_hook=pairs)
        closed(record,{'tasks','clock','resolution_ns'})
        rows=record['tasks']
        if type(rows) is not list or not 1<=len(rows)<=16:raise ValueError('diagnostic task population')
        tasks=[]
        for row in rows:
            closed(row,DIAGNOSTIC_FIELDS)
            if type(row['status']) is not str:raise ValueError('diagnostic status')
            if type(row['timings']) is not list or len(row['timings'])>53:raise ValueError('diagnostic timing bound')
            timings=[]
            for timing in row['timings']:
                closed(timing,{'stage','elapsed_ns','probes'})
                if type(timing['stage']) is not str:raise ValueError('diagnostic stage')
                timings.append(StageTiming(DiagnosticStage(timing['stage']),timing['elapsed_ns'],timing['probes']))
            tasks.append(TaskDiagnostic(**{**row,'status':DiagnosticStatus(row['status']),'timings':tuple(timings)}))
        result=DiagnosticSnapshot(tuple(tasks),record['clock'],record['resolution_ns'])
        if result.encode()!=body:raise ValueError('diagnostic noncanonical body')
        return result
    except (UnicodeError,TypeError,RecursionError,OverflowError) as error:
        raise ValueError('malformed diagnostic body') from error


def verify_mathematics(result,expected):
    """Independent scalar/readiness comparison with the frozen oracle tolerances.

    No oracle is generated here. The controller supplies its pinned independent
    expectations. These are the existing 2e-15 relative / 1e-12 absolute bounds.
    """
    closed(expected,{'cells','evidence','selected_headers'})
    headers=expected['selected_headers'];cells=expected['cells']
    if (type(headers) is not list or not headers or len(headers)>100
            or any(type(h) is not str for h in headers) or len(set(headers))!=len(headers)
            or type(cells) is not dict or set(cells)!=set(headers) or expected['evidence']!=[]):
        raise ValueError('independent expected cells')
    if ([c.feature_id for c in result.values]!=headers
            or len(result.quality)!=len(headers) or {q.feature_id for q in result.quality}!=set(headers)
            or result.evidence):raise ValueError('independent selected cells/quality/evidence')
    quality={q.feature_id:q for q in result.quality}
    for column in result.values:
        want=cells[column.feature_id];closed(want,{'value','status','expected','observed','reasons'})
        q=quality[column.feature_id]
        if (len(column.values)!=1 or type(want['status']) is not str
                or type(want['expected']) is not int or type(want['observed']) is not int
                or type(want['reasons']) is not list or any(type(r) is not str for r in want['reasons'])
                or (q.status.value,q.expected,q.observed,[r.value for r in q.reasons])!=
                    (want['status'],want['expected'],want['observed'],want['reasons'])):
            raise ValueError('independent quality/coverage')
        value=column.values[0];target=want['value']
        if target is None:
            if value is not None:raise ValueError('independent null')
        elif (type(target) is not float or not math.isfinite(target) or type(value) is not float
                or not math.isfinite(value) or not math.isclose(value,target,rel_tol=2e-15,abs_tol=1e-12)):
            raise ValueError('independent numeric value')


def verify_products(records,bodies,diagnostic_body,*,items,mathematics,
        reference_products,summary,preflight,mode,publishing):
    """Full codec, numerical, reference, diagnostic and admission joins.

    Only the frozen successful one-result-per-task path is eligible. Failed or
    partial original records remain retained by the runner and are rejected here.
    The returned original facts support the separate publication replay; they
    are not a new authority or complete-root emission.
    """
    if (type(publishing) is not bool or type(mode) is not str or mode not in ('sequential','thread','process')
            or type(items) is not tuple or not 1<=len(items)<=16
            or any(type(i) is not WorkItem or type(i.task) is not TaskManifest for i in items)):
        raise ValueError('product trusted context')
    tasks={i.task.task_sha256:i.task for i in items}
    if len(tasks)!=len(items) or type(mathematics) is not dict or set(mathematics)!=set(tasks):
        raise ValueError('product trusted population')
    if type(records) is not list or len(records)!=len(tasks):raise ValueError('product population')
    for record in records:closed(record,PRODUCT_FIELDS)
    if [r['task_sha256'] for r in records]!=sorted(tasks):raise ValueError('product task order/population')
    store=RetainedProducts(bodies);snapshot=decode_diagnostics(diagnostic_body)
    if [d.task_sha256 for d in snapshot.tasks]!=[i.task.task_sha256 for i in items]:
        raise ValueError('diagnostic original task order/population')
    projection=diagnostic_projection(snapshot);diagnostics={d.task_sha256:d for d in snapshot.tasks}
    validate_summary(summary)
    if summary['cancelled']:raise ValueError('cancelled study candidate')
    facts=[]
    for record,execution in zip(records,summary['tasks']):
        task=tasks[record['task_sha256']]
        decoded=store.decode(record['task_wire'],'encode_task',decode_task,encode_task)
        if decoded!=task:raise ValueError('product task differs from trusted original')
        references=record['results']
        if type(references) is not list or len(references)!=1:raise ValueError('product frozen result count')
        result=store.decode(references[0],'encode_result',decode_result,encode_result)
        if references[0]['bytes']>262144:raise ValueError('original task result cap')
        verify_mathematics(result,mathematics[task.task_sha256])
        # Original public SDK/manifest constructors bind metadata, units, selected
        # headers, configuration, input identities and complete entity coverage.
        envelope=prepare_publication((result,),destination_scope=task.destination_scope,
            generation_id=task.generation_id,job_id=task.job_id,partition_id=task.partition_id,
            limits=SinkRequirements(max_results=1,max_chunk_bytes=1048576,max_total_bytes=1048576,
                max_result_cells=100,max_evidence_rows=1))
        OutputManifest(task,envelope)
        entities=[(e.instrument_id,e.session_id,c.feature_id) for c in result.values for e in c.entities]
        required={(i,task.config.session.session_id,h.feature_id) for i in task.instruments for h in task.features}
        if len(entities)!=len(set(entities)) or set(entities)!=required:raise ValueError('product entities')
        output=None
        if publishing:
            output=store.decode(record['output'],'encode_output',decode_output,encode_output)
            receipt=store.decode(record['receipt'],'encode_receipt',decode_receipt,encode_receipt)
            if (output.task!=task or output.envelope!=envelope or output.receipt!=receipt
                    or record['committed_lookup']!='committed_matching_receipt'
                    or type(record['readback_results']) is not list or record['readback_results']!=references):
                raise ValueError('product output/receipt/readback join')
            # Reopen readback independently, even when its reference is deduped.
            for ref in record['readback_results']:
                if store.decode(ref,'encode_result',decode_result,encode_result)!=result:raise ValueError('product readback')
        elif (record['output'] is not None or record['receipt'] is not None
                or record['committed_lookup']!='not_applicable' or record['readback_results']!=[]):
            raise ValueError('nonpublishing product has publication')
        want_disposition={'task_sha256':task.task_sha256,'state':'computed','reason':None,
            'result_wire':references,'spill_wire':None,'output_wire':record['output'],
            'commit_state':'committed' if publishing else 'not_attempted','evidence':'observed'}
        if type(record['disposition']) is not dict or record['disposition']!=want_disposition:
            raise ValueError('product successful disposition')
        if execution['reason'] is not None or execution['result_count']!=1 or execution['output_present']!=publishing:
            raise ValueError('product successful original outcome')
        check_body(diagnostic_body,record['raw_diagnostics'],scope='runner',name='diagnostics.json')
        if record['raw_diagnostics']['codec']!='diagnostic_snapshot':raise ValueError('product diagnostic codec')
        diagnostic=diagnostics[task.task_sha256]
        expected_status=DiagnosticStatus.VERIFIED if publishing else DiagnosticStatus.COMPUTED
        if (diagnostic.attempt!=1+[i.task.task_sha256 for i in items].index(task.task_sha256)
                or diagnostic.intent_sha256!=task.task_sha256 or diagnostic.status!=expected_status
                or diagnostic.reason is not None or diagnostic.family!=(task.family if task.family in
                    ('bars','trades','quotes','history','sma_reference','daily_baseline','interval_baseline',
                     'relative_volume','interval_relative_volume','relative_returns','assembly','direction_counts','above_sma_fraction') else 'custom')
                or diagnostic.family_sha256!=hashlib.sha256(task.family.encode('utf-8')).hexdigest()
                or diagnostic.partition_sha256!=hashlib.sha256(task.partition_id.encode('utf-8')).hexdigest()
                or record['diagnostics']!=[d for d in projection if d['task_sha256']==task.task_sha256]):
            raise ValueError('product full diagnostic identity/status/projection')
        suffix=[{'stage':t.stage.value,'elapsed_ns':None if t.elapsed_ns is None else str(t.elapsed_ns),
            'probes':t.probes} for t in diagnostic.timings]
        timings=execution['timings']
        # Generic summary validation also supports failed/partial observations.
        # The frozen successful telemetry path always records all three stages.
        if [(t['stage'],t['probes']) for t in timings]!=[
                ('queue_wait',0),('calculation',0),('serialization',0)]:
            raise ValueError('product successful execution timing shape')
        prefix=suffix[:-3]
        expected_prefix=[('serialization',0),('queue_probe',1),('queue_wait',0),('publication',0)] if publishing else []
        if ([(t['stage'],t['probes']) for t in prefix]!=expected_prefix or suffix[-3:]!=timings):
            raise ValueError('product original diagnostic timings')
        facts.append({'task':task,'output':output,'reason':None})
    admission=verify_original(summary,items,records,preflight,store,expected_mode=mode)
    store.complete()
    if type(reference_products) is not list or stable_products(records)!=reference_products:
        raise ValueError('product pinned reference parity')
    return {'admission':admission,'facts':{'reserved_resident_bytes':summary['reserved_resident_bytes'],'tasks':facts},
        'full_sample_accepted':False}
