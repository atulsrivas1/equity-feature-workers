"""Full original preparation readback, with independently rebuilt mathematics.

The controller authenticates the actual original preparation return/callbacks.
This reopens full original codecs and independent math without inventing an
actual SupervisorOutcome summary that preparation did not retain.
"""
import hashlib
from pathlib import Path
from eq142_graph import wire,bind_source
from eq142_proofs import read_proof,decode_record,validate_reference
from eq142_owned_runner import PROFILES,SERIES
from eq142_evidence import stable_products,diagnostic_projection
from eq142_products import RetainedProducts,decode_diagnostics,verify_mathematics,PRODUCT_FIELDS,closed
from equity_feature_workers import decode_task,encode_task,decode_output,encode_output,OutputManifest
from equity_feature_workers.diagnostics import DiagnosticStatus
from equity_feature_io_sdk import decode_result,encode_result,decode_receipt,encode_receipt,prepare_publication
from equity_feature_io_contracts import SinkRequirements
from eq142_body_loading import read_product_set,read_original_reference


def same(actual,expected):
    if wire(actual)!=wire(expected):raise ValueError('reference preparation expected join')


def validate_preparation(reference_path,reference_return,entries):
    """Validate the fixed12 actual callback-selected preparation pairs.

    Full source/capacity/namespace ownership and original preflight execution
    remain the owning controller's responsibility; no original outcome is forged.
    """
    reference_path=Path(reference_path)
    reference=read_original_reference(reference_path,reference_return)
    if set(reference['profiles'])!=set(PROFILES):raise ValueError('reference full profile population')
    expected=[(profile,series) for profile in PROFILES for series in SERIES]
    if type(entries) is not list or len(entries)!=12:raise ValueError('reference preparation callbacks')
    object_refs={};prepared={}
    # First close ALL12 metadata sets and the shared object-store envelope.
    # Per-series admission alone could still read disjoint256MiB populations
    # before a later global-store rejection. No product bodies are loaded here.
    for entry,pair in zip(entries,expected):
        if type(entry) is not dict or set(entry)!={'profile','series','products_reference','diagnostics_reference'}:
            raise ValueError('reference callback closed record')
        if (entry['profile'],entry['series'])!=pair:raise ValueError('reference callback fixed order')
        profile,series=pair;local=reference_path.parent/(profile+'-'+series)
        records=decode_record(read_proof(local,entry['products_reference'],scope='reference_preparation',name='products.json'),maximum=65536)
        validate_reference(entry['diagnostics_reference'],scope='reference_preparation',name='diagnostics.json')
        if type(records) is not list or not 1<=len(records)<=16:raise ValueError('reference bounded metadata population')
        for record in records:
            closed(record,PRODUCT_FIELDS)
            if type(record['results']) is not list or len(record['results'])!=1 or type(record['readback_results']) is not list or len(record['readback_results'])>1:
                raise ValueError('reference bounded metadata results')
            refs=[record['task_wire'],*record['results'],*record['readback_results']]
            refs.extend(ref for ref in (record['output'],record['receipt']) if ref is not None)
            for ref in refs:
                if type(ref) is not dict:raise ValueError('reference object ref type')
                validate_reference(ref,scope='products',name=ref.get('retained_name'))
                if ref['sha256'] in object_refs:same(object_refs[ref['sha256']],ref)
                object_refs[ref['sha256']]=ref
                if len(object_refs)>512:raise ValueError('reference global object count')
        prepared[pair]=records
    all_bodies=read_product_set(reference_path.parent/'objects',object_refs)
    from eq142_workloads import items_for,independent_expectations
    for entry,pair in zip(entries,expected):
        if type(entry) is not dict or set(entry)!={'profile','series','products_reference','diagnostics_reference'}:
            raise ValueError('reference callback closed record')
        if (entry['profile'],entry['series'])!=pair:raise ValueError('reference callback fixed order')
        profile,series=pair;publishing=series=='eq142_owned_runner_e2e'
        local=reference_path.parent/(profile+'-'+series)
        records=prepared[pair]
        diagnostic_body=read_proof(local,entry['diagnostics_reference'],scope='reference_preparation',name='diagnostics.json')
        items=items_for(profile);tasks={item.task.task_sha256:item.task for item in items}
        mathematics=independent_expectations(profile,items)
        same(mathematics,reference['profiles'][profile]['mathematics'])
        if set(reference['profiles'][profile]['products'])!=set(SERIES):raise ValueError('reference series population')
        if type(records) is not list or len(records)!=len(tasks):raise ValueError('reference task count')
        for record in records:closed(record,PRODUCT_FIELDS)
        same([record['task_sha256'] for record in records],sorted(tasks))
        same(stable_products(records),reference['profiles'][profile]['products'][series])
        selected={}
        for record in records:
            refs=[record['task_wire'],*record['results'],*record['readback_results']]
            refs.extend(ref for ref in (record['output'],record['receipt']) if ref is not None)
            for ref in refs:
                for population in (selected,object_refs):
                    if ref['sha256'] in population:same(population[ref['sha256']],ref)
                    population[ref['sha256']]=ref
                    if len(population)>512:raise ValueError('reference object count')
        bodies={sha:all_bodies[sha] for sha in selected}
        store=RetainedProducts(bodies);diagnostics=decode_diagnostics(diagnostic_body)
        if [d.task_sha256 for d in diagnostics.tasks]!=[item.task.task_sha256 for item in items]:
            raise ValueError('reference original diagnostic order')
        diagnostic_by_task={d.task_sha256:d for d in diagnostics.tasks};projection=diagnostic_projection(diagnostics)
        for record in records:
            task=tasks[record['task_sha256']]
            if store.decode(record['task_wire'],'encode_task',decode_task,encode_task)!=task:
                raise ValueError('reference full task metadata')
            if type(record['results']) is not list or len(record['results'])!=1:
                raise ValueError('reference original result count')
            result=store.decode(record['results'][0],'encode_result',decode_result,encode_result)
            if record['results'][0]['bytes']>262144:raise ValueError('reference task result cap')
            verify_mathematics(result,mathematics[task.task_sha256])
            envelope=prepare_publication((result,),destination_scope=task.destination_scope,
                generation_id=task.generation_id,job_id=task.job_id,partition_id=task.partition_id,
                limits=SinkRequirements(max_results=1,max_chunk_bytes=1048576,max_total_bytes=1048576,
                                       max_result_cells=100,max_evidence_rows=1))
            OutputManifest(task,envelope)
            actual_entities=[(e.instrument_id,e.session_id,c.feature_id) for c in result.values for e in c.entities]
            required={(i,task.config.session.session_id,h.feature_id) for i in task.instruments for h in task.features}
            if len(actual_entities)!=len(set(actual_entities)) or set(actual_entities)!=required:
                raise ValueError('reference exact result entities')
            if publishing:
                output=store.decode(record['output'],'encode_output',decode_output,encode_output)
                receipt=store.decode(record['receipt'],'encode_receipt',decode_receipt,encode_receipt)
                if output.task!=task or output.envelope!=envelope or output.receipt!=receipt:
                    raise ValueError('reference output/receipt join')
                same(record['readback_results'],record['results'])
                if record['committed_lookup']!='committed_matching_receipt':raise ValueError('reference readback disposition')
                for ref in record['readback_results']:
                    if store.decode(ref,'encode_result',decode_result,encode_result)!=result:raise ValueError('reference full readback')
            elif record['output'] is not None or record['receipt'] is not None or record['readback_results']!=[] or record['committed_lookup']!='not_applicable':
                raise ValueError('reference unexpected publication')
            same(record['disposition'],{'task_sha256':task.task_sha256,'state':'computed','reason':None,
                'result_wire':record['results'],'spill_wire':None,'output_wire':record['output'],
                'commit_state':'committed' if publishing else 'not_attempted','evidence':'observed'})
            same(record['raw_diagnostics'],entry['diagnostics_reference'])
            d=diagnostic_by_task[task.task_sha256]
            if (d.attempt!=1+[item.task.task_sha256 for item in items].index(task.task_sha256)
                    or d.intent_sha256!=task.task_sha256 or d.reason is not None
                    or d.status!=(DiagnosticStatus.VERIFIED if publishing else DiagnosticStatus.COMPUTED)
                    or d.family!='custom' or d.family_sha256!=hashlib.sha256(task.family.encode()).hexdigest()
                    or d.partition_sha256!=hashlib.sha256(task.partition_id.encode()).hexdigest()):
                raise ValueError('reference original diagnostic identity/status')
            same(record['diagnostics'],[row for row in projection if row['task_sha256']==task.task_sha256])
            expected_stages=([('serialization',0),('queue_probe',1),('queue_wait',0),('publication',0)] if publishing else [])
            expected_stages += [('queue_wait',0),('calculation',0),('serialization',0)]
            if [(t.stage.value,t.probes) for t in d.timings]!=expected_stages:
                raise ValueError('reference successful diagnostic source shape')
        store.complete()
    bind_source(reference['source_inventory']).check()
    same(read_original_reference(reference_path,reference_return),reference)
    return {'reference':reference,'product_references':object_refs,
            'scope':'full_preparation_codec_math_readback_not_source_capacity_or_namespace_authority',
            'full_sample_accepted':False}
