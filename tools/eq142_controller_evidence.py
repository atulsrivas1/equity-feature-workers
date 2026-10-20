"""Selected original-run proof loader; the controller authenticates ownership.

The owning controller must supply the original reference return and actual
normal native outcome in its newly exclusive fixed sample namespace. This
adapter independently regenerates numerical/source/job/plan context, but is not
the whole source-capacity-count namespace or final measurement controller.
"""
from pathlib import Path
from eq142_graph import wire,digest,bind_source,describe_job
from eq142_proofs import read_proof,decode_record
from eq142_owned_runner import decode_return
from eq142_evidence import bounded_read
from eq142_admission import budget_for
from eq142_fixture import load
from eq142_run_evidence import verify_run_evidence
from eq142_body_loading import read_product_set,read_trace_set,read_original_reference


def same(actual,expected):
    if wire(actual)!=wire(expected):raise ValueError('controller independently rebuilt context mismatch')


def rebuilt_context(reference,reference_sha256,*,selector,control,purpose,sample_id,native,directory):
    """No producer-owned expected job, mathematics, budget or plan is trusted."""
    from benchmark_execution_baseline import source_state
    from eq142_workloads import items_for,independent_expectations
    from eq142_delegate import describe_plan
    same(source_state(),reference['state'])
    bound=bind_source(reference['source_inventory']);bound.check()
    profile=selector['profile'];items=items_for(profile)
    mathematics=independent_expectations(profile,items)
    same(mathematics,reference['profiles'][profile]['mathematics'])
    budget=budget_for(selector['degree'],reference['cpu_slots'])
    plan=None
    if control=='instrumented_B':
        plan,_=describe_plan(items,mode=selector['mode'],budget=budget,
            expected_inventory=reference['source_inventory'],
            output_dir=str((directory/'runner/trace').resolve()),
            operations=load('RECORD_CONTRACTS.json')['trace']['operations'],
            qualification_profile_dir=str((directory/'runner/code-counts').resolve()) if purpose=='qualification_counts' else None,
            owned_process_witness={'job_name':native['job_name'],
                'output_dir':str((directory/'runner/owned-processes').resolve())})
    return {'selector':dict(selector),'control':control,'purpose':purpose,'sample_id':sample_id,
            'reference_sha256':reference_sha256,'reference':reference,'items':items,'budget':budget,
            'mathematics':mathematics,'source':bound.source,'plan':plan,
            'descriptive_job':describe_job(items,mode=selector['mode'],budget=budget,qualification=bound)}


def load_selected_run(reference_path,reference_return,*,directory,selector,control,purpose,
                      sample_id,native,native_reference):
    """Read fixed original proofs and regenerate their trusted checking context.

    Outer wrapper reference is source-derived readback, never its printed writer
    return. Only the nested closed wrapper body carries run_control's actual
    return; native/source/witness checks must complete before any caller accepts
    this readback. Failed/arbitrary namespaces do not acquire writer authority.
    """
    from benchmark_execution_baseline import source_state
    directory=Path(directory).absolute();reference_path=Path(reference_path)
    if directory.name!=sample_id:raise ValueError('controller fixed sample basename')
    if type(native) is not dict or native.get('exit_code')!=0 or native.get('error') is not None:
        raise ValueError('controller requires actual normal native result')
    reference=read_original_reference(reference_path,reference_return)
    native_body=read_proof(directory,native_reference,scope='sample',name='native.json')
    same(decode_record(native_body,maximum=16777216),native)
    returned_body=bounded_read(directory/'runner-reference.json',4096)
    returned=decode_return(returned_body)
    # The full fixed reader below rejects ancestor indirections and changes.
    wrapper_ref={'codec':'eq142_owned_runner_return','sha256':digest(returned_body),
                 'bytes':len(returned_body),'retained_name':'runner-reference.json'}
    if read_proof(directory,wrapper_ref,scope='sample',name='runner-reference.json')!=returned_body:
        raise ValueError('controller wrapper changed during readback')
    runner=directory/'runner'
    runner_body=read_proof(runner,returned['runner_reference'],scope='runner',name='runner.json')
    report=decode_record(runner_body,maximum=1048576)
    records={'runner.json':runner_body};runner_refs={'runner.json':returned['runner_reference']}
    for field,name in (('actual_supervisor_outcome','supervisor.json'),('product_proof','products.json')):
        runner_refs[name]=report[field];records[name]=read_proof(runner,report[field],scope='runner',name=name)
    if control=='instrumented_B':
        runner_refs['publication.json']=report['publication_observation']
        records['publication.json']=read_proof(runner,runner_refs['publication.json'],scope='runner',name='publication.json')
    products=decode_record(records['products.json'],maximum=65536)
    if type(products) is not list or not 1<=len(products)<=16:
        raise ValueError('controller original product population')
    runner_refs['diagnostics.json']=products[0]['raw_diagnostics']
    records['diagnostics.json']=read_proof(runner,runner_refs['diagnostics.json'],scope='runner',name='diagnostics.json')
    product_refs={}
    for product in products:
        refs=[product['task_wire'],*product['results'],*product['readback_results']]
        refs.extend(ref for ref in (product['output'],product['receipt']) if ref is not None)
        for ref in refs:
            previous=product_refs.get(ref['sha256'])
            if previous is not None:same(previous,ref)
            product_refs[ref['sha256']]=ref
            if len(product_refs)>512:raise ValueError('controller selected object population')
    context=rebuilt_context(reference,reference_return['sha256'],selector=selector,control=control,
                            purpose=purpose,sample_id=sample_id,native=native,directory=directory)
    product_bodies=read_product_set(reference_path.parent/'objects',product_refs)
    witness_refs={ref['retained_name']:ref for ref in report['owned_process_witnesses']['references']}
    if not 1<=len(witness_refs)<=17:raise ValueError('controller original witness population')
    witnesses={name:read_proof(runner/'owned-processes',ref,scope='owned_processes',name=name)
               for name,ref in witness_refs.items()}
    trace_refs={};profiles={};trace_bodies={};profile_bodies={}
    if control=='instrumented_B':
        trace_refs,trace_bodies=read_trace_set(runner/'trace',report['retained_spans']['fragments'],context['plan'])
        if purpose=='qualification_counts':
            for ref in report['qualification_code_counts']['profiles']:
                name=ref['retained_name']
                if name in profiles or len(profiles)>=17:raise ValueError('controller profile population')
                profiles[name]=ref;profile_bodies[name]=read_proof(runner/'code-counts',ref,scope='code_counts',name=name)
    verified=verify_run_evidence(runner_body=runner_body,returned_body=returned_body,records=records,
        product_bodies=product_bodies,witness_bodies=witnesses,trace_bodies=trace_bodies,
        profile_bodies=profile_bodies,native=native,context=context)
    bind_source(reference['source_inventory']).check();same(source_state(),reference['state'])
    return {'verified':verified,'context':context,'report':report,'returned':returned,
            'wrapper_reference':wrapper_ref,'wrapper_reference_origin':'source_derived_fixed_namespace_readback',
            'runner_reference':returned['runner_reference'],'native_reference':native_reference,
            'runner_references':runner_refs,'witness_references':witness_refs,
            'trace_references':trace_refs,'profile_references':profiles,'product_references':product_refs,
            'scope':'selected_original_run_not_complete_controller','full_sample_accepted':False}
