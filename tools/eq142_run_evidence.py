"""Development run evidence component; not a complete-sample certificate.

The controller must independently establish actual writer/source/namespace and
full original proof authority. No study or performance acceptance is implied.
"""
from eq142_graph import wire
from eq142_proofs import check_body,decode_record
from eq142_products import verify_products
from eq142_execution import verify_execution
from eq142_runner_bundle import verify_runner_bundle
from eq142_publication import verify_publication
from eq142_code_counts import compare_aggregate,validate_profile
from eq142_admission import original_reservation,delegate_reservation


def same(actual,expected):
    if wire(actual)!=wire(expected):raise ValueError('run evidence expected join')


def records_for_report(report,bodies):
    """Selected normal runner bodies; whole fixed namespace is still separate."""
    expected={'runner.json','supervisor.json','products.json','diagnostics.json'}
    if report['control']=='instrumented_B':expected.add('publication.json')
    if type(bodies) is not dict or set(bodies)!=expected:raise ValueError('run record population')
    def reopen(reference,name,maximum):
        return decode_record(check_body(bodies[name],reference,scope='runner',name=name),maximum=maximum)
    products=reopen(report['product_proof'],'products.json',65536)
    summary=reopen(report['actual_supervisor_outcome'],'supervisor.json',65536)
    publication=reopen(report['publication_observation'],'publication.json',65536) if 'publication.json' in expected else None
    return products,summary,publication


def verify_profiled_counts(report,profile_bodies,*,mode,plan,execution):
    """Full same-run Python code events vs raw trace aggregate, no timing claim."""
    record=report['qualification_code_counts']
    if type(record) is not dict or set(record)!=set(('profiles','comparison','manual_source_expectations','timing_authority')):
        raise ValueError('run profiled count record')
    if record['timing_authority'] is not False:raise ValueError('run count timing authority')
    names=['parent.json']+([r['file'] for r in plan['reservations']] if mode=='process' else [])
    refs=record['profiles']
    if (type(refs) is not list or len(refs)!=len(names) or type(profile_bodies) is not dict
            or set(profile_bodies)!=set(names)):
        raise ValueError('run count profile population')
    profiles=[]
    for name,reference in zip(names,refs):
        body=check_body(profile_bodies[name],reference,scope='code_counts',name=name)
        value=decode_record(body,maximum=65536);validate_profile(value);profiles.append(value)
    manual={name:execution['fixed_source_counts'][name] for name in ('trace.bind_labels','trace.install','trace.restore')}
    same(record['manual_source_expectations'],manual)
    comparison=compare_aggregate(profiles,report['retained_spans']['counters'],manual_operations=tuple(manual))
    same(record['comparison'],comparison)
    # Reusable prior index must additionally bind this result to the exact
    # source/runtime/frozen selector matrix and original native/writer controls.
    totals={}
    for profile in profiles:
        for row in profile['counters']:
            totals[row['operation']]=totals.get(row['operation'],0)+row['call_entries']
    return {'comparison':comparison,'call_entries':totals,'timing_authority':False}


def verify_run_evidence(*,runner_body,returned_body,records,product_bodies,witness_bodies,
        trace_bodies,profile_bodies,native,context):
    """Draft assembly over independently regenerated controller context.

    Context must include actual source-bound reference, rebuilt WorkItems and
    independent oracle, current source/job, original budget and pure plan.
    This alone has no whole-namespace/index/capacity/root/E2E acceptance.
    """
    selector=context['selector'];mode=selector['mode'];publishing=selector['series']=='eq142_owned_runner_e2e'
    traced=context['control']=='instrumented_B';profiled=context['purpose']=='qualification_counts'
    report=decode_record(runner_body,maximum=1048576)
    # Closed normal schema and writer/native identity are checked by the final
    # bundle call below. Any early malformed record is a failed candidate.
    if type(records) is not dict or type(records.get('runner.json')) is not bytes or records['runner.json']!=runner_body:
        raise ValueError('run selected runner body')
    products,summary,publication=records_for_report(report,records)
    reference=context['reference'];profile=selector['profile'];items=context['items'];budget=context['budget']
    mathematics=context['mathematics'];plan=context['plan']
    same(reference['profiles'][profile]['mathematics'],mathematics)
    original=original_reservation(items,mode=mode,budget=budget,pending_bytes=8388608 if publishing else 0)
    verified_products=verify_products(products,product_bodies,records['diagnostics.json'],items=items,
        mathematics=mathematics,reference_products=reference['profiles'][profile]['products'][selector['series']],
        summary=summary,preflight=original,mode=mode,publishing=publishing)
    expected_output=sum(r['output']['bytes'] for r in reference['profiles'][profile]['products'][selector['series']]
        if r['output'] is not None)
    delegated=delegate_reservation(plan,items,budget,original=original,expected_output_bytes=expected_output,
        publication_observation=traced,owned_witness_count=1+(len(plan['reservations']) if traced else 0))
    bundle=verify_runner_bundle(runner_body,returned_body,sample_id=context['sample_id'],purpose=context['purpose'],
        selector=selector,control=context['control'],reference_sha256=context['reference_sha256'],source=context['source'],
        descriptive_job=context['descriptive_job'],original_admission=original,delegate_admission=delegated,
        product_records=products,product_admission=verified_products['admission'],witness_bodies=witness_bodies,
        native=native,plan=plan)
    execution=None;publication_checked=None;counts=None
    if traced:
        execution=verify_execution(report,plan,trace_bodies,
            task_features={i.task.task_sha256:tuple(h.feature_id for h in i.task.features) for i in items},
            mathematics=mathematics,publishing=publishing,maximum_in_flight=budget.max_in_flight)
        refs=[r for product in products for r in (product['output'],product['receipt']) if r is not None]
        selected={r['sha256']:product_bodies[r['sha256']] for r in refs}
        publication_checked=verify_publication(publication,verified_products['facts'],selected,
            initial=original['initial_reserved_resident_bytes'],maximum=budget.max_resident_bytes,
            combined=delegated['combined_logical_resident_bytes']-expected_output,publishing=publishing)
        same(report['publication_admission_replay'],publication_checked)
        if profiled:counts=verify_profiled_counts(report,profile_bodies,mode=mode,plan=plan,execution=execution)
        elif profile_bodies!={}:raise ValueError('run unexpected profiler bodies')
    elif trace_bodies!={} or profile_bodies!={}:raise ValueError('run untraced observer bodies')
    return {'runner':bundle,'products':verified_products,'execution':execution,'publication':publication_checked,
        'profiled_counts':counts,'scope':'selected_run_semantics_not_complete_root','full_sample_accepted':False}
