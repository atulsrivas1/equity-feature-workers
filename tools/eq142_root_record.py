"""Development root record component; not a complete-sample certificate.

The controller must independently establish actual writer/source/namespace and
full original proof authority. No study or performance acceptance is implied.
"""
from eq142_graph import wire
from eq142_proofs import check_body,decode_record,validate_reference

FIELDS=set('schema sample_id source selector control acceptance_scope clock_disposition runner_return_reference runner_reference native_reference supervisor_reference products_reference publication_reference count_qualification_reference capacity_before_reference expected_reference outer_source_before_sha256 control_trace_disposition complete failure_reason limitations full_sample_accepted'.split())
LIMITATIONS=tuple(sorted(('no_global_numeric_critical_path','native_ipc_not_captured',
 'physical_allocations_not_captured','bookkeeping_not_product_purity',
 'final_measurement_and_candidate_finalization_excluded','non_atomic_capacity_observation')))
ROUTES={
 'runner_return_reference':('sample','runner-reference.json','eq142_owned_runner_return'),
 'runner_reference':('runner','runner.json','eq142_development_runner'),
 'native_reference':('sample','native.json','native_receipt'),
 'supervisor_reference':('runner','supervisor.json','eq142_supervisor_outcome'),
 'products_reference':('runner','products.json','eq142_product_projection'),
 'publication_reference':('runner','publication.json','eq142_publication_observation'),
 'count_qualification_reference':('qualification','count-qualification.json','eq142_count_qualification'),
 'capacity_before_reference':('sample','capacity-before.json','capacity_receipt'),
 'expected_reference':('reference','reference.json','eq142_development_reference')}


def same(actual,expected):
 if wire(actual)!=wire(expected):raise ValueError('root record trusted binding differs')


def verify_root_record(body,reference,*,sample_id,source,selector,control,actual_references,source_before_sha256):
 """Compare the compact record to trusted fully verified controller facts.

 This function deliberately has no complete-evidence success return. Calling it
 with producer-invented expected maps cannot qualify any proof or sample. The
 controller must derive every map from the actual source/writer/readers and
 reject incomplete/failed runs BEFORE emitting a root. No circular reliance on
 the root's own booleans, claimed source, cached counts or expected references.
 """
 from eq142_owned_runner import text_pattern,ID_PATTERN
 text_pattern(sample_id,ID_PATTERN);text_pattern(source_before_sha256,r'[0-9a-f]{64}')
 if control not in ('uninstrumented_A','instrumented_B'):raise ValueError('root record control')
 if type(actual_references) is not dict or set(actual_references)!=set(ROUTES):
  raise ValueError('root record reference population')
 for field,(scope,name,codec) in ROUTES.items():
  selected=actual_references[field]
  if field=='publication_reference' and control=='uninstrumented_A':
   if selected is not None:raise ValueError('root untraced publication reference')
   continue
  validate_reference(selected,scope=scope,name=name)
  if selected['codec']!=codec:raise ValueError('root record routed codec')
 raw=check_body(body,reference,scope='sample',name='complete-root.json')
 if reference['codec']!='eq142_complete_root':raise ValueError('root record fixed codec')
 record=decode_record(raw,maximum=65536)
 if type(record) is not dict or set(record)!=FIELDS:raise ValueError('root closed record')
 if (record['schema']!='eq142-execution-trace:2'
     or record['acceptance_scope']!='development_complete_evidence_candidate_not_official_study'
     or record['clock_disposition']!='local_monotonic_durations_and_parent_causal_bounds'
     or record['complete'] is not True or record['failure_reason'] is not None
     or record['full_sample_accepted'] is not False):raise ValueError('root record disposition')
 disposition=('untraced_original_control_no_attributed_stage_costs' if control=='uninstrumented_A'
  else 'complete_instrumented_local_trace')
 same(record['control_trace_disposition'],disposition);same(record['sample_id'],sample_id)
 same(record['control'],control);same(record['source'],source);same(record['selector'],selector)
 same(record['outer_source_before_sha256'],source_before_sha256)
 same(record['limitations'],list(LIMITATIONS))
 for field in ROUTES:same(record[field],actual_references[field])
 return {'scope':'compact_root_record_joins_only_not_full_evidence_closure','full_sample_accepted':False}
