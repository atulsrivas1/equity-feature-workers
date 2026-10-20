"""Hand-specified synthetic compact root records; no real evidence accepted."""
from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_graph import wire,digest
from eq142_root_record import verify_root_record


def ref(codec,name):return {'codec':codec,'retained_name':name,'bytes':3,'sha256':'a'*64}


def fixture(control):
 refs={
  'runner_return_reference':ref('eq142_owned_runner_return','runner-reference.json'),
  'runner_reference':ref('eq142_development_runner','runner.json'),
  'native_reference':ref('native_receipt','native.json'),
  'supervisor_reference':ref('eq142_supervisor_outcome','supervisor.json'),
  'products_reference':ref('eq142_product_projection','products.json'),
  'publication_reference':ref('eq142_publication_observation','publication.json') if control=='instrumented_B' else None,
  'count_qualification_reference':ref('eq142_count_qualification','count-qualification.json'),
  'capacity_before_reference':ref('capacity_receipt','capacity-before.json'),
  'expected_reference':ref('eq142_development_reference','reference.json')}
 context={'sample_id':'synthetic-unqualified-root','source':{'synthetic_unqualified_context':True},
  'selector':{'profile':'small','series':'eq142_admitted_control','mode':'sequential','degree':1},
  'control':control,'actual_references':copy.deepcopy(refs),'source_before_sha256':'b'*64}
 record={'schema':'eq142-execution-trace:2','sample_id':context['sample_id'],'source':copy.deepcopy(context['source']),
  'selector':copy.deepcopy(context['selector']),'control':control,
  'acceptance_scope':'development_complete_evidence_candidate_not_official_study',
  'clock_disposition':'local_monotonic_durations_and_parent_causal_bounds',**refs,
  'outer_source_before_sha256':'b'*64,
  'control_trace_disposition':'complete_instrumented_local_trace' if control=='instrumented_B' else 'untraced_original_control_no_attributed_stage_costs',
  'complete':True,'failure_reason':None,'full_sample_accepted':False,
  'limitations':['bookkeeping_not_product_purity','final_measurement_and_candidate_finalization_excluded',
   'native_ipc_not_captured','no_global_numeric_critical_path','non_atomic_capacity_observation','physical_allocations_not_captured']}
 return record,context


def check(record,context):
 body=wire(record);selected={'codec':'eq142_complete_root','retained_name':'complete-root.json','sha256':digest(body),'bytes':len(body)}
 return verify_root_record(body,selected,**context)


class PortableRootRecord(unittest.TestCase):
 def test_both_synthetic_control_records_have_only_partial_join_result(self):
  for control in ('uninstrumented_A','instrumented_B'):
   record,context=fixture(control);actual=check(record,context)
   self.assertEqual(actual,{'scope':'compact_root_record_joins_only_not_full_evidence_closure','full_sample_accepted':False})
 def test_producer_tampering_and_boolean_flags_refuse(self):
  for change in ('missing','unknown','complete_integer','incomplete','failure','official','source','selector','source_digest','limitations','traced','ref'):
   record,context=fixture('instrumented_B')
   if change=='missing':del record['native_reference']
   elif change=='unknown':record['extra']=None
   elif change=='complete_integer':record['complete']=1
   elif change=='incomplete':record['complete']=False
   elif change=='failure':record['failure_reason']='ignored'
   elif change=='official':record['full_sample_accepted']=True
   elif change=='source':record['source']={'producer_selected':True}
   elif change=='selector':record['selector']['degree']=True
   elif change=='source_digest':record['outer_source_before_sha256']='c'*64
   elif change=='limitations':record['limitations'].reverse()
   elif change=='traced':record['control_trace_disposition']='untraced_original_control_no_attributed_stage_costs'
   else:record['native_reference']['sha256']='c'*64
   with self.assertRaises(ValueError,msg=change):check(record,context)
 def test_wrong_routed_ref_and_untraced_observer_cannot_be_coherently_admitted(self):
  for change in ('wrong_codec','wrong_name','bool_length','extra_expected_ref','A_publication'):
   record,context=fixture('uninstrumented_A');refs=context['actual_references']
   if change=='wrong_codec':refs['native_reference']['codec']='eq142_complete_root'
   elif change=='wrong_name':refs['native_reference']['retained_name']='elsewhere.json'
   elif change=='bool_length':refs['native_reference']['bytes']=True
   elif change=='extra_expected_ref':refs['injected']=None
   else:refs['publication_reference']=ref('eq142_publication_observation','publication.json')
   record.update(refs)
   with self.assertRaises(ValueError,msg=change):check(record,context)
 def test_rewritten_selected_body_and_unexpected_root_reference_refuse(self):
  record,context=fixture('instrumented_B');body=wire(record)
  selected={'codec':'eq142_complete_root','retained_name':'complete-root.json','sha256':digest(body),'bytes':len(body)}
  with self.assertRaises(ValueError):verify_root_record(body+b' ',selected,**context)
  selected['codec']='eq142_measurement_envelope'
  with self.assertRaises(ValueError):verify_root_record(body,selected,**context)


if __name__=='__main__':unittest.main()
