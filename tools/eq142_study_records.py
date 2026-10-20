"""Pure V9 fixed metadata joins; controller authenticates all expected context.

Record booleans/hashes do not replace actual completed source-matched reviews,
qualification receipts or full raw history. No timing authority is created by
these helpers alone.
"""
import re
from eq142_graph import wire,digest
from eq142_proofs import validate_reference,check_body
from eq142_root_record import LIMITATIONS
from eq142_study_statistics import baseline_reasons

PURPOSES=('controller_qualification_only','official_instrumentation_study')
RECEIPTS=('independent_source_review','windows_current_qualification','linux_current_qualification','current_ci_artifacts_review')
BINDINGS=('canonical_contract_head','worker_head','source_state_sha256','source_inventory_sha256','protocol_sha256','precode_sha256',
 'matrix_sha256','expected_reference','capacity_freeze_reference','count_qualification_reference')
DURATION_SCOPE='summed_local_inclusive_or_composite_spans_not_makespan_CPU_or_exclusive_numeric_cost'

def hexadecimal(value,digits):
 if type(value) is not str or re.fullmatch('[0-9a-f]{'+str(digits)+'}',value) is None:
  raise ValueError('study source hexadecimal identity')

def validate_admission(record):
 if type(record) is not dict or set(record)!={'schema','workspace_purpose',*BINDINGS,'pre_timing_receipt_sha256s',
  'all_actual_gates_verified','official_timing_authority','full_sample_accepted'}:
  raise ValueError('study admission closed record')
 if record['schema']!='eq142-study-admission:1' or record['workspace_purpose'] not in PURPOSES or record['full_sample_accepted'] is not False:
  raise ValueError('study admission schema/purpose')
 for name in BINDINGS:
  if name.endswith('_head'):hexadecimal(record[name],40)
  elif name.endswith('_sha256'):hexadecimal(record[name],64)
 for field,scope,name,codec in [('expected_reference','reference','reference.json','eq142_development_reference'),
  ('capacity_freeze_reference','qualification','qualification-capacity.json','capacity_receipt'),
  ('count_qualification_reference','qualification','count-qualification.json','eq142_count_qualification')]:
  validate_reference(record[field],scope=scope,name=name)
  if record[field]['codec']!=codec:raise ValueError('study admission fixed codec')
 receipts=record['pre_timing_receipt_sha256s']
 if type(receipts) is not dict or set(receipts)!=set(RECEIPTS):raise ValueError('study admission four receipt identities')
 official=record['workspace_purpose']=='official_instrumentation_study'
 for value in receipts.values():
  if official or value is not None:hexadecimal(value,64)
 for field in ('all_actual_gates_verified','official_timing_authority'):
  if type(record[field]) is not bool or record[field]!=official:raise ValueError('study admission permanent authority')
 if len(wire(record))>16384:raise ValueError('study admission encoded bound')

def verify_admission(record,*,actual_expected):
 """actual_expected must come from controller's real independently checked gates."""
 validate_admission(record);validate_admission(actual_expected)
 if wire(record)!=wire(actual_expected):raise ValueError('study admission actual source/gate/reference join')
 return record

def study_summary(admission,admission_reference,matrix,cohorts,*,before_B):
 """cohorts are freshly recomputed from fully revalidated history, not raw JSON."""
 validate_admission(admission)
 if admission['workspace_purpose']!='official_instrumentation_study':raise ValueError('qualification fixture has no study summary authority')
 validate_reference(admission_reference,scope='qualification',name='study-admission.json')
 if admission_reference['codec']!='eq142_study_admission':raise ValueError('study summary admission codec')
 check_body(wire(admission),admission_reference,scope='qualification',name='study-admission.json')
 if type(before_B) is not bool or type(cohorts) is not list or len(cohorts)!=72:
  raise ValueError('study summary fixed cohort population/stage')
 if type(matrix) is not dict or type(matrix.get('cohorts')) is not list or len(matrix['cohorts'])!=72:
  raise ValueError('study summary actual matrix')
 if digest(wire(matrix))!=admission['matrix_sha256']:raise ValueError('study summary actual matrix hash')
 for field in ('source_state_sha256','source_inventory_sha256','capacity_freeze_reference'):
  if field not in matrix or wire(matrix[field])!=wire(admission[field]):raise ValueError('study summary actual matrix source/capacity join')
 refusal=False
 for i,(row,cohort) in enumerate(zip(matrix['cohorts'],cohorts)):
  if row['cohort_id']!=f'c{i:03}' or any(wire(cohort[k])!=wire(row[k]) for k in ('cohort_id','selector','supported','omission_reason')):
   raise ValueError('study summary exact ordered matrix joins')
  if len(wire(cohort))>24576:raise ValueError('study summary cohort encoded bound')
  if row['supported']:
   if cohort['A'] is None or (cohort['B'] is None)!=before_B:raise ValueError('study summary stage coverage')
   refusal=refusal or bool(baseline_reasons(cohort['A']))
   if before_B and cohort['attributed_cost_accepted']:raise ValueError('study summary before-B acceptance')
 if before_B!=refusal:raise ValueError('study summary baseline stage reason')
 record={'schema':'eq142-study-summary:1',
  'stage':'a_baseline_refusal_before_B' if before_B else 'completed_official_instrumentation_study',
  'workspace_purpose':'official_instrumentation_study',**{k:admission[k] for k in BINDINGS},
  'admission_reference':admission_reference,'cohorts':cohorts,'all_collected_raw_evidence_valid':True,
  'attributed_cost_acceptance':not before_B and all(c['attributed_cost_accepted'] for c in cohorts if c['supported']),
  'optimization_adoption_attempted':False,'full_sample_accepted':False,'operation_duration_scope':DURATION_SCOPE,
  'limitations':list(LIMITATIONS)}
 head={k:v for k,v in record.items() if k!='cohorts'}
 if len(wire(head))>65536 or len(wire(record))>2097152:raise ValueError('study summary total encoded bound')
 return record
