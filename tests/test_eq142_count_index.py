"""Synthetic staged index boundaries only; no underlying proof qualification."""
from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_count_index import verify_count_index,validate_entry,DISPOSITION
PROFILES=('small','balanced','skewed','family_complete','family_missing_hl','family_old_close_gap')
SERIES=('eq142_admitted_control','eq142_owned_runner_e2e')
from eq142_graph import wire,digest


def ref(codec,name):return {'codec':codec,'retained_name':name,'sha256':'a'*64,'bytes':3}


def fixture():
 entries=[]
 for profile in PROFILES:
  modes=(('sequential',1),('thread',2),('process',2)) if profile.startswith('family_') else (
   ('sequential',1),('thread',1),('thread',2),('thread',4),('thread',8),('process',1),('process',2),('process',4),('process',8))
  for mode,degree in modes:
   for series in SERIES:
    entries.append({'cohort_id':f'cohort-{len(entries):03d}',
     'selector':{'profile':profile,'series':series,'mode':mode,'degree':degree},
     'runner_return_reference':ref('eq142_owned_runner_return','runner-reference.json'),
     'runner_reference':ref('eq142_development_runner','runner.json'),
     'native_reference':ref('native_receipt','native.json'),
     'profiles':[ref('eq142_code_counts','parent.json')]+([ref('eq142_code_counts','delegate-000.json')] if mode=='process' else []),
     'comparison':{'operations':2,'call_entries':5,'typed_byte_returns':2,'typed_returned_bytes':8,
      'scope':'qualification aggregate only; no timing or completion-status authority'},
     'source_invariant_disposition':DISPOSITION,'timing_authority':False})
 source={'synthetic_unqualified_context':True};matrix='b'*64;capacity=ref('capacity_receipt','qualification-capacity.json')
 record={'schema':'eq142-count-qualification:1','stage':'qualification_only_no_timing_authority','source':source,
  'matrix_sha256':matrix,'capacity_freeze_reference':capacity,'entries':entries,
  'all_source_invariants_passed':True,'full_sample_accepted':False}
 return record,{'source':copy.deepcopy(source),'matrix_sha256':matrix,'capacity_freeze_reference':copy.deepcopy(capacity),'checked_entries':copy.deepcopy(entries)}


def check(record,context):
 body=wire(record);reference={'codec':'eq142_count_qualification','retained_name':'count-qualification.json','sha256':digest(body),'bytes':len(body)}
 return verify_count_index(body,reference,**context)


class PortableCountIndex(unittest.TestCase):
 def test_exact_ordered_seventy_two_synthetic_context_joins(self):
  record,context=fixture();self.assertEqual(len(record['entries']),72)
  actual=check(record,context);self.assertEqual(actual['cohorts'],72)
  self.assertIs(actual['timing_authority'],False);self.assertIs(actual['full_sample_accepted'],False)
  self.assertEqual(actual['scope'],'index_joins_against_trusted_independently_checked_entries_only')
 def test_missing_extra_reordered_rebound_producer_records_refuse(self):
  for change in ('missing','extra','reordered','source','matrix','capacity','true_as_one','accepted','field','count'):
   record,context=fixture()
   if change=='missing':record['entries'].pop()
   elif change=='extra':record['entries'].append(copy.deepcopy(record['entries'][0]))
   elif change=='reordered':record['entries'].reverse()
   elif change=='source':record['source']={'producer_selected':True}
   elif change=='matrix':record['matrix_sha256']='c'*64
   elif change=='capacity':record['capacity_freeze_reference']['sha256']='c'*64
   elif change=='true_as_one':record['all_source_invariants_passed']=1
   elif change=='accepted':record['full_sample_accepted']=True
   elif change=='field':record['extra']=0
   else:record['entries'][0]['comparison']['call_entries']+=1
   with self.assertRaises(ValueError,msg=change):check(record,context)
 def test_malformed_coherent_checked_context_cannot_bypass_closed_bounds(self):
  for change in ('empty','over72','duplicate_id','duplicate_selector','boolean_degree','focused_degree','process_missing_child','wrong_profile_name','boolean_count','extra_count','timing','bad_cohort','bad_ref'):
   record,context=fixture();entries=context['checked_entries'];entry=entries[0]
   if change=='empty':entries.clear()
   elif change=='over72':entries.append(copy.deepcopy(entry))
   elif change=='duplicate_id':entries[1]['cohort_id']=entry['cohort_id']
   elif change=='duplicate_selector':entries[1]['selector']=copy.deepcopy(entry['selector'])
   elif change=='boolean_degree':entry['selector']['degree']=True
   elif change=='focused_degree':entry['selector'].update(profile='family_complete',mode='thread',degree=8)
   elif change=='process_missing_child':entry['selector'].update(mode='process',degree=1)
   elif change=='wrong_profile_name':entry['profiles'][0]['retained_name']='delegate-000.json'
   elif change=='boolean_count':entry['comparison']['operations']=True
   elif change=='extra_count':entry['comparison']['physical_bytes']=1
   elif change=='timing':entry['timing_authority']=True
   elif change=='bad_cohort':entry['cohort_id']='../escape'
   else:entry['runner_reference']['codec']='encode_result'
   record['entries']=copy.deepcopy(entries)
   with self.assertRaises(ValueError,msg=change):check(record,context)
 def test_selected_body_changed_and_noncanonical_reference_rejected(self):
  record,context=fixture();body=wire(record);reference={'codec':'eq142_count_qualification','retained_name':'count-qualification.json','sha256':digest(body),'bytes':len(body)}
  with self.assertRaises(ValueError):verify_count_index(body+b' ',reference,**context)
  reference['retained_name']='arbitrary.json'
  with self.assertRaises(ValueError):verify_count_index(body,reference,**context)


if __name__=='__main__':unittest.main()
