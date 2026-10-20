"""Independent portable V9 controls and one real full-matrix native qualification.

Portable classes never skip for optional calculators or host CPU capacity. The
actual class requires the original optional package, Windows ownership and five
allowed slots; it retains its complete raw root for separate evidence review.
"""
import copy,hashlib,json,os,subprocess,sys,tempfile,tracemalloc,unittest
from pathlib import Path
from unittest.mock import patch,Mock
from functools import wraps
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
import eq142_study_statistics as s
import eq142_study_statistics as stats
import eq142_study_records as records
import eq142_failure as f
import eq142_gate_bundle as gates
import eq142_failure_audit as audit
import eq142_body_loading as loading
import eq142_reference_validation as preparation
import eq142_workspace as workspace
import eq142_native
from supplied_native import NativeFailure
from eq142_owned_runner import PROFILES,SERIES
from eq142_graph import wire,digest
from eq142_fixture import load,HAVE_INTRADAY,require_cpu_slots
OPS=load('RECORD_CONTRACTS.json')['trace']['operations']
ROW={'cohort_id':'c000','selector':{'profile':'small','mode':'sequential','degree':1,'series':'eq142_admitted_control'},'supported':True,'omission_reason':None}
cohort_summary=s.cohort_summary

def no_optional_calculator_imports(method):
    @wraps(method)
    def guarded(*args,**kwargs):
        original=__import__
        def checked(name,*a,**kw):
            if name in ('eq142_workloads','benchmark_execution_baseline','equity_features_intraday'):
                raise AssertionError('Portable admission attempted an optional calculator import')
            return original(name,*a,**kw)
        with patch('builtins.__import__',side_effect=checked):return method(*args,**kwargs)
    return guarded

def samples(control,wall=100,cpu=100,rss=100):
 prefix='a' if control=='uninstrumented_A' else 'b'
 return [{'sample_id':f'{prefix}-c000-{kind}{index}',
  'candidate_reference':{'codec':'eq142_complete_candidate','sha256':f'{n:064x}','bytes':10,'retained_name':'candidate.json'},
  'wall_ns':str(wall),'user_ticks_100ns':str(cpu),'kernel_ticks_100ns':'0','sampled_aggregate_RSS_bytes':rss}
  for n,(kind,index) in enumerate([('w',0),('w',1),*[('r',i) for i in range(7)]])]

def control(which,**kwargs):return s.control_summary('c000',which,samples(which,**kwargs))

class IsolatedControllerEntry(unittest.TestCase):
 def test_isolated_bootstrap_resolves_owned_modules_without_dispatch(self):
  script=ROOT/'tools'/'eq142_complete_study.py'
  check="""import importlib.util,json,runpy,sys
from pathlib import Path
script=Path(sys.argv[1])
entry=runpy.run_path(str(script),run_name='eq142_bootstrap_check')
assert callable(entry['main'])
assert 'eq142_workspace' not in sys.modules
assert 'eq142_gate_bundle' not in sys.modules
for name in ('eq142_workspace','eq142_gate_bundle'):
 assert Path(importlib.util.find_spec(name).origin).resolve()==script.parent/(name+'.py')
assert Path(importlib.util.find_spec('eq142_fixture').origin).resolve()==script.parent.parent/'tests'/'eq142_fixture.py'
print(json.dumps({'owned_module_resolution':True,'controller_dispatched':False}))
"""
  with tempfile.TemporaryDirectory(prefix='eq142-cli-bootstrap-') as directory:
   result=subprocess.run([sys.executable,'-I','-c',check,str(script)],cwd=directory,
    text=True,capture_output=True,check=False)
   self.assertEqual(result.returncode,0,result.stderr)
   self.assertEqual(json.loads(result.stdout),{'owned_module_resolution':True,'controller_dispatched':False})
   self.assertEqual(list(Path(directory).iterdir()),[])

class CPUClockMetadata(unittest.TestCase):
 def test_actual_producer_clock_roundtrips_through_unchanged_validator(self):
  import time
  from eq142_envelope import validate_cpu_clock
  original=time.get_clock_info('process_time')
  clock=workspace.DevelopmentWorkspace._cpu_clock()
  validate_cpu_clock(clock)
  self.assertEqual(float(clock['reported_resolution_seconds']),original.resolution)
  self.assertEqual(clock['implementation'],original.implementation)
  self.assertNotIn('e',clock['reported_resolution_seconds'].lower())
 def test_exponent_resolution_preserves_exact_decimal_and_other_fields_under_hostile_context(self):
  from decimal import localcontext,ROUND_DOWN
  from types import SimpleNamespace
  from eq142_envelope import validate_cpu_clock
  for resolution,expected in [(1e-7,'0.000000099999999999999995'),
                               (1e-9,'0.0000000010000000000000001'),(0.125,'0.125')]:
   original=SimpleNamespace(implementation='SyntheticSourceClock()',monotonic=True,adjustable=False,resolution=resolution)
   with localcontext() as context:
    context.prec=1;context.rounding=ROUND_DOWN;context.Emax=2;context.Emin=-2
    with patch.object(workspace.time,'get_clock_info',return_value=original) as observed:
     clock=workspace.DevelopmentWorkspace._cpu_clock()
    observed.assert_called_once_with('process_time')
    validate_cpu_clock(clock)
   self.assertEqual(clock,{'name':'process_time','implementation':'SyntheticSourceClock()',
    'monotonic':True,'adjustable':False,'reported_resolution_seconds':expected,
    'cross_process_raw_ticks_qualified':False})

class ExactStudyStatistics(unittest.TestCase):
 def test_seven_value_independent_goldens_and_integer_precision(self):
  self.assertEqual(s.metric(list(range(1,8)),s.INT63),{'median':'4','inclusive_IQR':{'numerator':'3','denominator':1},'p95':'7'})
  self.assertEqual(s.metric([1,1,2,4,4,4,9],s.INT63)['inclusive_IQR'],{'numerator':'5','denominator':2})
  n=2**53+1
  self.assertEqual(s.metric([n+i for i in range(7)],s.INT63)['median'],str(n+3))
  self.assertEqual(s.metric([s.INT63]*7,s.INT63)['p95'],str(s.INT63))
  self.assertEqual(s.metric([2*s.INT63]*7,2*s.INT63)['median'],str(2*s.INT63))
 def test_noise_exact_boundary_and_nonnoise_CPU_description(self):
  self.assertTrue(s.noise_pass({'median':'300','inclusive_IQR':{'numerator':'10','denominator':1}}))
  self.assertFalse(s.noise_pass({'median':'299','inclusive_IQR':{'numerator':'10','denominator':1}}))
  self.assertTrue(s.noise_pass({'median':'150','inclusive_IQR':{'numerator':'10','denominator':2}}))
 def test_control_warmups_excluded_from_stats_but_absolute_cap_enforced(self):
  a=samples('uninstrumented_A');a[0]['wall_ns']='999999';a[1]['sampled_aggregate_RSS_bytes']=s.GIB
  result=s.control_summary('c000','uninstrumented_A',a)
  self.assertEqual(result['wall_ns']['median'],'100');self.assertEqual(result['max_retained_sampled_aggregate_RSS_bytes'],100)
  a[0]['sampled_aggregate_RSS_bytes']=s.GIB+1
  with self.assertRaises(ValueError):s.control_summary('c000','uninstrumented_A',a)
 def test_population_order_and_strict_decimal_types(self):
  cases=[]
  for value in ['01','+1','-1','1.0','1e2',True,s.INT63+1]:
   a=samples('uninstrumented_A');a[3]['wall_ns']=value;cases.append(a)
  a=samples('uninstrumented_A');a[0],a[1]=a[1],a[0];cases.append(a)
  cases.append(samples('uninstrumented_A')[:-1])
  for a in cases:
   with self.assertRaises(ValueError):s.control_summary('c000','uninstrumented_A',a)
 def test_exact_overhead_boundaries_and_nonwins(self):
  A=control('uninstrumented_A');B=control('instrumented_B',wall=105,cpu=125,rss=125)
  ops=[{}]*47
  result=s.cohort_summary(ROW,A,B,ops)
  self.assertTrue(result['attributed_cost_accepted']);self.assertEqual(result['failure_reasons'],[])
  B=control('instrumented_B',wall=111,cpu=126,rss=126)
  result=s.cohort_summary(ROW,A,B,ops)
  self.assertEqual(result['failure_reasons'],['CPU_overhead','RSS_overhead','small_focused_p95','wall_overhead'])
  self.assertFalse(result['attributed_cost_accepted'])
 def test_before_B_zero_denominators_and_fixed_gate_applicability(self):
  A=control('uninstrumented_A',cpu=0,rss=0)
  result=s.cohort_summary(ROW,A,None,[],before_B=True)
  self.assertEqual(result['failure_reasons'],['A_RSS_zero','A_median_CPU_zero'])
  self.assertFalse(result['attributed_cost_accepted'])
  self.assertIsNone(result['gates']['median_owned_CPU']);self.assertIsNone(result['gates']['B_source_counts'])
  self.assertTrue(result['gates']['source_capacity_clock_evidence'])
  row=copy.deepcopy(ROW);row['selector']['profile']='balanced'
  result=s.cohort_summary(row,control('uninstrumented_A'),control('instrumented_B'),[{}]*47)
  self.assertIsNone(result['gates']['small_focused_p95'])
 def test_CPU_sum_exact_and_CPU_variation_not_wall_noise(self):
  a=samples('uninstrumented_A')
  for sample in a:sample['user_ticks_100ns']=str(s.INT63);sample['kernel_ticks_100ns']=str(s.INT63)
  result=s.control_summary('c000','uninstrumented_A',a)
  self.assertEqual(result['owned_CPU_ticks_100ns']['median'],str(2*s.INT63))
  self.assertEqual(s.baseline_reasons(result),[])
 def test_omitted_row_has_no_observations_or_acceptance(self):
  row={**ROW,'supported':False,'omission_reason':'cpu_primary_group_slots_insufficient'}
  result=s.cohort_summary(row,None,None,[])
  self.assertTrue(all(v is None for v in result['gates'].values()));self.assertFalse(result['attributed_cost_accepted'])
  with self.assertRaises(ValueError):s.cohort_summary(row,control('uninstrumented_A'),None,[])
 def test_operation_local_inclusive_sums_and_unavailable_byte_policy(self):
  op='manifest.encode_task';populations=[]
  for n in range(7):
   populations.append([{'id':f's{i}','reservation_id':'parent','operation':op,'start_ns':'0','end_ns':str(n+10),
    'status':'returned','observed_bytes':5,'byte_observation':'returned_encoded_bytes'} for i in range(2)])
  result=s.operation_summaries(OPS,populations);row=next(x for x in result if x['operation']==op)
  self.assertEqual(row,{'operation':op,'call_entries_min':2,'call_entries_max':2,'median_sum_local_duration_ns':'26',
   'p95_sum_local_duration_ns':'32','median_observed_return_bytes':'10','p95_observed_return_bytes':'10'})
  unknown=next(x for x in result if x['operation'] not in s.OBSERVABLE)
  self.assertIsNone(unknown['median_observed_return_bytes']);self.assertEqual(unknown['median_sum_local_duration_ns'],'0')
  zero=next(x for x in result if x['operation']=='sdk.encode_receipt');self.assertEqual(zero['median_observed_return_bytes'],'0')
  populations[0].append(populations[0][0])
  with self.assertRaises(ValueError):s.operation_summaries(OPS,populations)

def record(error=None,refs=None):
 return f.failure_record(ValueError() if error is None else error,sample_id='qualification',stage='reference_preparation',
  available_references=[] if refs is None else refs,original_outcome_disposition='unknown')

class FailureRetention(unittest.TestCase):
 def test_type_unicode_truthful_truncation_without_message(self):
  class SecretError(Exception):
   def __str__(self):raise AssertionError('message must never be read')
  SecretError.__module__='unicode_\u03b1';SecretError.__qualname__='\u03b2'*300
  error=SecretError('private');value=record(error);identity=value['error_type']
  self.assertEqual(identity['qualified_name'],('unicode_\u03b1.'+'\u03b2'*300)[:256]);self.assertTrue(identity['truncated'])
  self.assertNotIn(b'private',f.wire(value));self.assertIn(b'\\u03b1',f.wire(value))
 def test_actual_native_code_kept_and_non_native_code_not_fabricated(self):
  self.assertEqual(record(NativeFailure('OWNED_DEADLINE'))['native_code'],'OWNED_DEADLINE')
  class OtherError(Exception):code='OTHER'
  self.assertIsNone(record(OtherError())['native_code'])
 def test_nested_failure_attempted_once_and_primary_cause_identity(self):
  primary=NativeFailure('OWNED_DEADLINE');cause=ValueError('private cause');primary.__cause__=cause
  class Owner:
   _owns_root=True;_failed=False;calls=0
   def _retain_failure(self,error):
    self.calls+=1;self.captured=error;raise OSError('secondary private diagnostic')
   @f.controller_guard
   def outer(self):return self.inner()
   @f.controller_guard
   def inner(self):raise primary
  owner=Owner()
  try:owner.outer()
  except NativeFailure as caught:
   self.assertIs(caught,primary);self.assertIs(caught.__cause__,cause);self.assertEqual(caught.code,'OWNED_DEADLINE')
  else:self.fail('primary not raised')
  self.assertEqual(owner.calls,1);self.assertTrue(owner._failure_attempted);self.assertTrue(owner._failed)
  with self.assertRaisesRegex(ValueError,'prior failure retained'):owner.outer()
  self.assertEqual(owner.calls,1)
 def test_unowned_root_gets_no_diagnostic_attempt(self):
  class Owner:
   _owns_root=False
   def _retain_failure(self,error):raise AssertionError('unowned root must not write')
   @f.controller_guard
   def start(self):raise ValueError('original')
  owner=Owner()
  with self.assertRaisesRegex(ValueError,'original'):owner.start()
  self.assertTrue(owner._failed);self.assertFalse(getattr(owner,'_failure_attempted',False))
 def test_actual_bounded_write_exclusive_and_no_message(self):
  with tempfile.TemporaryDirectory() as temporary:
   directory=Path(temporary);value=record(ValueError('private sensitive message'))
   result=f.retain_failure(directory,value);body=(directory/'failure.json').read_bytes()
   self.assertEqual(result['bytes'],len(body));self.assertNotIn(b'private sensitive message',body)
   self.assertEqual(hashlib.sha256(body).hexdigest(),result['sha256'])
   with self.assertRaises(ValueError):f.retain_failure(directory,value)
   self.assertEqual((directory/'failure.json').read_bytes(),body)
 def test_prefix_bounds_origins_and_fixed_names(self):
  actual={'origin':'actual_writer_return','context':'sample','reference':{'codec':'native_receipt','sha256':'a'*64,'bytes':1,'retained_name':'native.json'}}
  derived={'origin':'caller_retained_unaccepted_diagnostic','context':'sample','reference':{'codec':'eq142_unaccepted_diagnostic','sha256':'b'*64,'bytes':0,'retained_name':'native.json.partial'}}
  record(refs=[actual,derived]);record(refs=[actual]*64)
  with self.assertRaises(ValueError):record(refs=[actual]*65)
  bad=copy.deepcopy(actual);bad['reference']={'codec':'eq142_owned_runner_return','sha256':'a'*64,'bytes':1,'retained_name':'runner-reference.json'}
  with self.assertRaisesRegex(ValueError,'never observed writer'):record(refs=[bad])
  for field,value in [('retained_name','arbitrary.json'),('bytes',True),('bytes',16777217)]:
   bad=copy.deepcopy(derived);bad['reference'][field]=value
   with self.assertRaises(ValueError):record(refs=[bad])
 def test_closed_schema_and_stage_and_native_codes(self):
  for field,badvalue in [('stage','unknown'),('native_code',True),('native_code','private error text'),
   ('full_sample_accepted',True),('sample_id','../unsafe')]:
   bad=record();bad[field]=badvalue
   with self.assertRaises(ValueError):f.validate_failure(bad)
  bad=record();bad['message']='private'
  with self.assertRaises(ValueError):f.validate_failure(bad)

class BoundedIdentity(unittest.TestCase):
 def test_arbitrary_metadata_size_does_not_get_concatenated(self):
  class E(Exception):pass
  E.__module__='m'*100000;E.__qualname__='q'*100000;error=E()
  tracemalloc.start()
  try:
   tracemalloc.reset_peak();identity=f.error_identity(error);current,peak=tracemalloc.get_traced_memory()
  finally:tracemalloc.stop()
  self.assertEqual(identity,{'qualified_name':'m'*256,'truncated':True})
  self.assertLess(peak,16384) # rejects the independently observed300084-byte fullconcat
 def test_dot_boundary_and_unicode_codepoint_exact_prefix(self):
  class E(Exception):pass
  for module,name in [('m'*255,'q'),('m'*254,'\u03b1\u03b2'),('m'*256,'q'),('m'*257,'q'),('', '\u03b1'*300),('m','q')]:
   E.__module__=module;E.__qualname__=name
   identity=f.error_identity(E());full=module+'.'+name
   self.assertEqual(identity,{'qualified_name':full[:256],'truncated':len(full)>256})

def reference(codec,name):return {'codec':codec,'sha256':'a'*64,'bytes':20,'retained_name':name}

def admission(official):
 return {'schema':'eq142-study-admission:1','workspace_purpose':records.PURPOSES[int(official)],
  'canonical_contract_head':'1'*40,'worker_head':'2'*40,'source_state_sha256':'3'*64,
  'source_inventory_sha256':'4'*64,'protocol_sha256':'5'*64,'precode_sha256':'6'*64,'matrix_sha256':digest(wire(matrix())),
  'expected_reference':reference('eq142_development_reference','reference.json'),
  'capacity_freeze_reference':reference('capacity_receipt','qualification-capacity.json'),
  'count_qualification_reference':reference('eq142_count_qualification','count-qualification.json'),
  'pre_timing_receipt_sha256s':{k:'8'*64 if official else None for k in records.RECEIPTS},
  'all_actual_gates_verified':official,'official_timing_authority':official,'full_sample_accepted':False}

def matrix():
 return {'schema':'eq142-source-capacity-matrix:1','source_state_sha256':'3'*64,'source_inventory_sha256':'4'*64,
  'capacity_freeze_reference':reference('capacity_receipt','qualification-capacity.json'),
  'cohorts':[{'cohort_id':f'c{i:03}','selector':{'profile':'small','mode':'sequential','degree':1,
  'series':'eq142_admitted_control'},'supported':i==0,'omission_reason':None if i==0 else 'cpu_primary_group_slots_insufficient'} for i in range(72)]}

def cohorts(before_B=False,refusal=False):
 return [cohort_summary(row,control('uninstrumented_A',cpu=0 if refusal else 100) if row['supported'] else None,
  control('instrumented_B') if row['supported'] and not before_B else None,[{}]*47 if row['supported'] and not before_B else [],before_B=before_B)
  for row in matrix()['cohorts']]

def admission_reference(value):
 body=wire(value)
 return {'codec':'eq142_study_admission','sha256':digest(body),'bytes':len(body),'retained_name':'study-admission.json'}

class StudyRecords(unittest.TestCase):
 def test_closed_qualification_never_inherits_official_authority(self):
  value=admission(False);records.validate_admission(value)
  for field in ['all_actual_gates_verified','official_timing_authority','full_sample_accepted']:
   bad=copy.deepcopy(value);bad[field]=True
   with self.assertRaises(ValueError):records.validate_admission(bad)
  with self.assertRaises(ValueError):records.study_summary(value,reference('eq142_study_admission','study-admission.json'),matrix(),cohorts(),before_B=False)
 def test_official_needs_exact_four_hashes_but_not_self_authentication(self):
  value=admission(True);records.verify_admission(value,actual_expected=copy.deepcopy(value))
  bad=copy.deepcopy(value);bad['pre_timing_receipt_sha256s']['independent_source_review']=None
  with self.assertRaises(ValueError):records.validate_admission(bad)
  bad=copy.deepcopy(value);bad['worker_head']='9'*40
  with self.assertRaisesRegex(ValueError,'actual source/gate/reference join'):records.verify_admission(bad,actual_expected=value)
  bad=copy.deepcopy(value);bad['pre_timing_receipt_sha256s']['extra']='f'*64
  with self.assertRaises(ValueError):records.validate_admission(bad)
 def test_summary_completion_and_before_B_zero_CPU_refusal(self):
  ref=admission_reference(admission(True))
  result=records.study_summary(admission(True),ref,matrix(),cohorts(),before_B=False)
  self.assertEqual(result['stage'],'completed_official_instrumentation_study');self.assertTrue(result['attributed_cost_acceptance'])
  self.assertFalse(result['full_sample_accepted']);self.assertEqual(len(result['cohorts']),72)
  result=records.study_summary(admission(True),ref,matrix(),cohorts(True,True),before_B=True)
  self.assertEqual(result['stage'],'a_baseline_refusal_before_B');self.assertFalse(result['attributed_cost_acceptance'])
  with self.assertRaisesRegex(ValueError,'baseline stage reason'):
   records.study_summary(admission(True),ref,matrix(),cohorts(True,False),before_B=True)
 def test_matrix_order_coverage_and_summary_encoded_bounds(self):
  ref=admission_reference(admission(True));rows=cohorts();rows[0],rows[1]=rows[1],rows[0]
  with self.assertRaisesRegex(ValueError,'ordered matrix joins'):records.study_summary(admission(True),ref,matrix(),rows,before_B=False)
  rows=cohorts();rows[0]['padding']='x'*24576
  with self.assertRaisesRegex(ValueError,'cohort encoded bound'):records.study_summary(admission(True),ref,matrix(),rows,before_B=False)
  with self.assertRaises(ValueError):records.study_summary(admission(True),ref,matrix(),cohorts()[:-1],before_B=False)
 def test_new_codecs_remain_fixed_route_and_bound(self):
  from eq142_proofs import validate_reference
  for codec,name,limit in [('eq142_study_admission','study-admission.json',16384),('eq142_study_summary','study-summary.json',2097152)]:
   ref=reference(codec,name);ref['bytes']=limit;validate_reference(ref,scope='qualification',name=name)
   with self.assertRaises(ValueError):validate_reference(ref,scope='sample',name=name)
   ref['bytes']=limit+1
   with self.assertRaises(ValueError):validate_reference(ref,scope='qualification',name=name)

class ExactLocalJoins(unittest.TestCase):
 def test_stale_admission_hash_length_matrix_digest_and_source_joins_reject(self):
  value=admission(True);ref=admission_reference(value);actual_matrix=matrix()
  records.study_summary(value,ref,actual_matrix,cohorts(),before_B=False)
  for field,new in [('sha256','0'*64),('bytes',ref['bytes']+1)]:
   bad=copy.deepcopy(ref);bad[field]=new
   with self.assertRaises(ValueError):records.study_summary(value,bad,actual_matrix,cohorts(),before_B=False)
  bad_value=copy.deepcopy(value);bad_value['matrix_sha256']='0'*64
  with self.assertRaisesRegex(ValueError,'matrix hash'):
   records.study_summary(bad_value,admission_reference(bad_value),actual_matrix,cohorts(),before_B=False)
  for field in ('source_state_sha256','source_inventory_sha256','capacity_freeze_reference'):
   bad_matrix=copy.deepcopy(actual_matrix)
   if field=='capacity_freeze_reference':bad_matrix[field]['sha256']='0'*64
   else:bad_matrix[field]='0'*64
   joined=copy.deepcopy(value);joined['matrix_sha256']=digest(wire(bad_matrix))
   with self.assertRaisesRegex(ValueError,'matrix source/capacity join'):
    records.study_summary(joined,admission_reference(joined),bad_matrix,cohorts(),before_B=False)

class V9Portable(unittest.TestCase):
 def test_streamed_operation_totals_match_full_seven_golden_path(self):
  raw=[]
  for i in range(7):
   raw.append([{'id':'s0','reservation_id':'parent','operation':'manifest.encode_task',
    'start_ns':'1','end_ns':str(i+2),'status':'returned','observed_bytes':i,'byte_observation':'returned_encoded_bytes'}])
  self.assertEqual(stats.summarize_operation_totals(OPS,[stats.operation_totals(OPS,x) for x in raw]),stats.operation_summaries(OPS,raw))
  values=stats.operation_totals(OPS,raw[0]);self.assertEqual(len(values),47)
  self.assertEqual(values['manifest.encode_task'],[1,1,0])
  self.assertEqual(stats.summarize_operation_totals(OPS,[values]*7)[OPS.index('manifest.encode_task')]['median_sum_local_duration_ns'],'1')
  for mutation in ['count','bytes','bool','extra']:
   bad=copy.deepcopy(values)
   if mutation=='count':bad['manifest.encode_task'][0]=131073
   elif mutation=='bytes':bad[next(x for x in OPS if x not in stats.OBSERVABLE)][2]=0
   elif mutation=='bool':bad['manifest.encode_task'][1]=True
   else:bad['unknown']=[0,0,None]
   with self.assertRaises(ValueError):stats.summarize_operation_totals(OPS,[bad]*7)

 def test_stat_only_failure_accounting_keeps_final_and_partial_and_pending(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);q=root/'qualification';q.mkdir()
   (q/'study-summary.json').write_bytes(b'123');(q/'study-summary.json.partial').write_bytes(b'45')
   (q/'failure.json').write_bytes(b'6')
   result=audit.audit_failure_namespace(root,pending_failure_allowance=2097152)
   self.assertEqual(result['extra_names_bytes'],2097155)
   self.assertEqual(result['total_bytes_including_pending_allowance'],2097158)
   self.assertEqual(result['qualification_normal_components']['reference_and_index'],3)
   self.assertFalse(result['acceptance_authority']);self.assertFalse(result['physical_allocation_guaranteed'])
   self.assertEqual((q/'study-summary.json.partial').read_bytes(),b'45')

 def test_failure_audit_unknown_names_and_per_file_caps(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);q=root/'qualification';q.mkdir();p=q/'unknown.json';p.write_bytes(b'x')
   with self.assertRaises(ValueError):audit.audit_failure_namespace(root)
   p.unlink();p=q/'study-summary.json'
   with p.open('wb') as target:target.truncate(2097153)
   with self.assertRaisesRegex(ValueError,'per-file'):audit.audit_failure_namespace(root)
   p.unlink();(q/'c072').mkdir()
   with self.assertRaisesRegex(ValueError,'unknown directory'):audit.audit_failure_namespace(root)

 def test_failure_extra_reserve_and_global_object_count(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);trace=root/'qualification/c000/runner/trace';trace.mkdir(parents=True)
   for i in range(7):
    with (trace/f'delegate-{i:03}.json.partial').open('wb') as target:target.truncate(16777216)
   with self.assertRaisesRegex(ValueError,'extra-name'):audit.audit_failure_namespace(root)
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);objects=root/'reference/objects';objects.mkdir(parents=True)
   for i in range(513):(objects/f'{i:064x}.json').write_bytes(b'x')
   with self.assertRaisesRegex(ValueError,'product/component'):audit.audit_failure_namespace(root)

 def test_failure_regular_route_is_source_fixed(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);local=root/'regular/c071/b-c071-r6/runner/trace';local.mkdir(parents=True)
   (local/'parent.json.partial').write_bytes(b'{}')
   result=audit.audit_failure_namespace(root);self.assertEqual(result['extra_names_bytes'],2)
   self.assertEqual(result['categories']['regular.trace'],0)
   (local/'delegate-016.json').write_bytes(b'{}')
   with self.assertRaises(ValueError):audit.audit_failure_namespace(root)

 def test_gate_bundle_byte_and_current_source_rechecks_only(self):
  from eq142_study_records import RECEIPTS
  from eq142_graph import wire,digest
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);refs={};reference={'state':{'files':{'tools/synthetic.py':'a'*64}},'source_inventory':{'fixture':'not_actual_qualification'}}
   for name in RECEIPTS:
    body=b'{"SYNTHETIC_GATE_FIXTURE_NOT_ACTUAL_QUALIFICATION":true}\n';(root/(name+'.json')).write_bytes(body)
    refs[name]={'sha256':digest(body),'bytes':len(body),'retained_name':name+'.json'}
   manifest={'schema':'eq142-reviewed-study-gates:1','canonical_contract_head':'1'*40,'worker_head':'2'*40,
    'source_state_sha256':digest(wire(reference['state'])),'source_inventory_sha256':digest(wire(reference['source_inventory'])),'receipts':refs}
   path=root/'reviewed-gates.json';body=wire(manifest);path.write_bytes(body)
   bundle=gates.ReviewedGateBundle(path,review_approved_manifest_sha256=digest(body))
   with patch.object(gates,'verify_committed_harness') as source_check:
    actual=bundle.check(canonical_contract_head='1'*40,worker_head='2'*40,reference=reference)
    source_check.assert_called_once()
   self.assertEqual(actual,{k:refs[k]['sha256'] for k in RECEIPTS})
   with self.assertRaisesRegex(ValueError,'current source'):bundle.check(canonical_contract_head='1'*40,worker_head='3'*40,reference=reference)
   (root/(RECEIPTS[0]+'.json')).write_bytes(b'x')
   with patch.object(gates,'verify_committed_harness'),self.assertRaisesRegex(ValueError,'receipt changed'):bundle.check(canonical_contract_head='1'*40,worker_head='2'*40,reference=reference)
   with self.assertRaisesRegex(ValueError,'separate review approval'):gates.ReviewedGateBundle(path,review_approved_manifest_sha256='0'*64)

class PortableRoot(unittest.TestCase):
 def owner(self,purpose):
  owner=workspace.DevelopmentWorkspace.__new__(workspace.DevelopmentWorkspace)
  owner._failed=False;owner._owns_root=False;owner._purpose=purpose;owner.admission_ref=None
  return owner
 def test_permanent_qualification_cannot_enter_official_schedule(self):
  owner=self.owner('controller_qualification_only')
  with self.assertRaises(AttributeError):owner.workspace_purpose='official_instrumentation_study'
  with self.assertRaisesRegex(ValueError,'cannot convert'):owner.run_study(None)
  self.assertTrue(owner._failed)
  with self.assertRaisesRegex(ValueError,'prior failure'):owner.run_next_candidate()
 def test_official_without_actual_admission_cannot_start_native(self):
  owner=self.owner('official_instrumentation_study')
  with patch.object(owner,'_run_candidate',side_effect=AssertionError('must not dispatch')) as run:
   with self.assertRaisesRegex(ValueError,'actual study admission'):owner.run_next_candidate()
   run.assert_not_called()
 def test_all_A_order_and_required_baseline_gate_before_B(self):
  owner=self.owner('official_instrumentation_study');owner.admission_ref={}
  owner.matrix={'cohorts':[{'cohort_id':'c000','supported':True,'selector':{}},
   {'cohort_id':'c001','supported':False,'selector':{}},{'cohort_id':'c002','supported':True,'selector':{}}]}
  schedule=owner._schedule();self.assertEqual(len(schedule),36)
  self.assertTrue(all(x[0]=='uninstrumented_A' for x in schedule[:18]));self.assertTrue(all(x[0]=='instrumented_B' for x in schedule[18:]))
  self.assertEqual([(x[2],x[3]) for x in schedule[:9]],[('w',0),('w',1),*[('r',i) for i in range(7)]])
  owner._cursor=18
  with patch.object(owner,'_run_candidate',side_effect=AssertionError('B must not dispatch')) as run:
   with self.assertRaisesRegex(ValueError,'baseline gates must pass'):owner.run_next_candidate()
   run.assert_not_called()
 def test_individual_fixture_requires_both_count_index_and_admission(self):
  owner=self.owner('controller_qualification_only');owner.admission_ref={};owner.index_ref=None
  owner.matrix={'cohorts':[{'cohort_id':'c000','supported':True,'selector':{}}]}
  with self.assertRaisesRegex(ValueError,'real full count qualification'):owner.run_qualification_fixture('c000','uninstrumented_A')
  self.assertTrue(owner._failed)
 def test_available_prefix_bounded_and_wrapper_origin_truthful(self):
  owner=self.owner('controller_qualification_only');owner._available=[]
  for i in range(100):
   owner._remember('sample',{'codec':'native_receipt','sha256':f'{i:064x}','bytes':i+1,'retained_name':'native.json'})
  self.assertEqual(len(owner._available),64)
  owner._available=[]
  owner._remember('sample',{'codec':'eq142_owned_runner_return','sha256':'a'*64,'bytes':10,'retained_name':'runner-reference.json'},derived=True)
  self.assertEqual(owner._available[0]['origin'],'caller_retained_unaccepted_diagnostic')
  self.assertEqual(owner._available[0]['reference']['codec'],'eq142_unaccepted_diagnostic')
 def test_actual_guard_failure_retains_once_and_original_object(self):
  from eq142_failure import controller_guard
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);(root/'qualification').mkdir()
   owner=self.owner('controller_qualification_only');owner.root=root;owner._owns_root=True
   owner._failure_attempted=False;owner._active_directory=root/'qualification';owner._active_sample_id='qualification'
   owner._phase='reference_preparation';owner._available=[];owner._outcome='not_executed'
   primary=ValueError('private')
   @controller_guard
   def fail(self):raise primary
   try:fail(owner)
   except ValueError as caught:self.assertIs(caught,primary)
   else:self.fail('primary failure was swallowed')
   self.assertTrue(owner._failed);self.assertTrue(owner._failure_attempted)
   record=json.loads((root/'qualification/failure.json').read_bytes())
   self.assertEqual(record['error_type']['qualified_name'],'builtins.ValueError')
   self.assertFalse(record['full_sample_accepted']);self.assertNotIn('private',json.dumps(record))
   self.assertFalse(owner.failure_storage_audit['acceptance_authority'])

def product_refs(count,size):
    return {f'{i:064x}':{'codec':'encode_result','sha256':f'{i:064x}',
                        'bytes':size,'retained_name':f'{i:064x}.json'} for i in range(count)}

def trace_refs():
    plan={'reservations':[{'id':'delegate:000','file':'delegate-000.json','max_bytes':16777216,'max_spans':100}]}
    rows=[]
    for identity,name in [('parent','parent.json'),('delegate:000','delegate-000.json')]:
        rows.append({'reservation_id':identity,'codec':'eq142_local_fragment','sha256':digest(b'{}\n'),
            'bytes':3,'retained_name':name,'span_count':0,
            'owned_process_witness':{'codec':'eq142_owned_process_witness','sha256':'a'*64,'bytes':3,'retained_name':name}})
    return plan,rows

class BoundedSets(unittest.TestCase):
    def test_product_total_count_types_and_key_reject_before_any_read(self):
        cases=[product_refs(257,1048576),product_refs(513,1)]
        bad=product_refs(1,1);bad['0'*64]['bytes']=True;cases.append(bad)
        bad=product_refs(1,1);bad['0'*64]['sha256']='a'*64;cases.append(bad)
        with patch.object(loading,'read_proof',side_effect=AssertionError('must reject before body read')):
            for refs in cases:
                with self.assertRaises(ValueError):loading.read_product_set('unused',refs)

    def test_exact256m_declared_boundary_admitted_before_first_read(self):
        class FirstAdmittedRead(Exception):pass
        with patch.object(loading,'read_proof',side_effect=FirstAdmittedRead) as read:
            with self.assertRaises(FirstAdmittedRead):loading.read_product_set('unused',product_refs(256,1048576))
        self.assertEqual(read.call_count,1)

    def test_trace_complete_reservation_and_ref_preflight_before_body_reads(self):
        plan,rows=trace_refs();cases=[]
        bad=copy.deepcopy(plan);bad['reservations']*=4;cases.append((bad,rows))
        bad=copy.deepcopy(plan);bad['reservations'][0]['max_spans']=131072;cases.append((bad,rows))
        bad=copy.deepcopy(rows);bad.reverse();cases.append((plan,bad))
        bad=copy.deepcopy(rows);bad[0]['bytes']=8388609;cases.append((plan,bad))
        bad=copy.deepcopy(rows);bad[0]['span_count']=True;cases.append((plan,bad))
        bad=copy.deepcopy(rows);bad[0]['owned_process_witness']['retained_name']='delegate-000.json';cases.append((plan,bad))
        with patch.object(loading,'read_proof',side_effect=AssertionError('must reject before trace read')):
            for reservation,refs in cases:
                with self.assertRaises(ValueError):loading.read_trace_set('unused',refs,reservation)

    def test_actual_small_body_readback_stays_fixed_and_bounded(self):
        plan,rows=trace_refs()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for row in rows:(root/row['retained_name']).write_bytes(b'{}\n')
            refs,bodies=loading.read_trace_set(root,rows,plan)
            self.assertEqual(list(refs),['parent.json','delegate-000.json'])
            self.assertEqual(bodies,{'parent.json':b'{}\n','delegate-000.json':b'{}\n'})
            body=b'raw original codec bytes';sha=digest(body);(root/(sha+'.json')).write_bytes(body)
            ref={'codec':'encode_result','sha256':sha,'bytes':len(body),'retained_name':sha+'.json'}
            self.assertEqual(loading.read_product_set(root,{sha:ref}),{sha:body})

class PortableControllerAdmission(unittest.TestCase):
 @no_optional_calculator_imports
 def test_original_reference_closed_shape_before_optional_import(self):
  valid={'schema':'eq142-development-reference:1','stage':'qualification_only_not_frozen_study',
   'state':{'files':{},'installed_runtime':{}},'source_inventory':{},'cpu_slots':4,
   'profiles':{PROFILES[0]:{'mathematics':{},'products':{s:[] for s in SERIES}}}}
  self.assertEqual(loading.decode_original_reference(wire(valid)),valid)
  cases=[]
  for value in [True,3,65,'4']:
   bad=copy.deepcopy(valid);bad['cpu_slots']=value;cases.append(wire(bad))
  bad=copy.deepcopy(valid);bad['state']['extra']=0;cases.append(wire(bad))
  bad=copy.deepcopy(valid);bad['profiles']['unknown']=bad['profiles'].pop(PROFILES[0]);cases.append(wire(bad))
  cases += [wire(valid).replace(b'"cpu_slots":4',b'"cpu_slots":4,"cpu_slots":4'),wire(valid)+b'\n']
  for body in cases:
   with self.subTest(body=body):
    with self.assertRaises(ValueError):loading.decode_original_reference(body)

 @no_optional_calculator_imports
 def test_global_preparation_bytes_reject_before_any_product_body(self):
  entries=[];body_by_name={};counter=0
  for profile in PROFILES:
   for series in SERIES:
    records=[]
    for unused in range(16):
     refs=[]
     for codec in ('encode_task','encode_result'):
      sha=f'{counter:064x}';counter+=1
      refs.append({'codec':codec,'sha256':sha,'bytes':1048576,'retained_name':sha+'.json'})
     records.append({'task_sha256':f'{unused:064x}','disposition':{},'task_wire':refs[0],
      'results':[refs[1]],'output':None,'receipt':None,'committed_lookup':'not_applicable',
      'readback_results':[],'diagnostics':[],'raw_diagnostics':{}})
    body=wire(records);name=profile+'-'+series
    body_by_name[name]=body
    entries.append({'profile':profile,'series':series,
     'products_reference':{'codec':'eq142_product_projection','sha256':digest(body),'bytes':len(body),'retained_name':'products.json'},
     'diagnostics_reference':{'codec':'diagnostic_snapshot','sha256':'f'*64,'bytes':1,'retained_name':'diagnostics.json'}})
  self.assertEqual(counter,384) # each pair32MiB; aggregate384MiB exceeds256MiB
  seen=[]
  def metadata_only(directory,ref,*,scope,name):
   self.assertEqual(scope,'reference_preparation');self.assertEqual(name,'products.json')
   seen.append(Path(directory).name);return body_by_name[Path(directory).name]
  with patch.object(preparation,'read_original_reference',return_value={'profiles':{p:{} for p in PROFILES}}),\
       patch.object(preparation,'read_proof',side_effect=metadata_only),\
       patch.object(loading,'read_proof',side_effect=AssertionError('product bodies must not be read')):
   with self.assertRaisesRegex(ValueError,'aggregate declared byte cap'):
    preparation.validate_preparation(Path('synthetic/reference.json'),{},entries)
  self.assertEqual(len(seen),12)

 def test_shared_registration_512_limit_precedes_filesystem_and_copies(self):
  owner=workspace.QualificationWorkspace.__new__(workspace.QualificationWorkspace)
  def ref(i):
   sha=f'{i:064x}';return {'codec':'encode_result','sha256':sha,'bytes':1,'retained_name':sha+'.json'}
  owner.product_refs={f'{i:064x}':ref(i) for i in range(511)}
  last=ref(511)
  with patch.object(owner,'_member') as member:
   owner._objects({last['sha256']:last});member.assert_called_once()
  last['bytes']=2
  self.assertEqual(owner.product_refs[f'{511:064x}']['bytes'],1)
  overflow=ref(512)
  with patch.object(owner,'_member',side_effect=AssertionError('no513th registration')) as member:
   with self.assertRaisesRegex(ValueError,'shared product object count'):owner._objects({overflow['sha256']:overflow})
   member.assert_not_called()
  self.assertEqual(len(owner.product_refs),512)

class ControllerFailureCorrections(unittest.TestCase):
 def fault_owner(self,root):
  owner=workspace.DevelopmentWorkspace.__new__(workspace.DevelopmentWorkspace)
  owner.root=root;owner._purpose='controller_qualification_only';owner._failed=False;owner._owns_root=True
  owner._failure_attempted=False;owner._active_directory=root/'qualification';owner._active_sample_id='qualification'
  owner._phase='create_sample';owner._available=[];owner._outcome='not_executed'
  owner.admission_ref={};owner.index_ref={};owner.reference_return={'sha256':'0'*64}
  owner.matrix={'cohorts':[{'cohort_id':'c000','supported':True,'selector':{'profile':'small',
   'mode':'sequential','degree':1,'series':'eq142_owned_runner_e2e'}}]}
  owner._source={'synthetic_context_only':True};owner.paths={'regular/c000':root/'regular/c000'};owner.children={}
  (root/'qualification').mkdir();(root/'regular/c000').mkdir(parents=True)
  return owner
 def test_new_sample_creation_refusal_uses_owned_qualification_fallback(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);owner=self.fault_owner(root)
   previous=root/'regular/c000/a-c000-w0';previous.mkdir();refused=root/'regular/c000/b-c000-w0';refused.mkdir()
   owner._active_directory=previous;owner._active_sample_id=previous.name
   with self.assertRaises(FileExistsError):owner.run_qualification_fixture('c000','instrumented_B')
   self.assertFalse((previous/'failure.json').exists());self.assertFalse((refused/'failure.json').exists())
   failure=json.loads((root/'qualification/failure.json').read_bytes())
   self.assertEqual(failure['sample_id'],'qualification');self.assertEqual(failure['stage'],'create_sample')
   self.assertEqual(failure['original_outcome_disposition'],'not_executed')
 def test_new_count_cohort_refusal_uses_owned_qualification_fallback(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);owner=self.fault_owner(root);owner.index_ref=None;owner.checked_entries=[]
   previous=root/'regular/c000/a-c000-w0';previous.mkdir();(root/'qualification/c000').mkdir()
   owner._active_directory=previous;owner._active_sample_id=previous.name;owner._source_check=Mock()
   with self.assertRaises(FileExistsError):owner.qualify_counts()
   self.assertFalse((previous/'failure.json').exists())
   failure=json.loads((root/'qualification/failure.json').read_bytes())
   self.assertEqual(failure['sample_id'],'qualification');self.assertEqual(failure['stage'],'count_qualification')
 def test_count_only_allowance_keeps_failure_and_cannot_retry(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);owner=self.fault_owner(root);owner.index_ref=None;owner.checked_entries=[]
   owner._source_check=Mock();owner._capacity=Mock();owner._directory=Mock(return_value='qualification/c000')
   primary=NativeFailure('SYNTHETIC_COUNT_STOP',{'synthetic_count_failure':True})
   with patch.object(eq142_native,'run_owned',side_effect=primary) as native:
    try:owner.qualify_counts()
    except NativeFailure as caught:self.assertIs(caught,primary)
    else:self.fail('count failure was swallowed')
    self.assertEqual(native.call_count,1)
    self.assertEqual(native.call_args.kwargs['timeout_s'],600)
    command=native.call_args.args[0]
    self.assertEqual(command[command.index('--purpose')+1],'qualification_counts')
    self.assertEqual(command[command.index('--control')+1],'instrumented_B')
    with self.assertRaises(ValueError):owner.qualify_counts()
    self.assertEqual(native.call_count,1)
   self.assertIsNone(owner.index_ref);self.assertEqual(owner.checked_entries,[])
   failure=json.loads((root/'qualification/c000/failure.json').read_bytes())
   self.assertEqual(failure['native_code'],'SYNTHETIC_COUNT_STOP')
   self.assertFalse(failure['full_sample_accepted']);self.assertTrue(owner._failed)
 def test_actual_exception_native_payload_is_retained_unaccepted_with_primary_cause(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);owner=self.fault_owner(root)
   payload={'synthetic_native_failure_evidence':'ACTUAL_EXCEPTION_PAYLOAD','full_sample_accepted':False}
   primary=NativeFailure('SYNTHETIC_NATIVE_REFUSAL',payload);cause=OSError('original private cause');primary.__cause__=cause
   owner._source_check=Mock();owner._capacity=Mock(return_value=({},{}));owner._directory=Mock(return_value='regular/c000/a-c000-w0')
   with patch.object(eq142_native,'run_owned',side_effect=primary) as native_call:
    try:owner.run_qualification_fixture('c000','uninstrumented_A')
    except NativeFailure as caught:self.assertIs(caught,primary);self.assertIs(caught.__cause__,cause)
    else:self.fail('primary native failure was lost')
   self.assertEqual(native_call.call_args.kwargs['timeout_s'],120)
   command=native_call.call_args.args[0]
   self.assertEqual(command[command.index('--purpose')+1],'development_candidate')
   local=root/'regular/c000/a-c000-w0'
   self.assertEqual(json.loads((local/'native.json').read_bytes()),payload);self.assertIs(primary.evidence,payload)
   failure=json.loads((local/'failure.json').read_bytes())
   self.assertEqual(failure['original_outcome_disposition'],'actual_failed_report_only')
   self.assertEqual(failure['native_code'],'SYNTHETIC_NATIVE_REFUSAL')
   native=[entry for entry in failure['available_references'] if entry['reference']['retained_name']=='native.json']
   self.assertEqual(len(native),1);self.assertEqual(native[0]['origin'],'caller_retained_unaccepted_diagnostic')
   self.assertEqual(native[0]['reference']['codec'],'eq142_unaccepted_diagnostic')
   self.assertTrue(owner._failure_attempted);self.assertTrue(owner._failed)
 def test_secondary_native_retention_failure_keeps_partial_and_original(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);owner=self.fault_owner(root)
   primary=NativeFailure('SYNTHETIC_NATIVE_REFUSAL',{'actual_synthetic_evidence':True})
   cause=OSError('original');primary.__cause__=cause
   owner._source_check=Mock();owner._capacity=Mock(return_value=({},{}));owner._directory=Mock(return_value='regular/c000/a-c000-w0')
   def failed_write(path,value,*,max_bytes):
    self.assertEqual(path.name,'native.json');self.assertEqual(max_bytes,16777216)
    path.with_name(path.name+'.partial').write_bytes(b'partial');raise OSError('secondary payload write')
   with patch.object(eq142_native,'run_owned',side_effect=primary),patch.object(workspace,'persist_record',side_effect=failed_write):
    try:owner.run_qualification_fixture('c000','uninstrumented_A')
    except NativeFailure as caught:self.assertIs(caught,primary);self.assertIs(caught.__cause__,cause)
    else:self.fail('primary native failure was lost')
   local=root/'regular/c000/a-c000-w0'
   self.assertEqual((local/'native.json.partial').read_bytes(),b'partial')
   self.assertFalse((local/'native.json').exists())
   failure=json.loads((local/'failure.json').read_bytes())
   self.assertEqual(failure['original_outcome_disposition'],'unknown')
   self.assertEqual(failure['native_code'],'SYNTHETIC_NATIVE_REFUSAL')
   self.assertEqual(failure['available_references'],[])

class CommittedGateSource(unittest.TestCase):
 def test_actual_git_head_tracked_membership_current_bytes_and_dirty_refusal(self):
  with tempfile.TemporaryDirectory(prefix='eq142-gate-source-') as temporary:
   root=Path(temporary).resolve();self.assertEqual(root.parent,Path(tempfile.gettempdir()).resolve())
   (root/'tools').mkdir();file=root/'tools/synthetic.py';file.write_bytes(b'synthetic=1\n')
   def git(*args):
    return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.PIPE).decode().strip()
   git('init','--quiet');git('add','tools/synthetic.py')
   git('-c','user.name=Atul Srivastava','-c','user.email=102820540+atulsrivas1@users.noreply.github.com',
       'commit','--quiet','-m','Synthetic committed-source admission fixture')
   head=git('rev-parse','HEAD');files={'tools/synthetic.py':hashlib.sha256(file.read_bytes()).hexdigest()}
   gates.verify_committed_harness(root,head,files)
   with self.assertRaisesRegex(ValueError,'worker head changed'):gates.verify_committed_harness(root,'0'*40,files)
   file.write_bytes(b'synthetic=2\n')
   with self.assertRaisesRegex(ValueError,'source file changed'):gates.verify_committed_harness(root,head,files)
   current={'tools/synthetic.py':hashlib.sha256(file.read_bytes()).hexdigest()}
   with self.assertRaisesRegex(ValueError,'committed harness'):gates.verify_committed_harness(root,head,current)
   file.write_bytes(b'synthetic=1\n');untracked=root/'tools/untracked.py';untracked.write_bytes(b'untracked=1\n')
   with self.assertRaises((ValueError,subprocess.CalledProcessError)):
    gates.verify_committed_harness(root,head,{**files,'tools/untracked.py':hashlib.sha256(untracked.read_bytes()).hexdigest()})
   with self.assertRaisesRegex(ValueError,'relative path'):
    gates.verify_committed_harness(root,head,{'tools/../outside.py':'0'*64})

@unittest.skipUnless(HAVE_INTRADAY,'Original optional intraday distribution required')
@unittest.skipUnless(os.name=='nt','Owned complete controller requires Windows')
class ActualCompleteRoot(unittest.TestCase):
    def test_full_supported_count_matrix_and_six_actual_native_candidates(self):
        require_cpu_slots(self,5)
        from benchmark_execution_baseline import source_state
        from eq142_evidence import persist_record
        from eq142_proofs import read_proof,decode_record
        from eq142_envelope import verify_candidate
        root=Path(tempfile.mkdtemp(prefix='eq142-complete-root-test-')).resolve()
        if root.parent!=Path(tempfile.gettempdir()).resolve() or not root.name.startswith('eq142-complete-root-test-'):
            raise ValueError('test owned root identity')
        print('Retained complete controller correctness root: '+str(root),file=sys.stderr)
        before=source_state()
        owner=workspace.DevelopmentWorkspace(root/'study',workspace_purpose='controller_qualification_only')
        owner.qualify_counts();owner.admit();result=[]
        self.assertEqual(len(owner.matrix['cohorts']),72)
        supported=[row for row in owner.matrix['cohorts'] if row['supported']]
        self.assertEqual(len(owner.checked_entries),len(supported))
        for mode,degree in [('sequential',1),('thread',2),('process',2)]:
            rows=[row for row in owner.matrix['cohorts'] if row['selector']=={
                'profile':'small','mode':mode,'degree':degree,'series':'eq142_owned_runner_e2e'}]
            self.assertEqual(len(rows),1);self.assertTrue(rows[0]['supported'])
            for control_name in ('uninstrumented_A','instrumented_B'):
                candidate=owner.run_qualification_fixture(rows[0]['cohort_id'],control_name)
                self.assertFalse(candidate['full_sample_accepted']);result.append(candidate)
                entry=owner._history[-1];directory=owner.paths[entry['key']]
                body=decode_record(read_proof(directory,entry['candidate_reference'],scope='sample',name='candidate.json'),maximum=4096)
                closed_root=decode_record(read_proof(directory,entry['root_reference'],scope='sample',name='complete-root.json'),maximum=65536)
                body['full_sample_accepted']=True
                with self.assertRaises(ValueError):
                    verify_candidate(body,root=closed_root,root_reference=entry['root_reference'],
                        measurement_reference=entry['measurement_reference'],final_source_sha256=digest(wire(before)))
        owner.replay_count_qualification();owner.journal.reconcile(owner.children);owner._source_check()
        self.assertEqual(source_state(),before)
        self.assertFalse(owner.admission['official_timing_authority'])
        self.assertFalse(owner.admission['all_actual_gates_verified'])
        self.assertFalse((root/'study/qualification/study-summary.json').exists())
        persist_record(root/'qualification-check.json',{'schema':'eq142-controller-test:1',
            'stage':'actual_full_supported_count_and_six_native_fixtures',
            'source_before_sha256':digest(wire(before)),'source_after_sha256':digest(wire(source_state())),
            'matrix_sha256':owner.matrix_sha256,'supported_cohorts':len(supported),
            'count_qualification_reference':owner.index_ref,'admission_reference':owner.admission_ref,
            'fixtures':result,'official_timing_authority':False,'full_sample_accepted':False},max_bytes=1048576)
