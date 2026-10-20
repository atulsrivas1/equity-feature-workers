"""Exact prospective V9 summary arithmetic; trusted raw proof context required.

No I/O, calculators, probes, admission or proof authority. The controller must
independently verify all nine original samples before supplying observations.
"""
from fractions import Fraction
import re
from eq142_graph import wire
from eq142_proofs import validate_reference

INT63=9223372036854775807
SAFE=9007199254740991
GIB=1073741824
OBSERVABLE=frozenset(('manifest.canonical','manifest.encode_task','manifest.encode_output',
 'sdk.encode_result','sdk.encode_receipt','supervisor.pickle_size','config.to_json','context.to_json'))
GATES=('A_wall_noise','B_wall_noise','median_wall','median_owned_CPU','observed_RSS',
 'small_focused_p95','full_product_parity','B_source_counts','source_capacity_clock_evidence')

def integer(value,maximum,*,minimum=0):
 if type(value) is not int or not minimum<=value<=maximum:raise ValueError('statistics integer bound')
 return value

def decimal(value,maximum,*,minimum=0):
 if type(value) is not str or re.fullmatch('0|[1-9][0-9]*',value) is None or len(value)>len(str(maximum)):
  raise ValueError('statistics canonical decimal')
 return integer(int(value),maximum,minimum=minimum)

def metric(values,maximum,*,minimum=0):
 if type(values) is not list or len(values)!=7:raise ValueError('statistics seven retained values')
 ordered=sorted(integer(x,maximum,minimum=minimum) for x in values)
 iqr=Fraction(ordered[4]+ordered[5]-ordered[1]-ordered[2],2)
 return {'median':str(ordered[3]),'inclusive_IQR':{'numerator':str(iqr.numerator),'denominator':iqr.denominator},'p95':str(ordered[6])}

def noise_pass(summary):
 # Internal metric output still gets closed strict checks when recomputed.
 median=decimal(summary['median'],INT63,minimum=1);q=summary['inclusive_IQR']
 numerator=decimal(q['numerator'],2*INT63)
 denominator=integer(q['denominator'],2,minimum=1)
 return 30*numerator<=median*denominator

def control_summary(cohort_id,control,samples):
 if control not in ('uninstrumented_A','instrumented_B') or type(samples) is not list or len(samples)!=9:
  raise ValueError('statistics exact control population')
 prefix='a' if control=='uninstrumented_A' else 'b'
 refs=[];walls=[];cpus=[];rss=[]
 for sample,(kind,index) in zip(samples,[('w',0),('w',1),*[('r',i) for i in range(7)]]):
  if type(sample) is not dict or set(sample)!={'sample_id','candidate_reference','wall_ns','user_ticks_100ns','kernel_ticks_100ns','sampled_aggregate_RSS_bytes'}:
   raise ValueError('statistics closed raw sample projection')
  if sample['sample_id']!=f'{prefix}-{cohort_id}-{kind}{index}':raise ValueError('statistics fixed sample order')
  validate_reference(sample['candidate_reference'],scope='sample',name='candidate.json')
  if sample['candidate_reference']['codec']!='eq142_complete_candidate':raise ValueError('statistics candidate codec')
  wall=decimal(sample['wall_ns'],INT63,minimum=1)
  cpu=decimal(sample['user_ticks_100ns'],INT63)+decimal(sample['kernel_ticks_100ns'],INT63)
  memory=integer(sample['sampled_aggregate_RSS_bytes'],GIB)
  refs.append({'sample_id':sample['sample_id'],'candidate_reference':dict(sample['candidate_reference'])})
  if kind=='r':walls.append(wall);cpus.append(cpu);rss.append(memory)
 return {'warmups':refs[:2],'retained':refs[2:],'wall_ns':metric(walls,INT63,minimum=1),
  'owned_CPU_ticks_100ns':metric(cpus,2*INT63),'max_retained_sampled_aggregate_RSS_bytes':max(rss),
  'all_nine_absolute_bounds_and_proofs_valid':True}

def baseline_reasons(A):
 reasons=[]
 if not noise_pass(A['wall_ns']):reasons.append('A_wall_noise')
 if decimal(A['owned_CPU_ticks_100ns']['median'],2*INT63)==0:reasons.append('A_median_CPU_zero')
 if integer(A['max_retained_sampled_aggregate_RSS_bytes'],GIB)==0:reasons.append('A_RSS_zero')
 return sorted(reasons)

def operation_totals(operations,spans):
 """One raw validated sample at a time; never retain seven full trace bodies."""
 if type(operations) is not list or len(operations)!=47 or len(set(operations))!=47:
  raise ValueError('statistics fixed47 source operations')
 if type(spans) is not list or len(spans)>131072:raise ValueError('statistics span population bound')
 values={op:[0,0,0 if op in OBSERVABLE else None] for op in operations};seen=set()
 for span in spans:
  key=(span['reservation_id'],span['id'])
  if key in seen:raise ValueError('statistics duplicate original span')
  seen.add(key);op=span['operation']
  if op not in values:raise ValueError('statistics unknown source operation')
  start=decimal(span['start_ns'],INT63);end=decimal(span['end_ns'],INT63)
  if end<start:raise ValueError('statistics local clock reversal')
  value=values[op];value[0]+=1;value[1]+=end-start
  observed=span['observed_bytes']
  if op in OBSERVABLE:
   if span['status']!='returned' or observed is None or span['byte_observation']=='unavailable':
    raise ValueError('statistics incomplete observable return')
   value[2]+=integer(observed,SAFE)
   if value[2]>SAFE:raise ValueError('statistics observed return byte sum bound')
  elif observed is not None or span['byte_observation']!='unavailable':
   raise ValueError('statistics source-unobserved bytes')
 return values

def summarize_operation_totals(operations,populations):
 if type(operations) is not list or len(operations)!=47 or len(set(operations))!=47:
  raise ValueError('statistics fixed47 source operations')
 if type(populations) is not list or len(populations)!=7:raise ValueError('statistics seven compact operation populations')
 for population in populations:
  if type(population) is not dict or set(population)!=set(operations):raise ValueError('statistics compact operation set')
  for op,values in population.items():
   if type(values) is not list or len(values)!=3:raise ValueError('statistics compact operation row')
   integer(values[0],131072);integer(values[1],131072*INT63)
   if op in OBSERVABLE:integer(values[2],SAFE)
   elif values[2] is not None:raise ValueError('statistics compact unobserved bytes')
  if sum(values[0] for values in population.values())>131072:raise ValueError('statistics compact span count')
 output=[]
 for op in operations:
  entries=[v[op][0] for v in populations];durations=metric([v[op][1] for v in populations],131072*INT63)
  byte_values=metric([v[op][2] for v in populations],SAFE) if op in OBSERVABLE else None
  output.append({'operation':op,'call_entries_min':min(entries),'call_entries_max':max(entries),
   'median_sum_local_duration_ns':durations['median'],'p95_sum_local_duration_ns':durations['p95'],
   'median_observed_return_bytes':None if byte_values is None else byte_values['median'],
   'p95_observed_return_bytes':None if byte_values is None else byte_values['p95']})
 return output

def operation_summaries(operations,retained_spans):
 """Input is seven flattened populations from fully validated raw fragments.

 Each original span appears once; local inclusive sums may overlap. No span
 duration is a cross-process interval or an exclusive numerical cost.
 """
 if type(operations) is not list or len(operations)!=47 or len(set(operations))!=47:
  raise ValueError('statistics fixed47 source operations')
 if type(retained_spans) is not list or len(retained_spans)!=7:raise ValueError('statistics seven span populations')
 return summarize_operation_totals(operations,[operation_totals(operations,spans) for spans in retained_spans])

def cohort_summary(row,A,B,operations,*,before_B=False):
 """Summaries must be freshly derived from actual complete raw history."""
 if type(before_B) is not bool:raise ValueError('statistics stage flag')
 if type(row) is not dict or set(row)!={'cohort_id','selector','supported','omission_reason'}:
  raise ValueError('statistics source matrix row')
 if type(row['supported']) is not bool:raise ValueError('statistics support type')
 gates={gate:None for gate in GATES};reasons=[]
 if not row['supported']:
  if A is not None or B is not None or operations!=[]:raise ValueError('statistics omitted observations')
 elif A is None or (B is None)!=before_B:raise ValueError('statistics stage control coverage')
 else:
  reasons=baseline_reasons(A);gates['A_wall_noise']=noise_pass(A['wall_ns'])
  gates['source_capacity_clock_evidence']=True
  if before_B:
   if operations!=[]:raise ValueError('statistics before-B operations')
  else:
   if len(operations)!=47:raise ValueError('statistics complete operation population')
   gates.update({'B_wall_noise':noise_pass(B['wall_ns']),
    'median_wall':20*int(B['wall_ns']['median'])<=21*int(A['wall_ns']['median']),
    'median_owned_CPU':int(A['owned_CPU_ticks_100ns']['median'])>0 and 4*int(B['owned_CPU_ticks_100ns']['median'])<=5*int(A['owned_CPU_ticks_100ns']['median']),
    'observed_RSS':A['max_retained_sampled_aggregate_RSS_bytes']>0 and B['max_retained_sampled_aggregate_RSS_bytes']<=GIB and 4*B['max_retained_sampled_aggregate_RSS_bytes']<=5*A['max_retained_sampled_aggregate_RSS_bytes'],
    'small_focused_p95':10*int(B['wall_ns']['p95'])<=11*int(A['wall_ns']['p95']) if row['selector']['profile'] not in ('balanced','skewed') else None,
    'full_product_parity':True,'B_source_counts':True,'source_capacity_clock_evidence':True})
   for gate,reason in [('B_wall_noise','B_wall_noise'),('median_wall','wall_overhead'),
    ('median_owned_CPU','CPU_overhead'),('observed_RSS','RSS_overhead'),('small_focused_p95','small_focused_p95')]:
    if gates[gate] is False:reasons.append(reason)
 result={**row,'A':A,'B':B,'gates':gates,'failure_reasons':sorted(set(reasons)),'operations':operations,
  'attributed_cost_accepted':row['supported'] and not before_B and not reasons}
 if len(wire(result))>24576:raise ValueError('statistics cohort wire cap')
 return result
