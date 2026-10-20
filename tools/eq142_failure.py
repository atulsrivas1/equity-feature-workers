"""V9 bounded failure record and one-attempt controller guard.

Private retained diagnostics only. References are available prefixes; they
never grant successful proof, namespace ownership or writer provenance.
"""
from functools import wraps
from pathlib import Path
import re
from eq142_graph import wire
from eq142_proofs import validate_reference,REGISTRY
from eq142_owned_runner import PROFILES,SERIES
from eq142_evidence import persist_record

PHASES=('initial_source','create_workspace','capacity_freeze','reference_preparation','reference_readback',
 'count_qualification','count_index_replay','study_admission','create_sample','sample_source_before',
 'sample_capacity_before','native_owned_call','native_receipt','sample_evidence_readback','complete_root',
 'sample_capacity_after','sample_source_after','measurement_envelope','final_source_check',
 'candidate_finalization','study_A_baseline_check','study_history_replay','study_summary')
PREPARATIONS={f'prep:{p}:{s}' for p in PROFILES for s in SERIES}
ORIGINS=('actual_writer_return','caller_retained_unaccepted_diagnostic')
OUTCOMES=('not_executed','actual_returned','actual_failed_report_only','unknown')

def error_identity(error):
 cls=type(error)
 module=type.__getattribute__(cls,'__module__');name=type.__getattribute__(cls,'__qualname__')
 if type(module) is not str or type(name) is not str:raise ValueError('failure type identity')
 # Existing class metadata may be huge. Never copy the complete identity just
 # to truncate it; construct only its first256 codepoints.
 total=len(module)+1+len(name)
 prefix=module[:256]
 if len(prefix)<256:prefix+='.'+name[:255-len(prefix)]
 return {'qualified_name':prefix,'truncated':total>256}

def check_error_identity(value):
 if (type(value) is not dict or set(value)!={'qualified_name','truncated'}
  or type(value['qualified_name']) is not str or not 1<=len(value['qualified_name'])<=256
  or type(value['truncated']) is not bool
  or value['truncated'] and len(value['qualified_name'])!=256):raise ValueError('failure type closed identity')

def check_available(entry):
 if type(entry) is not dict or set(entry)!={'origin','context','reference'} or entry['origin'] not in ORIGINS:
  raise ValueError('failure available reference envelope')
 context=entry['context'];ref=entry['reference']
 if type(context) is not str or context not in set(REGISTRY)|PREPARATIONS:raise ValueError('failure fixed logical context')
 if entry['origin']=='actual_writer_return':
  scope='reference_preparation' if context in PREPARATIONS else context
  validate_reference(ref,scope=scope,name=ref.get('retained_name') if type(ref) is dict else None)
  if scope=='sample' and ref['codec']=='eq142_owned_runner_return':
   raise ValueError('wrapper readback is never observed writer return')
 else:
  if (type(ref) is not dict or set(ref)!={'codec','sha256','bytes','retained_name'}
   or ref['codec']!='eq142_unaccepted_diagnostic' or type(ref['sha256']) is not str
   or re.fullmatch('[0-9a-f]{64}',ref['sha256']) is None or type(ref['bytes']) is not int
   or not 0<=ref['bytes']<=16777216 or type(ref['retained_name']) is not str):
   raise ValueError('failure unaccepted diagnostic reference')
  name=ref['retained_name'];base=name[:-8] if name.endswith('.partial') else name
  scope='reference_preparation' if context in PREPARATIONS else context
  if not any(re.fullmatch(pattern,base) for maximum,pattern in REGISTRY[scope].values()):
   raise ValueError('failure unaccepted fixed basename')

def failure_record(error,*,sample_id,stage,available_references,original_outcome_disposition):
 from supplied_native import NativeFailure
 native_code=error.code if isinstance(error,NativeFailure) else None
 value={'schema':'eq142-complete-root-failure:1','sample_id':sample_id,'stage':stage,
  'error_type':error_identity(error),'native_code':native_code,
  'available_references':available_references,'original_outcome_disposition':original_outcome_disposition,
  'full_sample_accepted':False}
 validate_failure(value)
 return value

def validate_failure(value):
 if type(value) is not dict or set(value)!={'schema','sample_id','stage','error_type','native_code',
  'available_references','original_outcome_disposition','full_sample_accepted'}:
  raise ValueError('failure closed record')
 if (value['schema']!='eq142-complete-root-failure:1' or value['stage'] not in PHASES
  or type(value['sample_id']) is not str or re.fullmatch('[a-z][a-z0-9_.-]{0,127}',value['sample_id']) is None
  or value['original_outcome_disposition'] not in OUTCOMES or value['full_sample_accepted'] is not False):
  raise ValueError('failure fixed schema')
 check_error_identity(value['error_type'])
 if value['native_code'] is not None and (type(value['native_code']) is not str
  or re.fullmatch('[A-Z][A-Z0-9_]{0,127}',value['native_code']) is None):
  raise ValueError('failure native code')
 refs=value['available_references']
 if type(refs) is not list or len(refs)>64:raise ValueError('failure available prefix cap')
 for entry in refs:check_available(entry)
 if len(wire(value))>1048576:raise ValueError('failure encoded cap')

def controller_guard(method):
 """One attempt across nested handlers; preserve original identity and cause."""
 @wraps(method)
 def guarded(self,*args,**kwargs):
  if getattr(self,'_failed',False):raise ValueError('controller prior failure retained; no reuse')
  try:return method(self,*args,**kwargs)
  except BaseException as original:
   self._failed=True
   if getattr(self,'_owns_root',False) and not getattr(self,'_failure_attempted',False):
    self._failure_attempted=True # before capture or write, including failing capture
    try:self._retain_failure(original)
    except BaseException:
     try:BaseException.add_note(original,'Controller failure diagnostic could not be completed; original failure retained.')
     except BaseException:pass
   raise
 return guarded

def retain_failure(directory,value):
 """Owning controller selects its already exclusive fixed sample directory."""
 validate_failure(value);directory=Path(directory).absolute()
 for p in (directory,*directory.parents):
  if p.is_symlink() or p.is_junction():raise ValueError('failure namespace indirection')
 if not directory.is_dir():raise ValueError('failure fixed directory absent')
 return {'codec':'eq142_complete_root_failure',**persist_record(directory/'failure.json',value,max_bytes=1048576)}
