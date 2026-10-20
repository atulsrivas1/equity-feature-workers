"""Bounded stat-only failed-workspace accounting, never acceptance or cleanup.

All raw names remain retained. Logical file lengths are not physical allocation
or a snapshot guarantee. The controller poisons reuse before this diagnostic.
"""
from pathlib import Path
import os,re
from eq142_owned_runner import PROFILES,SERIES
from eq142_namespace import CATEGORY_MAX

RESERVE=109051904
QUAL_NORMAL={'trace':4831838208,'native':1207959552,'summary':150994944,
 'profiles':80216064,'witnesses':5013504,'returns':294912,'capacity':2359296,
 'other_runner':14155776,'reference_and_index':33554432}
PREP={p+'-'+s for p in PROFILES for s in SERIES}
INVOKE=r'(parent|delegate-(?:00[0-9]|01[0-5]))[.]json'

def cohort(name):return re.fullmatch(r'c(?:0[0-6][0-9]|07[01])',name) is not None

def directory_allowed(parts):
 if not parts:return True
 first=parts[0]
 if first=='reference':
  return len(parts)==1 or len(parts)==2 and parts[1] in PREP|{'objects'}
 if first=='qualification':
  if len(parts)==1:return True
  sample=parts[1:]
 elif first=='regular':
  if len(parts)==1:return True
  if not cohort(parts[1]):return False
  if len(parts)==2:return True
  if re.fullmatch('[ab]-'+parts[1]+r'-(?:w[01]|r[0-6])',parts[2]) is None:return False
  sample=parts[1:2]+parts[3:]
 else:return False
 if not sample or not cohort(sample[0]):return False
 if len(sample)==1:return True
 if sample[1]!='runner':return False
 if len(sample)==2:return True
 return len(sample)==3 and sample[2] in ({'trace','owned-processes','code-counts'} if first=='qualification' else {'trace','owned-processes'})

def file_policy(parts):
 """Return original category/component/per-file max and partial eligibility."""
 directory=parts[:-1];name=parts[-1]
 if not directory_allowed(directory):raise ValueError('failure audit fixed directory')
 partial=name.endswith('.partial');base=name[:-8] if partial else name
 if directory==('reference',):
  if base!='reference.json':raise ValueError('failure audit reference name')
  return ('qualification','reference_and_index',16777216,partial,base)
 if len(directory)==2 and directory[0]=='reference':
  if directory[1]=='objects':
   if partial or re.fullmatch('[0-9a-f]{64}[.]json',base) is None:raise ValueError('failure audit product name')
   return ('shared.products',None,1048576,False,base)
  if base not in ('products.json','diagnostics.json') or partial and base=='diagnostics.json':
   raise ValueError('failure audit preparation name')
  return ('qualification','reference_and_index',65536 if base=='products.json' else 1048576,partial,base)
 if directory==('qualification',):
  metadata={'qualification-capacity.json':16384,'count-qualification.json':1048576,
   'study-admission.json':16384,'study-summary.json':2097152,'failure.json':1048576}
  if base not in metadata:raise ValueError('failure audit metadata name')
  return ('qualification','reference_and_index',metadata[base],partial,base)
 if not directory or directory[0] not in ('qualification','regular'):raise ValueError('failure audit file location')
 qualification=directory[0]=='qualification'
 tail=directory[2:] if qualification else directory[3:]
 if not tail:
  files={'native.json':('native',16777216),'runner-reference.json':('returns',4096),
   'capacity-before.json':('capacity',16384),'capacity-after.json':('capacity',16384),
   'complete-root.json':('summary',65536),'measurement.json':('summary',65536),
   'candidate.json':('summary',4096),'failure.json':('summary',1048576)}
  if base not in files or qualification and base in ('complete-root.json','measurement.json','candidate.json'):
   raise ValueError('failure audit sample file')
  component,maximum=files[base]
 elif tail==('runner',):
  files={'runner.json':('summary',1048576),'diagnostics.json':('summary',1048576),
   'products.json':('other_runner',65536),'supervisor.json':('other_runner',65536),'publication.json':('other_runner',65536)}
  if base not in files or partial and base=='diagnostics.json':raise ValueError('failure audit runner name')
  component,maximum=files[base]
 elif len(tail)==2 and tail[0]=='runner':
  if re.fullmatch(INVOKE,base) is None:raise ValueError('failure audit invocation name')
  if tail[1]=='trace':component,maximum='trace',8388608 if base=='parent.json' else 16777216
  elif tail[1]=='owned-processes':component,maximum='witnesses',4096
  elif tail[1]=='code-counts' and qualification:component,maximum='profiles',65536
  else:raise ValueError('failure audit invocation directory')
 else:raise ValueError('failure audit file depth')
 category='qualification' if qualification else (
  'regular.trace' if component=='trace' else 'regular.native' if component=='native' else
  'regular.diagnostics' if base=='diagnostics.json' else 'regular.summary' if component=='summary' else 'regular.metadata')
 return category,component if qualification else None,maximum,partial,base

def audit_failure_namespace(root,*,pending_failure_allowance=0):
 if type(pending_failure_allowance) is not int or not 0<=pending_failure_allowance<=2097152:
  raise ValueError('failure audit pending diagnostic bound')
 root=Path(root).absolute()
 for p in (root,*root.parents):
  if p.is_symlink() or p.is_junction():raise ValueError('failure audit root indirection')
 categories={k:0 for k in CATEGORY_MAX};components={k:0 for k in QUAL_NORMAL}
 extras=pending_failure_allowance;pending=[root];directories=0;files=0;products=0
 while pending:
  directory=pending.pop();directories+=1
  if directories>10000:raise ValueError('failure audit directory bound')
  with os.scandir(directory) as members:
   for entry in members:
    path=directory/entry.name;parts=path.relative_to(root).parts
    if entry.is_symlink() or path.is_junction():raise ValueError('failure audit member indirection')
    if entry.is_dir(follow_symlinks=False):
     if not directory_allowed(parts):raise ValueError('failure audit unknown directory')
     pending.append(path);continue
    files+=1
    if files>100000 or not entry.is_file(follow_symlinks=False):raise ValueError('failure audit file bound/type')
    category,component,maximum,partial,base=file_policy(parts)
    size=entry.stat(follow_symlinks=False).st_size
    if not 0<=size<=maximum:raise ValueError('failure audit per-file encoded cap')
    if partial or base=='failure.json':extras+=size
    else:
     categories[category]+=size
     if component is not None:components[component]+=size
     if category=='shared.products':products+=1
 if products>512 or extras>RESERVE or any(components[k]>cap for k,cap in QUAL_NORMAL.items()):
  raise ValueError('failure audit product/component/extra-name cap')
 categories['qualification']+=extras
 if any(categories[k]>cap for k,cap in CATEGORY_MAX.items()):raise ValueError('failure audit category cap')
 total=sum(categories.values());regular=sum(v for k,v in categories.items() if k!='qualification')
 if regular>69289902080 or total>77309411328:raise ValueError('failure audit hard encoded cap')
 return {'categories':categories,'qualification_normal_components':components,'extra_names_bytes':extras,
  'total_bytes_including_pending_allowance':total,'files_observed':files,'directories_observed':directories,
  'physical_allocation_guaranteed':False,'atomic_snapshot':False,'acceptance_authority':False}
