"""Recheck an externally reviewed, source-matched pre-timing receipt bundle.

The supplied approval SHA must come from the separate completed gate review.
This reader checks identities and retained bytes; it does NOT infer completed
qualification from receipt booleans or issue its own review approval. Passing
the hash of an unreviewed manifest cannot satisfy the external review gate.
"""
from pathlib import Path
import json,re,subprocess
from eq142_graph import wire,digest
from eq142_study_records import RECEIPTS,hexadecimal

NAMES={name:name+'.json' for name in RECEIPTS}

def bounded_body(path,maximum):
 path=Path(path).absolute()
 for p in (path,*path.parents):
  if p.is_symlink() or p.is_junction():raise ValueError('gate receipt indirection')
 if not path.is_file() or not 1<=path.stat().st_size<=maximum:raise ValueError('gate receipt body bound')
 with path.open('rb') as source:body=source.read(maximum+1)
 if not 1<=len(body)<=maximum:raise ValueError('gate receipt body changed')
 return body

def verify_committed_harness(root,worker_head,files):
 """Actual current source bytes, tracked membership and HEAD-normalized diff."""
 root=Path(root).absolute()
 if type(files) is not dict or not 1<=len(files)<=512:raise ValueError('gate committed source population')
 names=sorted(files)
 for name,expected in files.items():
  if (type(name) is not str or re.fullmatch(r'(?:tools|tests)/[A-Za-z0-9_./-]+',name) is None
   or any(part in ('.','..','') for part in name.split('/'))):raise ValueError('gate committed source relative path')
  hexadecimal(expected,64)
  if digest(bounded_body(root/name,16777216))!=expected:raise ValueError('gate actual source file changed')
 def git(*args):return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.PIPE).decode().strip()
 if git('rev-parse','HEAD')!=worker_head:raise ValueError('gate actual committed worker head changed')
 tracked=git('ls-files','--error-unmatch','--',*names).splitlines()
 if set(tracked)!=set(names):raise ValueError('gate source must be tracked at current head')
 if subprocess.run(['git','-C',str(root),'diff','--quiet','HEAD','--',*names],stdout=subprocess.PIPE,stderr=subprocess.PIPE).returncode!=0:
  raise ValueError('gate source differs from actual committed harness')

class ReviewedGateBundle:
 """A source/byte recheck for a specifically externally approved manifest."""
 def __init__(self,path,*,review_approved_manifest_sha256):
  hexadecimal(review_approved_manifest_sha256,64)
  self.path=Path(path).absolute();self.approved=review_approved_manifest_sha256
  body=bounded_body(self.path,65536)
  if digest(body)!=self.approved:raise ValueError('gate manifest differs from separate review approval')
  def pairs(items):
   result={}
   for k,v in items:
    if k in result:raise ValueError('gate manifest duplicate field')
    result[k]=v
   return result
  d=json.loads(body,object_pairs_hook=pairs)
  if (type(d) is not dict or set(d)!={'schema','canonical_contract_head','worker_head','source_state_sha256','source_inventory_sha256','receipts'}
   or d['schema']!='eq142-reviewed-study-gates:1'):raise ValueError('gate manifest closed schema')
  for key in ('canonical_contract_head','worker_head'):hexadecimal(d[key],40)
  for key in ('source_state_sha256','source_inventory_sha256'):hexadecimal(d[key],64)
  if type(d['receipts']) is not dict or set(d['receipts'])!=set(RECEIPTS):raise ValueError('gate four actual receipt identities')
  for name,ref in d['receipts'].items():
   if type(ref) is not dict or set(ref)!={'sha256','bytes','retained_name'} or ref['retained_name']!=NAMES[name]:
    raise ValueError('gate fixed receipt filename')
   hexadecimal(ref['sha256'],64)
   if type(ref['bytes']) is not int or not 1<=ref['bytes']<=16777216:raise ValueError('gate receipt declared bound')
  self._body=body;self.manifest=d

 def check(self,*,canonical_contract_head,worker_head,reference):
  from eq142_fixture import ROOT
  if bounded_body(self.path,65536)!=self._body:raise ValueError('reviewed gate manifest changed')
  expected={'canonical_contract_head':canonical_contract_head,'worker_head':worker_head,
   'source_state_sha256':digest(wire(reference['state'])),'source_inventory_sha256':digest(wire(reference['source_inventory']))}
  if any(self.manifest[k]!=v for k,v in expected.items()):raise ValueError('gate receipts do not match actual current source')
  verify_committed_harness(ROOT,worker_head,reference['state']['files'])
  for name,ref in self.manifest['receipts'].items():
   body=bounded_body(self.path.parent/NAMES[name],ref['bytes'])
   if len(body)!=ref['bytes'] or digest(body)!=ref['sha256']:raise ValueError('externally reviewed actual gate receipt changed')
  return {name:self.manifest['receipts'][name]['sha256'] for name in RECEIPTS}
