"""Explicit V9 controller qualification or separately gated official study.

No automatic adoption. Raw owned workspaces remain private; performance results
require the separately reviewed public derivative. All failed roots are kept.
"""
import argparse,json,os,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]

def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('action',choices=('qualify','official'))
 parser.add_argument('--directory',required=True)
 parser.add_argument('--reviewed-gate-manifest')
 parser.add_argument('--review-approved-manifest-sha256')
 args=parser.parse_args()
 if os.name!='nt':raise ValueError('Owned complete controller requires qualified64bit Windows; pure readers are portable')
 if args.action=='official':
  if not args.reviewed_gate_manifest or not args.review_approved_manifest_sha256:
   parser.error('official requires actual separately reviewed gate manifest and its approved SHA256')
  from eq142_gate_bundle import ReviewedGateBundle
  gates=ReviewedGateBundle(args.reviewed_gate_manifest,review_approved_manifest_sha256=args.review_approved_manifest_sha256)
 else:
  if args.reviewed_gate_manifest or args.review_approved_manifest_sha256:parser.error('qualification cannot acquire official gates')
  gates=None
 from eq142_workspace import DevelopmentWorkspace
 workspace=DevelopmentWorkspace(Path(args.directory),workspace_purpose=(
  'official_instrumentation_study' if args.action=='official' else 'controller_qualification_only'))
 if args.action=='official':result=workspace.run_study(gates)
 else:
  workspace.qualify_counts();workspace.admit();fixtures=[]
  for mode,degree in [('sequential',1),('thread',2),('process',2)]:
   matches=[row for row in workspace.matrix['cohorts'] if row['selector']=={
    'profile':'small','mode':mode,'degree':degree,'series':'eq142_owned_runner_e2e'}]
   if len(matches)!=1 or not matches[0]['supported']:raise ValueError('full controller fixture requires supported sequential/thread2/process2')
   for control in ('uninstrumented_A','instrumented_B'):
    fixtures.append(workspace.run_qualification_fixture(matches[0]['cohort_id'],control))
  workspace.journal.reconcile(workspace.children);workspace._source_check()
  result={'stage':'controller_qualification_only','count_qualification_reference':workspace.index_ref,
   'admission_reference':workspace.admission_ref,'fixtures':fixtures,'official_timing_authority':False,'full_sample_accepted':False}
 print(json.dumps(result,sort_keys=True))

if __name__=='__main__':main()
