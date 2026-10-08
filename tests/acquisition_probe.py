"""Fresh-installed optional boundary, command consent and strict public typing."""
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
import tempfile

import equity_feature_workers as worker

assert worker.__version__=='0.1.0a13'
assert 'site-packages' in Path(worker.__file__).resolve().parts
assert not any(n.startswith(('equity_feature_files','equity_feature_acquisition')) for n in sys.modules)
assert {'SourceOffer','AcquisitionPlan','ExecutionApproval','plan_acquisition','execute_plan'} <= set(worker.__all__)
assert sorted(importlib.metadata.requires('equity-feature-workers')) == ['equity-feature-io-sdk==0.1.0a2','equity-features==0.0.4a4']
def cli(*args):return subprocess.run([sys.executable,'-I','-m','equity_feature_workers.cli',*args],capture_output=True,text=True,encoding='utf-8')
denied=cli('--acquisition-demo');assert denied.returncode==1 and 'EXECUTION_NOT_AUTHORIZED' in denied.stderr
assert 'Traceback' not in denied.stderr
if sys.argv[1]=='light':
    missing=cli('--acquisition-demo','--approve-owned-fixture')
    assert missing.returncode==1 and 'NO_INSTALLED_CAPABILITY' in missing.stderr and 'Traceback' not in missing.stderr
    print(json.dumps({'light_import':True,'default_denied':True,'optional_demo_unavailable':True}))
else:
    approved=cli('--acquisition-demo','--approve-owned-fixture');assert approved.returncode==0,approved.stderr
    records=json.loads(approved.stdout);r=records[0]
    assert r['verified_readback'] and r['owned_synthetic_only'] and not r['provider_access']
    assert (r['values']['session.trade.count'],r['values']['session.trade.volume'],r['values']['session.trade.notional'])==(3,10,1011)
    assert abs(r['values']['session.trade.vwap']-101.1)<1e-12
    with tempfile.TemporaryDirectory() as temporary:
        p=Path(temporary)/'negative.py'
        p.write_text('from equity_feature_workers import ExecutionApproval, SourceOffer\nExecutionApproval(1)\nSourceOffer("owned", "invalid", {})\n',encoding='utf-8')
        result=subprocess.run([sys.executable,'-I','-m','mypy','--strict',str(p)],capture_output=True,text=True,encoding='utf-8')
        assert result.returncode==1 and result.stdout.count(' error:')==2,result.stdout
    print(json.dumps({'owned_csv_command':True,'verified_readback':True,'default_denied':True,'negative_public_typing':2,
                      'files_version':importlib.metadata.version('equity-feature-files'),
                      'acquisition_version':importlib.metadata.version('equity-feature-acquisition')}))
