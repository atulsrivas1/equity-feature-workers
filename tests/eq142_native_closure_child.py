"""Owned native exit fixture; parent can exit before its own short-lived child."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from eq142_identity import capture_owned_process
import json


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--job',required=True)
    parser.add_argument('--directory',type=Path,required=True);parser.add_argument('--child',action='store_true')
    parser.add_argument('--release',type=Path,required=True)
    args=parser.parse_args()
    body=(json.dumps(capture_owned_process(args.job),sort_keys=True,separators=(',',':'),
        ensure_ascii=True,allow_nan=False)+'\n').encode('ascii')
    if len(body)>4096:raise ValueError('Owned fixture witness bound')
    with (args.directory/('child.json' if args.child else 'parent.json')).open('xb') as output:output.write(body)
    if args.child:
        deadline=time.monotonic()+5
        while not args.release.exists():
            if time.monotonic()>=deadline:raise TimeoutError('Owned observer release not received')
            time.sleep(0.005)
    else:
        child=subprocess.Popen([sys.executable,__file__,'--job',args.job,'--directory',str(args.directory),
            '--release',str(args.release),'--child'])
        deadline=time.monotonic()+5
        while not (args.directory/'child.json').exists():
            if time.monotonic()>=deadline:raise TimeoutError('Owned descendant did not publish its witness')
            time.sleep(0.005)
        # Only this fixture's process exits; its inherited owned Job still holds
        # the descendant. The external sampler must observe final descendant exit.
        os._exit(0)
