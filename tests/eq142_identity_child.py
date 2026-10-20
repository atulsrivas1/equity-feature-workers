"""Owned child emits its direct native witness; no timing workload."""
import argparse
from pathlib import Path
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from eq142_identity import capture_owned_process
from eq142_evidence import persist_record

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--job',required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--release',type=Path)
    args=parser.parse_args()
    persist_record(args.output,capture_owned_process(args.job),max_bytes=4096)
    if args.release is not None:
        deadline=time.monotonic()+5
        while not args.release.exists():
            if time.monotonic()>=deadline:raise TimeoutError('Owned fixture release was not observed')
            time.sleep(0.005)
