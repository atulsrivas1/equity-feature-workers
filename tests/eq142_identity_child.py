"""Owned child emits its direct native witness; no timing workload."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from eq142_identity import capture_owned_process
from eq142_evidence import persist_record

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--job',required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    persist_record(args.output,capture_owned_process(args.job),max_bytes=4096)
