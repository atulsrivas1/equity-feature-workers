"""Development owned runner writer binding; no complete sample or study authority.

The caller creates the outer directory. The original runner exclusively creates
its fixed child and returns its actual writer reference. Errors are not retried.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import re

from eq142_graph import wire

PROFILES=('small','balanced','skewed','family_complete','family_missing_hl','family_old_close_gap')
SERIES=('eq142_admitted_control','eq142_owned_runner_e2e')
CONTROLS=('uninstrumented_A','instrumented_B')
PURPOSES=('development_candidate','qualification_counts')
ID_PATTERN=r'[a-z][a-z0-9_.-]{0,127}'
JOB_PATTERN=r'Local\\equity-feature-supplied-[0-9a-f]{32}'


def text_pattern(value,pattern):
    if type(value) is not str or re.fullmatch(pattern,value) is None:
        raise ValueError('owned runner identifier')


def validate_return(record):
    """Pure closed return record check; provenance requires original/native joins."""
    if type(record) is not dict or set(record)!={'schema','sample_id','purpose','runner_reference','process_binding'}:
        raise ValueError('owned runner closed record')
    if record['schema']!='eq142-owned-runner-return:1' or record['purpose'] not in PURPOSES:
        raise ValueError('owned runner schema/purpose')
    text_pattern(record['sample_id'],ID_PATTERN)
    ref=record['runner_reference']
    if (type(ref) is not dict or set(ref)!={'codec','sha256','bytes','retained_name'}
            or ref['codec']!='eq142_development_runner' or ref['retained_name']!='runner.json'
            or type(ref['bytes']) is not int or not 1<=ref['bytes']<=1048576):
        raise ValueError('owned runner original reference')
    text_pattern(ref['sha256'],r'[0-9a-f]{64}')
    process=record['process_binding']
    if (type(process) is not dict or set(process)!={'pid','process_startup_nonce'}
            or type(process['pid']) is not int or not 1<=process['pid']<=4294967295):
        raise ValueError('owned runner process binding')
    text_pattern(process['process_startup_nonce'],r'[0-9a-f]{32}')
    if len(wire(record))>4096:raise ValueError('owned runner record bound')


def decode_return(body):
    """Strict bounded canonical decoding, including duplicate-key rejection."""
    if type(body) is not bytes or not 1<=len(body)<=4096:raise ValueError('owned runner body bound')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('owned runner duplicate key')
            result[key]=value
        return result
    try:
        record=json.loads(body,object_pairs_hook=pairs)
        validate_return(record)
        if wire(record)!=body:raise ValueError('owned runner noncanonical body')
    except (UnicodeError,RecursionError,TypeError,OverflowError) as error:
        raise ValueError('owned runner malformed body') from error
    return record


def validate_options(*,sample_id,purpose,reference_sha256,profile,mode,degree,control,series,job_name):
    text_pattern(sample_id,ID_PATTERN);text_pattern(reference_sha256,r'[0-9a-f]{64}')
    text_pattern(job_name,JOB_PATTERN)
    if (purpose not in PURPOSES or profile not in PROFILES or mode not in ('sequential','thread','process')
            or type(degree) is not int or degree not in (1,2,4,8) or mode=='sequential' and degree!=1
            or control not in CONTROLS or series not in SERIES
            or purpose=='qualification_counts' and control!='instrumented_B'):
        raise ValueError('owned runner selectors')


def owned_directory(directory,sample_id):
    path=Path(directory).absolute()
    def no_indirections(value):
        for entry in (value,*value.parents):
            if entry.is_symlink() or entry.is_junction():raise ValueError('owned runner directory indirection')
    no_indirections(path)
    path=path.resolve(strict=True);no_indirections(path)
    if not path.is_dir() or path.name!=sample_id:raise ValueError('owned runner sample directory')
    members=list(path.iterdir())
    if len(members)>1 or any(p.name!='capacity-before.json' or not p.is_file()
            or p.is_symlink() or p.is_junction() for p in members):
        raise ValueError('owned runner existing member')
    return path


def run_owned_control(reference_path,reference_sha256,*,directory,sample_id,purpose,
                      profile,mode,degree,control,series,job_name):
    validate_options(sample_id=sample_id,purpose=purpose,reference_sha256=reference_sha256,
        profile=profile,mode=mode,degree=degree,control=control,series=series,job_name=job_name)
    directory=owned_directory(directory,sample_id)
    # Lazy optional imports keep pure decoding and refusal controls mandatory.
    from benchmark_execution_baseline import run_control
    from eq142_evidence import persist_record
    from eq142_trace import fragment_clock_and_timelines
    ref=run_control(reference_path,reference_sha256,directory=directory/'runner',
        profile=profile,mode=mode,degree=degree,control=control,series=series,
        job_name=job_name,qualify_code_counts=purpose=='qualification_counts')
    record={'schema':'eq142-owned-runner-return:1','sample_id':sample_id,'purpose':purpose,
        'runner_reference':ref,'process_binding':fragment_clock_and_timelines(set())[0]}
    validate_return(record)
    return {'codec':'eq142_owned_runner_return',
        **persist_record(directory/'runner-reference.json',record,max_bytes=4096)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',required=True,type=Path)
    parser.add_argument('--sample-id',required=True)
    parser.add_argument('--purpose',required=True,choices=PURPOSES)
    parser.add_argument('--reference',required=True,type=Path)
    parser.add_argument('--reference-sha256',required=True)
    parser.add_argument('--profile',required=True,choices=PROFILES)
    parser.add_argument('--mode',required=True,choices=('sequential','thread','process'))
    parser.add_argument('--degree',required=True,type=int)
    parser.add_argument('--control',required=True,choices=CONTROLS)
    parser.add_argument('--series',required=True,choices=SERIES)
    parser.add_argument('--job',required=True)
    args=parser.parse_args()
    result=run_owned_control(args.reference,args.reference_sha256,directory=args.directory,
        sample_id=args.sample_id,purpose=args.purpose,profile=args.profile,mode=args.mode,
        degree=args.degree,control=args.control,series=args.series,job_name=args.job)
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':main()
