"""Frozen EQ066 actual source/calculation/sink scaling; public synthetic evidence."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import random
import statistics
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tests'))
from pilot_fixture import ORACLE, make_fixture, population, isolated_sample
from pilot_memory import hardware, admission, summary

SCOPED=('packages','tools','tests','requirements-dev.txt','.github/workflows/foundation.yml')


def frozen():
    def git(*args):return subprocess.check_output(['git',*args],cwd=ROOT)
    assert not git('status','--porcelain','--',*SCOPED),'Freeze committed scoped source before measurements'
    head=git('rev-parse','HEAD').decode().strip()
    files=git('ls-files','--',*SCOPED).decode().splitlines(); hashes={}
    for name in files:
        data=(ROOT/name).read_bytes();assert data==git('show',head+':'+name),(name,'raw working bytes differ')
        hashes[name]=hashlib.sha256(data).hexdigest()
    return head,hashes


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--fixture-root',required=True,type=Path)
    parser.add_argument('--output-root',required=True,type=Path)
    args=parser.parse_args();head,hashes=frozen()
    fixtures=args.fixture_root.resolve();outputs=args.output_root.resolve()
    # New explicit owned roots only. Never replace or delete an existing dataset/output.
    assert not fixtures.exists() and not outputs.exists()
    fixtures.mkdir(parents=True);outputs.mkdir(parents=True)
    facts=hardware();records=[];references=[];orders=[];inputs={}
    report=dict(schema='eq066-physical-pipeline1',head=head,source_sha256=hashes,hardware=facts,
        core_commit='20c08c7370581d03c8a0404579667f68d67ac88b',io_commit='4603c6e50331a5e8a82b13b62a0cdd5ffaa0e4bf',
        oracle_sha256=hashes['tests/pilot_oracle.json'],seed=766,repeats=3,
        planned_configurations=54,planned_measured_samples=162,records=records,references=references,execution_order=orders,
        cache='fresh coordinators/outputs, same frozen input files; untimed reference warms files; uncontrolled OS page cache/thermal/background load; no cold-disk claim',
        measurement='pipeline includes construction/acquisition/worker startup/IPC/compute/serialization/publication/readback/generation/accepted catalog/sink close; task ends are first full read-return candidates verified later; isolated wall additionally includes imports/report/sampling overhead',
        bytes='fixture/catalog file lengths, canonical pickle bytes, result codec bytes and final output file lengths are distinct; no physical disk-read/peak-disk claim',
        stage_limits='overlapping per-task/containing-group spans are not wall elapsed or isolated lock-blocking cost; uncontended BUSY/probe evidence is not a contention stress proof',
        private_pilot='NOT_ADMITTED',corrected_month='NOT_ADMITTED',annual='NOT_ADMITTED')
    args.out.parent.mkdir(parents=True,exist_ok=True)
    def save():args.out.write_text(json.dumps(report,sort_keys=True,indent=2,allow_nan=False)+'\n',encoding='utf-8',newline='\n')
    configurations=[(backend,mode,workers) for backend in ('parquet','duckdb') for mode in ('sequential','thread','process')
                    for workers in (1,2,4,8) if mode!='sequential' or workers==1]
    assert len(configurations)==18
    for workload in ('small','large','skew'):
        fixture=fixtures/workload;inputs[workload]=make_fixture(fixture,workload)
        baseline={}
        for backend in ('parquet','duckdb'):
            reason=admission(facts,1)
            if reason:raise RuntimeError('single-worker reference admission unavailable: '+reason)
            sample=isolated_sample(fixture,outputs/(workload+'-'+backend+'-reference'),workload,backend,'sequential',1)
            assert 'skipped' not in sample,'logical reference admission unavailable'
            sample['excluded_from_measured_protocol']=True
            references.append(sample);baseline[backend]=sample['content_sha256'];save()
        assert len(set(baseline.values()))==1,'backend logical parity failed'
        shuffled=configurations.copy();random.Random(766).shuffle(shuffled)
        for repeat,rotation in enumerate((0,6,12)):
            order=shuffled[rotation:]+shuffled[:rotation]
            for index,(backend,mode,workers) in enumerate(order):
                identity=dict(workload=workload,backend=backend,mode=mode,workers=workers,repeat=repeat,position=index)
                orders.append(identity)
                reason=admission(facts,workers)
                if reason:
                    records.append(identity|dict(skipped=reason,admission='observed CPU/RAM'));save();continue
                sample=isolated_sample(fixture,outputs/(workload+'-'+backend+'-'+mode+'-'+str(workers)+'-'+str(repeat)),
                    workload,backend,mode,workers)
                if 'skipped' in sample:
                    records.append(identity|sample);save();continue
                assert sample['content_sha256']==baseline[backend],'full result/status/quality/evidence/input parity failed'
                assert sample['input_population']==inputs[workload]
                records.append(identity|sample);save()
                print(f'{workload} {backend} {mode}{workers} repeat{repeat}: {sample["pipeline_elapsed_ns"]/1e9:.3f}s verified',flush=True)
        assert population(fixture)==inputs[workload]
    assert len(records)==162 and frozen()==(head,hashes)
    groups=[]
    for workload in ('small','large','skew'):
        for backend,mode,workers in configurations:
            samples=[r for r in records if (r['workload'],r['backend'],r['mode'],r['workers'])==(workload,backend,mode,workers) and 'skipped' not in r]
            if not samples:continue
            reference=[r for r in records if (r['workload'],r['backend'],r['mode'],r['workers'])==(workload,backend,'sequential',1) and 'skipped' not in r]
            median=statistics.median(r['pipeline_elapsed_ns'] for r in samples)
            groups.append(dict(workload=workload,backend=backend,mode=mode,workers=workers,samples=len(samples),
                elapsed_ns=summary([r['pipeline_elapsed_ns'] for r in samples]),
                speedup_vs_measured_sequential1=statistics.median(r['pipeline_elapsed_ns'] for r in reference)/median,
                sampled_job_peak_bytes=max(r['memory']['sampled_job_peak_bytes'] for r in samples),
                task_latency_ns=summary([v for r in samples for v in r['task_latency_ns']])))
    report.update(input_population=inputs,groups=groups,measured_samples=sum('skipped' not in r for r in records),
        skipped_samples=sum('skipped' in r for r in records),all_full_parity_and_independent_goldens=True,
        qualification='complete measured protocol on stated hardware; synthetic engineering qualification only')
    save();print('Frozen physical pipeline repeated protocol/full logical parity/independent goldens/RSS observations PASS')


if __name__=='__main__':main()
