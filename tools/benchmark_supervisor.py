"""Reproducible synthetic EQ062 compute-mode comparison, outside pure packages."""
from __future__ import annotations
import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import pickle
import platform
import statistics
import subprocess
import sys
import time
from importlib.metadata import version

from equity_feature_contracts import CanonicalBatch, Column, Coverage, AvailabilitySpec, InputScope
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_io_sdk import encode_result
from equity_feature_workers import BoundedSupervisor, ResourceBudget, WorkItem, prepare_session

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tests'))
from test_commands import inputs,Source
from supervisor_fixture import check_pure_oracle

def ping():return os.getpid()

class BoundedSource(Source):
    def capabilities(self):return replace(super().capabilities(),max_batch_rows=self.batch.row_count)

def workload(count,rows,skew=False):
    original,_,_=inputs()
    items=[];acquisitions=0
    for n in range(count):
        instrument=f'B{n:04d}'
        size=rows*8 if skew and n==0 else max(2,rows//8) if skew else rows
        end=100+size
        config=replace(original.config,session=replace(original.config.session,close_ns=end),
                       availability=AvailabilitySpec(end,end+10,end+10))
        fields=dict(instrument_id=(instrument,)*size,session_id=('S',)*size,
            start_ns=tuple(100+i for i in range(size)),end_ns=tuple(101+i for i in range(size)),
            known_at_ns=tuple(101+i for i in range(size)),
            open=tuple(100 if i%2==0 else 102 for i in range(size)),
            high=tuple(103 if i%2==0 else 104 for i in range(size)),
            low=tuple(99 if i%2==0 else 101 for i in range(size)),
            close=tuple(102 if i%2==0 else 103 for i in range(size)),
            volume=tuple(2 if i%2==0 else 3 for i in range(size)),
            actual_notional=tuple(203 if i%2==0 else 309 for i in range(size)))
        metadata=replace(inputs()[1].metadata,source=replace(inputs()[1].metadata.source,input_id='benchmark-'+instrument),
                         coverage=Coverage(size,size,True),scope=InputScope(100,end,'example-v1'))
        batch=CanonicalBatch(original.request.kind,tuple(Column(k,v) for k,v in fields.items()),metadata)
        requested=replace(original.request,instruments=(instrument,),end_ns=end,availability=config.availability,
                          max_rows=size,max_batch_rows=size)
        spec=replace(original,request=requested,config=config,partition_id=instrument+'-S',
                     governed_sessions=(IntervalSpec('P',0,100),IntervalSpec('S',100,end)),max_input_bytes=2097152)
        source=BoundedSource(batch,requested)
        prepared=prepare_session(spec,source)
        acquisitions+=source.called
        items.append(WorkItem(prepared.task,(prepared.batch,)))
    assert acquisitions==count
    return tuple(items),acquisitions

def digest(out):
    assert all(e.results is not None and e.reason is None and e.output is None for e in out.tasks)
    rows=dict(out.row_ledger)
    for execution in out.tasks:
        values={column.feature_id:column.values[0] for column in execution.results[0].values}
        pairs=rows[execution.task.task_sha256]//2
        assert values['session.bar.volume']==pairs*5
        assert values['session.bar.notional']==pairs*512
        assert values['session.bar.close_weighted_price']==102.6
    return hashlib.sha256(b''.join(encode_result(r) for e in out.tasks for r in e.results)).hexdigest()

def main():
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);p.add_argument('--repeats',type=int,default=3)
    args=p.parse_args();assert 2<=args.repeats<=10
    scopes=['packages','tools','tests','requirements-dev.txt','.github/workflows/foundation.yml']
    dirty=subprocess.check_output(['git','status','--porcelain','--',*scopes],cwd=ROOT,text=True)
    assert not dirty,'Freeze committed source before measurements'
    head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    tracked=subprocess.check_output(['git','ls-files','--',*scopes],cwd=ROOT,text=True).splitlines()
    hashes={}
    for name in tracked:
        actual=(ROOT/name).read_bytes();committed=subprocess.check_output(['git','show',head+':'+name],cwd=ROOT)
        assert actual==committed,(name,'working bytes differ from committed source')
        hashes[name]=hashlib.sha256(actual).hexdigest()
    check_pure_oracle()
    records=[];startups=[]
    capacity=os.cpu_count() or 1
    for mode in ('sequential','thread','process'):
        for workers in (1,2,4,8):
            if mode=='sequential' and workers!=1:continue
            if workers+1>capacity:
                startups.append({'mode':mode,'workers':workers,'skipped':'compute workers plus serialized coordinator allowance exceed observed logical CPU count'})
                continue
            samples=[]
            for _ in range(args.repeats):
                begin=time.perf_counter_ns()
                if mode=='sequential':ping()
                else:
                    pool=ThreadPoolExecutor(max_workers=workers) if mode=='thread' else ProcessPoolExecutor(
                        max_workers=workers,mp_context=multiprocessing.get_context('spawn'))
                    try:pool.submit(ping).result()
                    finally:pool.shutdown(wait=True)
                samples.append(time.perf_counter_ns()-begin)
            startups.append({'mode':mode,'workers':workers,'first_task_and_pool_shutdown_ns':samples,
                             'note':'one ping; lazy pools need not start all configured workers here'})
    for label,count,rows,skew in [('small',4,2,False),('large',32,256,False),('skew',32,256,True)]:
        begin=time.perf_counter_ns();items,acquisitions=workload(count,rows,skew)
        acquisition_ns=time.perf_counter_ns()-begin
        wire=pickle.dumps(items,protocol=5);copy_ns=[]
        for _ in range(args.repeats):
            begin=time.perf_counter_ns();restored=pickle.loads(pickle.dumps(items,protocol=5));copy_ns.append(time.perf_counter_ns()-begin)
            assert len(restored)==len(items)
        baseline=None;ledger=None
        for mode in ('sequential','thread','process'):
            for workers in (1,2,4,8):
                if mode=='sequential' and workers!=1:continue
                if workers+1>capacity:
                    records.append({'workload':label,'mode':mode,'workers':workers,'skipped':'CPU admission requires workers+1 slots'})
                    continue
                samples=[];reserved=0;result_bytes=0;identity=None
                limits=ResourceBudget(workers=workers,max_cpu_slots=workers+1,max_in_flight=workers,
                    max_input_bytes=33554432,max_resident_bytes=134217728)
                for _ in range(args.repeats):
                    supervisor=BoundedSupervisor(mode=mode,budget=limits)
                    begin=time.perf_counter_ns();out=supervisor.run(items);samples.append(time.perf_counter_ns()-begin)
                    identity=digest(out);reserved=out.reserved_resident_bytes;result_bytes=out.result_bytes
                    if baseline is None:baseline=identity;ledger=out.row_ledger
                    assert identity==baseline and out.row_ledger==ledger and out.distinct_reuse_rows==sum(i.rows for i in items)
                records.append({'workload':label,'tasks':count,'rows':sum(i.rows for i in items),'acquisitions':acquisitions,
                    'preparation_and_acquisition_once_ns':acquisition_ns,'mode':mode,'workers':workers,'compute_threads':1,
                    'backend_thread_allowance':1,'actual_native_backend_compute_threads':0,'elapsed_executor_ns':samples,
                    'elapsed_with_one_preparation_estimate_ns':[n+acquisition_ns for n in samples],
                    'median_executor_ns':statistics.median(samples),'pickle_bytes':len(wire),'pickle_roundtrip_ns':copy_ns,
                    'reserved_logical_resident_bytes':reserved,'result_codec_bytes':result_bytes,'logical_result_sha256':identity,
                    'single_worker_parity':True,'independent_small_oracle':True})
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()==head
    assert not subprocess.check_output(['git','status','--porcelain','--',*scopes],cwd=ROOT,text=True)
    report={'schema':'efworker-supervisor-comparison1','head':head,'source_sha256':hashes,'runtime':platform.python_version(),
        'system':platform.system(),'machine':platform.machine(),'logical_cpu_count':capacity,'repeats':args.repeats,
        'versions':{name:version(name) for name in ('equity-feature-workers','equity-features','equity-feature-io-sdk')},
        'cache_state':'same bounded caller-owned canonical batches reused across mode samples; no backend I/O in timed execution',
        'memory_method':'logical preflight reservations and exact pickle/result codec bytes; RSS and child peak memory not measured here',
        'scope':'synthetic compute-mode microcomparison; acquisition preparation timed once, no sink/network/private pilot throughput claim',
        'startups':startups,'samples':records}
    Path(args.out).write_text(json.dumps(report,sort_keys=True,indent=2)+'\n',encoding='utf-8',newline='\n')
    print('Mode comparison/source identity/row and logical result parity PASS',len(records),'configurations')

if __name__=='__main__':main()
