"""Owned synthetic actual-source/physical-sink pilot, outside numerical packages."""
from __future__ import annotations
import hashlib
import json
import math
import os
from pathlib import Path
import pickle
import subprocess
import sys
import tempfile
import time
from importlib.metadata import version

# Fixture modules only; installed qualification never adds package source roots.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pilot_memory import summary
from pilot_process import communicate_owned

ORACLE = json.loads(Path(__file__).with_name('pilot_oracle.json').read_text(encoding='utf-8'))
VERSIONS = {'equity-feature-contracts':'0.0.4a4','equity-features':'0.0.4a4',
    'equity-feature-io-contracts':'0.1.0a2','equity-feature-io-sdk':'0.1.0a2',
    'equity-feature-duckdb':'0.1.0a8','equity-feature-parquet':'0.1.0a1',
    'equity-feature-duckdb-sink':'0.1.0a0','duckdb':'1.5.6','numpy':'2.2.6','pyarrow':'20.0.0'}
SCOPE = 'synthetic-pilot-output'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def counts(workload):
    definition = ORACLE['workloads'][workload]
    return [definition.get('rows_each', definition.get('first_rows')) if n == 0 else
            definition.get('rows_each', definition.get('other_rows')) for n in range(definition['instruments'])]


def population(root):
    return {p.name: dict(bytes=p.stat().st_size, sha256=sha(p)) for p in sorted(root.iterdir()) if p.is_file()}


def make_fixture(root, workload):
    import duckdb
    import shutil
    root = root.resolve(); root.mkdir(parents=True, exist_ok=False)
    db = root/'catalog.duckdb'; original = root/'original.parquet'; optimized = root/'optimized.parquet'
    rows = [(instrument, f'1970-01-01T00:00:00.{100+i:09d}Z', 100 if i%2 == 0 else 102,
             2 if i%2 == 0 else 3, 100+i, True)
            for instrument, size in enumerate(counts(workload)) for i in range(size)]
    assert len(rows) == ORACLE['workloads'][workload]['total_rows']
    with duckdb.connect(str(db), config={'threads':1}) as con:
        con.execute('CREATE TABLE fixture(instrument_id BIGINT,ts_utc VARCHAR,price DOUBLE,size BIGINT,known_at_ns BIGINT,eligible BOOLEAN)')
        con.executemany('INSERT INTO fixture VALUES (?,?,?,?,?,?)', rows)
        con.execute('COPY (SELECT * FROM fixture ORDER BY instrument_id,known_at_ns) TO ? (FORMAT PARQUET)', [str(original)])
        con.execute('DROP TABLE fixture')
        shutil.copyfile(original, optimized)
        con.execute('CREATE SCHEMA catalog')
        con.execute('CREATE TABLE catalog.catalog.datasets(layer VARCHAR,snapshot VARCHAR,dataset VARCHAR,source_schema VARCHAR,view_schema VARCHAR,view_name VARCHAR)')
        con.execute('CREATE TABLE catalog.catalog.files(layer VARCHAR,snapshot VARCHAR,dataset VARCHAR,source_schema VARCHAR,session_date DATE,path VARCHAR,bytes BIGINT,rows BIGINT,provenance VARCHAR,original_dataset VARCHAR,substituted_dataset VARCHAR,column_signature VARCHAR,optimized_path VARCHAR,optimized_bytes BIGINT,view_schema VARCHAR,view_name VARCHAR)')
        con.execute("INSERT INTO catalog.catalog.datasets VALUES ('prepared','synthetic-pilot1','FICTION','trades','synthetic1','source1')")
        con.execute('INSERT INTO catalog.catalog.files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ['prepared','synthetic-pilot1','FICTION','trades','1970-01-01',str(original),original.stat().st_size,
             len(rows),'owned_synthetic_caller_assertion','FICTION',None,'synthetic-trades1',
             str(optimized),optimized.stat().st_size,'synthetic1','source1'])
    return population(root)


def golden(result, size):
    from equity_feature_contracts import Status
    values = {c.feature_id.rsplit('.',1)[1]: c.values[0] for c in result.values}
    expected = next(v for v in ORACLE['per_instrument_totals'].values() if v['count'] == size)
    for field, value in expected.items():
        assert type(values[field]) is int and values[field] == value
    assert math.isclose(values['vwap'], 506/5, rel_tol=1e-12, abs_tol=1e-12)
    assert math.isclose(values['mean_size'], 5/2, rel_tol=1e-12, abs_tol=1e-12)
    assert len(values) == 5
    assert all(q.status == Status.AVAILABLE and q.expected == q.observed == size and not q.reasons for q in result.quality)
    assert len(result.metadata.inputs) == 1 and result.metadata.inputs[0].metadata.coverage.complete


class CandidateReader:
    """Read-return times become successful ends only after authoritative verification."""
    def __init__(self, sink):
        self.sink, self.ends = sink, {}
    def __getattr__(self, name):
        return getattr(self.sink, name)
    def read(self, receipt):
        result = self.sink.read(receipt)
        self.ends.setdefault(receipt.identity.partition_id, time.perf_counter_ns())
        return result


def run_pipeline(root, output, workload, backend, mode, workers, require_installed=False):
    # Optional native I/O imports stay in the coordinator; spawn children run pure callbacks.
    import equity_feature_contracts as c
    import equity_feature_workers as worker_package
    from equity_feature_contracts.adapters import AcquisitionRequest
    from equity_feature_contracts.specs import IntervalSpec
    from equity_feature_duckdb import (CatalogConfig, SourceSelection, FilePin, resolve_source,
        MappingPolicy, CoverageAssertion, ReadConfig, DuckDBHistoricalAdapter, VerificationPolicy)
    from equity_feature_parquet import ParquetSink
    from equity_feature_duckdb_sink import DuckDBSink
    from equity_feature_io_contracts import SinkRequirements
    from equity_feature_io_sdk import descriptor, encode_result
    from equity_features.session import compute_trades
    from equity_feature_workers import (SessionCommandSpec, prepare_session, WorkItem, BoundedSupervisor,
        ResourceBudget, ProgressRecorder, SerialPublisher, PublicationLimits, Dependency,
        GenerationSpec, GenerationStore, BarrierLimits, CatalogStore, CatalogLimits)
    versions = {n:version(n) for n in VERSIONS}; assert versions == VERSIONS
    assert worker_package.__version__ == '0.1.0a11'
    if require_installed:
        assert 'site-packages' in Path(worker_package.__file__).resolve().parts
    root = root.resolve(); output = output.resolve(); output.mkdir(parents=True, exist_ok=False)
    before = population(root)
    revision = hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest()
    unit=c.PriceUnit(0,'USD'); session=c.SessionSpec('synthetic-pilot','S',0,1000000000,'UTC')
    availability=c.AvailabilitySpec(1000000000,1000000010,1000000010)
    config=c.ConfigSpec('synthetic-pilot','v1',(c.Parameter('eligibility_policy','pilot1'),),session,
        c.WindowSpec(1,'S',('P','S')),availability,price_unit=unit)
    requirements=SinkRequirements(max_results=100,max_chunk_bytes=1048576,max_total_bytes=10485760,
        max_result_cells=100000,max_evidence_rows=100000)
    recorder=ProgressRecorder(); starts={}; items=[]; setup=[]
    pipeline_start=time.perf_counter_ns()
    original=ParquetSink(output/'sink',SCOPE,limits=requirements) if backend=='parquet' else DuckDBSink(output/'sink.duckdb',SCOPE,limits=requirements)
    with original as physical:
        sink=CandidateReader(physical)
        for n,size in enumerate(counts(workload)):
            instrument=f'B{n:04d}'; partition=instrument+'-S'; starts[partition]=time.perf_counter_ns()
            resolved=resolve_source(CatalogConfig(root/'catalog.duckdb'),
                SourceSelection('prepared','synthetic-pilot1','FICTION','trades',('1970-01-01',)),
                pins=(FilePin(str(root/'original.parquet'),before['original.parquet']['sha256']),))
            mapping=MappingPolicy('trades',unit,'binary64_exact','exact',
                tuple((i,f'B{i:04d}') for i in range(len(counts(workload)))),'event','pilot1')
            source=DuckDBHistoricalAdapter(ReadConfig(root/'catalog.duckdb',resolved,mapping,'synthetic-pilot',
                'owned-synthetic1','pilot-grid1',(session,),(('1970-01-01','S'),),c.InputScope(0,1000000000,'pilot1'),
                CoverageAssertion((instrument,),('S',),0,1000000000,size,'frozen-count1'),max_files=1,
                max_batch_rows=256,threads=1,memory_limit_mb=256,verification=VerificationPolicy(require_original_pins=True)))
            request=AcquisitionRequest('pilot-'+workload+'-'+instrument,c.DataKind.TRADE,'synthetic-pilot',
                (instrument,),('S',),0,1000000000,'synthetic-pilot1',unit,availability,
                max_batch_rows=min(256,size),max_rows=size,max_batches=(size+255)//256)
            spec=SessionCommandSpec('pilot-'+workload,'generation1',partition,'trades',request,config,
                descriptor(compute_trades(None,config,entity=c.EntityKey(instrument,'S'))).features,
                (IntervalSpec('P',-1000000000,0),IntervalSpec('S',0,1000000000)),revision,SCOPE,4194304)
            setup.append(time.perf_counter_ns()-starts[partition])
            prepared=prepare_session(spec,source,progress=recorder); assert prepared.batch is not None
            assert prepared.batch.row_count == size
            event_ids=prepared.batch.column('event_id'); assert event_ids is not None and len(set(event_ids.values)) == size
            items.append(WorkItem(prepared.task,(prepared.batch,)))
        publisher=SerialPublisher(sink,SCOPE,limits=PublicationLimits(),requirements=requirements)
        budget=ResourceBudget(workers=workers,max_cpu_slots=workers+1,max_in_flight=workers,
            max_input_bytes=33554432,max_resident_bytes=134217728)
        outcome=BoundedSupervisor(mode=mode,budget=budget).run(tuple(items),publisher=publisher,progress=recorder)
        assert not outcome.cancelled and sum(v for _,v in outcome.row_ledger) == ORACLE['workloads'][workload]['total_rows']
        assert not publisher.pending and all(e.output is not None and e.results is not None and e.reason is None for e in outcome.tasks)
        dependencies=tuple(Dependency(e.task.task_sha256,e.task,e.output,sink) for e in outcome.tasks)
        generation=GenerationSpec('synthetic-pilot','pilot-'+workload,'generation1',tuple(e.task for e in outcome.tasks))
        generations=GenerationStore(output/'generations',limits=BarrierLimits(),requirements=requirements)
        catalog=CatalogStore(output/'catalog',limits=CatalogLimits(),requirements=requirements)
        start=time.perf_counter_ns(); assert generations.publish(generation,dependencies,progress=recorder).complete
        generation_ns=time.perf_counter_ns()-start
        start=time.perf_counter_ns(); selected=catalog.select(generation,generations,dependencies,expected_sequence=0,progress=recorder)
        assert selected.accepted and selected.snapshot is not None
        assert catalog.verify(selected.snapshot,generations,dependencies).accepted
        catalog_ns=time.perf_counter_ns()-start
        logical=[]; codec_bytes=0; latencies=[]
        for execution in sorted(outcome.tasks,key=lambda e:e.task.task_sha256):
            size=dict((f'B{i:04d}',v) for i,v in enumerate(counts(workload)))[execution.task.instruments[0]]
            assert execution.output.receipt is not None
            read=sink.read(execution.output.receipt)
            assert tuple(map(encode_result,read)) == tuple(map(encode_result,execution.results))
            for result in read:
                golden(result,size); wire=encode_result(result); logical.append(wire); codec_bytes+=len(wire)
            latency=sink.ends[execution.task.partition_id]-starts[execution.task.partition_id]
            assert latency > 0; latencies.append(latency)
    elapsed=time.perf_counter_ns()-pipeline_start
    assert population(root) == before
    snapshot=recorder.snapshot(); assert len(snapshot.tasks) == 4*len(items)
    assert all(t.reason is None and t.status.value in ('ready','verified','generation_complete','catalog_accepted') for t in snapshot.tasks)
    stages={}
    for observation in snapshot.tasks:
        for timing in observation.timings:
            record=stages.setdefault(timing.stage.value,dict(elapsed_ns=[],probes=[]))
            record['elapsed_ns'].append(timing.elapsed_ns); record['probes'].append(timing.probes)
    return dict(coordinator_pid=os.getpid(),workload=workload,backend=backend,mode=mode,workers=workers,rows=sum(counts(workload)),
        tasks=len(items),pipeline_elapsed_ns=elapsed,rows_per_second=sum(counts(workload))*1e9/elapsed,
        tasks_per_second=len(items)*1e9/elapsed,task_latency_ns=latencies,task_latency_distribution_ns=summary(latencies),
        content_sha256=hashlib.sha256(b''.join(logical)).hexdigest(),full_result_quality_evidence_input_parity=True,
        source_revision=revision,input_population=before,source_setup_ns=setup,stages=stages,
        generation_ns=generation_ns,catalog_select_and_verify_ns=catalog_ns,
        canonical_pickle_bytes=sum(len(pickle.dumps(i.batches,protocol=5)) for i in items),
        result_codec_bytes=codec_bytes,final_output_file_bytes=sum(p.stat().st_size for p in output.rglob('*') if p.is_file()),
        declared_logical_budget=budget.__dict__,actual_admitted_input_bytes=outcome.admitted_input_bytes,
        logical_reserved_resident_bytes=outcome.reserved_resident_bytes,
        reporter_reserved_bytes=recorder.reserved_bytes,reporter_attempts=len(snapshot.tasks),
        versions=versions|{'equity-feature-workers':worker_package.__version__},
        backend_threads=1,source_max_batch_rows=256,request_max_batch_rows=sorted({min(256,n) for n in counts(workload)}),independent_five_trade_goldens=True,
        numerical_algorithm_unchanged=True,private_pilot='NOT_ADMITTED',corrected_month='NOT_ADMITTED',annual='NOT_ADMITTED')


def isolated_sample(fixture, output, workload, backend, mode, workers, require_installed=False):
    settings=dict(root=str(fixture),output=str(output),workload=workload,backend=backend,mode=mode,
                  workers=workers,require_installed=require_installed)
    command=[sys.executable]+(['-I'] if require_installed else [])+[str(Path(__file__).resolve()),'--sample']
    environment=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1')
    start=time.perf_counter_ns()
    stdout,stderr,memory,returncode=communicate_owned(command,json.dumps(settings),env=environment,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    if returncode:
        # Private developer logs may inspect the exception; no raw paths/text enter public reports.
        raise RuntimeError('isolated synthetic pipeline failed: '+stderr)
    result=json.loads(stdout); result['isolated_process_wall_ns']=time.perf_counter_ns()-start
    assert any(p['pid']==result['coordinator_pid'] and p['samples']>0 for p in memory['processes'].values()), 'actual coordinator memory unavailable'
    result['memory']=memory
    return result


if __name__ == '__main__':
    if sys.argv[1:] == ['--sample']:
        settings=json.loads(sys.stdin.read()); settings['root']=Path(settings['root']); settings['output']=Path(settings['output'])
        from equity_feature_io_contracts import SinkError, SinkErrorCode
        from equity_feature_workers import CommandError, CommandErrorCode
        try:
            result=run_pipeline(**settings)
        except (SinkError,CommandError) as error:
            if error.code not in (SinkErrorCode.RESOURCE_LIMIT,CommandErrorCode.LIMIT):
                raise
            result=dict(coordinator_pid=os.getpid(),skipped='actual logical pipeline resource limit',
                        admission='logical pipeline limits',reason=error.code.value)
        print(json.dumps(result,sort_keys=True,allow_nan=False))
    elif sys.argv[1:] == ['--installed-smoke']:
        with tempfile.TemporaryDirectory() as temporary:
            base=Path(temporary); make_fixture(base/'fixture','small'); reference=None
            for backend in ('parquet','duckdb'):
                sample=isolated_sample(base/'fixture',base/backend,'small',backend,'sequential',1,True)
                if reference is None: reference=sample['content_sha256']
                assert sample['content_sha256']==reference
        print('Actual installed DuckDB source -> pure trades -> both physical sinks -> verified generation/catalog; full parity/goldens/RSS PASS')
    else:
        raise SystemExit('explicit --sample or --installed-smoke required')
