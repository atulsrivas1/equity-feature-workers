"""New owned synthetic physical inputs; no borrowed checkout fixture execution."""
from dataclasses import replace
import hashlib
from pathlib import Path
import shutil
import duckdb
from equity_feature_contracts import EntityKey, InputScope, IntervalSpec
from equity_feature_contracts.adapters import AcquisitionRequest
from equity_feature_duckdb import (CatalogConfig, SourceSelection, FilePin, resolve_source,
    MappingPolicy, CoverageAssertion, ReadConfig, DuckDBHistoricalAdapter, VerificationPolicy)
from equity_feature_evidence import make_content_receipt
from equity_feature_io_sdk import descriptor
from equity_feature_workers import InputManifest, TaskManifest
from equity_features.session import compute_trades
from equity_feature_provenance import AcquiredInput, TaskEvidence
from provenance_fixture import fixture


def population(root):
    return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir() if p.is_file()}


def create(root: Path):
    root.mkdir()
    catalog=root/'catalog.duckdb';original=root/'original.parquet';optimized=root/'optimized.parquet'
    with duckdb.connect(str(catalog),config={'threads':1}) as con:
        con.execute('CREATE TABLE fixture(instrument_id BIGINT,ts_utc VARCHAR,price DOUBLE,size BIGINT,known_at_ns BIGINT,eligible BOOLEAN)')
        rows=[(n,'1970-01-01T00:00:00.'+f'{t:09d}'+'Z',p,s,k,True)
              for n in (0,1) for t,p,s,k in ((110,100,2,110),(130,102,3,130),(160,101,5,210))]
        con.executemany('INSERT INTO fixture VALUES (?,?,?,?,?,?)',rows)
        con.execute('COPY (SELECT * FROM fixture ORDER BY instrument_id,known_at_ns) TO ? (FORMAT PARQUET)',[str(original)])
        con.execute('DROP TABLE fixture');shutil.copyfile(original,optimized)
        con.execute('CREATE SCHEMA catalog')
        con.execute('CREATE TABLE catalog.catalog.datasets(layer VARCHAR,snapshot VARCHAR,dataset VARCHAR,source_schema VARCHAR,view_schema VARCHAR,view_name VARCHAR)')
        con.execute('CREATE TABLE catalog.catalog.files(layer VARCHAR,snapshot VARCHAR,dataset VARCHAR,source_schema VARCHAR,session_date DATE,path VARCHAR,bytes BIGINT,rows BIGINT,provenance VARCHAR,original_dataset VARCHAR,substituted_dataset VARCHAR,column_signature VARCHAR,optimized_path VARCHAR,optimized_bytes BIGINT,view_schema VARCHAR,view_name VARCHAR)')
        con.execute("INSERT INTO catalog.catalog.datasets VALUES ('prepared','synthetic1','FICTION','trades','synthetic1','source1')")
        con.execute('INSERT INTO catalog.catalog.files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ['prepared','synthetic1','FICTION','trades','1970-01-01',str(original),original.stat().st_size,
             6,'owned_synthetic_caller_assertion','FICTION',None,'synthetic-trades1',str(optimized),
             optimized.stat().st_size,'synthetic1','source1'])
    return population(root)


def acquire(root,*,instrument='A',K=210,pin=None):
    declared=fixture(instrument=instrument,K=K)
    config=declared.expected_task.config;unit=config.price_unit
    pin=pin or hashlib.sha256((root/'original.parquet').read_bytes()).hexdigest()
    resolved=resolve_source(CatalogConfig(root/'catalog.duckdb'),
        SourceSelection('prepared','synthetic1','FICTION','trades',('1970-01-01',)),
        pins=(FilePin(str(root/'original.parquet'),pin),))
    source=DuckDBHistoricalAdapter(ReadConfig(root/'catalog.duckdb',resolved,
        MappingPolicy('trades',unit,'binary64_exact','exact',((0,'A'),(1,'B')),'event','synthetic-v1'),
        'demo','owned-synthetic1','synthetic-calendar',(config.session,),(('1970-01-01','S'),),
        InputScope(100,200,'synthetic-v1'),CoverageAssertion((instrument,),('S',),100,200,3,'fixed-count1'),
        max_files=1,max_batch_rows=3,threads=1,memory_limit_mb=256,
        verification=VerificationPolicy(require_original_pins=True)))
    request=AcquisitionRequest('native-'+instrument,declared.acquired[0].batch.kind,'demo',
        (instrument,),('S',),100,200,'synthetic1',unit,config.availability,
        max_batch_rows=3,max_rows=3,max_batches=1)
    acquired=source.read(request);batch=acquired.canonical
    assert batch is not None and acquired.receipt is not None
    result=compute_trades(batch,config,entity=EntityKey(instrument,'S'))
    retained=make_content_receipt(result,config,(('trades',batch),),declared.receipt.environment,
        declared.receipt.execution,declared.receipt.evidence)
    task=TaskManifest('job','G',instrument+'-S','trades',(instrument,),config,descriptor(result).features,
        (InputManifest('trades',acquired.receipt.identity_digest,'original-revision1','complete',result.metadata.inputs[0]),),
        (IntervalSpec('P',0,100),IntervalSpec('S',100,200)),'synthetic-output','serialized_destination',1,1048576)
    return TaskEvidence(task,(AcquiredInput('trades',acquired.receipt,batch),),result,retained)
