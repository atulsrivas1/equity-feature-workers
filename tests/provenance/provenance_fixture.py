"""Actual public typed declarations for supplied-only controls, not acquisition proof."""
from equity_feature_contracts import (AvailabilitySpec, BatchMetadata, CanonicalBatch, Column,
    ConfigSpec, Coverage, DataKind, EntityKey, InputScope, IntervalSpec, Parameter,
    PriceUnit, SessionSpec, SourceBinding, WindowSpec)
from equity_features.session import compute_trades
from equity_feature_duckdb import AcquisitionReceipt, MappingReport
from equity_feature_evidence import (EnvironmentDeclaration, PackageVersion, ExecutionClaim,
    EvidenceDisclosure, make_content_receipt)
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import descriptor, prepare_publication
from equity_feature_workers import InputManifest, TaskManifest, OutputManifest
from equity_feature_provenance import AcquiredInput, TaskEvidence


def fixture(*, instrument='A', generation='G', K=210, state='complete'):
    unit=PriceUnit(0,'USD')
    config=ConfigSpec('provenance-trades','v1',(Parameter('eligibility_policy','synthetic-v1'),),
        SessionSpec('demo','S',100,200,'caller'),WindowSpec(1,'S',('P','S')),
        AvailabilitySpec(200,K,210),price_unit=unit)
    source=SourceBinding('synthetic','snapshot1','map1:retained-map1:'+'1'*64,'trades1:mapped:'+'1'*64)
    count=0 if state in ('empty','missing') else 3
    coverage=Coverage(None if state in ('partial','missing') else count,count,state not in ('partial','missing'))
    values=(('instrument_id',(instrument,)*count),('session_id',('S',)*count),
        ('event_ns',(110,130,160)[:count]),('order_key',(1,2,3)[:count]),
        ('event_id',('t1','t2','t3')[:count]),('eligible',(True,)*count),
        ('price',(100,102,101)[:count]),('size',(2,3,5)[:count]),('known_at_ns',(110,130,210)[:count]))
    batch=None if state=='missing' else CanonicalBatch(DataKind.TRADE,tuple(Column(k,v) for k,v in values),
        BatchMetadata('demo',source,coverage,unit,scope=InputScope(100,200,'synthetic-v1')))
    normalization=None if state=='missing' else MappingReport('retained-map1','1'*64,count,(),(),
        'event','synthetic-v1','explicit_column')
    acquisition=AcquisitionReceipt('2'*64,'3'*64,'4'*64,'5'*64,'6'*64,(),
        ('2026-01-01',) if state in ('missing','partial') else (),source,normalization,
        coverage,config.availability,'synthetic-calendar','missing' if state=='missing' else 'data',
        count,'no_selected_files',0)
    result=compute_trades(batch,config,entity=EntityKey(instrument,'S'))
    receipt=make_content_receipt(result,config,() if batch is None else (('trades',batch),),
        EnvironmentDeclaration('CPython','3.12','synthetic',(
            PackageVersion('equity-feature-contracts','0.0.4a4'),PackageVersion('equity-features','0.0.4a4'))),
        ExecutionClaim(),EvidenceDisclosure('not_supplied',0))
    binding=result.metadata.inputs[0] if result.metadata.inputs else None
    task=TaskManifest('job',generation,instrument+'-S','trades',(instrument,),config,
        descriptor(result).features,(InputManifest('trades',acquisition.identity_digest,'revision1',state,binding),),
        (IntervalSpec('P',0,100),IntervalSpec('S',100,200)),'synthetic-output','serialized_destination',1,1048576)
    envelope=prepare_publication((result,),destination_scope=task.destination_scope,generation_id=task.generation_id,
        job_id=task.job_id,partition_id=task.partition_id,limits=SinkRequirements(max_results=1,
            max_chunk_bytes=1048576,max_total_bytes=1048576,max_result_cells=8192,max_evidence_rows=512))
    return TaskEvidence(task,(AcquiredInput('trades',acquisition,batch),),result,receipt,OutputManifest(task,envelope))
