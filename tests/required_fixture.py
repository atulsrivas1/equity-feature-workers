"""Pre-code literal source/grid fixtures; expected values live in separate oracle."""
from dataclasses import replace
import equity_feature_contracts as c
from equity_feature_contracts.adapters import AcquisitionRequest, AdapterBatch, AdapterCapabilities
from equity_feature_contracts.buckets import BucketContext, VolumeBucket, BucketVolume
from equity_feature_contracts.history import HistoryContext
from equity_feature_contracts.relative import ReturnReference
from equity_feature_contracts.volume import TargetVolume
from equity_features.history import compute_history


SESSIONS=tuple(c.SessionSpec('demo','S'+str(i+1),i*100,(i+1)*100,'caller-supplied') for i in range(3))
UNIT=c.PriceUnit(0,'USD')
AVAILABILITY=c.AvailabilitySpec(300,310,310)


def daily(instrument='A',closes=(100,110,120)):
    facts=dict(instrument_id=(instrument,)*3,session_id=('S1','S2','S3'),start_ns=(0,100,200),end_ns=(100,200,300),
               open=closes,close=closes,high=tuple(v+1 for v in closes),low=tuple(v-1 for v in closes),
               volume=(100,200,600),known_at_ns=(100,200,300))
    return c.CanonicalBatch(c.DataKind.DAILY,tuple(c.Column(k,v) for k,v in facts.items()),
        c.BatchMetadata('demo',c.SourceBinding('synthetic','snapshot1','map1','daily-'+instrument),c.Coverage(3,3,True),UNIT,
                        scope=c.InputScope(0,300,'fixture-v1')))


def context(instrument='A',anchor=None):
    return HistoryContext(c.EntityKey(instrument,'S3'),'grid-v1',SESSIONS,(c.Coverage(1,1,True),)*3,anchor)


def config(feature='history.sma',instrument='A'):
    count=3 if feature in ('history.return','history.rsi','history.return_volatility') else 2
    prior=feature.startswith(('baseline.','relative.volume')) or feature in ('history.prior_high','history.prior_low')
    return c.ConfigSpec('fixture-'+feature+'-'+instrument,'v1',(c.Parameter('period',2),c.Parameter('evidence_limit',8)),
        SESSIONS[-1],c.WindowSpec(count,'S3',('S1','S2','S3'),'prior_only' if prior else 'completed_eod'),AVAILABILITY,price_unit=UNIT)


def request(kind=c.DataKind.DAILY,instrument='A'):
    return AcquisitionRequest('req-'+kind+'-'+instrument,kind,'demo',(instrument,),('S1','S2','S3'),0,300,'snapshot1',UNIT,
                              AVAILABILITY,max_batch_rows=3,max_rows=3,max_batches=1,selection='completed_intervals')


def bucket_inputs():
    bucket=VolumeBucket('first-half',0,50,'grid-v1')
    ctx=BucketContext(c.EntityKey('A','S3'),'grid-v1',SESSIONS,bucket,(c.Coverage(1,1,True),)*3)
    b=daily();b=replace(b,kind=c.DataKind.BAR,columns=tuple(c.Column(col.name,(50,150,250)) if col.name in ('end_ns','known_at_ns') else col for col in b.columns),
                       metadata=replace(b.metadata,source=replace(b.metadata.source,input_id='bucket-A')))
    return b,ctx,config('baseline.interval_volume')


def target_volume(cfg,ctx):
    source=c.InputBinding('target_volume',c.DataKind.DAILY,c.BatchMetadata('demo',c.SourceBinding('synthetic','snapshot1','map1','target-volume'),c.Coverage(1,1,True),UNIT))
    return TargetVolume(ctx.entity,600,300,source,0,c.InputScope(200,300,'all'),c.Coverage(1,1,True),'completed_eod','raw_shares')


def bucket_target(cfg,ctx):
    source=c.InputBinding('bucket_target',c.DataKind.BAR,c.BatchMetadata('demo',c.SourceBinding('synthetic','snapshot1','map1','target-bucket'),c.Coverage(1,1,True),UNIT))
    return BucketVolume(ctx.entity,600,250,source,0,c.InputScope(200,250,'all'),c.Coverage(1,1,True),ctx.bucket,'raw_shares')


def return_reference(instrument='A',closes=(100,110,120)):
    cfg=config('history.return',instrument);ctx=context(instrument)
    return ReturnReference(compute_history(daily(instrument,closes),cfg,context=ctx,feature_ids=('history.return',)),cfg,ctx)


class LiteralSource:
    def __init__(self,batch,requested): self.batch,self.requested,self.calls=batch,requested,0
    def capabilities(self): return AdapterCapabilities((self.requested.kind,),('demo',),(UNIT,),max_batch_rows=3)
    def iter_batches(self,requested,cancellation):
        self.calls+=1
        b=self.batch
        if b is None:
            yield AdapterBatch(requested.request_id,0,True,c.SourceBinding('synthetic','snapshot1','map1','absent'),
                c.Coverage(None,0,False),c.Coverage(None,0,False),None,'missing','synthetic missing')
        else:
            yield AdapterBatch(requested.request_id,0,True,b.metadata.source,b.metadata.coverage,c.Coverage(b.row_count,b.row_count,True),b)
