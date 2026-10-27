"""New public governed-history fixture with independent rational expectations."""
from equity_feature_contracts import (SessionSpec,EntityKey,HistoryContext,Coverage,
    CanonicalBatch,Column,DataKind,BatchMetadata,SourceBinding,PriceUnit,ConfigSpec,
    Parameter,WindowSpec,AvailabilitySpec)
from equity_features.history import compute_history


def history_fixture(*,future_close=999,future_known=None,target_close=130):
    sessions=tuple(SessionSpec('demo','S'+str(i),i*10+1,i*10+10,'synthetic') for i in range(7))
    context=HistoryContext(EntityKey('A','S5'),'conformance-grid1',sessions,(Coverage(1,1,True),)*7)
    values=(('instrument_id',('A',)*7),('session_id',tuple(s.session_id for s in sessions)),
        ('start_ns',tuple(s.open_ns for s in sessions)),('end_ns',tuple(s.close_ns for s in sessions)),
        ('close',(100,110,105,120,115,target_close,future_close)),
        ('known_at_ns',(10,20,30,40,50,60,future_known)))
    batch=CanonicalBatch(DataKind.DAILY,tuple(Column(k,v) for k,v in values),
        BatchMetadata('demo',SourceBinding('synthetic','history-snapshot1','history-map1','daily1'),
            Coverage(7,7,True),PriceUnit(0,'USD')))
    config=ConfigSpec('conformance-history','v1',(Parameter('period',3),Parameter('evidence_limit',0)),
        sessions[5],WindowSpec(4,'S5',tuple(s.session_id for s in sessions),'completed_eod'),
        AvailabilitySpec(60,65,65),price_unit=PriceUnit(0,'USD'))
    return batch,context,config


def compute(batch,context,config):
    return compute_history(batch,config,context=context,feature_ids=('history.return',))
