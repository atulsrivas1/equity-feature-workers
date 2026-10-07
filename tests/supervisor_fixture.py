"""Pre-code synthetic whole-instrument facts and independent row/result oracle."""
from dataclasses import replace
import json
from pathlib import Path
import equity_feature_contracts as c
from equity_features.session import compute_bars
from manifest_fixture import fixture

ORACLE=json.loads(Path(__file__).with_name('supervisor_oracle.json').read_text(encoding='utf-8'))

def facts(instrument):
    task,_,_=fixture()
    columns=dict(instrument_id=(instrument,)*2,session_id=('S',)*2,start_ns=(100,150),end_ns=(150,200),
        known_at_ns=(150,210),open=(100,102),high=(103,104),low=(99,101),close=(102,103),
        volume=(200,300),actual_notional=(20300,30900))
    batch=c.CanonicalBatch(c.DataKind.BAR,tuple(c.Column(k,v) for k,v in columns.items()),
        c.BatchMetadata('demo',c.SourceBinding('synthetic','snapshot1','map1','bars-'+instrument),
            c.Coverage(2,2,True),c.PriceUnit(0,'USD'),scope=c.InputScope(100,200,'example-v1')))
    return task.config,batch

def check_pure_oracle():
    ledger=[]
    for instrument in ORACLE['instruments']:
        config,batch=facts(instrument)
        result=compute_bars(batch,config,entity=c.EntityKey(instrument,'S'))
        values={col.feature_id:col.values[0] for col in result.values}
        assert all(values[k]==v for k,v in ORACLE['per_instrument'].items())
        ledger.extend([[instrument,i] for i in range(batch.row_count)])
    assert ledger==ORACLE['row_ledger'] and len(ledger)==ORACLE['distinct_rows']
    return ledger
