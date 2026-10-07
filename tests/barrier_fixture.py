"""Pre-code owned synthetic member proofs; literal aggregate oracle is separate."""
import json
from pathlib import Path
import equity_feature_contracts as c
from equity_feature_contracts.breadth import DeclaredUniverseSpec, BreadthSpec, MemberFeatures, SMAInput, CompletedClose
from equity_features.history import compute_sma_reference
from required_fixture import context, config, daily, return_reference

ORACLE=json.loads(Path(__file__).with_name('barrier_oracle.json').read_text(encoding='utf-8'))
UNIVERSE=DeclaredUniverseSpec('demo','S3','synthetic-universe','membership-v1',200,('A','B','C'))
AGGREGATE=BreadthSpec(c.EntityKey('UNIVERSE','S3'))


def member(instrument):
    closes=tuple(ORACLE['closes'][instrument])
    b=daily(instrument,closes);ctx=context(instrument);cfg=config('history.sma',instrument)
    sma=SMAInput(compute_sma_reference(b,cfg,context=ctx),cfg,ctx)
    source=c.InputBinding('daily_history',c.DataKind.DAILY,b.metadata)
    close=CompletedClose(ctx.entity,closes[-1],300,source,2,c.InputScope(200,300,'fixture-v1'),c.Coverage(1,1,True))
    return MemberFeatures(ctx.entity,return_reference(instrument,closes),sma,close)
