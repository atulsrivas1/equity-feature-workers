"""Original supplied synthetic study inputs and independent scalar expectations.

Outside calculations/timing: exact original generator, selected-family variants,
expanded-weight/centered Decimal oracle under a completely explicit Context.
"""
from dataclasses import replace
from decimal import Context,localcontext,ROUND_HALF_EVEN,InvalidOperation,DivisionByZero,Overflow
import hashlib
import math
import pickle

import benchmark_supplied_inputs as original
import equity_feature_contracts as c
from equity_feature_workers import InputManifest,WorkItem
from equity_feature_workers.intraday import intraday_headers,intraday_history,compute_intraday_inputs

PROFILES=('small','balanced','skewed','family_complete','family_missing_hl','family_old_close_gap')
CONTEXT=Context(prec=350,rounding=ROUND_HALF_EVEN,Emin=-999999,Emax=999999,capitals=1,clamp=0,
    flags=[],traps=[InvalidOperation,DivisionByZero,Overflow])
RELATIVE_TOLERANCE=2e-15
ABSOLUTE_TOLERANCE=1e-12


def items_for(profile):
    if profile not in PROFILES:raise ValueError('unknown profile')
    if profile in original.PROFILES:return original.items_for(profile)[0]
    (item,),_=original.items_for('small');batch=item.batches[0]
    variant={'family_complete':'complete','family_missing_hl':'missing_hl','family_old_close_gap':'old_close_gap'}[profile]
    if variant=='missing_hl':
        batch=replace(batch,columns=tuple(col for col in batch.columns if col.name not in ('high','low')))
    if variant=='old_close_gap':
        batch=replace(batch,columns=tuple(c.Column(col.name,col.values[1:]) for col in batch.columns),
            metadata=replace(batch.metadata,coverage=c.Coverage(256,255,False),interval_coverage=
                (replace(batch.metadata.interval_coverage[0],coverage=c.Coverage(1,0,False)),)+batch.metadata.interval_coverage[1:]))
    batches=(batch,)
    groups=(('intraday.ema','intraday.rsi'),('intraday.atr',),tuple(f for f in original.FEATURES if f.startswith('intraday.band_')))
    result=[]
    for index,selected in enumerate(groups):
        history=intraday_history(item.task.config,selected)
        manifest=InputManifest('bars:000',hashlib.sha256(pickle.dumps(batch,protocol=5)).hexdigest(),'revision1',
            'complete' if batch.metadata.coverage.complete else 'partial',c.InputBinding('bars:000',batch.kind,batch.metadata))
        task=replace(item.task,job_id='eq142-'+variant,partition_id='family'+str(index),
            features=intraday_headers(selected),inputs=(manifest,),ordered_history=history[0],
            warmup_sessions=history[1],initialization_sha256=history[2])
        result.append(WorkItem(task,batches,compute_intraday_inputs))
    return tuple(result)


def independent_expectations(profile,items):
    if profile not in PROFILES:raise ValueError('unknown profile')
    result={}
    for item in items:
        slots=256 if profile.startswith('family_') else item.rows
        with localcontext(CONTEXT):
            values=original.original_oracle(original.prices_for(slots))
        cells={}
        for header in item.task.features:
            feature=header.feature_id
            band=feature.startswith('intraday.band_')
            status='available';expected=20 if band else slots;observed=expected;reasons=[]
            if profile=='family_missing_hl' and feature=='intraday.atr':
                status='missing_input';observed=1;reasons=['absent_input']
            if profile=='family_old_close_gap' and not band:
                status='incomplete_coverage';observed=255;reasons=['governed_gap']
            cells[feature]={'value':values[feature] if status=='available' else None,
                'status':status,'expected':expected,'observed':observed,'reasons':reasons}
        result[item.task.task_sha256]={'cells':cells,'evidence':[],
            'selected_headers':[h.feature_id for h in item.task.features]}
    return result


def verify_result(task,result,expected):
    """Compare independent math/readiness, not just observed baseline hashes."""
    actual={v.feature_id:v for v in result.values}
    quality={q.feature_id:q for q in result.quality}
    if list(actual)!=expected['selected_headers'] or set(quality)!=set(actual) or result.evidence:
        raise AssertionError('selected result/quality/evidence mismatch')
    for feature,want in expected['cells'].items():
        column=actual[feature];q=quality[feature]
        if len(column.values)!=1 or (q.status.value,q.expected,q.observed,[r.value for r in q.reasons])!=(want['status'],want['expected'],want['observed'],want['reasons']):
            raise AssertionError('independent quality mismatch: '+feature)
        value=column.values[0]
        if want['value'] is None:
            if value is not None:raise AssertionError('independent null mismatch: '+feature)
        elif type(value) is not float or not math.isfinite(value) or not math.isclose(value,want['value'],rel_tol=RELATIVE_TOLERANCE,abs_tol=ABSOLUTE_TOLERANCE):
            raise AssertionError('independent numeric mismatch: '+feature)
