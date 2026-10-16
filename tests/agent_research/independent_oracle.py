"""Independent test-only data reconstruction and JSON/SHA event oracle.

No import from the new consumer implementation, private codecs or calculators.
Source values are the immutable pre-code oracle, not generated expectations.
"""
from __future__ import annotations
from enum import Enum
from pathlib import Path
import hashlib
import json
from typing import Any
import equity_feature_contracts as c


def literal_oracle() -> dict[str, Any]:
    return json.loads((Path(__file__).resolve().parents[2]/'docs/stories/EQ-109_ORACLES.json').read_bytes())


def fixture() -> tuple[c.ConfigSpec,c.CanonicalBatch,c.FeatureResult]:
    raw=literal_oracle()
    classes={name:getattr(c,name) for name in ('ConfigSpec','Parameter','SessionSpec','WindowSpec','AvailabilitySpec',
        'PriceUnit','CanonicalBatch','Column','BatchMetadata','SourceBinding','Coverage','InputScope','AdjustmentSpec',
        'FeatureResult','FeatureColumn','QualityRow','EntityKey','ResultMetadata','InputBinding')}
    enums={'inputs.DataKind':c.DataKind,'results.ValueType':c.ValueType,'results.Status':c.Status,'results.Reason':c.Reason}
    def build(v: Any) -> Any:
        if type(v)is list:return tuple(build(x) for x in v)
        if type(v)is not dict:return v
        if set(v)=={'int'}:return int(v['int'])
        if set(v)=={'float64'}:return float.fromhex(v['float64'])
        if set(v)=={'enum','value'}:return enums[v['enum']](v['value'])
        if set(v)=={'fields','record'} or set(v)=={'fields','type'}:
            name=v.get('record',v.get('type')).split('.')[1]
            fields={k:build(x) for k,x in v['fields'].items()}
            if name=='CanonicalBatch':fields['kind']=c.DataKind(fields['kind'])
            return classes[name](**fields)
        raise AssertionError('oracle shape')
    return build(raw['configuration']['value']['value']),build(raw['batch']['value']['value']),build(raw['producer']['native_result'])


def canonical(v: object) -> bytes:
    return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')


def event_bytes(event: Any) -> bytes:
    """Separate explicit public record projection, no production event encoder."""
    def context(v: Any) -> Any:
        if v is None:return None
        return dict(namespace=v.namespace,session_id=v.session_id,availability=dict(
            market_cutoff_ns=v.availability.market_cutoff_ns,knowledge_cutoff_ns=v.availability.knowledge_cutoff_ns,
            evaluation_ns=v.availability.evaluation_ns,mode=v.availability.mode,reconstruction_reason=v.availability.reconstruction_reason))
    def fp(v: Any) -> Any:
        if v is None:return None
        return dict(domain=v.domain,digest=v.digest,byte_length=v.byte_length,schema_version=v.schema_version,
            canonicalization=v.canonicalization,algorithm=v.algorithm)
    def ref(v: Any) -> Any:
        return dict(chain_id=v.chain_id,sequence=v.sequence,event_id=v.event_id,kind=v.kind.value,digest=v.digest)
    def payload(v: Any) -> Any:
        return dict(kind=v.kind.value,digest=v.digest,retained_bytes=v.retained_bytes,full_bytes=v.full_bytes,state=v.state.value)
    def time(v: Any) -> Any:
        return dict(at_ns=v.at_ns,clock_source=v.clock_source,uncertainty_ns=v.uncertainty_ns)
    v=event.subject;kind=event.kind.value
    if kind=='request':subject=dict(kind=kind,request_key=v.request_key,objective=v.objective,context=context(v.context))
    elif kind=='model_declaration':subject=dict(kind=kind,provider=v.provider,model=v.model,version=v.version,identity_state=v.identity_state.value)
    elif kind=='tool_call':
        assert not v.arguments.entries
        subject=dict(kind=kind,request=ref(v.request),tool_name=v.tool_name,tool_version=v.tool_version,
            arguments=dict(state=v.arguments.state.value,total_count=v.arguments.total_count,entries=[]))
    elif kind=='tool_result':subject=dict(kind=kind,call=ref(v.call),outcome=v.outcome.value,payload=payload(v.payload))
    elif kind=='feature_read':subject=dict(kind=kind,result=ref(v.result),receipt=payload(v.receipt),configuration=fp(v.configuration),
        inputs=[dict(role=x.role,input_id=x.input_id,content=fp(x.content)) for x in v.inputs],output=fp(v.output),feature_ids=list(v.feature_ids),context=context(v.context))
    elif kind=='decision':subject=dict(kind=kind,request=ref(v.request),feature_reads=[ref(x) for x in v.feature_reads],action=v.action.value,reason_code=v.reason_code)
    else:raise AssertionError('unexpected event kind')
    return canonical(dict(actor=dict(actor_id=event.actor.actor_id,kind=event.actor.kind.value),
        canonicalization='agent-event-json-v1',chain_id=event.chain_id,domain='equity-feature.agent-event',event_id=event.event_id,
        occurred=time(event.occurred),previous_hash=event.previous_hash,recorded=time(event.recorded),schema_version='1',sequence=event.sequence,subject=subject))


def independent_chain(events: tuple[Any,...]) -> tuple[tuple[bytes,...],str]:
    previous='0'*64;wires=[]
    for index,event in enumerate(events,1):
        assert event.sequence==index and event.previous_hash==previous
        raw=event_bytes(event);assert len(raw)<=16384
        previous=hashlib.sha256(raw).hexdigest();wires.append(raw)
    return tuple(wires),previous
