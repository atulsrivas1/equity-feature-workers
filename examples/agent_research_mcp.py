"""Fixed external official-ClientSession research consumer.

Caller owns initialization, transport, credentials, ledger path and process cleanup.
Only synthetic fixed operations are issued. No runtime loading or tool selectors
are accepted from an MCP request. This source is not a packaged API.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any,Literal,cast
import anyio
# Pinned reference jsonschema has no py.typed/stubs; only this import is untyped.
# Runtime validation remains mandatory for every received public response.
from jsonschema import Draft202012Validator,ValidationError,SchemaError  # type: ignore[import-untyped]
import agent_research as r


@dataclass(frozen=True)
class CollectedResearch:
    assessment: r.Assessment
    replay: r.ReplayObservation
    calls: tuple[tuple[str,bytes,str],...]
    events: tuple[Any,...]
    retained_payloads: tuple[bytes,...]
    artifact_reference: str
    summary_reference: str


def _canonical(value: object) -> bytes:
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')


def _check(ok: bool) -> None:
    if not ok:raise r.ResearchExampleError('inconsistent_evidence') from None


def _unavailable(response: dict[str,Any]) -> bool:
    if response['ok']:return False
    failure=response['failure']
    # The accepted failure union has distinct client and MCP branches.
    # Local reference_expired has no HTTP status and never refreshes authority.
    return bool((failure.get('source')=='client' and failure.get('status') in (401,403))
        or (failure.get('source')=='mcp' and failure.get('code')=='reference_expired'))


async def collect(session: Any, *, mode: Literal['enabled','uncatalogued','disabled']='enabled',
                  narrative: str='session.trade.count=3', retain_input: bool=True,
                  cancel_replay: bool=False) -> CollectedResearch:
    """Consume this owner's fixed synthetic profile; never automatically retry.

    All public tool response text bytes are retained exactly as received and
    compared with structuredContent. JSON-RPC framing is the caller's separate
    transport observation. Missing/report flags never create supplied content.
    """
    _check(type(mode) is str and mode in ('enabled','uncatalogued','disabled'))
    _check(type(retain_input) is bool and type(cancel_replay) is bool)
    listed=await session.list_tools()
    _check(len(listed.tools)==12 and {tool.name for tool in listed.tools}==set(r._TOOL_NAMES))
    schemas={tool.name:tool.outputSchema for tool in listed.tools}
    calls: list[tuple[str,bytes,str]]=[]
    aggregate=0
    async def call(name: str,arguments: dict[str,object]) -> dict[str,Any]:
        nonlocal aggregate
        _check(len(calls)<32)
        answer=await session.call_tool(name,arguments)
        _check(len(answer.content)==1 and answer.content[0].type=='text')
        value=answer.structuredContent
        _check(type(value) is dict and type(value.get('ok')) is bool)
        text=answer.content[0].text
        _check(type(text) is str and len(text)<=65536 and text.isascii())
        raw=text.encode('ascii')
        _check(len(raw)<=65536 and _canonical(value)==raw and answer.isError is (not value['ok']))
        try:Draft202012Validator(schemas[name]).validate(value)
        except (ValidationError,SchemaError):raise r.ResearchExampleError('inconsistent_evidence') from None
        aggregate+=len(raw);_check(aggregate<=1048576)
        calls.append((name,raw,'succeeded' if value['ok'] else 'failed'))
        return cast(dict[str,Any],value)
    capability=await call('equity_evidence_capabilities',{})
    _check(capability['ok'] and capability['payload']['enabled']==(mode!='disabled'))
    key='eq109-new-uncatalogued' if mode=='uncatalogued' else 'eq109-static-seed'
    admitted=await call('equity_calculate',dict(profile='cmd',idempotency_key=key))
    _check(admitted['ok']);reference=admitted['payload']['reference']
    for _ in range(1 if mode=='enabled' else 20):
        status=await call('equity_job_status',dict(reference=reference))
        _check(status['ok'])
        if status['payload']['data']['state']=='succeeded':break
        await anyio.sleep(.02)
    _check(status['payload']['data']['state']=='succeeded')
    summary=await call('equity_result_summary',dict(reference=reference))
    r._summary_bound(summary)
    artifact=await call('equity_artifact_reference',dict(reference=reference))
    _check(artifact['ok']);handle=artifact['payload']['reference']
    reports: dict[str,bytes]={}
    report_total=0
    grant_denied=False
    for kind,tool in (('producer','equity_evidence_read'),('receipt','equity_receipt'),
                      ('explanation','equity_explanation'),('diagnostics','equity_diagnostics')):
        arguments=dict(reference=handle,kind=kind,offset=0) if kind=='producer' else dict(reference=handle)
        response=await call(tool,arguments)
        if _unavailable(response):
            grant_denied=True
            break
        _check(response['ok']);payload=response['payload']
        _check(payload['reference']==handle and payload['kind']==kind and type(payload['offset']) is int and payload['offset']==0)
        if mode!='enabled':
            _check(payload['presence']==('not_enabled' if mode=='disabled' else 'not_supplied'))
            _check(all(payload[key] is None for key in ('quality','artifact','chunk','next_offset')) and payload['complete'] is False)
            continue
        _check(payload['presence']=='supplied')
        descriptor=payload['artifact'];quality=payload['quality']
        _check(type(descriptor) is dict and type(descriptor['byte_length']) is int and 0<descriptor['byte_length']<=32768)
        _check(descriptor['input_content_verified'] is False and descriptor['source_authenticated'] is False and descriptor['execution_verified'] is False)
        _check(re.fullmatch('[0-9a-f]{64}',descriptor['sha256']) is not None)
        representation={'producer':'public_producer_json','receipt':'execution_receipt',
                        'explanation':'feature_explanation','diagnostics':'equity.mcp.diagnostics.v1'}[kind]
        _check(descriptor['schema_version']=='1' and descriptor['representation']==representation)
        combined=bytearray()
        while True:
            _check(payload['reference']==handle and payload['kind']==kind and type(payload['offset']) is int and payload['offset']==len(combined))
            _check(payload['artifact']==descriptor and payload['quality']==quality and payload['presence']=='supplied')
            chunk=payload['chunk'].encode('ascii')
            _check(0<len(chunk)<=4096 and len(combined)+len(chunk)<=descriptor['byte_length'])
            combined.extend(chunk)
            _check(type(payload['complete']) is bool)
            if payload['complete']:
                _check(payload['next_offset'] is None and len(combined)==descriptor['byte_length'])
                break
            _check(len(chunk)==4096 and type(payload['next_offset']) is int and payload['next_offset']==len(combined))
            response=await call('equity_evidence_read',dict(reference=handle,kind=kind,offset=len(combined)))
            if _unavailable(response):
                grant_denied=True
                break
            _check(response['ok']);payload=response['payload']
        if grant_denied:break
        raw=bytes(combined)
        _check(hashlib.sha256(raw).hexdigest()==descriptor['sha256'])
        report_total+=len(raw);_check(report_total<=65536)
        reports[kind]=raw
    cfg,batch,result=r.fixture()
    if mode=='enabled' and not grant_denied:
        _check(reports['producer']==_canonical(summary['payload']['data']))
    retained_inputs=(('trades',batch),) if retain_input else ()
    receipt,explanation,diagnostics=(reports.get(kind) for kind in ('receipt','explanation','diagnostics'))
    assessment=r.assess(cfg,retained_inputs,result,receipt,explanation,diagnostics,narrative)
    observed=r.replay(cfg,retained_inputs,result,receipt,explanation,diagnostics,narrative,cancel_before_start=cancel_replay)
    events=r.history(tuple(calls),assessment,cfg,retained_inputs,result,receipt)
    # Canonical public response text and assembled report bytes are distinct.
    retained=tuple(raw for _,raw,_ in calls)+tuple(reports.values())
    return CollectedResearch(assessment,observed,tuple(calls),events,retained,handle,reference)
