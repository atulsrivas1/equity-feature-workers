"""Retained full product bodies and validated local trace references.

Development proof storage only. Content deduplication here is not a calculation
cache, input certificate, physical-copy count or native IPC measurement.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re

from equity_feature_io_contracts import PublicationState
from equity_feature_io_sdk import encode_result,encode_receipt
from equity_feature_workers import encode_task,encode_output
from eq142_graph import digest,wire
from eq142_trace import validate_fragment,TraceSpecificationError,EXCLUSIVE_DISPOSITION

PRODUCT_CODECS=frozenset(('encode_task','encode_result','encode_output','encode_receipt'))


def bounded_read(path,maximum):
    """Read at most the declared limit plus one sentinel byte, even on mutation."""
    if path.is_symlink() or path.stat().st_size>maximum:raise ValueError('proof body bound')
    with path.open('rb') as source:body=source.read(maximum+1)
    if len(body)>maximum:raise ValueError('proof body bound')
    return body


def persist_record(path,value,*,max_bytes):
    """Exclusive bounded JSON publication; retain partial bytes on rejection."""
    path=Path(path)
    if type(max_bytes) is not int or not 1<=max_bytes<=16777216:raise ValueError('record bound')
    temporary=path.with_name(path.name+'.partial')
    if path.exists() or temporary.exists():raise ValueError('duplicate proof record')
    encoder=json.JSONEncoder(sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
    count=0
    with temporary.open('xb') as output:
        for chunk in encoder.iterencode(value):
            body=chunk.encode('ascii')
            if count+len(body)+1>max_bytes:raise ValueError('record bound')
            output.write(body);count+=len(body)
        output.write(b'\n');count+=1
    os.link(temporary,path);temporary.unlink()
    body=bounded_read(path,max_bytes)
    return {'sha256':digest(body),'bytes':count,'retained_name':path.name}


class ProductStore:
    """Own bounded proof directory; never overwrite an existing body."""
    def __init__(self,root,*,max_bytes=268435456,max_objects=512):
        if type(max_bytes) is not int or not 1<=max_bytes<=268435456 or type(max_objects) is not int or not 1<=max_objects<=512:
            raise ValueError('proof store bound')
        self.root=Path(root);self.max_bytes=max_bytes;self.max_objects=max_objects
        self.root.mkdir(parents=True,exist_ok=True)
        if self.root.is_symlink():raise ValueError('unowned proof directory')
        self.inventory={}
        for path in self.root.iterdir():
            if len(self.inventory)>=self.max_objects:raise ValueError('proof store bound')
            if (not path.is_file() or path.is_symlink() or path.suffix!='.json'
                    or re.fullmatch('[0-9a-f]{64}',path.stem) is None or path.stat().st_size>1048576):
                raise ValueError('unexpected proof member')
            body=bounded_read(path,min(1048576,self.max_bytes-sum(self.inventory.values())))
            if len(body)>1048576 or digest(body)!=path.stem:raise ValueError('changed proof member')
            self.inventory[path.stem]=len(body)
            self.check_limit()
        self.check_limit()

    def check_limit(self):
        if len(self.inventory)>self.max_objects or sum(self.inventory.values())>self.max_bytes:
            raise ValueError('proof store bound')

    def put(self,codec,body):
        if codec not in PRODUCT_CODECS or type(body) is not bytes or len(body)>1048576:
            raise ValueError('proof body bound/codec')
        sha=digest(body);path=self.root/(sha+'.json')
        if sha in self.inventory:
            if bounded_read(path,1048576)!=body:raise ValueError('proof body changed')
        else:
            if len(self.inventory)>=self.max_objects or sum(self.inventory.values())+len(body)>self.max_bytes:
                raise ValueError('proof store bound')
            with path.open('xb') as out:out.write(body)
            self.inventory[sha]=len(body)
        return {'codec':codec,'sha256':sha,'bytes':len(body),'retained_name':path.name}

    def verify(self,reference):
        if (type(reference) is not dict or set(reference)!={'codec','sha256','bytes','retained_name'}
                or type(reference['codec']) is not str or reference['codec'] not in PRODUCT_CODECS
                or type(reference['sha256']) is not str or re.fullmatch('[0-9a-f]{64}',reference['sha256']) is None
                or type(reference['retained_name']) is not str):
            raise ValueError('invalid proof reference')
        if reference['retained_name']!=reference['sha256']+'.json' or reference['sha256'] not in self.inventory:
            raise ValueError('unretained proof reference')
        path=self.root/reference['retained_name']
        if path.is_symlink() or path.stat().st_size>1048576:raise ValueError('changed proof reference')
        body=bounded_read(path,1048576)
        if type(reference['bytes']) is not int or len(body)!=reference['bytes'] or digest(body)!=reference['sha256']:
            raise ValueError('changed proof reference')
        return body


def original_reason(value):
    """Closed code taxonomy, not the unavailable original exception class.

    Supervisor dispositions take precedence for CANCELLED; shared sink/command
    spellings use their common sink vocabulary without claiming exception origin.
    """
    from equity_feature_io_contracts import SinkErrorCode
    from equity_feature_workers.commands import CommandErrorCode
    if value is None:return None
    if value in ('CALCULATION_FAILED','CANCELLED'):return {'domain':'supervisor','value':value}
    if value in {v.value for v in SinkErrorCode}:return {'domain':'sink','value':value}
    if value in {v.value for v in CommandErrorCode}:return {'domain':'command','value':value}
    raise ValueError('unknown original reason')


def diagnostic_projection(snapshot):
    """Original stable seven-field projection; full encoded snapshot is retained."""
    return [{'task_sha256':d.task_sha256,'intent_sha256':d.intent_sha256,'family':d.family,
        'family_sha256':d.family_sha256,'partition_sha256':d.partition_sha256,
        'status':d.status.value,'reason':d.reason} for d in snapshot.tasks]


def retain_diagnostics(snapshot,directory):
    body=snapshot.encode()
    if type(body) is not bytes or len(body)>1048576:raise ValueError('diagnostic bound')
    path=Path(directory)/'diagnostics.json'
    with path.open('xb') as out:out.write(body)
    return {'codec':'diagnostic_snapshot','sha256':digest(body),'bytes':len(body),'retained_name':path.name}


def record_products(outcome,snapshot,store,directory,*,sink=None):
    """Retain complete original codec bodies, including partial/cancelled results.

    Commit/readback is witnessed only from actual outputs and backend calls. A
    missing output with a reason does not prove absent commit or rollback.
    """
    diagnostics=retain_diagnostics(snapshot,directory);projection=diagnostic_projection(snapshot)
    records=[]
    for execution in outcome.tasks:
        task=execution.task;identity=task.task_sha256
        results=[] if execution.results is None else [store.put('encode_result',encode_result(r)) for r in execution.results]
        output=None;receipt=None;lookup='not_applicable';readback=[];commit='not_attempted'
        if execution.output is not None:
            output=store.put('encode_output',encode_output(execution.output))
            actual_receipt=execution.output.receipt
            if actual_receipt is not None:
                receipt=store.put('encode_receipt',encode_receipt(actual_receipt));commit='committed'
                if sink is not None:
                    try:
                        observed=sink.lookup(actual_receipt.idempotency_key)
                        if observed.state==PublicationState.COMMITTED and observed.receipt==actual_receipt:
                            lookup='committed_matching_receipt'
                            readback_values=sink.read(actual_receipt)
                            if type(readback_values) is not tuple or len(readback_values)!=len(results):
                                raise ValueError('invalid original readback shape')
                            readback_bodies=tuple(encode_result(r) for r in readback_values)
                        else:lookup='absent' if observed.state!=PublicationState.COMMITTED else 'original_failure'
                    except Exception:
                        # Post-outcome verification is not rewritten into the
                        # supervisor's original reason or fabricated rollback.
                        lookup='original_failure';readback=[]
                    if lookup=='committed_matching_receipt':
                        readback=[store.put('encode_result',body) for body in readback_bodies]
        elif sink is not None and execution.reason is not None:
            # The accepted supervisor may commit and then reject output metadata.
            # Without a retained actual receipt/key witness, state stays unknown.
            commit='unknown';lookup='original_failure'
        state='cancelled' if execution.reason=='CANCELLED' else 'failed' if execution.reason is not None else 'computed'
        disposition={'task_sha256':identity,'state':state,'reason':original_reason(execution.reason),
            'result_wire':results,'spill_wire':None,'output_wire':output,'commit_state':commit,'evidence':'observed'}
        if execution.spill is not None:raise ValueError('Spill is outside this frozen study; retain outcome before rejection')
        records.append({'task_sha256':identity,'disposition':disposition,'task_wire':store.put('encode_task',encode_task(task)),
            'results':results,'output':output,'receipt':receipt,'committed_lookup':lookup,'readback_results':readback,
            'diagnostics':[d for d in projection if d['task_sha256']==identity],'raw_diagnostics':diagnostics})
    return records


def stable_products(records):
    """Drop raw volatile diagnostics reference only; keep all product identities."""
    return [{key:value for key,value in r.items() if key!='raw_diagnostics'} for r in records]


def combine_fragments(directory,reservations):
    """Bounded filesystem adapter to pure retained trace closure.

    Only declared bodies are read here. Full fixed-namespace ownership and writer
    reference provenance remain mandatory complete-root/controller checks.
    """
    from eq142_retained_trace import validate_reservations,_combine_records
    root=Path(directory)
    if root.is_symlink():raise TraceSpecificationError('missing_sidecar')
    validate_reservations(reservations)
    def records():
        for reservation in reservations:
            path=root/reservation['file']
            if not path.is_file() or path.is_symlink():raise TraceSpecificationError('missing_sidecar')
            try:body=bounded_read(path,reservation['max_bytes'])
            except ValueError:raise TraceSpecificationError('byte_limit') from None
            yield reservation,body
    return _combine_records(records())
