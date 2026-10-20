"""Pure retained trace closure; no filesystem, probes or complete sample authority.

Full root readers still bind original writer references, source, fixed namespaces,
native ownership and products. Byte bounds describe encoded evidence, not heap.
"""
from __future__ import annotations
import json
import re
from eq142_graph import digest,wire
from eq142_trace import validate_fragment,TraceSpecificationError,EXCLUSIVE_DISPOSITION


def validate_reservations(reservations):
    if type(reservations) not in (tuple,list) or not 1<=len(reservations)<=17:
        raise TraceSpecificationError('missing_sidecar')
    for reservation in reservations:
        if (type(reservation) is not dict or not {'id','file','max_bytes','max_spans'}<=set(reservation)
                or type(reservation['file']) is not str
                or not 1<=len(reservation['file'])<=128 or not re.fullmatch('[a-zA-Z0-9_.-]+[.]json',reservation['file'])
                or type(reservation['id']) is not str or not re.fullmatch('[a-z][a-z0-9_.:-]{0,63}',reservation['id'])
                or type(reservation['max_bytes']) is not int or not 4096<=reservation['max_bytes']<=16777216
                or type(reservation['max_spans']) is not int or not 1<=reservation['max_spans']<=131072):
            raise TraceSpecificationError('unknown_invocation')
    # Admit the entire reservation envelope before reading or parsing bodies.
    if sum(r['max_bytes'] for r in reservations)>67108864:raise TraceSpecificationError('byte_limit')
    if sum(r['max_spans'] for r in reservations)>131072:raise TraceSpecificationError('event_limit')
    names=[r['file'] for r in reservations];ids=[r['id'] for r in reservations]
    if len(set(names))!=len(names) or len(set(ids))!=len(ids):
        raise TraceSpecificationError('duplicate_sidecar')


def decode_fragment(body):
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise TraceSpecificationError('collector_error')
            value[key]=item
        return value
    try:
        value=json.loads(body,object_pairs_hook=pairs)
        if wire(value)!=body:raise TraceSpecificationError('collector_error')
        return value
    except (ValueError,TypeError,UnicodeError,RecursionError,OverflowError) as error:
        if isinstance(error,TraceSpecificationError):raise
        raise TraceSpecificationError('collector_error') from error


def combine_retained_fragments(bodies,reservations):
    """Recompute all joins from exact bounded bodies, never cached summaries."""
    validate_reservations(reservations)
    if type(bodies) is not dict or set(bodies)!={r['file'] for r in reservations}:
        raise TraceSpecificationError('missing_sidecar')
    if any(type(bodies[r['file']]) is not bytes or not 1<=len(bodies[r['file']])<=r['max_bytes']
            for r in reservations):raise TraceSpecificationError('byte_limit')
    if sum(len(body) for body in bodies.values())>67108864:raise TraceSpecificationError('byte_limit')
    return _combine_records((reservation,bodies[reservation['file']]) for reservation in reservations)


def _combine_records(records):
    """Shared reducer; the live I/O adapter streams one bounded body at a time."""
    timelines={};processes={};counters={};refs=[];event_ids=set()
    for reservation,body in records:
        fragment=decode_fragment(body);validate_fragment(fragment)
        if fragment['reservation_id']!=reservation['id'] or len(fragment['spans'])>reservation['max_spans']:
            raise TraceSpecificationError('unknown_invocation')
        process=fragment['process_binding'];nonce=process['process_startup_nonce']
        binding={'pid':process['pid'],'clock':fragment['clock']}
        if nonce in processes and processes[nonce]!=binding:raise TraceSpecificationError('unknown_timeline')
        processes[nonce]=binding
        for timeline in fragment['timeline_bindings']:
            record={'process_startup_nonce':nonce,**timeline}
            if timeline['id'] in timelines and timelines[timeline['id']]!=record:
                raise TraceSpecificationError('unknown_timeline')
            timelines[timeline['id']]=record
        for span in fragment['spans']:
            if span['id'] in event_ids:raise TraceSpecificationError('duplicate_sidecar')
            event_ids.add(span['id'])
        for counter in fragment['counters']:
            key=(counter['operation'],counter['scope_id'],counter['byte_observation'])
            if key not in counters and len(counters)>=2048:raise TraceSpecificationError('map_limit')
            prior=counters.setdefault(key,{**counter,'attempts':0,'returned':0,'rejected':0,
                'returned_observed_bytes':None if counter['returned_observed_bytes'] is None else 0})
            for field in ['attempts','returned','rejected']:prior[field]+=counter[field]
            if prior['returned_observed_bytes'] is not None:prior['returned_observed_bytes']+=counter['returned_observed_bytes']
        refs.append({'reservation_id':reservation['id'],'codec':'eq142_local_fragment','sha256':digest(body),
            'bytes':len(body),'retained_name':reservation['file'],'span_count':len(fragment['spans']),
            'owned_process_witness':fragment['owned_process_witness']})
    if len(event_ids)>131072 or sum(r['bytes'] for r in refs)>67108864 or len(counters)>2048:
        raise TraceSpecificationError('event_limit')
    return {'representation':'retained_fragments','fragments':refs,'total_span_count':len(event_ids),
        'processes':processes,'timelines':timelines,'counters':[counters[k] for k in sorted(counters)],
        'exclusive_cost_disposition':EXCLUSIVE_DISPOSITION,'global_numeric_critical_path_ns':None,
        'native_ipc_encoded_bytes':None}
