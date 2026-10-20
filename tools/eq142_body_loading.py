"""Bounded-set readers; fixed namespace/provenance stays with the owner.

Admit complete reference/reservation envelopes before bulk body reads. These
encoded bounds are not physical allocation or original execution guarantees.
"""
from pathlib import Path
from eq142_graph import wire
from eq142_proofs import validate_reference,read_proof,decode_record
from eq142_retained_trace import validate_reservations


def decode_original_reference(body):
    """Pure closed envelope, before optional calculators or source probes load."""
    from eq142_owned_runner import PROFILES,SERIES
    reference=decode_record(body,maximum=16777216)
    if (type(reference) is not dict
            or set(reference)!={'schema','stage','state','source_inventory','cpu_slots','profiles'}
            or reference['schema']!='eq142-development-reference:1'
            or reference['stage']!='qualification_only_not_frozen_study'
            or type(reference['state']) is not dict or set(reference['state'])!={'files','installed_runtime'}
            or type(reference['source_inventory']) is not dict
            or type(reference['cpu_slots']) is not int or not 4<=reference['cpu_slots']<=64
            or type(reference['profiles']) is not dict or not reference['profiles']
            or not set(reference['profiles'])<=set(PROFILES)):
        raise ValueError('original reference closed record')
    for profile in reference['profiles'].values():
        if (type(profile) is not dict or set(profile)!={'mathematics','products'}
                or type(profile['mathematics']) is not dict or type(profile['products']) is not dict
                or set(profile['products'])!=set(SERIES)):
            raise ValueError('original reference profile schema')
    return reference


def read_original_reference(path,original_return):
    """Strict original-record readback; caller authenticates actual return origin."""
    path=Path(path)
    body=read_proof(path.parent,{'codec':'eq142_development_reference',**original_return},
                    scope='reference',name='reference.json')
    reference=decode_original_reference(body)
    from benchmark_execution_baseline import read_reference,source_state
    actual=read_reference(path,original_return['sha256'])
    if wire(reference)!=wire(actual) or wire(reference['state'])!=wire(source_state()):
        raise ValueError('original reference exact source/runtime identity')
    return reference


def read_product_set(directory,references):
    if type(references) is not dict or not 1<=len(references)<=512:
        raise ValueError('product set fixed count')
    declared=0
    for sha,ref in references.items():
        if type(sha) is not str:raise ValueError('product set digest key')
        validate_reference(ref,scope='products',name=sha+'.json')
        if ref['sha256']!=sha:raise ValueError('product set key binding')
        declared+=ref['bytes']
    if declared>268435456:raise ValueError('product set aggregate declared byte cap')
    remaining=268435456;bodies={}
    for sha,ref in references.items():
        if ref['bytes']>remaining:raise ValueError('product set remaining allowance')
        body=read_proof(directory,ref,scope='products',name=sha+'.json')
        remaining-=len(body)
        bodies[sha]=body
    return bodies


def read_trace_set(directory,extended_references,plan):
    reservations=[{'id':'parent','file':'parent.json','max_spans':16384,'max_bytes':8388608},*plan['reservations']]
    validate_reservations(reservations)
    if type(extended_references) is not list or len(extended_references)!=len(reservations):
        raise ValueError('trace set reservation population')
    refs={};declared=0;spans=0
    for extended,reservation in zip(extended_references,reservations):
        if (type(extended) is not dict or set(extended)!={'reservation_id','codec','sha256','bytes','retained_name','span_count','owned_process_witness'}
                or extended['reservation_id']!=reservation['id']
                or type(extended['span_count']) is not int or not 0<=extended['span_count']<=reservation['max_spans']):
            raise ValueError('trace set closed source reservation')
        name=reservation['file'];ref={k:extended[k] for k in ('codec','sha256','bytes','retained_name')}
        validate_reference(ref,scope='trace',name=name)
        validate_reference(extended['owned_process_witness'],scope='owned_processes',name=name)
        if ref['codec']!='eq142_local_fragment' or extended['owned_process_witness']['codec']!='eq142_owned_process_witness':
            raise ValueError('trace set fixed codecs')
        if ref['bytes']>reservation['max_bytes'] or name in refs:
            raise ValueError('trace set per-reservation bound')
        declared+=ref['bytes'];spans+=extended['span_count'];refs[name]=ref
    if declared>67108864 or spans>131072:raise ValueError('trace set aggregate byte/span cap')
    remaining=67108864;bodies={}
    for name,ref in refs.items():
        if ref['bytes']>remaining:raise ValueError('trace set remaining allowance')
        body=read_proof(directory,ref,scope='trace',name=name)
        remaining-=len(body);bodies[name]=body
    return refs,bodies
