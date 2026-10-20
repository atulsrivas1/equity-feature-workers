"""Bounded fixed-codec proof readers, not complete-root or writer authority.

The root/controller selects the actual fixed namespace and expected basename.
These helpers cannot certify that caller-selected directory or reference origin.
Original SDK product codecs retain their own decoder/encoding; only development
records use decode_record's canonical wire format. No study command is enabled.
"""
from __future__ import annotations
import json
from pathlib import Path
import re
from types import MappingProxyType
from eq142_graph import digest,wire

INVOCATION=r'(?:parent|delegate-(?:00[0-9]|01[0-5]))[.]json'
REGISTRY={
    'products':{name:(1048576,r'[0-9a-f]{64}[.]json')
        for name in ('encode_task','encode_result','encode_output','encode_receipt')},
    'reference':{'eq142_development_reference':(16777216,r'reference[.]json')},
    'reference_preparation':{
        'eq142_product_projection':(65536,r'products[.]json'),
        'diagnostic_snapshot':(1048576,r'diagnostics[.]json')},
    'runner':{
        'eq142_development_runner':(1048576,r'runner[.]json'),
        'eq142_supervisor_outcome':(65536,r'supervisor[.]json'),
        'eq142_product_projection':(65536,r'products[.]json'),
        'eq142_publication_observation':(65536,r'publication[.]json'),
        'diagnostic_snapshot':(1048576,r'diagnostics[.]json')},
    'trace':{'eq142_local_fragment':(16777216,INVOCATION)},
    'owned_processes':{'eq142_owned_process_witness':(4096,INVOCATION)},
    'code_counts':{'eq142_code_counts':(65536,INVOCATION)},
    'sample':{
        'eq142_owned_runner_return':(4096,r'runner-reference[.]json'),
        'native_receipt':(16777216,r'native[.]json'),
        'capacity_receipt':(16384,r'capacity-(?:before|after)[.]json'),
        'eq142_complete_root':(65536,r'complete-root[.]json'),
        'eq142_measurement_envelope':(65536,r'measurement[.]json'),
        'eq142_complete_candidate':(4096,r'candidate[.]json'),
        'eq142_complete_root_failure':(1048576,r'failure[.]json')},
    'qualification':{
        'eq142_count_qualification':(1048576,r'count-qualification[.]json'),
        'capacity_receipt':(16384,r'qualification-capacity[.]json'),
        'eq142_complete_root_failure':(1048576,r'failure[.]json'),
        'eq142_study_admission':(16384,r'study-admission[.]json'),
        'eq142_study_summary':(2097152,r'study-summary[.]json')},
}
REGISTRY=MappingProxyType({scope:MappingProxyType(codecs) for scope,codecs in REGISTRY.items()})


def validate_reference(reference,*,scope,name):
    """Pure closed reference/name check against the published V9 registry."""
    if type(scope) is not str or scope not in REGISTRY or type(name) is not str:
        raise ValueError('proof fixed scope/name')
    if (type(reference) is not dict or any(type(key) is not str for key in reference)
            or set(reference)!={'codec','sha256','bytes','retained_name'}):
        raise ValueError('proof closed reference')
    codec=reference['codec']
    if type(codec) is not str or codec not in REGISTRY[scope]:raise ValueError('proof codec/context')
    maximum,pattern=REGISTRY[scope][codec]
    if (type(reference['sha256']) is not str or re.fullmatch('[0-9a-f]{64}',reference['sha256']) is None
            or type(reference['bytes']) is not int or not 1<=reference['bytes']<=maximum
            or type(reference['retained_name']) is not str or reference['retained_name']!=name
            or re.fullmatch(pattern,name) is None):raise ValueError('proof name/hash/length')
    if scope=='products' and name!=reference['sha256']+'.json':raise ValueError('proof product name/hash')


def check_body(body,reference,*,scope,name):
    """Exact immutable bytes only; no I/O, codec semantics or writer provenance."""
    validate_reference(reference,scope=scope,name=name)
    if (type(body) is not bytes or len(body)!=reference['bytes'] or digest(body)!=reference['sha256']):
        raise ValueError('proof changed body')
    return body


def decode_record(body,*,maximum):
    """Canonical development JSON only; original product codecs decode elsewhere."""
    if (type(maximum) is not int or not 1<=maximum<=16777216
            or type(body) is not bytes or not 1<=len(body)<=maximum):raise ValueError('proof record bound')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('proof duplicate JSON key')
            result[key]=value
        return result
    try:
        value=json.loads(body,object_pairs_hook=pairs)
        if wire(value)!=body:raise ValueError('proof noncanonical record')
    except (UnicodeError,TypeError,RecursionError,OverflowError) as error:
        raise ValueError('proof malformed record') from error
    return value


def read_proof(directory,reference,*,scope,name):
    """Bounded I/O adapter; root must prove fixed directory/stage ownership."""
    validate_reference(reference,scope=scope,name=name)
    root=Path(directory).absolute()
    def no_indirections(path):
        for entry in (path,*path.parents):
            if entry.is_symlink() or entry.is_junction():raise ValueError('proof namespace indirection')
    no_indirections(root);root=root.resolve(strict=True);no_indirections(root)
    if not root.is_dir():raise ValueError('proof directory')
    path=root/name
    if (path.is_symlink() or path.is_junction() or not path.is_file()
            or path.stat().st_size!=reference['bytes']):raise ValueError('proof file shape')
    with path.open('rb') as source:body=source.read(reference['bytes']+1)
    return check_body(body,reference,scope=scope,name=name)
