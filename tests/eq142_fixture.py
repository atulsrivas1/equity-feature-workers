"""Load original hand-authored fixtures; never derive expected graphs from tools."""
from dataclasses import fields
from enum import Enum
import importlib
from importlib.util import find_spec
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'tests/fixtures/eq142_graph'
HAVE_INTRADAY=find_spec('equity_features_intraday') is not None


def available_cpu_slots():
    """Actual host bound for development tests, never a synthetic study host."""
    slots=os.cpu_count()
    if type(slots) is not int or slots<1:raise ValueError('Unknown development CPU count')
    if hasattr(os,'sched_getaffinity'):slots=min(slots,len(os.sched_getaffinity(0)))
    if os.name=='nt' and slots>=2:
        import ctypes
        from ctypes import wintypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.GetCurrentProcess.restype=wintypes.HANDLE
        kernel.GetProcessAffinityMask.argtypes=(wintypes.HANDLE,ctypes.POINTER(ctypes.c_size_t),ctypes.POINTER(ctypes.c_size_t))
        kernel.GetProcessAffinityMask.restype=wintypes.BOOL
        process=ctypes.c_size_t();system=ctypes.c_size_t()
        if not kernel.GetProcessAffinityMask(kernel.GetCurrentProcess(),ctypes.byref(process),ctypes.byref(system)):
            raise ctypes.WinError(ctypes.get_last_error())
        allowed=process.value.bit_count()
        if allowed<1:raise ValueError('Unknown Windows development CPU affinity')
        slots=min(slots,allowed)
    if slots<1:raise ValueError('Unknown development CPU affinity')
    return min(slots,8)


def require_cpu_slots(test,minimum):
    slots=available_cpu_slots()
    if slots<minimum:
        test.skipTest(f'EQ142 development CPU capacity requires {minimum} allowed slots; observed {slots}')
    return slots


def load(name):
    return json.loads((FIXTURES/name).read_bytes())


def qualification():
    from eq142_graph import bind_source, digest
    hooks = load('HOOK_INVENTORY.json')
    runtime_version='.'.join(map(str,sys.version_info[:3]))
    supported=load('PYTHON_RUNTIME_SOURCE_HASHES.json')['versions']
    if runtime_version not in supported:raise AssertionError('Unqualified Python runtime source version')
    from eq142_graph import digest
    for record in [*hooks['records'],*hooks['terminal_observations']]:
        if record['source_category']!='python_runtime':continue
        approved=supported[runtime_version]['modules'][record['source_module']]
        actual=digest(Path(importlib.import_module(record['source_module']).__file__).read_bytes())
        if actual not in approved['raw_sha256_variants'].values():raise AssertionError('Unexpected Python runtime file bytes')
        record['source_sha256']=actual
        record['definition_line']=approved['definitions'][record['definition']]
    hooks['runtime']='CPython '+runtime_version
    protocol = load('BENCHMARK_PROTOCOL.json')
    source = {'canonical_docs_head':'52f9525c52d7839e130fcfbc8559d9ab377d7a87',
        'package_build_sources':{'core':protocol['sources']['baseline_core_package_build'],
            'worker':protocol['sources']['baseline_worker_main'],'io':protocol['sources']['baseline_io_main']},
        'distributions':{'equity-feature-contracts':'0.0.4a4','equity-features':'0.0.4a4',
            'equity-features-intraday':'0.1.0a3','equity-feature-io-contracts':'0.1.0a2',
            'equity-feature-io-sdk':'0.1.0a2','equity-feature-workers':'0.1.0a14'},
        'installed_module_sha256':{r['source_module']:r['source_sha256'] for r in hooks['records'] if r['source_category']=='installed_package'},
        'tool_sha256':{'eq142_graph.py':digest((ROOT/'tools/eq142_graph.py').read_bytes())},
        'protocol_sha256':digest((FIXTURES/'BENCHMARK_PROTOCOL.json').read_bytes()),
        'expected_fixture_sha256':digest((FIXTURES/'CANONICAL_CASES.json').read_bytes())}
    bound=bind_source({'hooks':hooks,'source':source})
    return bound


def bound_inventory(bound):
    """The exact already-bound runtime variant, not an arbitrary new observation."""
    hooks=load('HOOK_INVENTORY.json')
    version='.'.join(map(str,sys.version_info[:3]))
    supported=load('PYTHON_RUNTIME_SOURCE_HASHES.json')['versions'][version]
    for record in [*hooks['records'],*hooks['terminal_observations']]:
        if record['source_category']=='python_runtime':
            approved=supported['modules'][record['source_module']]
            actual=digest_file(Path(importlib.import_module(record['source_module']).__file__))
            if actual not in approved['raw_sha256_variants'].values():raise AssertionError('Unexpected Python runtime file bytes')
            record['source_sha256']=actual;record['definition_line']=approved['definitions'][record['definition']]
    hooks['runtime']='CPython '+version
    return {'hooks':hooks,'source':bound.source}


def digest_file(path):
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def typed_batches(body):
    """Closed nine-type constructor, with no arbitrary module/name dispatch."""
    import equity_feature_contracts as c
    types = {name:getattr(c,name) for name in ('CanonicalBatch','Column','BatchMetadata','SourceBinding',
        'Coverage','PriceUnit','InputScope','IntervalCoverage','AdjustmentSpec')}
    def decode(v):
        if v is None or type(v) in (str,int,bool):return v
        if type(v) is not dict:raise ValueError('fixture shape')
        if set(v)=={'tuple'} and type(v['tuple']) is list:return tuple(decode(x) for x in v['tuple'])
        if set(v)=={'enum','value'} and v['enum']=='DataKind':return c.DataKind(v['value'])
        if set(v)=={'type','fields'} and v['type'] in types:
            cls=types[v['type']]
            if set(v['fields'])!={f.name for f in fields(cls)}:raise ValueError('fixture fields')
            return cls(**{k:decode(x) for k,x in v['fields'].items()})
        raise ValueError('fixture type')
    return decode(body)


def canonical_cases():
    from equity_feature_workers import decode_task
    for case in load('CANONICAL_CASES.json')['cases']:
        if 'task_json' in case:
            yield case, decode_task(case['task_json'].encode('ascii')), typed_batches(case['input_batches'])
