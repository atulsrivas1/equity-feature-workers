"""Load original hand-authored fixtures; never derive expected graphs from tools."""
from dataclasses import fields
from enum import Enum
import importlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'tests/fixtures/eq142_graph'


def load(name):
    return json.loads((FIXTURES/name).read_bytes())


def qualification():
    from eq142_graph import bind_source, digest
    hooks = load('HOOK_INVENTORY.json')
    protocol = load('BENCHMARK_PROTOCOL.json')
    source = {'canonical_docs_head':'a54f9196cada1fc9b3bea334711b4e4db9ac1d81',
        'package_build_sources':{'core':protocol['sources']['baseline_core_package_build'],
            'worker':protocol['sources']['baseline_worker_main'],'io':protocol['sources']['baseline_io_main']},
        'distributions':{'equity-feature-contracts':'0.0.4a4','equity-features':'0.0.4a4',
            'equity-features-intraday':'0.1.0a3','equity-feature-io-contracts':'0.1.0a2',
            'equity-feature-io-sdk':'0.1.0a2','equity-feature-workers':'0.1.0a14'},
        'installed_module_sha256':{r['source_module']:r['source_sha256'] for r in hooks['records'] if r['source_category']=='installed_package'},
        'tool_sha256':{'eq142_graph.py':digest((ROOT/'tools/eq142_graph.py').read_bytes())},
        'protocol_sha256':digest((FIXTURES/'BENCHMARK_PROTOCOL.json').read_bytes()),
        'expected_fixture_sha256':digest((FIXTURES/'CANONICAL_CASES.json').read_bytes())}
    return bind_source({'hooks':hooks,'source':source})


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
