"""Independent foundation expectations, no backend/source data required."""
import importlib
import importlib.abc
from importlib.metadata import distribution
from pathlib import Path
import sys

class Deny(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('equity_feature_io', 'equity_feature_workers', 'equity_feature_duckdb')) or fullname.split('.')[0] in {'duckdb','pyarrow','numpy','requests','httpx'}:
            raise AssertionError('Core imported forbidden dependency: ' + fullname)

def core_check():
    deny=Deny();sys.meta_path.insert(0,deny)
    try:
        import equity_feature_contracts as c
        import equity_features as f
        from equity_features.session import compute_bars
        assert c.__version__ == f.__version__ == '0.0.4a4'
        assert len(c.builtin_registry().list_features()) == 39
        unit=c.PriceUnit(0,'USD')
        config=c.ConfigSpec('synthetic-bars','v1',(c.Parameter('eligibility_policy','example-v1'),),c.SessionSpec('demo','S',100,200,'caller-supplied'),c.WindowSpec(1,'S',('P','S')),c.AvailabilitySpec(200,210,210),price_unit=unit)
        facts=(('instrument_id',('A','A')),('session_id',('S','S')),('start_ns',(100,150)),('end_ns',(150,200)),('known_at_ns',(150,210)),('open',(100,102)),('high',(103,104)),('low',(99,101)),('close',(102,103)),('volume',(200,300)),('actual_notional',(20300,30900)))
        batch=c.CanonicalBatch(c.DataKind.BAR,tuple(c.Column(n,v) for n,v in facts),c.BatchMetadata('demo',c.SourceBinding('synthetic','snapshot1','map1','bars1'),c.Coverage(2,2,True),unit,scope=c.InputScope(100,200,'example-v1')))
        result=compute_bars(batch,config,entity=c.EntityKey('A','S'))
        values={column.feature_id:column.values[0] for column in result.values}
        assert values['session.bar.volume'] == 500
        assert values['session.bar.notional'] == 51200
        assert values['session.bar.close_weighted_price'] == 102.6
        assert values['session.price.overnight_gap'] is None
    finally: sys.meta_path.remove(deny)

core_check()
expected={'equity-feature-io-contracts':['equity-feature-contracts==0.0.4a4'],'equity-feature-io-sdk':['equity-feature-io-contracts==0.1.0a0'],'equity-feature-workers':['equity-feature-io-sdk==0.1.0a0']}
for name in sys.argv[1:]:
    if name=='core':continue
    module=importlib.import_module(name.replace('-','_'))
    assert module.__version__ == '0.1.0a0'
    assert module.__all__ == ['__version__']
    assert 'site-packages' in Path(module.__file__).resolve().parts
    assert sorted(distribution(name).requires or []) == sorted(expected[name])
    assert not distribution(name).entry_points
assert not any(name.split('.')[0] in {'duckdb','pyarrow','numpy'} for name in sys.modules)
print('Installed versions/PEP561 metadata/no-commands/core isolation/independent500/51200/102.6 goldens PASS')
