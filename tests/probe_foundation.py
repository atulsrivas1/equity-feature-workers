"""Independent foundation expectations, no backend/source data required."""
import importlib
import importlib.abc
from importlib.metadata import distribution
from pathlib import Path
import sys

class Deny(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('equity_feature_io', 'equity_feature_workers', 'equity_feature_duckdb', 'equity_feature_parquet', 'equity_feature_example_extensions', 'equity_feature_factory_fixture')) or fullname.split('.')[0] in {'duckdb','pyarrow','numpy','pandas','requests','httpx'}:
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
expected={'equity-feature-io-contracts':['equity-feature-contracts==0.0.4a4'],'equity-feature-io-sdk':['equity-feature-io-contracts==0.1.0a2'],'equity-feature-workers':['equity-feature-io-sdk==0.1.0a2','equity-features==0.0.4a4']}
for name in sys.argv[1:]:
    if name=='core':continue
    module=importlib.import_module(name.replace('-','_'))
    assert module.__version__ == ('0.1.0a10' if name=='equity-feature-workers' else '0.1.0a2')
    if name=='equity-feature-workers':
        assert {'TaskManifest', 'InputManifest', 'OutputManifest', 'ClaimIdentity', 'encode_task', 'decode_task', 'SessionCommandSpec', 'run_session', 'run_registered', 'RequiredCommandSpec', 'RequiredOutcome', 'run_required', 'run_required_registered', 'BarrierLimits', 'Dependency', 'TaskNode', 'evaluate_readiness', 'inspect_barrier', 'run_assembly', 'UniverseShard', 'BreadthCommandSpec', 'run_breadth', 'PublicationLimits', 'PublicationProgress', 'SerialPublisher', 'GenerationSpec', 'GenerationOutcome', 'GenerationStore'} <= set(module.__all__)
        assert all(hasattr(module, item) for item in module.__all__)
        assert {'PreparedSession','prepare_session','compute_session_inputs','WorkItem','InputReuseCache','ResourceBudget',
                'Partition','partition_tasks','SpillReference','ResultSpill','TaskExecution','SupervisorOutcome','BoundedSupervisor'} <= set(module.__all__)
        assert {'ClaimLimits','ClaimProgress','ClaimStore','TaskClaim','CatalogLimits','CatalogEntry','CatalogSnapshot','CatalogOutcome','CatalogStore'} <= set(module.__all__)
        assert {'DiagnosticStage','DiagnosticStatus','StageTiming','TaskDiagnostic','DiagnosticSnapshot','ProgressRecorder'} <= set(module.__all__)
    else:
        assert {'ResultSink','SinkRequirements'} <= set(module.__all__) if name=='equity-feature-io-contracts' else {'publish','prepare_publication','SourceRegistry','SinkRegistry'} <= set(module.__all__)
        assert all(hasattr(module, item) for item in module.__all__)
    assert 'site-packages' in Path(module.__file__).resolve().parts
    assert sorted(distribution(name).requires or []) == sorted(expected[name])
    entries = list(distribution(name).entry_points)
    if name == 'equity-feature-workers':
        assert [(e.group,e.name,e.value) for e in entries] == [('console_scripts','equity-feature-worker','equity_feature_workers.cli:main')]
    else: assert not entries
assert not any(name.split('.')[0] in {'duckdb','pyarrow','numpy'} for name in sys.modules)
print('Installed versions/PEP561 metadata/explicit-command/core isolation/independent500/51200/102.6 goldens PASS')
