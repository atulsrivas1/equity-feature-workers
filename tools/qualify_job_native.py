"""Pre-code feasibility only: accepted installed APIs with owned test fixtures."""
import hashlib
import json
from pathlib import Path
import sys
from dataclasses import asdict, fields, replace
base = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(base / 'tests'))
from test_commands import inputs
from test_acquisition import Factory, Credentials, Token
from test_required_inputs import spec as required_spec
from required_fixture import request, daily, LiteralSource
from equity_feature_example_extensions import ExampleSink
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_io_sdk import SourceRegistry, SinkRegistry, encode_result
from equity_feature_workers import SourceOffer, ExecutionApproval, plan_acquisition, execute_plan, PlanningError
import equity_feature_workers
from equity_feature_contracts import builtin_registry
assert 'site-packages' in Path(equity_feature_workers.__file__).parts
def flow(spec, source, feature, sink=None, token=None):
    sources, sinks = SourceRegistry(), SinkRegistry()
    sources.register('owned.source', Factory(source))
    sinks.register('owned.sink', Factory(sink or ExampleSink()))
    offer = SourceOffer('owned.source', source.capabilities(), {})
    plan = plan_acquisition(spec, (feature,), sources=sources, offers=(offer,), sinks=sinks,
                            sink_id='owned.sink', sink_config={}, requirements=LIMITS)
    return plan, lambda: execute_plan(spec, plan, sources=sources, sinks=sinks, credentials=Credentials(),
                                      authorize=lambda p: ExecutionApproval(p.plan_sha256), cancellation=token)
reports = []
fixtures = []
for family, feature in (('trades','session.trade.volume'), ('bars','session.bar.volume'), ('quotes','session.quote.sampled_spread')):
    spec, batch, source = inputs(family)
    plan, execute = flow(spec, source, feature)
    outcome = execute()
    assert outcome.output.committed and outcome.output.receipt is not None
    values = {c.feature_id: c.values[0] for c in outcome.results[0].values}
    md = outcome.results[0].metadata
    binding = batch.metadata.source
    context = {'namespace':md.namespace, 'dataset':{'dataset_id':'owned.'+family,'revision':'owned-v1',**asdict(binding)},
               'config':{'config_id':spec.config.identity,'revision':'owned-config-v1','schema_version':spec.config.schema_version,
                         'digest':spec.config.digest,'algorithm_version':spec.config.algorithm_version},
               'registry_snapshot':hashlib.sha256(builtin_registry().to_json().encode('ascii')).hexdigest(),
               'math_policy_version':md.math_policy_version,
               'features':[{'feature_id':feature,'algorithm_version':'v1'}],
               'availability':{f.name:str(getattr(spec.config.availability,f.name)) if f.name.endswith('_ns') else getattr(spec.config.availability,f.name) for f in fields(spec.config.availability)}}
    payload = {'operation':'calculate','context':context,'scope':{'instrument_id':spec.entity.instrument_id,
              'session_id':spec.entity.session_id,'start_ns':str(spec.request.start_ns),'end_ns':str(spec.request.end_ns)}}
    wire_digest = hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')).hexdigest()
    assert wire_digest != plan.command_sha256 and wire_digest != plan.plan_sha256
    fixtures.append({'family':family,'native_spec':asdict(spec),'request':{'schema':'equity.remote','version':'1.0','kind':'request','request_id':'owned-'+family,
                    'payload':payload|{'command_digest':wire_digest,'idempotency_key':'owned-'+family}}})
    if family == 'trades':
        assert values['session.trade.count'] == 3 and values['session.trade.volume'] == 10 and values['session.trade.notional'] == 1011
    elif family == 'bars':
        assert values['session.bar.volume'] == 500 and values['session.bar.notional'] == 51200
    else:
        assert values['session.quote.sampled_spread'].mean_spread == 1.0
    reports.append({'family':family,'requested_feature':feature,'execution_features':list(plan.execution_features),
                    'command_sha256':plan.command_sha256,'plan_sha256':plan.plan_sha256,
                    'task_sha256':outcome.output.task.task_sha256,'content_sha256':outcome.output.receipt.content_sha256,
                    'native_result_sha256':hashlib.sha256(encode_result(outcome.results[0])).hexdigest(), 'committed_readback':True})
spec = required_spec(request=request())
source = LiteralSource(daily(), spec.request)
plan, execute = flow(spec, source, 'history.sma')
out = execute().command
assert out.output.committed
assert out.results[0].values[0].values[0] == 115.0
reports.append({'family':'history','requested_feature':'history.sma','value':out.results[0].values[0].values[0],
                'command_sha256':plan.command_sha256,'plan_sha256':plan.plan_sha256,
                'task_sha256':out.output.task.task_sha256,'content_sha256':out.output.receipt.content_sha256,
                'committed_readback':True})
token = Token()
original_spec, _, original_source = inputs('trades')
first_plan, first_execute = flow(original_spec, original_source, 'session.trade.volume')
first = first_execute()
second_spec = replace(original_spec,job_id='job2',generation_id='generation2',partition_id='A-S2')
second_plan, second_execute = flow(second_spec, original_source, 'session.trade.volume')
second = second_execute()
assert first_plan.command_sha256 != second_plan.command_sha256 and first_plan.plan_sha256 != second_plan.plan_sha256
assert first.output.task.task_sha256 != second.output.task.task_sha256
assert first.results[0].metadata.inputs == second.results[0].metadata.inputs
assert encode_result(first.results[0]) == encode_result(second.results[0])
reports.append({'case':'distinct_job_identities_stable_source_and_destination','original_request_preserved':second_spec.request == original_spec.request,
                'native_command_plan_task_distinct':True,'full_input_bindings_and_result_bytes_equal':True})
class CommitThenCancel(ExampleSink):
    def commit(self, session):
        receipt = super().commit(session)
        self.captured = receipt
        token.cancelled = True
        return receipt
spec, _, source = inputs('trades')
sink = CommitThenCancel()
plan, execute = flow(spec, source, 'session.trade.volume', sink, token)
try:
    execute()
    raise AssertionError('expected native postcommit cancellation')
except PlanningError as error:
    assert error.code.value == 'CANCELLED', error.code
    assert sink.lookup(sink.captured.idempotency_key).receipt == sink.captured
    result = sink.read(sink.captured)
    assert result[0].values[0].values[0] == 3
    reports.append({'case':'postcommit_cancel','native_error':error.code.value,'committed_receipt_preserved':True,
                    'verified_readback_after_cancel':True,'content_sha256':sink.captured.content_sha256})
print(json.dumps({'stage':'pre-code-native-api-feasibility','worker_version':equity_feature_workers.__version__,
                   'installed_worker':True,'service_runtime_implemented':False,'records':reports,'fixtures':fixtures},sort_keys=True,indent=2))
