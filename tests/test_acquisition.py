"""Literal acquisition oracle, exact consent boundaries and accepted worker parity."""
from dataclasses import replace
from fractions import Fraction
import json
from pathlib import Path
import subprocess
import sys
import unittest

from equity_feature_contracts import PriceUnit
from equity_feature_contracts.adapters import AdapterCapabilities
from equity_feature_example_extensions import ExampleSink
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_io_sdk import SourceRegistry, SinkRegistry, encode_result
from equity_feature_workers import (SourceOffer, ExecutionApproval, PlanningError, PlanningErrorCode,
    plan_acquisition, execute_plan, run_session)
from equity_feature_workers.acquisition_demo import run_owned_acquisition
from test_commands import inputs


class Factory:
    protocol_version = '1'
    def __init__(self, instance, *, hostile=False):
        self.instance, self.hostile, self.calls = instance, hostile, 0
    def validate_config(self, config):
        self.calls += 1
        if self.hostile: raise ValueError('secret-private-factory-path')
        return config
    def create(self, config, credentials):
        self.calls += 1
        return self.instance


class Credentials:
    def get(self, name):
        raise AssertionError('credential lookup forbidden in owned test')


class Token:
    def __init__(self, cancelled=False): self.cancelled = cancelled
    def is_cancelled(self): return self.cancelled


class Acquisition(unittest.TestCase):
    def setup_flow(self, family='trades'):
        self.spec, self.batch, self.source = inputs(family)
        self.sources, self.sinks = SourceRegistry(), SinkRegistry()
        self.sf, self.kf = Factory(self.source), Factory(ExampleSink())
        self.sources.register('selected.source', self.sf);self.sinks.register('selected.sink', self.kf)
        self.offer = SourceOffer('selected.source', self.source.capabilities(), {'private_path':'owned-fixture'})
        self.feature = {'trades':'session.trade.volume','bars':'session.bar.volume','quotes':'session.quote.sampled_spread'}[family]
        return self.make_plan()
    def make_plan(self, *, offers=None, **kw):
        return plan_acquisition(kw.pop('spec',self.spec), kw.pop('features',(self.feature,)),
            sources=self.sources, offers=offers if offers is not None else (self.offer,), sinks=self.sinks,
            sink_id=kw.pop('sink_id','selected.sink'), sink_config=kw.pop('sink_config',{}),
            requirements=kw.pop('requirements',LIMITS), **kw)
    def execute(self, plan, *, authorize=None, **kw):
        return execute_plan(kw.pop('spec',self.spec),plan,sources=self.sources,sinks=self.sinks,
            credentials=Credentials(),authorize=authorize or (lambda p:ExecutionApproval(p.plan_sha256)),**kw)
    def rejected(self, code, operation):
        with self.assertRaises(PlanningError) as caught: operation()
        self.assertEqual(caught.exception.code,code)
        self.assertIsNone(caught.exception.__cause__);self.assertIsNone(caught.exception.__context__)
        self.assertNotIn('secret',str(caught.exception));self.assertNotIn('private',str(caught.exception))
    def test_metadata_only_plan_does_not_create_or_read(self):
        p=self.setup_flow();self.assertEqual((self.sf.calls,self.kf.calls,self.source.called),(0,0,0))
        self.assertEqual(p.requirements[0].role,'trades')
        self.assertEqual(p.requested_features,('session.trade.volume',))
        self.assertEqual(len(p.execution_features),5)
    def test_default_denial_and_mismatched_consent_have_zero_access(self):
        p=self.setup_flow()
        for auth in (lambda p:None, lambda p:True, lambda p:ExecutionApproval('0'*64)):
            self.rejected(PlanningErrorCode.UNAUTHORIZED,lambda:self.execute(p,authorize=auth))
        self.assertEqual((self.sf.calls,self.kf.calls,self.source.called),(0,0,0))
    def test_authorizer_exception_is_redacted_without_context(self):
        p=self.setup_flow()
        def hostile(p):raise ValueError('secret-private-key')
        self.rejected(PlanningErrorCode.APPROVAL,lambda:self.execute(p,authorize=hostile))
        self.assertEqual((self.sf.calls,self.kf.calls,self.source.called),(0,0,0))
    def test_unknown_duplicate_and_outside_worker_features(self):
        self.setup_flow()
        self.rejected(PlanningErrorCode.INVALID,lambda:self.make_plan(features=(self.feature,self.feature)))
        for f in ('unknown.feature','session.trade.top_k','session.bar.volume'):
            self.rejected(PlanningErrorCode.UNSUPPORTED,lambda:self.make_plan(features=(f,)))
    def test_missing_installed_offer(self):
        self.setup_flow();self.rejected(PlanningErrorCode.NO_CAPABILITY,lambda:self.make_plan(offers=()))
        missing=SourceOffer('unregistered',self.offer.capabilities,{})
        self.rejected(PlanningErrorCode.NO_CAPABILITY,lambda:self.make_plan(offers=(missing,)))
    def test_incompatible_capability_variants(self):
        self.setup_flow();cap=self.offer.capabilities
        for wrong in (replace(cap,namespaces=('other',)),replace(cap,price_units=(PriceUnit(1,'USD'),)),
                      replace(cap,adjustment_bases=('split',)),replace(cap,sampling=('trade_snapshot',)),
                      replace(cap,historical=False),replace(cap,max_batch_rows=1)):
            self.rejected(PlanningErrorCode.NO_CAPABILITY,lambda:self.make_plan(offers=(SourceOffer('selected.source',wrong,{}),)))
    def test_ambiguous_selection_and_explicit_selected_only_execution(self):
        self.setup_flow();other=Factory(self.source,hostile=True);self.sources.register('other.source',other)
        offers=(self.offer,SourceOffer('other.source',self.offer.capabilities,{}))
        self.rejected(PlanningErrorCode.AMBIGUOUS,lambda:self.make_plan(offers=offers))
        p=self.make_plan(offers=offers,source_id='selected.source');self.execute(p)
        self.assertEqual(other.calls,0);self.assertEqual(self.source.called,1)
    def test_exact_identity_binds_source_and_sink_configuration_and_requirements(self):
        p=self.setup_flow()
        for changed in (self.make_plan(sink_config={'path':'other-private'}),
                        self.make_plan(requirements=replace(LIMITS,max_total_bytes=LIMITS.max_total_bytes+1)),
                        self.make_plan(offers=(SourceOffer('selected.source',self.offer.capabilities,{'private_path':'other'}),)),
                        self.make_plan(features=('session.trade.vwap',))):
            self.assertNotEqual(p.plan_sha256,changed.plan_sha256)
            self.rejected(PlanningErrorCode.UNAUTHORIZED,lambda:self.execute(changed,authorize=lambda q:ExecutionApproval(p.plan_sha256)))
        self.sinks.register('other.sink',Factory(ExampleSink()))
        self.assertNotEqual(p.plan_sha256,self.make_plan(sink_id='other.sink').plan_sha256)
        self.assertEqual(self.source.called,0)
    def test_original_mutable_config_is_frozen_and_repr_opaque(self):
        self.setup_flow();supplied={'private_path':'owned-fixture'}
        offer=SourceOffer('selected.source',self.offer.capabilities,supplied);supplied['private_path']='secret-other-path'
        p=self.make_plan(offers=(offer,));self.assertEqual(offer.config['private_path'],'owned-fixture')
        self.assertNotIn('owned-fixture',repr(offer));self.assertNotIn('owned-fixture',repr(p))
        with self.assertRaises(TypeError):offer.config['x']='changed'
    def test_config_rejects_secrets_executable_nested_and_nonfinite_values(self):
        self.setup_flow()
        for config in ({'api_key':'secret'}, {'module':'evil'}, {'nested':{}}, {'number':float('nan')}, {'bad-key':1}):
            self.rejected(PlanningErrorCode.INVALID,lambda:SourceOffer('selected.source',self.offer.capabilities,config))
    def test_stale_command_identity_precedes_consent_or_factory(self):
        p=self.setup_flow();changed=replace(self.spec,job_id='different-job')
        self.rejected(PlanningErrorCode.STALE,lambda:self.execute(p,spec=changed))
        self.assertEqual((self.sf.calls,self.kf.calls,self.source.called),(0,0,0))
    def test_forged_plan_digest_rejected_before_access(self):
        p=self.setup_flow();self.rejected(PlanningErrorCode.STALE,lambda:self.execute(replace(p,plan_sha256='0'*64)))
        self.assertEqual(self.source.called,0)
    def test_changed_constructed_capability_rejected_before_sink_and_acquisition(self):
        p=self.setup_flow();self.source.capabilities=lambda:replace(self.offer.capabilities,max_batch_rows=5)
        self.rejected(PlanningErrorCode.STALE,lambda:self.execute(p));self.assertEqual((self.kf.calls,self.source.called),(0,0))
    def test_cancel_before_or_during_approval_precedes_all_access(self):
        p=self.setup_flow();token=Token(True)
        self.rejected(PlanningErrorCode.CANCELLED,lambda:self.execute(p,cancellation=token))
        token.cancelled=False
        def cancel(q):token.cancelled=True;return ExecutionApproval(q.plan_sha256)
        self.rejected(PlanningErrorCode.CANCELLED,lambda:self.execute(p,cancellation=token,authorize=cancel))
        self.assertEqual((self.sf.calls,self.kf.calls,self.source.called),(0,0,0))
    def test_hostile_factory_error_is_fixed(self):
        p=self.setup_flow();self.sf.hostile=True
        self.rejected(PlanningErrorCode.SOURCE,lambda:self.execute(p));self.assertEqual(self.source.called,0)
    def test_literal_trade_values_and_direct_result_receipt_parity(self):
        p=self.setup_flow();out=self.execute(p)
        direct=run_session(self.spec,self.source,ExampleSink(),requirements=LIMITS)
        self.assertEqual(tuple(map(encode_result,out.results)),tuple(map(encode_result,direct.results)))
        self.assertEqual(out.output.task,direct.output.task);self.assertEqual(out.output.receipt.content_sha256,direct.output.receipt.content_sha256)
        values={c.feature_id.rsplit('.',1)[1]:c.values[0] for c in out.results[0].values}
        self.assertEqual((values['count'],values['volume'],values['notional']),(3,10,1011))
        self.assertAlmostEqual(values['vwap'],101.1,delta=1e-12);self.assertAlmostEqual(values['mean_size'],float(Fraction(10,3)),delta=1e-12)
    def test_bars_and_quotes_existing_worker_paths(self):
        for family in ('bars','quotes'):
            p=self.setup_flow(family);out=self.execute(p);self.assertTrue(out.output.committed)
    def test_genuine_local_csv_owned_literal_oracle_and_causal_nulls(self):
        oracle=json.loads((Path(__file__).parent/'acquisition_oracle.json').read_bytes())['reconstruction']
        record=run_owned_acquisition(approved=True);v=record['values']
        self.assertEqual((v['session.trade.count'],v['session.trade.volume'],v['session.trade.notional']),
                         (oracle['count'],oracle['volume'],oracle['notional_tick_shares']))
        self.assertAlmostEqual(v['session.trade.vwap'],oracle['vwap'],delta=1e-12)
        causal=run_owned_acquisition(approved=True,reconstruction=False)
        self.assertTrue(all(v is None for v in causal['values'].values()))
        self.assertIn('"observed":{\"int\":\"3\"}',causal['result'])
    def test_cli_default_denied_and_explicit_owned_approval(self):
        denied=subprocess.run([sys.executable,'-I','-m','equity_feature_workers.cli','--acquisition-demo'],capture_output=True,text=True)
        self.assertEqual(denied.returncode,1);self.assertIn('EXECUTION_NOT_AUTHORIZED',denied.stderr)
        accepted=subprocess.run([sys.executable,'-I','-m','equity_feature_workers.cli','--acquisition-demo','--approve-owned-fixture'],capture_output=True,text=True,check=True)
        records=json.loads(accepted.stdout);self.assertTrue(records[0]['verified_readback']);self.assertFalse(records[0]['provider_access'])
    def test_optional_demo_missing_package_is_safe(self):
        from unittest.mock import patch
        import builtins
        original=builtins.__import__
        def missing(name,*a,**kw):
            if name=='equity_feature_files':raise ImportError('secret-missing-location')
            return original(name,*a,**kw)
        with patch('builtins.__import__',missing):
            self.rejected(PlanningErrorCode.NO_CAPABILITY,lambda:run_owned_acquisition(approved=True))
    def test_bounded_inventory_and_duplicate_offers(self):
        self.setup_flow()
        self.rejected(PlanningErrorCode.INVALID,lambda:self.make_plan(offers=(self.offer,self.offer)))
        self.rejected(PlanningErrorCode.INVALID,lambda:self.make_plan(features=tuple('f'+str(i) for i in range(129))))
    def test_required_history_and_explicit_absence_paths(self):
        from test_required_inputs import spec
        from required_fixture import request, daily, LiteralSource
        s=spec(request=request());self.setup_flow();self.spec=s;self.feature='history.sma'
        source=LiteralSource(daily(),s.request);self.sf.instance=source
        self.offer=SourceOffer('selected.source',source.capabilities(),{})
        p=self.make_plan();out=self.execute(p)
        self.assertEqual(out.command.results[0].values[0].values[0],115)
        self.assertTrue(out.command.output.committed);self.assertEqual(source.calls,1)
        self.spec=spec();p=self.make_plan(offers=());self.assertIsNone(p.source)
        out=self.execute(p);self.assertIsNone(out.command.results[0].values[0].values[0])
        self.assertEqual(source.calls,1)
    def test_multi_raw_role_is_explicitly_unsupported(self):
        self.setup_flow('bars')
        self.rejected(PlanningErrorCode.UNSUPPORTED,lambda:self.make_plan(features=('session.price.overnight_gap',)))
        self.assertEqual((self.sf.calls,self.kf.calls,self.source.called),(0,0,0))
    def test_source_missing_and_empty_preserve_existing_worker_outcomes(self):
        self.setup_flow();self.source.batch=None;p=self.make_plan();out=self.execute(p)
        self.assertTrue(out.output.committed);self.assertTrue(all(c.values[0] is None for c in out.results[0].values))
    def test_hostile_source_and_sink_failures_are_redacted(self):
        p=self.setup_flow()
        def hostile(*a):raise RuntimeError('secret-provider-path')
        self.source.iter_batches=hostile
        self.rejected(PlanningErrorCode.SOURCE,lambda:self.execute(p))
        p=self.setup_flow();self.kf.hostile=True
        self.rejected(PlanningErrorCode.SINK,lambda:self.execute(p));self.assertEqual(self.source.called,0)


if __name__=='__main__':unittest.main()
