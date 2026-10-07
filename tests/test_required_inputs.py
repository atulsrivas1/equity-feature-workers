"""Installed required-input commands checked against pre-code literal mathematics."""
from dataclasses import replace
from fractions import Fraction
import contextlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_contracts.relative import RelativeSpec
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_example_extensions import ExampleSink
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_io_sdk import SourceRegistry, SinkRegistry
from equity_feature_workers import RequiredCommandSpec, run_required, run_required_registered, CommandError, CommandErrorCode
from equity_feature_workers.cli import main, NoCredentials
from required_fixture import (SESSIONS, daily, context, config, request, bucket_inputs, target_volume,
                              bucket_target, return_reference, LiteralSource)

ORACLE=json.loads(Path(__file__).with_name('required_oracle.json').read_text(encoding='utf-8'))
GRID=tuple(IntervalSpec(s.session_id,s.open_ns,s.close_ns) for s in SESSIONS)


def spec(family='history',feature='history.sma',ctx=None,**kwargs):
    ctx=context() if ctx is None and family!='relative_returns' else ctx
    return RequiredCommandSpec('job','generation','A-S3',family,config(feature),GRID,'revision1','synthetic-conformance',65536,
                               context=ctx,feature_ids=(feature,) if family=='history' else (),**kwargs)


def execute(s,b=None):
    return run_required(s,LiteralSource(b,s.request) if s.request else None,ExampleSink(),requirements=LIMITS)


class RequiredInputs(unittest.TestCase):
    def rejected(self,code,call):
        with self.assertRaises(CommandError) as caught:call()
        self.assertEqual(caught.exception.code,code)
        self.assertNotIn('secret',str(caught.exception))

    def test_eight_history_goldens_direct_raw_no_derived_job(self):
        ids=('history.return','history.sma','history.ema','history.prior_high','history.prior_low',
             'history.rsi','history.atr','history.return_volatility')
        for fid in ids:
            with self.subTest(feature=fid):
                anchor='S1' if fid in ('history.ema','history.rsi') else 'S2' if fid=='history.atr' else None
                s=spec(feature=fid,ctx=context(anchor=anchor),request=request())
                out=execute(s,daily());r=out.command.results[0]
                expected=float(Fraction(1,24200))**.5 if fid=='history.return_volatility' else ORACLE[fid]
                self.assertAlmostEqual(r.values[0].values[0],expected,delta=1e-12)
                self.assertEqual(r.quality[0].status,c.Status.AVAILABLE)
                self.assertTrue(out.command.output.committed)
                self.assertEqual(out.command.output.task.warmup_sessions,('S1','S2'))
                self.assertTrue(s.requirements())
                self.assertEqual({i.role for i in out.command.output.task.inputs},{'history_context','daily_history'})
                self.assertEqual(type(out.witness).__name__,'ReturnReference') if fid=='history.return' else self.assertIsNone(out.witness)

    def test_exact_sma_witness_not_reconstructed_float(self):
        out=execute(spec('sma_reference',request=request()),daily())
        self.assertEqual((out.witness.numerator,out.witness.denominator),(ORACLE['sma_numerator'],ORACLE['sma_denominator']))
        self.assertEqual(out.witness.compare_price(115,c.PriceUnit(0,'USD')),0)
        self.assertEqual(out.witness.result,out.command.results[0])

    def test_daily_baseline_and_supplied_relative_volume(self):
        baseline=execute(spec('daily_baseline','baseline.daily_volume',request=request()),daily()).witness
        self.assertEqual((baseline.numerator,baseline.denominator),(300,2))
        self.assertEqual(baseline.result.values[0].values[0],ORACLE['baseline_value'])
        cfg=config('baseline.daily_volume');ctx=context()
        s=spec('relative_volume','baseline.daily_volume',target=target_volume(cfg,ctx),baseline=baseline)
        out=execute(s)
        self.assertEqual(out.command.results[0].values[0].values[0],ORACLE['relative_volume'])
        self.assertIsNone(out.witness)
        self.assertFalse(any(i.state=='missing' for i in out.command.output.task.inputs))

    def test_whole_bucket_baseline_and_supplied_ratio(self):
        b,ctx,cfg=bucket_inputs()
        base=execute(spec('interval_baseline','baseline.interval_volume',ctx,request=request(c.DataKind.BAR)),b).witness
        self.assertEqual((base.numerator,base.denominator),(300,2))
        s=spec('interval_relative_volume','baseline.interval_volume',ctx,target=bucket_target(cfg,ctx),baseline=base)
        self.assertEqual(execute(s).command.results[0].values[0].values[0],4.)

    def test_supplied_market_and_optional_sector_absence(self):
        symbol=return_reference();market=return_reference('M',(100,105,110))
        cfg=config('history.return')
        rs=RelativeSpec(c.EntityKey('A','S3'),c.EntityKey('M','S3'))
        s=RequiredCommandSpec('job','generation','A-S3','relative_returns',cfg,GRID,'revision1','synthetic-conformance',65536,
            relative_spec=rs,symbol=symbol,market=market,feature_ids=('relative.market_return','relative.sector_return'))
        out=execute(s);values={v.feature_id:v.values[0] for v in out.command.results[0].values}
        self.assertAlmostEqual(values['relative.market_return'],ORACLE['relative_market_return'],delta=1e-12)
        self.assertIsNone(values['relative.sector_return'])
        self.assertEqual({i.role for i in out.command.output.task.inputs if i.state=='missing'}, {'absent.sector','absent.membership'})
        market_only=replace(s,feature_ids=('relative.market_return',))
        self.assertFalse(any(i.state=='missing' for i in execute(market_only).command.output.task.inputs))
        self.rejected(CommandErrorCode.CONFIG,lambda:replace(market_only,feature_ids=('relative.sector_return',)))

    def test_explicit_raw_absence_missing_empty_partial_distinct(self):
        for declared_request in (None,request()):
            out=execute(spec(request=declared_request))
            self.assertIsNone(out.command.results[0].values[0].values[0])
            self.assertEqual(next(i.state for i in out.command.output.task.inputs if i.role=='daily_history'),'missing')
        b=daily()
        empty=replace(b,columns=tuple(c.Column(x.name,()) for x in b.columns),metadata=replace(b.metadata,coverage=c.Coverage(0,0,True)))
        partial=replace(b,metadata=replace(b.metadata,coverage=c.Coverage(4,3,False)))
        for b,state in ((empty,'empty'),(partial,'partial')):
            ctx=replace(context(),slot_coverage=(c.Coverage(1,0,False),)*3) if state=='empty' else context()
            out=execute(spec(ctx=ctx,request=request()),b)
            self.assertEqual(next(i.state for i in out.command.output.task.inputs if i.role=='daily_history'),state)
            if state=='empty':self.assertIsNone(out.command.results[0].values[0].values[0])
            else:self.assertEqual(out.command.results[0].values[0].values[0],115.)

    def test_missing_supplied_dependencies_do_not_fetch(self):
        out=execute(spec('relative_volume','relative.volume'))
        self.assertIsNone(out.command.results[0].values[0].values[0])
        self.assertEqual({i.role for i in out.command.output.task.inputs if i.state=='missing'}, {'absent.target','absent.baseline'})
        self.rejected(CommandErrorCode.CONFIG,lambda:run_required(spec(),LiteralSource(daily(),request()),ExampleSink(),requirements=LIMITS))

    def test_gap_unknown_future_and_initialization(self):
        self.rejected(CommandErrorCode.CONFIG,lambda:spec(feature='history.ema',request=request()))
        for times in ((100,None,300),(100,311,300)):
            b=daily();b=replace(b,columns=tuple(c.Column(x.name,times) if x.name=='known_at_ns' else x for x in b.columns))
            self.assertIsNone(execute(spec(request=request()),b).command.results[0].values[0].values[0])
        ctx=replace(context(),slot_coverage=(c.Coverage(1,1,True),c.Coverage(1,0,False),c.Coverage(1,1,True)))
        b=daily();b=replace(b,columns=tuple(c.Column(x.name,(x.values[0],x.values[2])) for x in b.columns),
                           metadata=replace(b.metadata,coverage=c.Coverage(3,2,False)))
        self.assertIsNone(execute(spec(ctx=ctx,request=request()),b).command.results[0].values[0].values[0])
        self.rejected(CommandErrorCode.CALCULATION,lambda:execute(spec(ctx=ctx,request=request()),daily()))

    def test_request_grid_unit_auction_and_raw_kind_preflight(self):
        for r in (replace(request(),kind=c.DataKind.BAR),replace(request(),start_ns=1),
                  replace(request(),price_unit=c.PriceUnit(1,'USD')),replace(request(),instruments=('B',))):
            self.rejected(CommandErrorCode.CONFIG,lambda:spec(request=r))
        ctx=replace(context(),sessions=(replace(SESSIONS[0],include_opening_auction=True),)+SESSIONS[1:])
        self.rejected(CommandErrorCode.CONFIG,lambda:spec(ctx=ctx,request=request()))
        self.rejected(CommandErrorCode.CONFIG,lambda:replace(spec(),governed_sessions=GRID[1:]))

    def test_metadata_entity_admission_before_sink(self):
        s=spec(request=request());source=LiteralSource(daily(),request())
        real=s.calculate(daily())
        wrong=replace(real,values=tuple(replace(col,entities=(c.EntityKey('B','S3'),)) for col in real.values),
                      quality=tuple(replace(q,entity=c.EntityKey('B','S3')) for q in real.quality),
                      evidence=tuple(replace(e,entity=c.EntityKey('B','S3')) for e in real.evidence))
        with patch.object(RequiredCommandSpec,'calculate',return_value=wrong):
            self.rejected(CommandErrorCode.RESULT,lambda:run_required(s,source,ExampleSink(),requirements=LIMITS))
        wrong=replace(real,metadata=replace(real.metadata,inputs=()),evidence=())
        with patch.object(RequiredCommandSpec,'calculate',return_value=wrong):
            self.rejected(CommandErrorCode.RESULT,lambda:run_required(s,source,ExampleSink(),requirements=LIMITS))

    def test_limit_cancellation_and_safe_failure(self):
        s=spec(request=request())
        # Request/context preflight fits, but the acquired batch exceeds the remaining declared acquisition budget.
        small=replace(s,max_input_bytes=1300)
        self.rejected(CommandErrorCode.LIMIT,lambda:execute(small,daily()))
        token=type('Cancelled',(),{'is_cancelled':lambda self:True})()
        source=LiteralSource(daily(),request())
        self.rejected(CommandErrorCode.CANCELLED,lambda:run_required(s,source,ExampleSink(),requirements=LIMITS,cancellation=token))
        self.assertEqual(source.calls,0)
        class Broken(LiteralSource):
            def iter_batches(self,*args):raise RuntimeError('secret source path')
        self.rejected(CommandErrorCode.SOURCE,lambda:run_required(s,Broken(daily(),request()),ExampleSink(),requirements=LIMITS))

    def test_registered_direct_cli_parity_explicit_ownership(self):
        s=spec(request=request());direct=execute(s,daily())
        class SF:
            protocol_version='1'
            def validate_config(self,config):
                if config:raise ValueError('unexpected configuration')
                return config
            def create(self,config,credentials):return LiteralSource(daily(),request())
        from equity_feature_example_extensions import SinkFactory
        sources=SourceRegistry();sinks=SinkRegistry();sources.register('literal.daily',SF());sinks.register('example.memory',SinkFactory())
        factory=run_required_registered(s,sources=sources,sinks=sinks,source_id='literal.daily',source_config={},
            sink_id='example.memory',sink_config={'destination_scope':'synthetic-conformance'},credentials=NoCredentials(),requirements=LIMITS)
        self.assertEqual(direct.command.output.task.task_sha256,factory.command.output.task.task_sha256)
        self.assertEqual(direct.command.output.receipt.content_sha256,factory.command.output.receipt.content_sha256)
        for family,s,b in (('history',s,daily()),('sma_reference',spec('sma_reference',request=request()),daily()),
                           ('relative_volume',spec('relative_volume','relative.volume'),None)):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(main(['--required',family],spec=s,source=LiteralSource(b,request()) if s.request else None,
                                      sink=ExampleSink(),requirements=LIMITS),0)
            record=json.loads(output.getvalue())[0]
            self.assertTrue(record['verified_readback'])
            self.assertEqual(record['owned_witness_type'],'SMAReference' if family=='sma_reference' else None)
        self.rejected(CommandErrorCode.CONFIG,lambda:run_required_registered(spec(),sources=sources,sinks=sinks,
            source_id='literal.daily',source_config={},sink_id='example.memory',sink_config={},credentials=NoCredentials(),requirements=LIMITS))

    def test_incompatible_owned_baseline_and_reference_preflight(self):
        baseline=execute(spec('daily_baseline','baseline.daily_volume',request=request()),daily()).witness
        self.rejected(CommandErrorCode.CONFIG,lambda:spec('relative_volume','relative.volume',baseline=baseline))
        self.rejected(CommandErrorCode.CONFIG,lambda:spec('relative_volume','baseline.daily_volume',
            baseline=baseline,target=replace(target_volume(baseline.config,context()),entity=c.EntityKey('B','S3'))))
        rs=RelativeSpec(c.EntityKey('A','S3'),c.EntityKey('M','S3'))
        self.rejected(CommandErrorCode.CONFIG,lambda:RequiredCommandSpec('job','generation','A-S3','relative_returns',
            config('history.return'),GRID,'revision1','synthetic-conformance',65536,relative_spec=rs,
            symbol=return_reference(),market=return_reference('X'),feature_ids=('relative.market_return',)))

    def test_cli_all_remaining_required_families_and_factory_absence(self):
        b,ctx,_=bucket_inputs()
        s=RequiredCommandSpec('job','generation','A-S3','relative_returns',config('history.return'),GRID,'revision1',
            'synthetic-conformance',65536,relative_spec=RelativeSpec(c.EntityKey('A','S3')),
            symbol=return_reference(),feature_ids=('relative.market_return',))
        for command,batch in ((spec('daily_baseline','baseline.daily_volume',request=request()),daily()),
            (spec('interval_baseline','baseline.interval_volume',ctx,request=request(c.DataKind.BAR)),b),
            (spec('interval_relative_volume','baseline.interval_volume',ctx),None),(s,None)):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(main(['--required',command.family],spec=command,
                    source=LiteralSource(batch,command.request) if command.request else None,sink=ExampleSink(),requirements=LIMITS),0)
            self.assertTrue(json.loads(output.getvalue())[0]['verified_readback'])
        from equity_feature_example_extensions import SinkFactory
        sources=SourceRegistry();sinks=SinkRegistry();sinks.register('example.memory',SinkFactory())
        out=run_required_registered(s,sources=sources,sinks=sinks,source_id=None,source_config={},sink_id='example.memory',
            sink_config={'destination_scope':'synthetic-conformance'},credentials=NoCredentials(),requirements=LIMITS)
        self.assertIsNone(out.command.results[0].values[0].values[0])
        self.assertEqual({i.role for i in out.command.output.task.inputs if i.state=='missing'},{'absent.market'})

    def test_task_identity_binds_ordered_context_and_dependency(self):
        plain=spec(request=request())
        changed=replace(plain,context=replace(context(),grid_version='grid-v2'))
        a=execute(plain,daily()).command.output.task;b=execute(changed,daily()).command.output.task
        self.assertNotEqual(a.task_sha256,b.task_sha256)
        self.assertNotEqual(a.reuse_sha256,b.reuse_sha256)
        moved=replace(plain,job_id='job2',destination_scope='other-destination')
        self.assertEqual(plain.task(()).reuse_sha256,moved.task(()).reuse_sha256)

    def test_source_order_and_whole_bucket_certificates_not_inferred(self):
        b=daily();b=replace(b,columns=tuple(c.Column(x.name,tuple(reversed(x.values))) for x in b.columns))
        self.rejected(CommandErrorCode.SOURCE,lambda:execute(spec(request=request()),b))
        b,ctx,_=bucket_inputs()
        b=replace(b,columns=tuple(c.Column(x.name,(51,150,250)) if x.name=='end_ns' else x for x in b.columns))
        self.rejected(CommandErrorCode.CALCULATION,lambda:execute(spec('interval_baseline','baseline.interval_volume',ctx,
            request=request(c.DataKind.BAR)),b))


if __name__=='__main__':unittest.main()
