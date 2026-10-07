"""Independent literal command goldens and boundary/failure admission."""
from dataclasses import replace
from fractions import Fraction
import json
import contextlib
import io
import subprocess
import sys
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_contracts.adapters import AdapterBatch, AdapterCapabilities, AcquisitionRequest
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_example_extensions import ExampleSink
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_io_sdk import descriptor
from equity_feature_workers import CommandError, CommandErrorCode, SessionCommandSpec, run_session
from equity_feature_workers.cli import main, run_demo
from equity_features.session import compute_bars, compute_quotes, compute_trades
from manifest_fixture import fixture


class Source:
    def __init__(self, batch, request, deliveries=None):
        self.batch, self.request, self.deliveries = batch, request, deliveries
        self.called = 0
    def capabilities(self):
        return AdapterCapabilities((self.request.kind,), ('demo',), (c.PriceUnit(0,'USD'),),
                                   sampling=(self.request.sampling,), max_batch_rows=4)
    def iter_batches(self, request, cancellation):
        self.called += 1
        if self.deliveries is not None:
            yield from self.deliveries
        elif self.batch is None:
            yield AdapterBatch(request.request_id,0,True,c.SourceBinding('synthetic','snapshot1','map1','missing'),
                c.Coverage(None,0,False),c.Coverage(None,0,False),None,'missing','synthetic missing')
        else:
            b=self.batch
            yield AdapterBatch(request.request_id,0,True,b.metadata.source,b.metadata.coverage,
                               c.Coverage(b.row_count,b.row_count,True),b)


def inputs(family='bars'):
    task, _, _ = fixture()
    config=task.config
    if family=='bars':
        from equity_feature_example_extensions import ExampleSource, example_request
        request=example_request()
        batch=next(ExampleSource().iter_batches(request,type('Continue',(),{'is_cancelled':lambda self:False})())).batch
    else:
        kind=c.DataKind.TRADE if family=='trades' else c.DataKind.QUOTE
        sampling='none' if family=='trades' else 'trade_snapshot'
        if family=='trades':
            facts=dict(instrument_id=('A',)*3,session_id=('S',)*3,event_ns=(110,130,160),order_key=(1,2,3),
                       event_id=('t1','t2','t3'),eligible=(True,)*3,price=(100,102,101),size=(2,3,5),known_at_ns=(110,130,210))
        else:
            facts=dict(instrument_id=('A',)*4,session_id=('S',)*4,event_ns=(110,120,130,140),order_key=(1,2,3,4),
                       event_id=('q1','q2','q3','q4'),bid=(100,101,105,None),ask=(102,101,104,102),known_at_ns=(110,120,130,140))
            config=replace(config,parameters=config.parameters+(c.Parameter('observation_limit',4),))
        n=len(facts['instrument_id'])
        batch=c.CanonicalBatch(kind,tuple(c.Column(k,v) for k,v in facts.items()),
            c.BatchMetadata('demo',c.SourceBinding('synthetic','snapshot1','map1',family),c.Coverage(n,n,True),
                            c.PriceUnit(0,'USD'),sampling=sampling,scope=c.InputScope(100,200,'example-v1')))
        request=AcquisitionRequest('request',kind,'demo',('A',),('S',),100,200,'snapshot1',c.PriceUnit(0,'USD'),
            config.availability,sampling=sampling,max_batch_rows=n,max_rows=n,max_batches=1)
    calc={'bars':compute_bars,'trades':compute_trades,'quotes':compute_quotes}[family]
    spec=SessionCommandSpec('job1','generation1','A-S',family,request,config,
        descriptor(calc(None,config,entity=c.EntityKey('A','S'))).features,
        (IntervalSpec('P',0,100),IntervalSpec('S',100,200)),'revision1','synthetic-conformance',65536)
    return spec,batch,Source(batch,request)


class Commands(unittest.TestCase):
    def rejected(self, code, call):
        with self.assertRaises(CommandError) as caught: call()
        self.assertEqual(caught.exception.code,code)
        self.assertNotIn('secret',str(caught.exception))

    def test_bars_direct_factory_installed_cli(self):
        direct,factory=run_demo(),run_demo(factory=True)
        self.assertEqual(direct['task_sha256'],factory['task_sha256'])
        self.assertEqual(direct['content_sha256'],factory['content_sha256'])
        v=direct['values']
        self.assertEqual((v['session.bar.volume'],v['session.bar.notional'],v['session.bar.close_weighted_price']),(500,51200,102.6))
        self.assertIsNone(v['session.price.overnight_gap'])
        result=subprocess.run([sys.executable,'-I','-m','equity_feature_workers.cli','--demo','both'],check=True,capture_output=True,text=True)
        records=json.loads(result.stdout)
        self.assertTrue(all(r['verified_readback'] for r in records))
        self.assertEqual(records[0]['values']['session.bar.volume'],500)

    def test_trades_independent_exact_goldens(self):
        spec,b,source=inputs('trades')
        outcome=run_session(spec,source,ExampleSink(),requirements=LIMITS)
        v={col.feature_id.rsplit('.',1)[1]:col.values[0] for col in outcome.results[0].values}
        self.assertEqual((v['count'],v['volume'],v['notional']),(3,10,1011))
        self.assertAlmostEqual(v['vwap'],101.1,delta=1e-12)
        self.assertAlmostEqual(v['mean_size'],float(Fraction(10,3)),delta=1e-12)
        self.assertTrue(outcome.output.committed)

    def test_quotes_independent_goldens_and_evidence(self):
        spec,b,source=inputs('quotes')
        outcome=run_session(spec,source,ExampleSink(),requirements=LIMITS)
        sample,counts=(col.values[0] for col in outcome.results[0].values)
        self.assertEqual(sample.mean_spread,1.0)
        self.assertAlmostEqual(sample.mean_bps,float(Fraction(10000,101)),delta=1e-12)
        self.assertEqual(counts,c.QuoteStateCounts(1,1,1,1))
        self.assertEqual(len(outcome.results[0].evidence),4)

    def test_injected_cli_all_families_and_missing_components(self):
        for family in ('bars','trades','quotes'):
            spec,b,source=inputs(family)
            output=io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main(['--session',family],spec=spec,source=source,sink=ExampleSink(),requirements=LIMITS),0)
            self.assertTrue(json.loads(output.getvalue())[0]['verified_readback'])
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as caught:
            main(['--session','trades'])
        self.assertEqual(caught.exception.code,2)

    def test_missing_empty_partial_distinct(self):
        spec,b,_=inputs('trades')
        empty=replace(b,columns=tuple(c.Column(col.name,()) for col in b.columns),metadata=replace(b.metadata,coverage=c.Coverage(0,0,True)))
        partial=replace(b,metadata=replace(b.metadata,coverage=c.Coverage(4,3,False)))
        outcomes=[run_session(spec,Source(batch,spec.request),ExampleSink(),requirements=LIMITS) for batch in (None,empty,partial)]
        self.assertEqual([o.output.task.inputs[0].state for o in outcomes],['missing','empty','partial'])
        self.assertEqual(len({o.output.task.task_sha256 for o in outcomes}),3)
        self.assertTrue(all(col.values[0] is None for col in outcomes[0].results[0].values))
        self.assertEqual(outcomes[1].results[0].values[0].values[0],0)
        self.assertTrue(all(q.status==c.Status.INCOMPLETE_COVERAGE for q in outcomes[2].results[0].quality))
        self.assertIsNone(outcomes[0].output.task.inputs[0].known_at_ns)

    def test_compatible_chunks_preserve_coverage_and_bind_order(self):
        spec,b,_=inputs('trades')
        request=replace(spec.request,max_batch_rows=2,max_batches=2)
        spec=replace(spec,request=request)
        deliveries=[]
        for i,sl in enumerate((slice(0,2),slice(2,3))):
            chunk=replace(b,columns=tuple(c.Column(col.name,col.values[sl]) for col in b.columns),
                          metadata=replace(b.metadata,source=replace(b.metadata.source,input_id='chunk'+str(i))))
            deliveries.append(AdapterBatch(request.request_id,i,i==1,chunk.metadata.source,b.metadata.coverage,
                c.Coverage(chunk.row_count,chunk.row_count,True),chunk))
        out=run_session(spec,Source(b,request,deliveries),ExampleSink(),requirements=LIMITS)
        self.assertEqual(out.results[0].values[1].values[0],10)
        self.assertEqual(out.output.task.inputs[0].binding.metadata.coverage,c.Coverage(3,3,True))
        self.assertTrue(out.output.task.inputs[0].binding.metadata.source.input_id.startswith('worker-chunks-'))
        changed=replace(deliveries[1].batch,metadata=replace(deliveries[1].batch.metadata,scope=c.InputScope(100,199,'example-v1')))
        deliveries[1]=replace(deliveries[1],batch=changed)
        self.rejected(CommandErrorCode.SOURCE,lambda:run_session(spec,Source(b,request,deliveries),ExampleSink(),requirements=LIMITS))

    def test_request_and_inventory_rejected_before_callbacks(self):
        spec,b,source=inputs()
        for changes in ({'request':replace(spec.request,namespace='other')},{'features':spec.features[:-1]},
                        {'config':replace(spec.config,session=replace(spec.config.session,include_closing_auction=True))}):
            self.rejected(CommandErrorCode.CONFIG,lambda:replace(spec,**changes))
        self.assertEqual(source.called,0)

    def test_limits_and_cancellation(self):
        spec,b,source=inputs('trades')
        self.rejected(CommandErrorCode.LIMIT,lambda:run_session(replace(spec,max_input_bytes=1024),source,ExampleSink(),requirements=LIMITS))
        request=replace(spec.request,max_rows=2,max_batch_rows=2)
        self.rejected(CommandErrorCode.LIMIT,lambda:run_session(replace(spec,request=request),Source(b,request),ExampleSink(),requirements=LIMITS))
        class Cancelled:
            def is_cancelled(self): return True
        untouched=Source(b,spec.request)
        self.rejected(CommandErrorCode.CANCELLED,lambda:run_session(spec,untouched,ExampleSink(),requirements=LIMITS,cancellation=Cancelled()))
        self.assertEqual(untouched.called,0)

    def test_safe_source_and_readback_failure(self):
        spec,b,source=inputs()
        class BrokenSource(Source):
            def iter_batches(self,*args): raise ValueError('secret credential value')
        self.rejected(CommandErrorCode.SOURCE,lambda:run_session(spec,BrokenSource(b,spec.request),ExampleSink(),requirements=LIMITS))
        class BrokenSink(ExampleSink):
            def read(self,receipt): raise ValueError('secret credential value')
        self.rejected(CommandErrorCode.READBACK,lambda:run_session(spec,source,BrokenSink(),requirements=LIMITS))

    def test_wrong_actual_entities_and_headers(self):
        spec,b,source=inputs()
        result=compute_bars(b,spec.config,entity=c.EntityKey('A','S'))
        other=c.EntityKey('B','S')
        wrong=replace(result,values=tuple(replace(col,entities=(other,)) for col in result.values),
                      quality=tuple(replace(q,entity=other) for q in result.quality))
        from equity_feature_workers import commands
        with patch.dict(commands._CALCULATORS,{'bars':lambda *args,**kwargs:wrong}):
            self.rejected(CommandErrorCode.RESULT,lambda:run_session(spec,source,ExampleSink(),requirements=LIMITS))
        truncated=replace(result,values=result.values[:-1],quality=tuple(q for q in result.quality if q.feature_id!=result.values[-1].feature_id))
        with patch.dict(commands._CALCULATORS,{'bars':lambda *args,**kwargs:truncated}):
            self.rejected(CommandErrorCode.RESULT,lambda:run_session(spec,source,ExampleSink(),requirements=LIMITS))

    def test_extra_after_final_and_bad_ordinal_rejected(self):
        spec,b,_=inputs()
        d=AdapterBatch(spec.request.request_id,0,True,b.metadata.source,b.metadata.coverage,c.Coverage(2,2,True),b)
        for deliveries in ([d,d],[replace(d,ordinal=1)]):
            self.rejected(CommandErrorCode.SOURCE,lambda:run_session(spec,Source(b,spec.request,deliveries),ExampleSink(),requirements=LIMITS))

    def test_batch_bound_interchunk_cancel_and_preacquisition_sink_admission(self):
        spec,b,source=inputs()
        d=AdapterBatch(spec.request.request_id,0,False,b.metadata.source,b.metadata.coverage,c.Coverage(2,2,True),b)
        self.rejected(CommandErrorCode.LIMIT,lambda:run_session(spec,Source(b,spec.request,[d,d]),ExampleSink(),requirements=LIMITS))
        class Token:
            stopped=False
            def is_cancelled(self): return self.stopped
        token=Token()
        class Stops(Source):
            def iter_batches(self,request,cancellation):
                yield d
                token.stopped=True
                yield replace(d,ordinal=1,final=True)
        # Cancellation after a callback yields is checked before processing its content.
        self.rejected(CommandErrorCode.CANCELLED,lambda:run_session(replace(spec,request=replace(spec.request,max_batches=2,max_rows=4)),
            Stops(b,spec.request),ExampleSink(),requirements=LIMITS,cancellation=token))
        self.rejected(CommandErrorCode.CONFIG,lambda:run_session(spec,source,ExampleSink(),
            requirements=replace(LIMITS,visibility='manifest_last')))
        self.assertEqual(source.called,0)

    def test_wrong_readback_and_failed_commit_safe(self):
        spec,b,source=inputs()
        class EmptyReadback(ExampleSink):
            def read(self,receipt): return ()
        self.rejected(CommandErrorCode.READBACK,lambda:run_session(spec,source,EmptyReadback(),requirements=LIMITS))
        class FailedCommit(ExampleSink):
            def commit(self,session): raise ValueError('secret backend exception')
        self.rejected(CommandErrorCode.SINK,lambda:run_session(spec,source,FailedCommit(),requirements=LIMITS))

    def test_unknown_and_future_knowledge_not_substituted(self):
        spec,b,_=inputs('trades')
        for known in ((None,None,None),(110,130,999)):
            batch=replace(b,columns=tuple(c.Column(col.name,known) if col.name=='known_at_ns' else col for col in b.columns))
            out=run_session(spec,Source(batch,spec.request),ExampleSink(),requirements=LIMITS)
            self.assertTrue(all(col.values[0] is None for col in out.results[0].values))
            self.assertIsNone(out.output.task.inputs[0].known_at_ns)


if __name__=='__main__': unittest.main()
