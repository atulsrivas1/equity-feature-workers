"""Independent declared readiness, actual receipts, universe counts and exact proofs."""
from dataclasses import replace
from fractions import Fraction
import contextlib
import io
import json
import unittest

import equity_feature_contracts as c
from equity_feature_contracts.composition import CompositionSpec, FamilyResult
from equity_feature_contracts.breadth import CompletedClose, SMAInput
from equity_feature_contracts.specs import IntervalSpec
from equity_feature_example_extensions import ExampleSink
from equity_feature_example_extensions.sink import LIMITS
from equity_feature_io_contracts import ArtifactReference, PublicationState, PublicationStatus
from equity_feature_io_sdk import prepare_publication, publish
from equity_feature_workers import (
    BarrierLimits, Dependency, TaskNode, UniverseShard, BreadthCommandSpec, CommandError, CommandErrorCode,
    OutputManifest, RequiredCommandSpec, evaluate_readiness, inspect_barrier, run_assembly, run_breadth, run_required,
)
from equity_feature_workers.cli import main
from barrier_fixture import ORACLE, UNIVERSE, AGGREGATE, member
from required_fixture import SESSIONS, context, config, request, daily, LiteralSource

GRID=tuple(IntervalSpec(s.session_id,s.open_ns,s.close_ns) for s in SESSIONS)
BOUNDS=BarrierLimits(64,1048576)


def produce(name='A',direction=True,batch=None,ctx=None):
    feature='history.return' if direction else 'history.sma';family='history' if direction else 'sma_reference'
    cfg=config(feature,name);ctx=context(name) if ctx is None else ctx
    spec=RequiredCommandSpec('job','generation',name+'-'+feature,family,cfg,GRID,'revision1','synthetic-conformance',65536,
                            request=request(instrument=name),context=ctx,feature_ids=(feature,) if direction else ())
    batch=daily(name,tuple(ORACLE['closes'][name])) if batch is None else batch
    sink=ExampleSink();out=run_required(spec,LiteralSource(batch,spec.request),sink,requirements=LIMITS)
    dep=Dependency(name,out.command.output.task,out.command.output,sink)
    family_result=FamilyResult(name,out.command.results[0],cfg,ctx,out.witness)
    return spec,out,dep,family_result


def inputs(direction=True):
    dependencies=tuple(produce(i,direction)[2] for i in UNIVERSE.members)
    members=tuple(member(i) for i in UNIVERSE.members)
    shards=(UniverseShard('AB',('A','B'),('A','B')),UniverseShard('C',('C',),('C',)))
    spec=BreadthCommandSpec('job','generation','universe','direction_counts' if direction else 'above_sma_fraction',
        config('history.return' if direction else 'history.sma','UNIVERSE'),GRID,'revision1','synthetic-conformance',131072,
        UNIVERSE,AGGREGATE,shards)
    return spec,dependencies,members


class StatusSink:
    def __init__(self,original,state=None,corrupt=False):self.original,self.state,self.corrupt,self.calls=original,state,corrupt,0
    def capabilities(self):return self.original.capabilities()
    def lookup(self,key):
        self.calls+=1
        return PublicationStatus(self.state,None) if self.state is not None else self.original.lookup(key)
    def read(self,receipt):return () if self.corrupt else self.original.read(receipt)


class Barriers(unittest.TestCase):
    def rejected(self,code,call):
        with self.assertRaises(CommandError) as caught:call()
        self.assertEqual(caught.exception.code,code)
        self.assertNotIn('secret',str(caught.exception))

    def test_actual_lookup_and_readback_not_supplied_committed_flag(self):
        _,out,d,_=produce()
        barrier=inspect_barrier((d,),limits=BOUNDS,requirements=LIMITS)
        self.assertTrue(barrier.ready)
        self.assertEqual(barrier.verified[0].command.results,out.command.results)
        bad=replace(d,sink=StatusSink(d.sink,PublicationState.STAGING))
        barrier=inspect_barrier((bad,),limits=BOUNDS,requirements=LIMITS)
        self.assertFalse(barrier.ready);self.assertEqual(barrier.waiting[0].reason,'STAGING')
        # A supplied structural receipt and arbitrary staged files would still not satisfy this lookup.
        self.assertEqual(barrier.verified,())

    def test_staging_envelope_can_resolve_actual_committed_receipt(self):
        _,out,d,_=produce()
        staged=OutputManifest(out.command.output.task,out.command.output.envelope)
        self.assertFalse(staged.committed)
        barrier=inspect_barrier((replace(d,output=staged),),limits=BOUNDS,requirements=LIMITS)
        self.assertTrue(barrier.ready)
        self.assertEqual(barrier.verified[0].command.output.receipt,out.command.output.receipt)

    def test_optional_absence_wait_and_pending_policy(self):
        _,_,d,_=produce()
        for state in (PublicationState.ABSENT,PublicationState.STAGING,PublicationState.ABORTED,PublicationState.UNKNOWN):
            dep=replace(d,sink=StatusSink(d.sink,state),required=False,optional_policy='allow_absent')
            barrier=inspect_barrier((dep,),limits=BOUNDS,requirements=LIMITS)
            self.assertEqual(barrier.ready,state is PublicationState.ABSENT)
            self.assertEqual(bool(barrier.omitted),state is PublicationState.ABSENT)
        missing=Dependency('A',d.task,required=False,optional_policy='allow_absent')
        self.assertTrue(inspect_barrier((missing,),limits=BOUNDS,requirements=LIMITS).ready)
        self.assertFalse(inspect_barrier((replace(missing,optional_policy='wait'),),limits=BOUNDS,requirements=LIMITS).ready)
        self.rejected(CommandErrorCode.CONFIG,lambda:replace(missing,required=True))

    def test_unrelated_ready_execution_while_declared_history_waits(self):
        spec_a,out_a,dep_a,_=produce('A');_,out_b,dep_b,_=produce('B')
        consumer=replace(out_a.command.output.task,partition_id='consumer')
        waiting=Dependency('B',dep_b.task)
        nodes=(TaskNode(out_a.command.output.task),TaskNode(consumer,required=(dep_b.task.task_sha256,)))
        ready=evaluate_readiness(nodes,(waiting,),limits=BOUNDS,requirements=LIMITS)
        self.assertFalse(ready.barrier.ready)
        self.assertEqual(ready.ready_tasks,(out_a.command.output.task,))
        source=LiteralSource(daily('A'),spec_a.request)
        independent=run_required(spec_a,source,ExampleSink(),requirements=LIMITS)
        self.assertTrue(independent.command.output.committed);self.assertEqual(source.calls,1)
        self.assertEqual(independent.command.output.task.task_sha256,ready.ready_tasks[0].task_sha256)

    def test_corrupt_dependency_fault_does_not_block_unrelated_root(self):
        _,out,d,_=produce();_,other,_,_=produce('B')
        corrupt=replace(d,sink=StatusSink(d.sink,corrupt=True))
        consumer=replace(out.command.output.task,partition_id='consumer')
        ready=evaluate_readiness((TaskNode(other.command.output.task),TaskNode(consumer,required=(d.task.task_sha256,))),
            (corrupt,),limits=BOUNDS,requirements=LIMITS)
        self.assertEqual(ready.ready_tasks,(other.command.output.task,))
        self.assertEqual(ready.barrier.waiting[0].reason,CommandErrorCode.READBACK.value)

    def test_cycles_unknown_duplicates_before_sink_callbacks(self):
        _,out,d,_=produce();sink=StatusSink(d.sink);d=replace(d,sink=sink)
        task=out.command.output.task;other=replace(task,partition_id='other')
        for nodes in ((TaskNode(task,required=(task.task_sha256,)),),
                      (TaskNode(task,required=(other.task_sha256,)),TaskNode(other,required=(task.task_sha256,))),
                      (TaskNode(task,required=('unknown',)),),(TaskNode(task),TaskNode(task))):
            self.rejected(CommandErrorCode.CONFIG,lambda:evaluate_readiness(nodes,(d,),limits=BOUNDS,requirements=LIMITS))
        self.assertEqual(sink.calls,0)
        self.rejected(CommandErrorCode.CONFIG,lambda:inspect_barrier((d,d),limits=BOUNDS,requirements=LIMITS))

    def test_owned_assembly_order_missing_policy_and_readback_identity(self):
        _,_,a,ca=produce('A');_,_,b,cb=produce('B')
        spec=CompositionSpec('demo',SESSIONS[-1],config().availability,('B','A'))
        out=run_assembly(spec,(b,a),(ca,cb),limits=BOUNDS,requirements=LIMITS)
        self.assertEqual(tuple(c.instance_id for c in out.bundle.components),('B','A'))
        self.assertEqual(out.bundle.components[1],ca)
        missing=Dependency('B',b.task,required=False,optional_policy='allow_absent')
        out=run_assembly(spec,(missing,a),(ca,),limits=BOUNDS,requirements=LIMITS)
        self.assertEqual(out.bundle.missing_instances,('B',))
        waiting=run_assembly(spec,(replace(missing,optional_policy='wait'),a),(ca,),limits=BOUNDS,requirements=LIMITS)
        self.assertIsNone(waiting.bundle)
        with self.assertRaises(c.ContractError):
            replace(ca,context=replace(ca.context,grid_version='other-grid'))

    def test_assembly_wrong_actual_owned_grid_rejected(self):
        _,_,d,component=produce()
        spec=CompositionSpec('demo',SESSIONS[-1],config().availability,('A',))
        task=replace(d.task,governed_sessions=(IntervalSpec('S1',0,99),)+GRID[1:])
        wrong=Dependency('A',task)
        self.rejected(CommandErrorCode.RESULT,lambda:run_assembly(spec,(wrong,),(component,),limits=BOUNDS,requirements=LIMITS))

    def test_universe_complete_counts_and_exact_above_fraction(self):
        for direction in (True,False):
            spec,deps,members=inputs(direction)
            out=run_breadth(spec,deps,members,ExampleSink(),limits=BOUNDS,requirements=LIMITS)
            self.assertTrue(out.barrier.ready);self.assertTrue(out.command.output.committed)
            cell=out.breadth.result.values[0].values[0]
            if direction:self.assertEqual((cell.advancing,cell.declining,cell.unchanged,cell.expected),(1,1,1,3))
            else:self.assertEqual((cell.above,cell.eligible,cell.expected,cell.fraction),(1,3,3,Fraction(1,3)))
            self.assertEqual(out.breadth.exclusions,())
            self.assertEqual(out.command.output.task.instruments,('UNIVERSE',))

    def test_required_universe_member_staging_or_absence_no_aggregate(self):
        spec,deps,members=inputs()
        for missing in (Dependency('B',deps[1].task),replace(deps[1],sink=StatusSink(deps[1].sink,PublicationState.STAGING))):
            out=run_breadth(spec,(deps[0],missing,deps[2]),members,ExampleSink(),limits=BOUNDS,requirements=LIMITS)
            self.assertFalse(out.barrier.ready);self.assertIsNone(out.command);self.assertIsNone(out.breadth)
            self.assertEqual(out.barrier.waiting[0].instance_id,'B')

    def test_missing_extra_overlapping_universe_shards_reject(self):
        spec,deps,members=inputs()
        for shards in ((UniverseShard('AB',('A','B'),('A','B')),),
                       (UniverseShard('AB',('A','B'),('A','B')),UniverseShard('BC',('B','C'),('C',))),
                       (UniverseShard('ABCD',('A','B','C','D'),('A','B','C')),)):
            self.rejected(CommandErrorCode.CONFIG,lambda:replace(spec,shards=shards))
        self.rejected(CommandErrorCode.CONFIG,lambda:run_breadth(spec,deps[:2],members,ExampleSink(),limits=BOUNDS,requirements=LIMITS))
        self.rejected(CommandErrorCode.CONFIG,lambda:run_breadth(spec,deps,members[:2],ExampleSink(),limits=BOUNDS,requirements=LIMITS))
        self.rejected(CommandErrorCode.CONFIG,lambda:run_breadth(spec,(replace(deps[0],required=False),)+deps[1:],members,
            ExampleSink(),limits=BOUNDS,requirements=LIMITS))

    def test_committed_unavailable_member_is_mathematical_exclusion(self):
        spec,deps,members=inputs(False)
        # All three SMA tasks are physically read back, but C's explicit missing close is not eligible.
        members=members[:2]+(replace(members[2],close=None),)
        out=run_breadth(spec,deps,members,ExampleSink(),limits=BOUNDS,requirements=LIMITS)
        self.assertTrue(out.barrier.ready)
        cell=out.breadth.result.values[0].values[0]
        self.assertEqual((cell.above,cell.eligible,cell.expected,cell.fraction),(1,2,3,.5))
        self.assertEqual(out.breadth.result.quality[0].status,c.Status.INCOMPLETE_COVERAGE)
        self.assertEqual(out.breadth.exclusions[0].entity.instrument_id,'C')

    def test_unrequested_fields_do_not_affect_identity(self):
        spec,deps,members=inputs()
        other=tuple(replace(m,sma=None,close=None) for m in members)
        a=run_breadth(spec,deps,members,ExampleSink(),limits=BOUNDS,requirements=LIMITS)
        b=run_breadth(spec,deps,other,ExampleSink(),limits=BOUNDS,requirements=LIMITS)
        self.assertEqual(a.command.output.task.task_sha256,b.command.output.task.task_sha256)
        self.assertEqual(a.command.output.receipt.content_sha256,b.command.output.receipt.content_sha256)

    def test_limits_cancellation_and_safe_readback_failure(self):
        _,_,d,_=produce();sink=StatusSink(d.sink);d=replace(d,sink=sink)
        self.rejected(CommandErrorCode.LIMIT,lambda:inspect_barrier((d,),limits=BarrierLimits(1,1),requirements=LIMITS))
        self.assertEqual(sink.calls,0)
        # Structurally valid SDK receipt metadata can exceed the worker wire limit.
        artifacts=tuple(ArtifactReference(str(i)+'x'*4000,'0'*64,0) for i in range(300))
        oversized=replace(d.output,receipt=replace(d.output.receipt,artifacts=artifacts))
        self.rejected(CommandErrorCode.LIMIT,lambda:inspect_barrier((replace(d,output=oversized),),
            limits=BarrierLimits(1,8388608),requirements=LIMITS))
        self.assertEqual(sink.calls,0)
        token=type('Cancelled',(),{'is_cancelled':lambda self:True})()
        self.rejected(CommandErrorCode.CANCELLED,lambda:inspect_barrier((d,),limits=BOUNDS,requirements=LIMITS,cancellation=token))
        self.assertEqual(sink.calls,0)
        class Broken(StatusSink):
            def lookup(self,key):raise RuntimeError('secret backend path')
        barrier=inspect_barrier((replace(d,sink=Broken(d.sink)),),limits=BOUNDS,requirements=LIMITS)
        self.assertFalse(barrier.ready);self.assertEqual(barrier.waiting[0].reason,'READBACK_FAILED')
        self.assertNotIn('secret',str(barrier.waiting))

    def test_legitimate_sdk_commit_with_wrong_task_entity_does_not_satisfy_barrier(self):
        _,out,d,_=produce()
        result=out.command.results[0]
        wrong=replace(result,values=tuple(replace(col,entities=(c.EntityKey('B','S3'),)) for col in result.values),
                      quality=tuple(replace(q,entity=c.EntityKey('B','S3')) for q in result.quality),
                      evidence=tuple(replace(e,entity=c.EntityKey('B','S3')) for e in result.evidence))
        envelope=prepare_publication((wrong,),destination_scope=d.task.destination_scope,generation_id=d.task.generation_id,
            job_id=d.task.job_id,partition_id=d.task.partition_id,limits=LIMITS)
        sink=ExampleSink();receipt=publish(sink,envelope,(wrong,),requirements=LIMITS)
        declared=OutputManifest(d.task,envelope,receipt)
        self.assertTrue(declared.committed)
        barrier=inspect_barrier((Dependency('A',d.task,declared,sink),),limits=BOUNDS,requirements=LIMITS)
        self.assertFalse(barrier.ready)
        self.assertEqual(barrier.waiting[0].reason,CommandErrorCode.RESULT.value)

    def test_supplied_receipt_mismatch_and_wrong_owned_values(self):
        _,out,d,component=produce()
        supplied=replace(out.command.output,receipt=replace(out.command.output.receipt,caller_committed_at_ns=1))
        barrier=inspect_barrier((replace(d,output=supplied),),limits=BOUNDS,requirements=LIMITS)
        self.assertFalse(barrier.ready);self.assertEqual(barrier.waiting[0].reason,CommandErrorCode.READBACK.value)
        _,_,_,other=produce('A',batch=daily('A',(100,110,130)))
        spec=CompositionSpec('demo',SESSIONS[-1],config().availability,('A',))
        self.rejected(CommandErrorCode.RESULT,lambda:run_assembly(spec,(d,),(other,),limits=BOUNDS,requirements=LIMITS))

    def test_exact_half_tick_above_sma_not_rounded_scalar_reconstruction(self):
        spec,_,_=inputs(False)
        closes=(9007199254740991,9007199254740992,9007199254740993)
        b=daily('A',closes);b=replace(b,metadata=replace(b.metadata,source=replace(b.metadata.source,input_id='large-A')))
        producer,out,dep,_=produce('A',False,b)
        self.assertEqual((out.witness.numerator,out.witness.denominator),(18014398509481985,2))
        close=CompletedClose(c.EntityKey('A','S3'),closes[-1],300,c.InputBinding('daily_history',c.DataKind.DAILY,b.metadata),
                             2,c.InputScope(200,300,'fixture-v1'),c.Coverage(1,1,True))
        large=replace(member('A'),return_reference=None,sma=SMAInput(out.witness,producer.config,producer.context),close=close)
        spec=replace(spec,universe=replace(UNIVERSE,members=('A',)),shards=(UniverseShard('A',('A',),('A',)),))
        self.assertEqual(float(close.coefficient),out.witness.result.values[0].values[0])
        aggregate=run_breadth(spec,(dep,),(large,),ExampleSink(),limits=BOUNDS,requirements=LIMITS)
        self.assertEqual(aggregate.breadth.result.values[0].values[0].fraction,Fraction(1,1))

    def test_empty_explicit_universe_and_source_provenance_guard(self):
        spec,deps,members=inputs(False)
        empty=replace(spec,universe=replace(UNIVERSE,members=()),shards=())
        out=run_breadth(empty,(),(),ExampleSink(),limits=BOUNDS,requirements=LIMITS)
        self.assertTrue(out.barrier.ready);self.assertIsNone(out.breadth.result.values[0].values[0])
        self.assertEqual(out.breadth.result.quality[0].status,c.Status.NOT_APPLICABLE)
        alien=replace(members[0].close,source=replace(members[0].close.source,
            metadata=replace(members[0].close.source.metadata,source=replace(members[0].close.source.metadata.source,input_id='alien'))))
        self.rejected(CommandErrorCode.RESULT,lambda:run_breadth(spec,deps,(replace(members[0],close=alien),)+members[1:],
            ExampleSink(),limits=BOUNDS,requirements=LIMITS))

    def test_owned_grid_version_conflict_before_lookup(self):
        spec,deps,members=inputs()
        _,out,changed,_=produce('B',ctx=replace(context('B'),grid_version='different-grid'))
        counted=tuple(replace(d,sink=StatusSink(d.sink)) for d in (deps[0],changed,deps[2]))
        supplied=(members[0],replace(members[1],return_reference=out.witness),members[2])
        self.rejected(CommandErrorCode.RESULT,lambda:run_breadth(spec,counted,supplied,ExampleSink(),limits=BOUNDS,requirements=LIMITS))
        self.assertTrue(all(d.sink.calls==0 for d in counted))

    def test_injected_cli_wait_and_assembly_breadth_parity(self):
        for direction in (True,False):
            spec,deps,members=inputs(direction)
            for selected in (deps,(Dependency('A',deps[0].task),)+deps[1:]):
                with contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(main(['--breadth',spec.family],spec=spec,dependencies=selected,members=members,
                        sink=ExampleSink(),requirements=LIMITS,barrier_limits=BOUNDS),0)
                record=json.loads(output.getvalue())[0]
                self.assertEqual(record['ready'],selected==deps)
                if selected==deps:self.assertTrue(record['verified_readback'])
                else:self.assertNotIn('verified_readback',record)
        _,_,d,component=produce()
        comp=CompositionSpec('demo',SESSIONS[-1],config().availability,('A',))
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(['--assembly'],spec=comp,dependencies=(d,),components=(component,),requirements=LIMITS,barrier_limits=BOUNDS),0)
        self.assertTrue(json.loads(output.getvalue())[0]['components_verified_readback'])
        self.assertNotIn('verified_readback',json.loads(output.getvalue())[0])


if __name__=='__main__':unittest.main()
