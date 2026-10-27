"""Owned integration helpers using accepted public APIs and pinned fixture source."""
from dataclasses import replace
from pathlib import Path
import sys
# Qualification copies this fixed synthetic helper tree, never package sources.
sys.path.append(str(Path(__file__).resolve().parents[1]/'provenance'))
from native_fixture import create,acquire,population
from equity_feature_io_contracts import SinkRequirements
from equity_feature_workers import (GenerationStore,BarrierLimits,SerialPublisher,
    PublicationLimits,Dependency)
from equity_feature_evidence import fingerprint_batch
from equity_feature_contracts import EntityKey
import equity_feature_diagnostics as d
import equity_feature_conformance as c

LIMITS=SinkRequirements(max_results=1,max_chunk_bytes=1048576,max_total_bytes=1048576,
    max_result_cells=8192,max_evidence_rows=512)


def store(root):return GenerationStore(root,limits=BarrierLimits(),requirements=LIMITS)


def publish(sink,value):
    publisher=SerialPublisher(sink,'synthetic-output',limits=PublicationLimits(),requirements=LIMITS)
    publisher.submit(value.expected_task,(value.result,));outcome=publisher.drain()[0]
    assert outcome.committed and outcome.output is not None
    return replace(value,output=outcome.output)


def deps(values,sink):
    return tuple(Dependency(v.expected_task.task_sha256,v.expected_task,v.output,
        sink if v.output is not None else None) for v in values)


def diagnose(value,K):
    instrument=value.result.quality[0].entity.instrument_id
    # Actual expected identities/content are anchored before invoking diagnostics;
    # behavioral status/reason/count assertions below are independent literals.
    batch=value.acquired[0].batch;assert batch is not None
    anchor=fingerprint_batch(batch)
    checks=tuple(d.KnowledgeCheck('row'+str(n),'trades:'+str(n),known,content=anchor)
        for n,known in enumerate(batch.column('known_at_ns').values))
    findings=tuple(c.FindingSnapshot('row'+str(n),'trades:'+str(n),'knowledge_time',anchor,
        'failed' if K==200 and n==2 else 'passed','error' if K==200 and n==2 else 'info',
        ('late_knowledge',) if K==200 and n==2 else (),True,0,None) for n in range(3))
    expected=c.DiagnosticSnapshot(c.ContextSnapshot('demo',instrument,'S',200,K,210),
        c.PolicySnapshot('native-known','strict','deny'),findings,3,2 if K==200 else 3,
        1 if K==200 else 0,0,3,0,
        ('adjustment','declared_context','freshness','market_time','membership','revision'),
        'denied' if K==200 else 'admitted',False,False)
    return c.check_diagnostics('native-'+instrument+'-K'+str(K),
        d.DecisionContext('demo',EntityKey(instrument,'S'),value.expected_task.config.availability),
        d.DiagnosticPolicy('native-known','strict','deny'),checks,expected)
