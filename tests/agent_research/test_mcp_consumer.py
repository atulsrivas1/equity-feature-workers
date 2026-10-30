"""Patched public outcomes/real dispatch source controls, not native/SDK proof."""
from contextlib import ExitStack
import copy
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch
import anyio

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'examples'));sys.path.insert(0,str(ROOT/'tests/agent_research'))
import agent_research_mcp as consumer
import agent_research as research
from independent_oracle import fixture,literal_oracle,canonical
from equity_feature_client import RemoteClient,ProducerExpectation,ScopeKey,FeatureRef,Outcome,JobView,ResultView,DiscoveryView,Failure
from equity_feature_mcp import StdioServer,MCPProfile,CommandRegistration
from equity_feature_mcp.evidence import EvidenceCatalog,EvidenceRegistration,DiagnosticAssociation
from equity_feature_evidence import make_content_receipt,EnvironmentDeclaration,PackageVersion,ExecutionClaim,EvidenceDisclosure
from equity_feature_explanations import explain_result
from equity_feature_contracts import EntityKey
from equity_feature_diagnostics import DiagnosticReport,DiagnosticPolicy,DecisionContext,CheckFinding


class MCPConsumerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg,self.batch,self.result=fixture();self.original=literal_oracle()['producer']['complete_producer_payload']
        expected=ProducerExpectation(self.original['context'],ScopeKey('A','S',100,200),tuple(FeatureRef(**v) for v in self.original['executed_features']),self.original['backend_id'],self.original['backend_version'])
        self.profile=MCPProfile('synthetic-owner',(),(CommandRegistration('cmd',expected),),60)
        def view(cls,kind,payload):return Outcome(view=cls(canonical(dict(schema='equity.remote',version='1.1',kind=kind,request_id='synthetic',payload=payload))))
        self.producer=view(ResultView,'result',self.original)
        self.receipt=make_content_receipt(self.result,self.cfg,(('trades',self.batch),),EnvironmentDeclaration('CPython','3.12','synthetic-qualification',(PackageVersion('equity-feature-contracts','0.0.4a5'),PackageVersion('equity-features','0.0.4a6'))),ExecutionClaim(),EvidenceDisclosure('not_supplied',0))
        explanation=explain_result(self.result,self.receipt,self.cfg,entity=EntityKey('A','S'),feature_id='session.trade.count',evidence_limit=16)
        diagnostics=DiagnosticReport(DecisionContext('demo',EntityKey('A','S'),self.cfg.availability),DiagnosticPolicy('synthetic-advisory','advisory','deny'),(CheckFinding('observation','synthetic-output','freshness',self.receipt.output,'unknown','warning',('missing_observation',),False,1,None),))
        entry=EvidenceRegistration('cmd','job','result',self.producer.view,self.cfg,receipt=self.receipt,explanation=explanation,diagnostics=diagnostics,associations=(DiagnosticAssociation('observation','output'),),report_sharing_authorized=True)
        self.catalog=EvidenceCatalog(self.profile,(entry,))
        self.client=RemoteClient('http://127.0.0.1:1',lambda:'A'*43,attempts=1)
        self.server=StdioServer(self.client,self.profile,evidence=self.catalog)
        self.server._ledger._clock=lambda:0
        self.server._dispatch(dict(jsonrpc='2.0',id=1,method='initialize',params=dict(protocolVersion='2025-11-25',capabilities={},clientInfo=dict(name='source-test',version='1'))))
        self.server._dispatch(dict(jsonrpc='2.0',method='notifications/initialized'))
        job=dict(job_id='job',command_digest=expected.command_digest,state='succeeded',result_id='result',error=None)
        self.stack=ExitStack()
        for name,result in (('calculate',view(JobView,'job',job)),('job_status',view(JobView,'job',job)),('result_read',self.producer),('artifact_read',self.producer),('discover',view(DiscoveryView,'discovery',dict(transport_versions=['1.1'],registry_snapshot='0'*64,features=[])))):
            self.stack.enter_context(patch.object(RemoteClient,name,return_value=result))
        self.denied=False
    def tearDown(self) -> None:
        self.stack.close();self.server.close()
    def session(self,mutate=None):
        owner=self
        class PatchedSession:
            async def list_tools(self):
                data=owner.server._dispatch(dict(jsonrpc='2.0',id=2,method='tools/list',params={}))['result']['tools']
                return SimpleNamespace(tools=[SimpleNamespace(**tool) for tool in data])
            async def call_tool(self,name,arguments):
                answer=owner.server._dispatch(dict(jsonrpc='2.0',id=3,method='tools/call',params=dict(name=name,arguments=arguments)))
                if 'error' in answer:raise AssertionError('unexpected dispatch error')
                payload=copy.deepcopy(answer['result']['structuredContent'])
                if mutate is not None:mutate(name,arguments,payload)
                return SimpleNamespace(content=[SimpleNamespace(type='text',text=canonical(payload).decode('ascii'))],structuredContent=payload,isError=not payload['ok'])
        return PatchedSession()
    def run_collect(self,**kw):
        session=kw.pop('session',self.session())
        async def run():return await consumer.collect(session,**kw)
        return anyio.run(run)
    def test_full_content_dispatch_and_explicit_pure_replay(self) -> None:
        value=self.run_collect()
        self.assertEqual((value.assessment.narrative_status,value.assessment.decision,value.replay.status,value.replay.calculation_count),('matched','DEFER','matched',1))
        self.assertEqual(len(value.events),2*len(value.calls)+4)
        self.assertTrue(any(name=='equity_evidence_read' for name,_,_ in value.calls))
    def test_missing_retained_input_is_not_declared_complete(self) -> None:
        value=self.run_collect(retain_input=False)
        self.assertEqual((value.assessment.verified_input_count,value.replay.status,value.replay.calculation_count),(0,'unavailable',0))
    def test_wrong_narrative_rejects_even_matched_replay(self) -> None:
        value=self.run_collect(narrative='session.trade.count=4')
        self.assertEqual((value.assessment.decision,value.replay.status),('REJECT','matched'))
    def test_C18_changed_continuation_descriptor_refuses(self) -> None:
        def mutate(name,args,value):
            if name=='equity_evidence_read' and args['offset']>0:value['payload']['artifact']['sha256']='0'*64
        with self.assertRaises(research.ResearchExampleError):self.run_collect(session=self.session(mutate))
    def test_C18_changed_chunk_data_refuses(self) -> None:
        def mutate(name,args,value):
            if name=='equity_evidence_read' and args['offset']>0:
                chunk=value['payload']['chunk'];value['payload']['chunk']=('x' if chunk[0]!='x' else 'y')+chunk[1:]
        with self.assertRaises(research.ResearchExampleError):self.run_collect(session=self.session(mutate))
    def test_C24_arbitrary_argument_dispatch_refuses_before_client(self) -> None:
        with patch.object(RemoteClient,'artifact_read') as provider:
            answer=self.server._dispatch(dict(jsonrpc='2.0',id=3,method='tools/call',params=dict(name='equity_result_summary',arguments=dict(reference='A'*43,code='caller-code'))))
            self.assertEqual(answer['error']['code'],-32602);provider.assert_not_called()
    def test_invalid_public_schema_error_payload_free(self) -> None:
        def mutate(name,args,value):
            if name=='equity_evidence_capabilities':value['payload']['secret_payload']='do-not-render'
        with self.assertRaises(research.ResearchExampleError) as caught:self.run_collect(session=self.session(mutate))
        self.assertNotIn('do-not-render',str(caught.exception))


    def test_MCP01_continuation_offset_binds_requested_offset(self) -> None:
        def mutate(name,args,value):
            if name=='equity_evidence_read' and args['offset']>0:value['payload']['offset']=0
        with self.assertRaises(research.ResearchExampleError):self.run_collect(session=self.session(mutate))
    def test_MCP01_continuation_kind_binds_requested_kind(self) -> None:
        def mutate(name,args,value):
            if name=='equity_evidence_read' and args['offset']>0:value['payload']['kind']='receipt'
        with self.assertRaises(research.ResearchExampleError):self.run_collect(session=self.session(mutate))
    def test_MCP01_continuation_reference_binds_current_handle(self) -> None:
        def mutate(name,args,value):
            if name=='equity_evidence_read' and args['offset']>0:value['payload']['reference']='Z'*43
        with self.assertRaises(research.ResearchExampleError):self.run_collect(session=self.session(mutate))
    def test_MCP02_real_dispatch_denial_first_and_continuation_defer(self) -> None:
        for fail_at in (2,3):
            calls=[0]
            def denied(*args,**kw):
                calls[0]+=1
                return Outcome(failure=Failure('authorization','remote_denial','decode',401)) if calls[0]==fail_at else self.producer
            with self.subTest(fail_at=fail_at),patch.object(RemoteClient,'artifact_read',side_effect=denied):
                value=self.run_collect()
                self.assertEqual((value.assessment.decision,value.assessment.narrative_status,value.replay.status,value.replay.calculation_count),('DEFER','unavailable','not_requested',0))
                self.assertEqual(calls[0],fail_at)
                self.assertEqual(value.calls[-1][2],'failed')
            if fail_at==2:
                # Native denial retired this local reference family. A separate
                # source fixture is necessary for the independent continuation case.
                self.tearDown();self.setUp()
    def test_MCP03_nonASCII_public_text_closed_error(self) -> None:
        session=self.session()
        class NonASCII:
            async def list_tools(self):return await session.list_tools()
            async def call_tool(self,name,args):
                result=await session.call_tool(name,args);result.content[0].text+='\u00e9';return result
        with self.assertRaises(research.ResearchExampleError):self.run_collect(session=NonASCII())


    def test_MCP04_real_dispatch_local_expiry_has_no_HTTP_status(self) -> None:
        for offset in (0,4096):
            session=self.session();owner=self
            class Expiring:
                async def list_tools(self):return await session.list_tools()
                async def call_tool(self,name,args):
                    if name=='equity_evidence_read' and args['offset']==offset:
                        owner.server._ledger._clock=lambda:60000000000
                    return await session.call_tool(name,args)
            with self.subTest(offset=offset):
                value=self.run_collect(session=Expiring())
                self.assertEqual((value.assessment.decision,value.assessment.narrative_status,value.replay.status,value.replay.calculation_count),('DEFER','unavailable','not_requested',0))
                import json
                failed=json.loads(value.calls[-1][1]);self.assertEqual(failed['failure']['source'],'mcp')
                self.assertEqual(failed['failure']['code'],'reference_expired');self.assertNotIn('status',failed['failure'])
            if offset==0:self.tearDown();self.setUp()


if __name__=='__main__':unittest.main()
