"""Public MCP dispatch with patched public client outcomes; not native integration."""
from pathlib import Path
import copy
from dataclasses import replace
import hashlib
import io
import json
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tests/mcp'))
import test_server
from test_evidence_owner import real_public_conversion_fixture, canonical
from equity_feature_client import DiscoveryView, Failure, JobView, Outcome, RemoteClient, ResultView
from equity_feature_mcp import StdioServer, MCPProfile
from equity_feature_mcp import server as runtime
from equity_feature_mcp import _wire
from equity_feature_mcp.evidence import EvidenceCatalog


class Tools(unittest.TestCase):
    request=test_server.Server.request
    initialize=test_server.Server.initialize
    call=test_server.Server.call
    view=test_server.Server.view

    def setUp(self):
        self.profile,self.registration,_=real_public_conversion_fixture()
        self.catalog=EvidenceCatalog(self.profile,(self.registration,))
        self.client=RemoteClient('http://127.0.0.1:1',lambda:'A'*43)
        self.server=StdioServer(self.client,self.profile,evidence=self.catalog)
        self.now=0;self.server._ledger._clock=lambda:self.now
        self.original=self.registration.view.payload_json()
        self.job=dict(job_id='job',command_digest=self.profile.commands[0].expectation.command_digest,
                      state='succeeded',result_id='result',error=None)

    def tearDown(self):self.server.close()

    def admitted(self):
        self.initialize()
        with patch.object(RemoteClient,'calculate',return_value=self.view(JobView,'job',self.job)):
            self.job_reference=self.call('equity_calculate',dict(profile='cmd',idempotency_key='key'))['payload']['reference']
        with patch.object(RemoteClient,'artifact_read',return_value=self.view(ResultView,'result',self.original)):
            return self.call('equity_artifact_reference',dict(reference=self.job_reference))['payload']['reference']

    def test_every_chunk_receipt_missing_and_cache_hit_reauthorize_no_result_read(self):
        reference=self.admitted()
        with patch.object(RemoteClient,'artifact_read',return_value=self.view(ResultView,'result',self.original)) as actual,patch.object(RemoteClient,'result_read') as wrong:
            receipt=self.call('equity_receipt',dict(reference=reference))
            self.assertTrue(receipt['ok']);self.assertEqual(receipt['payload']['presence'],'supplied')
            self.assertFalse(receipt['payload']['artifact']['input_content_verified'])
            self.assertEqual(json.loads(receipt['payload']['chunk'])['inputs'][0]['content']['digest'],'0'*64)
            self.assertEqual(self.call('equity_explanation',dict(reference=reference))['payload']['presence'],'not_supplied')
            self.assertEqual(self.call('equity_diagnostics',dict(reference=reference))['payload']['presence'],'not_supplied')
            assembled='';offset=0
            while True:
                payload=self.call('equity_evidence_read',dict(reference=reference,kind='producer',offset=offset))['payload']
                assembled+=payload['chunk']
                self.assertEqual(payload['artifact']['sha256'],hashlib.sha256(canonical(self.original)).hexdigest())
                if payload['complete']:break
                offset=payload['next_offset']
            self.assertEqual(assembled,canonical(self.original).decode())
            self.assertEqual(actual.call_count,3+(len(canonical(self.original))+4095)//4096)
            wrong.assert_not_called()

    def test_unknown_native_and_job_role_references_refuse_before_provider(self):
        reference=self.admitted()
        with patch.object(RemoteClient,'artifact_read') as actual:
            for value in ('native-result-id','unknown',self.job_reference):
                result=self.call('equity_receipt',dict(reference=value))
                self.assertEqual(result['failure']['code'],'unknown_reference')
            actual.assert_not_called()
        self.assertIn(reference,self.server._ledger._records)

    def test_exact_integer_and_closed_argument_refusals_before_provider(self):
        reference=self.admitted()
        with patch.object(RemoteClient,'artifact_read') as actual:
            for offset in (True,False,1.0,0.0,-1,1,4095,4097,1048576,None):
                answer=self.server._dispatch(self.request('tools/call',dict(name='equity_evidence_read',arguments=dict(reference=reference,kind='producer',offset=offset))))
                self.assertEqual(answer['error']['code'],-32602,offset)
            for name in ('equity_receipt','equity_explanation','equity_diagnostics','equity_evidence_capabilities'):
                args={} if name.endswith('capabilities') else {'reference':reference}
                answer=self.server._dispatch(self.request('tools/call',dict(name=name,arguments={**args,'code':'not_allowed'})))
                self.assertEqual(answer['error']['code'],-32602)
            actual.assert_not_called()

    def test_missing_and_past_end_offsets_are_after_one_native_read(self):
        reference=self.admitted()
        with patch.object(RemoteClient,'artifact_read',return_value=self.view(ResultView,'result',self.original)) as actual:
            for kind,offset in (('producer',1044480),('explanation',4096),('diagnostics',4096)):
                answer=self.call('equity_evidence_read',dict(reference=reference,kind=kind,offset=offset))
                self.assertEqual(answer['failure']['code'],'evidence_bounds')
            self.assertEqual(actual.call_count,3)

    def test_auth_denial_invalidates_linked_family_and_never_serves_cache(self):
        for status in (401,403):
            with self.subTest(status=status):
                if self.server._initialized:self.tearDown();self.setUp()
                reference=self.admitted()
                with patch.object(RemoteClient,'artifact_read',return_value=self.view(ResultView,'result',self.original)):
                    self.assertTrue(self.call('equity_receipt',dict(reference=reference))['ok'])
                with patch.object(RemoteClient,'artifact_read',return_value=Outcome(failure=Failure('authorization','remote_denial','decode',status))) as actual:
                    result=self.call('equity_receipt',dict(reference=reference))
                    self.assertFalse(result['ok']);self.assertEqual(result['failure']['status'],status)
                    self.assertEqual(self.call('equity_receipt',dict(reference=reference))['failure']['code'],'reference_expired')
                    self.assertEqual(self.call('equity_job_status',dict(reference=self.job_reference))['failure']['code'],'reference_expired')
                    self.assertEqual(actual.call_count,1)

    def test_same_ids_changed_complete_producer_fails_binding_and_tombstones(self):
        reference=self.admitted();changed=copy.deepcopy(self.original)
        changed['columns'][0]['values'][0]['value']='11'
        # Direct patched public view is only a dispatch adversary, not a native validation claim.
        view=self.view(ResultView,'result',changed)
        with patch.object(RemoteClient,'artifact_read',return_value=view) as actual:
            answer=self.call('equity_receipt',dict(reference=reference))
            self.assertEqual(answer['failure']['code'],'evidence_binding')
            self.assertEqual(self.call('equity_receipt',dict(reference=reference))['failure']['code'],'reference_expired')
            self.assertEqual(actual.call_count,1)

    def test_uncatalogued_and_disabled_stay_truthful_after_authorization(self):
        for catalog,presence in ((EvidenceCatalog(self.profile,()),'not_supplied'),(None,'not_enabled')):
            with self.subTest(presence=presence):
                self.server.close();self.client=RemoteClient('http://127.0.0.1:1',lambda:'A'*43)
                self.server=StdioServer(self.client,self.profile,evidence=catalog);self.server._ledger._clock=lambda:self.now
                reference=self.admitted()
                with patch.object(RemoteClient,'artifact_read',return_value=self.view(ResultView,'result',self.original)) as actual:
                    result=self.call('equity_receipt',dict(reference=reference))
                    self.assertEqual(result['payload']['presence'],presence)
                    for key in ('quality','artifact','chunk','next_offset'):self.assertIsNone(result['payload'][key])
                    self.assertFalse(result['payload']['complete']);self.assertEqual(actual.call_count,1)

    def test_generic_capabilities_authenticate_and_disclose_no_result_membership(self):
        self.initialize()
        view=DiscoveryView(canonical(dict(schema='equity.remote',version='1.1',kind='discovery',request_id='owned',
            payload=dict(transport_versions=['1.1'],registry_snapshot='0'*64,features=[]))))
        with patch.object(RemoteClient,'discover',return_value=Outcome(view=view)) as actual:
            payload=self.call('equity_evidence_capabilities',{})['payload']
            self.assertTrue(payload['enabled']);self.assertFalse(payload['automatic_reports'])
            self.assertEqual(set(payload),{'representation','enabled','modes','kinds','chunk_bytes','automatic_reports','input_content_verified','source_authenticated','hosted_qualified'})
            self.assertEqual(actual.call_count,1)

    def test_final_local_expiry_suppresses_prepared_report_before_output(self):
        reference=self.admitted()
        # Source-only stdio buffer, actual final active-reference path; no child/socket/native proof.
        original=runtime._read
        def late(*args,**kwargs):
            answer=original(*args,**kwargs);self.now=60000000000;return answer
        incoming=io.BytesIO(_wire.encode_frame(self.request('tools/call',dict(name='equity_receipt',arguments=dict(reference=reference)))))
        output=io.BytesIO()
        with patch.object(RemoteClient,'artifact_read',return_value=self.view(ResultView,'result',self.original)) as actual,patch.object(runtime,'_read',side_effect=late):
            self.server.run_stdio(incoming,output)
        value=json.loads(output.getvalue())['result']['structuredContent']
        self.assertEqual(value['failure']['code'],'reference_expired');self.assertNotIn('payload',value)
        self.assertEqual(actual.call_count,1)

    def test_twelve_schemas_discovery_and_worst_escaped_wire_bounds(self):
        self.initialize()
        result=self.server._dispatch(self.request('tools/list',identifier='\\'*64))
        bounds=json.loads((ROOT/'docs/stories/EQ-108_WIRE_BOUNDS.json').read_bytes())
        self.assertEqual(len(_wire.encode_frame(result)),bounds['discovery_bytes'])
        frozen=json.loads((ROOT/'docs/stories/EQ-108_TOOL_SCHEMAS.json').read_bytes())
        self.assertEqual(result['result']['tools'],frozen['tools'])
        self.assertEqual(len(result['result']['tools']),12)
        self.assertEqual(self.server._dispatch(self.request('ping'))['result'],{})


if __name__=='__main__':unittest.main()
