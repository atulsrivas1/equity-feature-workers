"""Independent admission/source boundary controls; no installed/native proof."""
from pathlib import Path
import copy
from dataclasses import replace
import json
import os
import subprocess
import sys
import unittest

from equity_feature_client import ProducerExpectation,ResultView
from equity_feature_mcp import MCPProfile,CommandRegistration,StdioServer
from equity_feature_mcp import evidence as module
from equity_feature_mcp.evidence import EvidenceCatalog
import equity_feature_contracts as c
import equity_feature_diagnostics as diagnostics
import equity_feature_explanations as explanations
from test_evidence_owner import real_public_conversion_fixture,canonical,typed_diagnostic,ORACLES

ROOT=Path(__file__).resolve().parents[2]


class Admission(unittest.TestCase):
    def test_independent_full_report_expectations_and_quality_match_owner(self):
        from expected_reports import expected_reports
        from equity_feature_mcp.evidence import DiagnosticAssociation
        from equity_feature_contracts import EntityKey
        expected,quality=expected_reports(ROOT)
        profile,entry,native=real_public_conversion_fixture()
        explanation=explanations.explain_result(native,entry.receipt,entry.configuration,
            entity=EntityKey('A','S'),feature_id='session.trade.volume')
        report=diagnostics.DiagnosticReport(diagnostics.DecisionContext('demo',EntityKey('A','S'),native.metadata.availability),
            diagnostics.DiagnosticPolicy('synthetic-advisory','advisory','deny'),
            (diagnostics.CheckFinding('observation','synthetic-output','freshness',entry.receipt.output,'unknown','warning',('missing_observation',),False,1,None),))
        catalog=EvidenceCatalog(profile,(replace(entry,explanation=explanation,diagnostics=report,
            associations=(DiagnosticAssociation('observation','output'),)),))
        self.assertEqual(dict(catalog._entries[0].artifacts),expected)
        self.assertEqual(json.loads(catalog._entries[0].quality),quality)

    def test_base_import_and_disabled_server_require_no_optional_modules(self):
        # Fresh interpreter with only base client/MCP source roots and a rejecting
        # optional-module finder; this is intentionally not an installed check.
        source='''import sys,importlib.abc,json
sys.path[:0]=ROOTS
blocked=('equity_feature_contracts','equity_feature_io_sdk','equity_features','equity_feature_evidence','equity_feature_explanations','equity_feature_diagnostics','equity_feature_service','equity_feature_workers','mcp')
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if any(fullname==n or fullname.startswith(n+'.') for n in blocked):raise ImportError('optional blocked')
sys.meta_path.insert(0,Deny())
from equity_feature_mcp import StdioServer,MCPProfile,CommandRegistration
from equity_feature_mcp.evidence import EvidenceCatalog
from equity_feature_client import RemoteClient,ProducerExpectation,ScopeKey,FeatureRef
raw=json.loads(FIXTURE.read_bytes())['native_cases'][0]['complete_producer_payload']
expected=ProducerExpectation(raw['context'],ScopeKey('A','S',100,200),tuple(FeatureRef(**v) for v in raw['executed_features']),raw['backend_id'],raw['backend_version'])
profile=MCPProfile('owner',(),(CommandRegistration('cmd',expected),),60)
server=StdioServer(RemoteClient('http://127.0.0.1:1',lambda:'A'*43),profile)
server.close()
try:EvidenceCatalog(profile,())
except ValueError as e:assert e.args==('evidence_dependency',)
else:raise AssertionError('catalog requires extra')
assert not any(any(v==n or v.startswith(n+'.') for n in blocked) for v in sys.modules)
print('base import/disabled owner optional-module refusal PASS; source only')
'''
        preamble='from pathlib import Path\nROOTS='+repr([str(ROOT/'packages/client/src'),str(ROOT/'packages/mcp/src')])+'\nFIXTURE=Path('+repr(str(ROOT/'docs/EQ081_CLIENT_FIXTURES.json'))+')\n'
        environment=os.environ.copy();environment.pop('PYTHONPATH',None)
        result=subprocess.run([sys.executable,'-I','-c',preamble+source],env=environment,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_all_byte_budget_vectors_use_inclusive_primitive(self):
        for limit in (65536,131072,262144,1048576):
            at=b'"'+b'x'*(limit-2)+b'"'
            self.assertEqual(module._bounded(at,limit),at)
            with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._bounded(at+b' ',limit)

    def test_node_boundary_counts_container_keys_and_values(self):
        self.assertEqual(module._tree([None]*32767),32768)
        with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._tree([None]*32768)
        self.assertEqual(module._tree({'x':None}),3)

    def test_exact_config_sequence_types_not_iterable_callbacks(self):
        _,entry,_=real_public_conversion_fixture();called=[]
        class Untrusted:
            def __iter__(self):called.append(True);raise AssertionError('called')
        cfg=copy.deepcopy(entry.configuration);object.__setattr__(cfg,'parameters',Untrusted())
        profile,_,_=real_public_conversion_fixture()
        with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):EvidenceCatalog(profile,(replace(entry,configuration=cfg),))
        self.assertEqual(called,[])

    def test_profile_and_owner_complete_snapshot_must_match(self):
        profile,entry,_=real_public_conversion_fixture();catalog=EvidenceCatalog(profile,(entry,))
        changed=MCPProfile('different',profile.slices,profile.commands,60)
        from equity_feature_client import RemoteClient
        client=RemoteClient('http://127.0.0.1:1',lambda:'A'*43)
        try:
            with self.assertRaisesRegex(ValueError,'^profile_invalid$'):StdioServer(client,changed,evidence=catalog)
        finally:client.close()

    def test_explanation_full_focus_disclosure_and_missing_receipt_refuse(self):
        profile,entry,native=real_public_conversion_fixture()
        valid=explanations.explain_result(native,entry.receipt,entry.configuration,
            entity=native.values[0].entities[0],feature_id=native.values[0].feature_id)
        EvidenceCatalog(profile,(replace(entry,explanation=valid),))
        for field,value in (('feature_id','other'),('display_limit',65),('selected_retained_count',99)):
            bad=copy.deepcopy(valid);object.__setattr__(bad,field,value)
            with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):EvidenceCatalog(profile,(replace(entry,explanation=bad),))
        with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):EvidenceCatalog(profile,(replace(entry,receipt=None,explanation=valid),))

    def test_unsupported_backend_is_not_admitted_for_explanation(self):
        profile,entry,native=real_public_conversion_fixture()
        explanation=explanations.explain_result(native,entry.receipt,entry.configuration,
            entity=native.values[0].entities[0],feature_id=native.values[0].feature_id)
        raw=entry.view.payload_json();raw['backend_id']='declared-other'
        raw['metadata']['fields']['backend_id']['value']='declared-other'
        expected=ProducerExpectation(raw['context'],profile.commands[0].expectation.scope,
            profile.commands[0].expectation.executed_features,'declared-other',raw['backend_version'])
        changed=MCPProfile('owner',(),(CommandRegistration('cmd',expected),),60)
        view=ResultView(canonical(dict(schema='equity.remote',version='1.1',kind='result',request_id='owned',payload=raw)))
        # The plain producer remains usable; old-backend report must not attach.
        EvidenceCatalog(changed,(replace(entry,view=view,receipt=None),))
        with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):EvidenceCatalog(changed,(replace(entry,view=view,explanation=explanation),))

    def test_public_report_counts_unknowns_and_no_association_inference(self):
        profile,entry,native=real_public_conversion_fixture()
        report,_=typed_diagnostic(ORACLES['one_unknown_diagnostic']['value'])
        catalog=EvidenceCatalog(profile,(replace(entry,diagnostics=report),))
        raw=dict(catalog._entries[0].artifacts)['diagnostics'];value=json.loads(raw)
        self.assertEqual(value['report']['unknown_count'],1)
        self.assertIsNone(value['report']['findings'][0]['content'])
        self.assertEqual(value['report']['admission'],'denied');self.assertFalse(value['input_content_verified'])


if __name__=='__main__':unittest.main()
