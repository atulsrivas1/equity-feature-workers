"""Synthetic published-result owner demonstration; no network/native execution.

Run with the optional MCP evidence extra. A production owner supplies an actual
completed public ResultView and identities; this example reads fixed public
synthetic fixtures to demonstrate only preparation and immutable byte accounting.
"""
from pathlib import Path
import json

from equity_feature_client import FeatureRef, ProducerExpectation, ResultView, ScopeKey
from equity_feature_client.native import convert_result
from equity_feature_contracts import ConfigSpec
from equity_feature_contracts import results as r
from equity_feature_diagnostics import CheckFinding, DecisionContext, DiagnosticPolicy, DiagnosticReport
from equity_feature_evidence import (ContentFingerprint, EnvironmentDeclaration, EvidenceDisclosure,
    ExecutionClaim, FormulaBinding, InputFingerprint, PackageVersion, fingerprint_config, fingerprint_result, make_receipt)
from equity_feature_explanations import explain_result
from equity_feature_mcp import CommandRegistration, MCPProfile
from equity_feature_mcp.evidence import DiagnosticAssociation, EvidenceCatalog, EvidenceRegistration


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    case = json.loads((root/'docs/EQ081_CLIENT_FIXTURES.json').read_bytes())['native_cases'][0]
    producer = case['complete_producer_payload']
    expected = ProducerExpectation(producer['context'], ScopeKey('A','S',100,200),
        tuple(FeatureRef(**v) for v in producer['executed_features']), producer['backend_id'], producer['backend_version'])
    fixtures = json.loads((root/'tests/service/fixtures/jobs_native.json').read_bytes())['fixtures']
    config_value = next(v for v in fixtures if v['family']=='trades')['native_spec']['config']
    configuration = ConfigSpec.from_json(json.dumps(config_value))
    view = ResultView(json.dumps(dict(schema='equity.remote',version='1.1',kind='result',request_id='synthetic',payload=producer),
        sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii'))
    converted = convert_result(view, expected, config=configuration)
    assert converted.ok and converted.view is not None
    result = converted.view
    # Explicit synthetic declarations only: these input hashes are not verified.
    receipt = make_receipt(result, configuration=fingerprint_config(configuration),
        inputs=tuple(InputFingerprint(b.role,b.metadata.source.input_id,ContentFingerprint('canonical_batch','0'*64,0)) for b in result.metadata.inputs),
        output=fingerprint_result(result),formulas=tuple(FormulaBinding(v.feature_id,v.algorithm_version) for v in result.values),
        environment=EnvironmentDeclaration('CPython','3.12','synthetic-owner',(PackageVersion('equity-feature-contracts','0.0.4a4'),)),
        execution=ExecutionClaim(),evidence=EvidenceDisclosure('not_supplied',0))
    entity = r.EntityKey('A','S')
    explanation = explain_result(result,receipt,configuration,entity=entity,feature_id='session.trade.volume')
    diagnostic = DiagnosticReport(DecisionContext('demo',entity,result.metadata.availability),
        DiagnosticPolicy('synthetic-advisory','advisory','deny'),
        (CheckFinding('observation','synthetic-output','freshness',receipt.output,'unknown','warning',
                      ('missing_observation',),False,1,None),))
    profile = MCPProfile('synthetic-owner',(),(CommandRegistration('cmd',expected),),60)
    catalog = EvidenceCatalog(profile,(EvidenceRegistration('cmd','synthetic-job','synthetic-result',view,configuration,
        receipt=receipt,explanation=explanation,diagnostics=diagnostic,
        associations=(DiagnosticAssociation('observation','output'),),report_sharing_authorized=True),))
    manifest = catalog.payload_json()
    assert len(manifest['entries']) == 1
    assert all(not value['input_content_verified'] for value in manifest['entries'][0]['artifacts'].values() if value is not None)
    print(json.dumps(dict(source_demonstration_only=True,entries=1,input_content_verified=False,
                          manifest_bytes=len(catalog.snapshot),manifest_sha256=catalog.snapshot_sha256),sort_keys=True))


if __name__ == '__main__':
    main()
