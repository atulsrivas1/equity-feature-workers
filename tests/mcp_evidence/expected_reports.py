"""Independent qualification expectations through accepted public pure APIs.

No EvidenceCatalog or MCP projection helper is used to produce expected bytes.
This module does not launch a service/process or fetch input data.
"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')


def expected_reports(root: Path):
    from equity_feature_client import ResultView,ProducerExpectation,ScopeKey,FeatureRef
    from equity_feature_client.native import convert_result
    from equity_feature_io_sdk import encode_result
    from equity_feature_contracts import ConfigSpec,EntityKey
    from equity_feature_evidence import (ContentFingerprint,EnvironmentDeclaration,EvidenceDisclosure,
        ExecutionClaim,FormulaBinding,InputFingerprint,PackageVersion,fingerprint_config,fingerprint_result,
        make_receipt,encode_receipt)
    from equity_feature_explanations import explain_result,encode_explanation
    from equity_feature_diagnostics import DiagnosticReport,DiagnosticPolicy,DecisionContext,CheckFinding
    fixture=json.loads((root/'docs/EQ081_CLIENT_FIXTURES.json').read_bytes())['native_cases'][0]
    producer=fixture['complete_producer_payload']
    expected=ProducerExpectation(producer['context'],ScopeKey('A','S',100,200),
        tuple(FeatureRef(**v) for v in producer['executed_features']),producer['backend_id'],producer['backend_version'])
    config_value=next(v for v in json.loads((root/'tests/service/fixtures/jobs_native.json').read_bytes())['fixtures'] if v['family']=='trades')['native_spec']['config']
    cfg=ConfigSpec.from_json(json.dumps(config_value))
    converted=convert_result(ResultView(canonical(dict(schema='equity.remote',version='1.1',kind='result',request_id='independent',payload=producer))),expected,config=cfg)
    assert converted.ok
    result=converted.view
    assert hashlib.sha256(encode_result(result)).hexdigest()==fixture['native_sha256']
    receipt=make_receipt(result,configuration=fingerprint_config(cfg),
        inputs=tuple(InputFingerprint(b.role,b.metadata.source.input_id,ContentFingerprint('canonical_batch','0'*64,0)) for b in result.metadata.inputs),
        output=fingerprint_result(result),formulas=tuple(FormulaBinding(v.feature_id,v.algorithm_version) for v in result.values),
        environment=EnvironmentDeclaration('CPython','3.12','synthetic-owner',(PackageVersion('equity-feature-contracts','0.0.4a4'),)),
        execution=ExecutionClaim(),evidence=EvidenceDisclosure('not_supplied',0))
    explanation=explain_result(result,receipt,cfg,entity=EntityKey('A','S'),feature_id='session.trade.volume')
    report=DiagnosticReport(DecisionContext('demo',EntityKey('A','S'),result.metadata.availability),
        DiagnosticPolicy('synthetic-advisory','advisory','deny'),
        (CheckFinding('observation','synthetic-output','freshness',receipt.output,'unknown','warning',('missing_observation',),False,1,None),))
    # Full public dataclass declaration/derived fields, with only the documented
    # projection's decimal string conversions. Do not call the product encoder.
    projection=asdict(report)
    for key in ('market_cutoff_ns','knowledge_cutoff_ns','evaluation_ns'):
        projection['context']['availability'][key]=str(projection['context']['availability'][key])
    projection['unknown_prerequisite_count']=str(projection['unknown_prerequisite_count'])
    for finding in projection['findings']:
        finding['unknown_prerequisites']=str(finding['unknown_prerequisites'])
        if finding['age_ns'] is not None:finding['age_ns']=str(finding['age_ns'])
        if finding['content'] is not None:finding['content']['byte_length']=str(finding['content']['byte_length'])
    assert projection['admission']=='not_applied'
    diagnostic=canonical(dict(representation='equity.mcp.diagnostics.v1',schema_version='1',report=projection,
        associations=[dict(check_id='observation',role='output',input_role=None,input_id=None)],input_content_verified=False))
    artifacts=dict(producer=canonical(producer),receipt=encode_receipt(receipt),explanation=encode_explanation(explanation),diagnostics=diagnostic)
    # Independent literal quality facts for the frozen five-column producer.
    assert len(result.quality)==5 and all(row.status.value=='available' for row in result.quality)
    assert all(row.expected==row.observed==3 and row.reasons==() for row in result.quality)
    quality=dict(row_count=5,expected_unknown_count=0,
        statuses=dict(available=5,insufficient_history=0,missing_input=0,incomplete_coverage=0,not_applicable=0),
        reasons={name:0 for name in ('absent_input','null_field','observed_empty','no_eligible_observations',
            'missing_action_evidence','insufficient_history','governed_gap','unknown_availability','future_knowledge',
            'partial_universe','zero_denominator','empty_universe','ineligible','future_market')})
    return artifacts,quality
