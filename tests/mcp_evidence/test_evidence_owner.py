"""Evidence-extra literal owner/projection controls; no native service execution."""
from pathlib import Path
import copy
from dataclasses import replace
import hashlib
from importlib.resources import files
import json
from types import SimpleNamespace
import unittest
from jsonschema import Draft202012Validator

import equity_feature_contracts as c
from equity_feature_contracts import results as r
import equity_feature_evidence as receipts
import equity_feature_explanations as explanations
import equity_feature_diagnostics as diagnostics
from equity_feature_client import ResultView, ProducerExpectation, ScopeKey, FeatureRef
from equity_feature_client.native import convert_result
from equity_feature_mcp import MCPProfile, CommandRegistration
from equity_feature_mcp.evidence import DiagnosticAssociation, EvidenceRegistration, EvidenceCatalog
from equity_feature_mcp import evidence as module

ROOT = Path(__file__).resolve().parents[2]
ORACLES = json.loads((ROOT/'docs/stories/EQ-108_ORACLES.json').read_bytes())
ADMISSION = json.loads((ROOT/'docs/stories/EQ-108_ADMISSION.json').read_bytes())


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii')


def config(identity='synthetic-bars', timezone='caller-supplied'):
    return c.ConfigSpec(identity,'v1',(c.Parameter('eligibility_policy','example-v1'),),
        c.SessionSpec('demo','S',100,200,timezone),c.WindowSpec(1,'S',('P','S')),
        c.AvailabilitySpec(200,210,210),price_unit=c.PriceUnit(0,'USD'))


def real_public_conversion_fixture():
    raw=json.loads((ROOT/'docs/EQ081_CLIENT_FIXTURES.json').read_bytes())['native_cases'][0]['complete_producer_payload']
    expected=ProducerExpectation(raw['context'],ScopeKey('A','S',100,200),
        tuple(FeatureRef(**v) for v in raw['executed_features']),raw['backend_id'],raw['backend_version'])
    view=ResultView(canonical(dict(schema='equity.remote',version='1.1',kind='result',request_id='owned',payload=raw)))
    cfg=config()
    result=convert_result(view,expected,config=cfg)
    assert result.ok
    native=result.view
    receipt=receipts.make_receipt(native,configuration=receipts.fingerprint_config(cfg),
        inputs=tuple(receipts.InputFingerprint(b.role,b.metadata.source.input_id,
            receipts.ContentFingerprint('canonical_batch','0'*64,0)) for b in native.metadata.inputs),
        output=receipts.fingerprint_result(native),formulas=tuple(receipts.FormulaBinding(v.feature_id,v.algorithm_version) for v in native.values),
        environment=receipts.EnvironmentDeclaration('CPython','3.12','synthetic-owner',
            (receipts.PackageVersion('equity-feature-contracts','0.0.4a4'),)),
        execution=receipts.ExecutionClaim(),evidence=receipts.EvidenceDisclosure('not_supplied',0))
    profile=MCPProfile('owner',(),(CommandRegistration('cmd',expected),),60)
    registration=EvidenceRegistration('cmd','job','result',view,cfg,receipt=receipt,report_sharing_authorized=True)
    return profile,registration,native


def independent_count_fixture():
    receipt=receipts.decode_receipt(ORACLES['accepted_receipt']['canonical_ascii'].encode('ascii'))
    entity=r.EntityKey('A','S')
    result=r.FeatureResult((r.FeatureColumn('session.trade.count','v1',r.ValueType.INT64,'trades',(entity,),(3,)),),
        (r.QualityRow(entity,'session.trade.count',r.Status.AVAILABLE,3,3),),receipt.metadata)
    return config('explain-fixture','caller'),receipt,result


def literal_count_owner_fixture():
    """Explicit synthetic wire fixture for the frozen receipt/explanation seed.

    This prepares test transport data, not an executed native result or product
    serializer. Public conversion must equal the independent count=3 result.
    """
    cfg,receipt,result=independent_count_fixture()
    raw=json.loads((ROOT/'docs/EQ081_CLIENT_FIXTURES.json').read_bytes())['native_cases'][0]['complete_producer_payload']
    feature=dict(feature_id='session.trade.count',algorithm_version='v1')
    raw['context']['config'].update(config_id=cfg.identity,digest=cfg.digest)
    raw['context']['dataset']['input_id']='trades1'
    raw['context']['features']=[feature]
    raw['executed_features']=[feature]
    raw['metadata']['fields']['config_digest']['value']=cfg.digest
    # The accepted receipt oracle was frozen at its own producer version; the transport data declares that same version.
    raw['backend_version']=result.metadata.backend_version
    raw['metadata']['fields']['backend_version']['value']=result.metadata.backend_version
    raw['metadata']['fields']['inputs']['items'][0]['fields']['metadata']['fields']['source']['fields']['input_id']['value']='trades1'
    raw['columns']=[dict(feature_id='session.trade.count',algorithm_version='v1',schema_version='1',dtype='int64',unit='trades',
        entities=[dict(instrument_id='A',session_id='S')],values=[dict(type='int64',value='3')])]
    raw['quality']=[dict(entity=dict(instrument_id='A',session_id='S'),feature_id='session.trade.count',
        status='available',expected='3',observed='3',reasons=[])]
    expected=ProducerExpectation(raw['context'],ScopeKey('A','S',100,200),(FeatureRef(**feature),),raw['backend_id'],raw['backend_version'])
    raw['command_digest']=expected.command_digest
    view=ResultView(canonical(dict(schema='equity.remote',version='1.1',kind='result',request_id='literal',payload=raw)))
    converted=convert_result(view,expected,config=cfg)
    assert converted.ok and converted.view==result
    explanation=explanations.explain_result(result,receipt,cfg,entity=r.EntityKey('A','S'),feature_id='session.trade.count')
    assert receipts.encode_receipt(receipt)==ORACLES['accepted_receipt']['canonical_ascii'].encode()
    assert explanations.encode_explanation(explanation)==ORACLES['explanation_without_batches']['canonical_ascii'].encode()
    profile=MCPProfile('owner',(),(CommandRegistration('cmd',expected),),60)
    return profile,EvidenceRegistration('cmd','job','result',view,cfg,receipt=receipt,explanation=explanation,report_sharing_authorized=True)


def rebound_configuration(registration, cfg):
    """Rebind every digest so malformed nested input is the only adversary."""
    raw=registration.view.payload_json()
    raw['context']['config']['digest']=cfg.digest
    raw['metadata']['fields']['config_digest']['value']=cfg.digest
    expected=ProducerExpectation(raw['context'],ScopeKey('A','S',100,200),
        tuple(FeatureRef(**v) for v in raw['executed_features']),raw['backend_id'],raw['backend_version'])
    raw['command_digest']=expected.command_digest
    view=ResultView(canonical(dict(schema='equity.remote',version='1.1',kind='result',request_id='owned',payload=raw)))
    profile=MCPProfile('owner',(),(CommandRegistration('cmd',expected),),60)
    return profile,replace(registration,view=view,configuration=cfg,receipt=None)


def typed_diagnostic(projection):
    """Independent test fixture constructor, NOT an exported diagnostic decoder."""
    report=projection['report'];ctx=report['context'];av=ctx['availability'];policy=report['policy']
    context=diagnostics.DecisionContext(ctx['namespace'],r.EntityKey(**ctx['entity']),
        c.AvailabilitySpec(int(av['market_cutoff_ns']),int(av['knowledge_cutoff_ns']),int(av['evaluation_ns']),av['mode'],av['reconstruction_reason']))
    findings=[]
    for v in report['findings']:
        fp=v['content']
        content=None if fp is None else receipts.ContentFingerprint(fp['domain'],fp['digest'],int(fp['byte_length']),fp['schema_version'],fp['canonicalization'],fp['algorithm'])
        findings.append(diagnostics.CheckFinding(v['check_id'],v['subject_id'],v['kind'],content,v['status'],v['severity'],
            tuple(v['reasons']),v['assessed'],int(v['unknown_prerequisites']),None if v['age_ns'] is None else int(v['age_ns'])))
    out=diagnostics.DiagnosticReport(context,diagnostics.DiagnosticPolicy(**policy),tuple(findings),report['schema_version'])
    associations=tuple(DiagnosticAssociation(**v) for v in projection['associations'])
    return out,associations


class Owner(unittest.TestCase):
    def test_joint_configuration_context_bindings_and_intraday_scope(self):
        _,registration,_=real_public_conversion_fixture()
        cfg=registration.configuration
        changed=(replace(cfg,identity='other'),replace(cfg,algorithm_version='v2'),
            replace(cfg,session=replace(cfg.session,namespace='other')),
            replace(cfg,session=replace(cfg.session,open_ns=90)),
            replace(cfg,availability=replace(cfg.availability,evaluation_ns=211)),
            replace(cfg,availability=replace(cfg.availability,knowledge_cutoff_ns=209)))
        for modified in changed:
            profile,entry=rebound_configuration(registration,modified)
            self.assertTrue(convert_result(entry.view,profile.commands[0].expectation,config=modified).ok)
            with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):EvidenceCatalog(profile,(entry,))
        # SessionCommand request end is market cutoff C, not scheduled/actual close.
        intraday=replace(cfg,session=replace(cfg.session,close_ns=250))
        profile,entry=rebound_configuration(registration,intraday)
        self.assertLess(profile.commands[0].expectation.scope.end_ns,intraday.session.close_ns)
        self.assertEqual(len(EvidenceCatalog(profile,(entry,)).payload_json()['entries']),1)

    def test_all_frozen_mutation_vectors_with_valid_positive_controls(self):
        cfg,receipt,result=independent_count_fixture();deps=module._dependencies()
        vectors=ORACLES['mutation_vectors']
        self.assertEqual([v['id'] for v in vectors],['M%02d'%i for i in range(1,11)])
        schema=json.loads(files('equity_feature_mcp').joinpath('schemas/diagnostics.json').read_bytes())
        for vector in vectors:
            with self.subTest(vector=vector['id']):
                key=vector['seed'];pointer=vector['pointer'].split('/')[1:];value=vector['value']
                if vector['id']=='M03':
                    raw=copy.deepcopy(ORACLES[key]['value'])
                    validator=Draft202012Validator(schema);validator.validate(raw)
                    raw['report']['unknown_prerequisite_count']=value
                    self.assertTrue(list(validator.iter_errors(raw)))
                elif key in ('one_unknown_diagnostic','large_associated_diagnostic'):
                    typed,associations=typed_diagnostic(ORACLES[key]['value'])
                    baseline=module._diagnostic(typed,associations,result,receipt,receipts.fingerprint_config(cfg),receipts.fingerprint_result(result),deps)
                    self.assertEqual(baseline,ORACLES[key]['canonical_ascii'].encode())
                    obj={'report':typed,'associations':associations}[pointer[0]]
                    for name in pointer[1:-1]:obj=obj[int(name)] if name.isdigit() else getattr(obj,name)
                    name=pointer[-1];old=getattr(obj,name)
                    object.__setattr__(obj,name,int(value) if type(old) is int else value)
                    # This private projection seam exposes public constructor errors;
                    # EvidenceCatalog maps them to the fixed owner error contract.
                    with self.assertRaises((c.ContractError,ValueError)):
                        module._diagnostic(typed,associations,result,receipt,receipts.fingerprint_config(cfg),receipts.fingerprint_result(result),deps)
                else:
                    profile,registration=literal_count_owner_fixture()
                    admitted=EvidenceCatalog(profile,(registration,))
                    artifacts=dict(admitted._entries[0].artifacts)
                    self.assertEqual(artifacts['receipt'],ORACLES['accepted_receipt']['canonical_ascii'].encode())
                    self.assertEqual(artifacts['explanation'],ORACLES['explanation_without_batches']['canonical_ascii'].encode())
                    if key=='explanation_without_batches':
                        explanation=registration.explanation
                        valid=registration
                        self.assertEqual(pointer,['inputs','0','content_verified'])
                        object.__setattr__(explanation.inputs[0],'content_verified',value)
                        bad=valid
                    else:
                        self.assertEqual(pointer,['configuration','digest'])
                        EvidenceCatalog(profile,(registration,))
                        bad=replace(registration,receipt=replace(registration.receipt,
                            configuration=replace(registration.receipt.configuration,digest=value)))
                    with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):EvidenceCatalog(profile,(bad,))

    def test_consistently_rebound_forged_nested_configuration_refuses(self):
        _,registration,_=real_public_conversion_fixture()
        for component,field,value in (('session','close_ns',50),('session','early_close',1),
                ('window','count',0),('availability','evaluation_ns',100),
                ('adjustment','basis','unsupported'),('price_unit','scale',19)):
            cfg=copy.deepcopy(registration.configuration)
            object.__setattr__(getattr(cfg,component),field,value)
            profile,modified=rebound_configuration(registration,cfg)
            # The reused converter checks the rebound digest, not component semantics.
            self.assertTrue(convert_result(modified.view,profile.commands[0].expectation,config=cfg).ok)
            with self.subTest(component=component,field=field):
                with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
                    EvidenceCatalog(profile,(modified,))
            self.assertEqual(getattr(getattr(cfg,component),field),value)

    def test_forged_view_envelope_refuses_despite_valid_producer(self):
        profile,registration,_=real_public_conversion_fixture()
        raw=json.loads(registration.view.encoded)
        raw.update(schema='wrong',version='9.0',kind='discovery')
        forged=object.__new__(ResultView)
        object.__setattr__(forged,'encoded',canonical(raw))
        object.__setattr__(forged,'filename',None)
        self.assertTrue(convert_result(forged,profile.commands[0].expectation,config=registration.configuration).ok)
        with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
            EvidenceCatalog(profile,(replace(registration,view=forged),))

    def test_configuration_preflight_limits_and_owned_copy(self):
        _,registration,_=real_public_conversion_fixture();deps=module._dependencies()
        cfg=registration.configuration
        cloned=module._configuration(cfg,deps)
        self.assertEqual(cloned,cfg)
        for component in ('session','window','availability','adjustment','price_unit'):
            self.assertIsNot(getattr(cloned,component),getattr(cfg,component))
        for field,value in (('identity','x'*65537),('parameters',(c.Parameter('x',1),)*257)):
            bad=copy.deepcopy(cfg);object.__setattr__(bad,field,value)
            with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._configuration(bad,deps)
        bad=copy.deepcopy(cfg);object.__setattr__(bad.session,'intervals',(c.IntervalSpec('a',100,101),)*129)
        with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._configuration(bad,deps)
        bad=copy.deepcopy(cfg);object.__setattr__(bad.window,'governed_sessions',('S',)*257)
        with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._configuration(bad,deps)
        bad=replace(cfg,parameters=(c.Parameter('large',1<<4096),))
        with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._configuration(bad,deps)
        valid=replace(cfg,parameters=(c.Parameter('large',(1<<4096)-1),))
        self.assertEqual(module._configuration(valid,deps),valid)

    def test_original_receipt_and_no_batch_explanation_are_literal_goldens(self):
        cfg,receipt,result=independent_count_fixture()
        self.assertEqual(receipts.encode_receipt(receipt),ORACLES['accepted_receipt']['canonical_ascii'].encode())
        self.assertEqual(receipts.fingerprint_config(cfg),receipt.configuration)
        self.assertEqual(receipts.fingerprint_result(result),receipt.output)
        value=explanations.explain_result(result,receipt,cfg,entity=r.EntityKey('A','S'),feature_id='session.trade.count')
        raw=explanations.encode_explanation(value)
        self.assertEqual(raw,ORACLES['explanation_without_batches']['canonical_ascii'].encode())
        self.assertEqual(hashlib.sha256(raw).hexdigest(),ORACLES['explanation_without_batches']['sha256'])
        self.assertFalse(value.inputs[0].content_verified)

    def test_diagnostic_null_large_numbers_associations_and_six_counts(self):
        cfg,receipt,result=independent_count_fixture();deps=module._dependencies()
        for key in ('one_unknown_diagnostic','large_associated_diagnostic'):
            with self.subTest(key=key):
                expected=ORACLES[key];typed,associations=typed_diagnostic(expected['value'])
                raw=module._diagnostic(typed,associations,result,receipt,receipts.fingerprint_config(cfg),receipts.fingerprint_result(result),deps)
                self.assertEqual(raw,expected['canonical_ascii'].encode())
                self.assertEqual(len(raw),expected['byte_length'])
                self.assertEqual(hashlib.sha256(raw).hexdigest(),expected['sha256'])

    def test_quality_all_statuses_reasons_expected_null_and_overlapping_counts(self):
        fixture=ORACLES['quality']
        rows=tuple(r.QualityRow(r.EntityKey(**v['entity']),v['feature_id'],r.Status(v['status']),v['expected'],v['observed'],
            tuple(r.Reason(x) for x in v['reasons'])) for v in fixture['input_rows'])
        self.assertEqual(json.loads(module._quality(SimpleNamespace(quality=rows))),fixture['expected_summary'])
        self.assertGreater(sum(fixture['expected_summary']['reasons'].values()),len(rows))

    def test_catalog_public_conversion_bytes_defensive_copies_and_exact_charge(self):
        profile,registration,result=real_public_conversion_fixture();catalog=EvidenceCatalog(profile,(registration,))
        manifest=catalog.payload_json();entry=manifest['entries'][0]
        self.assertEqual(entry['artifacts']['producer']['sha256'],hashlib.sha256(canonical(registration.view.payload_json())).hexdigest())
        self.assertEqual(catalog.snapshot_sha256,hashlib.sha256(catalog.snapshot).hexdigest())
        before=catalog.snapshot
        manifest['entries'].clear()
        object.__setattr__(registration.receipt.environment,'platform','changed-after-preparation')
        self.assertEqual(catalog.snapshot,before)
        self.assertEqual(len(catalog.payload_json()['entries']),1)
        self.assertEqual(catalog.profile_snapshot,profile.snapshot)
        self.assertEqual(sum(entry['quality']['statuses'].values()),len(result.quality))

    def test_receipt_config_output_and_owner_permission_refusals(self):
        profile,registration,_=real_public_conversion_fixture()
        with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
            replace(registration,report_sharing_authorized=False)
        for value in (True,1,None,'true'):
            if value is True:continue
            with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
                replace(registration,report_sharing_authorized=value)
        for modified in (replace(registration,configuration=config('wrong')),replace(registration,receipt=replace(registration.receipt,output=replace(registration.receipt.output,digest='f'*64)))):
            with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
                EvidenceCatalog(profile,(modified,))

    def test_duplicate_and_catalog_count_limits(self):
        profile,registration,_=real_public_conversion_fixture()
        with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):EvidenceCatalog(profile,(registration,registration))
        with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):EvidenceCatalog(profile,(registration,)*9)
        empty=EvidenceCatalog(profile,())
        self.assertEqual(empty.payload_json()['entries'],[])

    def test_no_diagnostic_association_inference_and_no_input_verification(self):
        cfg,receipt,result=independent_count_fixture();deps=module._dependencies()
        typed,associations=typed_diagnostic(ORACLES['large_associated_diagnostic']['value'])
        for supplied in ((),associations+(associations[0],),tuple(replace(v,input_role='wrong') if v.role=='input' else v for v in associations)):
            with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
                module._diagnostic(typed,supplied,result,receipt,receipts.fingerprint_config(cfg),receipts.fingerprint_result(result),deps)
        raw=module._diagnostic(typed,associations,result,receipt,receipts.fingerprint_config(cfg),receipts.fingerprint_result(result),deps)
        self.assertFalse(json.loads(raw)['input_content_verified'])

    def test_forged_derived_counts_and_complete_context_refuse(self):
        cfg,receipt,result=independent_count_fixture();deps=module._dependencies()
        for field in ('requested_count','passed_count','failed_count','unknown_count','assessed_count','unknown_prerequisite_count'):
            typed,associations=typed_diagnostic(ORACLES['large_associated_diagnostic']['value'])
            object.__setattr__(typed,field,getattr(typed,field)+1)
            with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
                module._diagnostic(typed,associations,result,receipt,receipts.fingerprint_config(cfg),receipts.fingerprint_result(result),deps)
        typed,associations=typed_diagnostic(ORACLES['one_unknown_diagnostic']['value'])
        object.__setattr__(typed.context,'entity',r.EntityKey('B','S'))
        with self.assertRaisesRegex(ValueError,'^evidence_invalid$'):
            module._diagnostic(typed,associations,result,receipt,receipts.fingerprint_config(cfg),receipts.fingerprint_result(result),deps)

    def test_frozen_chunk_boundaries_in_isolated_prepared_byte_reader(self):
        fixture=ORACLES['chunks'];raw=fixture['canonical_ascii'].encode()
        # Isolated reader unit seam, not a claimed valid registered native report.
        entry=module._Entry('cmd','job','result','0'*64,b'{}',b'{}',(('producer',raw),),
            canonical(dict(artifacts={'producer':dict(sha256=fixture['sha256'],byte_length=len(raw))})))
        catalog=object.__new__(EvidenceCatalog);object.__setattr__(catalog,'_entries',(entry,))
        assembled=''
        for expected in fixture['expected']:
            payload,error=module._read(catalog,reference='opaque',profile_id='cmd',job_id='job',result_id='result',command_digest='0'*64,producer=b'{}',kind='producer',offset=expected['offset'])
            self.assertIsNone(error)
            for key in ('offset','next_offset','complete'):self.assertEqual(payload[key],expected[key])
            self.assertEqual(payload['chunk'],expected['text']);assembled+=payload['chunk']
        self.assertEqual(assembled,fixture['canonical_ascii'])
        self.assertEqual(hashlib.sha256(assembled.encode()).hexdigest(),fixture['sha256'])

    def test_frozen_manifest_arithmetic_and_byte_node_depth_primitives(self):
        fixture=ADMISSION['manifest'];self.assertEqual(canonical(fixture['value']),fixture['canonical_ascii'].encode())
        self.assertEqual(len(canonical(fixture['value'])),1198)
        self.assertEqual(ADMISSION['arithmetic_fixture']['aggregate_bytes'],1208)
        for key in ('aggregate_at_public_ceiling','aggregate_one_above'):
            value=ADMISSION['arithmetic_fixture'][key]
            self.assertEqual(value['manifest_bytes']+sum(value['artifact_lengths']),value['expected_total'])
        self.assertEqual(module._tree({'a':[None]}),4)
        raw=b'"'+b'x'*131070+b'"'
        self.assertEqual(len(module._bounded(raw,131072)),131072)
        with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._bounded(b'"'+b'x'*131071+b'"',131072)
        value=None
        for _ in range(20):value=[value]
        module._tree(value)
        with self.assertRaisesRegex(ValueError,'^evidence_bounds$'):module._tree([value])


if __name__=='__main__':unittest.main()
