"""Development sample assembly; explicitly not frozen benchmark acceptance."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from eq142_fixture import HAVE_INTRADAY,require_cpu_slots
if HAVE_INTRADAY:
    from benchmark_execution_baseline import prepare_reference,run_control,source_state,verify_result,SERIES


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class DevelopmentSample(unittest.TestCase):
    def test_traced_execution_source_counts_and_parent_causal_closure(self):
        from eq142_execution import verify_execution
        from eq142_workloads import items_for,independent_expectations
        from eq142_admission import budget_for
        from eq142_delegate import describe_plan
        from eq142_fixture import load
        from eq142_proofs import read_proof
        profiles=('small','family_complete','family_missing_hl','family_old_close_gap')
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);slots=require_cpu_slots(self,5)
            ref=prepare_reference(root/'reference',cpu_slots=slots,profiles=profiles)
            reference_path=root/'reference/reference.json';reference=json.loads(reference_path.read_bytes())
            cases=[(profile,'sequential',1) for profile in profiles]
            cases.extend(('family_missing_hl',mode,2) for mode in ('thread','process'))
            for profile,mode,degree in cases:
                for series in SERIES:
                    with self.subTest(profile=profile,mode=mode,series=series):
                        local=root/('-'.join((profile,mode,series)))
                        returned=run_control(reference_path,ref['sha256'],directory=local,profile=profile,
                            mode=mode,degree=degree,control='instrumented_B',series=series)
                        report=json.loads(read_proof(local,returned,scope='runner',name='runner.json'))
                        items=items_for(profile);budget=budget_for(degree,slots)
                        plan,_=describe_plan(items,mode=mode,budget=budget,expected_inventory=reference['source_inventory'],
                            output_dir=str((local/'trace').resolve()),operations=load('RECORD_CONTRACTS.json')['trace']['operations'])
                        bodies={}
                        for extended in report['retained_spans']['fragments']:
                            proof={key:extended[key] for key in ('codec','sha256','bytes','retained_name')}
                            bodies[proof['retained_name']]=read_proof(local/'trace',proof,scope='trace',name=proof['retained_name'])
                        checked=verify_execution(report,plan,bodies,
                            task_features={i.task.task_sha256:tuple(h.feature_id for h in i.task.features) for i in items},
                            mathematics=independent_expectations(profile,items),
                            publishing=series=='eq142_owned_runner_e2e',maximum_in_flight=budget.max_in_flight)
                        self.assertFalse(checked['full_sample_accepted'])
                        self.assertEqual(checked['actual_wait_groups'],0 if mode=='sequential' else
                            sum(c['attempts'] for c in report['retained_spans']['counters'] if c['operation']=='parent.wait'))

    def test_full_semantic_product_joins_all_profiles_controls_and_original_modes(self):
        from eq142_products import verify_products
        from eq142_workloads import items_for,independent_expectations,PROFILES
        from eq142_admission import original_reservation,budget_for
        from eq142_proofs import read_proof
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);slots=require_cpu_slots(self,5)
            ref=prepare_reference(root/'reference',cpu_slots=slots)
            reference_path=root/'reference/reference.json';reference=json.loads(reference_path.read_bytes())
            cases=[(profile,'sequential',1) for profile in PROFILES]
            cases.extend(('family_missing_hl',mode,2) for mode in ('thread','process'))
            for profile,mode,degree in cases:
                for series in SERIES:
                    for control in ('uninstrumented_A','instrumented_B'):
                        with self.subTest(profile=profile,mode=mode,series=series,control=control):
                            local=root/('-'.join((profile,mode,series,control)))
                            runner_ref=run_control(reference_path,ref['sha256'],directory=local,profile=profile,
                                mode=mode,degree=degree,control=control,series=series)
                            report=json.loads(read_proof(local,runner_ref,scope='runner',name='runner.json'))
                            records=json.loads(read_proof(local,report['product_proof'],scope='runner',name='products.json'))
                            summary=json.loads(read_proof(local,report['actual_supervisor_outcome'],scope='runner',name='supervisor.json'))
                            refs=[]
                            for record in records:
                                refs.extend([record['task_wire'],*record['results'],*record['readback_results']])
                                refs.extend(r for r in (record['output'],record['receipt']) if r is not None)
                            bodies={r['sha256']:read_proof(root/'reference/objects',r,scope='products',name=r['retained_name']) for r in refs}
                            diagnostic=read_proof(local,records[0]['raw_diagnostics'],scope='runner',name='diagnostics.json')
                            items=items_for(profile);publishing=series=='eq142_owned_runner_e2e'
                            mathematics=independent_expectations(profile,items)
                            self.assertEqual(mathematics,reference['profiles'][profile]['mathematics'])
                            preflight=original_reservation(items,mode=mode,budget=budget_for(degree,slots),
                                pending_bytes=8388608 if publishing else 0)
                            checked=verify_products(records,bodies,diagnostic,items=items,mathematics=mathematics,
                                reference_products=reference['profiles'][profile]['products'][series],
                                summary=summary,preflight=preflight,mode=mode,publishing=publishing)
                            self.assertEqual(checked['admission'],report['actual_admission_verification'])
                            self.assertFalse(checked['full_sample_accepted'])

    def test_original_controls_actual_modes_and_incomplete_full_sample_label(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ref=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,4),profiles=('family_missing_hl',))
            reference=root/'reference/reference.json'
            for mode,degree in (('sequential',1),('thread',2),('process',2)):
                for control in ('uninstrumented_A','instrumented_B'):
                    with self.subTest(mode=mode,control=control):
                        require_cpu_slots(self,degree+3)
                        local=root/(mode+'-'+control)
                        verified=[]
                        def observed_verify(*args):
                            result=verify_result(*args);verified.append(time.perf_counter_ns());return result
                        with patch('benchmark_execution_baseline.verify_result',side_effect=observed_verify):
                            result=run_control(reference,ref['sha256'],directory=local,profile='family_missing_hl',
                                mode=mode,degree=degree,control=control,series=SERIES[1])
                        report=json.loads((local/result['retained_name']).read_bytes())
                        self.assertEqual(report['status'],'development_product_and_local_trace_checks_passed')
                        self.assertFalse(report['full_sample_accepted'])
                        self.assertIsNone(report['selected_phase_wall_ns'])
                        self.assertEqual(report['selected_phase_scope'],'external_owned_runner_final_exit_required')
                        self.assertEqual(len(verified),3)
                        self.assertTrue(all(int(report['runner_control_phase_start_ns'])<=t<=int(report['runner_control_phase_end_ns']) for t in verified))
                        self.assertEqual(len(report['product_outcomes']),3)
                        self.assertEqual(report['original_admission']['evidence'],'source_derived_preflight')
                        self.assertTrue((local/'products.json').is_file())
                        if control=='instrumented_B':
                            self.assertGreater(report['retained_spans']['total_span_count'],0)
                            self.assertGreater(report['delegate_admission']['logical_spec_resident_reservation_bytes'],0)
                            if mode!='sequential':self.assertTrue(report['parent_relations'])
                        else:self.assertIsNone(report['retained_spans'])

    def test_source_reference_rejection_is_retained_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ref=prepare_reference(root/'reference',cpu_slots=require_cpu_slots(self,4),profiles=('family_missing_hl',))
            reference=root/'reference/reference.json';local=root/'rejected'
            options=dict(directory=local,profile='family_missing_hl',mode='sequential',degree=1,
                control='uninstrumented_A',series=SERIES[0])
            with self.assertRaises(ValueError):run_control(reference,'0'*64,**options)
            body=(local/'runner.json').read_bytes();record=json.loads(body)
            self.assertEqual(record['status'],'failed_before_phase');self.assertFalse(record['full_sample_accepted'])
            with self.assertRaises(FileExistsError):run_control(reference,ref['sha256'],**options)
            self.assertEqual((local/'runner.json').read_bytes(),body)
            # Change the observed helper digest without modifying a shared file.
            actual=source_state()
            self.assertIn('tools/build_supplied_inputs.py',actual['files'])
            actual['files']['tools/build_supplied_inputs.py']='0'*64
            with patch('benchmark_execution_baseline.source_state',return_value=actual):
                with self.assertRaises(ValueError):run_control(reference,ref['sha256'],
                    **{**options,'directory':root/'changed-helper'})
            rejected=json.loads((root/'changed-helper/runner.json').read_bytes())
            self.assertEqual(rejected['status'],'failed_before_phase')


@unittest.skipUnless(HAVE_INTRADAY and os.name=='nt','Optional installed intraday plus Windows owned Job required')
class NativeOwnedDevelopmentSample(unittest.TestCase):
    def test_wrapper_runner_native_bundle_actual_original_controls(self):
        from eq142_native import run_owned
        from eq142_owned_runner import decode_return
        from eq142_runner_bundle import verify_runner_bundle
        from eq142_products import verify_products
        from eq142_proofs import read_proof
        from eq142_workloads import items_for,independent_expectations
        from eq142_admission import budget_for,original_reservation,delegate_reservation
        from eq142_delegate import describe_plan
        from eq142_graph import bind_source,describe_job
        from eq142_fixture import load
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);slots=require_cpu_slots(self,5)
            ref=prepare_reference(root/'reference',cpu_slots=slots,profiles=('family_missing_hl',))
            reference_path=root/'reference/reference.json';reference=json.loads(reference_path.read_bytes())
            bound=bind_source(reference['source_inventory']);items=items_for('family_missing_hl')
            mathematics=independent_expectations('family_missing_hl',items)
            for control,mode,degree in (('uninstrumented_A','sequential',1),('instrumented_B','process',2)):
                for series in SERIES:
                    with self.subTest(control=control,mode=mode,series=series):
                        sample_id='-'.join(('bundle',control.lower(),mode,series));local=root/sample_id;local.mkdir()
                        native=run_owned([sys.executable,str(ROOT/'tools/eq142_owned_runner.py'),
                            '--directory',str(local),'--sample-id',sample_id,'--purpose','development_candidate',
                            '--reference',str(reference_path),'--reference-sha256',ref['sha256'],
                            '--profile','family_missing_hl','--mode',mode,'--degree',str(degree),
                            '--control',control,'--series',series,'--job','{job_name}'],cwd=ROOT,timeout_s=120)
                        returned_body=(local/'runner-reference.json').read_bytes();returned=decode_return(returned_body)
                        runner=local/'runner';runner_body=read_proof(runner,returned['runner_reference'],scope='runner',name='runner.json')
                        report=json.loads(runner_body);budget=budget_for(degree,slots);publishing=series==SERIES[1]
                        preflight=original_reservation(items,mode=mode,budget=budget,pending_bytes=8388608 if publishing else 0)
                        records=json.loads(read_proof(runner,report['product_proof'],scope='runner',name='products.json'))
                        summary=json.loads(read_proof(runner,report['actual_supervisor_outcome'],scope='runner',name='supervisor.json'))
                        refs=[]
                        for record in records:
                            refs.extend([record['task_wire'],*record['results'],*record['readback_results']])
                            refs.extend(r for r in (record['output'],record['receipt']) if r is not None)
                        bodies={r['sha256']:read_proof(root/'reference/objects',r,scope='products',name=r['retained_name']) for r in refs}
                        diagnostics=read_proof(runner,records[0]['raw_diagnostics'],scope='runner',name='diagnostics.json')
                        products=verify_products(records,bodies,diagnostics,items=items,mathematics=mathematics,
                            reference_products=reference['profiles']['family_missing_hl']['products'][series],
                            summary=summary,preflight=preflight,mode=mode,publishing=publishing)
                        plan=None
                        if control=='instrumented_B':
                            plan,_=describe_plan(items,mode=mode,budget=budget,expected_inventory=reference['source_inventory'],
                                output_dir=str((runner/'trace').resolve()),operations=load('RECORD_CONTRACTS.json')['trace']['operations'],
                                owned_process_witness={'job_name':native['job_name'],'output_dir':str((runner/'owned-processes').resolve())})
                        expected_output=sum(r['output']['bytes'] for r in records if r['output'] is not None)
                        delegate=delegate_reservation(plan,items,budget,original=preflight,expected_output_bytes=expected_output,
                            publication_observation=control=='instrumented_B',owned_witness_count=1+(len(plan['reservations']) if plan else 0))
                        witnesses={r['retained_name']:read_proof(runner/'owned-processes',r,scope='owned_processes',name=r['retained_name'])
                            for r in report['owned_process_witnesses']['references']}
                        checked=verify_runner_bundle(runner_body,returned_body,sample_id=sample_id,purpose='development_candidate',
                            selector={'profile':'family_missing_hl','series':series,'mode':mode,'degree':degree},control=control,
                            reference_sha256=ref['sha256'],source=bound.source,
                            descriptive_job=describe_job(items,mode=mode,budget=budget,qualification=bound),
                            original_admission=preflight,delegate_admission=delegate,product_records=records,
                            product_admission=products['admission'],witness_bodies=witnesses,native=native,plan=plan)
                        self.assertFalse(checked['full_sample_accepted']);self.assertNotIn('complete',checked)

    def test_actual_owned_spawn_counter_and_bounded_native_receipt(self):
        from eq142_native import run_owned
        from eq142_evidence import persist_record
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);slots=require_cpu_slots(self,5)
            ref=prepare_reference(root/'reference',cpu_slots=slots,profiles=('family_missing_hl',))
            local=root/'owned-runner'
            native=run_owned([sys.executable,str(ROOT/'tools/benchmark_execution_baseline.py'),
                'qualification-runner','--directory',str(local),
                '--reference',str(root/'reference/reference.json'),'--reference-sha256',ref['sha256'],
                '--profile','family_missing_hl','--mode','process','--degree','2',
                '--control','instrumented_B','--series',SERIES[1],'--job','{job_name}'],cwd=ROOT,timeout_s=60)
            self.assertEqual(native['exit_code'],0);self.assertEqual(native['probe_errors'],[])
            self.assertEqual(native['final_accounting']['active_processes'],0)
            actual=json.loads((local/'runner.json').read_bytes())
            self.assertEqual(actual['status'],'development_product_and_local_trace_checks_passed')
            self.assertTrue(actual['native_job_supplied']);self.assertFalse(actual['full_sample_accepted'])
            for field in ('user','kernel'):
                self.assertGreaterEqual(actual['after_phase'][field],actual['before_phase'][field])
            traced={record['pid'] for record in actual['retained_spans']['processes'].values()}
            self.assertTrue(traced<=set(native['pids_observed']))
            self.assertGreaterEqual(len(traced),2)
            reference=persist_record(root/'native.json',native,max_bytes=16777216)
            self.assertEqual(reference['bytes'],len((root/'native.json').read_bytes()))


if __name__=='__main__':unittest.main()
