"""Synthetic runner grammar/native joins, not source or complete root authority."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_graph import wire,digest
from eq142_identity import join_owned_processes
from eq142_runner_bundle import verify_runner_bundle
from test_eq142_native_closure import receipt,witness,account


def ref(body,codec,name):
    return {'codec':codec,'sha256':digest(body),'bytes':len(body),'retained_name':name}


def fixture(*,traced=False,publishing=False,profiled=False,separate_launcher=False):
    selector={'profile':'small','series':'eq142_owned_runner_e2e' if publishing else 'eq142_admitted_control',
        'mode':'sequential','degree':1}
    parent=witness(pid=2 if separate_launcher else 1,created='202' if separate_launcher else '101')
    native=receipt()
    if separate_launcher:
        for value in (native['final_accounting'],native['native_closure']['job_empty_accounting'],native['samples'][0]):
            value['total_processes']=3
    body=wire(parent);parent_ref=ref(body,'eq142_owned_process_witness','parent.json')
    witnesses=[parent];names=['parent.json'];references=[parent_ref]
    plan={'mode':'sequential','reservations':[{'file':'delegate-000.json'}]} if traced else None
    if traced:
        names.append('delegate-000.json');witnesses.append(deepcopy(parent))
        references.append(ref(body,'eq142_owned_process_witness','delegate-000.json'))
    processes={parent['process_startup_nonce']:{'pid':parent['pid'],'clock':parent['clock']}}
    # Deliberately synthetic expected maps; caller qualification is a separate gate.
    expected={'source':{'synthetic_source_context':'not_source_qualification'},
        'descriptive_job':{'synthetic_job_context':True},'original_admission':{'synthetic_initial':100},
        'delegate_admission':{'synthetic_reserve':200},'product_records':[],
        'product_admission':{'synthetic_product_context':'not_product_verification'}}
    report={'schema':'eq142-development-runner:2','stage':'qualification_only_not_study_acceptance',
        **selector,'control':'instrumented_B' if traced else 'uninstrumented_A','reference_sha256':'c'*64,
        **{k:v for k,v in expected.items() if k not in ('product_records','product_admission')},
        'product_outcomes':expected['product_records'],'actual_admission_verification':expected['product_admission'],
        'status':'development_product_and_local_trace_checks_passed','native_job_supplied':True,
        'native_control_scope':{'parent_ownership_metadata_in_selected_phase':True,
            'original_supervisor_without_trace_hooks':not traced,'common_metadata_may_dilute_ratios':True,
            'observer_cost_subtraction':False},
        'executor_observation_attempted':traced,'executor_shutdowns':[] if traced else None,
        'future_terminal_observations':[] if traced else None,'executor_terminal_observation_complete':True if traced else None,
        'full_sample_accepted':False,'parent_owned_process_witness':parent_ref,
        'actual_supervisor_outcome':ref(b'{}\n','eq142_supervisor_outcome','supervisor.json'),
        'product_proof':ref(b'[]\n','eq142_product_projection','products.json'),
        'publication_observation':ref(b'{}\n','eq142_publication_observation','publication.json') if traced else None,
        'retained_spans':{'fragments':[{'retained_name':name,'owned_process_witness':reference}
            for name,reference in zip(names,references)],'processes':processes} if traced else None,
        'parent_relations':[],
        'owned_process_witnesses':{'references':references,'processes':join_owned_processes(witnesses,processes,job_name=native['job_name']),
            'scope':'actual_validated_trace_processes_only' if traced else 'runner_process_only',
            'cross_process_raw_ticks_qualified':False,'full_sample_accepted':False},
        'runner_control_phase_start_ns':'1200','runner_control_phase_end_ns':'1300','runner_control_phase_wall_ns':'100',
        'selected_phase_wall_ns':None if publishing or profiled else '100',
        'selected_phase_scope':'qualification_profiled_no_timing_authority' if profiled else (
            'external_owned_runner_final_exit_required' if publishing else 'admitted_control'),
        'before_phase':account(total=1,active=1,user=1,kernel=2,faults=1),
        'after_phase':account(total=2,active=1,user=5,kernel=10,faults=2)}
    if traced:report['publication_admission_replay']={'synthetic_replay':'not_verified_here'}
    if profiled:report['qualification_code_counts']={'synthetic_profile':'not_verified_here'}
    returned={'schema':'eq142-owned-runner-return:1','sample_id':'synthetic-small',
        'purpose':'qualification_counts' if profiled else 'development_candidate',
        'runner_reference':ref(wire(report),'eq142_development_runner','runner.json'),
        'process_binding':{k:parent[k] for k in ('pid','process_startup_nonce')}}
    return {'report':report,'returned':returned,'context':{'sample_id':returned['sample_id'],'purpose':returned['purpose'],
        'selector':selector,'control':report['control'],'reference_sha256':'c'*64,**deepcopy(expected),
        'witness_bodies':dict.fromkeys(names,body),'native':native,'plan':plan}}


def check(data,*,rehash=True):
    body=wire(data['report']);returned=deepcopy(data['returned'])
    if rehash:returned['runner_reference']=ref(body,'eq142_development_runner','runner.json')
    return verify_runner_bundle(body,wire(returned),**data['context'])


class PortableRunnerBundle(unittest.TestCase):
    def test_closed_controls_phase_units_and_distinct_logical_parent_without_io(self):
        for traced,publishing,profiled in ((False,False,False),(False,True,False),
                (True,False,False),(True,True,False),(True,False,True)):
            for separate in (False,True):
                data=fixture(traced=traced,publishing=publishing,profiled=profiled,separate_launcher=separate)
                with patch.object(Path,'open',side_effect=AssertionError('pure join')):
                    result=check(data)
                self.assertEqual(result['phase'],{'wall_ns':'100','user_ticks_100ns':'4','kernel_ticks_100ns':'8'})
                self.assertEqual(result['native_validation']['parent_is_initial_runner'],not separate)
                self.assertFalse(result['full_sample_accepted']);self.assertNotIn('complete',result)

    def test_coherently_rehashed_unknown_status_source_products_and_bool_bindings_reject(self):
        mutations=[('extra',0),('schema','eq142-development-runner:1'),('status','failed_development_control'),
            ('degree',True),('native_job_supplied',1),('full_sample_accepted',0),('source',{}),
            ('descriptive_job',{}),('original_admission',{}),('delegate_admission',{}),
            ('product_outcomes',[{}]),('actual_admission_verification',{}),('publication_admission_replay',{})]
        for field,value in mutations:
            data=fixture();data['report'][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):check(data)
        data=fixture();data['report']['degree']=2
        with self.assertRaises(ValueError):check(data,rehash=False)

    def test_phase_counters_capture_and_selected_scope_are_not_accepted_from_cached_labels(self):
        mutations=[(('runner_control_phase_wall_ns',),'101'),(('runner_control_phase_start_ns',),'01200'),
            (('runner_control_phase_end_ns',),'1234'),(('selected_phase_wall_ns',),'99'),
            (('selected_phase_scope',),'external_owned_runner_final_exit_required'),
            (('after_phase','user'),0),(('after_phase','kernel'),21),
            (('after_phase','active_processes'),0),(('before_phase','total_processes'),True)]
        for path,value in mutations:
            data=fixture();target=data['report']
            for field in path[:-1]:target=target[field]
            target[path[-1]]=value
            with self.subTest(path=path),self.assertRaises(ValueError):check(data)

    def test_witness_reference_process_binding_and_native_lifetime_cannot_drift_together(self):
        for kind in ('missing','extra','mutable','wrapper_pid','wrapper_nonce','cached_scope','cached_process','native_exit'):
            data=fixture(traced=True)
            if kind=='missing':data['context']['witness_bodies'].pop('delegate-000.json')
            elif kind=='extra':data['context']['witness_bodies']['delegate-001.json']=b'{}\n'
            elif kind=='mutable':data['context']['witness_bodies']['parent.json']=bytearray(data['context']['witness_bodies']['parent.json'])
            elif kind=='wrapper_pid':data['returned']['process_binding']['pid']=2
            elif kind=='wrapper_nonce':data['returned']['process_binding']['process_startup_nonce']='d'*32
            elif kind=='cached_scope':data['report']['owned_process_witnesses']['scope']='runner_process_only'
            elif kind=='cached_process':data['report']['owned_process_witnesses']['processes'][0]['pid']=2
            else:data['context']['native']['exit_code']=1
            with self.subTest(kind=kind),self.assertRaises(ValueError):check(data)


if __name__=='__main__':unittest.main()
