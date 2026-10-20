"""Synthetic staged envelope grammar only; no complete root or actual clocks."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_envelope import INSIDE,FINALIZATION,verify_measurement,verify_candidate
from eq142_graph import wire,digest
from test_eq142_native_closure import receipt


def ref(body,codec,name):return {'codec':codec,'sha256':digest(body),'bytes':len(body),'retained_name':name}


def fixture(*,e2e=False):
    native=receipt();source='a'*64
    root={'sample_id':'synthetic-envelope','selector':{'profile':'small','series':'eq142_owned_runner_e2e' if e2e else 'eq142_admitted_control',
        'mode':'sequential','degree':1},'control':'instrumented_B','outer_source_before_sha256':source}
    root_ref=ref(wire(root),'eq142_complete_root','complete-root.json')
    capacity_ref=ref(b'{}\n','capacity_receipt','capacity-after.json')
    cpuclock={'name':'process_time','implementation':'GetProcessTimes()','monotonic':True,'adjustable':False,
        'reported_resolution_seconds':'0.0000001','cross_process_raw_ticks_qualified':False}
    phase={'wall_ns':'100','user_ticks_100ns':'4','kernel_ticks_100ns':'8'}
    record={'schema':'eq142-measurement-envelope:1','sample_id':root['sample_id'],'root_reference':root_ref,
        'selector':deepcopy(root['selector']),'control':root['control'],
        'observer':{k:native['native_closure']['observer'][k] for k in ('pid','creation_filetime_100ns_since_1601_utc','clock')},
        'checkpoints':dict(zip(INSIDE,map(str,(0,1,2,3,4,5,95,96,97,98,99,100,101,102)))),
        'outer_wall_ns':'102','outer_coordinator_cpu_ns':'10',
        'selected_wall_ns':'102' if e2e else '100','selected_user_ticks_100ns':'10' if e2e else '4',
        'selected_kernel_ticks_100ns':'20' if e2e else '8','capacity_after_reference':capacity_ref,
        'outer_source_after_sha256':source,'finalization_policy':FINALIZATION,'full_sample_accepted':False,
        'cpu_observation':{'clock':cpuclock,'start_ns':'200','end_ns':'210'}}
    context={'root':root,'root_reference':root_ref,'native':native,'phase':phase,
        'capacity_after_reference':capacity_ref,'expected_source_sha256':source,'expected_cpu_clock':deepcopy(cpuclock)}
    return record,context


class PortableEnvelope(unittest.TestCase):
    def test_local_wall_and_separate_cpu_units_select_both_classes(self):
        for e2e in (False,True):
            record,context=fixture(e2e=e2e)
            checked=verify_measurement(record,**context)
            self.assertFalse(checked['full_sample_accepted']);self.assertNotIn('complete',checked)
        record,context=fixture();record['cpu_observation'].update(start_ns='0',end_ns='2')
        record['outer_coordinator_cpu_ns']='2';verify_measurement(record,**context)

    def test_order_scope_self_finalization_and_native_containment_refuse(self):
        changes=[(('extra',),0),(('full_sample_accepted',),0),(('outer_wall_ns',),'103'),
            (('checkpoints','native_owned_call_entered_before_Kernel_setup'),'11'),
            (('checkpoints','native_owned_call_returned_after_descendants_exit_and_owned_cleanup'),'89'),
            (('checkpoints','outer_cpu_end_captured'),'103'),(('observer','pid'),True),
            (('observer','creation_filetime_100ns_since_1601_utc'),'910'),
            (('finalization_policy',),'all_writes_included'),(('outer_source_after_sha256',),'b'*64)]
        for path,value in changes:
            record,context=fixture();target=record
            for field in path[:-1]:target=target[field]
            target[path[-1]]=value
            with self.subTest(path=path),self.assertRaises(ValueError):verify_measurement(record,**context)

    def test_cpu_counter_domain_metadata_and_selected_values_are_independent(self):
        changes=[(('cpu_observation','clock','name'),'perf_counter'),(('cpu_observation','start_ns'),'211'),
            (('cpu_observation','end_ns'),'0209'),(('outer_coordinator_cpu_ns',),'9'),
            (('selected_wall_ns',),'102'),(('selected_user_ticks_100ns',),'10'),
            (('selected_kernel_ticks_100ns',),'20'),(('cpu_observation','clock','monotonic'),1),
            (('cpu_observation','clock','reported_resolution_seconds'),'0'),
            (('cpu_observation','clock','cross_process_raw_ticks_qualified'),True)]
        for path,value in changes:
            record,context=fixture();target=record
            for field in path[:-1]:target=target[field]
            target[path[-1]]=value
            with self.subTest(path=path),self.assertRaises(ValueError):verify_measurement(record,**context)
        record,context=fixture();context['native']['external_observer_cpu_ns']=11
        with self.assertRaises(ValueError):verify_measurement(record,**context)

    def test_candidate_final_source_and_actual_reference_binding_refuse_changed_proofs(self):
        measurement,context=fixture();root_ref=context['root_reference']
        measurement_ref=ref(wire(measurement),'eq142_measurement_envelope','measurement.json')
        candidate={'schema':'eq142-complete-candidate:1','sample_id':context['root']['sample_id'],
            'root_reference':root_ref,'measurement_reference':measurement_ref,
            'final_controller_post_envelope_source_state_sha256':'a'*64,
            'disposition':'development_evidence_closed_not_official_study','full_sample_accepted':False}
        options={'root':context['root'],'root_reference':root_ref,'measurement_reference':measurement_ref,'final_source_sha256':'a'*64}
        self.assertFalse(verify_candidate(candidate,**options)['full_sample_accepted'])
        for field,value in (('extra',True),('sample_id','other'),('full_sample_accepted',0),
                ('final_controller_post_envelope_source_state_sha256','b'*64),('disposition','released')):
            changed=deepcopy(candidate);changed[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):verify_candidate(changed,**options)
        changed=deepcopy(candidate);changed['measurement_reference']['sha256']='b'*64
        with self.assertRaises(ValueError):verify_candidate(changed,**options)


if __name__=='__main__':unittest.main(verbosity=2)
