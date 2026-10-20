"""Independent V8 selector order, support boundaries and complete-source joins."""
from copy import deepcopy
from pathlib import Path
import sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_matrix import build_matrix,verify_matrix
from eq142_graph import digest,wire
from eq142_capacity import CapacityUnavailable
from test_eq142_capacity import sample

PROFILES=('small','balanced','skewed','family_complete','family_missing_hl','family_old_close_gap')


def inputs(slots=4):
    capacity=sample();mask=hex((1<<slots)-1)
    capacity.update(primary_thread_mask_before=mask,primary_thread_mask_after=mask,
                    primary_system_mask=mask,allowed_affinity_mask=mask,
                    allowed_cpu_ids=list(range(slots)),allowed_cpu_slots=slots)
    capacity['raw_machine_observation'].update(allowed_affinity_mask=mask,allowed_cpu_slots=slots,
        degrees=[d for d in (1,2,4,8) if d<=slots-1],omitted_degrees=[d for d in (1,2,4,8) if d>slots-1])
    reference={'schema':'eq142-development-reference:1','stage':'qualification_only_not_frozen_study',
        'state':{'files':{'tools/synthetic.py':'1'*64},'installed_runtime':{'synthetic':{'version':'1','files':{'a':{'sha256':'2'*64,'bytes':3}}}}},
        'source_inventory':{'synthetic_unqualified_inventory':True},'cpu_slots':slots,
        'profiles':{name:{} for name in PROFILES}}
    return reference,capacity


def freeze(capacity):
    return {'codec':'capacity_receipt','retained_name':'qualification-capacity.json',
            'sha256':digest(wire(capacity)),'bytes':len(wire(capacity))}


def context(capacity):
    return {'probe_sources':capacity['probe_sources'],'expected_device':capacity['storage_device'],
            'expected_retained_bytes':0}


class PortableMatrix(unittest.TestCase):
    def test_exact72_order_ids_and_hand_support_counts_without_io(self):
        expected=[]
        for name in PROFILES:
            modes=(('sequential',1),('thread',2),('process',2)) if name.startswith('family_') else (
                ('sequential',1),('thread',1),('thread',2),('thread',4),('thread',8),
                ('process',1),('process',2),('process',4),('process',8))
            expected.extend((name,mode,degree,series) for mode,degree in modes
                for series in ('eq142_admitted_control','eq142_owned_runner_e2e'))
        for slots,supported in ((4,24),(5,48),(7,60),(11,72),(48,72)):
            ref,cap=inputs(slots)
            with patch.object(Path,'open',side_effect=AssertionError('pure matrix must not read')):
                matrix=build_matrix(ref,cap,freeze(cap),**context(cap))
            self.assertEqual(len(matrix['cohorts']),72)
            self.assertEqual([row['cohort_id'] for row in matrix['cohorts']],[f'c{i:03d}' for i in range(72)])
            self.assertEqual([tuple(row['selector'][k] for k in ('profile','mode','degree','series')) for row in matrix['cohorts']],expected)
            self.assertEqual(sum(row['supported'] for row in matrix['cohorts']),supported)
            self.assertEqual(matrix['cohorts'][-1]['selector'],{'profile':'family_old_close_gap','mode':'process','degree':2,'series':'eq142_owned_runner_e2e'})

    def test_full_runtime_install_inventory_and_freeze_change_matrix_identity(self):
        ref,cap=inputs();matrix=build_matrix(ref,cap,freeze(cap),**context(cap))
        for field in ('files','installed_runtime'):
            changed=deepcopy(ref);changed['state'][field]['extra']={'unqualified_test_identity':1}
            with self.assertRaises(ValueError):verify_matrix(matrix,changed,cap,freeze(cap),**context(cap))
        changed=deepcopy(ref);changed['source_inventory']['different_runtime_source']=True
        with self.assertRaises(ValueError):verify_matrix(matrix,changed,cap,freeze(cap),**context(cap))
        cap['capture_end_ns']='21'
        with self.assertRaises(ValueError):verify_matrix(matrix,ref,cap,freeze(cap),**context(cap))

    def test_missing_duplicate_reordered_coherent_omission_and_types_reject(self):
        ref,cap=inputs();matrix=build_matrix(ref,cap,freeze(cap),**context(cap))
        mutations=[]
        bad=deepcopy(matrix);bad['cohorts'].pop();mutations.append(bad)
        bad=deepcopy(matrix);bad['cohorts'][1]=deepcopy(bad['cohorts'][0]);mutations.append(bad)
        bad=deepcopy(matrix);bad['cohorts'].reverse();mutations.append(bad)
        bad=deepcopy(matrix);bad['cohorts'][0].update(supported=False,omission_reason='cpu_primary_group_slots_insufficient');mutations.append(bad)
        for key,value in [('supported',1),('omission_reason','memory_capacity'),('extra',True)]:
            bad=deepcopy(matrix);bad['cohorts'][0][key]=value;mutations.append(bad)
        bad=deepcopy(matrix);bad['cohorts'][0]['selector']['degree']=True;mutations.append(bad)
        bad=deepcopy(matrix);bad['source_state_sha256']='a'*64;mutations.append(bad)
        for bad in mutations:
            with self.assertRaises(ValueError):verify_matrix(bad,ref,cap,freeze(cap),**context(cap))

    def test_memory_disk_source_and_cpu_failure_never_become_omissions(self):
        for kind in ('memory','disk','cpu','reference','freeze'):
            ref,cap=inputs()
            if kind=='memory':
                cap['free_memory_bytes']-=1;cap['raw_machine_observation'].update(initial_free_physical_bytes=cap['free_memory_bytes'],memory_cap_bytes=cap['free_memory_bytes']//4)
            elif kind=='disk':cap['free_storage_bytes']-=1;cap['storage_used_bytes']+=1
            elif kind=='cpu':ref,cap=inputs(3)
            elif kind=='reference':ref['cpu_slots']=True
            frozen=freeze(cap)
            if kind=='freeze':frozen['sha256']='a'*64
            with self.assertRaises((ValueError,CapacityUnavailable)):
                build_matrix(ref,cap,frozen,**context(cap))


if __name__=='__main__':unittest.main()
