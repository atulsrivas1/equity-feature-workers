"""Independent synthetic arithmetic and actual read-only Windows capacity."""
from copy import deepcopy
import ctypes as ct
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_capacity import validate_capacity,admit_capacity,CapacityProbe,CapacityUnavailable,GroupAffinity


def sample():
    # Synthetic explicit four-slot/80GiB-free/3GiB-free exact boundary.
    source={'baseline_machine_module_sha256':'1'*64,'baseline_machine_definition_line':325,
        'python_runtime_version':'3.12.10','shutil_module_sha256':'2'*64,
        'shutil_windows_definition_line':1379,'native_contract_version':'win32-capacity-api2'}
    return {'schema':'eq142-capacity-receipt:1','position':'before','capture_start_ns':'10','capture_end_ns':'20',
        'active_processor_groups_before':2,'active_processor_groups_after':2,'process_groups_before':[0,1],
        'process_groups_after':[0,1],'primary_thread_group_before':1,'primary_thread_group_after':1,
        'primary_thread_mask_before':'0xf','primary_thread_mask_after':'0xf','primary_system_mask':'0xff',
        'word_bits':64,'allowed_affinity_mask':'0xf','allowed_cpu_ids':[0,1,2,3],
        'allowed_cpu_domain':'windows_primary_processor_group:1','allowed_cpu_slots':4,
        'free_memory_bytes':3221225472,'storage_device':'9007199254740993','storage_total_bytes':107374182400,
        'storage_used_bytes':21474836480,'free_storage_bytes':85899345920,
        'study_encoded_bytes_already_retained':0,'remaining_study_allowance_bytes':77309411328,
        'study_storage_reserve_bytes':77309411328,'additional_storage_reserve_bytes':8589934592,
        'owned_job_sampled_rss_cap_bytes':1073741824,'additional_host_memory_reserve_bytes':2147483648,
        'raw_machine_observation':{'allowed_affinity_mask':'0xf','allowed_cpu_slots':4,
            'initial_free_physical_bytes':3221225472,'memory_cap_bytes':805306368,
            'degrees':[1,2],'omitted_degrees':[4,8],'backend_slots':1},
        'non_atomic_observation':True,'physical_enforcement':False,'probe_sources':source,
        'capacity_disposition':'conservative_observed_primary_group_budget_not_global_eligibility',
        'placement_enforced':False}


class PortableCapacity(unittest.TestCase):
    def admit(self,record,**changes):
        options={'degree':1,'expected_device':record['storage_device'],
            'expected_retained_bytes':record['study_encoded_bytes_already_retained']}
        return admit_capacity(record,record['probe_sources'],**{**options,**changes})

    def test_exact_cpu_ram_disk_boundaries_and_pure_execution(self):
        record=sample()
        with (patch('builtins.open',side_effect=AssertionError('pure checker cannot open files')),
                patch.object(Path,'open',side_effect=AssertionError('pure checker cannot open paths'))):
            validate_capacity(record,record['probe_sources']);actual=self.admit(record)
        self.assertEqual(actual,{'evidence':'observed_primary_group_and_host_capacity_with_caller_joined_namespace',
            'degree':1,'max_cpu_slots':2,'physical_enforcement':False,'worker_placement_observed':False,
            'full_sample_accepted':False})
        # Original machine's degree2 suggestion is not EQ142 admission authority.
        with self.assertRaises(CapacityUnavailable) as error:self.admit(record,degree=2)
        self.assertEqual(error.exception.code,'cpu_capacity')
        for what in ('before_memory','after_memory','disk'):
            altered=deepcopy(record)
            if what=='disk':
                altered['free_storage_bytes']-=1;altered['storage_used_bytes']+=1
            else:
                if what=='after_memory':altered['position']='after';altered['free_memory_bytes']=2147483648
                altered['free_memory_bytes']-=1
                altered['raw_machine_observation']['initial_free_physical_bytes']=altered['free_memory_bytes']
                altered['raw_machine_observation']['memory_cap_bytes']=altered['free_memory_bytes']//4
            validate_capacity(altered,altered['probe_sources']) # Raw low capacity is retainable.
            with self.subTest(what=what),self.assertRaises(CapacityUnavailable):self.admit(altered)
        record['position']='after';record['free_memory_bytes']=2147483648
        record['raw_machine_observation'].update(initial_free_physical_bytes=2147483648,memory_cap_bytes=536870912)
        self.admit(record)

    def test_remaining_encoded_bytes_and_same_device_context(self):
        record=sample()
        record.update(study_encoded_bytes_already_retained=17,remaining_study_allowance_bytes=77309411311,
            free_storage_bytes=85899345903,storage_used_bytes=21474836497)
        self.admit(record)
        for options in ({'expected_retained_bytes':16},{'expected_retained_bytes':True},
                {'expected_device':'9007199254740992'},{'expected_device':9007199254740993}):
            with self.subTest(options=options),self.assertRaises(ValueError):self.admit(record,**options)
        record['free_storage_bytes']-=1;record['storage_used_bytes']+=1
        with self.assertRaises(CapacityUnavailable):self.admit(record)

    def test_unsigned_device_width_and_noncanonical_values(self):
        for device in ('0','9007199254740992','9007199254740993','18446744073709551615'):
            record=sample();record['storage_device']=device
            retained=json.loads(json.dumps(record))
            self.admit(retained);self.assertEqual(retained['storage_device'],device)
        for device in (True,9007199254740993,9007199254740992.0,'01','-1','+1',' 1','1.0',
                '18446744073709551616','1'*21):
            record=sample();record['storage_device']=device
            with self.subTest(device=device),self.assertRaises(ValueError):validate_capacity(record,record['probe_sources'])

    def test_closed_types_topology_masks_raw_source_and_constant_joins(self):
        mutations=[('active_processor_groups_after',1),('active_processor_groups_before',True),
            ('process_groups_before',[0,0]),('process_groups_after',[0,False]),
            ('primary_thread_group_before',7),('primary_thread_group_after',0),
            ('primary_thread_mask_after','0x7'),('primary_thread_mask_before','0x7'),
            ('primary_system_mask','0x7'),('allowed_affinity_mask','0x0f'),
            ('allowed_cpu_ids',[False,1,2,3]),('allowed_cpu_ids',[0,1,2]),('allowed_cpu_slots',True),
            ('allowed_cpu_domain','windows_primary_processor_group:7'),('word_bits',64.0),
            ('capture_start_ns','21'),('capture_end_ns',20),('storage_total_bytes',107374182401),
            ('study_encoded_bytes_already_retained',1),('remaining_study_allowance_bytes',True),
            ('study_storage_reserve_bytes',77309411329),('additional_storage_reserve_bytes',0),
            ('owned_job_sampled_rss_cap_bytes',1),('additional_host_memory_reserve_bytes',0),
            ('physical_enforcement',0),('placement_enforced',True),('non_atomic_observation',1)]
        for key,value in mutations:
            record=sample();record[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):validate_capacity(record,record['probe_sources'])
        record=sample();record.update(active_processor_groups_before=1,active_processor_groups_after=1,
            process_groups_before=[7],process_groups_after=[7],primary_thread_group_before=7,
            primary_thread_group_after=7,allowed_cpu_domain='windows_primary_processor_group:7')
        with self.assertRaises(ValueError):validate_capacity(record,record['probe_sources'])
        for key,value in (('memory_cap_bytes',1),('degrees',[1,2,4]),('backend_slots',True),('extra',1)):
            record=sample();record['raw_machine_observation'][key]=value
            with self.subTest(raw=key),self.assertRaises(ValueError):validate_capacity(record,record['probe_sources'])
        record=sample();expected=deepcopy(record['probe_sources']);record['probe_sources']['shutil_module_sha256']='3'*64
        with self.assertRaises(ValueError):validate_capacity(record,expected)
        for degree in (True,1.0,3,0):
            with self.subTest(degree=degree),self.assertRaises(ValueError):self.admit(sample(),degree=degree)


@unittest.skipUnless(os.name=='nt','Windows capacity API required')
class WindowsCapacity(unittest.TestCase):
    def prepare(self,root):
        from eq142_fixture import require_cpu_slots,load
        require_cpu_slots(self,2)
        return CapacityProbe(root,load('CAPACITY_RUNTIME_SOURCE_HASHES.json'))

    def test_actual_source_native_raw_and_exact_device_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);probe=self.prepare(root)
            before=probe.capture('before',encoded_bytes_already_retained=0)
            self.assertTrue(before['storage_device']==str(os.stat(root).st_dev),'actual opaque device differs')
            self.assertEqual(before['allowed_cpu_slots'],len(before['allowed_cpu_ids']))
            self.assertIn(before['primary_thread_group_before'],before['process_groups_before'])
            self.assertEqual(ct.sizeof(GroupAffinity),16)
            # Actual persisted byte count is supplied independently by the caller.
            (root/'synthetic.bin').write_bytes(b'abcde')
            after=probe.capture('after',encoded_bytes_already_retained=(root/'synthetic.bin').stat().st_size)
            self.assertEqual(after['study_encoded_bytes_already_retained'],5)
            self.assertEqual(after['remaining_study_allowance_bytes'],77309411323)
            self.assertTrue(after['storage_device']==before['storage_device'],'opaque device changed')
            for record in (before,after):
                validate_capacity(record,probe.probe_sources)
                self.assertFalse(record['physical_enforcement']);self.assertFalse(record['placement_enforced'])

    def test_api_failure_and_unknown_topology_never_emit_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            probe=self.prepare(Path(directory))
            for name,value in (('GetActiveProcessorGroupCount',0),('GetActiveProcessorGroupCount',65),
                    ('GetProcessGroupAffinity',0),('GetThreadGroupAffinity',0),('GetProcessAffinityMask',0)):
                with self.subTest(api=name,value=value),patch.object(probe.kernel,name,return_value=value):
                    with self.assertRaises(CapacityUnavailable):probe.capture('before',encoded_bytes_already_retained=0)
            with patch.object(probe.kernel,'CloseHandle',side_effect=AssertionError('pseudo-handle must not close')):
                probe.capture('before',encoded_bytes_already_retained=0)

    def test_source_context_and_device_tampering_reject(self):
        import shutil
        with tempfile.TemporaryDirectory() as directory:
            probe=self.prepare(Path(directory))
            for owner,name in ((probe.baseline,'machine'),(shutil,'disk_usage'),(shutil,'nt'),(probe.nt,'_getdiskusage'),(os,'stat')):
                with self.subTest(source=name),patch.object(owner,name,lambda *args:None):
                    with self.assertRaises(ValueError):probe.capture('before',encoded_bytes_already_retained=0)
            # The unchanged directory observation cannot match a wrong context.
            probe.directory_device='0' if probe.directory_device!='0' else '1'
            with self.assertRaises(ValueError):probe.capture('before',encoded_bytes_already_retained=0)
            for method in ('is_symlink','is_junction'):
                with self.subTest(indirection=method),patch.object(Path,method,return_value=True):
                    with self.assertRaises(ValueError):self.prepare(Path(directory))
