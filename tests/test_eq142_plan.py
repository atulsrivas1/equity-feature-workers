"""Independent plan metadata reconstruction, never source or launch authority."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import pickle
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'));sys.path.insert(0,str(ROOT/'tests'))
from equity_feature_workers import ResourceBudget,WorkItem
from eq142_delegate import describe_plan
from eq142_graph import wire
from eq142_trace import TraceSpecificationError
from manifest_fixture import fixture


def never_called(task,batches):
    raise AssertionError('Metadata description must not calculate')


class PortablePlan(unittest.TestCase):
    def items(self):
        task,_,_=fixture()
        # Metadata-only supplied task descriptors: no input/calculation claim.
        return tuple(WorkItem(replace(task,instruments=(name,),partition_id=name),(),never_called) for name in ('A','B'))

    def test_operations_closed_before_caller_iteration_or_unsupported_members(self):
        calls=[]
        class OpaqueIterator:
            def __iter__(self):
                calls.append('caller iteration');return iter(['trace.bookkeeping'])
        options=dict(items=self.items(),mode='sequential',budget=ResourceBudget(),expected_inventory={},
            output_dir='synthetic/trace')
        for value in (OpaqueIterator(),'trace.bookkeeping',{'trace.bookkeeping':1},None,
                ['unknown.operation'],[True],[1],['trace.bookkeeping','trace.bookkeeping']):
            with self.subTest(kind=type(value)),self.assertRaises(TraceSpecificationError):
                describe_plan(**options,operations=value)
        self.assertEqual(calls,[])
        for value in (['trace.bookkeeping'],('trace.bookkeeping',)):
            plan,_=describe_plan(**options,operations=value)
            self.assertEqual(plan['operations'],['trace.bookkeeping'])

    def test_equal_decoded_reordered_witness_preserves_exact_original_pickle(self):
        original={'job_name':'Local\\equity-feature-supplied-'+'a'*32,'output_dir':'synthetic/runner/owned-processes'}
        options=dict(items=self.items(),mode='sequential',budget=ResourceBudget(),expected_inventory={},
            output_dir='synthetic/runner/trace',operations=['trace.bookkeeping'])
        first,size=describe_plan(**options,owned_process_witness=original)
        expected=pickle.dumps(first,protocol=5)
        for witness in (json.loads(json.dumps(original)),dict(reversed(list(original.items())))):
            plan,observed=describe_plan(**options,owned_process_witness=witness)
            self.assertEqual(plan,first);self.assertEqual(observed,size)
            self.assertEqual(pickle.dumps(plan,protocol=5),expected)
            self.assertEqual(list(plan['owned_process_witness']),['job_name','output_dir'])

    def test_hand_authored_independent_partition_plan_and_exact_pickle_metadata(self):
        items=self.items();inventory={'z':2,'a':1};budget=ResourceBudget(workers=2,max_cpu_slots=4,max_in_flight=2)
        with patch('eq142_delegate.bind_source',side_effect=AssertionError('no source probe')),patch.object(
                Path,'resolve',side_effect=AssertionError('no path lookup')),patch.object(
                Path,'open',side_effect=AssertionError('no I/O')):
            plan,size=describe_plan(items,mode='process',budget=budget,expected_inventory=inventory,
                output_dir='synthetic/runner/trace',operations=['trace.bookkeeping'])
        expected={'schema':'eq142-delegate-plan:1','mode':'process','expected_inventory':{'a':1,'z':2},
            'output_dir':'synthetic/runner/trace','operations':['trace.bookkeeping'],
            'reservations':[],'qualification_profile_dir':None,'owned_process_witness':None}
        for n,item in enumerate(items):
            hashes=[item.task.task_sha256]
            expected['reservations'].append({'id':'delegate:%03d'%n,
                'key':hashlib.sha256(b'eq142-invocation1\0'+wire(hashes)).hexdigest(),'task_hashes':hashes,
                'max_spans':57344,'max_bytes':16777216,'file':'delegate-%03d.json'%n})
        self.assertEqual(plan,expected);self.assertEqual(size,len(pickle.dumps(expected,protocol=5)))
        inventory['a']=999;self.assertEqual(plan['expected_inventory'],{'a':1,'z':2})

    def test_sequential_per_task_and_shared_population_partition_rules(self):
        items=self.items();inventory={}
        sequential,_=describe_plan(items,mode='sequential',budget=ResourceBudget(),expected_inventory=inventory,
            output_dir='synthetic/trace',operations=[])
        self.assertEqual([r['task_hashes'] for r in sequential['reservations']],
            [[item.task.task_sha256] for item in items])  # Population groups A, then B.
        shared=(items[0],WorkItem(replace(items[1].task,instruments=('A',)),(),never_called))
        threaded,_=describe_plan(shared,mode='thread',budget=ResourceBudget(workers=2,max_cpu_slots=4),
            expected_inventory=inventory,output_dir='synthetic/trace',operations=[])
        self.assertEqual(len(threaded['reservations']),1)
        self.assertEqual(threaded['reservations'][0]['task_hashes'],sorted(item.task.task_sha256 for item in shared))
        self.assertEqual(threaded['reservations'][0]['max_spans'],114688)
        self.assertEqual(threaded['reservations'][0]['max_bytes'],16777216)

    def test_qualification_and_ownership_are_metadata_copies_not_certificates(self):
        witness={'job_name':'Local\\equity-feature-supplied-'+'a'*32,'output_dir':'synthetic/runner/owned-processes'}
        operations=['trace.bookkeeping']
        plan,_=describe_plan(self.items(),mode='sequential',budget=ResourceBudget(),expected_inventory={},
            output_dir='synthetic/runner/trace',operations=operations,
            qualification_profile_dir='synthetic/runner/code-counts',owned_process_witness=witness)
        witness['output_dir']='changed';operations.clear()
        self.assertEqual(plan['owned_process_witness']['output_dir'],'synthetic/runner/owned-processes')
        self.assertEqual(plan['operations'],['trace.bookkeeping'])
        self.assertEqual(plan['qualification_profile_dir'],'synthetic/runner/code-counts')
        self.assertNotIn('full_sample_accepted',plan)

    def test_invalid_shape_and_unsupported_sequential_degree_fail_without_lookup(self):
        options=dict(items=self.items(),mode='sequential',budget=ResourceBudget(),expected_inventory={},
            output_dir='synthetic/trace',operations=[])
        for key,value in [('items',()),('items',list(options['items'])),('mode','unknown'),
                ('budget',ResourceBudget(workers=2,max_cpu_slots=4)),('expected_inventory',[]),
                ('output_dir',None),('output_dir',Path('untrusted')),('output_dir','x\0y'),
                ('qualification_profile_dir',False),('owned_process_witness',{}),
                ('owned_process_witness',{'job_name':'wrong','output_dir':'synthetic/owned'})]:
            with self.subTest(key=key),patch.object(Path,'resolve',side_effect=AssertionError('no lookup')):
                with self.assertRaises(TraceSpecificationError):describe_plan(**{**options,key:value})


if __name__=='__main__':unittest.main()
