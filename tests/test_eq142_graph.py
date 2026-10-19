"""Independent exact topology, readiness separation, source identity and cycles."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import sys
import types
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
sys.path.insert(0,str(ROOT/'tests'))
from eq142_graph import GraphSpecificationError, describe_task, describe_item, validate_graph, describe_job, validate_job
from eq142_fixture import qualification, canonical_cases, load


def never_called(task,batches):
    raise AssertionError('Description must not execute callbacks')


class OutputGraphs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bound=qualification()

    def graph(self, task, batches):
        return describe_task(task,self.bound.callback,batches,qualification=self.bound)

    def test_four_independent_exact_graph_sets(self):
        cases={case['id']:(task,batches) for case,task,batches in canonical_cases()}
        for expected in load('GRAPH_EXPECTATIONS.json')['cases']:
            with self.subTest(case=expected['id']):
                task,batches=cases[expected['id']];graph=self.graph(task,batches)
                self.assertEqual({n['id'] for n in graph['nodes']},set(expected['exact_node_ids']))
                self.assertEqual({(e['from'],e['to'],e['reason']) for e in graph['edges']},set(map(tuple,expected['exact_edges'])))
                self.assertEqual(graph['disposition'],'qualified_intraday_description')
                for n in graph['nodes']:
                    if n['id'].startswith('state:'):
                        self.assertFalse(n['ordering']['incremental_capability'])
                if 'position_output_predicate' in expected:
                    self.assertEqual(next(n['predicate'] for n in graph['nodes'] if n['id']=='output:intraday.band_position'),expected['position_output_predicate'])

    def test_all_canonical_operand_reachability(self):
        count=0
        for case,task,batches in canonical_cases():
            if case.get('kind')!='qualified_intraday_description':continue
            graph=self.graph(task,batches);adj={n['id']:set() for n in graph['nodes']}
            for e in graph['edges']:adj[e['from']].add(e['to'])
            def reaches(a,b):
                pending=list(adj[a]);seen=set()
                while pending:
                    n=pending.pop()
                    if n==b:return True
                    if n not in seen:seen.add(n);pending.extend(adj[n])
                return False
            for operand,feature in case.get('expected_operand_output_reachability_pairs',[]):
                self.assertTrue(reaches('operand:'+operand,'output:'+feature))
            outputs=[n for n in adj if n.startswith('output:')]
            for a in outputs:
                for b in outputs:
                    if a!=b:self.assertFalse(reaches(a,b))
            self.assertFalse(graph['input_identity_disposition']['new_reuse_certificate'])
            self.assertEqual(graph['input_identity_disposition']['body_verification'],'unverified_current_body')
            count+=1
        self.assertGreaterEqual(count,10)

    def test_caller_alias_qualifies_copies_and_wrappers_do_not(self):
        _,task,batches=next(canonical_cases());fn=self.bound.callback
        alias=fn
        self.assertEqual(describe_task(task,alias,batches,qualification=self.bound)['disposition'],'qualified_intraday_description')
        copy=types.FunctionType(fn.__code__,fn.__globals__,fn.__name__,fn.__defaults__,fn.__closure__)
        for unknown in (copy,lambda task,batches:fn(task,batches)):
            graph=describe_task(task,unknown,batches,qualification=self.bound)
            self.assertEqual(graph['disposition'],'conservative_unresolved')
            self.assertEqual(len(graph['nodes']),6)
            self.assertFalse(any(n['kind']=='output' for n in graph['nodes']))

    def test_mutated_module_export_code_and_inspected_alias_reject(self):
        import equity_feature_workers.intraday as module
        import equity_feature_workers.supervisor as supervisor
        _,task,batches=next(canonical_cases())
        original=module.compute_intraday_inputs
        try:
            module.compute_intraday_inputs=lambda task,batches:()
            with self.assertRaises(GraphSpecificationError) as error:self.graph(task,batches)
            self.assertEqual(error.exception.code,'source_mismatch')
        finally:module.compute_intraday_inputs=original
        old_code=original.__code__
        try:
            original.__code__=(lambda task,batches:()).__code__
            with self.assertRaises(GraphSpecificationError) as error:self.graph(task,batches)
            self.assertEqual(error.exception.code,'source_mismatch')
        finally:original.__code__=old_code
        old_size=supervisor._size
        try:
            supervisor._size=lambda x:0
            with self.assertRaises(GraphSpecificationError) as error:self.graph(task,batches)
            self.assertEqual(error.exception.code,'source_mismatch')
        finally:supervisor._size=old_size
        self.bound.check()

    def test_cycles_duplicates_unknown_endpoints_and_shapes(self):
        _,task,batches=next(canonical_cases());valid=self.graph(task,batches)
        changes=[('cycle',lambda g:g['edges'].append({'from':'join:result','to':'gate:resource','reason':'resource_admission','contract':'worker_resource_admission'})),
            ('duplicate',lambda g:g['nodes'].append(deepcopy(g['nodes'][0]))),
            ('duplicate',lambda g:g['edges'].append(deepcopy(g['edges'][0]))),
            ('unknown_endpoint',lambda g:g['edges'].append({'from':'absent','to':'join:result','reason':'result_join','contract':'full_result_identity_and_evidence'})),
            ('invalid_schema',lambda g:g['nodes'][0].update(kind='invented')),
            ('invalid_schema',lambda g:g.update(unrecognized=True))]
        for code,mutate in changes:
            with self.subTest(code=code):
                graph=deepcopy(valid);mutate(graph)
                with self.assertRaises(GraphSpecificationError) as error:validate_graph(graph)
                self.assertEqual(error.exception.code,code)

    def test_nested_unknown_fields_and_false_input_certificates_reject(self):
        _,task,batches=next(canonical_cases());valid=self.graph(task,batches)
        for mutate in [lambda g:g['scope'].update(extra=True),
            lambda g:g['source'].update(extra=True),lambda g:g['nodes'][0]['source_anchor'].update(extra=True),
            lambda g:g['input_identity_disposition'].update(new_reuse_certificate=True),
            lambda g:g['nodes'][0].update(owner='invented'),
            lambda g:g['source']['package_build_sources'].update(core='bad')]:
            graph=deepcopy(valid);mutate(graph)
            with self.assertRaises(GraphSpecificationError):validate_graph(graph)

    def test_generic_transitive_components_and_real_job_barriers(self):
        from equity_feature_workers import WorkItem, ResourceBudget
        _,task,batches=next(canonical_cases())
        populations=(('A',),('A','B'),('B','C'),('D',))
        items=tuple(WorkItem(replace(task,partition_id='pop'+str(i),instruments=p,
            job_id='job'+str(i%2),generation_id='generation'+str(i%2)),batches,never_called) for i,p in enumerate(populations))
        job=describe_job(items,mode='thread',budget=ResourceBudget(workers=4,max_cpu_slots=8,max_in_flight=4),qualification=self.bound)
        lookup={i.task.task_sha256:n for n,i in enumerate(items)}
        self.assertEqual(sorted(sorted(lookup[h] for h in c['ordered_task_hashes']) for c in job['components']),[[0,1,2],[3]])
        self.assertEqual(len(job['partitions']),2)
        self.assertEqual(len(job['job_scopes']),2)
        self.assertEqual([b['kind'] for b in job['barriers']],load('GRAPH_EXPECTATIONS.json')['job_expectations']['preserved_barriers'])
        self.assertTrue(all(b['evidence']=='source_required' for b in job['barriers']))
        self.assertEqual(job['resource_admission']['evidence'],'not_executed')
        self.assertIsNone(job['resource_admission']['initial_reserved_resident_bytes'])
        self.assertEqual(job['cancelled_or_undispatched'],[])
        bad=deepcopy(job);bad['components'][0]['partition_id']='absent'
        with self.assertRaises(GraphSpecificationError) as error:validate_job(bad)
        self.assertEqual(error.exception.code,'unknown_endpoint')
        bad=deepcopy(job);bad['job_scopes'][0]['task_hashes'].append(items[0].task.task_sha256)
        with self.assertRaises(GraphSpecificationError):validate_job(bad)

    def test_focused_family_tasks_are_one_component_and_no_new_parallel_shards(self):
        from equity_feature_workers import ResourceBudget
        from supplied_intraday_fixture import facts,work
        context,config,batch=facts()
        items=tuple(work(context,config,(batch,),features,name='family'+str(i)) for i,features in enumerate([
            ('intraday.ema','intraday.rsi'),('intraday.atr',),
            ('intraday.band_lower','intraday.band_middle','intraday.band_upper','intraday.band_width_ratio','intraday.band_position')]))
        for mode in ('thread','process'):
            job=describe_job(items,mode=mode,budget=ResourceBudget(workers=2,max_cpu_slots=4,max_in_flight=2),qualification=self.bound)
            self.assertEqual(len(job['components']),1);self.assertEqual(len(job['partitions']),1)
            self.assertEqual(job['components'][0]['population'],['A'])
            self.assertEqual(len(job['components'][0]['ordered_task_hashes']),3)


if __name__=='__main__':unittest.main()
