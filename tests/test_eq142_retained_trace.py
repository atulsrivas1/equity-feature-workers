"""Independent retained-body joins without filesystem or live collector authority."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from eq142_graph import wire,digest
from eq142_trace import Collector,TraceSpecificationError
from eq142_retained_trace import combine_retained_fragments,validate_reservations


class RetainedTrace(unittest.TestCase):
    def bodies(self):
        bodies={};reservations=[]
        with tempfile.TemporaryDirectory() as directory:
            for name in ('first','second'):
                collector=Collector(name,max_spans=8,max_bytes=16384,operations=('trace.bookkeeping',))
                frame=collector.begin('trace.bookkeeping',());collector.finish(frame,result=1)
                path=Path(directory)/(name+'.json');collector.persist(path)
                bodies[path.name]=path.read_bytes()
                reservations.append({'id':name,'file':path.name,'max_spans':8,'max_bytes':16384})
        return bodies,reservations

    def test_full_bodies_recompute_independent_global_counts_and_refs_without_io(self):
        bodies,reservations=self.bodies();before=deepcopy((bodies,reservations))
        with patch('builtins.open',side_effect=AssertionError('pure closure performs no I/O')),patch.object(
                Path,'open',side_effect=AssertionError('pure closure performs no I/O')):
            joined=combine_retained_fragments(bodies,reservations)
        self.assertEqual((bodies,reservations),before)
        self.assertEqual(joined['representation'],'retained_fragments');self.assertEqual(joined['total_span_count'],2)
        self.assertEqual(len(joined['processes']),1);self.assertEqual(len(joined['timelines']),1)
        self.assertEqual(joined['counters'],[{'operation':'trace.bookkeeping','scope_id':'stage:unbound',
            'byte_observation':'unavailable','attempts':2,'returned':2,'rejected':0,'returned_observed_bytes':None,
            'unique_physical_allocation_bytes':None}])
        self.assertEqual([r['reservation_id'] for r in joined['fragments']],['first','second'])
        for ref in joined['fragments']:
            body=bodies[ref['retained_name']]
            self.assertEqual(ref['codec'],'eq142_local_fragment');self.assertEqual(ref['sha256'],digest(body))
            self.assertEqual(ref['bytes'],len(body));self.assertEqual(ref['span_count'],1)
        self.assertIsNone(joined['global_numeric_critical_path_ns']);self.assertIsNone(joined['native_ipc_encoded_bytes'])

    def test_membership_reservations_and_encoded_bounds_reject_before_decode(self):
        bodies,reservations=self.bodies()
        for bad in ({},{**bodies,'unknown.json':b'{}'}, {'first.json':bodies['first.json']},
                {**bodies,'first.json':bytearray(bodies['first.json'])},{**bodies,'first.json':b''},
                {**bodies,'first.json':b'x'*16385}):
            with self.subTest(kind=type(bad)),patch('eq142_retained_trace.decode_fragment',
                    side_effect=AssertionError('must admit before decoding')):
                with self.assertRaises(TraceSpecificationError):combine_retained_fragments(bad,reservations)
        for bad in ([],[reservations[0],reservations[0]],
                [{**reservations[0],'max_bytes':True}], [{**reservations[0],'max_spans':False}],
                [{**reservations[0],'file':'../first.json'}],
                [{**reservations[0],'id':'r'+str(n),'file':'r'+str(n)+'.json','max_bytes':16777216} for n in range(5)]):
            with self.assertRaises(TraceSpecificationError):validate_reservations(bad)

    def test_noncanonical_duplicate_keys_and_cross_fragment_conflicts_reject(self):
        bodies,reservations=self.bodies();raw=bodies['second.json']
        changed=json.loads(raw);changed['process_binding']['pid']+=1
        duplicates=json.loads(raw)
        duplicates['spans'][0]['id']=json.loads(bodies['first.json'])['spans'][0]['id']
        wrong_id=json.loads(raw);wrong_id['reservation_id']='third'
        for replacement in (raw+b' ',raw.replace(b'{',b'{"schema":"duplicate",',1),b'\xff',
                b'[]',wire(changed),wire(duplicates),wire(wrong_id)):
            with self.subTest(size=len(replacement)),self.assertRaises(TraceSpecificationError):
                combine_retained_fragments({**bodies,'second.json':replacement},reservations)

    def test_streaming_filesystem_adapter_reads_once_and_preserves_full_result(self):
        from eq142_evidence import combine_fragments,bounded_read
        bodies,reservations=self.bodies()
        with tempfile.TemporaryDirectory() as directory:
            for name,body in bodies.items():(Path(directory)/name).write_bytes(body)
            observations=[]
            def read(path,maximum):
                observations.append((path.name,maximum));return bounded_read(path,maximum)
            with patch('eq142_evidence.bounded_read',side_effect=read):
                self.assertEqual(combine_fragments(directory,reservations),combine_retained_fragments(bodies,reservations))
            self.assertEqual(observations,[('first.json',16384),('second.json',16384)])
            with patch('eq142_evidence.bounded_read',side_effect=AssertionError('admit reservations first')):
                with self.assertRaises(TraceSpecificationError):combine_fragments(directory,[reservations[0]]*2)


if __name__=='__main__':unittest.main()
