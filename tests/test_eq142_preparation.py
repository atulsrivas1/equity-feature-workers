"""Fixed preparation callback ordering and original actual-return provenance."""
from contextlib import contextmanager
from copy import deepcopy
import hashlib,json,shutil,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_preparation import PreparationRecorder
from eq142_graph import digest
from eq142_proofs import read_proof
from eq142_fixture import HAVE_INTRADAY,require_cpu_slots

PAIRS=tuple((p,s) for p in ('small','balanced','skewed','family_complete','family_missing_hl','family_old_close_gap')
            for s in ('eq142_admitted_control','eq142_owned_runner_e2e'))


@contextmanager
def evidence_root():
    root=Path(tempfile.mkdtemp(prefix='eq142-preparation-test-')).resolve();passed=False
    try:
        yield root
        passed=True
    finally:
        if passed:
            if root.parent!=Path(tempfile.gettempdir()).resolve() or not root.name.startswith('eq142-preparation-test-'):
                raise ValueError('test cleanup root changed')
            shutil.rmtree(root)
        else:print('Retained failed preparation test: '+str(root),file=sys.stderr)


def fake_pairs(root):
    # Synthetic byte-reference grammar only, never actual original provenance.
    recorder=PreparationRecorder(root/'reference');(root/'reference').mkdir();pairs=[]
    for profile,series in PAIRS:
        local=root/'reference'/(profile+'-'+series);local.mkdir();refs=[]
        for name,codec,body in (('products.json','eq142_product_projection',b'[]\n'),
                                ('diagnostics.json','diagnostic_snapshot',b'{}\n')):
            (local/name).write_bytes(body)
            refs.append({'codec':codec,'retained_name':name,'bytes':len(body),'sha256':digest(body)})
        pairs.append((profile,series,*refs))
    return recorder,pairs


class PortablePreparation(unittest.TestCase):
    def test_exact12_order_copied_references_and_closed_once(self):
        with evidence_root() as root:
            recorder,pairs=fake_pairs(root)
            for pair in pairs:recorder(*pair)
            expected=deepcopy(pairs);pairs[0][2]['sha256']='a'*64
            actual=recorder.finish()
            self.assertEqual([(e['profile'],e['series']) for e in actual],list(PAIRS))
            self.assertEqual(actual[0]['products_reference'],expected[0][2])
            with self.assertRaises(ValueError):recorder.finish()
            with self.assertRaises(ValueError):recorder(*expected[0])

    def test_missing_duplicate_out_of_order_unknown_and_wrong_codec_poison(self):
        for case in ('missing','duplicate','order','unknown','codec'):
            with evidence_root() as root:
                recorder,pairs=fake_pairs(root)
                if case=='missing':
                    recorder(*pairs[0])
                    with self.assertRaises(ValueError):recorder.finish()
                else:
                    pair=deepcopy(pairs[0])
                    if case=='duplicate':recorder(*pairs[0])
                    elif case=='order':pair=deepcopy(pairs[1])
                    elif case=='unknown':pair=('other',*pair[1:])
                    elif case=='codec':pair[2]['codec']='eq142_development_runner'
                    with self.assertRaises(ValueError):recorder(*pair)
                with self.assertRaises(ValueError):recorder(*pairs[0])

    def test_changed_body_and_preexisting_namespace_refused(self):
        with evidence_root() as root:
            recorder,pairs=fake_pairs(root)
            with self.assertRaises(ValueError):PreparationRecorder(root/'reference')
            for pair in pairs:recorder(*pair)
            (root/'reference'/(PAIRS[0][0]+'-'+PAIRS[0][1])/'products.json').write_bytes(b'{}\n')
            with self.assertRaises(ValueError):recorder.finish()


@unittest.skipUnless(HAVE_INTRADAY,'Optional installed intraday extra is absent')
class ActualPreparation(unittest.TestCase):
    def test_original_writer_returns_all12_pairs_after_math_and_unchanged_schema(self):
        from benchmark_execution_baseline import prepare_reference,source_state
        import eq142_evidence
        slots=require_cpu_slots(self,4);before=source_state();events=[]
        original=eq142_evidence.persist_record
        original_diagnostics=eq142_evidence.retain_diagnostics
        def observed(path,value,**options):
            result=original(path,value,**options);events.append((Path(path),dict(result)));return result
        def observed_diagnostics(snapshot,directory):
            result=original_diagnostics(snapshot,directory)
            events.append((Path(directory)/'diagnostics.json',dict(result)))
            return result
        with evidence_root() as root:
            recorder=PreparationRecorder(root/'reference')
            with patch('benchmark_execution_baseline.persist_record',side_effect=observed),patch('eq142_evidence.retain_diagnostics',side_effect=observed_diagnostics):
                returned=prepare_reference(root/'reference',cpu_slots=slots,preparation_recorder=recorder)
            pairs=recorder.finish();self.assertEqual(len(pairs),12)
            expected={(path.parent.name,path.name):ref for path,ref in events}
            for entry in pairs:
                local=entry['profile']+'-'+entry['series']
                for field,name,codec in (('products_reference','products.json','eq142_product_projection'),
                                          ('diagnostics_reference','diagnostics.json','diagnostic_snapshot')):
                    self.assertEqual(entry[field],{'codec':codec,**expected[(local,name)]})
                    read_proof(root/'reference'/local,entry[field],scope='reference_preparation',name=name)
            body=(root/'reference/reference.json').read_bytes();reference=json.loads(body)
            self.assertEqual(set(reference),{'schema','stage','state','source_inventory','cpu_slots','profiles'})
            self.assertEqual(returned,{'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body),'retained_name':'reference.json'})
            self.assertEqual(source_state(),before)

    def test_failed_math_emits_no_callback_and_preserves_original_failure(self):
        from benchmark_execution_baseline import prepare_reference
        slots=require_cpu_slots(self,4);error=ValueError('independent mathematics injection');calls=[]
        with evidence_root() as root:
            with patch('benchmark_execution_baseline.verify_result',side_effect=error):
                with self.assertRaises(ValueError) as caught:
                    prepare_reference(root/'reference',cpu_slots=slots,profiles=('small',),preparation_recorder=lambda *args:calls.append(args))
            self.assertIs(caught.exception,error);self.assertEqual(calls,[])
            self.assertTrue((root/'reference/small-eq142_admitted_control/products.json').is_file())
            self.assertFalse((root/'reference/reference.json').exists())

    def test_callback_failure_retains_original_products_and_no_final_reference(self):
        from benchmark_execution_baseline import prepare_reference
        slots=require_cpu_slots(self,4);error=RuntimeError('callback injection');calls=[]
        def failure(*args):calls.append(args);raise error
        with evidence_root() as root:
            with self.assertRaises(RuntimeError) as caught:
                prepare_reference(root/'reference',cpu_slots=slots,profiles=('small',),preparation_recorder=failure)
            self.assertIs(caught.exception,error);self.assertEqual(len(calls),1)
            self.assertTrue((root/'reference/small-eq142_admitted_control/diagnostics.json').is_file())
            self.assertFalse((root/'reference/reference.json').exists())


if __name__=='__main__':unittest.main()
