"""Staged synthetic filesystem ledger checks, no actual writer/root authority."""
from pathlib import Path
import sys,tempfile,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'tests')]
from eq142_namespace import EncodedJournal,FixedMember
from eq142_graph import digest


def member(body,*,name='native.json'):
    return FixedMember('sample','regular.native',{'codec':'native_receipt','sha256':digest(body),
        'bytes':len(body),'retained_name':name})


class PortableNamespace(unittest.TestCase):
    def test_actual_bounded_readback_prefix_totals_and_empty_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);sample=root/'sample';sample.mkdir();body=b'{}\n'
            (sample/'native.json').write_bytes(body)
            journal=EncodedJournal(root);journal.register('study',root,{})
            journal.register('sample',sample,{'native.json':member(body)})
            self.assertEqual(journal.totals()['total_bytes'],3)
            self.assertEqual(journal.reconcile({'study':{'sample':'sample'},'sample':{}})['total_bytes'],3)
            journal.register('sample',sample,{'native.json':member(body)})
            self.assertEqual(journal.totals()['total_bytes'],3)

    def test_changed_original_bytes_retained_failure_prevents_reuse_even_after_restoration(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=root/'native.json';path.write_bytes(b'{}\n')
            journal=EncodedJournal(root);journal.register('study',root,{'native.json':member(b'{}\n')})
            path.write_bytes(b'[]\n')
            with self.assertRaises(ValueError):journal.reconcile({'study':{}})
            path.write_bytes(b'{}\n')
            with self.assertRaises(ValueError):journal.reconcile({'study':{}})
            with self.assertRaises(ValueError):journal.totals()

    def test_caller_reference_mutation_cannot_replace_original_and_extras_missing_types_refuse(self):
        for change in ('extra','missing','directory','mutated_reference'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);path=root/'native.json';body=b'{}\n';path.write_bytes(body)
                policy=member(body);journal=EncodedJournal(root)
                journal.register('study',root,{'native.json':policy})
                if change=='extra':(root/'unexpected.json').write_bytes(body)
                elif change=='missing':path.unlink()
                elif change=='directory':path.unlink();path.mkdir()
                else:policy.reference.update(sha256=digest(b'[]\n'));path.write_bytes(b'[]\n')
                with self.assertRaises(ValueError):journal.reconcile({'study':{}})

    def test_duplicate_disconnected_escape_and_orphan_namespace_refuse(self):
        for change in ('duplicate','disconnected','escape','missing_root'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)/'owned';root.mkdir();child=root/'child';child.mkdir();journal=EncodedJournal(root)
                journal.register('study',root,{})
                journal.register('child',child,{})
                if change=='duplicate':
                    with self.assertRaises(ValueError):journal.register('alias',child,{})
                elif change=='escape':
                    with self.assertRaises(ValueError):journal.register('outside',Path(directory),{})
                elif change=='disconnected':
                    # Registered child exists physically but is omitted from tree policy.
                    with self.assertRaises(ValueError):journal.reconcile({'study':{},'child':{}})
                else:
                    other=EncodedJournal(child)
                    with self.assertRaises(ValueError):other.reconcile({})


if __name__=='__main__':unittest.main(verbosity=2)
