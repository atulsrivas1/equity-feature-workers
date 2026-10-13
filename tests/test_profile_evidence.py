"""Independent measurement-report and output ownership checks, no timing ranks."""
import importlib.util
import json
import math
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('eq085_profile', Path(__file__).parents[1]/'tools/profile_pipeline.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ProfileEvidence(unittest.TestCase):
    def test_cumulative_recursion_is_not_additive_self_time(self):
        stats = {('/owner/private/site-packages/equity_features/session.py', 3, 'compute'):
                 (1, 3, .2, .9, {}), ('~', 0, 'extension'):(1, 1, .4, .4, {}),
                 ('/owner/private/site-packages/equity_feature_io_sdk/codec.py', 4, 'encode'):
                 (1, 1, .1, .5, {})}
        result = module.export_profile(stats)
        self.assertAlmostEqual(sum(result['category_self_seconds'].values()), .7)
        self.assertNotAlmostEqual(sum(r['cumulative_seconds'] for r in result['functions']), .7)
        self.assertTrue(result['cumulative_is_nonadditive'])
        self.assertEqual(result['functions'][0]['category'], 'opaque_native')
        self.assertNotIn('/owner/private', json.dumps(result))

    def test_distinct_unknown_module_identity_without_absolute_path(self):
        a = module.source_name('/private/a/shared.py')
        b = module.source_name('/private/b/shared.py')
        self.assertNotEqual(a, b)
        self.assertNotIn('/private', a+b)
        self.assertEqual(module.category(a, 'call'), 'unclassified')

    def test_nonfinite_negative_and_boolean_counts_rejected(self):
        for record in [(1, 1, math.nan, .1, {}), (1, 1, .1, -1, {}),
                       (True, 1, .1, .1, {}), (2, 1, .1, .1, {})]:
            with self.subTest(record=record), self.assertRaises(ValueError):
                module.export_profile({('~',0,'f'):record})

    def test_invalid_parity_elapsed_and_digest_are_rejected(self):
        good = dict(full_result_quality_evidence_input_parity=True,
                    independent_five_trade_goldens=True, pipeline_elapsed_ns=3,
                    content_sha256='a'*64, input_population={'f':{'sha256':'b'*64}},
                    rows=16, tasks=4, versions={'worker':'v1'})
        self.assertEqual(module.validate_sample(good, good), good)
        for change in ({'full_result_quality_evidence_input_parity':False},
                       {'pipeline_elapsed_ns':True}, {'pipeline_elapsed_ns':0},
                       {'content_sha256':'wrong'}, {'rows':17}, {'versions':{'worker':'v2'}}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                module.validate_sample(good | change, good)

    def test_new_outputs_do_not_overlap_or_replace(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            module.check_locations(root/'new', root/'report.json')
            existing = root/'existing';existing.mkdir()
            for work,out in [(existing,root/'out'),(root/'w',existing),
                             (root/'w',root/'w'/'out'),(module.ROOT/'work',root/'out')]:
                with self.subTest(work=work,out=out), self.assertRaises(ValueError):
                    module.check_locations(work,out)
            checkout=root/'checkout';checkout.mkdir();(checkout/'.git').write_text('gitdir: elsewhere')
            with self.assertRaises(ValueError):
                module.check_locations(checkout/'new',root/'out')

    def test_complete_report_exclusive_publication(self):
        with tempfile.TemporaryDirectory() as name:
            out=Path(name)/'report.json'
            module.write_report(out, {'complete':True,'samples':[1,2,3]})
            saved=out.read_bytes()
            with self.assertRaises(FileExistsError):
                module.write_report(out, {'different':True})
            self.assertEqual(out.read_bytes(),saved)
            self.assertEqual(list(out.parent.iterdir()),[out])
            with self.assertRaises(ValueError):
                module.write_report(out.parent/'bad.json', {'nan':math.nan})
            self.assertFalse((out.parent/'bad.json').exists())


if __name__ == '__main__':
    unittest.main()
