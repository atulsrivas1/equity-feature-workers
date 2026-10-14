"""Independent population corruption and conservative benchmark-decision fixtures."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from scaling_probe import sort_oracle, public_profile, expected_cell
from qualify_scaling import groups, scheduling_decision


SORTED = [
    (1, 'c4ca4238a0b923820dcc509a6f75849ba87ff679a2f3e71d9181a67b7542122c'),
    (2, 'c81e728d9d4c2f636f067f89cc14862ce4da3b7fbbce2345d7772b0674a318d5'),
    (0, 'cfcd208495d565ef66e7dff9f98764daeccbc87e4b5ce2fe28308fd9f2a7baf3'),
]


class ScalingEvidence(unittest.TestCase):
    def test_literal_hash_sorted_population(self):
        self.assertEqual(sort_oracle([SORTED[:1], SORTED[1:]], 3)['count'], 3)
        for invalid in [SORTED[:2], [SORTED[0], SORTED[0], SORTED[2]], list(reversed(SORTED)),
                        [(1, 'x'), *SORTED[1:]], [(3, SORTED[0][1]), *SORTED[1:]]]:
            with self.subTest(invalid=invalid), self.assertRaises(AssertionError):
                sort_oracle([invalid], 3)

    def test_current_projection_facts(self):
        a, b = expected_cell(17), expected_cell(81)
        self.assertEqual(a['instrument_id'], 'B0017')
        self.assertEqual(b['instrument_id'], 'B0017')
        self.assertEqual((a['result_ordinal'], b['result_ordinal']), (17, 81))
        self.assertEqual((a['value_int64'], a['quality_expected'], a['quality_observed']), (256, 256, 256))
        self.assertIsNone(a['value_float64'])
        self.assertEqual(a['quality_reasons'], [])

    def test_profile_owned_path_redaction(self):
        root = Path('/owned/scratch')
        self.assertEqual(public_profile({'extra': [str(root / 'A.parquet')]}, root),
                         {'extra': ['<owned-root>/A.parquet']})

    def test_group_median_and_incomplete_rejection(self):
        records = [dict(workload='small', backend='parquet', mode='sequential', workers=1, pipeline_elapsed_ns=n)
                   for n in (900, 100, 200)]
        result = groups(records)
        self.assertEqual(result[0]['median_ns'], 200)
        self.assertEqual(result[0]['min_ns'], 100)
        self.assertIsNone(scheduling_decision(result)['adopt_workers'])

    def test_all_populations_and_nonoverlap_required(self):
        records = []
        for workload in ('small', 'large', 'skew', 'capacity'):
            for backend in ('parquet', 'duckdb'):
                for mode, workers, elapsed in [('sequential', 1, 100), ('thread', 2, 80), ('thread', 4, 120)]:
                    records.append(dict(workload=workload, backend=backend, mode=mode, workers=workers,
                        samples=3, median_ns=elapsed, min_ns=elapsed-1, max_ns=elapsed+1))
        self.assertEqual(scheduling_decision(records)['adopt_workers'], 2)
        candidate = next(r for r in records if r['workload'] == 'capacity' and r['workers'] == 2)
        candidate['max_ns'] = 100
        self.assertIsNone(scheduling_decision(records)['adopt_workers'])
        candidate['max_ns'] = 81
        candidate['samples'] = 2
        self.assertIsNone(scheduling_decision(records)['adopt_workers'])


if __name__ == '__main__':
    unittest.main()
