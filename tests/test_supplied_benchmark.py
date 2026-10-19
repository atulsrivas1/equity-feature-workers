"""Frozen protocol/noise/resource controls; these do not execute real timing."""
import math
import json
import tempfile
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'tools'))
from benchmark_supplied_inputs import (decision, distribution, original_oracle, freeze_baseline, digest,
    bound_expected, bound_runtime, complete_sample, wire, PROFILES, REPEATS, SERIES, WARMUPS)


class SuppliedBenchmark(unittest.TestCase):
    def summary(self, wall=10, cpu=10, rss=100, p95=10, noise=.05):
        return {'median_wall_seconds': wall, 'median_cpu_seconds': cpu, 'max_sampled_rss_bytes': rss,
            'p95_wall_seconds': p95, 'noise_improvement_floor': noise}

    def test_original_expanded_oracle_six_slot_hand_values(self):
        result = original_oracle((10, 12, 11, 15, 14, 16), 3)
        self.assertEqual(len(result), 8)
        self.assertAlmostEqual(result['intraday.ema'], 59/4, delta=1e-12)
        self.assertAlmostEqual(result['intraday.rsi'], 1050/13, delta=1e-12)
        self.assertAlmostEqual(result['intraday.atr'], 116/27, delta=1e-12)
        self.assertEqual(result['intraday.band_middle'], 15.)
        self.assertAlmostEqual(result['intraday.band_width_ratio'], 4*math.sqrt(2/3)/15, delta=1e-15)
        self.assertAlmostEqual(result['intraday.band_position'], .5+1/(4*math.sqrt(2/3)), delta=1e-15)

    def test_exact_workloads_counts_and_separate_series(self):
        self.assertEqual(PROFILES, {'small': (256,), 'balanced': (1024,)*16, 'skewed': (256,)*15+(1024,)})
        self.assertEqual((WARMUPS, REPEATS, SERIES), (2, 7, ('compute', 'e2e')))

    def test_inclusive_iqr_nearest_rank_p95_and_no_warmup_in_distribution(self):
        def sample(n):
            return {'wall_seconds': n, 'cpu_seconds': n*2,
                'native': {'aggregate_sampled_peak_rss_bytes': n*10}}
        result = distribution([sample(n) for n in range(1, 8)])
        self.assertEqual((result['median_wall_seconds'], result['iqr_seconds'], result['p95_wall_seconds']), (4, 3, 7))
        self.assertEqual((result['median_cpu_seconds'], result['max_sampled_rss_bytes']), (8, 70))
        self.assertEqual(result['noise_improvement_floor'], 9/4)
        with self.assertRaises(AssertionError): distribution([sample(n) for n in range(9)])

    def test_strict_noise_threshold_and_sequential_default(self):
        a = self.summary(); b = self.summary(wall=9.5)
        result = decision(a, b, 1000, a, b, True)
        # A mathematical 5% boundary must never acquire a binary-rounding win.
        self.assertFalse(result['adopted'])
        b = self.summary(wall=9.49)
        result = decision(a, b, 1000, a, b, True)
        self.assertTrue(result['adopted'])
        self.assertEqual(result['fallback'], 'sequential'); self.assertFalse(result['default_mode_changed'])

    def test_resource_and_small_tail_failures_remain_nonwins(self):
        a = self.summary(); fast = self.summary(wall=8)
        for b, cap, small, valid in [
            (self.summary(wall=8, cpu=12.51), 1000, fast, True),
            (self.summary(wall=8, rss=126), 1000, fast, True),
            (fast, 99, fast, True), (fast, 1000, self.summary(p95=11.01), True),
            (fast, 1000, fast, False)]:
            self.assertFalse(decision(a, b, cap, a, small, valid)['adopted'])

    def test_zero_cpu_baseline_cannot_establish_cpu_adoption(self):
        a = self.summary(cpu=0); b = self.summary(wall=8, cpu=0)
        self.assertFalse(decision(a, b, 1000, a, b, True)['adopted'])

    def test_partial_or_candidate_contaminated_baseline_cannot_freeze(self):
        samples = [{'wall_seconds': 1, 'cpu_seconds': 1,
            'native': {'aggregate_sampled_peak_rss_bytes': 10}} for _ in range(7)]
        cohort = {'samples': samples, 'warmups': [{}, {}], 'distribution': distribution(samples)}
        record = {'candidates': {}, 'baseline': {}, 'expected_sha256': 'a'*64, 'machine': {}}
        with tempfile.TemporaryDirectory() as name:
            folder = Path(name)
            with self.assertRaises(AssertionError): freeze_baseline(record, folder, 'b'*40)
            self.assertEqual(list(folder.iterdir()), [])
            record['baseline'] = {s+'-'+p: cohort for s in SERIES for p in PROFILES}
            record['candidates'] = {'first': {}}
            with self.assertRaises(AssertionError): freeze_baseline(record, folder, 'b'*40)
            self.assertEqual(list(folder.iterdir()), [])
            record['candidates'] = {}
            frozen = freeze_baseline(record, folder, 'b'*40)
            self.assertEqual((folder/'baseline.json').read_bytes(), frozen)
            self.assertEqual(json.loads((folder/'baseline-sha256.json').read_bytes())['sha256'], digest(frozen))
            with self.assertRaises(AssertionError): freeze_baseline(record, folder, 'b'*40)
            self.assertEqual((folder/'baseline.json').read_bytes(), frozen)

    def test_expected_file_bytes_and_installed_map_are_bound(self):
        reference = {'runtime': {'selected': {'sha256': 'original'}}}
        with tempfile.TemporaryDirectory() as name:
            path = Path(name)/'expected.json'; path.write_bytes(wire(reference))
            frozen = digest(path.read_bytes())
            self.assertEqual(bound_expected(path, frozen), reference)
            with patch('benchmark_supplied_inputs.installed_runtime', return_value=reference['runtime']):
                self.assertEqual(bound_runtime(reference['runtime']), digest(wire(reference['runtime'])))
            with patch('benchmark_supplied_inputs.installed_runtime', return_value={'changed': {}}):
                with self.assertRaises(AssertionError): bound_runtime(reference['runtime'])
            path.write_bytes(wire(reference)+b' ')
            with self.assertRaises(AssertionError): bound_expected(path, frozen)

    def test_post_phase_binding_failure_preserves_actual_phase_evidence(self):
        reference = {'runtime': {'selected': {'sha256': 'original'}}}
        with tempfile.TemporaryDirectory() as name:
            folder = Path(name); expected = folder/'expected.json'; expected.write_bytes(wire(reference))
            frozen = digest(expected.read_bytes()); runtime_sha = digest(wire(reference['runtime']))
            for fault in ('runtime', 'expected', 'none'):
                with self.subTest(fault=fault):
                    expected.write_bytes(wire(reference))
                    if fault == 'expected': expected.write_bytes(b'changed')
                    result = {'status': 'phase_complete_pending_runtime', 'phase_wall_ns': 123,
                        'phase_cpu_seconds': .125, 'stable': [{'task_sha256': 'a'*64, 'result_sha256': 'b'*64}]}
                    path = folder/(fault+'.json')
                    installed = {'changed': {}} if fault == 'runtime' else reference['runtime']
                    with patch('benchmark_supplied_inputs.installed_runtime', return_value=installed):
                        if fault == 'none':
                            complete_sample(path, result, reference, expected, frozen, runtime_sha)
                        else:
                            with self.assertRaises(AssertionError):
                                complete_sample(path, result, reference, expected, frozen, runtime_sha)
                    saved = json.loads(path.read_bytes())
                    self.assertEqual((saved['phase_wall_ns'], saved['phase_cpu_seconds'], saved['stable']),
                        (123, .125, result['stable']))
                    self.assertEqual(saved['status'], 'completed' if fault == 'none' else 'failed_after_phase')


if __name__ == '__main__':
    unittest.main()
