"""Validate owned whole-tree CPU and cleanup before any frozen workload timing."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from supplied_native import NativeFailure, run_owned


@unittest.skipUnless(os.name == 'nt', 'Windows accounting; no Linux performance claim')
class SuppliedNative(unittest.TestCase):
    def command(self, output, scenario):
        return [sys.executable, str(ROOT / 'tests/supplied_native_child.py'), '--job', '{job_name}',
            '--output', str(output), '--scenario', scenario]

    def test_no_child_and_short_lived_children_full_job_cpu(self):
        with tempfile.TemporaryDirectory(prefix='supplied-native-') as directory:
            startup_processes = None
            for scenario, count in (('simple', 1), ('children', 4)):
                output = Path(directory) / (scenario + '.json')
                report = run_owned(self.command(output, scenario), cwd=ROOT, timeout_s=30)
                self.assertEqual(report['exit_code'], 0)
                self.assertEqual(report['suspended_accounting']['total_processes'], 1)
                self.assertEqual(report['final_accounting']['active_processes'], 0)
                self.assertEqual(report['final_accounting']['terminated_by_limit'], 0)
                self.assertGreater(report['cpu_seconds'], 0)
                self.assertGreater(report['aggregate_sampled_peak_rss_bytes'], 0)
                self.assertGreater(report['peak_job_private_commit_bytes'], 0)
                data = json.loads(output.read_text(encoding='utf-8'))
                # Windows venv redirectors may form a real launcher chain. All
                # of them belong to the Job and count toward CPU/RSS/startup.
                self.assertIn(report['runner_pid'], report['pids_observed'])
                self.assertIn(data['pid'], report['pids_observed'])
                if scenario == 'simple':
                    self.assertEqual(data['children'], [])
                    startup_processes = report['final_accounting']['total_processes']
                    self.assertGreaterEqual(startup_processes, 1)
                self.assertEqual(report['final_accounting']['total_processes'], count * startup_processes)
                # Independent actual process CPU lower bound includes exited children.
                cpu = data['process_cpu_total_ns']
                for child_output in output.parent.glob(output.stem + '-child*.json'):
                    child = json.loads(child_output.read_text(encoding='utf-8'))
                    self.assertIn(child['pid'], report['pids_observed'])
                    cpu += child['process_cpu_total_ns']
                for launcher_pid in data['children']:
                    self.assertIn(launcher_pid, report['pids_observed'])
                self.assertGreaterEqual(int(report['cpu_seconds'] * 1e9), cpu)
                self.assertLessEqual(data['before']['user'] + data['before']['kernel'], data['after']['user'] + data['after']['kernel'])
                self.assertGreaterEqual(report['external_observer_cpu_ns'], 0)
                self.assertGreater(report['observer_clock_nominal_resolution_s'], 0)

    def test_assignment_and_resume_rejection_never_execute_runner(self):
        with tempfile.TemporaryDirectory(prefix='supplied-native-') as directory:
            for fault in ('reject_assignment', 'reject_resume'):
                output = Path(directory) / (fault + '.json')
                with self.assertRaises(NativeFailure) as caught:
                    run_owned(self.command(output, 'simple'), cwd=ROOT, timeout_s=5, **{fault: True})
                self.assertFalse(output.exists())
                self.assertNotIn('cleanup_failed', caught.exception.evidence)
                if fault == 'reject_resume':
                    self.assertEqual(caught.exception.evidence['cleanup_accounting']['active_processes'], 0)
                else:
                    self.assertTrue(caught.exception.evidence['cleanup_runner_reaped'])

    def test_orphan_after_runner_exit_is_owned_and_reaped(self):
        with tempfile.TemporaryDirectory(prefix='supplied-native-') as directory:
            output = Path(directory) / 'orphan.json'
            with self.assertRaises(NativeFailure) as caught:
                run_owned(self.command(output, 'orphan'), cwd=ROOT, timeout_s=0.7)
            self.assertEqual(caught.exception.code, 'OWNED_DEADLINE')
            self.assertTrue(output.exists())
            child = json.loads(output.read_text(encoding='utf-8'))
            self.assertEqual(len(child['children']), 1)
            self.assertTrue(caught.exception.evidence['runner_exited_before_failure'])
            self.assertEqual(caught.exception.evidence['runner_exit_code_before_failure'], 0)
            self.assertGreater(caught.exception.evidence['samples'][-1]['active_processes'], 0)
            self.assertTrue(caught.exception.evidence['cleanup_job_termination_issued'])
            self.assertEqual(caught.exception.evidence['cleanup_accounting']['active_processes'], 0)
            self.assertNotIn('cleanup_failed', caught.exception.evidence)

    def test_nonzero_runner_exit_is_retained_not_silently_successful(self):
        with tempfile.TemporaryDirectory(prefix='supplied-native-') as directory:
            output = Path(directory) / 'failure.json'
            report = run_owned(self.command(output, 'fail'), cwd=ROOT, timeout_s=5)
            self.assertEqual(report['exit_code'], 17)
            self.assertEqual(report['final_accounting']['active_processes'], 0)


if __name__ == '__main__':
    unittest.main()
