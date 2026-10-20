"""Machine-readable actual test/skip ledger for fresh supplied-input installs."""
import json
import os
from pathlib import Path
import sys
import unittest


def main():
    tests = Path(sys.argv[1]); combined = sys.argv[3] == '1'
    sys.path.insert(0, str(tests))
    suite = unittest.TestSuite([
        unittest.defaultTestLoader.discover(str(tests), pattern='test_supplied_*.py'),
        unittest.defaultTestLoader.discover(str(tests), pattern='test_eq142_*.py')])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    skipped = [{'test': case.id(), 'reason': reason} for case, reason in result.skipped]
    allowed = []
    for item in skipped:
        identity = item['test']
        if (item['reason'].startswith('EQ142 development CPU capacity requires ') and
                identity.startswith(('test_eq142_admission.ActualAdmission.',
                    'test_eq142_sample.DevelopmentSample.','test_eq142_sample.NativeOwnedDevelopmentSample.',
                    'test_eq142_outcome.ActualOutcomeLedger.','test_eq142_outcome.ActualOutcomeFailureRetention.',
                    'test_eq142_owned_witness.OwnedWitnessIntegration.','test_eq142_owned_witness.NativeOwnedWitnessIntegration.',
                    'test_eq142_executor.ExecutorObservations.','test_eq142_publication.ActualPublicationObservation.',
                    'test_eq142_capacity.WindowsCapacity.',
                    'test_eq142_owned_runner.ActualOwnedRunner.',
                    'test_eq142_preparation.ActualPreparation.',
                    'test_eq142_code_counts.ActualOriginalCountParity.test_actual_harness_thread_and_spawn_profile_counts'))):
            # Capacity skips remain explicit ledger evidence, never full mode
            # qualification. Optional absence uses the separate checks below.
            continue
        assert (not combined and identity.startswith(('test_supplied_intraday.',
            'test_eq142_graph.','test_eq142_trace.','test_eq142_delegate.','test_eq142_workloads.',
            'test_eq142_evidence.OriginalProductWitnesses.','test_eq142_admission.','test_eq142_causal.',
            'test_eq142_sample.','test_eq142_code_counts.ActualOriginalCountParity.',
            'test_eq142_outcome.ActualOutcomeLedger.','test_eq142_outcome.ActualOutcomeFailureRetention.',
            'test_eq142_owned_witness.OwnedWitnessIntegration.','test_eq142_owned_witness.NativeOwnedWitnessIntegration.',
            'test_eq142_executor.ExecutorObservations.','test_eq142_publication.ActualPublicationObservation.',
            'test_eq142_owned_runner.ActualOwnedRunner.','test_eq142_preparation.ActualPreparation.'))) or (
            os.name != 'nt' and identity.startswith(('test_supplied_native.','test_eq142_native.',
                'test_eq142_sample.NativeOwnedDevelopmentSample.',
                'test_eq142_identity.WindowsOwnedWitness.','test_eq142_owned_witness.NativeOwnedWitnessIntegration.',
                'test_eq142_native_closure.WindowsNativeClosure.','test_eq142_capacity.WindowsCapacity.',
                'test_eq142_owned_runner.ActualOwnedRunner.'))), ('unexpected skip', item)
        allowed.append(identity)
    assert any(name.startswith('test_supplied_intraday.') for name in allowed) == (not combined)
    assert any(name.startswith('test_supplied_native.') for name in allowed) == (os.name != 'nt')
    report = {'methods': result.testsRun, 'skipped': skipped, 'failures': len(result.failures),
        'errors': len(result.errors), 'successful': result.wasSuccessful()}
    Path(sys.argv[2]).write_bytes((json.dumps(report, sort_keys=True, indent=2)+'\n').encode())
    assert result.wasSuccessful()


if __name__ == '__main__':
    main()
