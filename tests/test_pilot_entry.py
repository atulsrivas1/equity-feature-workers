"""Pure API doubles for identity-bound qualification observation; no process launch."""
import unittest
from unittest.mock import Mock
from pilot_entry import retain_entry, settle_entry, finish_entry_observation


def kernel():
    result = Mock()
    result.status = 259
    result.creation = 11
    result.OpenProcess.return_value = 101
    result.GetProcessId.return_value = 42
    result.CloseHandle.return_value = True
    result.WaitForSingleObject.return_value = 0
    def membership(handle, job, present):
        present._obj.value = 1
        return True
    def times(handle, created, *other):
        created._obj.dwLowDateTime = result.creation
        created._obj.dwHighDateTime = 0
        return True
    def status(handle, output):
        output._obj.value = result.status
        return True
    result.IsProcessInJob.side_effect = membership
    result.GetProcessTimes.side_effect = times
    result.GetExitCodeProcess.side_effect = status
    return result


class EntryObservation(unittest.TestCase):
    def test_cleanup_description_cannot_replace_primary(self):
        class BadDescription(OSError):
            def __str__(self):raise MemoryError('unavailable diagnostic')
        api = kernel();api.CloseHandle.side_effect = BadDescription()
        process = Mock();process.communicate.side_effect = BadDescription()
        owner = Mock(kernel=api, process=process);owner.close.side_effect = BadDescription()
        primary = ValueError('original validation failure')
        finish_entry_observation(owner, process, (101, 42, 11), primary)
        self.assertEqual(str(primary), 'original validation failure')
        self.assertEqual(primary.__notes__, ['owned qualification cleanup: BadDescription'] * 3)
        api.GetProcessId.return_value = 43
        with self.assertRaisesRegex(OSError, 'PID changed') as caught:retain_entry(api, 7, 42)
        self.assertEqual(caught.exception.__notes__, ['observer acquisition cleanup: BadDescription'])

    def test_note_failure_cannot_replace_primary_or_first_cleanup(self):
        class BadNote(OSError):
            def add_note(self, note):raise MemoryError('unavailable annotation')
        api = kernel();api.CloseHandle.return_value = False
        process = Mock();process.communicate.side_effect = OSError('pipe close failed')
        owner = Mock(kernel=api, process=process);first = BadNote('first cleanup')
        owner.close.side_effect = first
        primary = BadNote('original validation failure')
        finish_entry_observation(owner, process, (101, 42, 11), primary)
        self.assertEqual(str(primary), 'original validation failure')
        with self.assertRaises(BadNote) as caught:
            finish_entry_observation(owner, process, (101, 42, 11), None)
        self.assertIs(caught.exception, first)
        self.assertEqual(api.CloseHandle.call_count, 2)

    def test_exact_handle_identity_and_remaining_budget(self):
        api = kernel()
        entry = retain_entry(api, 7, 42)
        self.assertEqual(entry, (101, 42, 11))
        api.OpenProcess.assert_called_once_with(0x101000, False, 42)
        self.assertEqual(api.IsProcessInJob.call_args.args[:2], (101, 7))
        api.status = 1
        self.assertEqual(settle_entry(api, entry, 5, clock=iter([4, 4.5]).__next__), 1)
        api.WaitForSingleObject.assert_called_once_with(101, 1000)
        api.CloseHandle.assert_not_called()

    def test_invalid_pid_never_opens(self):
        for pid in [True, False, 0, -1, 2**32, '42', None]:
            with self.subTest(pid=pid):
                api = kernel()
                with self.assertRaises(ValueError):retain_entry(api, 7, pid)
                api.OpenProcess.assert_not_called()

    def test_acquisition_failures_close_observer_and_deny(self):
        for failure in ['pid', 'membership', 'times', 'creation', 'exit_query', 'not_live']:
            with self.subTest(failure=failure):
                api = kernel()
                if failure == 'pid':api.GetProcessId.return_value = 43
                if failure == 'membership':api.IsProcessInJob.side_effect = lambda *args: False
                if failure == 'times':api.GetProcessTimes.side_effect = lambda *args: False
                if failure == 'creation':api.creation = 0
                if failure == 'exit_query':api.GetExitCodeProcess.side_effect = lambda *args: False
                if failure == 'not_live':api.status = 1
                with self.assertRaises(OSError):retain_entry(api, 7, 42)
                api.CloseHandle.assert_called_once_with(101)

    def test_missing_original_job_never_opens(self):
        for job in [None, 0, -1, True]:
            with self.subTest(job=job):
                api = kernel()
                with self.assertRaises(ValueError):retain_entry(api, job, 42)
                api.OpenProcess.assert_not_called()

    def test_acquisition_primary_preserved_if_close_fails(self):
        api = kernel();api.GetProcessId.return_value = 43;api.CloseHandle.return_value = False
        with self.assertRaisesRegex(OSError, 'PID changed') as caught:retain_entry(api, 7, 42)
        self.assertIn('observer acquisition cleanup', caught.exception.__notes__[0])

    def test_wait_timeout_failure_identity_exit_and_deadline_deny(self):
        for failure in ['timeout', 'wait_failed', 'wait_bool', 'pid', 'creation', 'exit_query', 'still_active', 'late', 'backward', 'nonfinite', 'clock_bool']:
            with self.subTest(failure=failure):
                api = kernel();entry = retain_entry(api, 7, 42);api.status = 1
                readings = [4, 4.5]
                if failure == 'timeout':api.WaitForSingleObject.return_value = 258
                if failure == 'wait_failed':api.WaitForSingleObject.return_value = 0xffffffff
                if failure == 'wait_bool':api.WaitForSingleObject.return_value = False
                if failure == 'pid':api.GetProcessId.return_value = 43
                if failure == 'creation':api.creation = 12
                if failure == 'exit_query':api.GetExitCodeProcess.side_effect = lambda *args: False
                if failure == 'still_active':api.status = 259
                if failure == 'late':readings = [4, 5.1]
                if failure == 'backward':readings = [4, 3]
                if failure == 'nonfinite':readings = [float('nan')]
                if failure == 'clock_bool':readings = [True]
                with self.assertRaises(OSError):settle_entry(api, entry, 5, clock=iter(readings).__next__)

    def test_expired_budget_never_waits(self):
        api = kernel();entry = retain_entry(api, 7, 42)
        with self.assertRaises(OSError):settle_entry(api, entry, 5, clock=lambda: 6)
        api.WaitForSingleObject.assert_not_called()

    def test_cleanup_attempts_every_scope_and_preserves_primary(self):
        api = kernel();api.CloseHandle.return_value = False
        process = Mock();process.communicate.side_effect = OSError('pipe close failed')
        owner = Mock(kernel=api, process=process);owner.close.side_effect = OSError('owner close failed')
        primary = ValueError('original validation failure')
        finish_entry_observation(owner, process, (101, 42, 11), primary)
        owner.close.assert_called_once_with(failed=True)
        process.communicate.assert_called_once_with(timeout=5)
        api.CloseHandle.assert_called_once_with(101)
        self.assertEqual(len(primary.__notes__), 3)
        self.assertEqual(str(primary), 'original validation failure')

    def test_cleanup_unknown_is_failure_without_primary(self):
        api = kernel();api.CloseHandle.return_value = False
        owner = Mock(kernel=api, process=None)
        with self.assertRaisesRegex(OSError, 'observer close failed'):
            finish_entry_observation(owner, None, (101, 42, 11), None)
        owner.close.assert_called_once_with(failed=True)


if __name__ == '__main__':unittest.main()
