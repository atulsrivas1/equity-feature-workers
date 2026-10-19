"""Closed13+5 error map, sanitized unexpected faults and actual supervisor reasons."""
from dataclasses import replace
import traceback
import unittest
from unittest.mock import patch

from equity_feature_contracts import ContractError, ErrorCode
from equity_feature_workers import (
    BoundedSupervisor, CommandError, CommandErrorCode, ManifestError, ManifestErrorCode, ResourceBudget,
)
from equity_feature_workers._supplied import translated
from test_supplied_session import original_work

SECRET = 'synthetic-private-error-marker'


class SuppliedErrors(unittest.TestCase):
    def translated_code(self, error):
        try:
            with translated():
                raise error
        except CommandError as actual:
            self.assertNotIn(SECRET, str(actual))
            self.assertNotIn(SECRET, ''.join(traceback.format_exception(actual)))
            return actual.code
        self.fail('no typed error')

    def test_exhaustive_thirteen_canonical_members(self):
        self.assertEqual(len(ErrorCode), 13)
        for code in ErrorCode:
            expected = CommandErrorCode.RESULT if code is ErrorCode.INCONSISTENT_IDENTITY else CommandErrorCode.CALCULATION if code is ErrorCode.OVERFLOW else CommandErrorCode.CONFIG
            self.assertEqual(self.translated_code(ContractError(code, SECRET)), expected)

    def test_exhaustive_five_manifest_members(self):
        self.assertEqual(len(ManifestErrorCode), 5)
        for code in ManifestErrorCode:
            expected = CommandErrorCode.LIMIT if code is ManifestErrorCode.LIMIT else CommandErrorCode.RESULT if code is ManifestErrorCode.BINDING else CommandErrorCode.CONFIG
            self.assertEqual(self.translated_code(ManifestError(code)), expected)

    def test_unexpected_and_unknown_code_fail_closed(self):
        self.assertEqual(self.translated_code(RuntimeError(SECRET)), CommandErrorCode.CALCULATION)
        for error in (ContractError(ErrorCode.INVALID_SCHEMA, SECRET), ManifestError(ManifestErrorCode.INVALID), CommandError(CommandErrorCode.CONFIG)):
            error.code = SECRET
            self.assertEqual(self.translated_code(error), CommandErrorCode.CALCULATION)

    def test_actual_callback_supervisor_translates_pure_faults(self):
        item = original_work()[0][0]
        for code in ErrorCode:
            expected = CommandErrorCode.RESULT if code is ErrorCode.INCONSISTENT_IDENTITY else CommandErrorCode.CALCULATION if code is ErrorCode.OVERFLOW else CommandErrorCode.CONFIG
            with patch('equity_feature_workers.supplied_session.compute_bars', side_effect=ContractError(code, SECRET)):
                result = BoundedSupervisor(budget=ResourceBudget(max_resident_bytes=134217728)).run((item,))
            self.assertEqual(result.tasks[0].reason, expected.value)
            self.assertIsNone(result.tasks[0].results)
            self.assertNotIn(SECRET, repr(result))

    def test_actual_task_byte_admission_is_resource_failure(self):
        item = original_work()[0][0]
        tiny = replace(item.task, max_input_bytes=1)
        with self.assertRaises(CommandError) as caught:
            item.calculate(tiny, item.batches)
        self.assertEqual(caught.exception.code, CommandErrorCode.LIMIT)


if __name__ == '__main__':
    unittest.main()
