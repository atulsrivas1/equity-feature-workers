"""Execute every published pre-code decision through named independent fixtures."""
import json
from pathlib import Path
import unittest
import test_service as fixtures

MAPPING = {
    'owned_raw':'test_literal_precision_null_and_native_metadata',
    'owned_feature':'test_actual_native_feature_quality_metadata_and_version',
    'anonymous':'test_anonymous_foreign_and_bad_tokens_do_not_read',
    'duplicate_bearer':'test_actual_loopback_http_and_duplicate_credentials',
    'unknown_token':'test_anonymous_foreign_and_bad_tokens_do_not_read',
    'grant_expiry_equal':'test_expiry_and_revocation',
    'foreign_principal':'test_anonymous_foreign_and_bad_tokens_do_not_read',
    'raw_without_raw_right':'test_raw_right_and_discovery_are_independently_required',
    'feature_without_derived_right':'test_derived_slice_requires_derived_right',
    'missing_discover':'test_raw_right_and_discovery_are_independently_required',
    'revision_changed':'test_revision_scope_columns_and_injection_denied_before_read',
    'scope_one_ns_outside':'test_revision_scope_columns_and_injection_denied_before_read',
    'unknown_or_injected_column':'test_revision_scope_columns_and_injection_denied_before_read',
    'non_null_cursor':'test_scope_cursor_and_non_slice_operations',
    'unsupported_operation':'test_scope_cursor_and_non_slice_operations',
    'rows_100':'test_row_100_and_changed_producer_101_without_partial_output',
    'rows_101':'test_row_100_and_changed_producer_101_without_partial_output',
    'body_16385':'test_strict_json_versions_direction_and_body_limits',
    'response_262145':'test_response_actual_262144_and_262145_bytes',
    'request_61_same_interval':'test_policy_window_cannot_be_shortened',
    'transfer_one_byte_excess':'test_transfer_exact_boundary_and_one_byte_excess',
    'fresh_controller_same_ledger':'test_shared_request_limits_and_interval_reset',
    'revoke_before_publication':'test_revocation_wins_barrier_before_visibility',
    'bad_source_identity':'test_current_source_content_and_full_binding_tamper',
    'bad_native_result':'test_forged_native_result_fails_registration',
    'multiprocess_wsgi':'test_untrusted_clock_and_multiprocess_or_foreign_peer_denied',
    'token_expiry_equal':'test_expiry_and_revocation',
    'feature_via_1_0':'test_actual_native_feature_quality_metadata_and_version',
    'feature_via_1_1':'test_actual_structured_interval_and_nested_quality',
    'raw_strict_subset_scope':'test_revision_scope_columns_and_injection_denied_before_read',
    'http_correlation_changes':'test_http_correlation_does_not_change_native_source',
    'chunk_binding_as_dataset':'test_current_source_content_and_full_binding_tamper',
    'native_mapping_suffix_changed':'test_current_source_content_and_full_binding_tamper',
    'producer_rows_or_receipt_changed':'test_producer_receipt_and_mapping_tamper_redacted',
}


class FrozenDecisionCoverage(unittest.TestCase):
    def test_all_34_frozen_decisions_execute(self):
        path=Path(__file__).parent/'fixtures/service_decisions.json'
        frozen=json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual({v['id'] for v in frozen['vectors']},set(MAPPING))
        self.assertEqual(len(frozen['vectors']),34)
        for vector in frozen['vectors']:
            with self.subTest(decision=vector['id'],expected=vector['expected']):
                case=fixtures.ServiceVectors(MAPPING[vector['id']])
                result=unittest.TestResult()
                case.run(result)
                self.assertEqual(result.testsRun,1)
                self.assertEqual(result.failures+result.errors,[])
        oracle=frozen['owned_raw_oracle']
        self.assertEqual(oracle['event_ns'],['9007199254740992','9007199254740993'])
        self.assertEqual(oracle['price_i64'],['10100','10200'])
        self.assertEqual(oracle['size_i64'],['2','3'])


if __name__ == '__main__': unittest.main()
