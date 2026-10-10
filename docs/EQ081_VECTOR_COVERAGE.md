## EQ081 installed harness qualification rework - October10,2026 UTC

[Actual findings/correction disposition](https://github.com/atulsrivas1/equity-features/issues/91#issuecomment-6100731455).

Separate local automated reviewer /root/r7_policy_review inspected immutable worker19513121f4c64ae3d361f00d50a3a30741e1a630 / canonicalf6dce4180aaca68d3e2ecb4743345082095aa28b. Strict7 PASS, runtime unchanged from approvedeced. Complete40 independently FAILED:39passed/one failure6.154s; actual20ms remaining-budget sleep may wake before deadline, permitting second provider evaluation. Exact-one-provider assertion was timing-dependent, not evidence of runtime excess budget. P2 qualification fixture corrected to explicit controlled monotonic/sleep expiry after an actual first disconnected loopback POST; binary-exact31.25ms budget avoids float subtraction assertion error. Existing actual late-provider and late-decoder wall-time budget tests remain. First corrective fixture used20ms against1.0 baseline and asserted float subtraction<=.02; tiny representation difference raised test assertion inside closed public boundary, producing cancelled. That failed correction is preserved, not counted.

P2 schema-pin test read ROOT/packages/client/src from the intentionally package-free installed harness. Reviewer independently reproduced FileNotFoundError. Corrected importlib.resources.files('equity_feature_client') reads the actual bundled installed asset and preserves exact independent schema digests. Current author40 source tests PASS; runtime/package/build/workflow unchanged. The initial local195 builder FAILED in actual fresh installed wheel40 suite with missing schema resource and timing assertion, log eq081-client-1951312-build.log/session27923 exit1. Repeat archive builds and light absence stages passed before failure but no successful producer/receipt/form qualification claimed; partial output preserved. New corrected producer must qualify both forms and both OS.

Current correction requires renewed independent exact-head review. EQ081 remains In progress, six R7 accepted/four remaining. All32 remaining owner/rights/epoch/public-clientTLS and installed/artifact/main/release gates remain mandatory. Prior source36 review6100698801 remains scoped and valid; no acceptance from this correction.

Resume renewed exact correction review and fresh current client builder/WindowsLinux CI; inspect successful actual producers before archive/source/RECORD/native/publictyping/core/artifact claims. No client release acceptance.

# EQ081 current independent vector coverage

Development coverage only. A mapped case is not installed or release acceptance. [Separate 36-method source checkpoint review](https://github.com/atulsrivas1/equity-features/issues/91#issuecomment-6100698801).

| ID | Requirement | Actual evidence / remaining gap |
| --- | --- | --- |
| C01 | Optional package/local independence | Root imports and package graph checked; real fresh light/native qualification pending |
| C02 | Actual owned native HTTP | test_owned_http.test_admitted_audited_native_client_operations; Windows source PASS |
| C03 | Origin/authentication/provider | test_transport.test_origin_and_trust_configuration; test_client.test_bad_request_before_provider_and_late_provider_budget |
| C04 | Verified HTTPS | test_transport.test_actual_tls_trust_hostname_and_eof_faults internal actual TLS PASS; public client TLS pending |
| C05 | Auth denial/redaction | test_client.test_provider_secret_graph_not_retained; owned HTTP invalid credential401; authenticated foreign owner pending |
| C06 | Request bounds | test_models.test_exact_byte_limits_and_schema_pins; test_serialized_request_is_the_validated_snapshot |
| C07 | Response framing/bounds | test_transport actual EOF/header/body inclusive and one-over, missing EOF, critical duplicates |
| C08 | Parser hostility | test_models.test_hostile_json_before_validation; invalid precision/type cells |
| C09 | Response identity/null errors | test_client.test_success_identity_and_status_denials; test_exact_null_error_table_and_no_server_retry |
| C10 | Version inventory | Actual1.0 calculate/status/cancel/raw and1.1 result/artifact; incompatible/projection checks |
| C11 | Read-only retry | test_read_disconnect_retry_same_bytes_fresh_provider; test_retry_attempt_cap_and_budget_stop |
| C12 | Mutation ambiguity | test_mutations_not_replayed_and_unknown_once_sent; test_unverifiable_mutation_reply_is_unknown_without_replay |
| C13 | Cooperative deadlines | Provider expiry, new late decode expiry, retry budget stop; no hard DNS/OS wall deadline claim |
| C14 | No fallback/status identity | test_success_identity_and_status_denials; exact null-error/status category table |
| C15 | Exact nanoseconds/int64 | test_literal_source_and_exact_adjacent_scope; native raw5 exact1ns near9e18; reviewer min/max |
| C16 | Scalar precision | Native signedzero, decimal-scale denial, exact native4 bytes; reviewer int64/decimal128 bounds |
| C17 | Public raw conversion | test_raw5_complete_metadata_exact_values_and_ns; public CanonicalBatch/validate_batch |
| C18 | Required/optional columns | test_missing_required_columns_and_opaque_units_denied; raw5 optional absence preserved |
| C19 | Complete metadata | raw5 metadata equality; complete native4 original bytes; source/scope/backend/context pins |
| C20 | Producer conversion | test_native4_full_independent_bytes; no HTTP publication receipt invented |
| C21 | Structured quality/evidence | Complete native4 bytes; test_complete_producer_coverage_source_and_evidence_denials |
| C22 | Projection versus full producer | test_job_digest_attachment_and_projection; reviewer full-native conversion refusal for FeatureSliceView |
| C23 | Actual state/explicit polling | Owned native calculate/status/cancel operations, explicit bounded test polling |
| C24 | Idempotency/epoch | Stable calculate key and no replay; explicit epoch/restart response cases pending |
| C25 | Cancel semantics | Owned cancel on succeeded job returns actual succeeded state; no timeout rollback or automatic cancel |
| C26 | Retention/rights | Client has no cache/TTL refresh; actual authenticated revoke/expiry/no-resurrection qualification pending |
| C27 | Foreign owner | Current owned HTTP test is invalid credential401, not authenticated foreign-owner qualification; pending |
| C28 | Controlled attachment | test_job_digest_attachment_and_projection; actual artifact SHA filename; no automatic writes |
| C29 | Data-only/calculation boundaries | Fixed constructors/closed schema, no service import in runtimeclient; accepted calculations unchanged |
| C30 | Resource/failure cleanup | Provider weakref privacy; real connect interrupt/TLS cleanup; new actual blocked read close and late decode close |
| C31 | Independent kernel parity | NEW test_converted_trades_local_kernel_matches_independent_literals: actual public compute_trades count3/volume10/notional1011/full input metadata |
| C32 | Final delivery gates | New committed client builder/workflow introduced; execution/WindowsLinux artifacts/review/main acceptance pending |
