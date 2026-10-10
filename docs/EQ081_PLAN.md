# EQ081 optional Python remote client: pre-code plan

Canonical story [EQ081 #91](https://github.com/atulsrivas1/equity-features/issues/91), epic E10#84, milestone R7#8. Provisional complexity:13 points, not days. Preparation only: Backlog/unassigned; no client implementation, Ready/start admission or runtime approval. EQ075-080 are accepted prerequisites. [EQ080 final gate](https://github.com/atulsrivas1/equity-features/issues/90#issuecomment-6099827389), [acceptance](https://github.com/atulsrivas1/equity-features/issues/90#issuecomment-6099830029), [Done readback](https://github.com/atulsrivas1/equity-features/issues/90#issuecomment-6099831515) supersede earlier pending captures. Current accepted mains canonicaleb645ea50bebb6da4a396e95036c98a624024980/workerbafc86a0305621fdb0206fb89753475186954ac3. Receipt identities remain frozen; no acceptance-only receipt cycle.

## Intended component and boundaries

Create optional equity-feature-client0.1.0a0 in worker companion packages/client, independently packaged. Authentication, HTTP, retries and pure batch conversion live here, outside contracts/features/worker/I/O runtime. Local users need no client. Light transport uses standard-library networking and the accepted pinned schema validator; a native extra supplies accepted public I/O SDK/core contracts. No runtime import/dependency on the service package. Bundled accepted1.0/1.1 schemas are byte-pinned data assets; no new wire version or server change is proposed. Public API and exact dependency metadata are frozen before runtime. Use actual /v1/request POST application/json, not guessed endpoints. No Arrow wire/zero-copy, performance, hosted/private/provider or registry/stable publication claim; future architecture Arrow examples are not implemented transport.

No mathematics changes: use original formulas/units/timing/coverage and unchanged local kernels. Exact int64/ns signed decimal strings, decimal128 coefficients and finite IEEE float bits remain exact; no timestamp conversion, price rescaling, interpolation or recomputed quality. Pure conversion uses existing public CanonicalBatch/Column/BatchMetadata and validate_batch, with closed explicit public record/scalar constructors. Missing required columns deny conversion, optional absent columns stay absent. Retain projection separately and preserve original metadata; do not invent acquisition receipts or full-producer completeness from a projected view. Native derived results use public SDK decoders/content/receipt checks and complete original producer/config/quality/evidence, never arbitrary class loading. Freeze actual raw and result field mappings/identity checks with native fixtures before Ready.

## Transport, authentication and bounded retries

Trusted caller selects one fixed origin, HTTPS with ordinary certificate verification or literal loopback HTTP for owned qualification. No URL userinfo/query/fragment, redirects, ambient proxies/netrc, remote-selected origin/path or environment token fallback. Explicit token provider invoked per attempt, bounded printable ASCII token without whitespace/control injection; exact token limit and exception redaction are frozen before Ready. Errors expose closed category/code/status only, not caller/server text, raw bodies or credentials. No client cache or implicit capability/rights upgrade.

Request<=16384bytes and response<=262144bytes, closed1.0/1.1 validation, UTF8/duplicate-key/nonfinite/depth32/node10000 defenses. Successful response version/correlation/kind must match request. The accepted service can emit1.0/null-correlation pre-ingress authentication/quota errors even for1.1 requests; freeze these exact denial exceptions rather than broadly relaxing success checks. Verify complete HTTP framing/media type/no-store expectations; abandoned protected chunks can produce empty/truncated200 and must never be exposed as valid results. Attachment filename is controlled opaque-ID SHA256, no local path write.

Retry allowlist is explicit read operations discover/slice/job_status/result_read/artifact_read, never calculate or job_cancel. At most3 attempts with unchanged request bytes/correlation, bounded delays<=5s, peroperation timeout<=5s and cooperative monotonic budget<=15s. No hard wallclock guarantee for DNS/OS calls; checks deny late success and do not create hidden threads. Freeze exact transient/status/Retry-After grammar and transport fault classification before Ready. Authorization/schema/integrity/expiry failures do not auto refresh, downgrade or retry. Interrupted mutation reports outcome_unknown; caller owns any explicit follow-up and stable key. No invented idempotency lookup endpoint, durable epoch recovery, automatic resubmit, server rollback/cancel or refunded quota. Polling and cancellation are explicit.

## Required public feasibility and independent decisions before Ready

1. Freeze actual request/helper API, operation-to-kind/version/status/correlation mapping and immutable result wrappers from accepted schemas and public source. No server private codec import in client runtime.
2. Prove closed scalar/record reconstruction against current public CanonicalBatch and public SDK APIs, complete source/scope/projection metadata checks and original producer/receipt wire mapping. No raw acquisition-receipt proof is invented when raw transport omits it.
3. Freeze owned independent full raw batch/result references for trades/bars/quotes, adjacent large ns, unavailable/structured quality and scalar edges before client runtime. Existing accepted four-producer fixture is a reference, not new client qualification.
4. Freeze supported input/feature inventory, minimal/native extra dependencies and package/type/export boundaries. Unknown kinds or unavailable native conversion capability fail explicitly; no unqualified RequiredCommandSpec history.
5. Freeze exact credential limits, strict HTTP framing, TLS/redirect/proxy behavior, retry matrix/backoff/deadline/cancellation and cleanup fault cases. Cooperative timeout limitations remain explicit.
6. Prove a bounded owned native HTTP service entry using accepted OwnedEpochSupervisor/immutable inventory and mandatory OwnedAudit, plus separate owned protocol/TLS fault servers. A mocked-only transport or development auditNone does not establish installed native client/service qualification.
7. Publish decisions/fixtures and separate concrete planning findings disposition, then guard exact Project91 Backlog->Ready->In progress before implementation. Remaining preparation is routine evidence, not missing owner operational authorization.

## Independent decision requirements (not executed tests)

| ID | Required evidence |
| --- | --- |
| C01 optional installation | Local contracts/features work and have no client/network dependency; light client imports without native extra. |
| C02 actual owned HTTP | BothOS installed client talks to the accepted native service in an admitted owned epoch with mandatory audit; no mocked-only end-to-end claim. |
| C03 origin and credentials | Fixed /v1/request, explicit token provider; no URL userinfo/query/fragment, CRLF credentials, redirects, ambient proxies or netrc. |
| C04 HTTPS trust | Default certificate verification and owned local TLS success/failure; no verify=False or external host qualification. |
| C05 authentication failures | 401/403 do not refresh or retry automatically; error repr/cause/logs never expose token, URL credentials or body. |
| C06 request bounds | 16384bytes inclusive/one byte over; closed data-only schema; invalid request denied before token provider/network. |
| C07 response bounds | 262144bytes inclusive/one byte over, complete framing, partial/empty/truncated success denied, close response on every path. |
| C08 parser hostility | Duplicate keys, invalid UTF8, nonfinite constants, excessive nesting/node counts, unknown fields/tags/versions denied. |
| C09 correlation | Successful version/request_id/kind must match request; wrong nonnull error correlation denied; exact pre-ingress error exceptions frozen. |
| C10 versions | 1.0 slice/discovery compatibility,1.1 native/job/results; 1.0 cannot invoke1.1 operations; no implicit downgrade. |
| C11 safe retry | Bounded attempts only on an explicit read-operation allowlist; unchanged request bytes/correlation, fresh token each attempt. |
| C12 mutation ambiguity | calculate/job_cancel are never automatically replayed; interrupted mutation reports outcome_unknown, no new key or implicit reexecution. |
| C13 retry budget | 1-3 attempts, peroperation timeout<=5s, cooperative total budget<=15s, bounded retry delay<=5s; no hard DNS/OS wallclock claim. |
| C14 status and denial | Only frozen transport/quota/server transient cases retry; invalid schema/integrity/authorization/expiry never trigger fallback. |
| C15 large timestamps | Adjacent ns near9e18 and both int64 boundaries stay exact signed decimal integers, no datetime/float narrowing. |
| C16 scalar fidelity | Decimal128 coefficients and float64 finite bits including signed zero/null/bool/string/list retain exact types and units. |
| C17 raw batch conversion | Public CanonicalBatch/Column/BatchMetadata plus validate_batch; source/scope/data_kind/projection match pinned expectations. |
| C18 missing columns | Missing required columns refuse batch conversion; optional absent columns stay absent, never fabricated null/zero. |
| C19 complete metadata | Original source binding including original-read/retained-map suffixes, units, coverage, sampling, adjustments and intervals preserved. |
| C20 derived integrity | Public SDK decode_result/decode_envelope/decode_receipt/verify_content/verify_receipt and captured receipt SHA bind complete original producer. |
| C21 derived structure | Frozen trades/bars/quotes and unavailable/structured quality/evidence/context match independent complete producer references. |
| C22 projection safety | Feature projection cannot silently change full native producer/config/quality inventory; incompatible or unqualified forms refused. |
| C23 job lifecycle | Actual native coarse states preserved, no invented percentages/stages; explicit polling only, no hidden background threads. |
| C24 idempotency and epoch | Caller retains stable key; no claim of durable restart recovery or safe automatic resubmit after unknown/old job denial. |
| C25 cancellation | Explicit cancel only; client timeout/close never implies server rollback, cancellation or refunded quota. |
| C26 result retention | Original half-open TTL/grant retirement/revocation and current derived_read/retain/export rules; reads do not refresh expiry. |
| C27 foreign owner | Second principal cannot read first result/artifact; no client cache, crossowner reuse or capability upgrade. |
| C28 attachments | Controlled ASCII opaque-ID hash filename verified; bytes remain bounded in memory, no caller/remote filesystem path or auto download write. |
| C29 data-only boundary | No arbitrary Python class loading/pickle/SQL/code/server adapter or sink selection; pure validation/conversion outside calculations. |
| C30 closed resources | Read/decode/token/timeout/close failures leave no open sockets, hidden worker threads or retained credential/body logs. |
| C31 independent numerics | Converted input through unchanged local kernels matches frozen native outputs; independent trades count3/volume10/notional1011 and full metadata. |
| C32 release qualification | Exact source/native/security/docs review, currentWinLinux freshwheel-sdist light/native/typing/consumer/RECORD/core invariance, source/archive/ZIP proofs and actual-main readback before Done. |

## Delivery sequence and accepted end state

First publish this paired plan and continuity; next freeze the seven public-feasibility prerequisites and owned fixtures, obtain separate planning review, publish Ready/start admission. Then implement one story, map all32 requirements to independent tests, document API/examples/decisions/changelog/knowledge/failures and renew exact final-head review after corrections. Qualify bothOS fresh wheel/sdist light/native forms, installed typing/public consumers/RECORD/dependency/source/core invariance; guarded paired publication and independent actual-main source/UTF8/Atul attribution/currentartifact proof precede Released/Done and criterion closure. Preserve accepted service/worker/core/I/O package bytes and closed schemas; acknowledge any necessary qualification-selector changes explicitly. Existing installed capabilities confer no hosted dataset rights. No public hosting, paid/provider/private acquisition, registry/stable/account administration or destructive actions authorized. EQ082-084 remain Backlog; private generation and deferred R6 providers remain separately gated.

## Accepted source pins

- `remote-v1.schema.json` Git-byte SHA256 `9d5e83f1d89bb34491d959158ae628e4b7b0590155244beb13945a3b94f362a0` at accepted workerbafc86a.
- `remote-v1.1.schema.json` Git-byte SHA256 `dda0f9d2e5968ff2ea3bf79f02740a18f9ae24cd93c492f7c38033883ed8a6b2` at accepted workerbafc86a.
