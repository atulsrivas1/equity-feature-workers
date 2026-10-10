# EQ079 scoped result delivery and expiry — pre-code plan

[Story89](https://github.com/atulsrivas1/equity-features/issues/89), E10#84, R7 milestone8. Thirteen provisional complexity points, not a schedule. EQ078 is accepted Closed/Done: [final gate](https://github.com/atulsrivas1/equity-features/issues/88#issuecomment-6096083611), [acceptance](https://github.com/atulsrivas1/equity-features/issues/88#issuecomment-6096091774), [Done readback](https://github.com/atulsrivas1/equity-features/issues/88#issuecomment-6096099276). R7 has four accepted stories and six remaining. EQ079 stays Backlog/unassigned during preparation; no result endpoint implementation is claimed. Live Project controls admission.

## Concrete delivery decision

Extend only optional equity-feature-service, candidate0.1.0a2. Implement the already closed result_read and artifact_read requests using opaque result_id and cursor:null. Non-null cursors deny; one bounded complete JSON result is sufficient for the admitted finite profile. Both operations return the existing1.1 kind=result complete native producer payload. Artifact_read adds a controlled JSON attachment header, rather than exposing a native receipt, publication envelope, artifact reference, filesystem path or arbitrary destination. Schema1.0/1.1 bytes remain unchanged. Only explicit1.1 requests are supported for result_read/artifact_read;1.0 requests deny incompatible_version before any result delivery. Never silently upgrade or downgrade a closed envelope.

Owner and original pinned grant/policy/dataset/config/full executed-feature scope must remain current. Require derived_read and retain for both operations, with additional explicit export for artifact_read. Adding export to service action vocabulary grants nothing to existing credentials or grants. Raw-only, calculation-only or job-management rights do not permit derived delivery. Do not reduce co-produced features, quality or evidence to evade a missing permission. Failed, cancelled, expired, unknown, foreign or old-epoch IDs produce a uniform authorization denial without existence disclosure.

## Integrity, precision and timing

On preparation and immediately before single-chunk emission resolve the retained record under the shared ledger lock. Pure public SDK decode_result/decode_envelope/decode_receipt, verify_content and verify_receipt validate full retained native forms; compare actual receipt SHA256 with the captured committed receipt SHA256. Reconstruct the accepted complete feature producer encoding from immutable command context and native result and compare exact canonical wire payload bytes. No source read, sink lookup, calculation or completion drain runs on an HTTP thread. Native creating-thread ownership remains unchanged.

Deliver the complete original producer context, command digest, backend/version, metadata, columns, units, unavailable status, quality, evidence and full executed_features. Preserve signed int64/nanoseconds as decimal strings and decimal128/structured values through the accepted codec. Floating cells preserve the existing exact bit representation, including signed zero where supported. No rounding, float timestamp conversion or null fabrication. Native producers and mathematical packages are unchanged.

Expiry is the half-open interval t < min(completed_ns + 300000000000, original_grant_expires_ns), using the accepted trusted wall clock and pinned original grant end. Original credential retirement or loss of retain invalidates retained forms; a new token cannot resurrect them. Reads/retries never refresh expiry. Maintenance plus synchronous checks deny at the boundary even while other native work is blocked. Tombstones remain until original grant expiry, then disappear under the accepted bounded ledger rules. No durable restart recovery claim; prior-epoch IDs deny.

Accepted reservation limits remain262144 bytes/job,1MiB/principal,2MiB/global; native65536, wire131072 and envelope+receipt32768. Exact actual encoded outbound frame bytes including caller correlation count against shared transfer budgets. Preparation reserves this amount atomically, with no retained full-result copy in a prepared iterator: keep only opaque IDs/current auth/correlation and recompute at emission. Expiry/revocation during preparation cannot leave a staged full payload retained indefinitely. Abandoned iterator close releases transfer reservation once; actual emitted transfer and request allowance never refund. No protected result transfer after reauthorization failure; denial frames still follow shared actual-byte accounting.

Attachment filename is fixed ASCII result-<sha256(opaque result_id)>.json. Never use a caller path, raw artifact ID or token. Success uses application/json, no-store, nosniff and exact Content-Length; denial has no attachment header. Unsupported range, GET, cursor, extra field, SQL/path/code/config/destination inputs deny under the closed protocol.

Service expiry clears only service-owned retained native/wire/envelope/receipt forms and their ledger accounting. It does not claim secure erase, total process-memory limits or deletion of external native sink storage, backups or audit history. Operator storage cleanup and physical quota/cache/audit qualification remain separate EQ080/EQ084 gates. Preserve foreign completion records and historical committed receipts. No provider/private dataset, public hosting, registry publication or cost authorization is inferred.

## Frozen producer references and independent tests

Four owned synthetic references are captured before new runtime from accepted worker companion1ebf261b2f0c895bfafdfb6a81c2fd3d70d1ce20: trades, bars, structured quotes and trades with adjacent nanoseconds near9000000000000000000. Full public native content/receipt verification passed for each; one actual source read each. The three base native hashes equal EQ078 independent pre-code goldens. Fixture JSON SHA256 is2ca65b4f548fe634e5acf7c7848cd4ae9de87420dbe35c3272d4937ba2df315a,139381 UTF8 bytes. These references are producer evidence, not new HTTP/security/release qualification. RequiredCommandSpec history remains unadvertised. Pure codec edge fixtures and native available/unavailable/structured complete metadata assertions precede final qualification.

| Vector | Required independent evidence |
| --- | --- |
| D01 trades complete producer | Full frozen payload/hash and independent count3/volume10/notional1011 |
| D02 bars complete producer | Full frozen payload/hash; unavailable overnight gap stays unavailable |
| D03 quotes complete producer | Structured values, quality, evidence and metadata exact |
| D04 adjacent large ns | Exact signed decimal ns, no float collapse |
| D05 scalar precision | Int64/decimal128 boundaries and supported float bit cases through public codec |
| D06 missing derived_read | Deny result and artifact before delivery |
| D07 missing export | Result permitted; artifact denied; no implicit new grant |
| D08 raw/calculation/management only | Deny derived visibility |
| D09 foreign/unknown/old epoch | Uniform denial without existence disclosure |
| D10 original grant/config/revision | Alternate grant or changed binding cannot authorize old result |
| D11 full co-produced features | Missing one executed feature denies entire result |
| D12 half-open credential expiry | Authentication denial at exact boundary |
| D13 grant/result expiry | Independent half-open grant and completion TTL boundaries |
| D14 repeated reads/new token | No TTL refresh or retired credential resurrection |
| D15 prepare then revoke | No first visible chunk and reservation released once |
| D16 prepare then expiry | No staged complete result retention; deny before emission |
| D17 no-poll cleanup | Expire terminal forms while unrelated native work is blocked |
| D18 exact transfer | Actual frame inclusive remaining bytes accepted; one fewer denied |
| D19 shared budget/window | Controllers share reservations and fixed60s request/transfer window |
| D20 abandoned/repeated close | Reservation released exactly once; no emitted bytes charged |
| D21 native mutation | Pure native integrity verification denies without source read |
| D22 envelope/receipt mutation | Captured SHA and public verification deny tampered publication |
| D23 retained wire mutation | Exact producer reconstruction denies altered context/quality/value |
| D24 cursor/version rejection | No fabricated paging/version; unchanged closed schemas |
| D25 path/SQL/extra input | Closed rejection before native work |
| D26 safe attachment headers | Controlled filename; no internal path/token or denial attachment |
| D27 repeated delivery | Zero additional source/sink/calculation calls |
| D28 failed/cancelled/committed failure | No result visibility; preserve historical and foreign completions |
| D29 restart/record retirement | Old epoch denies; no replay/recovery or eviction claim |
| D30 installed native HTTP | Fresh wheel/sdist light/full, Win/Linux, typing/core invariance and full native suite |

## Admission and delivery gates

Separate concrete pre-code review must verify the explicit1.1-only decision, public reconstruction feasibility and precise retained/emission accounting before Ready/start. Publish this plan and fixture references before runtime. Then guard exact Project89 identity and Backlog->Ready->In progress, assign owner and link paired draft PRs. All30 vectors must map to actual evidence, not planning assertions. Same-story API/changelog/lesson/handoff/issue evidence accompanies implementation. Actual separate final-head security/native/docs review, current Windows/Linux CI and installed artifacts, guarded paired publication, independent actual-main whole trees/public UTF8/Atul attribution/artifact readback precede Released/Done. Continue90 only after accepted89.

## Concrete planning review disposition

Separate reviewer /root/r7_policy_review independently approved producer/public API feasibility and30 requirements at the initial paired heads. [Exact review and limits](https://github.com/atulsrivas1/equity-features/issues/89#issuecomment-6096242264). Clarified protected-result versus denial-frame accounting; no runtime change. Four closed result frames and independent scalar codec boundaries pass accepted-code feasibility. Guarded Ready/start and all new runtime/security/installed gates remain required.
