# EQ078 asynchronous calculation jobs — pre-code plan

[Story88](https://github.com/atulsrivas1/equity-features/issues/88), E10#84, R7 milestone8. Thirteen provisional complexity points, not days. EQ075/076/077 are accepted; [EQ077 acceptance](https://github.com/atulsrivas1/equity-features/issues/87#issuecomment-6094456762) supersedes pending captures. R5/R5.1 and revised R6 are closed. At selection no other R7 story or component PR is active. This plan precedes implementation; live Project controls admission/status. No asynchronous runtime or release acceptance is claimed here.

## Behavior and package boundary

Extend optional equity-feature-service in the worker companion. Submit closed calculate requests, return opaque job IDs promptly, poll actual queued/running/terminal progress, request cooperative cancellation and enforce finite shared scheduling. Resolve only server-approved immutable command registrations: exact native config, selected supported feature/algorithm inventory, dataset/source scope, logical adapter/sink registrations and trusted operator-owned configurations. Caller data cannot supply code, SQL, paths, credentials, destination, arbitrary config fields or serialized Python. Unsupported unregistered feature families deny, even if installed locally. No fetching/scheduling/authentication/publication dependency enters calculations or accepted workers/I/O. All existing accepted package bytes and numerical semantics stay intact.

The existing closed1.0/1.1 job envelope already carries queued/running/succeeded/failed/cancel_requested/cancelled/expired. These are coarse actual progress, not numerical percentages or invented per-row progress. Only succeeded has a result ID; only failed has a typed error. Preserve both schemas, current slice behavior and original feature producer metadata. Rich stage streaming is not required or advertised. EQ079 owns result download/physical expiry; EQ080 owns expanded cache/audit and verified CPU/RSS isolation; EQ084 owns hosted operations. EQ078 must reserve and account its own bounded unpublished results and refuse unauthorized visibility rather than pretending those later gates are already qualified.

## Native execution feasibility to freeze before runtime

Accepted worker0.1.0a13 exposes SessionCommandSpec, RequiredCommandSpec, plan_acquisition/execute_plan, run_registered and run_required_registered. SessionCommandSpec covers bars/trades/quotes; required-input commands cover explicit history/reference/baseline compositions. Choose explicit server registrations only for actually qualified commands; no generic family inference from caller strings. Prefer native acquisition planning/approval/execution so installed capability cannot bypass consent. Establish exact public signatures, approved source/sink requirements, native output receipt/readback and command context binding with owned fixtures before selecting the runtime path. Do not silently bypass native publication or claim success from calculated values alone.

One registration freezes an exact bounded server-owned AcquisitionRequest independent of HTTP correlation, exact original normalized SourceBinding/receipt/content and native ConfigSpec/governed intervals. The worker's request ID, destination and ownership are native facts; HTTP request_id is only correlation. Job-specific native job/generation/partition IDs must not mutate source request-derived binding. Compare actual acquisition and output full identities, complete metadata/quality/evidence and producer command digest. A wrapping adapter/sink may add authorization and verification around native boundaries, but may not rewrite inputs or receipts. Record an explicit feasibility correction before runtime if the accepted API cannot enforce a boundary.

Accepted ProgressRecorder, BoundedSupervisor and native publication objects are creating-thread owned. Create and use them on their execution thread; publish separate synchronized immutable job snapshots to HTTP callers. Never poll an active recorder from another thread or invoke its private internals. Native cancellation is cooperative; 30-second deadlines cause cancellation/visibility denial and hold running capacity until actual native execution exits. Do not claim hard preemption or free a slot while work continues. A trusted source/sink must cooperate; refuse unsupported runtime registrations. Tests must prove execution really occurs asynchronously, rather than calling a synchronous fake and changing labels.

## Identity, rights and scheduling decisions

Shared process-owned scheduler: at most one running job globally, one queued job per principal, two queued globally, and one running per principal. All controllers use the same scheduler/ledger. Reserve queue/output/retention capacity atomically before admission; fixed policy caps100 rows,1MiB input/output,39 features,one instrument/session,30-second wall deadline apply. Existing request/transfer budgets remain shared and failures/retries/polls/cancel attempts consume request allowance. Native output reservation stays charged until result expiry/deletion or failure cleanup; already used transfer/rate allowances never refund. Bounded terminal/idempotency records need explicit finite capacity and lifetime before implementation; reject excess instead of retaining an unbounded dict.

Job authorization requires original owner and still-active pinned grant/policy/dataset/config/source/sink scope; calculate, job-management, retention and derived visibility rights remain distinct. Add explicit validated action vocabulary under the existing policy without granting rights to old slice grants. Reauthorize admission, actual start, every native input/publication boundary and status/cancel/output visibility. Revocation and visibility updates share an atomic lock. Foreign/unknown/expired IDs deny without existence disclosure. Trusted clock and policy expiry cannot use caller time.

Idempotency key is scoped to principal and exact full command identity including original grant/rights/policy revisions and immutable dataset/config/registry/algorithm/availability/source/sink facts. Recompute the wire digest with accepted canonical encoding. Same key and identical admitted command returns original job ID without another execution, including repeated polling after failed/cancelled execution. Changed command under same key conflicts; a fresh key is an explicit new attempt and consumes execution resources. Do not automatically rerun a partially committed native publication or create new results on transport retry. Retired/expired idempotency lifetime and restart behavior must be explicit: initial in-memory service has no durable restart recovery claim, and expired records cannot silently become a replay of prior receipts.

State transitions: queued -> running -> succeeded/failed; queued cancellation -> cancelled without source work; running cancellation -> cancel_requested until actual native exit -> cancelled. Final successful native verified publication may win a simultaneous cancellation only at the documented atomic commit/visibility boundary. Never relabel a committed receipt as rolled back; cancellation after successful commit is terminal idempotent. Revoked/expired authorization denies output even if historical native receipts exist. Shutdown closes admission, cancels queued/running work, joins cooperatively and preserves reservations until actual completion; no destructive cleanup of foreign records.

## Independent vectors to freeze before implementation

| Vector | Expected outcome |
| --- | --- |
| J01 bounded owned registered calculate | Immediate opaque queued job; native worker execution and verified receipt |
| J02 blocker in native source | HTTP submit/status remains responsive; one running slot remains charged |
| J03 actual completion | Succeeded only after native commit/readback; original producer context/quality preserved |
| J04 identical owner/key/command retry | Same job ID, one execution; request budget consumed per attempt |
| J05 same key, changed config/scope/feature | Conflict before source work |
| J06 same key, different principal | Independent identity, no foreign job/result disclosure |
| J07 malformed/zero/stale command digest | Reject before scheduling |
| J08 unknown config/source/sink/features | Reject before factories or I/O |
| J09 grant lacks calculate/management/retain | Deny respective operation with no implicit rights |
| J10 token expiry versus grant expiry | Authentication versus authorization denial, respectively |
| J11 foreign/unknown status or cancellation | Same fixed denial; no existence disclosure |
| J12 revocation between admission and start | No native source invocation |
| J13 revocation during blocked acquisition | Cancellation at native boundary, no visible result |
| J14 revocation races publication/HTTP status | Atomic visibility rule, no post-revoke exposure |
| J15 queued cancellation | Cancelled, zero native calls; reservation released once |
| J16 running cancellation | cancel_requested until real exit; no premature slot release |
| J17 cancellation after verified success | Terminal idempotent response, original receipt retained |
| J18 one running/two queued global exact cap | Admit permitted independent principals, serialized actual execution |
| J19 one queued per principal / one above global | Deny excess before work, no leaked capacity |
| J20 second controller | Shared caps/idempotency, cannot reset scheduler |
| J21 exact100/101rows and1MiB input/result bounds | Inclusive allow versus before-work/visibility denial |
| J22 30-second deadline with blocked cooperative source | Cancellation requested; held running slot until actual exit |
| J23 mutated full source binding/receipt/content | Typed failure; no relabelled producer input |
| J24 native unavailable quality | Job success can contain unavailable cells; no fabricated available value |
| J25 adjacent ns/int64/precision goldens | Native results equal direct pure package result and metadata |
| J26 failure/retry with native committed receipt | No duplicate execution on same key; preserve historical receipt |
| J27 terminal/idempotency/result capacity and expiry | Finite startup caps, deterministic denial/release rules |
| J28 shutdown and repeated cancellation | Actual join/cleanup accounting, no foreign completion drain |
| J29 unsupported schema/extra fields/SQL/path/code | Closed codec rejects before any worker invocation |
| J30 fresh installed wheel/sdist, HTTP two principals | Actual native worker proof, optional package/core invariance |

Use owned synthetic native adapters and registered native sink fixtures, independent expected trades/count/price/size/ns/quality goldens, fake trusted clock and barrier-controlled adversaries. An adapter/sink spy alone is not native integration proof. Cover exact real original-Parquet DuckDB acquisition with full receipt when the approved registration uses it; never acquire private/provider data. Before runtime, publish concrete fixture/context/identity choices and resolve acquisition planning, sink ownership, native authorization boundaries and finite-record expiry. Planning vectors are requirements, not executed tests.

## Review, delivery and next actions

Same-story canonical/component API/example/changelog/decisions/lesson/handoff and linked issue evidence are mandatory. Run separate final-head source/security/native/docs review with actual findings disposition, independent HTTP/race/worker tests, strict public typing, Windows/Linux CI, repeat wheel/sdist fresh installs, exact package/dependency/source receipts and core invariance. Qualify actual declared experimental channel, guard paired publication, renew independent actual-main whole-tree/publicUTF8/Atul attribution and artifact readback before Released/Done. No hard CPU/RSS/load claim without measurements; no public hosting, paid access, provider/private rights, stable/registry publication or full R7 closure inferred.

First publish this plan as paired drafts, review the concrete feasibility decisions and freeze owned vectors/native fixtures. Admit Ready/start only after dependencies, operational qualification inputs and exact supported inventory are established; then implement within those decisions. Later R7 stories remain Backlog until separately pulled. Record failures and preserve superseded evidence.

## Concrete pre-code choices and feasibility checkpoint

The five planning prerequisites now have a concrete candidate decision in canonical docs/decisions/R7_ASYNC_JOBS.md and companion docs/EQ078_DECISIONS.md, with frozen docs/stories/EQ-078_NATIVE_FEASIBILITY.json / companion docs/EQ078_NATIVE_FEASIBILITY.json. Initial remote registration inventory is trades/bars/trade_snapshot quotes via SessionCommandSpec; RequiredCommandSpec history SMA115.0 proves native API feasibility but is not advertised without multi-session input-rights admission. Public companion tools/qualify_job_native.py replays installed worker0.1.0a13 planning/execution/commit/readback and independent trade/bar/quote/history goldens, exact wire versus native digest distinction, stable source request across different job identities and postcommit cancellation with readable historical receipt. Three full native-pinned wire request fixtures validate against unchanged1.0. This does not implement asynchronous jobs or prove service auth/concurrency/artifacts.

Native ExampleSink denies arbitrary destination_scope: first adversary probe failed SINK_FAILED; corrected decision preserves synthetic-conformance and varies native job/generation/partition IDs, with equal original input/result bytes. Initial global/service-only environments lacked workers; accepted installed-review supplies actual site-packages worker. Actual failures are preserved, not inferred successes. Numerical formulas/accepted packages/schemas unchanged. Candidate finite profile:16 records global/eight principal/8192 encoded metadata bytes each,262144 aggregate reserved retained-output bytes per job with explicit native/wire/envelope/receipt subcaps,300second payloadTTL and3600second max job-grant validity, live tombstones retained until original grant expiry, no silent eviction/reexecution. Existing policy bounds can only tighten. Coarse synchronized actual state uses public execute_plan without changing native ProgressRecorder. Renew separate concrete planning review of these exact published candidates before Ready/start and runtime. Earlier open-choice wording is historical preparation, superseded only for the candidate decisions actually adopted after review.
