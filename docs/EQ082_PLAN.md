# EQ082 MCP discovery/query/job tools: pre-code plan

Canonical story [EQ082 #92](https://github.com/atulsrivas1/equity-features/issues/92), epic E10#84, milestone R7#8. Provisional complexity13 points, not days. Preparation only: Backlog/unassigned; no MCP implementation, Ready/start admission or integration approval. EQ075-081 accepted prerequisites; [EQ081 final gate](https://github.com/atulsrivas1/equity-features/issues/91#issuecomment-6102031603), [acceptance](https://github.com/atulsrivas1/equity-features/issues/91#issuecomment-6102034209) and [Done readback](https://github.com/atulsrivas1/equity-features/issues/91#issuecomment-6102035209) supersede pending captures. Qualified canonical mainafa8944984d7e41641f76cd2cf2489684831ae6f / worker4365fe90aa5125662b222782873bf106bd49b778. Frozen EQ081 receipt SHA6ae1ac5218437fc1af1a5f8b6f3fddb05e66a6ec4a0e953c865346bccb6dc2e1 retains original implementation9d70/e904; finalmetadata qualification is additive equivalent input proof. No acceptance-only receipt cycle.

## Intended component and scope

Create optional equity-feature-mcp0.1.0a0 in companion packages/mcp. Wrap the accepted public RemoteClient, outside service/worker/core/I/O/calculations. No direct fetching/native source or sink access inside tool handlers, no local math, model call or LLM dependency. One trusted owner program supplies a fixed client and immutable logical registrations; tool arguments never select credentials/origin/proxy/TLS policy or executable code. Public programmatic stdio runner and operator integration are frozen before Ready; avoid arbitrary configuration/plugin loading by protocol callers. Standard-library bounded JSON-RPC server is a proposed implementation approach, not yet proven. An independently pinned official MCP client SDK is proposed qualification-only; exact dependencies/constructors/import boundaries require feasibility first.

Initial proposed interoperable transport is local subprocess stdio, composing the already authenticated remote service through the client. No new public network listener or hosted MCP activation is authorized. Proposed target compatibility is explicitly pinned MCP2025-11-25; not a claim that it is the newest specification or every later version is supported. The official newer2026 material was visible during research; broad upgrades are not silently inferred. Protocol revision and reference-client interoperability are frozen by actual feasibility before Ready. Proposed tools cover discovery, admitted slice query, explicit calculate/status/cancel and bounded result summaries/artifact references. Exact names/schemas/version inventory and error semantics remain to freeze.

No mathematics change. Use unchanged accepted representations, public expectations and independent native producer fixtures. Preserve full identity and metadata where exposed; summarize verified values rather than recomputing statistics. HTTP contains no native publication envelope/receipt/SHA/artifact identity; the service owns receipt verification. MCP must not fabricate that proof or a physical/download URL from an opaque result ID. Projected feature slices and bounded summaries do not become complete original producers. Initial one-source complete producer inventory remains unchanged. Any additional admitted inventory needs concrete public expectation evidence, not a generic arbitrary command mapper.

## Authority, bounded results and failure behavior

One owner credential context per process; stdio process access is not remote dataset entitlement. Service current rights/owner/grant/epoch/TTL/revocation remain authoritative through every tool call. Logical registrations are owner-selected immutable data, not remotely supplied code/classes/SQL/paths. Proposed opaque artifact references identify admitted service results and explicit supported followup operations; they do not claim local files, resource reads, public URLs or durable recovery. Reference count/bytes/expiry and exact identity mapping require numerical caps and fixtures before Ready. No cross-owner deduplication, cache of result payloads, TTL refresh, provider/private acquisition or automatic file writes.

Tool successes use closed bounded structured output with equivalent bounded JSON text; tool data/metadata are untrusted data, never instructions. Tool hints describe behavior but do not grant rights. Freeze exact JSON-RPC protocol errors versus tool execution isError failures and accepted client's inert error mapping. Bound input read, frame/argument/output/summary/reference bytes, parser depth/nodes and active calls at physical boundaries. An SDK's ordinary unbounded readline or buffering is not a resource qualification; prove chosen framing and independent-client behavior. No general RSS, hostile-code sandbox or hard DNS/OS wallclock claim. Cooperative client budgets/retries remain unchanged; calculate/cancel never autoreplay and possibly sent unknown outcomes require manual stable-key followup.

Protocol cancellation, subprocess EOF/close and backend explicit job_cancel are distinct. Freeze actual accepted public close/cancellation feasibility, finite concurrency and owned cleanup before advertising behavior; no hidden rollback, automatic backend cancellation or thread exit claim. Unsupported MCP sampling/prompts/resources/tasks/pagination/version features are not advertised just because a framework supplies them. Actual reference-client integration plus an admitted owned native service with mandatory audit/OS containment is required; mocked handlers alone do not satisfy story acceptance. EQ084 hosted deployment/load/security/backup/recovery remains a separate operational gate.

## Seven Ready freezes

1. Exact protocol revision/schema assets/initialize-and-notification lifecycle/IDs/errors/capabilities, bounded physical stdio framing and reference SDK/version/dependency feasibility.
2. Exact public server/profile/registration constructor inventory and public RemoteClient operation/expectation mappings, immutable source-owner-epoch identities, no private service codec imports.
3. Exact closed names/input/output schemas, read/mutation annotations, structured JSON/text parity and unadvertised optional protocol features.
4. Numerical frame/argument/output/summary/reference/expiry/concurrency/staging caps and bounded failure/overflow/EOF cleanup behavior.
5. Concrete owned raw/native/authority/expiry/epoch/protocol/error fixture expectations, no fabricated receipt/physical artifact/download proof.
6. Actual owned reference-client stdio -> MCP -> accepted authenticated audited native service feasibility, mutation ambiguity/cancellation/partial-output/real-exit cleanup and bothOS installed qualification design.
7. Publish concrete decisions/fixtures and separate planning findings disposition, then guard exact Project92 Backlog->Ready->In progress before runtime. Routine feasibility is authorized; no new hosting/provider/private operational scope is granted.

## Independent requirements (not executed tests)

| ID | Required evidence |
| --- | --- |
| M01 optional boundary | MCP/client/transport remain outside calculations; local kernels and accepted packages unchanged, no model calls. |
| M02 actual integration | Independent MCP reference client launches installed owned stdio process, initializes, lists tools and makes real native service calls on bothOS. |
| M03 protocol pin | One explicit supported protocol revision and lifecycle; initialization/version/capability failures deny, no latest/all-version claim. |
| M04 framing | Finite UTF8 newline JSON-RPC frames; physically bounded reads, EOF/partial/extra/multiple-line adversaries, stdout only protocol. |
| M05 hostile parser | Duplicate keys, nonfinite numbers, depth/node limits, unknown protocol tags and malformed IDs denied without credential exposure. |
| M06 lifecycle | Calls before initialization/initialized notification, repeat initialize and postclose behavior frozen; unsupported capabilities not advertised. |
| M07 discovery schema | Tools/list returns complete frozen names/input/output schemas and explicit read/mutation hints; hints are not authorization. |
| M08 closed arguments | Reject extra fields/invalid scalar types/oversized argument maps before provider/network; no arbitrary schema or executable upload. |
| M09 fixed origin | One trusted immutable RemoteClient origin/token provider per process; tool callers cannot select URL, token, TLS policy or provider. |
| M10 immutable registrations | Owner freezes logical dataset/command/expectation profiles using public accepted APIs, tool IDs cannot replace complete source identity. |
| M11 authenticated discovery | Use actual remote discovery under current credentials; service denial cannot turn into locally cached capability or rights claim. |
| M12 bounded slice | Fixed admitted raw/feature projections; preserve source/scope/units/scalars and metadata, do not fabricate absent columns. |
| M13 calculate | Only immutable server-admitted command registration; caller stable idempotency key, no automatic mutation replay or hidden compute. |
| M14 native states | Job status preserves native coarse states, no invented stages or percent; polling explicit only. |
| M15 explicit cancellation | Backend job_cancel is explicit tool mutation; protocol request cancellation/EOF does not promise server rollback or automatic cancel. |
| M16 unknown outcome | Possibly sent calculate/cancel ambiguity exposed as fixed outcome_unknown, caller manual followup only. |
| M17 structured output | Successful structuredContent matches frozen output schema and bounded equivalent JSON text; no prompt text treated as instruction. |
| M18 summary fidelity | Bounded summary retains declared kind/context/quality/availability/counts from verified original representation; no recomputation or completeness overclaim. |
| M19 artifact references | Opaque controlled result/artifact references only, no filesystem/URL/credentials/path exposure, no invented published receipt or download address. |
| M20 no auto file output | Tools do not write downloaded artifacts or arbitrary caller filenames; service owns native publication, data stays bounded. |
| M21 full identity | Logical reference binds exact original owner/epoch/job/result/source/registration/version; stale or forged refs do not select another result. |
| M22 cross owner | Actual authenticated secondprincipal discovery then firstowner/unknown result-status-artifact-cancel same denial; no crossprocess reuse. |
| M23 grant expiry | Original halfopen token/grant/result TTL and revocation/retirement remain authoritative; no read refresh or resurrection. |
| M24 epoch ambiguity | Restart loses ephemeral reference state; old IDs deny, only explicit owner action starts another job, no durable recovery claim. |
| M25 finite state | Explicit process call/ref/count/byte caps and expiry; current rights checked each backend call, no unbounded argument/reference cache. |
| M26 backpressure | Bound active calls and input/output staging, cleanup holds ownership until actual exit; unsupported concurrency fails closed. |
| M27 error separation | Malformed/unknown JSON-RPC method versus legitimate tool execution failure classified distinctly, fixed redacted errors/no caller or server text leaks. |
| M28 transport failures | Accepted client TLS/framing/cooperative retry/unknown semantics preserved; no new hard DNS/OS wallclock or retry override. |
| M29 cleanup faults | Partial stdout, EOF, input failure, provider exception and interrupted owned process leave no orphan service/child/socket/thread or hidden retry. |
| M30 independent native fixtures | Owned trades/bars/quotes/unavailable/structured/scalar/largeNs cases compare complete accepted goldens and original local kernels. |
| M31 installed artifacts | Fresh Windows/Linux wheel/sdist/light boundaries/reference client/typing/RECORD/pinned deps/core/source and actual serverZIP proof. |
| M32 delivery | Same-story API/examples/decisions/changelog/lesson/handoff/issue evidence, exacthead separate review/currentCI/guarded publication/main readback and lifecycle. |

## Primary protocol inputs and local verification plan

Read October10,2026: [versioned stdio transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports), [tool schemas/results](https://modelcontextprotocol.io/specification/2025-11-25/server/tools), [lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle). These define newline-delimited UTF8 JSON-RPC, tools/list and tools/call structured schemas/results, and initialization/version negotiation. They inform the proposed compatibility target; actual bounded implementation/reference-client tests are pending. Source pages do not establish our package conformance, resource bounds or data rights.

First publish paired plan/accepted81 continuity, then freeze seven feasibility prerequisites and independently review concrete decisions before Ready/start. Implement only after admission; map all32 requirements to actual owned fixtures, preserve failed evidence and update same-story docs/API/examples/changelog/knowledge/lesson/handoff. Current Windows/Linux fresh wheel/sdist, reference-client integration, typing/public consumers/RECORD/pinned dependencies/core/runtime invariance/public source/current serverZIP and final separate review precede guarded paired publication and independent actual-main qualification/ReleasedDone. No registry/stable/hosted/provider/private rights or performance claim. EQ083-084 remain Backlog; R7 seven accepted/three remaining, epic/milestone open. EQ108 later extends this one MCP server; do not create a competing implementation or impose future R9/R10 gates on base R7.
