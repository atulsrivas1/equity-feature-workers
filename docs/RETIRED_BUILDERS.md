# Retired and deferred builders (workers)

PO final status 2026-10-29.

| Builder | Status | Reason |
|---|---|---|
| tools/build_resilience.py | DEFERRED (not retired, not blocking a release claim) | Keeps its accepted historical baseline (core 20c08c7, io 19aa266 cohort); it has not been re-run against the post-bump cohort. Port or retire after the controlled-machine qualification; until then it must not be cited as current evidence. |

All other builders (foundation, supplied_inputs, client, mcp, mcp_evidence, service, agent_research, provenance) pass end to end; see docs/PRERELEASE_BUMP_EF5.md.
