# Work agreements

All commits must use Atul Srivastava <102820540+atulsrivas1@users.noreply.github.com> as author and locally created committer. Do not use Codex attribution or Codex co-author trailers. Verify published attribution.

Owner direction on October 5, 2026 resumes GOV-005: require a separate completed Codex PR review covering the final head, findings disposition and existing acceptance/CI gates before merge or Done. A request, reaction or self-review is not a completed review. Missing activation blocks Code review. See docs/CODE_REVIEW.md. This supersedes the October 4 deferral.

These are the human owner's agreed project rules. Read this file, docs/PROJECT_KNOWLEDGE.md, docs/PUBLIC_DEVELOPMENT.md, docs/SESSION_HANDOFF.md and the current issue/Project status before starting work. Keep durable project memory in these tracked documents rather than assuming chat memory persists. Read the relevant linked decisions, lessons and source specifications before choosing or changing work.

1. Work publicly through numbered GitHub stories, epic links, release milestones and linked pull requests. GitHub Project status is the current work-status authority; docs/BACKLOG.md records versioned scope and dependencies.
2. Follow the backlog. Satisfy acceptance criteria and release gates before declaring completion. Record changes in scope and consequential decisions explicitly.
3. Keep calculation packages source-independent. No fetching, database/file access, credentials, job scheduling or output publication inside calculations. Adapters and workers remain separate.
4. Define mathematics first: formulas, units, timing, initialization, coverage and edge cases before implementing a feature.
5. Verify correctness with independent fixtures and relevant tests. Support performance claims with measured benchmarks and result parity. Never invent review, validation or acceptance evidence.
6. Preserve privacy and rights. Public examples use synthetic or explicitly licensed data. Never publish credentials, private datasets or code without established permission.
7. Work autonomously within agreed scope. Make routine implementation choices; ask when changing scope, public commitments or performing destructive actions. Preserve unrelated work.
8. Update continuity after meaningful decisions, changes, failures and validation. Record current evidence, remaining work and exact resume steps in docs/SESSION_HANDOFF.md. Never claim unfinished work is completed.
9. Library design and implementation belong in the development conversation. GitHub profile/account administration belongs in the separate setup conversation. This development conversation manages the project's issues, dashboard and delivery workflow as authorized.
10. Report planned, implemented, reviewed, tested, released and completed work distinctly. Update GitHub status as actual work changes; do not leave an active story in Backlog or close a merely merged implementation awaiting release.
11. Use the exact lifecycle: Backlog -> Ready -> In progress -> Code review -> Test -> Ready to release -> Released -> Done. Apply entry/exit criteria in docs/PUBLIC_DEVELOPMENT.md. Rework returns to the correct earlier state; blocked reasons/dependencies are explicit. Do not fabricate transitions or skip required evidence.
12. Documentation is part of every story. Update relevant GitHub issue acceptance/evidence, public design/API/examples/decision/release documentation and linked PR alongside the change. Document no-impact decisions when appropriate. Documentation cannot be deferred to a later cleanup story to declare this story complete.

Use short-lived codex/ branches when this agent creates branches. A human approval is required only where the user or applicable review policy requires it; do not invent an approval requirement. Record actual reviewer identity and limitations. Numerical tests and release gates remain mandatory even when work is autonomous.

Delivery uses release-based planning and continuous pulling, not mandatory sprints. Read docs/DELIVERY_POLICY.md. Start the highest-priority dependency-satisfied Ready story, initially one active story at a time. Review progress weekly during active work; release dates are evidence-based forecasts, with no invented deadlines or automated reminders.

Maintain knowledge using docs/knowledge/BACKLOG_WORKFLOW.md. After a meaningful result or correction, update the relevant evidence-linked lesson, preserve superseded decisions, update the handoff, and reconcile affected issue/dependency records. Do not copy raw private chats into public documentation or turn historical status into current execution authority. Knowledge preparation does not take ownership from an active release session.
## Code Review Rules

Review the changed behavior and its affected callers against the linked story, accepted mathematical specifications and package design. Report concrete actionable defects with file/line evidence, triggering inputs and consequences. Identify severity accurately; do not invent findings, benchmark results or reviewer independence. Documentation/design PRs need semantic and contract review as well as links.

### Mathematics, timing and data fidelity

- Check equations, denominators, units, initialization, eligibility, interval boundaries, auction rules, stable ties and exact versus approximate feature identities against independent expected fixtures.
- Check market/reference cutoffs, known-at availability, prior-only baselines, adjustment compatibility and future-data leakage; missing, empty and incomplete inputs must remain distinguishable.
- Preserve UTC int64 nanoseconds, scaled-price precision, null validity and guarded accumulation; check overflow, round trips and documented floating tolerances.
- For incremental features, check batch parity, provisional-state isolation, ordering/duplicate policy, checkpoint compatibility and correction/backfill replay. Reject unsupported capability assumptions and retrospective use of future-consumed state.

### Boundaries, compatibility and validation

- Calculation packages must not fetch data, read files/databases, consult wall time, access credentials, schedule jobs or publish outputs. Adapters, workers and network/MCP stay outside numerical packages.
- Custom IDs cannot override built-ins. Local caller code is trusted, not sandboxed; remote services must not deserialize uploaded executable objects.
- Check public schema/algorithm/config versions, identity and revision binding, typed errors, required dependencies and actual supported capability claims.
- Check meaningful independent tests, synthetic/licensed fixtures, documentation, story acceptance and release evidence. Performance claims require measured workload/hardware and numerical parity; check copy/conversion costs and avoid hidden per-record object loops.
- Flag private data, credentials or code without established publication rights. Do not mistake reference verifiers for implemented production packages.

### Review evidence and delivery

See docs/CODE_REVIEW.md. A separate Codex review is automated review, not author self-review or independent human review. Record its response URL and reviewed commit; resolve findings and establish final-head review coverage after relevant changes. No bot response, quota failure or missing configuration counts as approval. CI and numerical acceptance remain separate requirements.

## R4 authorized local review — October 6, 2026

The owner explicitly authorized separate local Codex reviewers throughout R4 and its handoff because hosted GitHub Codex review is not working. This extends the bounded R2/R3 alternative to GOV013 and EQ049–056; no further per-story approval is required. Each reviewer must independently inspect the actual final PR head and affected contracts/callers, record reviewer identity, executed checks, findings/disposition and limitations. Relevant changes require final-head coverage. Author self-review and CI alone do not satisfy this gate. This is local automated review, not hosted activation or human review. Existing numerical, documentation, CI, installed-artifact and publication/readback acceptance remains mandatory; GOV005 stays separate.


## R4.1 and architecture planning authorized local review — October 6, 2026

The owner explicitly extends separate local Codex reviewers to GOV-014 architecture planning and the bounded R4.1 prerequisite release, because hosted review remains unavailable. Each review must inspect the actual final head and affected contracts/callers, record identity, executed checks, findings/disposition and limitations. Relevant changes require renewed final-head coverage. Author self-review and CI alone remain insufficient. This does not authorize later releases by inference, activate hosted review or waive numerical, documentation, installation and publication gates.

## R4.1 execution handoff coverage

The owner-authorized bounded R4.1 local-review alternative includes its GOV-015 execution handoff preparation and final-head semantic review. Retain actual reviewer identity, findings/disposition, relevant CI and publication gates. This clarification grants no hosted activation, human-review claim, later-release scope or waiver of installed/numerical acceptance.

## Companion authority

Canonical story/epic/milestone/Project authority remains in atulsrivas1/equity-features. This repository carries component PR/build/API evidence only. Do not duplicate canonical lifecycle issues or use cross-repository auto-closing keywords. R4.1 workers is a skeleton; no R5 commands or scheduling. Read docs/PROJECT_KNOWLEDGE.md, docs/PUBLIC_DEVELOPMENT.md, docs/SESSION_HANDOFF.md and linked current canonical issue before work. Separate local final-head reviewers are authorized throughout bounded R4.1.
