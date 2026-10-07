# Public development workflow

Status: human-agreed workflow, 2026-10-04. Repository: atulsrivas1/equity-features. Issues, milestones and the public Project are linked in canonical equity-features docs/DASHBOARD.md. That dated preparation status is historical; current implementation and acceptance are recorded in the live Project and release receipts. Work agreements are in ../AGENTS.md and continuity in SESSION_HANDOFF.md.

## Planning and progress

Use [release-based continuous pull](https://github.com/atulsrivas1/equity-features/blob/main/docs/DELIVERY_POLICY.md), with one active story initially and weekly progress review. No mandatory sprints; milestones and readiness gates guide delivery. Target dates are forecasts set when evidence supports them.

Keep canonical equity-features docs/BACKLOG.md as the versioned scope/dependency baseline. Create one GitHub Issue per story, retaining existing IDs and retaining active EQ-001 through EQ-093 plus EQ-095 through EQ-130 and never reusing retired IDs. Epic tracking issues link their child stories. Use GitHub milestones for the release map, including inserted R4.1 before R5; milestones represent readiness outcomes rather than promised dates. Track Backlog, Ready, In progress, Code review, Test, Ready to release, Released and Done. Represent blocked work with an explicit linked dependency/reason, not an unsupported percentage-complete claim.

Issue title: [EQ-001] Freeze v1 feature scope. Issue body: problem/user value, included/excluded scope, acceptance checklist, prerequisites, release, validation and related design links. Labels identify epic and work type (design, feature, test, docs, performance); Project fields track status and release. Avoid maintaining different status lists in markdown and GitHub. GitHub is the work-status authority once set up; backlog markdown remains scope/version history and records deliberate scope changes.

Only move a story to Ready when its required decisions and inputs are available. Limit active implementation work initially to one story or a small explicitly dependent group. Assign authors when work starts rather than inventing ownership. Later-release issues remain backlog. Milestone completion requires its documented acceptance gate; a package can be complete without any proprietary dataset being admitted for production.

## Change delivery

Work on short-lived branches, preferably codex/eq-001-feature-scope when this agent creates a branch. Open a draft PR early for a reviewable design or implementation, include the story ID, and link dependencies. Each PR describes behavior, evidence and limitations. Close a story only at Done after its acceptance, documentation and applicable release evidence are complete. A merged implementation waiting for package release remains Ready to release. Avoid auto-closing keywords for implementation stories before their release gate.

## Story lifecycle

| Status | Required evidence |
| --- | --- |
| Backlog | Planned; decisions/dependencies not yet established |
| Ready | Acceptance criteria, scope and required inputs/prerequisites established |
| In progress | Actual design/implementation work underway with linked branch or draft |
| Code review | Concrete reviewable PR, self-checks and documentation available; review applies to design/docs too |
| Test | Review concerns resolved; relevant validation/acceptance checks being executed or assessed |
| Ready to release | Required checks passed, acceptance criteria met, documentation complete and release artifact/change prepared |
| Released | Delivered to declared users/channel with release evidence: package/tag/deployment, or validated documentation merged to default branch |
| Done | Released result verified, acceptance/evidence/links recorded, issue closed and remaining work separately tracked |

Update status promptly when actual work changes; don't infer Test or Released from a PR being opened. Tests may also run during development/review; Test denotes the formal acceptance gate. Failed review or tests return the story to the appropriate earlier stage with a reason. A blocked story retains its actual stage and a dependency explanation.

Epics aggregate child progress: In progress once a child is underway/completed, Done only when all child release/acceptance gates are met. Do not mark an epic Done because its tracking issue exists. Documentation-only publication is a story release, not completion of its parent release milestone. No fabricated delay is required between stages when their evidence is already present.

## Documentation with every story

Update issue acceptance/progress/evidence and relevant design/formula/API/examples/decision/release documents in the same linked change as the story. PRs state documentation impact and validation, even when no user-facing API changes. Update SESSION_HANDOFF.md after meaningful work with decisions, failures, evidence and exact resume steps. Documentation completeness is mandatory before Ready to release and Done, not a separate later cleanup. GitHub Project owns current status; documents link it instead of duplicating changing counts/statuses.

Use the default branch as the coherent reviewed state. Require relevant CI when implemented: tests, typing, package builds and examples. Numerical changes need mathematical fixtures and documented algorithm-version impact. Performance changes need measured comparisons and result parity. Avoid mirroring implementation in tests; verify meaningful independent expected outcomes. Design/docs changes require applicable link/scope checks, not irrelevant computation benchmarks.

Keep consequential architecture/formula decisions in docs/decisions/ with context, choice and consequences; decisions should link affected issues. PR review can be performed with tooling, but don't imply an independent human reviewed work when no human did. Record actual reviewers and validation. Secrets, paid provider data, private code and internal deployment paths are excluded from public fixtures/docs; use synthetic data or explicitly licensed examples.

## Portfolio presentation

README explains the concrete problem, source-independent API, current capabilities, runnable example, installation and honest development status. Include architecture and roadmap links, test/CI badges once real workflows exist, feature definitions and reproducible benchmark methodology. Tag releases only after acceptance; distinguish experimental APIs from stable ones. Show clear engineering choices and results rather than inflated performance claims or fabricated activity.

Use release notes for delivered behavior, compatibility changes and known limitations. Project updates describe meaningful milestones; no obligation to manufacture daily commits. Contribution guide, license, security reporting guidance and issue/PR templates are foundation tasks. Public announcements and profile edits remain user-directed setup work.

## First work item: EQ-001

Start with the feature scope, not code generation. Historical EQ001 delivered canonical equity-features docs/features/V1_SCOPE.md as a reviewable proposal that maps feature IDs to families, initial windows, input kinds, batch/incremental support, exact versus approximate measures and excluded strategies/labels. Scope completion requires explicit formula-story links and no undocumented promised feature. EQ-002 through EQ-006 settle formulas/timing; contracts and implementation follow those decisions.

Until GitHub setup finishes, store reviewable drafts here and clearly report local-only status. After the repository is available, migrate the documents without overwriting setup work, create/link the first issue and PR, and preserve the numbered backlog. Repository/profile administration stays in the setup conversation; design and implementation stay in this conversation.

## Owner decision: automated reviewer setup deferred

On2026-10-04 the owner deferred GOV-005/PR120 and instructed continuing EQ stories. Its proposed separate Codex review gate is postponed until activation is resumed and verified. Continue author self-review, CI, acceptance/documentation and publication verification, labeled accurately; never claim an automated or human review that did not occur. GOV-005 remains open/deferred; this decision does not waive tests or release gates.

## Prepared automated review guidance

PR120 prepares AGENTS.md Code Review Rules and CODE_REVIEW.md. GOV-005 remains deferred until the owner resumes hosted activation and its first real review is verified. This proposed setup does not suspend the current owner-authorized self-review/CI workflow. Once activated, record reviewer identity, response/commit coverage and findings disposition; do not equate automated and human review.

## Resumed owner direction — October 5, 2026

The owner requested the same attribution correction and separate Codex reviewer workflow as strategy-research. This supersedes the October 4 deferral above. PR120 was merged; it is historical guidance, not evidence of hosted activation. Future PRs require an actual completed Codex review covering the final head and findings disposition before merge/Done. Keep GOV-005 in Code review while activation or the first review is missing. Existing CI, numerical acceptance and publication checks remain required.

See [history correction](https://github.com/atulsrivas1/equity-features/blob/main/docs/decisions/OWNER_ATTRIBUTION_CORRECTION.md). Rebase outstanding work onto corrected main; do not merge the old ancestry back or overwrite other work. Original receipts retain their original SHAs as provenance.

## Companion execution

The lifecycle is tracked only on the linked canonical equity-features story. Component PRs provide actual reviewed heads, native CI, installed archive identities and publication/readback evidence. No duplicate story trackers.
