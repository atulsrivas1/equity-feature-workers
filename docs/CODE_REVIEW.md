# Codex PR review workflow

Status: repository guidance prepared; hosted activation and first review response are not yet verified. Setup: [GOV-005](https://github.com/atulsrivas1/equity-features/issues/119).

## Setup and scope

Connect atulsrivas1/equity-features to Codex and enable automatic code review for this repository. Prefer automatic review when a PR becomes ready; select additional push triggers where the settings support them. Keep changes scoped to this repository; do not enable unrelated security scanning or grant broad account/repository access as a side effect. Account authentication and integration authorization may require the owner.

The [official setup guide](https://learn.chatgpt.com/docs/third-party/github) describes automatic reviews and manual PR comments using `@codex review`. Repository instructions live in AGENTS.md under Code Review Rules. This hosted integration is separate from local command auto-approval and requires no new API key in this repository.

## Per-story delivery

1. Open the linked PR with story acceptance, design/documentation impact and relevant self-checks. Move the story to Code review when it is reviewable.
2. Make the PR ready and wait for the separate Codex review. If automatic review does not run, request it with `@codex review` after verifying integration access.
3. Record reviewer identity, review URL, reviewed commit, findings and their disposition. A reaction alone means the request was received, not completed. A completed bot response is automated review; it is not human approval or proof that every requirement was checked.
4. Resolve actionable findings. For disputed findings, document evidence and rationale rather than silently dismissing them. Relevant subsequent code/contract changes need another review covering the final commit; unresolved findings prevent release readiness.
5. Enter Test for formal acceptance. Require final-head CI, independent mathematical fixtures where applicable, and complete documentation. Then proceed through Ready to release, Released and Done with actual delivery/publication evidence.

No separate review response, quota failure or unavailable integration counts as a pass. Keep the PR open with the reason recorded and request a retry or an explicit alternative-review decision from the owner. Do not silently bypass the agreed review pass or relabel self-review as a separate review. This setup PR is also held for the first real Codex response and acceptance before merge.

## Enforcement and limitations

This is a documented workflow gate, not a claim that GitHub branch protection automatically enforces bot completion. Existing required CI stays in place. Do not add a generic required human-approval count or nonexistent bot status check: verify the integration's actual output and enforcement mechanism first. Codex review supplements tests; formula acceptance, package builds, performance qualification and source rights remain their own gates.

Record activation evidence on GOV-005 without credentials or account secrets. Verify a real review, its commit coverage, findings and final checks before declaring setup complete. Existing merged stories retain their original self-review evidence; they are not retroactively claimed Codex-reviewed.

## Owner deferral

Owner decision (2026-10-04): defer Codex integration setup and continue EQ stories. GOV-005/PR120 remain open and deferred in Code review; no hosted activation or automated review claimed. Until activation is explicitly resumed and verified, use the existing self-review plus CI/acceptance workflow, accurately labeled. This explicit owner decision postpones the proposed separate-review gate; it does not waive tests, documentation or release verification.

## Resumed owner direction — October 5, 2026

The owner requested the same attribution correction and separate Codex reviewer workflow as strategy-research. This supersedes the October 4 deferral above. PR120 was merged; it is historical guidance, not evidence of hosted activation. Future PRs require an actual completed Codex review covering the final head and findings disposition before merge/Done. Keep GOV-005 in Code review while activation or the first review is missing. Existing CI, numerical acceptance and publication checks remain required.

See [history correction](https://github.com/atulsrivas1/equity-features/blob/main/docs/decisions/OWNER_ATTRIBUTION_CORRECTION.md). Rebase outstanding work onto corrected main; do not merge the old ancestry back or overwrite other work. Original receipts retain their original SHAs as provenance.

## EQ035 authorized alternative — October 5, 2026

The owner explicitly authorized a separate local Codex reviewer for EQ035 while
hosted activation is unverified. This satisfies the alternative-review decision
above; it does not claim hosted activation or human review. A separate local
review agent must inspect the corrected breadth implementation and final PR220
head, record findings and their disposition, and identify its coverage and
limitations before merge or Done. Author self-review, a review request or CI
alone remains insufficient. GOV005 stays open for hosted activation; this
bounded decision does not waive numerical, documentation or publication gates.

## Bounded R2 continuation

The owner-selected local reviewer workflow continues through the remaining R2
composition, comparison and final acceptance stories, as recorded in their plans
and delivery reports. The separate agent /root/eq035_review reviews actual frozen
heads and records executed checks, findings and limitations. This does not activate
hosted review, constitute human review, or retroactively review earlier stories.
The final R2 audit must cover all sixteen actual R2 kernels and declared capabilities.
GOV-005 remains open; numerical, CI and artifact gates remain mandatory.


## R3 authorized local review — October6, 2026

The owner explicitly authorizes separate local Codex reviewers throughout bounded R3, continuing autonomous delivery after accepted R2. This extends the R2 alternative; no additional approval is required per story. Each review must inspect the actual final PR head, report executed checks, findings/disposition, reviewer identity and limitations. Author self-review and CI alone remain insufficient. Hosted activation remains unverified and GOV005 remains separate; numerical, documentation, bothOS CI and actual artifact acceptance remain mandatory.

## R4 authorized local review — October 6, 2026

The owner explicitly authorized separate local Codex reviewers throughout R4 and its handoff because hosted GitHub Codex review is not working. This extends the bounded R2/R3 alternative to GOV013 and EQ049–056; no further per-story approval is required. Each reviewer must independently inspect the actual final PR head and affected contracts/callers, record reviewer identity, executed checks, findings/disposition and limitations. Relevant changes require final-head coverage. Author self-review and CI alone do not satisfy this gate. This is local automated review, not hosted activation or human review. Existing numerical, documentation, CI, installed-artifact and publication/readback acceptance remains mandatory; GOV005 stays separate.


## R4.1 and architecture planning authorized local review — October 6, 2026

The owner explicitly extends separate local Codex reviewers to GOV-014 architecture planning and the bounded R4.1 prerequisite release, because hosted review remains unavailable. Each review must inspect the actual final head and affected contracts/callers, record identity, executed checks, findings/disposition and limitations. Relevant changes require renewed final-head coverage. Author self-review and CI alone remain insufficient. This does not authorize later releases by inference, activate hosted review or waive numerical, documentation, installation and publication gates.

## R4.1 execution handoff coverage

The owner-authorized bounded R4.1 local-review alternative includes its GOV-015 execution handoff preparation and final-head semantic review. Retain actual reviewer identity, findings/disposition, relevant CI and publication gates. This clarification grants no hosted activation, human-review claim, later-release scope or waiver of installed/numerical acceptance.
