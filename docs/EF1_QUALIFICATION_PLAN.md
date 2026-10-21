# EF1 current-candidate qualification plan

Next authored slice: mandatory `test_ef1_clock.py` (eight methods), sibling pure/I/O clock suites and `examples/ef1_clock.py` C01/C02/knowledge full DuckDB/browser demonstration are specified in [EF1_CLOCK_IMPLEMENTATION.md](EF1_CLOCK_IMPLEMENTATION.md). Coordinator owns exact source pins and qualification-tool extension after source review. All anchor/legacy checks remain required; this is a future evidence plan, with no clock execution or candidate acceptance claimed.

Source-authored only; no qualification was executed. Coordinator owns the new entry point, workflow and final reviewed source/artifact pins. This plan implements the scope of independently approved PACKAGING_AMENDMENT.md without changing historical build receipts or silently repinning old package studies.

## Current candidate

Workers0.1.0a15 exactly requires features0.0.4a5 and SDK0.1.0a2. Contracts/wire remain0.0.4a4 and intraday remains0.1.0a3. New supplied_session callback expects exact backend0.0.4a5 while retaining all existing header/input/config/entity/backend-kind checks. EF1 requires exact featurea5/contractsa4. Prior published workera14/featurea4 is unchanged and is not claimed compatible witha5.

Current source probes changed deliberately: supplied_original checks legacy backenda5; acquisition_probe expects workera15/exact featurea5; probe_foundation independently checks contractsa4 versus featuresa5 and workera15 dependencies; pilot_fixture expects featurea5/workera15 and adaptera9. Their independent numerical expectations remain unchanged. Mandatory EF1 tests have no version skip: setUpClass fails on missing EF1 API or any wrong feature/contracts/worker candidate. Ten authored EF1 test methods remain unexecuted; no green/zero-test/optional-skip acceptance.

## Pre-migration CI conflicts retained as review evidence

The first two rows describe the reviewed pre-migration defect. Current source authoring below replaces those two lanes' pins/version expectations while preserving their gates. The other historical companion lanes retain their original frozen cohorts.

| Existing path | Actual source/version conflict | Safe disposition |
| --- | --- | --- |
| `.github/workflows/foundation.yml` + `tools/build_foundation.py` | Workflow and builder materialize core20c08c7370581d03c8a0404579667f68d67ac88b/IO19aa266b9debb92a6c5eb707baafeb9ae7a82942; builder EXPECTED still requires featurea4. Workflow discovers all current tests against that old source; EF1 tests now fail strictly. Updated current acquisition/foundation/pilot probes also require new versions. | This historical pinned composition cannot qualify workera15. Coordinator supplies a separate reviewed new-source lane; do not merely update old expected strings or hide mandatory EF1 tests. |
| `.github/workflows/supplied-inputs.yml` + `tools/build_supplied_inputs.py` | Coreb1f5445eb9799b73753e413669b94166997fc468 supplies featurea4; PACKAGES fixes featuresa4/workera14. Current worker is a15/featurea5 and current supplied_original correctly expects backenda5. | Preserve historical pins/receipts. New EF1 entry point must package actual reviewed a5/a15 source and execute the current independent legacy fixture, separately recognizing backend evolution. |
| `.github/workflows/service.yml` + `tools/build_service.py`, and client equivalents | Older core/IO checkout refs and embedded WORKER_COMMIT7d86deb94b6c338be9cc893c729352c59517e28f select historical service/client composition. | Retain their original scope; neither is new EF1 acceptance. No service/client version migration or stored receipt rewrite is included in EQ213. |
| `.github/workflows/resilience.yml` + `tools/build_resilience.py` | Workflow selects component6538f21b441c8527b465899b98cfd0476e7af08a and builder retains ACCEPTED_WORKER_COMMIT84b15c01fcb9526e4557df83d055daa5303db639. | Historical accepted composition only; coordinator classifies applicability explicitly. |
| `.github/workflows/mcp.yml` + `tools/build_mcp_evidence.py` | Older optional coree1b4ac52d748fd6b5f13b9acaf6a1358a8caea6c/IO19aa266b; separate evidence package composition. | No new EF1 service/MCP acceptance inferred. |
| `.github/workflows/agent-research.yml` + `tools/build_agent_research.py` | Separately pinned cored2ac11ebca5fe2cd151c35fa7f889a1d70a61d6d and IO18d867; separate research contract. | Coordinator reviews actual applicability; do not count this as EF1 package validation or rewrite research evidence. |
| `.github/workflows/eq080-feasibility.yml` | Host-only historical pre-code probe, no candidate package qualification. | Retain its scope; success is not numerical/package/EF1 acceptance. |

These are source-read conflicts/limits, not observed CI execution failures. The scoped migration below supersedes the original no-build-script-change decision for current foundation and supplied-inputs only.

## Current foundation/supplied migration authored after review

Coordinator required coherent current foundation and supplied-input lanes for workera15. `build_foundation.py` now separates **PINS_CORE/PINS_IO**, used exclusively by its current CLI, from **CORE_COMMIT/IO_COMMIT**, which remain the exact historical20c08/19aa constants imported by service/client/resilience/MCP/research builders. This avoids propagating the new feature package into old frozen-worker cohorts. No historical companion builder import, source pin, receipt or gate was changed.

Current foundation PINS_CORE and supplied-inputs CORE use placeholder `EF1_REVIEWED_CORE_COMMIT_REQUIRED`; current foundation PINS_IO and supplied-inputs IO use `EF1_REVIEWED_IO_COMMIT_REQUIRED`. The two current workflow checkout refs use the identical placeholders. **Coordinator must replace all corresponding placeholders with matching exact reviewed SHAs before push.** Both current builders fail before build/output creation if placeholders are not40 lowercase hex characters. Placeholders are source-authoring markers, not valid source acceptance or runnable CI configuration.

Foundation's current EXPECTED dependency requires featurea5; installed source-adapter assertion requiresa9. Supplied-input PACKAGES selects featuresa5/workersa15, preserving contractsa4/SDK-contractsa2/intradaya3. All existing repeat archive, selected-source SDist rebuild, installed forms, payload/source closure, typing, optional absence, numerical/regression, platform/skip policies, result/parity and resource thresholds remain in place. No additional skip, reduced test population or relaxed diagnostic count was introduced. Root-owned `ef1.yml`/`qualify_ef1.py` separately provide strict positive EF1 qualification; this worker did not edit those files.

No builder, import, test, build, DB operation or benchmark was executed during migration. Independent corrected-source recheck, actual reviewed pin substitution and admitted execution remain pending.

## Proposed safe new entry point

Coordinator authors a new explicit EF1 qualification workflow/tool, for example `tools/build_ef1.py`, taking or freezing exact independently reviewed final core/IO/worker commits and expected distribution versions. It rejects missing/dirty/unpinned inputs, unexpected source/archive files and mismatched dependencies; uses clean source materialization and isolated fresh installations; never builds from another mutable release checkout. Exact repeat-wheel/sdist/source/installed payload closure and actual platform reports bind their own candidate identities.

Run on both declared platforms with fresh declared base and combined intraday wheel/sdist forms. Core/contracts mismatch is rejected. Positive EF1 forms must discover and execute the ten current EF1 test methods with **zero skips, errors or failures**; a dedicated expected-name inventory prevents accidentally discovering zero tests. If tests change, review that inventory with the changed source rather than lowering counts. Absence/version-negative controls run separately and expect typed failures, never a skipped mandatory positive suite.

Include current supplied_original independent numerical fixtures and legacy worker paths, full direct/worker metadata/values/status/quality/evidence/input/config parity, explicit expected backend versiona5, core39/default/optional-import invariance and exact intradaya3 behavior. Then run actual physical DuckDB synthetic A01 trade/bar, A02 covered-empty and missing-notional cases through the fixed adapter factory, bounded worker and existing sink. Require full canonical persisted bytes/receipt/readback, replay identity and documented uncertain/cancel cases; open the closed exported HTML in a browser after sink closure. Installed typing, version/dependency assertions and relevant existing regression checks remain separate gates.

The new lane must qualify changed SDK/result serialization using the unchanged canonicala4 wire; a Python distributiona5 does not rename the wire package version. Performance claims require a separately admitted measured workload/resource/parity protocol; no new timing is requested here. All local execution remains held during the official R18 resource reservation. Current source review, coordinator review before push, fresh final-head PR review and release/equity-scope gates remain required.

## Coordinator-reviewed source pins and installed lane — October20,2026

Supersedes the placeholder/pin-conflict preparation snapshots above. Current foundation, supplied-input and EF1 workflows now select exact reviewed core1fa5601b74cbafdf79af4d740c0f813eebec0f4c and IOeddd0d35e9715b73c859fc68f5a00de56793dbf7. Source authoring is committed/published in their linked draft PRs; numerical and artifact acceptance remain pending. Historical companion CORE_COMMIT/IO_COMMIT constants remain unchanged. Current worker candidate is exacta15/featuresa5 with contractswirea4; the legacy supplied backend literal changes explicitly to a5 as documented, superseding the initial unmodified-source snapshot.

Coordinator-authored tools/qualify_ef1.py and .github/workflows/ef1.yml build eight exact candidate packages and exercise four fresh wheel/sdist/mixed installed forms on Windows/Linux. Mandatory nonempty/nonskipped anchor+legacy suites, package-origin/version probes, dependency checks and physical DuckDB/readback/HTML examples are prospective. Independent source review caught and corrected a spawn-bootstrap recursion defect in its generated test runner. No execution success is claimed. Local tests/builds remain held during the separate official EQ142 study; remote CI is the admitted execution route for this candidate. Exact final-head review, CI, installed parity, full inventory and remaining producers stay required.

## Coordinator clock successor qualification

The successor keeps anchor and legacy qualification mandatory and adds exact independent suite populations: core anchors21, core clocks13, I/O anchors16, I/O clocks12, workers anchors10, workers clocks8, legacy supplied13. Every suite must execute exactly its expected population with zero skips. Fresh four-form installations probe installed clock/adapter module origins. Each form runs the original two anchor readbacks and seven clock readbacks: C10/20/30/40 present proof plus C20 missing/unknown/future original proof. Exports are generated only after physical sink readback; a generated HTML file does not count as browser inspection. The known inherited anchor fixture/demo now explicitly request completed_eod. No local runtime execution occurred; exact source pins and current Windows/Linux evidence remain required.

Clock bootstrap pins now select independently source-reviewed core eb062201826fb9b5296c980fa9d86e0663a0e127 and I/O8b84ddd88a6185db8da503102f89dfe00fc67c12 in the current foundation/supplied/EF1 workflows and builders. Historical companion constants are unchanged. The standalone core workflow will subsequently select this reviewed clock worker commit to qualify its final current core; no cyclic source identity or floating branch is used. These pins establish source selection only, with fresh exact-head review/CI still required.

## Current-state status installed qualification

Coordinator extends the existing four-form helper with exact26 library/18 I/O/19 worker status tests plus the retained anchor/clock/legacy suites. All test populations must execute exactly with zero skips. The installed-origin probe includes the status worker module. Each form adds S01/S02/S03 physical DuckDB/readback and closed HTML exports; original input bindings/event witnesses must roundtrip unchanged. Browser inspection and the original observable history/open diagnostics are separate pending gates. No local runtime was executed; source pins and actual Windows/Linux CI still require final review and qualification.

## Supplied status bootstrap qualification

Current foundation, supplied-input and EF1 lanes select reviewed status core b7dd36b2d523dcf5139900decaa7c62e6353dcbe and I/O22de254d6cd0ad79bbe746e9484ba4f64dc616a1. Historical companion constants remain unchanged. The core standalone lane subsequently pins this worker candidate and qualifies its actual current core checkout, preserving a finite immutable dependency graph. This selects source only; installed status numerical qualification remains pending.

## Unambiguous suite receipts

Downloaded clock532 receipts used test filename alone as a JSON mapping key, so core and worker clock suites collided in the summary even though logs show both actually executed. Status would have the same collision. Qualification receipts now preserve every suite as a layer/directory/pattern/expected record. Exact execution counts and zero-skip guards are unchanged. Earlier receipts remain historical with this reporting limitation; do not infer an absent execution from their overwritten summary key.

## Status readback fixture identity correction

Actual core70ea installed Ubuntu job119011247495 passed the preceding suites then failed one status worker test comparing supplied item order to readback order. The unchanged supervisor returns canonical task-SHA order. The corrected test asserts the exact task identity set/count and compares each full result with the original item selected by task_sha256. All full-result equality and replay assertions remain; runner/library/sink behavior and19-test population are unchanged. This is an unexecuted source correction pending independent review and applicable local validation under the protected resource hold.

Current status foundation/supplied/EF1 lanes now select independently reviewed I/O29956fd6b7c753ba8e3f79e586b0a89e09aec271, whose sole fixture annotation/hash correction preserves source data. Core bootstrapb7dd36b2d523dcf5139900decaa7c62e6353dcbe and historical companion constants remain unchanged. This pin selection and task-identity fixture correction are source-only; local qualification is still unexecuted under the protected hold. Hosted Actions completion is not a merge prerequisite under current owner policy.
