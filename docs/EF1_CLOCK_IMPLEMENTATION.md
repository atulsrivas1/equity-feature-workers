# EQ213 supplied session-clock execution

Source authoring only; no checks executed. The coordinator authorized the reviewed EQ165 #505 clock subset with worker EQ213 #571 under EF1 epic569/milestone34. Other EQ165/status scope remains unimplemented. This isolated branch starts at worker839e7fb5405b47dff51d00c3f0ef04f31a7c21fe; the old anchor checkout/CI and official R18 qualification remain unchanged.

Normative contract: ef1-contract1 in canonical `docs/ef1/CONTRACT.json`, all nine approved producer boundaries. This slice binds task.family `clock`, canonical config identity `ef1.clock`, one actual-or-missing `calendar_provenance` REFERENCE slot and one complete fixed five-header FeatureResult. Public library builder/decoder/discovery/calculation own mathematical and availability semantics. Worker code adds no clock mathematics, timezone/default calendar inference, wall-clock lookup or fabricated source knowledge.

Planned worker0.1.0a15/features0.0.4a5/contracts0.0.4a4/SDKa2/intradaya3 remain exact. Cross-repository CI/source pin changes belong to the coordinator after sibling source review. No test, import, build, database operation, benchmark, commit or push is authorized by this source checkpoint. Final source review, actual resource admission, installed artifacts/platform validation, full stored parity/browser demo and original EF1 scope/release gates remain pending.

## Authored API and reuse

Explicit module `equity_feature_workers.equity_clock` provides `clock_headers(config)`, `required_clock_inputs(config)`, `compute_clock_inputs(task,batches)` and `make_ef1_clock_work(config,...)`. Headers/requirements come directly from public `equity_features.ef1` discovery. The callback validates exact reconstructed ConfigSpec/digest, fixed header tuple, actual namespace/entity/source binding/row count/kind and governed-session bounds, then returns exactly `(compute_ef1(...),)` after full result identity checks. No worker formulas or result metadata rewriting are introduced.

One role is declared exactly once, present or missing. Present is the actual canonical REFERENCE proof, including complete empty if genuinely observed; missing is no batch plus explicit missing InputManifest. Original known-at is never filled from evaluation time. The calendar JSON is bounded canonical ASCII,4096codepoints/bytes and actual unchanged task preflight/1MiB cap, and includes scheduled_close_ns/early_close. Actual c1 proof/hash/revision/knowledge remain input facts. The public library validates those facts against the exact supplied schedule and determines quality. Scheduled-clock queries have no recurrence/warmup/initialization state; existing config/input identities bind every operand.

`ef1_runner.run_ef1_clocks` uses the same private sequential publication/readback implementation as `run_ef1_anchors`. Both public wrappers remain closed to their respective exact top-level callbacks; passing a clock to anchors or another callback to clocks rejects. Anchor signature/default budgets/callback admission and publication/readback behavior are preserved. The shared implementation retains distinct-config job/generation guards, one sink owner, exact SDK receipt/content readback, pending publisher access and factual receipt retention on cancellation/readback failure. No general scheduler/manifest/codec/resource-default change.

## Independent authored evidence and exact demo

`tests/ef1_clock_fixture.py` independently supplies C01's regular10..30, pre0..10, post30..40, scheduled40/early_true and actual c1 proof over0..40 known5. It does not call the I/O fixture factory or calculate expected feature values. Config windows explicitly use `completed_eod`; default `prior_only` is not equivalent. The coordinator reported that omission failed the separate prior anchor CI candidate; this clock fixture/demo follows the frozen context without weakening decoder checks. The old anchor checkout remains unchanged by this worker.

`tests/test_ef1_clock.py` authors eight mandatory methods with C01's exact expected phase/elapsed/remaining/fraction/early-close values at10/20/30/40; direct/worker/full sink SDK parity; missing/null/future/equal known-at; wrong hash/revision; malformed role/header/binding/history/byte admission; mismatched schedule config; same-request replay and distinct-config scopes; cancellation and retained postcommit receipt on readback failure; closed-anchor-wrapper preservation and wrong result bindings/config/backend. There are no optional candidate skips. Current exact library/API/contract/worker versions are mandatory; tests fail if unavailable.

The in-memory sink fixture validates the worker lifecycle only after actual execution. Physical DuckDB composition is separately authored in `examples/ef1_clock.py`: fixed `synthetic_clock_calendar`/`synthetic_clock_fixture`/`ClockInputs` I/O APIs, explicit supplied cutoffs and proof variants, existing separate-output path guard and DuckDBSink, bounded `run_ef1_clocks`, complete receipt/readback and escaped static HTML written exclusively after sink closure. It performs no source DB/provider acquisition, timezone inference, status calculation, JavaScript numeric conversion or remote asset loading.

After coordinator source/artifact qualification and resource admission, the intended command is:

```text
python examples/ef1_clock.py --output-db <absolute-output.duckdb> --html <absolute-new-clock.html> --run-id C01 --cutoff 20 --proof present
```

Cutoff choices10/20/30/40 match C01; missing/unknown/future proof modes demonstrate unavailable values with stored quality/evidence. Synthetic proof modes are explicit test declarations, never claims about observed source availability. Distinct selected configs use distinct job/generation identities; an identical intended replay keeps the same identity and a new HTML export path. Original source correction/revision rules still apply.

## Remaining gates

All evidence above is **authored and unexecuted**. Coordinator owns final source pin updates and any installed qualification entry-point changes; this worker has not edited cross-repository refs/workflows. Extend future qualification with the mandatory clock suite, sibling pure clock/I/O suites, physical DuckDB C01/C02/knowledge demos and rendered local browser review, while retaining all anchor/legacy suites and their strict counts/no-skip/platform/source/artifact gates. Clock tests do not replace previously pending anchor acceptance or establish whole EF1 delivery. Source recheck, actual execution and full original EQ164/EQ165/EQ169 release acceptance remain required. Status and other non-clock EQ165 producers remain unimplemented.
