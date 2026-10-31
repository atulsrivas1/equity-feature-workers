# EF3 breadth and adaptive volume worker

This source change implements the worker for [#612](https://github.com/atulsrivas1/equity-features/issues/612) over the
supplied-input adapter (io main `86cbff2`, `equity_feature_duckdb.breadth_adaptive`) and the pure library (core main
`ee92759`, `equity_features.ef3`). It consumes contract F: `ef3-breadth1` (`ef3.session_breadth`), `ef3-obsbreadth1`
(`ef3.observed_breadth`) and `ef3-adaptive1` (`ef3.adaptive_volume`). It contains no count, ratio, proximity, rate,
baseline or acceleration mathematics. Source preparation does not establish story, release or acceptance completion.

## Public API

- `equity_feature_workers.equity_breadth_adaptive`:
  - `make_ef3_breadth_adaptive_work(config, *, features, inputs, batches, instrument_id, job_id, generation_id,
    partition_id, destination_scope)`: builds one work item. Library refusals raised while the item is built surface
    here as worker failures (build-time).
  - `required_roles(config, features)`: the roles the selected outputs declare in the library's own definitions.
  - `compute_breadth_adaptive_inputs` and `admit_breadth_adaptive`: calculation and admission (compute-time).
- `equity_feature_workers.ef1_runner.run_ef3_breadth_adaptive`: a thin wrapper over the shared sequential `_run_ef1`
  (unchanged). The caller owns the sink.
- `examples/ef3_breadth_adaptive.py --case <id> --output-db <new .duckdb> --html <new .html> --run-id <id>`.

## Deviations from the EF3-D template (recorded)

1. **Bound roles.** The library records and validates only the roles the selected outputs need. The task therefore
   binds exactly those roles (`required_roles`), not every supplied slot. Supplied batches for other roles are not
   bound and are never read for that selection. This is the library's own rule, not a worker choice.
2. **No ordered-history warmup.** `ordered_history` is false for all three producers. The manifest rule for ordered
   history requires an initialization digest or warmup, and these producers bind neither: membership intervals travel
   inside the supplied batches and the adaptive stream is one epoch. `initialization_sha256` is `None`.
3. **Result identity.** Identity is checked against the bound (selected) roles, because the library records only them.
4. **Witness labels.** The library labels witnesses `member:<id>` (breadth) and `seq:<n>` (adaptive). The example
   normalizes them to the contract's bare member id and sequence number before comparison; values are not changed.
5. **Sink limits.** The DuckDB sink refuses requirements above its defaults (10 MiB total, 100000 cells and rows). The
   example clamps to them, as the sink requires.

## Example and fixtures

The example builds each contract result fixture through the I/O adapter (`declared_case`, `observed_case`,
`adaptive_case`) with the envelope from `case.config(features)`, then runs one work item per selected output and one
joint item when several outputs are selected. For each run it compares against the frozen contract literal: value
(float bits with sign, or exact int/bool), status, reasons, expected and observed counts, per-output witnesses for
single-output runs, and the flat joint evidence truncated to the result-wide `evidence_limit`. Each run then goes
through SDK readback, idempotent replay and close/reopen.

Results of the manual source run (`PYTHONPATH` over core `ee92759`, io `86cbff2`, this worker): all **99 of 99**
result fixtures matched the contract literal, readback, replay and reopen verified, 0 failed. One run of DB03 failed
with a CORRUPTION error because two processes wrote the same output file at once; run alone it passes with the same
content hash. Each fixture takes about 25 seconds (up to 17 runs each), so the registered results demo takes roughly
45 minutes.

## Refusals (reviewer run, not the author)

The reviewer ran the refusal fixtures through the worker path: 59 of 62 driven. Classification as recorded by the
reviewer:

- Build-time refusals (configuration errors, including RB18 and RB21) surface from the builder as `INVALID_COMMAND`.
- Compute-time data refusals surface as `INVALID_COMMAND`, except `INCONSISTENT_IDENTITY` (mapped to
  `RESULT_IDENTITY_MISMATCH`) and `OVERFLOW` (RB23, RB27, mapped to `CALCULATION_FAILED`).
- Nothing is published on refusal.
- Some inputs are refused by the shared validator already in the I/O adapter (duplicates, RB10, RB11, RB30), before
  the worker sees them.
- RB14, RB22, RO12 and RA11 are not reachable via the worker path.

Superseded by the Refusal coverage (R9c) section below: 62 of 62 driven, `contract_code` asserted.

## Refusal coverage (R9c)

`python examples/ef3_breadth_adaptive.py --refusals` builds each of the 62 refusal fixtures (the contract JSON stores only
the error code and a description, so each violation is built in the example on a valid contract fixture input: DB30,
DB21, OB01, AD06), drives it through the I/O adapter and the worker (admission, then calculation) and calls the library
directly with the same inputs. A fixture matches only when the code reported by the adapter or the worker
(`contract_code`) equals the contract error code and the library raises the same code; a fixture with several
equivalent violations runs each as a variant and matches only when every variant matches (RB15, RB18, RB26, RB28, RO05,
RO06, RA09). Result: **62 of 62 driven, 62 of 62 matched**.

- Stages (variants): 37 at compute, 21 at worker admission, 8 refused by the I/O adapter at build, 4 library only.
- Library only, with the reason: RB22, RO12, RA11 (update, restore, merge: a work item carries no capability request)
  and RB14 (eight or ten role slots: the worker always passes the fixed slot tuple of the producer).
- Adapter-build refusals: RB01, RB02, RB10, RB11, RB30 (the metadata constructor refuses a batch without a unit),
  RO01, RA01, RA06 (shared validator or constructors, typed `ContractError`).
- Findings from the first run (2 of 62 did not match): RB18 (tolerance missing while its output is selected) and RB21
  (unknown feature) were refused by the worker's selection check with no `contract_code`. Fixed in the worker: an
  output of this producer that the configuration does not expose is `invalid_config`, any other id is `unknown_feature`,
  a malformed selection is `invalid_schema`; a coverage or namespace contradiction in admission carries
  `inconsistent_identity`. The transport category is unchanged.

## Readable view

The readable view labelled multi-run EF3 breadth and adaptive cases with the EF3-D prefix labels (prefix length and
series), which are wrong for runs that select outputs. Fixed: those runs are now labelled by the selected output, or
`joint` for a joint run. D's prefix labels are unchanged. Views for AD06, AD07a, OB01, DB05 and XB01 were regenerated
and their labels checked by text.

## Executed vs not executed (honest)

- **Executed by the author:** all 99 result fixtures through the example's sink path, each matching the contract
  literal with readback, replay and reopen verified. The reviewer ran 76 of the 99 through the sink path; the other
  23 were not run through the example's sink path by the reviewer, so they rest on the author's run.
- **Refusals:** 62 of 62 driven by `--refusals`, 62 of 62 matched (Refusal coverage (R9c) below).
- **Results demo:** 99 cases registered; not run end to end here (about 45 minutes).
- **Not done:** a browser inspection of the HTML pages; installed-wheel qualification (source `PYTHONPATH` only); any
  statement about the library's correctness beyond agreement with the frozen contract literal.
- **Registry and grid limits:** the sink's default limits bound each fixture (at most 100 results and 10 MiB per sink).
  The breadth producers bind no grid, so there is no grid digest to register.

## Contract catalogue

Internal, synthetic, pre-release. Generated input and output contract pages (in the io repository) for the producers this page covers; core wins on conflict, and the catalogue computes nothing and names no data supplier. Overview: [catalogue INDEX](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/INDEX.md); use: [CATALOGUE.md](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/CATALOGUE.md).

- `ef3.adaptive_volume`: [input](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef3.adaptive_volume/input.md), [output](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef3.adaptive_volume/output.md)
- `ef3.observed_breadth`: [input](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef3.observed_breadth/input.md), [output](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef3.observed_breadth/output.md)
- `ef3.session_breadth`: [input](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef3.session_breadth/input.md), [output](https://github.com/atulsrivas1/equity-feature-io/blob/main/docs/contracts/catalogue/ef3.session_breadth/output.md)
