# Supplied daily observations worker

Bounded original EQ166#506, conversion EQ212#570 and orchestration EQ213#571,
EF1 epic569/milestone34. This additive opt-in follows the frozen
`ef1-daily-context2/daily-observations-v2/ef1-daily-map2` contract. It does not
establish original source-profile parity, rights, full EF1 audit or release.

## API and layer boundary

Import `equity_feature_workers.equity_daily_observations` explicitly:

- `daily_observations_headers(config)` discovers all four sorted feature headers.
- `required_daily_observations_inputs(config)` returns deduplicated requirements.
- `make_ef1_daily_observations_work(config, *, inputs, batches, instrument_id,
  job_id, generation_id, partition_id, destination_scope, max_input_bytes=1048576)`
  creates an admitted work item.
- `compute_daily_observations_inputs(task, batches)` returns the complete result
  in a one-result tuple.
- `equity_feature_workers.ef1_runner.run_ef1_daily_observations(items, sink, *,
  requirements, budget, publication_limits, cancellation)` uses the existing
  sequential supervisor, serialized publisher and complete canonical readback.

The family is exactly `daily_observations`; config identity
`ef1.daily_observations`, parameter `ef1_context`, algorithm
`daily-observations-v2`, context `ef1-daily-context2`. The public decoder and full
config rebuild admit the frozen context. Feature distribution0.0.4a5 and canonical
contracts0.0.4a4 remain exact; result backend is `ef1`/0.0.4a5.

Declarations are exactly six roles, in order: calendar_manifest, calendar_slots,
observation_coverage, raw_observations, common_observations, basis_provenance.
The unchanged generic admission receives only actual present CanonicalBatch
objects in that order, retaining present empty batches. After that admission,
the worker mechanically reconstructs six nullable arguments to the actual pure
`compute_ef1`. It does not infer roles from fact kinds, invent missing inputs,
select observations or implement raw admission, currency conversion or arithmetic.

Only the target interval is governed by the task; the window remains count1.
Historical252/366 inputs remain REFERENCE rows with original metadata and do not
expand the generic task calendar. Payload admission is at most1048576 actual
pickle protocol5 bytes of the present-batch tuple, separately from the unchanged
encoded TaskManifest bound. Six declarations, four scalar cells and at most28
evidence rows are fixed for this route. Returned config, source bindings,
headers, entity, backend, result math policy v1 and declared evidence limit28
are checked; no general scheduler, codec, SDK,
manifest or state guards changed.

SinkRequirements are unchanged generic capabilities: protocol1, efio-json1,
efio-key1, efio-content1, canonical0.0.4a4/schema1/mathv1, retention1,
max_results1, max_chunk_bytes1048576, max_total_bytes1048576,
max_result_cells4, max_evidence_rows28, visibility/writer_mode null. Callers pass
requirements supported by the actual sink and own destination lifetime.

## Independent synthetic fixture and stage accounting

`tests/ef1_daily_observations_oracle.json` is the coordinator-whitelisted,
byte-exact public synthetic derivative, SHA256
`479948037cd6d2fca3901cdb9cf3fcbba6256acbbcb9440bcd68e0362d7688c5`:
119 packets,91 complete independent results and28 stage-specific errors. Private
original source joins and receipts are excluded. This is the coordinator-frozen
V3A1 derivative: one raw-role-missing packet's four quality counts changed from
2/2 to3/3 with dependency slots[0,1,2] and no selected rows. All119 inputs/configs,
statuses/reasons/metadata/evidence,28 errors and118 whole packets remain unchanged.
The original6d95adc fixture and failed-run evidence remain preserved by the
coordinator. Layer authors did not amend the contract or numerical implementation
to fit an oracle. The full declared history is required to prove warmup when
the missing raw role prevents establishing a complete last-N suffix.

Tests compare the full actual result including quality, bindings and evidence;
they also call the real pure producer for every refusal code. Worker translation
normally preserves the existing mapping: inconsistent_identity -> RESULT,
overflow -> CALCULATION, remaining ContractError -> CONFIG. The intentional X212
duplicate packet has actual row_count inconsistent with supplied observed
metadata. Unchanged generic admission therefore returns RESULT before the pure
producer's DUPLICATE stage. Both real paths are asserted separately; no claim
that all28 errors first occur at their pure stage through the worker is made.

Additional checks cover wire/SHA, exact pickle5 B/B-1 admission, absent versus
empty role identity, source metadata substitution, foreign callbacks, full result
identity, actual process spawn, cancellation, task-SHA-bound readback/replay and
retention of committed receipts on subsequent readback failure. Result-rewrite
mocks verify worker defenses only and are not numerical refusal evidence.

## Independent review correction

Fresh automated reviewer `/root/ef1_daily_worker_independent_review` identified
P2 WORKER-DAILY-01 against the initial uncommitted source snapshot. Its actual
diagnostic established that valid computed result substitutions with declared
evidence_limit27 or29 escaped the worker and were published with full readback;
an unknown math policy escaped the direct API, although the runner's later
generic version guard refused publication. The actual evidence-row count guard
did not close these declared metadata identities. Earlier passing numerical and
regression runs do not establish coverage of this missing boundary.

The route now requires `result.metadata.math_policy_version == 'v1'` and
`result.metadata.evidence_limit == 28`, retaining the actual evidence-row bound.
Task admission also explicitly requires mathv1. The unchanged TaskManifest
constructor already rejects another math policy with VERSION; no claim that an
ordinary differently labeled task can be constructed is made. Generic SDK,
sink, manifest and version guards remain unchanged.

A new regression starts with a real computed FeatureResult and constructs valid
canonical replacements for math_policy_version=`substituted`, evidence_limit27
and29, preserving actual values, quality, evidence and other metadata. A narrowly
scoped pure-callback patch verifies direct API RESULT refusal and actual runner
failed TaskExecution RESULT, absent results/output and no begin/write/commit/read
calls. The runner reports the failure in its execution outcome; it does not
raise a result exception at its outer API. The focused final regression passed
one method in0.174s, exit0. A prior exploratory two-method check also passed but
included an unnecessary postconstruction tamper test, removed after coordinator
clarification; its local log is retained and it is not final-suite evidence.

The frozen119-packet fixture is unchanged. The final suite now has14 methods.
After the core author's reviewed orphan/source-ID/date corrections, the actual
selected source hashes were independently read before composition:
`_daily.py` SHA256
`67c805ff5114773454806ecb14a0594970bbef9f3c71d23bef1234c7cb3596f0`,
`_daily_context.py` SHA256
`17f3e6d6262e5f95c4d853bb0852b193492bc16fd1a1354746f36cca31cd9115`.
The private command `python -B work_daily_runtime.py full` passed all14 methods
in17.222s, exit0, including91 complete worker results,28 actual pure error codes
and outer worker refusals, and the new policy substitutions. Strict mypy across
all36 worker source files passed, exit0. Four fresh actual CLI child invocations
of the example all exited0 against new separate DuckDB files, each verifying
the independent full result, SDK/receipt and physical close/reopen readback.
HTML lengths remained37,685..37,735 bytes. `git diff --check` passed.

The initial reviewed source snapshot remains preserved byte-exact as local
historical evidence (SHA256
`eed0afe6822975c3fbe48a028b64f277294bfb7f59e487653216b4a38b4e0225`).
Its13-method, previous-core and prior regression results are not relabeled as
qualification of the corrected final source. No broad older-regression repeat
was made for the bounded metadata closure; earlier execution evidence retains
its source and scope. The corrected source/log snapshot is prepared for fresh
independent recheck; no recheck approval or committed/installed qualification is
claimed by this author.

## Demonstration

`examples/ef1_daily_observations.py` accepts absolute `--corpus`, `--output-db`,
`--html`, bounded `--run-id` and one of four `--case` values:

- V235-literal-last252
- V201-observed-not-scheduled
- V223-prior-skips-invalid-populated-period
- V240-full-leap-year-literal-history

Use the exact public test corpus above. The example verifies its SHA256, converts
literal rows with the real I/O converter, invokes the worker, writes to a new
separate DuckDB sink, compares the complete independent golden, closes/reopens
the database and verifies the full stored SDK bytes and receipt. Output paths
must be distinct and new. HTML is escaped, at most262144 UTF8 bytes and uses no
JavaScript or remote assets. All calendars, observations, coverage, identities,
knowledge and basis facts are explicit synthetic data. No N-drive reads or
invented original knowledge are used. Browser inspection remains a release gate.

## Actual source validation checkpoint — October21,2026

Windows11 build26300, CPython3.12.10, read-only existing qualification interpreter,
explicit source origins, no bytecode, OMP/OpenBLAS/MKL/NumExpr thread counts1.
Worker baseline3599689ab7a9bbd4c9d9853f45012045cb4355af;
core baseline877f8aea0489da4e3d41e7f04bf551a2e8e4a470;
I/O baseline2d9bab14f54046e771c0b20e92889d9286a8a36b.
All three selected source trees contain current uncommitted daily author work;
these are source-development checks, not exact committed artifact qualification.

Actual initial command: `python -B -m unittest discover -s tests -p
test_ef1_daily_observations.py -v`,13 methods in15.949s, failed with2 failures and2
setup errors. The V318 raw-None quality-fence result differed from its literal
expected2 versus actual3; coordinator independent contract review subsequently
approved V3A1 with the complete3-slot proof fence and separately checked the
literal SDK compatibility. The pure implementation required no workaround.
X212 earlier generic refusal was initially expected incorrectly. The author
corrected its stage accounting and two test setups (generic constructor rejects
a lambda callback; canonical evidence prevents removing all input bindings).
The failed log remains local; it is not acceptance evidence.

Actual focused nine-method worker boundary suite passed in3.637s, exit0, after
those setup corrections. Actual `python -B -m mypy --strict
--follow-imports=silent --no-incremental` on
`equity_daily_observations.py` and `ef1_runner.py` passed,2 source files, exit0.
The local private source-selection driver records origins and baseline heads;
it is excluded from publication. `git diff --check` passed.

Legacy source regression then ran eight unchanged modules: test_manifests,
test_supervisor, test_publication, test_required_inputs, test_commands,
test_supplied_errors, test_supplied_publication and test_supplied_session.
The initial driver omitted the Parquet source:66 actual checks passed and two
module imports failed; that failure is retained. After selecting the current
Parquet source explicitly, all124 tests passed in76.128s, exit0. The existing
foreign-writer publication check returned normally; no process was interrupted
and no locking behavior was changed. This local source run is distinct from
prior coordinator-owned exact-commit/native qualification.

With the final stable daily producer, the private command
`python -B work_daily_runtime.py ef1-regression` also passed124 methods in41.472s,
exit0, selecting test_ef1_anchors, test_ef1_clock, test_ef1_status, test_ef1_paths,
test_ef1_reference, test_ef1_period, test_ef1_observables and test_provenance.
These eight selections preserve the older EF1 source fixtures and provenance
checks; they are separate from the124 generic/supplied checks above.

After the coordinator's exact V3A1 grant and core author's stable-source notice,
the private source-selection command `python -B work_daily_runtime.py full`
loaded `test_ef1_daily_observations` with unittest and passed13 methods in16.972s, exit0:
91 complete worker golden results and28 direct pure refusal codes with actual
outer worker refusals, including X212's earlier immutable-metadata RESULT.
Actual `python -B -m mypy --strict --follow-imports=silent --no-incremental
packages/workers/src/equity_feature_workers` passed36 source files, exit0.

An initial private in-process demo driver called all four example cases with
full physical golden/reopen verification and returned0. Four subsequent actual
`python -B examples/ef1_daily_observations.py --corpus <absolute
public fixture> --case <one of the four IDs above> --output-db <new absolute
database> --html <new absolute HTML> --run-id <case ID>` invocations exited0.
Every case verified its full independent golden, full SDK/receipt and physical
close/reopen readback. Generated HTML lengths were37,720 bytes (last252),37,685
(current week),37,711 (latest prior) and37,735 (full leap-year history).
No browser inspection or installed-platform qualification is inferred.

Exact commit independent review,
build/install, native four-form Windows/Linux qualification, browser and all
original EF1 gates remain outstanding. No benchmark, other-platform,
publication, merge, release or whole-story completion is claimed.
