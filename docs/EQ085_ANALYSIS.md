# EQ085 actual synthetic attribution

Author full protocol completed on a fresh installed Windows CPython3.12.10 environment. Raw report sourceworkerf1b228c94acf6dff7bbcf3e25210fba317f4234f/corehelpera0c5f88ac7cab0f89e34ec6bd660e3763bc6b977; report4759842bytes/SHA256df84c1d149fabe8454d4d70504c4fd9d684d81fdb05a78fe9e95c954f2381d49. Three raw unprofiled repetitions per six pipeline cases plus excluded correctness references and separate diagnostics are retained. Complete encoded result/status/quality/evidence/input identity and independent goldens passed, as did all core conversion/codec roundtrip and ownership checks. This is observed engineering data, not a speedup, workload-capacity or hosted qualification claim.

## Whole sequential pipeline observations

| Workload | Sink | Rows | Median ms | Min–max ms (3samples) |
| --- | --- | ---: | ---: | ---: |
| small | parquet | 16 | 4844.471 | 4510.140–4889.354 |
| small | duckdb | 16 | 16647.391 | 15837.542–16716.048 |
| large | parquet | 4096 | 20785.580 | 20100.876–21715.514 |
| large | duckdb | 4096 | 57763.440 | 55517.981–59612.930 |
| skew | parquet | 2528 | 20609.062 | 18645.431–20771.334 |
| skew | duckdb | 2528 | 55568.335 | 54592.354–60135.300 |

Includes the public fixture pipeline's source setup/acquisition, sequential worker compute, serialization, physical publication/readback, generation/catalog selection and sink close. Excludes imports/setup preceding its pipeline_start marker. Different new output roots were used; one shared owned source per workload, warmed reference and uncontrolled OS page cache/background scheduling. Timing is not an isolated storage-device or calculation cost.

## Independent compute/conversion/codec observations

| Trades | Skew | Checked aggregate ms | From Arrow ms | From NumPy ms | Encode result ms | Decode result ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 128 | false | 1.371 | 1.691 | 0.560 | 1.301 | 2.133 |
| 16384 | false | 140.545 | 166.793 | 62.720 | 1.582 | 1.753 |
| 16384 | true | 147.696 | 186.496 | 63.338 | 1.669 | 2.047 |

These are separate calls with their own boundaries and three retained raw samples; do not add them to pipeline elapsed. Existing integer/Fraction trade/history, tie/stream parity and conversion mutation checks govern correctness. Exposed buffer bytes are not hidden copy totals, and process-lifetime high-water is not calculator allocation. Governing history512 diagnostic includes fixture/reference/check/timing helper work; its top-level cost is not a pure history-kernel time.

## Measured targets and decision

The diagnostic summary preserves top self-time project functions, all raw call counts and nonadditive cumulative fields. The concrete codec hypothesis is the repeated surrogate-character validation path in equity_feature_io_sdk/codec.py:_string and its generator. It scans strings through Python ord/any; the actual reports permit checking call multiplicity and caller-visible codec latency. A semantics-preserving ASCII fast path is a candidate for a separately scoped comparison: retain valid non-ASCII text, reject surrogate codepoints, preserve canonical bytes and all negative codec/receipt/identity fixtures. No implementation or predicted benefit is claimed here.

| Workload | Sink | `_string` calls | Character-generator calls | Generator self seconds (diagnostic) |
| --- | --- | ---: | ---: | ---: |
| small | parquet | 164852 | 2589332 | 1.3082 |
| small | duckdb | 175140 | 2719324 | 1.6116 |
| large | parquet | 659408 | 10377872 | 5.2059 |
| large | duckdb | 700560 | 10898736 | 5.7902 |
| skew | parquet | 659408 | 10311746 | 5.0444 |
| skew | duckdb | 700560 | 10829758 | 5.6755 |

These call counts identify the measured target; the diagnostic seconds do not
predict baseline savings. At16384rows, checked inward Arrow conversion medians
166.793–186.496ms are comparable to checked aggregate medians140.545–147.696ms
in separate call measurements. A second concrete target is validated canonical
materialization, preserving input ownership/mutation isolation, precision and
metadata rather than assuming a zero-copy representation. No pure allocation or
copy-only cost is inferred from the complete conversion calls.

Opaque native read/stat/connect calls and orchestration/publication also appear among expensive diagnostic frames. Imports and setup are inside the diagnostic call window, so these rows cannot all be assigned to physical source/storage I/O. A connection/repeated-verification attribution study is a candidate only with caller-specific boundaries and unchanged source/receipt/lock semantics. No caching, weakened revalidation, parallelism or sink-lock changes are made.

The bounded evidence does not justify selecting a new Rust calculator backend for these end-to-end fixtures. It identifies specific conversion/codec and wider pipeline work to measure first. Native calculator acceleration remains conditional; no universal statement about other workloads follows. EQ086/EQ088 retain their own readiness/acceptance and scope decisions. This story changes no numerical algorithm, public API, package dependency, default worker policy or private/source admission.

## Interpretation and delivery limits

cProfile diagnostics introduce Python-side overhead and use the whole checked call window; baseline pipeline elapsed uses its internal marker. Difference/ratio is not profiler overhead alone or identical-boundary speed comparison. Self-time categories are profiler attribution; cumulative children/recursion are nonadditive. Native calls remain opaque, unclassified frames remain visible, and no native CPU/stack/allocation decomposition or hardware-independent rank follows. Fresh bothOS installed-form smokes establish compatibility/parity, not cross-platform performance.

Raw measurement, summary, methodology, installed provenance and separate review/artifact receipts must be reconciled before final acceptance. Actual source-head Windows/Linux wheel/sdist qualification and final main delivery are distinct. R7 remains parked8/10; its automation PAUSED and EQ084 waits. No private provider/hosted deployment/registry/stable publication is involved.
