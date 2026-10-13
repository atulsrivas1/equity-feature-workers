# Synthetic compute, conversion and pipeline attribution

EQ085's developer tool lives outside all numerical packages. It profiles the
accepted public source/worker/sink pipeline and explicit core conversions using
owned synthetic fixtures. It changes no formulas, API, codecs or worker defaults.

Run from a clean committed component checkout with an installed foundation pair,
accepted I/O/source/sinks, NumPy2.2.6/PyArrow20.0.0/DuckDB1.5.6 and Python3.12.
Use a committed canonical checkout for the existing core benchmark helper:

```text
python -I tools/profile_pipeline.py --work-root NEW_OUTSIDE_REPOSITORIES_WORK_DIRECTORY --out NEW_OUTSIDE_REPOSITORIES_REPORT.json --core-benchmark CANONICAL_CHECKOUT/benchmarks/run_baseline.py
```

The roots must be distinct, new, outside Git checkouts, and must not contain one
another. The tool never deletes or replaces existing data/report files. Successful
publication links a fully written closed temporary file to an absent destination;
a failed run keeps owned synthetic evidence and produces no successful report.
The ordinary trusted developer tool is not a remote execution or sandbox API.

Full protocol: three fixed workloads, two sinks, one correctness reference plus
three unprofiled calls and one separate profiled call per case. Core diagnostics
use128/16384uniform and16384skew trades, explicit conversion/codec calls and512
governed history rows. `--smoke` selects small pipeline and core populations for
each fresh installed form; it still checks both sinks and full parity.

`eq085-profile1` records exact committed harness/oracle/core helper hashes,
installed project Python payload identities and versions, individual raw samples,
complete result digest and independent arithmetic, source/output byte observations,
baseline medians, diagnostic function call counts/self/cumulative time and visible
category mapping. Platform/interpreter and backend controls are retained in the
core baseline report. Package artifacts are separately bound by the foundation
builder's archive manifests and actual native CI receipts.

Profiling overhead is present in diagnostic executions and excluded from baseline
samples. The diagnostic wraps the whole checked `run_pipeline` call, including
imports and setup before its internal `pipeline_start`; baseline
`pipeline_elapsed_ns` starts at that internal marker. These are distinct timer
boundaries. Their difference or ratio is not profiling overhead alone or an
identical-boundary speed comparison. Import-related native stat/read calls cannot
be attributed wholly to source/storage I/O. Cumulative child/recursive time is
nonadditive. Self-time grouping is
profiler attribution, not proof of independent physical resources, native CPU,
copy counts or storage I/O. C extension calls remain opaque; unclassified entries
are retained. Native process lifetime peaks and exposed array buffers have the
existing benchmark limits. Input/output file length is not physical bytes read.
Warm references/uncontrolled page cache/background load forbid cold-disk or
general hardware comparisons. Fixed synthetic tasks are not private-corpus or
hosted service qualification. No timing rank/speedup threshold is asserted.

The foundation builder runs meaningful report tests and the small profile in each
fresh wheel/sdist environment on both Windows and Linux. It retains each JSON and
its SHA256 alongside the package artifact manifest. Numerical source and package
dependencies remain unchanged. Final attribution and any optimization hypothesis
need actual measured reports and completed separate review; EQ086/EQ088 own changes.
