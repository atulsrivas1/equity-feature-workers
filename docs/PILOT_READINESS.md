## Completed physical measurement — October 8, 2026

The full frozen protocol passed:54 configurations,162 measured samples,three repeats each,zero skipped and six excluded warm references. All three owned synthetic populations retained original file/catalog identities, five independent trade goldens and full encoded numerical/status/quality/evidence/input parity across both sinks and every execution configuration. The [raw report](EQ066_BENCHMARK.json) binds exact source 0fad00d8d928165833cf2e87316455fb2750c406 and56 scoped hashes; public UTF8 LF SHA256 `51141ad524d47c72574e5d304d31c5bd5196fdb955d22125ccd899a17e486c99`. This is configured synthetic engineering qualification; installed/native/release/actual-main acceptance is separately recorded in the linked receipts.

### Measured starting configuration

Use sequential execution with one compute worker and one backend thread as the bounded starting configuration for these fixtures. Pool configurations remain explicit opt-in comparisons requiring measurement of the target workload. The table records actual faster/slower configurations and variability rather than assuming more workers help. This three-repeat local experiment does not establish a universal production scaling/default or private capacity bound. Keep one serialized publisher, declared input/result/reporter limits and existing sink write ownership; do not increase worker or backend threads merely to match available CPU count. No runtime API or numerical default is changed by this documented recommendation.

All24 process configurations had slower median pipeline elapsed than their corresponding measured sequential1 baseline (ratio range0.778–0.995). Thread ratios ranged0.861–1.200; every fixed thread count was slower for at least one workload/sink pair. Sequential1 peak sampled job RSS was108.2–155.9MiB, versus200.1–323.7MiB for process8. This supports the bounded sequential1 starting choice for these fixtures while preserving measured individual thread improvements and their variability.

### Hardware and interpretation

Windows11 AMD64, CPython3.12.10, two Intel Xeon Platinum8160 CPUs at2.10GHz;96 logical CPUs,48 affinity/admitted CPU slots;physical RAM 273400750080 bytes and initial available RAM 237198262272 bytes. All declared CPU/RAM/logical admissions passed. RAM admission is the predeclared observed-memory heuristic, not a reservation. Actual per-sample component versions, effective source/request batch bounds, budget reservations, source/input revisions, process identities, bytes, stage arrays and probe counts remain in the report. Fresh coordinators and outputs reuse the same warmed input files; OS cache, background load and thermal state were uncontrolled. No cold-disk, linear speedup, hard RSS or production extrapolation claim.

Pipeline seconds include source/sink construction, acquisition, workers/IPC, computation, serialization/publication/full receipt readback, generation/catalog verification and sink close. Task latencies span source preparation to verified first full read return. The sampled RSS peak is the largest simultaneous live process-tree frame observed over the three repeats, including launcher/coordinator/children; shared pages can be counted more than once and short-lived peaks can be missed. Actual intervals, scan windows, counter disappearance/errors and native per-process high water remain visible; an unavailable counter is not zero. Stage durations overlap and are not summed into wall time; queue/backend probes do not establish isolated lock-blocking or stress-contention duration. File lengths, canonical pickle and result codec bytes are distinct from physical disk-read or peak disk growth.

### All measured configurations

Each row has three verified samples. Range/median is pipeline seconds; throughput is rows divided by median pipeline seconds. Ratio is measured sequential1 median divided by that configuration median, with values above1 indicating lower elapsed time on this fixture. Task p50/p95 combines verified task latencies across the three repeats. Peak MiB is sampled simultaneous job RSS, not an enforced cap.

| Workload | Sink | Mode/workers | Seconds min / median / max | Rows/s at median | Seq1 ratio | Task p50 / p95 seconds | Peak sampled MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| small | parquet | sequential1 | 3.260 / 3.687 / 4.085 | 4.3 | 1.000 | 0.948 / 1.744 | 153.4 |
| small | parquet | thread1 | 3.648 / 3.984 / 3.998 | 4.0 | 0.926 | 0.976 / 1.887 | 150.8 |
| small | parquet | thread2 | 3.343 / 3.434 / 4.165 | 4.7 | 1.074 | 0.972 / 1.831 | 152.6 |
| small | parquet | thread4 | 3.897 / 4.055 / 4.272 | 3.9 | 0.909 | 1.083 / 1.975 | 147.5 |
| small | parquet | thread8 | 3.358 / 3.448 / 4.191 | 4.6 | 1.069 | 0.997 / 1.848 | 152.3 |
| small | parquet | process1 | 3.933 / 3.939 / 4.067 | 4.1 | 0.936 | 1.527 / 2.285 | 153.5 |
| small | parquet | process2 | 4.383 / 4.741 / 4.830 | 3.4 | 0.778 | 1.608 / 2.598 | 151.6 |
| small | parquet | process4 | 4.003 / 4.062 / 4.765 | 3.9 | 0.908 | 1.644 / 2.468 | 199.7 |
| small | parquet | process8 | 3.972 / 4.075 / 4.110 | 3.9 | 0.905 | 1.579 / 2.304 | 201.1 |
| small | duckdb | sequential1 | 9.618 / 10.076 / 10.512 | 1.6 | 1.000 | 2.573 / 4.619 | 108.2 |
| small | duckdb | thread1 | 9.669 / 9.709 / 10.170 | 1.6 | 1.038 | 2.563 / 4.496 | 107.3 |
| small | duckdb | thread2 | 9.712 / 10.087 / 10.102 | 1.6 | 0.999 | 2.607 / 4.548 | 109.8 |
| small | duckdb | thread4 | 9.470 / 9.562 / 10.022 | 1.7 | 1.054 | 2.539 / 4.415 | 109.0 |
| small | duckdb | thread8 | 9.680 / 9.755 / 10.128 | 1.6 | 1.033 | 2.589 / 4.505 | 108.8 |
| small | duckdb | process1 | 10.345 / 10.487 / 10.649 | 1.5 | 0.961 | 3.216 / 5.079 | 111.6 |
| small | duckdb | process2 | 10.129 / 10.687 / 10.764 | 1.5 | 0.943 | 3.194 / 5.167 | 141.2 |
| small | duckdb | process4 | 10.129 / 10.408 / 10.590 | 1.5 | 0.968 | 3.226 / 5.109 | 199.9 |
| small | duckdb | process8 | 10.501 / 10.749 / 10.763 | 1.5 | 0.937 | 3.278 / 5.215 | 200.1 |
| large | parquet | sequential1 | 13.487 / 13.989 / 14.325 | 292.8 | 1.000 | 4.300 / 6.165 | 155.9 |
| large | parquet | thread1 | 13.869 / 15.319 / 16.297 | 267.4 | 0.913 | 4.476 / 6.642 | 155.6 |
| large | parquet | thread2 | 13.395 / 16.184 / 16.300 | 253.1 | 0.864 | 4.505 / 6.426 | 155.8 |
| large | parquet | thread4 | 13.845 / 16.252 / 16.946 | 252.0 | 0.861 | 4.582 / 6.561 | 156.0 |
| large | parquet | thread8 | 15.110 / 15.635 / 15.882 | 262.0 | 0.895 | 4.903 / 7.022 | 155.6 |
| large | parquet | process1 | 14.525 / 16.042 / 17.516 | 255.3 | 0.872 | 5.310 / 7.411 | 159.2 |
| large | parquet | process2 | 15.223 / 15.799 / 17.876 | 259.3 | 0.885 | 5.201 / 7.309 | 156.6 |
| large | parquet | process4 | 13.975 / 14.154 / 14.389 | 289.4 | 0.988 | 4.814 / 6.703 | 204.8 |
| large | parquet | process8 | 15.134 / 17.166 / 17.511 | 238.6 | 0.815 | 5.552 / 7.707 | 323.1 |
| large | duckdb | sequential1 | 39.505 / 40.161 / 40.687 | 102.0 | 1.000 | 10.090 / 17.284 | 112.7 |
| large | duckdb | thread1 | 39.378 / 40.636 / 46.377 | 100.8 | 0.988 | 11.221 / 19.163 | 112.8 |
| large | duckdb | thread2 | 40.366 / 41.167 / 41.420 | 99.5 | 0.976 | 10.247 / 17.462 | 112.5 |
| large | duckdb | thread4 | 40.339 / 41.867 / 42.433 | 97.8 | 0.959 | 10.593 / 17.984 | 112.4 |
| large | duckdb | thread8 | 38.867 / 42.505 / 43.991 | 96.4 | 0.945 | 10.439 / 17.176 | 112.7 |
| large | duckdb | process1 | 40.085 / 40.713 / 41.364 | 100.6 | 0.986 | 10.856 / 18.109 | 119.9 |
| large | duckdb | process2 | 39.663 / 42.252 / 42.279 | 96.9 | 0.951 | 10.668 / 18.193 | 147.3 |
| large | duckdb | process4 | 39.473 / 41.109 / 43.485 | 99.6 | 0.977 | 11.004 / 17.950 | 205.3 |
| large | duckdb | process8 | 41.079 / 42.338 / 42.760 | 96.7 | 0.949 | 11.026 / 18.236 | 323.2 |
| skew | parquet | sequential1 | 13.590 / 16.211 / 17.097 | 155.9 | 1.000 | 4.220 / 6.606 | 155.1 |
| skew | parquet | thread1 | 13.198 / 13.509 / 16.161 | 187.1 | 1.200 | 3.977 / 6.432 | 155.2 |
| skew | parquet | thread2 | 15.619 / 15.946 / 16.003 | 158.5 | 1.017 | 4.253 / 6.684 | 155.5 |
| skew | parquet | thread4 | 15.448 / 16.114 / 20.914 | 156.9 | 1.006 | 4.129 / 6.490 | 155.7 |
| skew | parquet | thread8 | 14.739 / 16.015 / 16.186 | 157.9 | 1.012 | 4.397 / 7.059 | 155.5 |
| skew | parquet | process1 | 16.256 / 16.806 / 20.544 | 150.4 | 0.965 | 5.618 / 10.214 | 156.8 |
| skew | parquet | process2 | 14.389 / 16.362 / 17.009 | 154.5 | 0.991 | 4.787 / 7.190 | 156.7 |
| skew | parquet | process4 | 16.233 / 16.737 / 17.080 | 151.0 | 0.969 | 5.181 / 7.739 | 205.9 |
| skew | parquet | process8 | 16.675 / 16.738 / 17.193 | 151.0 | 0.968 | 5.354 / 7.917 | 323.5 |
| skew | duckdb | sequential1 | 39.499 / 39.958 / 41.254 | 63.3 | 1.000 | 9.799 / 17.582 | 111.9 |
| skew | duckdb | thread1 | 38.464 / 39.807 / 39.926 | 63.5 | 1.004 | 9.385 / 16.861 | 112.7 |
| skew | duckdb | thread2 | 39.974 / 40.052 / 45.391 | 63.1 | 0.998 | 10.899 / 19.882 | 113.4 |
| skew | duckdb | thread4 | 38.402 / 39.606 / 44.923 | 63.8 | 1.009 | 10.759 / 19.230 | 112.7 |
| skew | duckdb | thread8 | 39.075 / 39.566 / 40.997 | 63.9 | 1.010 | 9.609 / 17.293 | 113.4 |
| skew | duckdb | process1 | 40.173 / 40.222 / 41.322 | 62.9 | 0.993 | 10.242 / 17.782 | 117.7 |
| skew | duckdb | process2 | 40.458 / 41.751 / 45.622 | 60.5 | 0.957 | 11.920 / 19.835 | 147.1 |
| skew | duckdb | process4 | 40.388 / 41.054 / 41.853 | 61.6 | 0.973 | 10.537 / 18.247 | 205.8 |
| skew | duckdb | process8 | 39.477 / 40.147 / 41.061 | 63.0 | 0.995 | 10.304 / 17.855 | 323.7 |

### Readiness matrix

| Scope | Status | Remaining admission |
| --- | --- | --- |
| Owned synthetic configured pipeline | Full measured protocol passed; release acceptance recorded separately | Final installed/native/artifact/review/publication/readback gates in delivery receipts |
| Private pilot | NOT_ADMITTED | Individually accepted rights/privacy, retained source truth/identity, PIT/known-at/adjustment policy, governed calendar, required warm-up/math fixtures |
| Corrected month | NOT_ADMITTED | Accepted private pilot plus coverage/correction/cancellation/restart and independently measured bounded capacity |
| Annual | NOT_ADMITTED | Accepted corrected month plus independent worst-case CPU/memory/disk/retention capacity |

Package engineering completion does not admit proprietary production data. Stop bounded R5 before R6; no private generation/providers/services/scheduler or stable registry release.

# Physical pipeline benchmark and readiness

Experimental workers0.1.0a11 tooling, EQ066. Calculations and public worker API are unchanged. The optional actual source is equity-feature-duckdb0.1.0a8/NumPy2.2.6/DuckDB1.5.6; both physical sinks use the fixed accepted I/O4603c6e baseline. These are explicit qualification dependencies, not mandatory worker dependencies. Public examples contain owned synthetic rows only.

## Reproduce

Use CPython3.12, requirements-dev.txt, fixed core20c08c and I/O4603c6e wheels/packages including optional duckdb source; install workers. From a clean committed checkout, run:

```powershell
python tools/benchmark_pipeline.py --out work/pilot-report.json --fixture-root work/pilot-fixtures --output-root work/pilot-outputs
```

All three locations must be new explicit owned locations (the output report parent may exist). The runner rejects an existing report and overlapping evidence/source roots before creating fixtures or outputs. It never overwrites a dataset/output root or deletes prior results. It checks exact committed scoped raw bytes before/after. Use source PYTHONPATH explicitly for author source experiments; that is distinct from fresh installed qualification. tools/build_foundation.py additionally runs installed eight-case pilot tests and a both-sink smoke in each isolated wheel/sdist environment with the source distribution. Full54-configuration scaling is local hardware evidence; CI's minimal tests are not full scaling qualification.

The [pre-code plan](EQ066_PLAN.md) and independent tests/pilot_oracle.json freeze pair100/2+102/3, exact integer totals, rational506/5 VWAP and5/2 mean size, three workloads16/4096/2528 rows,54 configs/162 repeated samples, seed766/rotations and fixed identities before implementation. Actual request batch limit=min(256,instrument rows), satisfying the existing request contract; source batch cap256 and max_batches=ceil(rows/256). Record each effective request limit. No largest-trade feature is added.

## Interpretation

Source files/catalog are created outside timing and retained unchanged across configurations. Six reference runs establish full encoded result/status/quality/evidence/input identity parity and warm input files; they are excluded from measured comparisons. Raw diagnostic reference timings may still be recorded. Ratios compare the three measured sequential1 repeats. Fresh processes/outputs do not imply cold disk; OS page cache, background load and temperature remain uncontrolled. CPU admission requires workers+one coordinator/backend slot within observed affinity/logical capacity. RAM admission uses the explicitly reported initial available-memory snapshot and conservative1GiB+workers*256MiB heuristic; unavailable RAM is unavailable admission. Actual logical supervisor preflight remains authoritative and separate from observed RSS.

Pipeline elapsed starts before physical source/sink construction/acquisition and ends after successful immutable generation/catalog/full content/golden verification and sink close. Isolated process wall also includes imports, preparation/report/identity checks and external sampling overhead. Per-task latency starts before its source setup, ends at its first successful full sink-read return, and enters distributions only after original receipt verification, final parity and unchanged-input assertions. containing-group diagnostic elapsed is not substituted. Overlapping stage arrays/probe counts are not job elapsed or isolated backend lock-blocking time. Generation publication and catalog selection plus verification are separately timed; extra direct verification uses no recorder attempt. One prepare/supervisor/generation/catalog observation per task uses64 attempts/896 spans/360960 reserved bytes at16 tasks, with a fresh recorder and no extra observed retries/factories.

Input original/optimized/catalog file lengths, canonical pickle bytes, result codec bytes and final closed output file lengths are reported separately; none is physical bytes read, peak disk growth or allocation-block accounting. Five literal trade goldens plus full encode_result parity preserve unavailable/status/quality/evidence/input metadata. Existing independent bar500/51200/102.6 and history.2/-.2/0 suites remain separate acceptance evidence.

## Memory observation

An external sampler observes the spawned coordinator's process tree at nominal10ms, reports actual intervals/scan windows/error counts, per-process PID+creation identities, native per-process high waters and peak sampled sum of live resident counters. Windows virtualenv python.exe may be a launcher: include launcher/helpers and require the actual coordinator PID reported by the trusted child to have positive observed samples. The independent allocation test reads the actual allocator PID, not its constant-size launcher, and touches64MiB before verifying an independently observed resident increase. Literal simultaneous frames verify180 versus the incorrect sum-of-separate-peaks200.

Windows [PROCESS_MEMORY_COUNTERS_EX](https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex) supplies WorkingSetSize/PeakWorkingSetSize (PrivateUsage is commit, not RSS); [PROCESSENTRY32W](https://learn.microsoft.com/en-us/windows/win32/api/tlhelp32/ns-tlhelp32-processentry32w) supplies snapshot ancestry. Linux [/proc](https://www.kernel.org/doc/html/v6.8/filesystems/proc.html) supplies VmRSS/VmHWM and stat parent/start identity. Sequential scans only approximate simultaneous memory; shared pages can be double counted, short peaks/children and ancestry races missed. Preserve unavailable counters, never impute zero. Cap sampler bookkeeping at20000 frames/256 identities and reject truncated/fatal/unavailable-root qualification. The external sampler is excluded from process-tree RSS but can affect CPU timing. Neither logical wire reservation nor observed RSS is a hard process/job RSS limit.

## Coordinator ownership

Windows creates the root with [CREATE_SUSPENDED](https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags), holds its creation identity and verifies membership in a noninheritable [kill-on-close job](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_limit_information) before any instruction runs. Exactly one primary thread is enumerated and held with suspend/resume+query rights; its owner PID and the original held root's live state are checked, and [ResumeThread](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-resumethread) must return previous count exactly1. Snapshot/thread cleanup succeeds before sample settings. All launcher/interpreter/pool children inherit verified job ownership, with no breakaway/UI/RSS flags. Rejected [nested-job assignment](https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject), missing identity or invalid resume fails qualification before input. A failed job close retains its identity, attempts every held-process fallback and one bounded job-close retry; the original Popen process handle covers prelaunch query failure. API failure remains qualification failure even if fallback succeeds.

Linux uses a new session/group and anchors failure signaling to a retained creation identity; an unavailable anchor is a cleanup failure, never permission to kill a reused numeric group. Timeout/interruption/initialization errors retain pipe ownership, bound each drain at five seconds, stop/join sampling before copying immutable retained identities, finalize sampling again after cleanup errors, and preserve the original exception with notes. Unavailable inherited pipes remain explicit failures; buffered output is not closed under a blocked reader. The Windows-specific native fault method is explicitly skipped on Linux; both platforms still run timeout/terminal-root/initialization/unrelated-process and source/sink parity checks. Trusted samples spawn calculation pools only after settings. Process lifetime control is separate from a hard memory cap.

## Historical readiness

Synthetic configured engineering pipeline: full measured protocol passed; installed/native/release/readback qualification is recorded separately in current delivery receipts. Private pilot: NOT_ADMITTED until source-specific rights/privacy, identity/retained truth, PIT/known-at/adjustments, calendar and required warm-up/math evidence are accepted. Corrected month: NOT_ADMITTED until that pilot plus independent correction/coverage/cancellation/restart and measured bounded capacity. Annual: NOT_ADMITTED until accepted corrected month plus worst-case CPU/memory/disk/retention capacity. Do not extrapolate synthetic throughput into proprietary generation approval. Package completion without proprietary production admission follows PUBLIC_DEVELOPMENT.md. No private jobs/providers/scheduler/new acceleration or R6 scope here.
