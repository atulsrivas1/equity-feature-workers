# EQ088 scheduling and output qualification

[Canonical story99](https://github.com/atulsrivas1/equity-features/issues/99) / [protocol](https://github.com/atulsrivas1/equity-features/blob/91045a2ba1157cf94c5d7cf797fb1825c4bbe586/docs/stories/EQ-088_PROTOCOL.md). Qualification-only tool, not a numerical/worker/sink API or default change. Core20c08c/I-O19aa266 package versions and production package trees remain unchanged. No math, timing, schema, receipt, ownership, publication, optional dependency or threading policy changes.

Run a frozen committed checkout in a fresh installed environment using Python3.12:

```text
python -I tools/qualify_scaling.py --work-root NEW_OWNED_ROOT --out NEW_REPORT_JSON
```

Both paths must be new, separate from each other and the checkout. Full protocol is8untimed references plus72planned pipeline samples,6sort-policy diagnostics and18output-layout queries. --smoke runs only small sequential pipeline and tiny diagnostics; smoke is not performance/capacity/spill acceptance. All product module origins must be installed. Tools reuse accepted pilot source/goldens/parity/owned process cleanup; new qualification-only capacity fixture64*256 is16384rows. Source artifacts, installed RECORDs, actual interpreter/host, raw samples/counters and failures bind reports. Pipeline timeout180s; each diagnostic child120s. Root disk admission1GiB is observed between samples; engine source spill temp cap512MiB is distinct from hardRSS/disk quotas.

Sort correctness checks complete unique ID population, exact independent MD5 payloads and global key order. Sink128MiB zero-spill remains unchanged; source32MB/256MB positive spill/control must be actually observed to fulfill acceptance. Current closed Parquet output cells deliberately have no statistics. Three synthetic projection layouts retain all fields/types/metadata/facts: current interleaved/no-stats, sorted/no-stats, sorted/stats diagnostic. No receipt/artifact is rewritten. Native counters/ranges do not prove physical disk reads. Wider worker adoption requires fixed full-protocol criteria and separate explicit reviewed source change; this tool never changes defaults.

Foundation CI now runs six pure evidence tests and tiny installed diagnostics in each fresh wheel/sdist environment on Windows/Linux; raw smoke reports are artifact entries. Relevant actual artifact/source/RECORD maps, separate exact-head review and actual-main checks/readback remain required. Full author run and interpretation are separate, currently unexecuted. No timing claim or R8 acceptance.

First pure evidence-test invocation found platform path-separator expectation mismatch in public-profile redaction (4passed/1failed). Correction normalizes the redacted owned-root separator; original failure retained here. No native run occurred. Earlier readiness review found four design gaps resolved in protocol revision2 before Ready; see canonical history. No failed run is discarded or promoted to PASS.

Separate initial source review00c3daef found four blockers before execution: omitted data_page_size/store_schema, missing prelaunch failure identity/private timeout journal, worktree bytes not checked against frozen Git source, and empty/missing installed RECORD accepted as a digest. All corrected before native invocation; exact renewed review remains mandatory. Attempt journal commits public identities before launch, keeps sanitized failure/timeout/cleanup-unknown status and private traceback hash; raw traceback paths remain outside public report.

First fresh Windows native smoke at57d4664 FAILED after2untimed references and2measured small sequential sink cases all passed full parity/goldens. The first512-ID sink diagnostic raised DuckDB ParserException before query execution: profiling_output `.json` set while current profiler format remained query_tree. Corrected setup enables JSON first, then sets the owned output path; sink policy remains unchanged. This is instrumentation failure, not a calculation failure or spill result. Preserve raw report/log identities below; renew exact source review before changed smoke on new roots. Helper-only raw source audit first rejected core WindowsCRLF, then verified exact permitted LF materialization separately; missing-sys venv helper stopped before creation and was corrected by direct command.

```json
{
  "smoke.json": {
    "bytes": 53192,
    "sha256": "145e5068e99533e35881be1e87a8fff1c38e72781e46306b4bfe4b77bc0299bf"
  },
  "smoke.log": {
    "bytes": 1505,
    "sha256": "788c3b453e127c946077693e22ca3bbe05ac40fffea26ab35335611012ada7a9"
  },
  "smoke-work/failed-sort-sink-128MiB-512.log": {
    "bytes": 1728,
    "sha256": "de3a19ea76cd9b419cd533793619e8da25ae704fa60590be58356aacba85d089"
  },
  "smoke-work/failed-attempt-5.log": {
    "bytes": 919,
    "sha256": "6a6ec14b7655c9caea851349690f523fafce3be8967cf26a87883dfa6c055a45"
  }
}
```

Second changed-source native smoke7e2a57d FAILED aftersame2references/2smallcases full parity. SETorder issue resolved; first sink512sort now raises DuckDB InternalException unique_ptrNULL during fetchmany. Exact cause unconfirmed, no spill/sort/pruning PASS. New qualification-only capability probe fixes population16 and three controls: sink/unprofiled/hash, source/profiled/hash, sink/profiled/integers. Original settings/owned120s child preserved; raw individual engine exceptions retained privately and hashed, errors never relabeled success. Separate exact capability source review before any invocation; no production setting relaxation or unchanged failed rerun.

```json
{
  "smoke-02.json": {
    "bytes": 53209,
    "sha256": "5c225617254451adb72a8d4ef4a7e6ad083dd490c6a52eeb5bea6c2408b0e679"
  },
  "smoke-02.log": {
    "bytes": 1505,
    "sha256": "788c3b453e127c946077693e22ca3bbe05ac40fffea26ab35335611012ada7a9"
  },
  "smoke-work-02/failed-sort-sink-128MiB-512.log": {
    "bytes": 2412,
    "sha256": "8a06e3bf6c417670c4a1765cf2fd7d86ebbc9099fb67008fad6a311b3c4fe582"
  }
}
```

Before any capability execution, separate source advice expands the original unexecuted3-control proposal to fixed6independent16-ID children: sink/source-policy × no-profile/JSON with identical hash SQL/parameters/fetch/oracle, plus sink integer-sort no-profile/JSON. Source control128MiB matches sink memory to reduce confounding; source temp512MiB/external access still differs and cannot alone prove one setting is causal. Each child15seconds plus existing owned cleanup; phases recorded privately before/after execute/fetch/EOF/connection close. New roots, no adaptive extra cases or unchanged512 rerun. No capability result yet; exact pre-execution review required.

Actual6-control native run at a88b43f completed: sink/source unprofiled hash, source profiled hash and sink unprofiled integer all verified complete16-ID results; sink profiled hash and integer both InternalException at fetching. Every connectionclose returned, nativeowned observer completed. This establishes association with profiled sink policy, not a causal proof of one setting or an upstream bug diagnosis. No production configuration/dependency relaxation. Capability reportSHA256 03c4bc1b94550ae657819457f4e78383608aea7c02b3e18cbe213313818efce5.

```json
[
  {
    "index": 0,
    "sink": true,
    "profiled": false,
    "hashes": true,
    "status": "completed",
    "error_type": null
  },
  {
    "index": 1,
    "sink": true,
    "profiled": true,
    "hashes": true,
    "status": "failed",
    "error_type": "InternalException"
  },
  {
    "index": 2,
    "sink": false,
    "profiled": false,
    "hashes": true,
    "status": "completed",
    "error_type": null
  },
  {
    "index": 3,
    "sink": false,
    "profiled": true,
    "hashes": true,
    "status": "completed",
    "error_type": null
  },
  {
    "index": 4,
    "sink": true,
    "profiled": false,
    "hashes": false,
    "status": "completed",
    "error_type": null
  },
  {
    "index": 5,
    "sink": true,
    "profiled": true,
    "hashes": false,
    "status": "failed",
    "error_type": "InternalException"
  }
]
```

Corrective qualification runs sink sorts UNPROFILED under exact originalCONFIG, marks native peak-temp profiling unsupported/null and retains elapsed/RSS/current scratch inventory plus full ID/payload/order oracle. Source profiled32MB/256MB positive spill/nonspill control remains required and unchanged. No missingcounter becomes0/PASS. Sixcontrol driver supported launch is trusted parent `python -B tools/probe_scaling_capability.py`; actual child -I remains; unsupported parent-I --help bootstrap failure was source-only before native, preserved in review05fe69ff. Renew exact corrective/evidence review before new smoke/fullprotocol.
