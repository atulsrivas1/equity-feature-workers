# EQ088 scheduling and output qualification

[Canonical story99](https://github.com/atulsrivas1/equity-features/issues/99) / [protocol](https://github.com/atulsrivas1/equity-features/blob/91045a2ba1157cf94c5d7cf797fb1825c4bbe586/docs/stories/EQ-088_PROTOCOL.md). Qualification-only tool, not a numerical/worker/sink API or default change. Core20c08c/I-O19aa266 package versions and production package trees remain unchanged. No math, timing, schema, receipt, ownership, publication, optional dependency or threading policy changes.

Run a frozen committed checkout in a fresh installed environment using Python3.12:

```text
python -I tools/qualify_scaling.py --work-root NEW_OWNED_ROOT --out NEW_REPORT_JSON
```

Both paths must be new, separate from each other and the checkout. Full protocol is8untimed references plus72planned pipeline samples,6sort-policy diagnostics and18output-layout queries. --smoke runs only small sequential pipeline and tiny diagnostics; smoke is not performance/capacity/spill acceptance. All product module origins must be installed. Tools reuse accepted pilot source/goldens/parity/owned process cleanup; new qualification-only capacity fixture64*256 is16384rows. Source artifacts, installed RECORDs, actual interpreter/host, raw samples/counters and failures bind reports. Pipeline timeout180s; each diagnostic child120s. Root disk admission1GiB is observed between samples; engine source spill temp cap512MiB is distinct from hardRSS/disk quotas.

Sort correctness checks complete unique ID population, exact independent MD5 payloads and global key order. Sink128MiB zero-spill remains unchanged; source32MB/256MB positive spill/control must be actually observed to fulfill acceptance. Current closed Parquet output cells deliberately have no statistics. Three synthetic projection layouts retain all fields/types/metadata/facts: current interleaved/no-stats, sorted/no-stats, sorted/stats diagnostic. No receipt/artifact is rewritten. Native counters/ranges do not prove physical disk reads. Wider worker adoption requires fixed full-protocol criteria and separate explicit reviewed source change; this tool never changes defaults.

Foundation CI now runs five pure evidence tests and tiny installed diagnostics in each fresh wheel/sdist environment on Windows/Linux; raw smoke reports are artifact entries. Relevant actual artifact/source/RECORD maps, separate exact-head review and actual-main checks/readback remain required. Full author run and interpretation are separate, currently unexecuted. No timing claim or R8 acceptance.

First pure evidence-test invocation found platform path-separator expectation mismatch in public-profile redaction (4passed/1failed). Correction normalizes the redacted owned-root separator; original failure retained here. No native run occurred. Earlier readiness review found four design gaps resolved in protocol revision2 before Ready; see canonical history. No failed run is discarded or promoted to PASS.
