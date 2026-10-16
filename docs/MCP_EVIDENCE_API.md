# Experimental owner-prepared MCP evidence

EQ108 implementation candidate, **unqualified and unreleased**. MCP0.1.0a1 extends the existing server with five tools. [Story208](https://github.com/atulsrivas1/equity-features/issues/208), [contract](stories/EQ-108_CONTRACT.md), [literal expectations](stories/EQ-108_ORACLES.json), [admission](stories/EQ-108_ADMISSION.json), [validation](stories/EQ-108_VALIDATION.json). Current GitHub Project is status authority.

The mandatory dependencies stay client0.1.0a0 and jsonschema4.26.0. The optional `evidence` extra adds exact public client native conversion/IO SDK, evidence0.1.0a2, explanations0.1.0a1 and diagnostics0.1.0a1. Importing the base MCP package/owner module needs none of the extra dependencies. Constructing an owner catalog requires them. No accepted math, result, receipt, content, client or service version changes.

## Prepare outside handlers

The trusted owner obtains a completed public `ResultView`, its exact public `ConfigSpec`, job/result identities and optional pure reports. It supplies permission to share those reports with principals holding the native artifact-read grant. The permission flag is an owner assertion, not a provider entitlement check or source authentication. Public examples use synthetic data only.

```python
from equity_feature_mcp import StdioServer
from equity_feature_mcp.evidence import EvidenceCatalog, EvidenceRegistration

catalog = EvidenceCatalog(profile, (
    EvidenceRegistration(
        "cmd", job_id, result_id, result_view, configuration,
        receipt=receipt, explanation=explanation, diagnostics=diagnostics,
        associations=associations,
        report_sharing_authorized=True,
    ),
))
server = StdioServer(client, profile, evidence=catalog)
```

`profile` and `client` are the existing trusted-owner objects. An entry must match a command registration and the complete profile/owner snapshot. Public `convert_result` performs pure SDK reconstruction and full producer round-trip validation during catalog preparation. Complete public config/output fingerprints bind receipts. Explanation is regenerated outside dispatch for its one explicit entity/feature focus, supported built-in `python-exact/0.0.4a4` result and display limit. Supplied diagnostics must match the complete result context, causal `known_at` availability and entity; reconstruction diagnostics are unsupported. All six counts, unknown prerequisites, findings, policy and fixed false authentication flags survive.

`DiagnosticAssociation(check_id, role, input_role=None, input_id=None)` explicitly binds each nonnull diagnostic fingerprint. Roles are `configuration`, `output`, `input`; input needs an exact supplied-receipt role/input ID. All six fingerprint fields must match. A null finding has no association. Subject labels/digests never imply input identity. Full diagnostic encoding is a new documented `equity.mcp.diagnostics.v1` projection, not a native diagnostic codec. Its signed/unsigned times, fingerprint lengths and prerequisite totals use canonical decimal strings; original public semantic bounds still apply.

No input batches enter the catalog. Receipt inputs are supplied declarations; **input_content_verified stays false**, even if the owner separately verified batches elsewhere. A diagnostic declaration matching a receipt hash does not establish input truth or actual feature consumption. Reports do not establish execution, actors, market accuracy, profitability, safety or historical acquisition.

The catalog holds immutable prepared bytes, with owner `snapshot`, `snapshot_sha256` and defensive `payload_json()`. Max8 entries, producer128KiB/receipt1MiB/explanation256KiB/diagnostic256KiB, manifest64KiB and aggregate4MiB apply inclusively. Aggregate is exactly manifest plus all supplied artifact bytes, counted per occurrence. Node/depth/error rules and excluded interpreter memory are explicit in the admission profile. These are logical admission bounds, not RSS, file/node/kernel quotas or deadlines. Owner errors are fixed `ValueError` codes `evidence_invalid`, `evidence_bounds`, `evidence_dependency`.

## Use existing opaque references

The owner seeds a native job and prepares reports for that completed result. A caller's explicit same-key `equity_calculate` attaches to it, then `equity_job_status`/existing result tools obtain the existing opaque job reference. `equity_artifact_reference` admits the artifact-role handle. No new tool accepts native IDs, paths, URLs, code, SQL, tokens, TLS settings or provider selection.

| Tool | Arguments | Result |
| --- | --- | --- |
| `equity_receipt` | `reference` | Receipt presence and first canonical chunk |
| `equity_explanation` | `reference` | One focused explanation presence and first chunk |
| `equity_diagnostics` | `reference` | Full diagnostic projection presence and first chunk |
| `equity_evidence_read` | `reference`, `kind`, `offset` | Next chunk for producer/receipt/explanation/diagnostics |
| `equity_evidence_capabilities` | none | Authenticated generic modes/limits; no result membership |

All result-specific calls require an **artifact-role** reference and perform actual native `artifact_read`, including cache hits, missing reports and disabled evidence. Native401/403 retains existing failure mapping and tombstones linked references. Complete stable producer payload/job/result/command/profile identity must match the prepared entry; mismatch returns `evidence_binding` and invalidates linked references. Unknown/foreign/wrong-role identifiers are `unknown_reference`; known tombstones/expiry remain `reference_expired`. No automatic replay, reference refresh or new record per chunk.

Supplied artifacts have full ASCII byte length and SHA256, with at most4096 printable ASCII bytes per chunk. First chunk is offset0; further offsets are exact nonboolean integers aligned4096. Float0.0 is refused before backend dispatch. Use `next_offset` until `complete`; assemble all bytes and check full length/hash before parsing a report. A chunk can end inside JSON. Partial transfer is distinct from omitted retained evidence and unknown diagnostics. SHA256 identifies content; it authenticates neither the owner nor the data source.

`not_supplied` means this completed result/report is absent from the static owner catalog. It does not imply an empty or failed result. Future uncatalogued jobs do not automatically acquire reports. `not_enabled` means this server has no evidence catalog. Neither exposes a digest, quality summary or cached report. Generic capabilities disclose no catalog counts, result presence or result identities. There is no dynamic publisher or second server.

Complete producer chunks preserve all original columns, source and quality/evidence fields. The owner-prepared quality summary counts **rows**, retains all five statuses and14reasons, and leaves `expected=None` unknown; reason counts may overlap. A focused explanation and globally omitted retained evidence do not imply complete explanation coverage. Diagnostic strict/advisory admission does not grant native service authority.

Native grants/original TTL remain authoritative on each backend read. Default local TTL60s/max300s,16 lifetime references and tombstones are unchanged. Final pre-output reference checking is local TTL/epoch/tombstone validation, not another native authorization or atomic grant guarantee. `equity_job_cancel` remains native job cancellation; notifications cannot preempt synchronous client reads/DNS/callbacks. EOF may only be observed after return and a partial prefix may escape; no atomic replacement/recovery/replay promise. Close releases the server's catalog reference; owner-retained plaintext may persist. No erasure/secure deletion is promised.

## Validation and migration

Existing seven tools/argument schemas keep their behavior. Discovery now lists12 tools and reports server version0.1.0a1; clients that hardcode seven tools need to accept additive inventory. [Schemas](stories/EQ-108_TOOL_SCHEMAS.json) and [wire bounds](stories/EQ-108_WIRE_BOUNDS.json) freeze the full list and framing. Source-development checks and patched-client tests are not installed/native/bothOS proof. Exact independent source/tooling review, fresh default/evidence forms on Windows/Linux, official-client/native operation ledgers, current artifacts and actual-main release/readback still precede acceptance. Hosted deployment, paused R7, private/provider acquisition, paid models and stable registry publication are outside this capability.

## Owner admission correction and supported configuration scope

The initial working-source review found EQ108-SRC-01: the reused public converter checks a configuration digest and producer round trip, but does not reconstruct nested configuration components or validate the full ResultView envelope. Two independent pure witnesses admitted a consistently rebound invalid SessionSpec and a forged wrong envelope. No native authorization bypass or actual native runtime failure was established. Original source freeze, review and probe logs remain retained.

Catalog preparation now reconstructs the exact public ConfigSpec and each nested public component through public constructors, then reconstructs the ResultView through its public constructor before conversion. It never invokes validation on borrowed records. Configuration has inclusive limits: canonical ASCII JSON64KiB, total UTF8 text64KiB counted per occurrence, each string at most65536 characters before encoding, parameters256, session intervals128, governed session slots256 and scalar integer parameters4096bits. Canonical configuration follows the same32768-node/depth20 rules. ResultView encoded envelope is at most262144bytes. These are logical preparation limits, not physical-memory or execution deadlines.

The initial catalog supports the existing SessionCommand scope: expected scope starts at configuration.session.open_ns and ends at configuration.availability.market_cutoff_ns. Intraday cutoff before session close remains supported. Config ID/schema/algorithm/digest, namespace/session and full availability must match the command/result context; result metadata availability must equal the copied configuration. Context revision has no corresponding ConfigSpec field and remains an owner declaration. These identity checks do not recompute features, authenticate source data or attest execution.

Corrected ordinary development checks pass15owner methods (including all ten frozen mutation vectors and positive controls),10dispatch methods, strict typing on7files and the synthetic example. Nineteen earlier existing regressions remain scoped to their captured source runs. This is not installed/native/WindowsLinux/current-artifact qualification. Exact renewed source review is pending; runtime publication and all new physical qualification sources remain gated.
