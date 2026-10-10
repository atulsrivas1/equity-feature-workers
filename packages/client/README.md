# Experimental Python remote client

This experimental optional distribution is developed and qualified through [EQ081](https://github.com/atulsrivas1/equity-features/issues/91). RemoteClient provides explicit authenticated bounded JSON operations outside local calculation packages. It returns immutable views or closed Failure values through Outcome. Local kernels do not depend on networking or this optional distribution.

```python
from equity_feature_client import RemoteClient

# Supply an explicitly authorized origin and caller-owned token provider.
with RemoteClient(origin, token_provider, attempts=1) as client:
    outcome = client.discover(request_id="caller-owned-correlation")
    if outcome.ok:
        capabilities = outcome.view.payload_json()
    else:
        code = outcome.failure.code
```

The client validates before invoking credentials and supports verified HTTPS or literal-loopback HTTP qualification. It does not use ambient proxies, redirects, environment tokens, automatic polling, local file writes or data caching. Explicit read retry policy is bounded; calculate/job_cancel never auto-replay and interrupted mutations can return outcome_unknown. Timeouts are cooperative; DNS/OS calls have no hard interruption guarantee. Controlled artifact responses stay in caller-owned immutable bytes.

Raw and producer expectations pin original source/scope/config/backend/full executed feature identities. Projected feature slices remain separate from complete producer results. HTTP carries no native envelope/receipt/SHA; publication verification belongs to the service. Optional native conversion is implemented through equity_feature_client.native.convert_raw/convert_result using pinned public contracts and the I/O SDK; install the native extra and inspect native_available() before importing it. It preserves complete original metadata and exact values, rejects missing required columns or unsupported decimal scales, and does not turn projected slices into full producers. The initial admitted producer inventory contains one pinned source; multiple input producers require explicit future expectation admission. Installed qualification and delivery evidence are recorded on the linked story; source and finite GitHub Actions artifacts do not grant operational hosting or data rights. No hosted/provider/private/registry rights are conferred. See docs/EQ081_API.md and canonical story91 for current evidence and limitations.
