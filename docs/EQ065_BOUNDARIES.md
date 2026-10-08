# EQ065 pre-code review boundary clarification

The frozen ec26c7b/799dbb6 plans and independent diagnostic_oracle remain unchanged. Separate local automated reviewer /root/r5_review inspected those exact plans and issue73 before runtime; no blocking contradiction found. This is design feedback, not source/artifact/release acceptance. Actual implementation will require a new final-head review.

Reserve terminal/failure task observations and worst-case sidecar bytes before callbacks/queue submission; process child/IPC/returned-parent copies remain inside actual existing transport/resident budgets. Missing clock/transport observations use explicit unavailable telemetry, never fabricated zeros, dropped-work success or loss of an actual durable receipt.

Only observations at an actual backend/lock boundary count backend BUSY probes. Queue mutex/capacity backpressure is a separate boundary/counter. Publication/conversion intervals can contain serialization/readback work; label combined boundaries rather than adding overlaps as wall time.

Keep per-attempt work status and telemetry availability distinct. Cancellation after computation and original receipt recovery after unknown commit do not imply a monotone task-status ladder. Bind unsealed full command intent (including declared source revision/config/request) to the final sealed observed-input TaskSHA, preserving one logical observation slot without aliasing configurations.

Enforce owner/reentry admission before clock/callback hooks. Validate exact integer, nondecreasing same-host counters and bounded deltas; copy and validate immutable child sidecars and task bindings before merge. Exclude caller exception/string/repr/config/path/credential details from diagnostic reason fields.

Both Parquet and DuckDB refer to qualified SINK flows using accepted source contracts/factories. This story does not implement a Parquet source adapter. Existing pure mathematics and original codecs/sink locking are unchanged.
