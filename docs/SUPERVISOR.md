# Bounded local whole-task supervisor

EQ062#70, experimental worker0.1.0a7 candidate. Numerical core/I/O/math/task/result/receipt codecs and mandatory runtime dependencies are unchanged. No durable claims, accepted-generation catalog, provider/service or private historical admission is added.

## Prepare once, compute independently, publish serially

`prepare_session(spec, source, cancellation=...)` admits the existing bounded adapter request, validates delivery and seals observed TaskManifest inputs. It returns PreparedSession(task,batch), retaining no adapter/sink/credential handle and publishing nothing. `compute_session_inputs(task,batches)` invokes the existing bars/trades/quotes core calculations; ordinary run_session preserves pre-acquisition sink admission and verified publication. Missing/empty/partial and known-at policies retain existing mathematics.

`WorkItem(task,batches,calculate=compute_session_inputs)` takes complete immutable sealed tasks and whole canonical batches. Supplied batch kind/metadata must match task input bindings; source identity/count/byte limits remain explicit. Witness-only extensions may have no raw batches; their supplied proofs/configuration remain the trusted local calculation's responsibility and existing result admission must still match the sealed task. Extensions are top-level importable Python functions taking `(task,batches)` and returning a tuple of canonical FeatureResults. Closures/lambdas/backend handles are not supplied through this interface. Functions' module-global state and internal allocations are trusted caller code, outside controlled memory/thread guarantees; this is not a sandbox or remote executable format.

`InputReuseCache(max_entries=64,max_bytes=8388608)` is owned by its creating Thread object. put(task,batches) checks actual bindings and stores the exact existing task.reuse_sha256 scope; get(task) returns the same immutable batch tuple or None. Changed revision, input/config/cutoff/availability/initialization/instrument scope misses. Conflicting data under the same key fails; bounded cache saturation does not trigger eviction, acquisition or a hidden reload. clear removes only caller-supplied cached references. Prepare once, put observed inputs, then get for compatible already-sealed tasks; never infer an expected task identity before acquisition.

## Whole-task partitioning and ordering

`partition_tasks(items,workers)` deterministically forms connected components of instrument sets. Intersecting instrument populations stay in one coarse serial component, including multi-instrument bridges. Components sort by instrument IDs and are assigned deterministically to the currently lightest shard by row weight/stable index. A component sorts by target-session open time then task digest. Every unique admitted task appears once; caller input order cannot change the plan. Tasks are never split into rows; task.merge_policy remains none. Continuous quote merge and implicit checkpoints/state transfer remain unsupported. Supplied ordered-history/warm-up requirements stay in each task; deficient warm-up retains the core's mathematical availability instead of borrowing future rows.

Row ledger counts actual supplied rows for each admitted task, including cancelled/unsubmitted tasks distinguished by their execution diagnosis. distinct_reuse_rows counts rows once per exact reuse scope; it is not a claim about unique physical source rows across incompatible scopes or proof an arbitrary callback consumed every row. Repeated feature consumers retain their separate per-task counts. Conflicting raw batches under one reuse scope fail before callbacks.

## Combined budget

`ResourceBudget` positive integer defaults:

| Field | Default | Admission |
| --- | ---: | --- |
| max_tasks |64| Whole inventory, maximum4096 |
| workers/max_in_flight |1/1| Maximum32 workers; in-flight <=workers |
| max_cpu_slots/compute_threads/backend_threads |2/1/1| workers*compute_threads+backend_threads <=slots; slots <=observed os.cpu_count |
| io_slots |1| Serialized coordinator I/O only |
| max_input_bytes |16777216| Sum actual owned WorkItem pickle sizes |
| max_resident_bytes |67108864| Input, process transport copies, result reservations, codec buffer and optional publisher pending reservation |
| max_task_result_bytes |262144| Actual SDK result codec content per task |
| max_task_transport_bytes |1048576| Process per-task actual pickle transport, including task metadata and result/fixed failure |
| max_total_result_bytes |16777216| Worst-case all-task result reservation; actual returned content recorded |
| max_result_cells/max_evidence_rows |100000/10000| Existing SDK admission per task; fixed64-result cap |

For process mode, reserve twice the largest actual serialized coarse partitions up to max_in_flight, for child-owned input and serialized IPC copies, in addition to parent-owned inputs. Also reserve twice all tasks' max_task_transport_bytes plus tuple overhead, conservatively covering child result populations, serialized result IPC and returned task metadata. Failure-task pickle size must fit before callbacks; successful child TaskExecution pickle size is checked before return. Oversized result transport becomes fixed RESOURCE_LIMIT with no returned rejected result. These serialized-byte reservations do not measure decoded-object memory. All-task conservative reservations may reject process mode under a budget that admits sequential mode. For every mode, reserve all tasks' maximum result bytes, one result codec buffer and maximum task JSON buffer. This implementation deliberately keeps the conservative all-shard result reservation even when spilling: each coarse callback returns a full shard, and all completed computations are collected before coordinator publication. Spill isolates/retains results; it does not promise to admit an otherwise impossible resident budget. Optional publication additionally reserves the publisher's declared pending bytes and bounds accumulated returned output metadata. These logical accounting reservations do not enforce hard RSS, interpreter/native allocations, arbitrary callback global state, CPU affinity or uninterruptible callback timeout. Observed os.cpu_count is a logical hardware admission reference, not an affinity/availability guarantee.

Compute/backend thread declarations are caller contracts. Built-in pure calculations use one thread; callers configure real adapter backend limits and record actual settings. The coordinator alone acquires and publishes; workers receive no adapter/sink connections or credentials. Backend layout, conversion, Parquet whole-root and DuckDB serialized database ownership remain in accepted sinks. No implicit thread safety for consumer extensions. Sequential mode requires workers1. Thread and process modes support bounded whole-task concurrency; process uses explicit spawn and local standard Python serialization. No per-row future tasks or unlimited pool queue.

## Results, backpressure and cancellation

`BoundedSupervisor(budget=ResourceBudget(),mode='sequential').run(items, cancellation=...,spill=...,publisher=...)` is owned by its creating Thread object and cannot reenter. Invalid inventory, CPU/input/transport/resident/result/queue admission fails before callbacks. Ordered outputs are stable task-digest order, independent of completion order. Sequential1 is selected from [three-repeat frozen comparison](EQ062_MODE_COMPARISON.json): every tested thread/process configuration had a higher median for these prepared small/large/skewed synthetic bars. Other workloads require their own measurement; no universal speedup or best-mode guarantee.

TaskExecution(task,results,spill,output,reason) distinguishes computed local results/spill from verified sink output. OutputNone and reasonNone can mean successful computation with no publisher requested; it is not committed generation or catalog acceptance. Mathematical missing values remain inside FeatureResult, not worker failure. Callback/serialization/fault diagnoses are fixed codes without private exception text. Completed but unpublished results remain returned or referenced even when publication waits/fails.

Cancellation stops new coarse-shard submissions, cancels not-started futures where possible, joins already running callbacks and stops publication. A started coarse shard finishes its serial member calculations; there is no forced per-task preemption. Unsubmitted tasks retain CANCELLED identity; completed work can be retained with cancellation diagnosis. Cancellation before start creates no spill directory. Caller owns and closes I/O instances. Invalid tokens fail with fixed diagnostics.

An optional SerialPublisher must initially have no pending tasks, match every task destination and be exclusively used for this supervisor invocation: no outside producers/drainers may submit during run. Its immutable limits/destination_scope properties permit resource admission; admit_owner() validates its creating Thread object before calculation without I/O or queue mutation. A foreign or terminated owner yields INVALID_SESSION before callbacks. One coordinator submits/drains bounded passes; unresolved work remains pending and result copies/references remain in outcomes. Later task drains can resolve earlier same-run pending tasks, updating those outcomes. No indefinite hidden retry. Actual committed receipt/readback still gates output. If returned receipt metadata exceeds remaining logical budget, keep the computed task/results and return RESOURCE_LIMIT without accepting that metadata; a sink may already be committed, so subsequent recovery must look up/replay through the publisher rather than blindly begin again. Existing pending input causes BUSY before computation, preserving unrelated work.

## Isolated result spill

`ResultSpill(root,max_bytes=33554432,max_tasks=64,protected_sources=())` configures a caller-owned local Windows/Linux root disjoint from supplied source paths in both directions. Constructors create no storage. One exclusive `efworker-spill-<opaque-run-id>` directory holds existing encoded TaskManifest and SDK result bytes in exclusively created files. Reserve the full attempt's count/aggregate bytes before writes, retain that reservation after partial failure, flush files, and register a SpillReference only for a complete write. Same-run reads require the exact registered immutable reference and bounded size/digest/task/result identity. Foreign references, forged records, corruption and symlinks fail; no result schema or executable storage codec is added.

Supervisor preflight includes all task JSON bytes plus worst-case result bytes against remaining spill quota before computation. Partial spill failures preserve numerical results and fixed reason. read(reference,budget=...) returns the sealed task and exact results. close(remove=False) closes logical ownership and preserves files; close(remove=True) removes only recorded owned files and an empty owned run directory. Unexpected entries remain untouched. Directory cleanup requires successful exclusive creation; even an empty pre-existing collision is preserved after failed creation. No recursive deletion or unrelated cleanup. Configured source-path exclusion and synthetic original byte preservation are tested; omitted source paths are not discovered/authenticated. No UNC/network filesystem, power-loss, cumulative EQ061 generation-stage quota, arbitrary filesystem adversary or durable restart claim. EQ063 owns later durable claims/recovery.

```python
prepared = prepare_session(spec, source)  # caller owns source
batches = () if prepared.batch is None else (prepared.batch,)
cache = InputReuseCache()
cache.put(prepared.task, batches)
item = WorkItem(prepared.task, cache.get(prepared.task))
# publisher and optional ResultSpill are caller-configured, on this coordinator.
outcome = BoundedSupervisor().run((item,), publisher=publisher, spill=spill)
for execution in outcome.tasks:
    if execution.output is not None:
        verified_outputs.append(execution.output)
    # Otherwise retain results/reference and handle fixed reason explicitly.
```

Factories remain existing explicit caller registrations; no automatic backend/source discovery or new CLI flow. Public Python composition is the execution interface. See [pre-code contract](EQ062_PLAN.md), same-story delivery evidence and `tools/benchmark_supervisor.py` for reproducible synthetic compute-mode comparison. Larger representative physical end-to-end/peak-job-memory and private source/PIT/warm-up/privacy/month/annual readiness remain EQ066.
