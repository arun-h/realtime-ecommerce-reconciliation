# Incident Log

This file documents significant issues encountered during development of the
real-time e-commerce reconciliation pipeline

These are retrospective write-ups of debugging sessions during local
development, not production incidents discovered after deployment.

---

## Incident 1: Reconciliation Pipeline Stream-Stream Join Failure

**Date:** 2026-06-13 to 2026-06-14
**Severity:** High — pipeline completely non-functional
**Status:** Resolved

### Summary

The Spark Structured Streaming job failed to start due to incompatible
watermark requirements across three Kafka streams being joined via
stream-stream left outer joins.

### Timeline

- Initial implementation used stream-stream joins across orders, payments,
  and inventory streams with independent watermarks
- First failure: `AnalysisException: Stream-stream LeftOuter join ... is not
  supported without a watermark in the join keys` — fixed by correcting the
  join condition to use a direct range comparison instead of an
  `unix_timestamp()` arithmetic expression
- Second failure: `AnalysisException: More than one event time columns are
  available` — caused by applying watermarks to all three streams
  independently before joining them
- Third failure: the same error persisted after attempting to strip
  watermarks via `.cast(TimestampType())` on the DataFrame — this approach
  does not work because Spark propagates watermark metadata at the catalyst
  query-plan level, not at the DataFrame API level, so a Python-side cast has
  no effect on the underlying plan
- Fourth failure: switching to asymmetric watermarking (watermark on the
  anchor "orders" stream only, no watermark on payments/inventory) resolved
  the multiple-event-time-column conflict, but surfaced a new constraint —
  Spark's streaming left outer join requires a watermark on **both** the
  preserved side and the nullable side of the join

### Root Cause

Spark Structured Streaming's stream-stream join implementation has strict,
sparsely documented constraints: a left outer join requires watermarks on
both the preserved and nullable side, but a joined output stream can carry
forward only one event-time column. For a 3-way join across independently
watermarked streams, these two requirements are mutually exclusive without
significant restructuring — there is no single watermark configuration that
satisfies both rules simultaneously.

### Resolution

Abandoned stream-stream joins entirely. Redesigned the pipeline to:

1. Consume all three Kafka topics through a single subscription
   (`subscribe="order-events,payment-events,inventory-events"`)
2. Inside one `foreachBatch`, write each topic's events independently to
   its own raw table in PostgreSQL — no streaming join involved
3. Run reconciliation as a SQL batch query inside the same `foreachBatch`,
   joining the three raw PostgreSQL tables directly with a time-bounded
   `LEFT JOIN`, using `ON CONFLICT DO UPDATE` for idempotent upserts

This trades strict streaming-join semantics for a simpler, more debuggable
architecture. Ingestion and reconciliation are decoupled, reconciliation
re-runs idempotently on every batch, and the system tolerates arbitrary
arrival-time gaps between related events — a payment arriving minutes after
its order is still reconciled correctly, which the original 60-second
streaming join window could never have handled regardless of how the
watermark issue was resolved.

### What I'd Do Differently

Prototype the join strategy against Spark's documented streaming join
constraints in isolation before writing the full multi-file pipeline. The
watermark / join-key interaction is easy to get wrong on a first attempt and
the resulting error messages (multiple event-time columns, missing
watermark on nullable side) do not obviously point back to "redesign your
join strategy" — each one looks locally fixable, which led to several
incremental patches before the structural issue was addressed.

---

## Incident 2: PySpark Streaming Job Crashes Under WSL2 Memory Constraints

**Date:** 2026-06-14
**Severity:** High — pipeline unstable, JVM crashes mid-execution
**Status:** Resolved

### Summary

After resolving the stream-stream join architecture issue, the Spark
streaming job began crashing intermittently with JVM thread pool shutdowns,
py4j connection failures, and "EOF reached before Python server
acknowledged" errors, despite the application code containing no logical
errors.

### Timeline

- The job ran four concurrent streaming queries (orders, payments,
  inventory, and DLQ), each with its own executor threads and state store
- First symptom: `ThreadPoolExecutor Shutting down` and
  `RejectedExecutionException` appearing mid-batch, with no clear
  application-level trigger
- Second symptom: `py4j.Py4JException: Received empty command` — the
  Python-to-JVM bridge dropping mid-call
- Initial hypothesis was a concurrency bug in the batch-processing code;
  ruled out after confirming the same crash pattern occurred regardless of
  which batch function was executing at the time of failure
- Root cause confirmed via `free -h`: WSL2 was allocated only 3.8GB total
  RAM, with roughly 2GB already consumed by the Docker containers (Kafka,
  PostgreSQL, MinIO) before the Spark JVM even started

### Root Cause

Running four concurrent Spark Structured Streaming queries, each with an
independent state store and executor thread pool, exceeded the available
JVM heap and the WSL2 memory ceiling. The crashes were not Spark bugs or
application bugs — they were out-of-memory failures surfacing as
distributed-systems-style connection errors, which made initial triage
significantly harder, since the stack traces pointed at py4j networking
and thread pool internals rather than at memory directly.

### Resolution

Two changes, applied together:

1. **Infrastructure:** increased the WSL2 memory allocation via
   `.wslconfig` (`memory=8GB`), then verified with `free -h` that the new
   ceiling actually took effect — this required a full `wsl --shutdown`
   from Windows PowerShell, not just closing the terminal window
2. **Architecture:** consolidated four independent streaming queries into
   a single Kafka subscription processed through one `foreachBatch`,
   cutting JVM thread and state-store overhead substantially
3. Reduced Spark driver/executor memory configuration from 2g to 1g each,
   since the actual workload (single-digit events per second in this
   development environment) never required the higher ceiling — the
   original sizing was carried over from generic tutorial defaults rather
   than sized to the real throughput

### What I'd Do Differently

Check `free -h` and `/proc/meminfo` as soon as errors started looking
infrastructure-shaped — thread pool shutdowns and network exceptions with
no clear application-level cause — rather than spending multiple iterations
on code-level debugging first. Distributed-systems failures that don't map
cleanly to the code just written are a strong early signal to check resource
constraints before re-reading application logic again.

---

## General Pattern Across Both Incidents

Both incidents shared a common diagnostic trap: the error messages were
technically accurate but pointed at the wrong layer of the stack relative to
the actual root cause (Spark's watermark engine in Incident 1, py4j
networking in Incident 2). In both cases, the fix that actually worked was a
structural simplification — fewer concurrent streaming queries, a single
Kafka subscription, reconciliation pushed into SQL rather than the
streaming engine — rather than a deeper, more clever fix within the original
architecture. When debugging surfaces increasingly obscure framework-level
errors, that is often a signal to simplify the design rather than continue
patching around the framework's constraints.