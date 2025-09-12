# ADR: Adaptive Chunking and Work-Stealing Scheduler for Parallel Scans

Status: Proposed (experimental, opt-in)
Date: 2025-09-12

Context
-------
Existing scans use fixed chunk size and a conventional thread pool. This can inflate tail latency and leave cores idle under mixed-cost workloads. We want better throughput and P95/P99 while remaining dependency-free and compatible with both GIL and free-threaded CPython.

Decision
--------
Introduce an optional module (proto_db.parallel) that implements:
- Adaptive chunk sizing using EMA with clamped bounds and target service times.
- A simple work-stealing scheduler with per-worker local deques and top-steal.
- Metrics hooks for latency distribution, steals, and queue depths.
- A helper (parallel_scan) for easy integration without altering existing query code.

Alternatives Considered
-----------------------
- Fixed chunk sizes with a global queue: simpler but higher contention and poor tail latency under skew.
- Cooperative batching within a standard ThreadPoolExecutor: improves overhead but doesn’t address idling or skew.
- OS-level pinning / NUMA-aware scheduling: out of scope and platform-specific.

Consequences
------------
- Pros: Better resource utilization, reduced tail latency under skew, minimal contention. No new dependencies.
- Cons: Added complexity and non-deterministic ordering across chunks; requires tuning in extreme workloads.

Compatibility and Semantics
---------------------------
- Default behavior is unchanged. Opt-in only.
- Result correctness must be maintained by callers; ordering is not guaranteed across chunks.

Future Work
-----------
- Deeper integration into query execution plans (index-aware range seeding).
- Enhanced backpressure and cooperative chunk fusion.
- Additional scheduling heuristics and lock-free structures where safe.
