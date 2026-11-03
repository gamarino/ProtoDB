Parallel Scans: Adaptive Chunking and Work-Stealing
===================================================

Overview
--------
This guide introduces a new, optional parallel scanning facility designed to improve throughput and tail latency for mixed-cost workloads. It offers:

- Adaptive chunk sizing to balance scheduling overhead and latency.
- A lightweight work-stealing scheduler using per-worker local deques.
- Metrics hooks for observability.

By default, ProtoBase behavior is unchanged. You can opt-in programmatically via the parallel_scan helper or by integrating the WorkStealingPool into your own pipelines.

Configuration
-------------
Configuration is provided through ParallelConfig (proto_db.parallel):

- parallel.max_workers: default min(cpu_cores, 8)
- parallel.scheduler: "work_stealing" or "thread_pool" (default: work_stealing)
- parallel.initial_chunk_size: default 1000
- parallel.min_chunk_size: default 128
- parallel.max_chunk_size: default 8192
- parallel.target_ms_low: default 0.5 ms
- parallel.target_ms_high: default 2.0 ms
- parallel.chunk_ema_alpha: default 0.2
- parallel.max_inflight_chunks_per_worker: default 2 (reserved for future task splitting)

Environment variables (optional) can override defaults, e.g. PROTO_PARALLEL_MAX_WORKERS, PROTO_PARALLEL_INITIAL_CHUNK, etc.

How it Works
------------
- Each worker keeps a local deque for tasks, pushing and popping from the bottom.
- When a worker runs out of work, it steals from the top of another worker’s deque.
- AdaptiveChunkController adjusts per-worker chunk size using an EMA of recent chunk service times, growing when work is too fast and shrinking when it is too slow.

Metrics and Observability
-------------------------
A metrics callback (MetricsCallback) can be provided to WorkStealingPool or parallel_scan. The callback receives named events with dictionaries:

Per worker (final_worker):
- worker_id, chunks_processed, records_processed
- avg_ms, p50_ms, p95_ms, p99_ms
- steals_attempted, steals_successful
- max_local_queue_depth, avg_local_queue_depth
- lock_contention_events, time_waiting_on_lock_ms

Global (final_global):
- global_feeder_depth_max

Usage Example
-------------

.. code-block:: python

    from proto_db.parallel import parallel_scan, ParallelConfig

    data = list(range(100000))

    def fetch(off, cnt):
        # slice-based fetch for demonstration
        return data[off:off+cnt]

    def process(x):
        # drop odds, double evens
        return x*2 if x % 2 == 0 else None

    cfg = ParallelConfig(max_workers=4, scheduler='work_stealing')
    results = parallel_scan(len(data), fetch, process, config=cfg)

Tuning Guidance
---------------
- Start with defaults. If chunks are too small (scheduling overhead dominates), lower target_ms_low to grow sizes faster, or raise initial_chunk_size.
- If tail latency grows, raise target_ms_high or lower max_chunk_size to prevent large batches.
- For CPU-bound workloads on GIL builds, consider balancing worker count with other threads; for I/O-bound, more workers may help.

Compatibility
-------------
- Implemented in pure Python; works on CPython with GIL and free-threaded builds.
- No external dependencies.
- Ordering is not guaranteed across chunks; if you require stable ordering, sort after processing or use sequential mode.
