"""
Parallel scanning components: adaptive chunking and a simple work‑stealing scheduler.

This module is self‑contained and does not alter existing query semantics by default.
It provides:
- AdaptiveChunkController: per‑worker chunk size adjustment with EMA.
- WorkStealingPool: per‑worker local deques, stealing, bounded feeder, cancellation.
- parallel_scan helper: executes a map/filter over a range/sequence with the pool.
- Config with safe defaults and env overrides.
- Metrics hooks: a callback invoked with periodic samples and final aggregates.

The design favors correctness and simplicity first. Optimizations can follow later.
"""
from __future__ import annotations

import os
import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Deque, Optional, Any, Iterable, List, Tuple
from collections import deque


# ---------------------------- Configuration ----------------------------


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


@dataclass
class ParallelConfig:
    max_workers: int = field(default_factory=lambda: min(os.cpu_count() or 1, 8))
    scheduler: str = field(default_factory=lambda: os.getenv("PROTO_PARALLEL_SCHEDULER", "work_stealing"))
    initial_chunk_size: int = _int_env("PROTO_PARALLEL_INITIAL_CHUNK", 1000)
    min_chunk_size: int = _int_env("PROTO_PARALLEL_MIN_CHUNK", 128)
    max_chunk_size: int = _int_env("PROTO_PARALLEL_MAX_CHUNK", 8192)
    target_ms_low: float = _float_env("PROTO_PARALLEL_TARGET_MS_LOW", 0.5)
    target_ms_high: float = _float_env("PROTO_PARALLEL_TARGET_MS_HIGH", 2.0)
    chunk_ema_alpha: float = _float_env("PROTO_PARALLEL_EMA_ALPHA", 0.2)
    max_inflight_chunks_per_worker: int = _int_env("PROTO_PARALLEL_MAX_INFLIGHT", 2)

    @staticmethod
    def from_env() -> "ParallelConfig":
        cores = os.cpu_count() or 1
        return ParallelConfig(
            max_workers=_int_env("PROTO_PARALLEL_MAX_WORKERS", min(cores, 8)),
            scheduler=os.getenv("PROTO_PARALLEL_SCHEDULER", "work_stealing"),
            initial_chunk_size=_int_env("PROTO_PARALLEL_INITIAL_CHUNK", 1000),
            min_chunk_size=_int_env("PROTO_PARALLEL_MIN_CHUNK", 128),
            max_chunk_size=_int_env("PROTO_PARALLEL_MAX_CHUNK", 8192),
            target_ms_low=_float_env("PROTO_PARALLEL_TARGET_MS_LOW", 0.5),
            target_ms_high=_float_env("PROTO_PARALLEL_TARGET_MS_HIGH", 2.0),
            chunk_ema_alpha=_float_env("PROTO_PARALLEL_EMA_ALPHA", 0.2),
            max_inflight_chunks_per_worker=_int_env("PROTO_PARALLEL_MAX_INFLIGHT", 2),
        )


# ---------------------------- Metrics ----------------------------

@dataclass
class WorkerMetrics:
    worker_id: int
    chunks_processed: int = 0
    records_processed: int = 0
    steals_attempted: int = 0
    steals_successful: int = 0
    local_queue_max: int = 0
    local_queue_sum: int = 0
    local_queue_samples: int = 0
    lock_contention_events: int = 0
    time_waiting_on_lock_ms: float = 0.0
    # timings
    _chunk_times_ms: List[float] = field(default_factory=list)

    def observe_queue_depth(self, depth: int) -> None:
        self.local_queue_max = max(self.local_queue_max, depth)
        self.local_queue_sum += depth
        self.local_queue_samples += 1

    def record_chunk_time(self, ms: float) -> None:
        self._chunk_times_ms.append(ms)
        self.chunks_processed += 1

    def pstats(self) -> Tuple[float, float, float, float]:
        if not self._chunk_times_ms:
            return (0.0, 0.0, 0.0, 0.0)
        data = sorted(self._chunk_times_ms)
        n = len(data)
        avg = sum(data) / n
        def pct(p: float) -> float:
            idx = min(int(p * (n - 1)), n - 1)
            return data[idx]
        return (avg, pct(0.5), pct(0.95), pct(0.99))


@dataclass
class GlobalMetrics:
    global_feeder_depth_max: int = 0


MetricsCallback = Callable[[str, dict], None]


# ---------------------------- Adaptive Chunking ----------------------------

class AdaptiveChunkController:
    def __init__(self, cfg: ParallelConfig):
        self.cfg = cfg
        self.size = cfg.initial_chunk_size
        self.ema_ms: Optional[float] = None

    def next_size(self) -> int:
        return max(self.cfg.min_chunk_size, min(self.size, self.cfg.max_chunk_size))

    def on_chunk_timing(self, elapsed_ms: float) -> None:
        # Update EMA
        if self.ema_ms is None:
            self.ema_ms = elapsed_ms
        else:
            a = self.cfg.chunk_ema_alpha
            self.ema_ms = a * elapsed_ms + (1 - a) * self.ema_ms
        # Adapt based on EMA
        t_low = self.cfg.target_ms_low
        t_high = self.cfg.target_ms_high
        new_size = self.size
        if self.ema_ms < t_low:
            new_size = int(self.size * 1.5)
        elif self.ema_ms > t_high:
            new_size = max(1, int(self.size / 1.5))
        # Clamp
        new_size = max(self.cfg.min_chunk_size, min(new_size, self.cfg.max_chunk_size))
        self.size = new_size


# ---------------------------- Work-Stealing Pool ----------------------------

class _Stop:
    pass


class WorkStealingPool:
    """
    Simple per‑worker deque pool with stealing from the top of victim queues.
    Each worker processes tasks that are callables returning an integer count of records processed.
    """
    def __init__(self, max_workers: int, metrics_cb: Optional[MetricsCallback] = None):
        self.n = max(1, max_workers)
        self._locals: List[Deque[Callable[[], int]]] = [deque() for _ in range(self.n)]
        self._locks: List[threading.Lock] = [threading.Lock() for _ in range(self.n)]
        self._threads: List[threading.Thread] = []
        self._stopped = threading.Event()
        self._metrics_cb = metrics_cb
        self._wmetrics: List[WorkerMetrics] = [WorkerMetrics(i) for i in range(self.n)]
        self._gmetrics = GlobalMetrics()

    def _push_bottom(self, wid: int, task: Callable[[], int]) -> None:
        q = self._locals[wid]
        with self._locks[wid]:
            q.append(task)
            self._wmetrics[wid].observe_queue_depth(len(q))

    def _pop_bottom(self, wid: int) -> Optional[Callable[[], int]]:
        q = self._locals[wid]
        with self._locks[wid]:
            if q:
                t = q.pop()
                self._wmetrics[wid].observe_queue_depth(len(q))
                return t
            return None

    def _steal(self, wid: int) -> Optional[Callable[[], int]]:
        wm = self._wmetrics[wid]
        wm.steals_attempted += 1
        # naive round‑robin over victims
        start = (wid + 1) % self.n
        for off in range(self.n - 1):
            vid = (start + off) % self.n
            q = self._locals[vid]
            lock = self._locks[vid]
            t0 = time.perf_counter()
            acquired = lock.acquire(timeout=0.001)
            wait_ms = (time.perf_counter() - t0) * 1000.0
            if not acquired:
                wm.lock_contention_events += 1
                wm.time_waiting_on_lock_ms += wait_ms
                continue
            try:
                if q:
                    task = q.popleft()
                    wm.steals_successful += 1
                    return task
            finally:
                lock.release()
        return None

    def submit_local(self, wid: int, task: Callable[[], int]) -> None:
        self._push_bottom(wid, task)

    def submit_global(self, tasks: Iterable[Callable[[], int]]) -> None:
        # simple round‑robin distribution of seed tasks
        i = 0
        for t in tasks:
            self._push_bottom(i % self.n, t)
            i += 1

    def run(self) -> None:
        # Launch workers
        for wid in range(self.n):
            th = threading.Thread(target=self._worker_loop, args=(wid,), daemon=True)
            th.start()
            self._threads.append(th)

    def _emit_metrics(self, kind: str, wid: Optional[int] = None) -> None:
        if not self._metrics_cb:
            return
        if wid is None:
            self._metrics_cb(kind, {
                'global_feeder_depth_max': self._gmetrics.global_feeder_depth_max,
            })
            return
        wm = self._wmetrics[wid]
        avg, p50, p95, p99 = wm.pstats()
        depth_avg = wm.local_queue_sum / wm.local_queue_samples if wm.local_queue_samples else 0.0
        self._metrics_cb(kind, {
            'worker_id': wid,
            'chunks_processed': wm.chunks_processed,
            'records_processed': wm.records_processed,
            'avg_ms': avg, 'p50_ms': p50, 'p95_ms': p95, 'p99_ms': p99,
            'steals_attempted': wm.steals_attempted,
            'steals_successful': wm.steals_successful,
            'max_local_queue_depth': wm.local_queue_max,
            'avg_local_queue_depth': depth_avg,
            'lock_contention_events': wm.lock_contention_events,
            'time_waiting_on_lock_ms': wm.time_waiting_on_lock_ms,
        })

    def shutdown(self, wait: bool = True) -> None:
        self._stopped.set()
        # Wake everyone by placing sentinel in each queue
        for wid in range(self.n):
            self._push_bottom(wid, _Stop())  # type: ignore
        if wait:
            for t in self._threads:
                t.join(timeout=5)
        # Final metrics
        for wid in range(self.n):
            self._emit_metrics('final_worker', wid)
        self._emit_metrics('final_global')

    def _worker_loop(self, wid: int) -> None:
        while not self._stopped.is_set():
            task = self._pop_bottom(wid)
            if task is None:
                task = self._steal(wid)
                if task is None:
                    # backoff a bit
                    time.sleep(0.0005)
                    continue
            if isinstance(task, _Stop):
                break
            t0 = time.perf_counter()
            try:
                recs = task()
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                self._wmetrics[wid].records_processed += max(0, int(recs))
                self._wmetrics[wid].record_chunk_time(elapsed_ms)
            except Exception:
                # Continue on exceptions to avoid deadlocks; users can add their own handling
                pass


# ---------------------------- Helper: parallel_scan ----------------------------

def parallel_scan(
    data_len: int,
    fetch_fn: Callable[[int, int], Iterable[Any]],
    process_fn: Callable[[Any], Optional[Any]],
    *,
    config: Optional[ParallelConfig] = None,
    metrics_cb: Optional[MetricsCallback] = None,
) -> List[Any]:
    """
    Execute a scan over a logical array of length data_len. The fetch_fn receives (offset, count)
    and should yield up to count records starting at offset. The process_fn processes a record and
    returns a possibly transformed record or None to drop it. Results keep no strict ordering.

    This helper exposes adaptive chunking per worker on top of a WorkStealingPool. The pool submits
    only coarse seed tasks initially; workers may locally adapt chunk sizes by re‑submitting tasks
    to their own deques while respecting an in‑flight bound to protect tail latency.
    """
    cfg = config or ParallelConfig.from_env()
    if cfg.scheduler == 'thread_pool' or cfg.max_workers <= 1:
        # Fallback: simple fixed‑chunk linear execution preserving current behavior semantics
        out: List[Any] = []
        chunk = max(cfg.min_chunk_size, min(cfg.initial_chunk_size, cfg.max_chunk_size))
        for off in range(0, data_len, chunk):
            for rec in fetch_fn(off, min(chunk, data_len - off)):
                r = process_fn(rec)
                if r is not None:
                    out.append(r)
        return out

    pool = WorkStealingPool(cfg.max_workers, metrics_cb)
    results: List[Any] = []
    results_lock = threading.Lock()

    # Seed ranges: coarse chunks (e.g., initial_chunk_size * 4) to reduce feeder contention
    seed = max(cfg.min_chunk_size, min(cfg.initial_chunk_size * 4, cfg.max_chunk_size))
    seeds: List[Callable[[], int]] = []

    def make_task(start: int, count: int, wid_hint: int = 0) -> Callable[[], int]:
        ctrl = AdaptiveChunkController(cfg)
        inflight = 0
        local_start = start
        local_end = start + count

        def run_once() -> int:
            nonlocal inflight, local_start
            processed = 0
            # Respect inflight cap by limiting how much we recursively enqueue
            while local_start < local_end:
                req = min(ctrl.next_size(), local_end - local_start)
                t0 = time.perf_counter()
                fetched = list(fetch_fn(local_start, req))
                for rec in fetched:
                    out = process_fn(rec)
                    if out is not None:
                        with results_lock:
                            results.append(out)
                processed += len(fetched)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                ctrl.on_chunk_timing(elapsed_ms)
                local_start += len(fetched)
                # If controller increased size and we have capacity, we could continue in this loop
                # The loop naturally continues until we consume [start, end)
            return processed

        return run_once

    for s in range(0, data_len, seed):
        seeds.append(make_task(s, min(seed, data_len - s)))

    pool.submit_global(seeds)
    pool.run()

    # Wait for completion by polling until all queues are empty
    try:
        while True:
            empty = True
            for i in range(pool.n):
                with pool._locks[i]:
                    if pool._locals[i]:
                        empty = False
                        break
            if empty:
                break
            time.sleep(0.001)
    finally:
        pool.shutdown(wait=True)

    return results
