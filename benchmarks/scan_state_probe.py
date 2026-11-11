"""Why ``parallel_scan`` stops scaling inside the suite (Task R136, question 4): a plugin.

Load with ``--plugin benchmarks.scan_state_probe`` and place ``scan_state_probe`` in ``--only``
right after ``parallel_scan`` (or after any workloads whose leftovers are under suspicion).  The
workload records the process state those workloads left (threads alive, RSS, objects tracked by
the cyclic GC, the time of one full collection, GIL state) and then reruns the ``parallel_scan``
body (the same ``_spin`` items, 1 and 8 workers) four ways, every result verified:

* ``as is``: the state the earlier workloads left;
* ``gc callbacks``: as is, counting the collections that run during the scans and their time;
* ``gc.freeze()``: the surviving objects moved to the permanent generation (a collection no
  longer walks them);
* ``gc.disable()``: no automatic collection during the scans;
* ``as is, again``: the first variant repeated last (the variants run in this order, so a
  change that persists after ``gc.unfreeze()``/``gc.enable()`` shows here).

Every scan also records the CPU time each other thread of the process used during it (from
``/proc/self/task/*/stat``): leftover threads that burn CPU would show there.
"""
import gc
import os
import sys
import threading
import time
from functools import partial

from benchmarks.harness import BenchResult, benchmark, measure
from benchmarks.workloads.concurrency import _spin


def _rss_mb() -> float:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return round(int(line.split()[1]) / 1024, 1)
    return -1.0


def _task_cpu() -> dict:
    """CPU ticks (utime + stime) of every thread of this process, by thread id."""
    out = {}
    for tid in os.listdir("/proc/self/task"):
        try:
            with open(f"/proc/self/task/{tid}/stat") as f:
                fields = f.read().rsplit(")", 1)[1].split()
            out[int(tid)] = int(fields[11]) + int(fields[12])
        except (OSError, IndexError, ValueError):
            pass
    return out


@benchmark("scan_state_probe", quick={"items": 200, "spin": 200, "workers": [1, 2]},
           full={"items": 2_000, "spin": 4_000, "workers": [1, 8]})
def scan_state_probe(ctx, p):
    """parallel_scan's body under the process state left by earlier workloads (R136 Q4)."""
    from proto_db import ParallelPool
    items = list(range(p["items"]))
    work = partial(_spin, spin=p["spin"])
    expected = [work(x) for x in items]
    t0 = time.perf_counter()
    tracked = len(gc.get_objects())
    count_s = time.perf_counter() - t0
    state = {"threads_alive": len(threading.enumerate()),
             "thread_names": sorted({t.name.rstrip("0123456789-_") for t in threading.enumerate()}),
             "os_threads": len(os.listdir("/proc/self/task")), "rss_mb": _rss_mb(),
             "gc_tracked_objects": tracked, "gc_get_objects_s": round(count_s, 3),
             "gc_threshold": gc.get_threshold(), "gc_count": gc.get_count(), "gc_enabled": gc.isenabled(),
             "gil_enabled": sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True}
    t0 = time.perf_counter()
    gc.collect()
    state["full_collection_s"] = round(time.perf_counter() - t0, 3)
    result = BenchResult({"items": len(items), "spin": p["spin"], "workers": p["workers"], "state": state},
                         None, sum(expected))
    gc_events = []

    def on_gc(phase, info):
        gc_events.append((phase, time.perf_counter()))

    seq_samples, _ = measure(lambda: [work(x) for x in items], ctx.repeat, ctx.warmup, expect=expected)
    seq = sorted(seq_samples)[len(seq_samples) // 2]
    result.add_timing("sequential loop", seq_samples, per=len(items))
    figures = {}
    for variant in ("as is", "gc callbacks", "gc.freeze()", "gc.disable()", "as is, again"):
        if variant == "gc callbacks":
            gc.callbacks.append(on_gc)
        elif variant == "gc.freeze()":
            gc.freeze()
        elif variant == "gc.disable()":
            gc.disable()
        try:
            for workers in p["workers"]:
                gc_events.clear()
                me = threading.get_native_id()
                cpu0 = _task_cpu()
                with ParallelPool(workers) as pool:
                    samples, got = measure(lambda: pool.scan(items, work), ctx.repeat, ctx.warmup, expect=expected)
                cpu1 = _task_cpu()
                result.check(f"{variant}, {workers} worker(s): results equal the sequential run", got == expected, "")
                med = result.add_timing(f"{variant}: ParallelPool.scan, {workers} worker(s)", samples,
                                        per=len(items))["median"]
                pool_tids = set(cpu1) - set(cpu0)
                others = {tid: cpu1[tid] - cpu0[tid] for tid in cpu0 if tid in cpu1 and tid != me}
                pauses, starts = [], {}
                for phase, t in gc_events:
                    if phase == "start":
                        starts["s"] = t
                    elif "s" in starts:
                        pauses.append(t - starts.pop("s"))
                figures[f"{variant}, {workers} worker(s)"] = {
                    "speedup": round(seq / med, 2),
                    "collections": len(pauses) if variant == "gc callbacks" else None,
                    "collection_s_total": round(sum(pauses), 3) if variant == "gc callbacks" else None,
                    "cpu_ticks_main": cpu1.get(me, 0) - cpu0.get(me, 0),
                    "cpu_ticks_new_threads": sum(cpu1[t] for t in pool_tids),
                    "cpu_ticks_other_old_threads": sum(others.values()),
                    "busiest_old_threads": sorted(others.values(), reverse=True)[:5]}
        finally:
            if variant == "gc callbacks":
                gc.callbacks.remove(on_gc)
            elif variant == "gc.freeze()":
                gc.unfreeze()
            elif variant == "gc.disable()":
                gc.enable()
    result.metrics["state"] = state
    result.metrics["figures"] = figures
    result.checksum = sum(expected)
    return result
