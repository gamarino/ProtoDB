"""Unit tests for the 2.0 parallel scan, proto_db.runtime.parallel (Task 8.1, D40).

Catches the 0.1 defects (proto_db/parallel.py): exceptions raised by the
mapped function swallowed by the worker loop (``except Exception: pass``);
results lost because completion was decided by polling for empty queues
while a long task was still running; environment variables parsed at import
time with invalid values silently replaced by defaults; worker threads left
running (daemon threads joined with a timeout).  It also checks ordered
output, cancellation of the remaining work after a failure, clean shutdown
(thread count back to its baseline), nested scans on one pool completing
without deadlock, and the ``parallel_filter`` helper agreeing with the plan
it parallelises.
"""
import random
import threading
import time

import pytest
from hypothesis import given, settings, strategies as st

from proto_db.errors import ProtoUserError, ProtoValidationError
from proto_db.query.expressions import Term
from proto_db.query.plans import Filter, Limit, PythonFilter, ValuesScan
from proto_db.runtime.parallel import (AdaptiveChunkController, ParallelConfig, ParallelPool, parallel_filter,
                                       parallel_scan)


def _settle(baseline: int, timeout: float = 5.0) -> int:
    """Return threading.active_count() once it reaches ``baseline`` (or after ``timeout``)."""
    deadline = time.monotonic() + timeout
    while threading.active_count() > baseline and time.monotonic() < deadline:
        time.sleep(0.005)
    return threading.active_count()


# -- results ----------------------------------------------------------------------------------

@pytest.mark.parametrize("n", [0, 1, 7, 1000])
@pytest.mark.parametrize("workers", [1, 2, 4, 8])
@pytest.mark.parametrize("chunk", [None, 1, 3])
def test_ordered_scan_equals_sequential_map(n, workers, chunk):
    items = list(range(n))
    assert parallel_scan(items, lambda x: x * x, workers=workers, chunk=chunk) == [x * x for x in items]


def test_unordered_scan_returns_every_result_once():
    items = list(range(5000))
    out = parallel_scan(items, lambda x: -x, workers=4, chunk=7, ordered=False)
    assert sorted(out) == sorted(-x for x in items)


def test_none_results_are_kept():
    """None is a value: the 0.1 helper dropped None results (filter semantics) -- the map keeps them."""
    assert parallel_scan([1, 2, 3], lambda x: None, workers=2) == [None, None, None]


def test_any_iterable_is_accepted():
    assert parallel_scan((x for x in range(100)), str, workers=3) == [str(x) for x in range(100)]
    assert parallel_scan(range(50), lambda x: x + 1, workers=3, chunk=4) == list(range(1, 51))


@settings(max_examples=40, deadline=None)
@given(delays=st.lists(st.sampled_from([0.0, 0.0, 0.0005, 0.001, 0.003]), max_size=60),
       workers=st.integers(1, 6), chunk=st.one_of(st.none(), st.integers(1, 5)),
       ordered=st.booleans())
def test_no_result_lost_whatever_the_task_duration(delays, workers, chunk, ordered):
    """Property: random per-item durations never lose, duplicate or reorder (ordered=True) results."""
    def fn(i):
        time.sleep(delays[i])
        return (i, delays[i])
    out = parallel_scan(range(len(delays)), fn, workers=workers, chunk=chunk, ordered=ordered)
    expected = [(i, d) for i, d in enumerate(delays)]
    assert (out if ordered else sorted(out)) == expected


def test_one_slow_chunk_is_not_lost():
    """The 0.1 completion check returned while a long task was still running: its results vanished."""
    def fn(x):
        if x == 0:
            time.sleep(0.3)
        return x
    assert parallel_scan(range(200), fn, workers=4, chunk=10) == list(range(200))


def test_work_runs_on_several_threads():
    seen = set()
    lock = threading.Lock()

    def fn(x):
        with lock:
            seen.add(threading.get_ident())
        time.sleep(0.002)
        return x
    parallel_scan(range(64), fn, workers=4, chunk=1)
    assert len(seen) > 1


# -- exceptions -------------------------------------------------------------------------------

def test_exception_in_fn_propagates_and_cancels_remaining_work():
    calls = []
    lock = threading.Lock()

    def fn(x):
        with lock:
            calls.append(x)
        if x == 5:
            raise ValueError("bad item")
        time.sleep(0.001)
        return x
    baseline = threading.active_count()
    t0 = time.perf_counter()
    with pytest.raises(ValueError, match="bad item") as info:
        parallel_scan(range(20_000), fn, workers=4, chunk=8)
    assert time.perf_counter() - t0 < 5.0
    assert len(calls) < 2_000, "remaining work was not cancelled"
    assert any("item 5" in note for note in getattr(info.value, "__notes__", []))
    assert _settle(baseline) == baseline


def test_first_exception_is_raised_others_attached_as_notes():
    barrier = threading.Barrier(3, timeout=10)

    def fn(x):
        barrier.wait()  # three items fail concurrently
        raise KeyError(x)
    with pytest.raises(KeyError) as info:
        parallel_scan(range(3), fn, workers=3, chunk=1)
    notes = "\n".join(info.value.__notes__)
    assert "2 other exception(s)" in notes
    assert notes.count("KeyError") == 2


def test_sequential_path_propagates_too():
    with pytest.raises(ZeroDivisionError):
        parallel_scan([1, 0, 2], lambda x: 1 / x, workers=1)


def test_base_exception_propagates():
    class Stop(BaseException):
        pass

    def fn(x):
        if x == 3:
            raise Stop()
        return x
    baseline = threading.active_count()
    with pytest.raises(Stop):
        parallel_scan(range(100), fn, workers=4, chunk=2)
    assert _settle(baseline) == baseline


# -- pool lifecycle ---------------------------------------------------------------------------

def test_threads_return_to_baseline_after_scan():
    baseline = threading.active_count()
    parallel_scan(range(1000), lambda x: x, workers=6, chunk=5)
    assert _settle(baseline) == baseline


def test_pool_context_manager_reuse_and_shutdown():
    baseline = threading.active_count()
    with ParallelPool(4) as pool:
        assert pool.workers == 4
        assert pool.scan(range(100), lambda x: x + 1) == list(range(1, 101))
        assert pool.scan(range(10), lambda x: x * 2, ordered=False, chunk=1) is not None
        assert parallel_scan(range(5), abs, pool=pool) == list(range(5))
    assert _settle(baseline) == baseline
    with pytest.raises(ProtoUserError):
        pool.scan([1], abs)
    pool.shutdown()  # idempotent


def test_explicit_shutdown_joins_threads():
    baseline = threading.active_count()
    pool = ParallelPool(3)
    pool.scan(range(300), lambda x: x, chunk=1)
    pool.shutdown()
    assert threading.active_count() == baseline


def test_nested_scan_on_the_same_pool_does_not_deadlock():
    """A fn that itself scans on the pool must complete even when every pool thread is busy."""
    result = {}

    def run():
        with ParallelPool(2) as pool:
            def outer(x):
                return sum(pool.scan(range(x), lambda y: y, chunk=1))
            result["out"] = pool.scan(range(30), outer, chunk=1)
    t = threading.Thread(target=run)
    t.start()
    t.join(timeout=60)
    assert not t.is_alive(), "nested scan deadlocked"
    assert result["out"] == [sum(range(x)) for x in range(30)]


def test_metrics_callback_reports_the_work_done():
    events = []
    parallel_scan(range(500), abs, workers=3, chunk=10, metrics_cb=lambda kind, data: events.append((kind, data)))
    final = [d for k, d in events if k == "final_global"]
    assert len(final) == 1 and final[0]["items"] == 500 and final[0]["chunks"] == 50
    per_worker = [d for k, d in events if k == "final_worker"]
    assert sum(d["records_processed"] for d in per_worker) == 500


# -- validation and configuration -------------------------------------------------------------

@pytest.mark.parametrize("kwargs", [{"workers": 0}, {"workers": -1}, {"workers": 1.5}, {"workers": True},
                                    {"chunk": 0}, {"chunk": "3"}])
def test_invalid_arguments_are_rejected(kwargs):
    with pytest.raises(ProtoValidationError):
        parallel_scan([1, 2], abs, **kwargs)


def test_fn_must_be_callable():
    with pytest.raises(ProtoValidationError):
        parallel_scan([1], 42)


def test_config_defaults_and_valid_env():
    cfg = ParallelConfig.from_env(environ={})
    assert cfg == ParallelConfig()
    cfg = ParallelConfig.from_env(environ={"PROTO_PARALLEL_MAX_WORKERS": "3", "PROTO_PARALLEL_MIN_CHUNK": "2",
                                           "PROTO_PARALLEL_INITIAL_CHUNK": "4", "PROTO_PARALLEL_MAX_CHUNK": "16",
                                           "PROTO_PARALLEL_TARGET_MS_LOW": "0.1",
                                           "PROTO_PARALLEL_TARGET_MS_HIGH": "1.5",
                                           "PROTO_PARALLEL_EMA_ALPHA": "0.5"})
    assert (cfg.max_workers, cfg.min_chunk_size, cfg.initial_chunk_size, cfg.max_chunk_size) == (3, 2, 4, 16)
    assert (cfg.target_ms_low, cfg.target_ms_high, cfg.chunk_ema_alpha) == (0.1, 1.5, 0.5)


@pytest.mark.parametrize("name, value", [
    ("PROTO_PARALLEL_MAX_WORKERS", "abc"), ("PROTO_PARALLEL_MAX_WORKERS", "0"),
    ("PROTO_PARALLEL_INITIAL_CHUNK", "1.5"), ("PROTO_PARALLEL_MIN_CHUNK", "-3"),
    ("PROTO_PARALLEL_MAX_CHUNK", ""), ("PROTO_PARALLEL_TARGET_MS_LOW", "x"),
    ("PROTO_PARALLEL_TARGET_MS_HIGH", "nan"), ("PROTO_PARALLEL_EMA_ALPHA", "0"),
    ("PROTO_PARALLEL_EMA_ALPHA", "1.5"), ("PROTO_PARALLEL_TARGET_MS_LOW", "-1"),
])
def test_invalid_env_value_names_the_variable(name, value):
    with pytest.raises(ProtoValidationError, match=name):
        ParallelConfig.from_env(environ={name: value})


def test_inconsistent_env_values_name_the_variables():
    with pytest.raises(ProtoValidationError, match="PROTO_PARALLEL_MIN_CHUNK"):
        ParallelConfig.from_env(environ={"PROTO_PARALLEL_MIN_CHUNK": "100", "PROTO_PARALLEL_MAX_CHUNK": "10"})
    with pytest.raises(ProtoValidationError, match="PROTO_PARALLEL_TARGET_MS_LOW"):
        ParallelConfig.from_env(environ={"PROTO_PARALLEL_TARGET_MS_LOW": "5", "PROTO_PARALLEL_TARGET_MS_HIGH": "1"})


def test_env_is_read_at_call_time(monkeypatch):
    monkeypatch.setenv("PROTO_PARALLEL_MAX_WORKERS", "many")
    with pytest.raises(ProtoValidationError, match="PROTO_PARALLEL_MAX_WORKERS"):
        parallel_scan([1, 2], abs)
    monkeypatch.setenv("PROTO_PARALLEL_MAX_WORKERS", "2")
    assert parallel_scan([1, -2], abs) == [1, 2]


@pytest.mark.parametrize("kwargs", [{"max_workers": 0}, {"min_chunk_size": 0}, {"min_chunk_size": 10, "max_chunk_size": 5},
                                    {"initial_chunk_size": 1, "min_chunk_size": 2},
                                    {"target_ms_low": 3.0, "target_ms_high": 2.0}, {"chunk_ema_alpha": 0.0}])
def test_invalid_config_fields_are_rejected(kwargs):
    with pytest.raises(ProtoValidationError):
        ParallelConfig(**kwargs)


def test_adaptive_chunk_controller_ema_and_clamping():
    """Port of the 0.1 test: fast chunks grow the size up to the maximum, slow ones shrink it to the minimum."""
    cfg = ParallelConfig(max_workers=2, initial_chunk_size=1000, min_chunk_size=128, max_chunk_size=8192,
                         target_ms_low=0.5, target_ms_high=2.0, chunk_ema_alpha=0.5)
    ctrl = AdaptiveChunkController(cfg)
    assert ctrl.next_size() == 1000
    ctrl.on_chunk_timing(0.1)
    assert ctrl.next_size() > 1000
    for _ in range(20):
        ctrl.on_chunk_timing(0.1)
    assert ctrl.next_size() == cfg.max_chunk_size
    for _ in range(40):
        ctrl.on_chunk_timing(10.0)
    assert ctrl.next_size() == cfg.min_chunk_size


# -- plan integration -------------------------------------------------------------------------

def _rows(n):
    rnd = random.Random(7)
    return [{"a": rnd.randrange(10), "i": i} for i in range(n)]


def test_parallel_filter_equals_filter_plan():
    plan = Filter(ValuesScan(_rows(3000)), Term("a", "==", 3))
    assert parallel_filter(plan, workers=4, chunk=16) == list(plan.execute())


def test_parallel_filter_equals_python_filter_plan():
    plan = PythonFilter(ValuesScan(_rows(3000)), lambda r: r["i"] % 7 == 0, "i % 7 == 0")
    assert parallel_filter(plan, workers=4) == list(plan.execute())


def test_parallel_filter_propagates_predicate_errors():
    plan = PythonFilter(ValuesScan(_rows(100)), lambda r: 1 / (r["i"] - 50), "1 / (i - 50)")
    with pytest.raises(ZeroDivisionError):
        parallel_filter(plan, workers=4, chunk=3)


def test_parallel_filter_rejects_other_plans():
    with pytest.raises(ProtoValidationError):
        parallel_filter(Limit(ValuesScan([1, 2]), 1))


def test_secondary_exceptions_are_kept_as_objects():
    from proto_db.errors import secondary_errors
    barrier = threading.Barrier(3, timeout=10)

    def fn(x):
        barrier.wait()
        raise KeyError(x)
    with pytest.raises(KeyError) as info:
        parallel_scan(range(3), fn, workers=3, chunk=1)
    others = secondary_errors(info.value)
    assert len(others) == 2 and all(isinstance(e, KeyError) for e in others)
    assert {info.value.args[0], *(e.args[0] for e in others)} == {0, 1, 2}
    assert secondary_errors(ValueError()) == ()
