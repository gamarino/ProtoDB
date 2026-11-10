"""Parallel scan over a sequence with threads (spec section 4, decision D40).

On CPython 3.14t (free-threading) a CPU-bound function mapped over many items
runs on several cores; with the GIL enabled the same code is correct but only
I/O-bound functions gain.

Public API:

- :func:`parallel_scan` ``(items, fn, *, workers=None, chunk=None, ordered=True)``:
  ``[fn(x) for x in items]`` computed by several threads.  ``None`` results are
  kept (``None`` is a value).  With ``ordered=True`` (default) the result list
  is in input order; with ``ordered=False`` the order is unspecified.
- :class:`ParallelPool`: a reusable pool (context manager, :meth:`ParallelPool.shutdown`).
- :func:`parallel_filter`: runs the predicate of a ``Filter`` or ``PythonFilter``
  plan over its materialised source in parallel; the result equals
  ``list(plan.execute())``.
- :class:`ParallelConfig` (validated, read from ``PROTO_PARALLEL_*`` environment
  variables at call time) and :class:`AdaptiveChunkController` (per-worker chunk sizing).

Semantics (D40):

- **Work distribution.** Items are claimed in chunks from one shared cursor
  under a lock; each worker claims its next chunk when it finishes the
  previous one, so faster workers take more chunks (dynamic load balancing).
  The calling thread is one of the workers, so a function that itself scans on
  the same pool always makes progress (no nested-scan deadlock): not-yet-started
  helper tasks are cancelled once the cursor is exhausted.
- **Completion.** A scan returns only after every helper task has finished or
  was cancelled before starting; results are collected from the tasks
  themselves, never inferred from empty queues, so no result can be lost.
- **Exceptions.** The first exception raised by ``fn`` (in time) is re-raised in
  the caller with its own type and traceback, with a note naming the item
  index; exceptions raised concurrently by other workers are listed in a
  second note and kept as objects (:func:`proto_db.errors.secondary_errors`).
  After the first exception, workers stop claiming chunks and
  stop between items, so the remaining work is cancelled.  ``BaseException``
  subclasses propagate the same way.
- **Shutdown.** A temporary pool is shut down (threads joined) before
  :func:`parallel_scan` returns or raises.

Environment variables (validated; an invalid value raises
``ProtoValidationError`` naming the variable): ``PROTO_PARALLEL_MAX_WORKERS``,
``PROTO_PARALLEL_INITIAL_CHUNK``, ``PROTO_PARALLEL_MIN_CHUNK``,
``PROTO_PARALLEL_MAX_CHUNK``, ``PROTO_PARALLEL_TARGET_MS_LOW``,
``PROTO_PARALLEL_TARGET_MS_HIGH``, ``PROTO_PARALLEL_EMA_ALPHA``.
"""
from __future__ import annotations

import math
import os
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field

from ..errors import ProtoUnexpectedError, ProtoUserError, ProtoValidationError, attach_secondary_errors
from ..query.expressions import evaluate
from ..query.plans import Filter, PythonFilter, QueryPlan

__all__ = ["ParallelConfig", "AdaptiveChunkController", "ParallelPool", "parallel_scan", "parallel_filter",
           "MetricsCallback"]

MetricsCallback = Callable[[str, dict], None]

_MAX_LISTED_ERRORS = 10


def _default_workers() -> int:
    count = os.process_cpu_count() if hasattr(os, "process_cpu_count") else os.cpu_count()
    return max(1, min(count or 1, 8))


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class ParallelConfig:
    """Validated parallel scan settings.

    ``max_workers``: default number of workers (the calling thread included),
    ``min(cpu count, 8)`` by default.  ``initial_chunk_size``,
    ``min_chunk_size``, ``max_chunk_size``: adaptive chunk sizes in items
    (``min <= initial <= max``).  ``target_ms_low`` / ``target_ms_high``: a
    chunk faster than the low target grows the size by 1.5x, slower than the
    high target shrinks it (``0 < low < high``).  ``chunk_ema_alpha``: weight
    of the newest chunk time in the moving average (``0 < alpha <= 1``).
    Invalid values raise ``ProtoValidationError``.
    """

    max_workers: int = field(default_factory=_default_workers)
    initial_chunk_size: int = 1000
    min_chunk_size: int = 128
    max_chunk_size: int = 8192
    target_ms_low: float = 0.5
    target_ms_high: float = 2.0
    chunk_ema_alpha: float = 0.2

    def __post_init__(self):
        for name in ("max_workers", "initial_chunk_size", "min_chunk_size", "max_chunk_size"):
            value = getattr(self, name)
            if not _is_int(value) or value < 1:
                raise ProtoValidationError(f"ParallelConfig.{name} must be an int >= 1, not {value!r}")
        for name in ("target_ms_low", "target_ms_high", "chunk_ema_alpha"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) \
                    or value <= 0:
                raise ProtoValidationError(f"ParallelConfig.{name} must be a finite number > 0, not {value!r}")
        if not self.min_chunk_size <= self.initial_chunk_size <= self.max_chunk_size:
            raise ProtoValidationError(
                f"ParallelConfig needs min_chunk_size <= initial_chunk_size <= max_chunk_size, got "
                f"{self.min_chunk_size}, {self.initial_chunk_size}, {self.max_chunk_size}")
        if not self.target_ms_low < self.target_ms_high:
            raise ProtoValidationError(f"ParallelConfig needs target_ms_low < target_ms_high, got "
                                       f"{self.target_ms_low}, {self.target_ms_high}")
        if self.chunk_ema_alpha > 1:
            raise ProtoValidationError(f"ParallelConfig.chunk_ema_alpha must be <= 1, not {self.chunk_ema_alpha}")

    _ENV = {
        "max_workers": ("PROTO_PARALLEL_MAX_WORKERS", int),
        "initial_chunk_size": ("PROTO_PARALLEL_INITIAL_CHUNK", int),
        "min_chunk_size": ("PROTO_PARALLEL_MIN_CHUNK", int),
        "max_chunk_size": ("PROTO_PARALLEL_MAX_CHUNK", int),
        "target_ms_low": ("PROTO_PARALLEL_TARGET_MS_LOW", float),
        "target_ms_high": ("PROTO_PARALLEL_TARGET_MS_HIGH", float),
        "chunk_ema_alpha": ("PROTO_PARALLEL_EMA_ALPHA", float),
    }

    @classmethod
    def from_env(cls, *, environ: Mapping[str, str] | None = None) -> "ParallelConfig":
        """Build a config from the ``PROTO_PARALLEL_*`` variables; unset ones keep their defaults.

        ``environ`` (keyword-only) replaces ``os.environ``, e.g. for testing.
        An unparsable or out-of-range value raises ``ProtoValidationError``
        naming the variable(s) involved.
        """
        env = os.environ if environ is None else environ
        values = {}
        for attr, (var, kind) in cls._ENV.items():
            raw = env.get(var)
            if raw is None:
                continue
            try:
                values[attr] = kind(raw.strip())
            except ValueError as e:
                raise ProtoValidationError(f"Environment variable {var}={raw!r} is not a valid "
                                           f"{kind.__name__}") from e
        try:
            return cls(**values)
        except ProtoValidationError as e:
            involved = [var for attr, (var, _) in cls._ENV.items() if attr in values and attr in str(e)]
            if not involved:
                raise
            raise ProtoValidationError(f"Invalid parallel configuration from environment variable(s) "
                                       f"{', '.join(involved)}: {e.message}") from e


class AdaptiveChunkController:
    """Per-worker chunk size adapted to measured chunk times with an exponential moving average.

    ``limit`` (optional) caps every size, so that a short input still splits
    into several chunks; the minimum is lowered to the cap when needed.
    Not thread-safe: each worker owns one controller.
    """

    def __init__(self, cfg: ParallelConfig, *, limit: int | None = None):
        cap = cfg.max_chunk_size if limit is None else max(1, limit)
        self.cfg = cfg
        self.min_size = min(cfg.min_chunk_size, cap)
        self.max_size = min(cfg.max_chunk_size, cap)
        self.size = max(self.min_size, min(cfg.initial_chunk_size, self.max_size))
        self.ema_ms: float | None = None

    def next_size(self) -> int:
        """The number of items to claim next."""
        return self.size

    def on_chunk_timing(self, elapsed_ms: float) -> None:
        """Feed the duration of the last chunk and adapt the size (1.5x up or down, clamped)."""
        a = self.cfg.chunk_ema_alpha
        self.ema_ms = elapsed_ms if self.ema_ms is None else a * elapsed_ms + (1 - a) * self.ema_ms
        size = self.size
        if self.ema_ms < self.cfg.target_ms_low:
            size = max(size + 1, int(size * 1.5))
        elif self.ema_ms > self.cfg.target_ms_high:
            size = int(size / 1.5)
        self.size = max(self.min_size, min(size, self.max_size))


def _check_workers(workers) -> int:
    if not _is_int(workers) or workers < 1:
        raise ProtoValidationError(f"workers must be an int >= 1, not {workers!r}")
    return workers


def _check_chunk(chunk) -> int | None:
    if chunk is not None and (not _is_int(chunk) or chunk < 1):
        raise ProtoValidationError(f"chunk must be None (adaptive) or an int >= 1, not {chunk!r}")
    return chunk


def _check_fn(fn) -> None:
    if not callable(fn):
        raise ProtoValidationError(f"parallel_scan needs a callable, not {fn!r}")


class _Scan:
    """Shared state of one scan: the input, the cursor and the recorded errors (lock-protected)."""

    def __init__(self, seq: Sequence, fn: Callable, cfg: ParallelConfig, chunk: int | None, drains: int):
        self.seq = seq
        self.fn = fn
        self.n = len(seq)
        self.cfg = cfg
        self.chunk = chunk
        self.limit = max(1, -(-self.n // (drains * 4)))  # at least ~4 chunks per worker
        self.lock = threading.Lock()
        self.cursor = 0
        self.cancel = threading.Event()
        self.errors: list[tuple[int | None, BaseException]] = []

    def claim(self, size: int) -> tuple[int, int] | None:
        """Claim the next ``size`` items: ``(start, end)``, or None when the input is exhausted."""
        with self.lock:
            start = self.cursor
            if start >= self.n:
                return None
            end = min(start + size, self.n)
            self.cursor = end
            return start, end

    def record(self, index: int | None, exc: BaseException) -> None:
        """Record an exception (first in time first) and cancel the remaining work."""
        with self.lock:
            if not any(e is exc for _, e in self.errors):
                self.errors.append((index, exc))
        self.cancel.set()

    def drain(self, worker_id: int) -> tuple:
        """Process chunks until the input is exhausted or the scan is cancelled.

        Returns ``(worker_id, [(start, results), ...], records, busy_ms)``.  An
        exception raised by ``fn`` is recorded, cancels the scan and is re-raised.
        """
        ctrl = None if self.chunk is not None else AdaptiveChunkController(self.cfg, limit=self.limit)
        seq, fn, cancel = self.seq, self.fn, self.cancel
        parts = []
        records = 0
        busy_ms = 0.0
        while not cancel.is_set():
            claimed = self.claim(self.chunk if ctrl is None else ctrl.next_size())
            if claimed is None:
                break
            start, end = claimed
            t0 = time.perf_counter()
            results = []
            index = start
            try:
                for index in range(start, end):
                    if cancel.is_set():
                        return worker_id, parts, records, busy_ms
                    results.append(fn(seq[index]))
            except BaseException as e:
                self.record(index, e)
                raise
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if ctrl is not None:
                ctrl.on_chunk_timing(elapsed_ms)
            parts.append((start, results))
            records += end - start
            busy_ms += elapsed_ms
        return worker_id, parts, records, busy_ms


class ParallelPool:
    """A reusable pool of ``workers`` workers (the calling thread counts as one).

    ``ParallelPool(workers=None, *, config=None)``: ``workers`` defaults to
    ``config.max_workers`` and ``config`` to :meth:`ParallelConfig.from_env`.
    Helper threads (``workers - 1``) are created lazily on the first scan and
    joined by :meth:`shutdown` (idempotent; also called on leaving a ``with``
    block).  Scanning after shutdown raises ``ProtoUserError``.  Thread-safe:
    several threads may scan on one pool concurrently, and ``fn`` may itself
    scan on the pool.
    """

    def __init__(self, workers: int | None = None, *, config: ParallelConfig | None = None):
        if workers is not None:
            _check_workers(workers)
        if config is not None and not isinstance(config, ParallelConfig):
            raise ProtoValidationError(f"config must be a ParallelConfig, not {config!r}")
        self._config = config if config is not None else ParallelConfig.from_env()
        self._workers = self._config.max_workers if workers is None else workers
        self._lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._closed = False

    @property
    def workers(self) -> int:
        """The number of workers, the calling thread included."""
        return self._workers

    @property
    def config(self) -> ParallelConfig:
        """The configuration used for chunk sizing."""
        return self._config

    def __enter__(self) -> "ParallelPool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.shutdown()

    def shutdown(self, wait: bool = True) -> None:
        """Refuse new scans and stop the helper threads (joined when ``wait`` is True).

        Scans already running complete normally.
        """
        with self._lock:
            self._closed = True
            executor, self._executor = self._executor, None
        if executor is not None:
            executor.shutdown(wait=wait)

    def scan(self, items: Iterable, fn: Callable, *, chunk: int | None = None, ordered: bool = True,
             metrics_cb: MetricsCallback | None = None) -> list:
        """Return ``[fn(x) for x in items]`` computed in parallel (module docstring, "Semantics").

        ``items`` that is not a ``Sequence`` is first copied into a list.
        ``chunk`` fixes the chunk size; None adapts it per worker.
        ``metrics_cb(kind, data)``, if given, is called in the calling thread
        after a successful scan with ``"final_worker"`` (one per worker that
        ran: ``worker_id``, ``chunks_processed``, ``records_processed``,
        ``busy_ms``) and then ``"final_global"`` (``workers``, ``chunks``,
        ``items``, ``elapsed_ms``); its exceptions propagate.
        """
        _check_fn(fn)
        chunk = _check_chunk(chunk)
        with self._lock:
            if self._closed:
                raise ProtoUserError("This ParallelPool has been shut down")
        seq = items if isinstance(items, Sequence) else list(items)
        t0 = time.perf_counter()
        drains = max(1, min(self._workers, len(seq)))
        state = _Scan(seq, fn, self._config, chunk, drains)
        futures = []
        if drains > 1:
            with self._lock:
                if self._closed:
                    raise ProtoUserError("This ParallelPool has been shut down")
                if self._executor is None:
                    self._executor = ThreadPoolExecutor(max_workers=self._workers - 1,
                                                        thread_name_prefix="proto-parallel")
                futures = [self._executor.submit(state.drain, w) for w in range(1, drains)]
        outcomes = []
        try:
            outcomes.append(state.drain(0))
        except BaseException as e:
            # An exception raised by fn was recorded by drain() and is re-raised below with the
            # others; anything else (e.g. an interrupt between items) is recorded here.
            state.record(None, e)
        finally:
            outcomes.extend(self._collect(futures, state))
        if state.errors:
            raise _first_error(state.errors)
        return self._assemble(state, outcomes, ordered, metrics_cb, t0)

    @staticmethod
    def _collect(futures: list, state: _Scan) -> list:
        """Cancel helper tasks that have not started, wait for the others and return their outcomes."""
        running = [f for f in futures if not f.cancel()]
        wait(running)
        outcomes = []
        for f in running:
            exc = f.exception()
            if exc is None:
                outcomes.append(f.result())
            else:
                state.record(None, exc)  # already recorded when raised by fn; never dropped otherwise
        return outcomes

    def _assemble(self, state: _Scan, outcomes: list, ordered: bool, metrics_cb, t0: float) -> list:
        parts = [part for _, worker_parts, _, _ in outcomes for part in worker_parts]
        if ordered:
            parts.sort(key=lambda part: part[0])
        result = [value for _, values in parts for value in values]
        if len(result) != state.n:
            raise ProtoUnexpectedError(f"parallel scan produced {len(result)} results for {state.n} items")
        if metrics_cb is not None:
            for worker_id, worker_parts, records, busy_ms in sorted(outcomes, key=lambda o: o[0]):
                metrics_cb("final_worker", {"worker_id": worker_id, "chunks_processed": len(worker_parts),
                                            "records_processed": records, "busy_ms": busy_ms})
            metrics_cb("final_global", {"workers": len(outcomes), "chunks": len(parts), "items": state.n,
                                        "elapsed_ms": (time.perf_counter() - t0) * 1000.0})
        return result


def _first_error(errors: list) -> BaseException:
    """The first recorded exception, annotated with its item index and the other exceptions."""
    index, first = errors[0]
    if index is not None:
        first.add_note(f"parallel_scan: raised by fn for item {index}")
    others = errors[1:]
    if others:
        listed = "; ".join(f"item {i}: {e!r}" if i is not None else repr(e) for i, e in others[:_MAX_LISTED_ERRORS])
        more = "; ..." if len(others) > _MAX_LISTED_ERRORS else ""
        first.add_note(f"parallel_scan: {len(others)} other exception(s) raised concurrently: {listed}{more}")
        attach_secondary_errors(first, [e for _, e in others])
    return first


def parallel_scan(items: Iterable, fn: Callable, *, workers: int | None = None, chunk: int | None = None,
                  ordered: bool = True, config: ParallelConfig | None = None, pool: ParallelPool | None = None,
                  metrics_cb: MetricsCallback | None = None) -> list:
    """Return ``[fn(x) for x in items]`` computed by several threads (module docstring, "Semantics").

    ``workers`` defaults to ``config.max_workers``; ``config`` defaults to
    :meth:`ParallelConfig.from_env` (read at call time).  With ``pool`` the
    scan runs on that pool (``workers`` and ``config`` must then be omitted);
    otherwise a temporary pool is created and shut down before returning.
    ``chunk``, ``ordered`` and ``metrics_cb`` are as in :meth:`ParallelPool.scan`.
    """
    _check_fn(fn)
    _check_chunk(chunk)
    if workers is not None:
        _check_workers(workers)
    if pool is not None:
        if workers is not None or config is not None:
            raise ProtoValidationError("parallel_scan: pass either pool or workers/config, not both")
        return pool.scan(items, fn, chunk=chunk, ordered=ordered, metrics_cb=metrics_cb)
    with ParallelPool(workers, config=config) as temporary:
        return temporary.scan(items, fn, chunk=chunk, ordered=ordered, metrics_cb=metrics_cb)


def parallel_filter(plan: QueryPlan, *, workers: int | None = None, chunk: int | None = None,
                    config: ParallelConfig | None = None, pool: ParallelPool | None = None) -> list:
    """Evaluate a ``Filter`` or ``PythonFilter`` plan with its predicate run in parallel.

    The plan's source is executed (sequentially) into a list, the predicate is
    evaluated over it with :func:`parallel_scan`, and the accepted elements are
    returned in source order: the result equals ``list(plan.execute())``.
    Exceptions raised by the predicate propagate.  Other plans raise
    ``ProtoValidationError``.
    """
    if isinstance(plan, Filter):
        expr = plan.expr

        def predicate(element):
            return evaluate(expr, element)
    elif isinstance(plan, PythonFilter):
        predicate = plan.fn
    else:
        raise ProtoValidationError(f"parallel_filter needs a Filter or PythonFilter plan, not "
                                   f"{type(plan).__name__}")
    elements = list(plan.source.execute())
    flags = parallel_scan(elements, predicate, workers=workers, chunk=chunk, config=config, pool=pool)
    return [element for element, keep in zip(elements, flags) if keep]
