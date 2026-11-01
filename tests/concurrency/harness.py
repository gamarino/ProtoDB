"""Thread harness for the stress suite: barrier start, bounded joins, error collection, attempt counts.

Every worker waits on a shared barrier so all threads start together; an exception
in any worker is recorded and aborts the barrier (no thread waits forever for a
dead peer).  Threads are daemons and are joined against one deadline: a thread
still alive then fails the test instead of hanging the suite.
"""
import threading
import time

import pytest

#: Seconds a worker waits for its peers at the start barrier.
BARRIER_TIMEOUT = 30.0


class Attempts:
    """Counts calls of the functions given to ``Database.run``; conflicts = attempts - commits.

    ``Database.run`` retries only on ``ProtoConflictError``, so every attempt beyond the
    first of a successful ``run`` call was ended by a conflict.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.attempts = 0
        self.commits = 0

    def wrap(self, fn):
        """Return ``fn`` counting each call as one attempt."""
        def counted(tr):
            with self._lock:
                self.attempts += 1
            return fn(tr)
        return counted

    def run(self, db, fn, **kwargs):
        """``db.run(fn)`` counting attempts and the successful commit."""
        result = db.run(self.wrap(fn), **kwargs)
        with self._lock:
            self.commits += 1
        return result

    @property
    def conflicts(self) -> int:
        """Attempts that ended in a conflict (and were retried)."""
        return self.attempts - self.commits

    def rate(self) -> float:
        """Conflicts per attempt."""
        return self.conflicts / self.attempts if self.attempts else 0.0

    def __str__(self):
        return f"{self.commits} commits, {self.attempts} attempts, {self.conflicts} conflicts ({self.rate():.1%})"


def run_threads(targets, *, timeout: float = 100.0) -> float:
    """Run each callable of ``targets`` in its own thread, all released by one barrier.

    Returns the wall time from release to the last join.  Fails the test if a thread
    raised (the first error is re-raised with every error listed) or if any thread is
    still alive ``timeout`` seconds after release (a hang or a deadlock).
    """
    barrier = threading.Barrier(len(targets) + 1)
    errors = []
    errors_lock = threading.Lock()

    def body(target, i):
        try:
            barrier.wait(BARRIER_TIMEOUT)
            target()
        except BaseException as e:  # recorded and reported by the caller
            with errors_lock:
                errors.append((i, e))
            barrier.abort()

    threads = [threading.Thread(target=body, args=(t, i), daemon=True, name=f"stress-{i}")
               for i, t in enumerate(targets)]
    for t in threads:
        t.start()
    try:
        barrier.wait(BARRIER_TIMEOUT)
    except threading.BrokenBarrierError:
        pass  # a worker failed before the release; its error is reported below
    start = time.monotonic()
    deadline = start + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    elapsed = time.monotonic() - start
    hung = [t.name for t in threads if t.is_alive()]
    if hung:
        pytest.fail(f"{len(hung)} thread(s) still running after {timeout}s (hang or deadlock): {hung}")
    if errors:
        summary = "; ".join(f"thread {i}: {type(e).__name__}: {e}" for i, e in errors[:5])
        raise AssertionError(f"{len(errors)} worker thread(s) failed: {summary}") from errors[0][1]
    return elapsed


def report(name: str, elapsed: float, *parts) -> None:
    """Print one result line (shown with ``-s`` or ``-rA``; also kept in the captured output)."""
    print(f"[stress] {name}: {elapsed:.2f}s; " + "; ".join(str(p) for p in parts))
