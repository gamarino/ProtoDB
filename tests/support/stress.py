"""Stress mode of the cluster tests (ruling R142).

Under heavy synthetic load (``benchmarks/cluster_stress.py``, ruling R125) a test that stops at
its first liveness floor -- a progress count, ``commits > 0``, a timing or silence bound, a
confirmation that should have succeeded in time -- never reaches its safety asserts, so the
run proves nothing about safety.  With ``PROTO_STRESS=1`` the liveness checks of the cluster
tests are recorded and counted instead of asserted, waits get ``FACTOR`` times their budget,
and the test goes on to its safety asserts, which stay unconditional in both modes.

With the variable unset (quiet mode, every ordinary run) each helper asserts exactly the
condition the test asserted before, with the same budget: quiet mode is as strict as it was.

Each recorded event is one JSON line appended to the file named by ``PROTO_STRESS_LOG`` (when
set): ``{"test": <pytest node id>, "kind": "liveness", "what": <message>}``.  The stress task
reads the file and reports the counts per test.
"""
import json
import os
import threading
import time

ENV = "PROTO_STRESS"
LOG_ENV = "PROTO_STRESS_LOG"

#: True in stress mode: liveness checks are recorded, never asserted.
ACTIVE = os.environ.get(ENV) == "1"

#: How many times its quiet budget a wait gets in stress mode.
FACTOR = 6

_lock = threading.Lock()


def current_test() -> str:
    """The pytest node id of the running test (``PYTEST_CURRENT_TEST`` without its phase)."""
    return os.environ.get("PYTEST_CURRENT_TEST", "?").rsplit(" (", 1)[0]


def record(what: str) -> None:
    """Count one liveness event of the running test (stress mode only)."""
    path = os.environ.get(LOG_ENV)
    if not path:
        return
    line = json.dumps({"test": current_test(), "kind": "liveness", "what": str(what)[:300]})
    with _lock, open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def floor(ok: bool, message: str) -> bool:
    """A liveness floor: asserted in quiet mode, recorded in stress mode; returns ``ok``."""
    if ok:
        return True
    if not ACTIVE:
        raise AssertionError(message)
    record(message)
    return False


def budget(seconds: float) -> float:
    """The time a wait may take: ``seconds`` in quiet mode, ``FACTOR`` times that under stress."""
    return seconds * FACTOR if ACTIVE else seconds


def attempts(n: int) -> int:
    """Attempts of a retried setup step: ``n`` in quiet mode, ``FACTOR`` times that under stress."""
    return n * FACTOR if ACTIVE else n


def wait(predicate, seconds: float, message, poll: float = 0.005) -> None:
    """Poll ``predicate`` until it holds.  Quiet mode: ``AssertionError(message)`` once
    ``seconds`` have passed.  Stress mode: missing ``seconds`` is recorded, and the wait goes on
    up to :func:`budget` (``seconds``) before it fails: the test needs the state to go on.
    ``message`` may be a callable, evaluated when it is needed."""
    start = time.monotonic()
    late = False
    while not predicate():
        elapsed = time.monotonic() - start
        if elapsed > seconds and not late:
            text = message() if callable(message) else message
            floor(False, f"{text} (within {seconds} s)")
            late = True
        if elapsed > budget(seconds):
            raise AssertionError(message() if callable(message) else message)
        time.sleep(poll)
