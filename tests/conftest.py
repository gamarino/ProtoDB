"""Shared pytest configuration for the ProtoDB test suite."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# -- GIL leak detection (Task 7.1, D39) -------------------------------------------------------
# Importing hnswlib re-enables the GIL for the whole process on CPython 3.14t, which would silently turn
# every later free-threading test into a GIL test.  hnsw searches therefore run in child processes
# (tests/unit/hnsw_checks.py).  This check fails the session, whatever the test order, if hnswlib was
# imported into the pytest process or the GIL state changed during the run.

_GIL_AT_START = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else None


def pytest_sessionfinish(session, exitstatus):
    """Fail the run if hnswlib leaked into this process or the GIL state changed."""
    import pytest
    gil_now = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else None
    problems = []
    if "hnswlib" in sys.modules:
        problems.append("hnswlib was imported into the pytest process (it re-enables the GIL)")
    if gil_now != _GIL_AT_START:
        problems.append(f"the GIL state changed during the run: {_GIL_AT_START} -> {gil_now}")
    if problems:
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        for problem in problems:
            if reporter is not None:
                reporter.write_line(f"GIL LEAK: {problem}", red=True)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
