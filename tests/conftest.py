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


# -- Silent protocol errors (Task 8.3n fix round 1, review M7) -------------------------------------
# A message that reaches a handler was framed and decoded; a ClusterProtocolError raised while
# handling it means a member of this code base built a malformed message (8.3n found one: a
# ``done`` carrying an unencoded root, logged by ``_deliver`` and otherwise silent).  That is a
# programming error, never a network fault, so any test that sees one fails.  A test that feeds
# crafted messages on purpose marks itself ``protocol_errors_expected``.

def pytest_configure(config):
    config.addinivalue_line("markers", "protocol_errors_expected: the test delivers malformed messages on purpose")


import pytest as _pytest


@_pytest.fixture(autouse=True)
def _fail_on_delivered_protocol_errors(request, monkeypatch):
    try:
        from proto_db.cluster import transport
        from proto_db.cluster.protocol import ClusterProtocolError
    except ImportError:          # pragma: no cover - the cluster package is part of the tree
        yield
        return
    from proto_db.errors import ProtoDBError
    seen = []

    def deliver(handler, msg, where):
        """``transport._deliver`` (log a ProtoDBError, go on), recording protocol errors."""
        try:
            handler(msg)
        except ProtoDBError as e:
            if isinstance(e, ClusterProtocolError):
                seen.append(f"{where}: a {msg.get('type')!r} message: {e}")
            transport.logger.error("%s: handling a %r message failed: %s", where, msg.get("type"), e)
    monkeypatch.setattr(transport, "_deliver", deliver)
    request.node.delivered_protocol_errors = seen
    yield
    if seen and request.node.get_closest_marker("protocol_errors_expected") is None:
        _pytest.fail("a member delivered a malformed message (a programming error): " + "; ".join(seen[:3]),
                     pytrace=False)
