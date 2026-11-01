"""Configuration of the free-threaded stress suite (Task 5.2).

Marker ``freethreaded``: documents that a test exists to exercise real parallelism
(CPython built with ``--disable-gil`` and running with the GIL off, e.g. 3.14t).
It never skips: with the GIL enabled the same tests still run and must still pass,
they only interleave less.  A summary line reports the interpreter build and
whether the GIL was enabled, so a stress run's result can be read against it.
"""
import sys
import sysconfig


def gil_report() -> str:
    """One line: Python version, build kind (free-threaded or not) and the GIL state."""
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    gil = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    return (f"concurrency suite: Python {sys.version.split()[0]} "
            f"{'free-threaded' if free_threaded else 'GIL'} build, "
            f"sys._is_gil_enabled()={gil}")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "freethreaded: stress test meant for a free-threaded interpreter with the GIL off "
        "(documents intent only: it also runs, and must pass, with the GIL enabled)")


def pytest_report_header(config):
    return gil_report()


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.write_line(gil_report())
