"""Catches a benchmark harness that reports numbers it did not verify (spec section 12, Task 9.1).

Runs ``python -m benchmarks --quick`` (tiny parameter sets, one thread count) in a child
interpreter with the same ``-X`` options and asserts that it exits 0 and that every
workload's record is verified (``status == "ok"``, every check passed, checksum equal to
the expected value).  The only accepted ``pending`` workload is the rqlite reference when
no rqlited binary is configured.  This checks the harness's correctness, never speed.
The other tests plant failures in the harness's own paths: a wrong first (or warm-up)
repetition, a verification failure (keeps the work dict), a crashing isolated workload (the
run continues, JSON is still written, exit 1), a workload that changes the GIL state, the
tmpfs warning, ``--compare`` regressions with its noise guard and its environment check.
"""
import json
import os
import subprocess
import sys

import pytest

from benchmarks import harness, workloads  # noqa: F401  (registers every workload)

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

pytestmark = pytest.mark.timeout(240)


def _python():
    args = [sys.executable]
    for key, value in sys._xoptions.items():
        args += ["-X", key if value is True else f"{key}={value}"]
    return args


def test_quick_run_verifies_every_workload(tmp_path):
    out = tmp_path / "quick.json"
    env = dict(os.environ)
    env.pop("PROTODB_BENCH_RQLITED", None)
    proc = subprocess.run(_python() + ["-m", "benchmarks", "--quick", "--threads", "1,2", "--repeat", "1",
                                       "--warmup", "0", "--workdir", str(tmp_path / "work"), "--json", str(out)],
                          cwd=REPO, capture_output=True, text=True, timeout=220, env=env)
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    doc = json.loads(out.read_text())
    assert set(doc["results"]) == set(harness.REGISTRY)
    assert doc["environment"]["gil_enabled"] == (sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True)
    for name, record in doc["results"].items():
        if name == "rqlite_reference":
            assert record["status"] == "pending", record
            continue
        assert record["status"] == "ok", (name, record.get("error"))
        assert record["checksum"] == record["expected"], name
        assert record["checks"] and all(c["ok"] for c in record["checks"]), name
        assert record["timings"], name
        assert record["work"], name


@pytest.fixture
def planted():
    """Register temporary workloads; removed afterwards."""
    names = []

    def register(name, fn, **kw):
        harness.benchmark(name, quick={}, full={}, **kw)(fn)
        names.append(name)
        return name
    yield register
    for name in names:
        harness.REGISTRY.pop(name, None)


def test_a_failed_verification_fails_the_run_and_keeps_the_work(tmp_path, planted, capsys):
    def broken(ctx, p):
        """A workload whose result does not match its model."""
        result = harness.BenchResult({"rows": 1}, 1, 2)
        result.add_timing("noop", [0.1])
        return result

    out = tmp_path / "r.json"
    status = harness.main(["--quick", "--only", planted("__broken__", broken), "--workdir", str(tmp_path),
                           "--json", str(out)])
    assert status == 1
    assert "verification failed" in capsys.readouterr().out
    record = json.loads(out.read_text())["results"]["__broken__"]
    assert record["status"] == "failed" and record["work"] == {"rows": 1}


def test_measure_verifies_every_repetition():
    """Catches a measure() that only looks at the last result."""
    calls = iter([1, 2, 2, 2])                       # the warm-up is wrong
    with pytest.raises(harness.VerificationError, match="warm-up 1"):
        harness.measure(lambda: next(calls), 3, 1, expect=2)
    calls = iter([2, 1, 2, 2])                       # the first timed sample is wrong
    with pytest.raises(harness.VerificationError, match="sample 1"):
        harness.measure(lambda: next(calls), 3, 1, expect=2)
    calls = iter([2, 2, 2, 5])                       # a check callback naming the problem
    with pytest.raises(harness.VerificationError, match="sample 3: too big"):
        harness.measure(lambda: next(calls), 3, 1, check=lambda r: r < 3 or "too big")
    samples, last = harness.measure(lambda: 2, 3, 1, expect=2)
    assert len(samples) == 3 and last == 2
    with pytest.raises(TypeError):
        harness.measure(lambda: 2, 1, 0)             # unverified measurement is refused


def test_a_planted_first_sample_failure_fails_the_run(tmp_path, planted):
    def skips_work_once(ctx, p):
        """Does half the work in its first run only (the failure the review planted)."""
        runs = []

        def run():
            runs.append(1)
            return 5 if len(runs) == 1 else 10
        result = harness.BenchResult({"rows": 10}, 10, 10)
        samples, _ = harness.measure(run, 3, 0, expect=10)
        result.add_timing("t", samples)
        return result

    assert harness.main(["--quick", "--only", planted("__first__", skips_work_once), "--workdir", str(tmp_path)]) == 1


PLUGIN = """
from benchmarks.harness import BenchResult, benchmark, measure


@benchmark("__iso_crash__", quick={}, full={}, isolate=True)
def iso_crash(ctx, p):
    \"\"\"An isolated workload that crashes.\"\"\"
    raise RuntimeError("planted crash in the child")


@benchmark("__after__", quick={}, full={})
def after(ctx, p):
    \"\"\"A workload that must still run after the crash.\"\"\"
    result = BenchResult({"rows": 1}, 1, 1)
    samples, _ = measure(lambda: 1, ctx.repeat, ctx.warmup, expect=1)
    result.add_timing("t", samples)
    return result
"""


def test_an_isolated_crash_is_recorded_and_the_run_continues(tmp_path):
    (tmp_path / "bench_plugin_for_test.py").write_text(PLUGIN)
    out = tmp_path / "r.json"
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), REPO]))
    proc = subprocess.run(_python() + ["-m", "benchmarks", "--quick", "--plugin", "bench_plugin_for_test",
                                       "--only", "__iso_crash__", "__after__", "--repeat", "1", "--warmup", "0",
                                       "--workdir", str(tmp_path / "work"), "--json", str(out)],
                          cwd=REPO, capture_output=True, text=True, timeout=120, env=env)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    results = json.loads(out.read_text())["results"]
    assert results["__iso_crash__"]["status"] == "failed"
    assert "planted crash in the child" in results["__iso_crash__"]["error"]
    assert results["__after__"]["status"] == "ok"


def test_a_workload_changing_the_gil_state_fails(tmp_path, planted, monkeypatch):
    def noop(ctx, p):
        """Verified, but the GIL state changes while it runs."""
        result = harness.BenchResult({"rows": 1}, 1, 1)
        result.add_timing("t", [0.1])
        return result

    states = iter([False, True])
    monkeypatch.setattr(harness, "gil_enabled", lambda: next(states))
    ctx = harness.Context(quick=True, threads=[1], repeat=1, warmup=0, workdir=str(tmp_path))
    record = harness.run_one(harness.REGISTRY[planted("__gil__", noop)], ctx, True)
    assert record["status"] == "failed" and "GIL state changed" in record["error"]


def test_tmpfs_work_directory_warns(tmp_path, planted, monkeypatch, capsys):
    def noop(ctx, p):
        """Trivial verified workload."""
        result = harness.BenchResult({"rows": 1}, 1, 1)
        result.add_timing("t", [0.1])
        return result

    monkeypatch.setattr(harness, "filesystem_of", lambda path: "tmpfs")
    assert harness.main(["--only", planted("__tmpfs__", noop), "--workdir", str(tmp_path)]) == 0
    assert "WARNING: the work directory is in memory" in capsys.readouterr().out


def test_compare_flags_a_regression_outside_the_base_spread():
    def doc(median, better="lower", samples=None):
        return {"results": {"w": {"status": "ok", "timings": {"t": {
            "unit": "s", "better": better, "median": median, "samples": samples or [median]}}}}}
    assert harness.compare(doc(1.3), doc(1.0), 0.15)
    assert not harness.compare(doc(1.1), doc(1.0), 0.15)
    assert not harness.compare(doc(1.3), doc(1.0, samples=[0.8, 1.0, 1.4]), 0.15)   # inside the base spread
    assert harness.compare(doc(70.0, "higher"), doc(100.0, "higher"), 0.15)
    assert not harness.compare(doc(120.0, "higher"), doc(100.0, "higher"), 0.15)


def test_compare_refuses_a_different_environment():
    base = {"mode": "full", "environment": {"gil_enabled": True, "cpu_model": "x"}}
    assert harness.environment_mismatch({"mode": "full", "environment": {"gil_enabled": False, "cpu_model": "x"}},
                                        base) == ["gil_enabled True -> False"]
    assert "mode full -> quick" in harness.environment_mismatch(
        {"mode": "quick", "environment": base["environment"]}, base)
