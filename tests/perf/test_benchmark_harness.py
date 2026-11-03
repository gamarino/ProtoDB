"""Catches a benchmark harness that reports numbers it did not verify (spec section 12, Task 9.1).

Runs ``python -m benchmarks --quick`` (tiny parameter sets, one thread count) in a child
interpreter with the same ``-X`` options and asserts that it exits 0 and that every
workload's record is verified (``status == "ok"``, every check passed, checksum equal to
the expected value).  The only accepted ``pending`` workload is the rqlite reference when
no rqlited binary is configured.  This checks the harness's correctness, never speed.
A second test checks that a verification failure fails the run with a non-zero exit status,
and a third that ``--compare`` flags a regression.
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


def test_a_failed_verification_fails_the_run(tmp_path, monkeypatch, capsys):
    name = "__broken_for_test__"

    @harness.benchmark(name, quick={}, full={})
    def broken(ctx, p):
        """A workload whose result does not match its model."""
        result = harness.BenchResult({"rows": 1}, 1, 2)
        result.add_timing("noop", [0.1])
        return result

    try:
        status = harness.main(["--quick", "--only", name, "--workdir", str(tmp_path)])
    finally:
        del harness.REGISTRY[name]
    assert status == 1
    assert "verification failed" in capsys.readouterr().out


def test_compare_flags_a_regression():
    def doc(median, better="lower"):
        return {"results": {"w": {"status": "ok", "timings": {"t": {"unit": "s", "better": better,
                                                                   "median": median}}}}}
    assert harness.compare(doc(1.3), doc(1.0), 0.15)
    assert not harness.compare(doc(1.1), doc(1.0), 0.15)
    assert harness.compare(doc(70.0, "higher"), doc(100.0, "higher"), 0.15)
    assert not harness.compare(doc(120.0, "higher"), doc(100.0, "higher"), 0.15)
