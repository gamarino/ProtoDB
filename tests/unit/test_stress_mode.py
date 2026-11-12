"""The stress mode of the cluster tests (ruling R142, tests/support/stress.py).

Quiet mode (``PROTO_STRESS`` unset) must be exactly as strict as the asserts the helpers
replaced: a missed liveness floor or wait raises, with the quiet budget.  Stress mode records
the miss, with the running test's id, and goes on.  Each case runs in a child interpreter,
because the mode is read once, at import.
"""
import json
import os
import subprocess
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_CHECK = r"""
import json, sys, time
from tests.support import stress
out = {"active": stress.ACTIVE, "budget": stress.budget(5), "attempts": stress.attempts(10)}
try:
    out["floor_ok"] = stress.floor(True, "never")
    out["floor_missed"] = stress.floor(False, "a planted liveness floor")
except AssertionError as e:
    out["floor_raised"] = str(e)
started = time.monotonic()
calls = []
def ready():
    calls.append(1)
    return time.monotonic() - started > 0.3
try:
    stress.wait(ready, 0.1, "a planted wait")
    out["wait"] = "held"
except AssertionError as e:
    out["wait_raised"] = str(e)
print(json.dumps(out))
"""


def _run(tmp_path, stress_on: bool):
    env = {k: v for k, v in os.environ.items() if k not in ("PROTO_STRESS", "PROTO_STRESS_LOG")}
    env["PYTHONPATH"] = _REPO
    env["PYTEST_CURRENT_TEST"] = "tests/unit/test_planted.py::test_planted (call)"
    log = tmp_path / "events.jsonl"
    if stress_on:
        env.update(PROTO_STRESS="1", PROTO_STRESS_LOG=str(log))
    proc = subprocess.run([sys.executable, "-c", _CHECK], cwd=_REPO, env=env, capture_output=True, text=True,
                          timeout=60)
    assert proc.returncode == 0, proc.stderr
    events = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    return json.loads(proc.stdout), events


def test_quiet_mode_asserts_every_liveness_floor_with_the_quiet_budget(tmp_path):
    out, events = _run(tmp_path, stress_on=False)
    assert out["active"] is False and out["budget"] == 5 and out["attempts"] == 10
    assert out["floor_ok"] is True
    assert out["floor_raised"] == "a planted liveness floor" and "floor_missed" not in out
    assert out["wait_raised"].startswith("a planted wait")
    assert events == []


def test_stress_mode_records_the_misses_and_goes_on(tmp_path):
    out, events = _run(tmp_path, stress_on=True)
    assert out["active"] is True and out["budget"] == 30 and out["attempts"] == 60
    assert out["floor_missed"] is False and "floor_raised" not in out
    assert out["wait"] == "held"                       # within the stress budget
    assert [e["test"] for e in events] == ["tests/unit/test_planted.py::test_planted"] * 2
    assert events[0]["what"] == "a planted liveness floor"
    assert events[1]["what"].startswith("a planted wait")
