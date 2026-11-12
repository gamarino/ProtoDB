"""The periodic cluster stress task (ruling R125): cluster tests under synthetic CPU load.

Usage, from the root of a clean export (``python -m benchmarks.export COMMIT DEST``)::

    python -m benchmarks.cluster_stress --hogs 12 --runs 3 --json OUT.json [TEST ...]

Each run starts ``--hogs`` CPU-bound busy processes, runs the selected tests once per GIL mode
with ``--junitxml``, stops the hogs, and classifies every failed or errored test:

- **liveness**: the failure is a :class:`ProtoOutcomeUnknownError` or a
  :class:`ProtoTimeoutError`, the conservative outcomes the protocol gives when a stall
  outlasts a lease (principle P5).  They are expected under this load: counted, never failed.
- **safety**: the failure text carries one of the safety messages of the tests' resolution
  helpers (an acknowledged commit lost, unknown outcomes resolved differently on two members,
  history lost after a member showed it, a phantom or doubled commit).  Any such failure fails
  the task.
- **unclassified**: anything else.  The task fails, and the failure is classified by hand,
  with its evidence (ruling R117): a product defect, a test defect, or load-induced liveness.

The tests run in stress mode (``PROTO_STRESS=1``, ruling R142, ``tests/support/stress.py``):
their liveness floors -- progress counts, ``commits > 0``, timing and silence bounds, the
confirmation and recovery timing checks -- are recorded in a per-run log instead of asserted,
waits get a larger budget, and a scenario step whose outcome is unknown is retried until it takes
effect once, so each test goes on to its safety asserts, which stay unconditional.  The evidence
is, per test, how many executions reached their safety asserts: those that passed (every
assert evaluated and held) and those that failed one of them.  An execution ended by a
liveness exception before them did not reach them, and is reported as such.

The task reports the work it did (tests run and passed per run and mode, the load average at
each run's start and end) and checks it: a run that executed no test fails the task.
"""
import argparse
import json
import os
import platform
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

#: The default selection: the transactions file R125 measured and the cluster safety tests.
DEFAULT_TESTS = (
    "tests/unit/test_cluster_transactions.py",
    "tests/concurrency/test_cluster_safety.py",
    "tests/concurrency/test_cluster_crash.py",
)

#: Exception names whose failures are liveness outcomes (P5), counted and not failed.
LIVENESS = ("ProtoOutcomeUnknownError", "ProtoTimeoutError")

#: Messages of the safety checks (tests/support/outcomes.py, the safety and crash tests).
SAFETY = (
    "acknowledged commits lost",
    "unknown outcomes not resolved alike",
    "commits a member showed and history later lost",
    "tokens no acknowledged or unknown commit wrote",
    "commits applied more than once",
    "members did not converge",
    "overlapping leases",
    "ids no commit wrote",
)


def _hogs(n: int) -> list:
    code = "while True: pass"
    return [subprocess.Popen([sys.executable, "-X", "gil=1", "-c", code]) for _ in range(n)]


def _stop(procs: list) -> None:
    for p in procs:
        p.kill()
    for p in procs:
        p.wait()


def classify(text: str) -> str:
    """``liveness``, ``safety`` or ``unclassified`` for one failure's text."""
    # Only the error lines count: the traceback also prints the source of the failing function,
    # whose other asserts quote the safety messages.
    errors = [ln for ln in text.splitlines() if ln.startswith("E ") or ln.startswith("E\t")]
    first = text.splitlines()[0] if text else ""
    if any(m in ln for m in SAFETY for ln in errors + [first]):
        return "safety"
    tail = "\n".join(errors[-3:] + [first])
    if any(name in tail for name in LIVENESS):
        return "liveness"
    return "unclassified"


def _parse(junit: str) -> dict:
    tests = passed = skipped = 0
    failures = []
    outcomes = {}                 # test id -> "passed", "skipped" or the failure's class
    for case in ET.parse(junit).getroot().iter("testcase"):
        tests += 1
        bad = case.find("failure")
        if bad is None:
            bad = case.find("error")
        test = f"{case.get('classname')}::{case.get('name')}"
        if case.find("skipped") is not None:
            skipped += 1
            outcomes[test] = "skipped"
        elif bad is None:
            passed += 1
            outcomes[test] = "passed"
        else:
            text = (bad.get("message") or "") + "\n" + (bad.text or "")
            failures.append({"test": test, "class": classify(text), "message": (bad.get("message") or "")[:300]})
            outcomes[test] = failures[-1]["class"]
    return {"tests": tests, "passed": passed, "skipped": skipped, "failures": failures, "outcomes": outcomes}


def _test_key(nodeid: str) -> str:
    """``tests/x/test_y.py::test_z[p]`` and junit's ``tests.x.test_y::test_z[p]`` as one key."""
    path, _, name = nodeid.partition("::")
    return path.removesuffix(".py").replace("/", ".") + "::" + name


def _liveness_events(path: str) -> dict:
    """Test key -> the liveness events the tests recorded in stress mode (tests/support/stress.py)."""
    events = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                events.setdefault(_test_key(event["test"]), []).append(event["what"])
    return events


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("tests", nargs="*", default=list(DEFAULT_TESTS))
    ap.add_argument("--hogs", type=int, default=12)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--json", required=True)
    ap.add_argument("--workdir", default=os.path.join(os.getcwd(), "benchmarks", ".work", "stress"))
    ap.add_argument("--timeout", type=int, default=3600, help="seconds per pytest run")
    args = ap.parse_args(argv)
    os.makedirs(args.workdir, exist_ok=True)
    source = None
    if os.path.exists("BENCH_SOURCE.json"):
        with open("BENCH_SOURCE.json", encoding="utf-8") as f:
            source = json.load(f)["commit"]
    runs = []
    for i in range(1, args.runs + 1):
        for gil in (0, 1):
            junit = os.path.join(args.workdir, f"run{i}-gil{gil}.xml")
            log = os.path.join(args.workdir, f"run{i}-gil{gil}.log")
            events_log = os.path.join(args.workdir, f"run{i}-gil{gil}.liveness.jsonl")
            if os.path.exists(events_log):
                os.remove(events_log)
            env = dict(os.environ, PROTO_STRESS="1", PROTO_STRESS_LOG=events_log)
            hogs = _hogs(args.hogs)
            load0 = os.getloadavg()[0]
            t0 = time.monotonic()
            try:
                with open(log, "w") as out:
                    proc = subprocess.run(
                        [sys.executable, "-X", f"gil={gil}", "-m", "pytest", "-q", "-p", "no:cacheprovider",
                         f"--basetemp={os.path.join(args.workdir, f'bt{i}-{gil}')}", f"--junitxml={junit}",
                         *args.tests], stdout=out, stderr=subprocess.STDOUT, timeout=args.timeout, env=env)
                load1 = os.getloadavg()[0]
            finally:
                _stop(hogs)
            result = _parse(junit)
            events = _liveness_events(events_log)
            result["liveness_events"] = {t: len(v) for t, v in sorted(events.items())}
            result["liveness_events_log"] = events_log
            result.update({"run": i, "gil": gil, "rc": proc.returncode, "seconds": round(time.monotonic() - t0, 1),
                           "load_start": round(load0, 2), "load_end": round(load1, 2), "log": log})
            runs.append(result)
            counts = {c: sum(f["class"] == c for f in result["failures"]) for c in ("liveness", "safety",
                                                                                   "unclassified")}
            print(f"run {i} gil{gil}: {result['passed']}/{result['tests']} passed, {counts}, "
                  f"load {load0:.1f}/{load1:.1f}, {result['seconds']} s", flush=True)
    problems = []
    for r in runs:
        if r["tests"] == 0:
            problems.append(f"run {r['run']} gil{r['gil']} executed no test")
        for f in r["failures"]:
            if f["class"] != "liveness":
                problems.append(f"run {r['run']} gil{r['gil']}: {f['class']} failure in {f['test']}")
    # The evidence (R142): per test, the executions that reached their safety asserts -- passed
    # (every assert evaluated and held) or failed one of them -- against those a liveness
    # exception ended first.
    per_test = {}
    for r in runs:
        for test, outcome in r["outcomes"].items():
            t = per_test.setdefault(test, {"executions": 0, "reached_safety": 0, "passed": 0,
                                           "ended_by_liveness": 0, "safety_failures": 0, "unclassified": 0,
                                           "skipped": 0, "liveness_events": 0})
            t["executions"] += 1
            t["liveness_events"] += r["liveness_events"].get(_test_key(test), 0)
            if outcome == "passed":
                t["passed"] += 1
                t["reached_safety"] += 1
            elif outcome == "safety":
                t["safety_failures"] += 1
                t["reached_safety"] += 1
            elif outcome == "liveness":
                t["ended_by_liveness"] += 1
            elif outcome == "skipped":
                t["skipped"] += 1
            else:
                t["unclassified"] += 1
    for test, t in sorted(per_test.items()):
        print(f"{test}: reached its safety asserts in {t['reached_safety']} of {t['executions']} executions "
              f"({t['ended_by_liveness']} ended by a liveness exception, {t['liveness_events']} liveness "
              f"events recorded)", flush=True)
    liveness = {}
    for r in runs:
        for f in r["failures"]:
            if f["class"] == "liveness":
                kind = next((n for n in LIVENESS if n in f["message"]), "liveness")
                liveness[kind] = liveness.get(kind, 0) + 1
    report = {
        "schema": "proto_db.cluster_stress/1",
        "commit": source,
        "python": platform.python_version(),
        "hogs": args.hogs, "cpus": os.cpu_count(), "tests": args.tests,
        "runs": runs,
        "test_executions": sum(r["tests"] for r in runs),
        "stress_mode": "PROTO_STRESS=1 (ruling R142)",
        "per_test": per_test,
        "reached_safety": sum(t["reached_safety"] for t in per_test.values()),
        "liveness_events": sum(t["liveness_events"] for t in per_test.values()),
        "liveness_failures": sum(liveness.values()), "liveness_by_kind": liveness,
        "problems": problems,
        "verdict": "safety held" if not problems else "needs classification",
    }
    with open(args.json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=1)
    print(report["verdict"], "; liveness failures:", liveness, "; problems:", problems)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
