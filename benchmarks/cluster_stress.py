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
    for case in ET.parse(junit).getroot().iter("testcase"):
        tests += 1
        bad = case.find("failure")
        if bad is None:
            bad = case.find("error")
        if case.find("skipped") is not None:
            skipped += 1
        elif bad is None:
            passed += 1
        else:
            text = (bad.get("message") or "") + "\n" + (bad.text or "")
            failures.append({"test": f"{case.get('classname')}::{case.get('name')}",
                             "class": classify(text), "message": (bad.get("message") or "")[:300]})
    return {"tests": tests, "passed": passed, "skipped": skipped, "failures": failures}


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
            hogs = _hogs(args.hogs)
            load0 = os.getloadavg()[0]
            t0 = time.monotonic()
            try:
                with open(log, "w") as out:
                    proc = subprocess.run(
                        [sys.executable, "-X", f"gil={gil}", "-m", "pytest", "-q", "-p", "no:cacheprovider",
                         f"--basetemp={os.path.join(args.workdir, f'bt{i}-{gil}')}", f"--junitxml={junit}",
                         *args.tests], stdout=out, stderr=subprocess.STDOUT, timeout=args.timeout)
                load1 = os.getloadavg()[0]
            finally:
                _stop(hogs)
            result = _parse(junit)
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
