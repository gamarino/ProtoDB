"""Benchmark harness of the ProtoDB performance suite (spec section 12, Task 9.1, decision D49).

A workload is a function registered with :func:`benchmark`::

    @benchmark("point_lookup", quick={"n": 300}, full={"n": 20_000})
    def point_lookup(ctx: Context, p: dict) -> BenchResult:
        ...

It receives a :class:`Context` (options, thread counts, repetitions, a private work
directory) and the parameter set of the selected mode, does its work, and returns a
:class:`BenchResult` holding

* ``work``: what was done (row counts, commits, queries...), printed before any timing;
* ``checksum`` and ``expected``: a digest of the results and the value an independent
  computation (a brute-force scan, a Python model, the baseline itself) says it must be;
* ``checks``: further named verifications (``result.check(name, ok, detail)``);
* ``timings``: repeated samples per variant, summarised as median, p95, min and max.

The runner calls :meth:`BenchResult.verify` before it reports a single timing: a checksum
mismatch or a failed check fails the run (exit status 1).  Baselines (dict/list, sqlite3,
rqlite) do the same work as ProtoDB and are verified the same way.  A workload whose
external reference is missing returns :meth:`BenchResult.pending` and is reported as
pending, never as a number.

:func:`measure` verifies **every** repetition, warm-ups included (``expect=`` or
``check=`` is mandatory): a run that did the wrong work in any sample fails, not only when
the last one is wrong.  A workload that raises, fails verification or crashes in its
isolated child is recorded as failed (with the traceback and, where available, its work
dict); the run continues with the remaining workloads, still writes the JSON and exits 1.

Methodology: every timed variant runs ``warmup`` untimed repetitions, then ``repeat``
timed ones; samples are reported as the median and a tail: p95 (nearest rank) with at least
:data:`P95_MIN_SAMPLES` samples, otherwise the maximum (minimum for rates), labelled as such;
a single sample is flagged ``SINGLE SAMPLE``.  The environment (CPU model, cores, frequency
governor and boost, Python version and build, ``sys._is_gil_enabled()``, the file system of
the work directory) is captured with the results, and ``sys._is_gil_enabled()`` is checked
again after every workload, so a module that silently re-enables the GIL (hnswlib does)
cannot contaminate a free-threaded measurement: such workloads are flagged ``isolate``
and run in a child process with the same interpreter options.

``--compare`` refuses a baseline taken in a different environment (GIL state, quick/full,
CPU, Python build) unless ``--allow-env-mismatch``, and flags a regression only when the
median moves past the threshold *and* outside the baseline's own sample range.
"""
import argparse
import datetime as _dt
import json
import math
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_THREADS = (1, 2, 4, 8, 16)
DEFAULT_THRESHOLD = 0.15
SCHEMA = 1


class VerificationError(Exception):
    """A workload's results do not match the independent computation they are checked against."""


# -- registry ----------------------------------------------------------------------------------

@dataclass
class Benchmark:
    """A registered workload: its function, parameter sets and isolation flag."""

    name: str
    fn: Callable
    quick: dict
    full: dict
    isolate: bool = False
    description: str = ""


REGISTRY: Dict[str, Benchmark] = {}


def benchmark(name: str, *, quick: dict, full: dict, isolate: bool = False, description: str = ""):
    """Register the decorated function as workload ``name`` with its quick and full parameters.

    ``isolate=True`` runs the workload in a child interpreter (same ``-X`` options): use it
    for workloads that import a module changing process-wide state, such as hnswlib, which
    re-enables the GIL on a free-threaded build.
    """
    def register(fn):
        if name in REGISTRY:
            raise ValueError(f"benchmark {name!r} registered twice")
        REGISTRY[name] = Benchmark(name, fn, dict(quick), dict(full), isolate,
                                   description or (fn.__doc__ or "").strip().splitlines()[0])
        return fn
    return register


# -- results -----------------------------------------------------------------------------------

def percentile(samples: List[float], q: float) -> float:
    """The nearest-rank ``q`` percentile (0 < q <= 100) of ``samples``."""
    ordered = sorted(samples)
    rank = max(1, math.ceil(q / 100.0 * len(ordered)))
    return ordered[rank - 1]


#: Fewer samples than this make a nearest-rank p95 equal to the maximum (or, for the bad side of
#: a rate, the minimum): below it the report names the extreme instead of calling it a p95.
P95_MIN_SAMPLES = 20


def summarize(samples: List[float]) -> dict:
    """Median, min, max, mean and count of ``samples``; ``p95`` only with at least
    :data:`P95_MIN_SAMPLES` samples (``None`` otherwise)."""
    ordered = sorted(samples)
    n = len(ordered)
    median = ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2
    return {"n": n, "median": median, "p95": percentile(ordered, 95) if n >= P95_MIN_SAMPLES else None,
            "min": ordered[0], "max": ordered[-1], "mean": sum(ordered) / n}


def tail(t: dict) -> tuple:
    """``(label, value)`` of a timing's bad-side tail: p95 (or p5 for rates) with enough samples,
    else the worst sample (``max`` for times, ``min`` for rates)."""
    if t["better"] == "higher":
        return ("p5", percentile(t["samples"], 5)) if t["n"] >= P95_MIN_SAMPLES else ("min", t["min"])
    return ("p95", t["p95"]) if t["n"] >= P95_MIN_SAMPLES else ("max", t["max"])


def latency_ms(latencies) -> dict:
    """``{"p50": ms, "p95": ms}`` of ``latencies`` (seconds); with fewer than
    :data:`P95_MIN_SAMPLES` latencies the tail is reported as ``"max"`` instead of ``"p95"``."""
    if not latencies:
        return {"p50": 0.0, "max": 0.0}
    s = summarize(latencies)
    key = "p95" if s["p95"] is not None else "max"
    return {"p50": round(s["median"] * 1000, 2), key: round((s["p95"] if s["p95"] is not None else s["max"]) * 1000, 2)}


_CURRENT = threading.local()


class BenchResult:
    """What a workload did, how it was verified, and how long it took (module docstring)."""

    def __init__(self, work: Optional[dict] = None, checksum=None, expected=None):
        _CURRENT.result = self          # lets the runner keep the work of a workload that fails later
        self.work = dict(work or {})
        self.checksum = checksum
        self.expected = expected
        self.timings: Dict[str, dict] = {}
        self.metrics: Dict[str, object] = {}
        self.checks: List[tuple] = []
        self.notes: List[str] = []
        self.status = "ok"

    @classmethod
    def pending(cls, reason: str) -> "BenchResult":
        """A result for a workload whose external reference is not available."""
        result = cls({"pending": reason})
        result.status = "pending"
        result.notes.append(reason)
        return result

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        """Record a named verification; a False one fails the run in :meth:`verify`."""
        self.checks.append((name, bool(ok), str(detail)))
        return bool(ok)

    def add_timing(self, label: str, samples: List[float], *, unit: str = "s", per: int = 1,
                   better: str = "lower") -> dict:
        """Record ``samples`` for variant ``label``.

        ``unit`` names what a sample measures (``s``: seconds for the whole batch,
        ``ops/s``: a rate); ``per`` is the number of operations in one sample (a per-op time
        is derived from it for ``s`` samples); ``better`` is ``lower`` or ``higher``.
        """
        if not samples:
            raise ValueError(f"timing {label!r} has no samples")
        entry = {"unit": unit, "better": better, "per": per, "samples": list(samples)}
        entry.update(summarize(samples))
        entry["tail_label"], entry["tail"] = tail(entry)
        if unit == "s" and per:
            entry["per_op_us"] = entry["median"] / per * 1e6
            entry["per_op_us_tail"] = entry["tail"] / per * 1e6
        self.timings[label] = entry
        return entry

    def add_rate(self, label: str, rates: List[float], unit: str = "ops/s") -> dict:
        """Record throughput samples (higher is better)."""
        return self.add_timing(label, rates, unit=unit, per=0, better="higher")

    def verify(self) -> None:
        """Raise :class:`VerificationError` if the checksum or any check failed."""
        if self.status == "pending":
            return
        failures = []
        if self.checksum != self.expected:
            failures.append(f"checksum {self.checksum!r} != expected {self.expected!r}")
        failures.extend(f"check {name!r} failed: {detail}" for name, ok, detail in self.checks if not ok)
        if failures:
            raise VerificationError("; ".join(failures))

    def to_dict(self) -> dict:
        """JSON-ready form."""
        return {"status": self.status, "work": self.work, "checksum": _jsonable(self.checksum),
                "expected": _jsonable(self.expected),
                "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in self.checks],
                "timings": self.timings, "metrics": _jsonable(self.metrics), "notes": self.notes}


def _jsonable(value):
    """Convert tuples/sets and other values to JSON-friendly forms."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


# -- context and measurement -------------------------------------------------------------------

@dataclass
class Context:
    """Options a workload receives: mode, thread counts, repetitions and a private directory."""

    quick: bool
    threads: List[int]
    repeat: int
    warmup: int
    workdir: str
    rqlite: Optional[str] = None
    scratch: List[str] = field(default_factory=list)
    #: ``{workload: {key: value}}`` from ``--param``: overrides of the selected parameter set.
    overrides: Dict[str, dict] = field(default_factory=dict)

    def params_of(self, bench: "Benchmark", quick: bool) -> dict:
        """The parameter set of ``bench`` for this mode, with the ``--param`` overrides applied."""
        params = dict(bench.quick if quick else bench.full)
        params.update(self.overrides.get(bench.name, {}))
        return params

    def tempdir(self, name: str) -> str:
        """A fresh, empty directory under the work directory, removed after the workload."""
        path = tempfile.mkdtemp(prefix=f"{name}-", dir=self.workdir)
        self.scratch.append(path)
        return path

    def cleanup(self) -> None:
        """Remove the directories handed out by :meth:`tempdir`."""
        for path in self.scratch:
            shutil.rmtree(path, ignore_errors=True)
        self.scratch.clear()


_UNSET = object()


def measure(fn: Callable[[], object], repeat: int, warmup: int = 1, setup: Optional[Callable] = None, *,
            expect=_UNSET, check: Optional[Callable[[object], object]] = None):
    """Run ``fn`` ``warmup`` times untimed then ``repeat`` times timed, verifying EVERY run.

    Every result (warm-ups included) must equal ``expect`` and/or pass ``check(result)``
    (which returns True/None when the result is right, or False / a detail string when it is
    not; it runs outside the timed region).  The first wrong run raises
    :class:`VerificationError` naming the run, so a silently failing repetition can never
    contribute a sample.  At least one of ``expect`` and ``check`` is required.

    Returns ``(samples_in_seconds, last_result)``.  ``setup``, when given, is called before
    every run outside the timed region and its result is passed to ``fn``.
    """
    if expect is _UNSET and check is None:
        raise TypeError("measure() needs expect= or check=: every repetition is verified")
    samples, result = [], None
    for i in range(warmup + repeat):
        arg = setup() if setup is not None else None
        t0 = time.perf_counter()
        result = fn(arg) if setup is not None else fn()
        elapsed = time.perf_counter() - t0
        run = f"warm-up {i + 1}" if i < warmup else f"sample {i - warmup + 1}"
        if expect is not _UNSET and result != expect:
            raise VerificationError(f"{run}: result {_short(result)} != expected {_short(expect)}")
        if check is not None:
            verdict = check(result)
            if verdict is False or isinstance(verdict, str):
                raise VerificationError(f"{run}: {verdict if isinstance(verdict, str) else 'check failed'}")
        if i >= warmup:
            samples.append(elapsed)
    return samples, result


def _short(value) -> str:
    text = repr(value)
    return text if len(text) <= 200 else text[:197] + "..."


# -- environment -------------------------------------------------------------------------------

def _cpu_model() -> str:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass                      # not Linux: fall back to platform
    return platform.processor() or platform.machine()


def filesystem_of(path: str) -> str:
    """The file system type of the mount holding ``path`` (Linux ``/proc/mounts``), else ``unknown``."""
    path = os.path.realpath(path)
    best, kind = "", "unknown"
    try:
        with open("/proc/mounts", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and (path == parts[1] or path.startswith(parts[1].rstrip("/") + "/")) \
                        and len(parts[1]) >= len(best):
                    best, kind = parts[1], parts[2]
    except OSError:
        pass                      # not Linux
    return kind


#: Written by :mod:`benchmarks.export` at the root of a clean ``git archive`` export: the
#: commit and a SHA-256 per exported file, so a run can prove it measured that commit unmodified.
SOURCE_FILE = "BENCH_SOURCE.json"
_SKIP_DIRS = {"__pycache__", ".work", ".pytest_cache", ".hypothesis"}


def tree_digests(root: str, paths) -> dict:
    """``{path: sha256}`` of the files ``paths`` (relative to ``root``)."""
    import hashlib
    out = {}
    for rel in paths:
        h = hashlib.sha256()
        with open(os.path.join(root, rel), "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        out[rel] = h.hexdigest()
    return out


def _python_files(root: str) -> set:
    """Every ``.py`` file under ``root``'s ``proto_db`` and ``benchmarks`` (caches and work dirs excluded)."""
    found = set()
    for top in ("proto_db", "benchmarks"):
        for dirpath, dirnames, filenames in os.walk(os.path.join(root, top)):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
            found.update(os.path.relpath(os.path.join(dirpath, f), root) for f in filenames if f.endswith(".py"))
    return found


def source_revision(root: str = REPO) -> Optional[dict]:
    """The provenance recorded by :mod:`benchmarks.export`, re-verified, or ``None`` without one.

    ``clean`` is True only when every exported file still has its exported digest and no
    Python file was added to ``proto_db`` or ``benchmarks`` after the export."""
    path = os.path.join(root, SOURCE_FILE)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        source = json.load(f)
    files = source.get("files", {})
    missing = [rel for rel in files if not os.path.exists(os.path.join(root, rel))]
    present = [rel for rel in files if rel not in missing]
    changed = [rel for rel, digest in tree_digests(root, present).items() if digest != files[rel]]
    added = sorted(_python_files(root) - set(files))
    clean = not changed and not missing and not added
    return {"commit": source["commit"][:7], "commit_full": source["commit"], "dirty": not clean, "clean": clean,
            "source": "git archive export", "changed": changed[:20], "missing": missing[:20], "added": added[:20]}


def _git_revision() -> dict:
    """The measured source: a verified export (:func:`source_revision`) or this git work tree.

    A directory that is not the top of a git work tree (an export inside another repository's
    tree, for instance) records no commit rather than the enclosing repository's."""
    exported = source_revision()
    if exported is not None:
        return exported
    try:
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=REPO, capture_output=True,
                             text=True, timeout=10).stdout.strip()
        if not top or os.path.realpath(top) != os.path.realpath(REPO):
            return {"commit": None, "dirty": None, "clean": False, "source": "not a git work tree"}
        rev = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO, capture_output=True,
                             text=True, timeout=10).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=REPO,
                               capture_output=True, text=True, timeout=10).stdout.strip() != ""
        return {"commit": rev or None, "dirty": dirty, "clean": not dirty, "source": "git work tree"}
    except (OSError, subprocess.SubprocessError):
        return {"commit": None, "dirty": None, "clean": False, "source": "git unavailable"}


def _version_of(module: str) -> Optional[str]:
    """The installed version of ``module`` without importing it (importing hnswlib enables the GIL)."""
    try:
        from importlib.metadata import PackageNotFoundError, version
    except ImportError:            # pragma: no cover - Python < 3.8
        return None
    try:
        return version(module)
    except PackageNotFoundError:
        return None


def _read_first(*paths: str) -> Optional[str]:
    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                return f.read().strip()
        except OSError:
            continue                # absent on this platform: try the next one
    return None


def cpu_frequency_policy() -> dict:
    """The CPU frequency governor, scaling driver and boost state (Linux sysfs; ``None`` when absent)."""
    base = "/sys/devices/system/cpu"
    return {"governor": _read_first(f"{base}/cpu0/cpufreq/scaling_governor"),
            "driver": _read_first(f"{base}/cpu0/cpufreq/scaling_driver"),
            "energy_performance_preference": _read_first(f"{base}/cpu0/cpufreq/energy_performance_preference"),
            "boost": _read_first(f"{base}/cpufreq/boost", f"{base}/intel_pstate/no_turbo")}


def gil_enabled() -> bool:
    """``sys._is_gil_enabled()``, True on interpreters without the function."""
    return sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True


def capture_environment(ctx: Context) -> dict:
    """CPU, cores, Python version and build, GIL state, library versions and the work directory."""
    import proto_db
    try:
        affinity = len(os.sched_getaffinity(0))
    except AttributeError:
        affinity = os.cpu_count()
    return {
        "timestamp": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "cpu_model": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "cpu_affinity": affinity,
        "cpu_frequency": cpu_frequency_policy(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "python_implementation": platform.python_implementation(),
        "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
        "gil_enabled": gil_enabled(),
        "xoptions": dict(sys._xoptions),
        "proto_db": proto_db.__version__,
        "git": _git_revision(),
        "sqlite": sqlite3.sqlite_version,
        "numpy": _version_of("numpy"),
        "hnswlib": _version_of("hnswlib"),
        "workdir": ctx.workdir,
        "workdir_fs": filesystem_of(ctx.workdir),
        "rqlite": ctx.rqlite,
    }


# -- runner ------------------------------------------------------------------------------------

def _interpreter_args() -> List[str]:
    """``sys.executable`` plus the ``-X`` options of this process (for child interpreters)."""
    args = [sys.executable]
    for key, value in sys._xoptions.items():
        args += ["-X", key if value is True else f"{key}={value}"]
    return args


def child_command(*argv: str) -> List[str]:
    """A command line running ``python -m <argv>`` with this interpreter and its ``-X`` options."""
    return _interpreter_args() + list(argv)


def _fmt_seconds(s: float) -> str:
    if s >= 1:
        return f"{s:.3f} s"
    if s >= 1e-3:
        return f"{s * 1e3:.2f} ms"
    return f"{s * 1e6:.1f} us"


def format_timing(label: str, t: dict) -> str:
    """One report line for a timing entry (``max``/``min`` instead of p95/p5 below 20 samples)."""
    name, value = t.get("tail_label"), t.get("tail")
    if name is None:
        name, value = tail(t)
    if t["unit"] == "s":
        line = f"  {label:<44} median {_fmt_seconds(t['median']):>11}  {name} {_fmt_seconds(value):>11}"
        if t.get("per", 0) > 1:
            line += f"  ({t['per_op_us']:.1f} us/op, {name} {t['per_op_us_tail']:.1f})"
    else:
        line = f"  {label:<44} median {t['median']:>11.1f} {t['unit']}  {name} {value:>9.1f}"
    line += f"  [n={t['n']}]"
    if t["n"] == 1:
        line += " SINGLE SAMPLE"
    return line


def _failed(params: dict, error: str, work: Optional[dict] = None) -> dict:
    return {"params": params, "status": "failed", "error": error, "work": work or {}}


def run_one(bench: Benchmark, ctx: Context, quick: bool) -> dict:
    """Run one workload in this process, verify it and return its JSON-ready record.

    A verification failure or an exception is recorded as ``failed`` (with the error and
    the work dict of the workload's result, when it had created one); it never stops the run.
    """
    params = ctx.params_of(bench, quick)
    gil_before = gil_enabled()
    started = time.perf_counter()
    _CURRENT.result = None
    result, error = None, None
    try:
        result = bench.fn(ctx, dict(params))
        result.verify()
    except VerificationError as e:
        error = f"verification failed: {e}"
    except Exception:         # a crash fails the run (exit 1); the remaining workloads still run
        error = traceback.format_exc()
    finally:
        ctx.cleanup()
    elapsed = time.perf_counter() - started
    gil_after = gil_enabled()
    if error is not None:
        partial = result if result is not None else getattr(_CURRENT, "result", None)
        record = _failed(params, error, partial.work if partial is not None else None)
    else:
        record = result.to_dict()
        record["params"] = params
    record.update({"elapsed_s": elapsed, "gil_before": gil_before, "gil_after": gil_after})
    record.setdefault("notes", [])
    if gil_after != gil_before and bench.isolate:
        record["notes"].append(f"the GIL state changed during the workload ({gil_before} -> {gil_after}); "
                               "it ran in its own interpreter, so no other workload was affected")
    elif gil_after != gil_before:
        record["status"] = "failed"
        record["error"] = (f"the GIL state changed during the workload ({gil_before} -> {gil_after}); "
                           "mark it isolate=True")
    return record


def run_isolated(bench: Benchmark, ctx: Context, quick: bool, plugins: List[str]) -> dict:
    """Run one workload in a child interpreter (same ``-X`` options, repetitions and plugins).

    A child that crashes or writes no valid record yields a ``failed`` record carrying the
    tail of the child's stderr (its traceback); the run continues.
    """
    params = ctx.params_of(bench, quick)
    out = tempfile.NamedTemporaryFile(prefix=f"{bench.name}-", suffix=".json", dir=ctx.workdir, delete=False)
    out.close()
    cmd = child_command("-m", "benchmarks", "--child", bench.name, "--child-out", out.name,
                        "--workdir", ctx.workdir, "--threads", ",".join(map(str, ctx.threads)),
                        "--repeat", str(ctx.repeat), "--warmup", str(ctx.warmup))
    for plugin in plugins:
        cmd += ["--plugin", plugin]
    for key, value in ctx.overrides.get(bench.name, {}).items():
        cmd += ["--param", f"{bench.name}.{key}={json.dumps(value)}"]
    if quick:
        cmd.append("--quick")
    if ctx.rqlite:
        cmd += ["--rqlite", ctx.rqlite]
    try:
        try:
            proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=3600)
        except subprocess.TimeoutExpired as e:
            return _failed(params, f"isolated child timed out after {e.timeout} s")
        sys.stdout.write(proc.stdout)
        try:
            with open(out.name, encoding="utf-8") as f:
                record = json.load(f)
        except (OSError, ValueError):
            return _failed(params, f"isolated child exited with {proc.returncode} without a valid record; "
                                   f"stderr tail:\n{proc.stderr[-4000:]}")
        record["isolated"] = True
        if proc.returncode not in (0, 1):
            record.update(status="failed", error=f"child exited with {proc.returncode}: {proc.stderr[-4000:]}")
        return record
    finally:
        os.unlink(out.name)


def print_record(name: str, record: dict) -> None:
    """Print a workload's work, verification and timings (work first, timings last)."""
    print(f"   work: {json.dumps(record.get('work', {}), sort_keys=True, default=str)}")
    if record["status"] == "pending":
        print(f"   PENDING: {'; '.join(record.get('notes', []))}")
        return
    if record["status"] == "failed":
        print(f"   FAILED: {record.get('error')}")
        return
    checks = record.get("checks", [])
    shown = repr(record.get("checksum"))
    shown = shown if len(shown) <= 100 else shown[:97] + "..."
    print(f"   verified: checksum {shown} == expected; "
          f"{sum(1 for c in checks if c['ok'])}/{len(checks)} checks passed")
    for label, t in record["timings"].items():
        print(format_timing(label, t))
    for key, value in record.get("metrics", {}).items():
        print(f"   {key}: {value}")
    for note in record.get("notes", []):
        print(f"   note: {note}")


#: Environment fields that must match for ``--compare`` to flag regressions.
COMPARE_ENV_KEYS = ("gil_enabled", "free_threaded_build", "cpu_model", "cpu_count", "python")


def environment_mismatch(current: dict, base: dict) -> List[str]:
    """The comparability problems between two result documents (mode and environment)."""
    problems = []
    if current.get("mode") != base.get("mode"):
        problems.append(f"mode {base.get('mode')} -> {current.get('mode')}")
    ce, be = current.get("environment", {}), base.get("environment", {})
    for key in COMPARE_ENV_KEYS:
        if ce.get(key) != be.get(key):
            problems.append(f"{key} {be.get(key)!r} -> {ce.get(key)!r}")
    return problems


def compare(current: dict, base: dict, threshold: float) -> List[str]:
    """Regressions of ``current`` against ``base``.

    A variant is flagged when its median is worse than the base median by more than
    ``threshold`` AND worse than the base's worst sample (noise guard: a change inside the
    base run's own spread is not reported).
    """
    flagged = []
    for name, rec in current.get("results", {}).items():
        old = base.get("results", {}).get(name)
        if not old or rec.get("status") != "ok" or old.get("status") != "ok":
            continue
        for label, t in rec.get("timings", {}).items():
            o = old.get("timings", {}).get(label)
            if not o or o.get("unit") != t.get("unit") or not o["median"]:
                continue
            ratio = t["median"] / o["median"]
            samples = o.get("samples") or [o["median"]]
            if t["better"] == "lower":
                worse = ratio > 1 + threshold and t["median"] > max(samples)
            else:
                worse = ratio < 1 / (1 + threshold) and t["median"] < min(samples)
            if worse:
                flagged.append(f"{name} / {label}: median {o['median']:.6g} -> {t['median']:.6g} "
                               f"{t['unit']} (x{ratio:.2f}, {t['better']} is better)")
    return flagged


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Command-line options of ``python -m benchmarks``."""
    p = argparse.ArgumentParser(prog="python -m benchmarks", description="ProtoDB performance suite (spec section 12).")
    p.add_argument("--quick", action="store_true", help="tiny parameter sets (checks the harness, not speed)")
    p.add_argument("--json", metavar="OUT", help="write results and environment to this JSON file")
    p.add_argument("--compare", metavar="BASE", help="flag regressions against a previous JSON result")
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                   help=f"relative median change flagged by --compare (default {DEFAULT_THRESHOLD})")
    p.add_argument("--allow-env-mismatch", action="store_true",
                   help="let --compare flag regressions although mode or environment differ")
    p.add_argument("--threads", default=",".join(map(str, DEFAULT_THREADS)),
                   help="thread counts for the scaling workloads (default 1,2,4,8,16)")
    p.add_argument("--only", nargs="+", metavar="NAME", help="run only these workloads")
    p.add_argument("--list", action="store_true", help="list the workloads and exit")
    p.add_argument("--repeat", type=int, help="timed repetitions per variant (default 5, quick 2)")
    p.add_argument("--warmup", type=int, help="untimed warm-up repetitions per variant (default 1)")
    p.add_argument("--workdir", help="directory for databases (default benchmarks/.work; must be on a "
                                     "real disk for the durability workloads, not tmpfs)")
    p.add_argument("--rqlite", default=os.environ.get("PROTODB_BENCH_RQLITED"),
                   help="path of the rqlited binary (or env PROTODB_BENCH_RQLITED); absent: rqlite pending")
    p.add_argument("--plugin", action="append", default=[], metavar="MODULE",
                   help="import an extra module registering workloads (repeatable)")
    p.add_argument("--param", action="append", default=[], metavar="NAME.KEY=JSON",
                   help="override one parameter of a workload, e.g. inventory.objects=1000000 (repeatable)")
    p.add_argument("--child", help=argparse.SUPPRESS)
    p.add_argument("--child-out", help=argparse.SUPPRESS)
    return p.parse_args(argv)


def parse_params(items: List[str]) -> Dict[str, dict]:
    """``["name.key=json", ...]`` -> ``{name: {key: value}}`` (a value that is not JSON is a string)."""
    out: Dict[str, dict] = {}
    for item in items:
        target, sep, raw = item.partition("=")
        name, dot, key = target.partition(".")
        if not sep or not dot or not name or not key:
            raise SystemExit(f"--param needs NAME.KEY=VALUE, got {item!r}")
        try:
            value = json.loads(raw)
        except ValueError:
            value = raw
        out.setdefault(name, {})[key] = value
    return out


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point: run the selected workloads, verify, report, write JSON, compare."""
    import importlib

    from benchmarks import workloads  # noqa: F401  (registers every workload)
    args = parse_args(argv)
    for plugin in args.plugin:
        importlib.import_module(plugin)
    if args.list:
        for name, b in REGISTRY.items():
            print(f"{name:<28} {'[isolated] ' if b.isolate else ''}{b.description}")
        return 0
    threads = [int(x) for x in args.threads.split(",") if x.strip()]
    if not threads or min(threads) < 1:
        raise SystemExit("--threads needs positive integers")
    workdir = os.path.abspath(args.workdir or os.path.join(REPO, "benchmarks", ".work"))
    os.makedirs(workdir, exist_ok=True)
    rqlite = os.path.abspath(args.rqlite) if args.rqlite else None
    ctx = Context(quick=args.quick, threads=threads, repeat=args.repeat or (2 if args.quick else 5),
                  warmup=args.warmup if args.warmup is not None else 1, workdir=workdir, rqlite=rqlite,
                  overrides=parse_params(args.param))

    if args.child:
        bench = REGISTRY[args.child]
        record = run_one(bench, ctx, args.quick)
        print_record(bench.name, record)
        with open(args.child_out, "w", encoding="utf-8") as f:
            json.dump(record, f, default=str)
        return 0 if record["status"] in ("ok", "pending") else 1

    names = args.only or list(REGISTRY)
    unknown = [n for n in names if n not in REGISTRY]
    if unknown:
        raise SystemExit(f"unknown workload(s): {', '.join(unknown)}; see --list")
    env = capture_environment(ctx)
    print(f"ProtoDB performance suite ({'quick' if args.quick else 'full'})")
    print(f"python {env['python']} free-threaded build={env['free_threaded_build']} "
          f"gil_enabled={env['gil_enabled']} cpus={env['cpu_count']} ({env['cpu_model']}); "
          f"cpu frequency {env['cpu_frequency']}")
    print(f"workdir {workdir} ({env['workdir_fs']}); threads {threads}; repeat {ctx.repeat} warmup {ctx.warmup}")
    print(f"source {env['git'].get('commit')} clean={env['git'].get('clean')} ({env['git'].get('source')})")
    if env["workdir_fs"] in ("tmpfs", "ramfs") and not args.quick:
        print("WARNING: the work directory is in memory: fsync costs nothing there and the durability "
              "workloads do not measure a disk")
    results, started = {}, time.perf_counter()
    for name in names:
        bench = REGISTRY[name]
        print(f"\n== {name} ==  {bench.description}")
        if bench.isolate:
            record = run_isolated(bench, ctx, args.quick, args.plugin)
            if record["status"] == "failed" and not record.get("isolated"):
                print_record(name, record)
        else:
            record = run_one(bench, ctx, args.quick)
            print_record(name, record)
        results[name] = record
    total = time.perf_counter() - started
    failed = [n for n, r in results.items() if r["status"] == "failed"]
    pending = [n for n, r in results.items() if r["status"] == "pending"]
    print(f"\n{len(results)} workload(s) in {total:.1f} s: {len(results) - len(failed) - len(pending)} verified, "
          f"{len(pending)} pending, {len(failed)} failed{(': ' + ', '.join(failed)) if failed else ''}")
    doc = {"schema": SCHEMA, "mode": "quick" if args.quick else "full", "environment": env,
           "options": {"threads": threads, "repeat": ctx.repeat, "warmup": ctx.warmup, "params": ctx.overrides},
           "total_s": total, "results": results}
    if args.json:
        os.makedirs(os.path.dirname(os.path.abspath(args.json)), exist_ok=True)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, default=str)
        print(f"results written to {args.json}")
    status = 1 if failed else 0
    if args.compare:
        with open(args.compare, encoding="utf-8") as f:
            base = json.load(f)
        problems = environment_mismatch(doc, base)
        if problems and not args.allow_env_mismatch:
            print(f"\ncompare with {args.compare}: NOT COMPARABLE ({'; '.join(problems)}); "
                  "pass --allow-env-mismatch to compare anyway")
            return status or 4
        flagged = compare(doc, base, args.threshold)
        print(f"\ncompare with {args.compare} (threshold {args.threshold:.0%}, outside the base spread): "
              f"{len(flagged)} regression(s){' [environment differs: ' + '; '.join(problems) + ']' if problems else ''}")
        for line in flagged:
            print(f"  REGRESSION {line}")
        if flagged and not status:
            status = 3
    return status
