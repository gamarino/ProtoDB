"""Where the acknowledgement path of reader nodes waits with the GIL on (Task R136, question 3).

The R110 cells (``cluster_cache``) showed writers on node A falling to 10-12 commits/s with the
GIL enabled while readers ran on B and C (separate processes).  This probe tests the reading that
B's and C's readers starve those nodes' acknowledging threads, by timing every hand-off of the
acknowledgement path inside each peer process:

* ``gil_wake``: a probe thread sleeps 1 ms in a loop and records how late it resumes.  A sleep
  releases the GIL; the lateness is the time to take it back (plus timer slack, measured with the
  GIL off), the same wait every thread of the ack path pays after each blocking call.
* ``dispatch``: the transport's receiving thread handing a message to the coordinator
  (``_on_message``): its duration.
* ``lane_wait``: from the dispatcher's ``_enqueue`` to the acceptor lane's ``get`` returning that
  message (a thread hand-off), by message type; ``lane_run``: from that ``get`` to the lane's next
  ``get`` (the handler, its log write and fsync, and the reply's encoding).
* ``send_wait``: from a frame's encoding (in ``Transport.send``, the replying thread) to the start
  of its ``sendall`` on the sender thread of that peer (another hand-off), by message type;
  ``sendall``: the call itself.
* ``sync``: every ``os.fsync``/``os.fdatasync`` (its return includes re-taking the GIL).

Only events inside the measuring window are kept (between the two ``_cache_stats`` calls of
:mod:`benchmarks.cache_peer` that bracket it).

Peer mode (``python gil_ack_probe.py CONFIG_JSON``, used as the ``cluster_cache`` peer script):
installs the probes, then runs :func:`benchmarks.cache_peer.main`; writes ``<node>.json`` to
``$PROTODB_PROBE_DIR`` at exit.  ``$PROTODB_PROBE_SWITCH`` (seconds) sets
``sys.setswitchinterval`` in reader processes (``readers`` > 0), a causal test: if the readers
starve the ack threads through the GIL's switch interval, a shorter interval on B and C must
raise A's commit rate.

Driver mode (``python -m benchmarks.gil_ack_probe --json OUT --workdir DIR [--reps N]``): runs the
point-read, default-cache cell of ``cluster_cache`` (3 TCP peers, 4 writers on A, readers on B
and C) in each variant (GIL on with 2 readers per reader node, GIL on with none, GIL on with 2
readers and a 0.5 ms switch interval on B and C, GIL off with 2 readers), interleaved, and writes
every cell's figures and each node's probe summary.
"""
import atexit
import json
import os
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EVENTS = []          # (kind, label, start, duration)
_WINDOW = []         # [start, end] from the two _cache_stats calls of the window


def _rec(kind, label, start, duration):
    EVENTS.append((kind, label, start, duration))


def install_peer_probes(node: str) -> None:
    """Patch the peer process (module docstring).  Call before the coordinator is created."""
    import socket
    from benchmarks import cache_peer
    from proto_db.cluster import coordinator as coord_module
    from proto_db.cluster import transport as transport_module
    C = coord_module.ClusterCoordinator

    stats_of = cache_peer._cache_stats

    def bracket(space):
        _WINDOW.append(time.monotonic())
        return stats_of(space)
    cache_peer._cache_stats = bracket

    on_message = C._on_message

    def timed_on_message(self, msg):
        t0 = time.monotonic()
        try:
            return on_message(self, msg)
        finally:
            _rec("dispatch", msg.get("type", "?"), t0, time.monotonic() - t0)
    C._on_message = timed_on_message

    enq_at = {}
    enqueue = C._enqueue

    def timed_enqueue(self, lane, msg):
        enq_at[id(msg)] = time.monotonic()
        return enqueue(self, lane, msg)
    C._enqueue = timed_enqueue

    init = C.__init__

    def init_and_wrap(self, *a, **kw):
        init(self, *a, **kw)
        lane = self._jobs
        get = lane.get
        state = {"t": None, "label": None}

        def timed_get(*ga, **gk):
            now = time.monotonic()
            if state["t"] is not None:
                _rec("lane_run", state["label"], state["t"], now - state["t"])
            msg = get(*ga, **gk)
            got = time.monotonic()
            label = msg.get("type", "?") if isinstance(msg, dict) else "stop"
            t_enq = enq_at.pop(id(msg), None)
            if t_enq is not None:
                _rec("lane_wait", label, t_enq, got - t_enq)
            state["t"], state["label"] = got, label
            return msg
        lane.get = timed_get
    C.__init__ = init_and_wrap

    frames = {}
    encode = transport_module.encode_frame

    def stamped_encode(message):
        frame = encode(message)
        frames[id(frame)] = (message.get("type", "?"), time.monotonic())
        return frame
    transport_module.encode_frame = stamped_encode

    sendall = socket.socket.sendall

    def timed_sendall(self, data, *a):
        t0 = time.monotonic()
        meta = frames.pop(id(data), None)
        try:
            return sendall(self, data, *a)
        finally:
            if meta is not None:
                _rec("send_wait", meta[0], meta[1], t0 - meta[1])
                _rec("sendall", meta[0], t0, time.monotonic() - t0)
    socket.socket.sendall = timed_sendall

    for call in ("fsync", "fdatasync"):
        original = getattr(os, call, None)
        if original is None:
            continue

        def timed_sync(fd, original=original, call=call):
            t0 = time.monotonic()
            try:
                return original(fd)
            finally:
                _rec("sync", call, t0, time.monotonic() - t0)
        setattr(os, call, timed_sync)

    def sleeper():
        while True:
            t0 = time.monotonic()
            time.sleep(0.001)
            _rec("gil_wake", "sleep 1 ms", t0, time.monotonic() - t0 - 0.001)
    threading.Thread(target=sleeper, name="probe-sleeper", daemon=True).start()

    def dump():
        out_dir = os.environ.get("PROTODB_PROBE_DIR")
        if not out_dir:
            return
        start, end = (_WINDOW[0], _WINDOW[1]) if len(_WINDOW) >= 2 else (None, None)
        kept = [e for e in list(EVENTS) if start is not None and start <= e[2] <= end]
        with open(os.path.join(out_dir, f"{node}.json"), "w") as f:
            json.dump({"node": node, "window": [start, end], "gil": sys._is_gil_enabled(),
                       "switch_interval": sys.getswitchinterval(), "summary": summarize(kept)}, f)
    atexit.register(dump)


def _pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else None


def summarize(events) -> dict:
    """Per (kind, label): count, median, p90, p99, max and total, in milliseconds."""
    groups = {}
    for kind, label, _, d in events:
        groups.setdefault(f"{kind}:{label}" if kind != "gil_wake" else kind, []).append(d)
    return {k: {"n": len(v), "p50_ms": round(statistics.median(v) * 1e3, 3), "p90_ms": round(_pct(v, 90) * 1e3, 3),
                "p99_ms": round(_pct(v, 99) * 1e3, 3), "max_ms": round(max(v) * 1e3, 3),
                "total_ms": round(sum(v) * 1e3, 1)} for k, v in sorted(groups.items())}


def peer_main() -> None:
    cfg = json.loads(sys.argv[1])
    switch = os.environ.get("PROTODB_PROBE_SWITCH")
    if switch and cfg["readers"]:
        sys.setswitchinterval(float(switch))
    install_peer_probes(cfg["node"])
    from benchmarks import cache_peer
    cache_peer.main()


VARIANTS = {
    "gil-on, 2 readers": {"gil": 1, "readers": 2, "switch": None},
    "gil-on, 0 readers": {"gil": 1, "readers": 0, "switch": None},
    "gil-on, 2 readers, switch 0.5 ms on B/C": {"gil": 1, "readers": 2, "switch": "0.0005"},
    "gil-off, 2 readers": {"gil": 0, "readers": 2, "switch": None},
}

_CELL_CHILD = r"""
import json, os, sys
sys.path.insert(0, os.getcwd())
from benchmarks.harness import Context
from benchmarks.workloads import cluster_cache
cluster_cache.PEER_SCRIPT = os.path.join(os.getcwd(), "benchmarks", "gil_ack_probe.py")
cfg = json.loads(sys.argv[2])
ctx = Context(quick=False, threads=[4], repeat=1, warmup=0, workdir=sys.argv[1])
shape = dict(cluster_cache.SHAPE, readers=cfg["readers"])
try:
    out = cluster_cache.protodb_cell(ctx, "point", "on", "on", cfg["seconds"], shape)
finally:
    ctx.cleanup()
print(json.dumps({k: out.get(k) for k in ("ok", "commits", "commit_rate", "commit_p50_ms", "reads", "read_rate",
                                          "read_p50_us", "versions_advanced", "replication_failures_by_peer")}))
"""


def driver() -> None:
    import argparse
    import subprocess
    ap = argparse.ArgumentParser(description="GIL-on ack-path probe (module docstring)")
    ap.add_argument("--json", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--seconds", type=float, default=5.0)
    ap.add_argument("--only", nargs="*")
    args = ap.parse_args()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    names = [n for n in VARIANTS if not args.only or n in args.only]
    runs = []
    for rep in range(args.reps):
        for name in (names if rep % 2 == 0 else names[::-1]):
            v = VARIANTS[name]
            probe_dir = os.path.join(args.workdir, f"probe-{rep}-{names.index(name)}")
            os.makedirs(probe_dir, exist_ok=True)
            env = dict(os.environ, PROTODB_PROBE_DIR=probe_dir)
            if v["switch"]:
                env["PROTODB_PROBE_SWITCH"] = v["switch"]
            else:
                env.pop("PROTODB_PROBE_SWITCH", None)
            cmd = [sys.executable, "-X", f"gil={v['gil']}", "-c", _CELL_CHILD, args.workdir,
                   json.dumps({"readers": v["readers"], "seconds": args.seconds})]
            started = time.time()
            p = subprocess.run(cmd, cwd=root, env=env, capture_output=True, text=True, timeout=900)
            lines = [x for x in p.stdout.splitlines() if x.startswith("{")]
            cell = json.loads(lines[-1]) if p.returncode == 0 and lines else {"error": (p.stderr or p.stdout)[-3000:]}
            nodes = {}
            for n in ("n0", "n1", "n2"):
                path = os.path.join(probe_dir, f"{n}.json")
                if os.path.exists(path):
                    with open(path) as f:
                        nodes[n] = json.load(f)
            runs.append({"variant": name, "rep": rep, "started": started, "cell": cell, "nodes": nodes})
            print(f"rep {rep} {name}: {cell.get('commit_rate', 0):.1f} commits/s, "
                  f"{cell.get('read_rate', 0):.0f} reads/s, ok={cell.get('ok')}", flush=True)
            with open(args.json, "w") as f:
                json.dump({"args": vars(args), "runs": runs}, f, indent=1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("{"):
        peer_main()
    else:
        driver()
