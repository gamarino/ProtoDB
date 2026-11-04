"""Per-commit time budget of a cluster peer, by phase and by thread (Task 8.3h).

An instrumentation module for the benchmark peers (:mod:`benchmarks.cluster_peer`), in the
spirit of :mod:`benchmarks.fsync_probe`: :func:`install` wraps a few functions of
``proto_db.transactions`` and methods of :class:`~proto_db.cluster.coordinator.ClusterCoordinator`
with recorders of ``(event, thread, start, end, extra)`` tuples, timed with
``time.monotonic()`` (``CLOCK_MONOTONIC``, one clock for every process of the machine, so the
events of several peers can be merged on one time line).  Nothing in ``proto_db`` changes:
the wrappers call the original and only note when it started and ended.

Events (``extra`` in brackets):

* ``commit``: :func:`proto_db.transactions._commit_under_lease`, a commit's whole stay in its
  node's group (queue, permission wait, round) [ok];
* ``save``: ``Transaction._commit`` (the commit's own record writes plus the group stay);
* ``run_round``: one runner's permission and rounds [commits taken from the queue];
* ``permit``: ``ClusterCoordinator.permit`` (the wait for a write permission);
* ``round``: one ``_permit_round`` [entries];
* ``replicate``, ``self_accept`` (the committer's own synced append), ``place`` (the data
  phase: records to the data replicas until their ``stored`` acks), ``finish`` (``done``);
* ``built``: one ``record_critical`` per commit that a round replicated (instant);
* ``apply_replicate``: a peer's handling of a replication on its acceptor lane;
* ``permit_req_in``, ``permit_reply_in``, ``done_in``: message arrivals (instant);
* ``sync``: every ``os.fsync``/``os.fdatasync`` [file name].

The probe adds two clock reads and a list append per event (about a microsecond), plus a
``readlink`` per sync.  It is informational; enable it with ``PROTODB_BENCH_BUDGET_DIR``.
:func:`analyze` (also ``python -m benchmarks.commit_budget DIR``) merges the event files of
one run into its budget.
"""
import bisect
import glob
import json
import os
import statistics
import threading
import time
from collections import Counter, defaultdict

EVENTS = []
_INSTALLED = []


def _rec(name, start, end, extra=None):
    EVENTS.append((name, threading.current_thread().name, start, end, extra))


def _timed(name, fn, extra_of=None):
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = None
        try:
            result = fn(*args, **kwargs)
            return result
        finally:
            _rec(name, start, time.monotonic(), extra_of(args, result) if extra_of else None)
    wrapper.__wrapped__ = fn
    return wrapper


def _instant(name, fn):
    def wrapper(*args, **kwargs):
        now = time.monotonic()
        _rec(name, now, now)
        return fn(*args, **kwargs)
    wrapper.__wrapped__ = fn
    return wrapper


def install() -> None:
    """Wrap the recorded functions (idempotent).  Call before any coordinator is created."""
    if _INSTALLED:
        return
    from proto_db import transactions as tx
    from proto_db.cluster.coordinator import ClusterCoordinator as C

    def patch(owner, attr, wrapper_of):
        original = getattr(owner, attr)
        setattr(owner, attr, wrapper_of(original))
        _INSTALLED.append((owner, attr, original))

    patch(tx, "_commit_under_lease", lambda f: _timed("commit", f, lambda a, r: bool(r)))
    patch(tx, "_run_round", lambda f: _timed("run_round", f, lambda a, r: len(a[2])))
    patch(tx, "_permit_round", lambda f: _timed("round", f, lambda a, r: len(a[3])))
    patch(tx.Transaction, "_commit", lambda f: _timed("save", f))
    patch(C, "permit", lambda f: _timed("permit", f))
    patch(C, "replicate", lambda f: _timed("replicate", f))
    patch(C, "_self_accept_with_records", lambda f: _timed("self_accept", f))
    patch(C, "_place_records", lambda f: _timed("place", f))
    patch(C, "finish", lambda f: _timed("finish", f))
    patch(C, "_apply_replicate", lambda f: _timed("apply_replicate", f))
    patch(C, "record_critical", lambda f: _instant("built", f))
    patch(C, "_on_permit_req", lambda f: _instant("permit_req_in", f))
    patch(C, "_on_permit_reply", lambda f: _instant("permit_reply_in", f))
    patch(C, "_on_done_owner", lambda f: _instant("done_in", f))

    for call in ("fsync", "fdatasync"):
        if hasattr(os, call):
            def sync_wrapper_of(f, call=call):
                def wrapper(fd):
                    start = time.monotonic()
                    try:
                        return f(fd)
                    finally:
                        end = time.monotonic()
                        try:
                            target = os.path.basename(os.readlink(f"/proc/self/fd/{fd}"))
                        except OSError:
                            target = "?"
                        _rec("sync", start, end, f"{call}:{target}")
                wrapper.__wrapped__ = f
                return wrapper
            patch(os, call, sync_wrapper_of)


def window(start: float, end: float) -> list:
    """The events that started in ``[start, end]``, as lists (JSON-ready)."""
    return [[n, t, round(s, 6), round(e, 6), x] for n, t, s, e, x in list(EVENTS) if start <= s <= end]


# -- analysis --------------------------------------------------------------------------------

def _med(xs):
    """Median in milliseconds."""
    return round(statistics.median(xs) * 1e3, 3) if xs else None


def _mean(xs):
    """Mean in milliseconds."""
    return round(sum(xs) / len(xs) * 1e3, 3) if xs else None


def _overlap(a0, a1, b0, b1):
    """Length of the intersection of two intervals."""
    return max(0.0, min(a1, b1) - max(a0, b0))


def analyze(dirpath):
    """Merge the n*.json event files of one run and return its budget (JSON-ready dict).

    * chain: the permission chain -- each round's holding interval (permission obtained to
      done sent), the hand-offs between consecutive holders, and the commits per hold;
    * commit_phases_ms_median / _mean: each acknowledged commit's stay in its group,
      split into the wait for its node's previous round, the permission wait, the build
      (base, rebase, merge), the holder's own synced append, the data phase (records to the
      replica until its stored ack), the rest of the replication and the return;
    * syncs_*: syncs per commit and per round by thread role and file, their latency, and
      the share that overlaps some node's holding interval.
    """
    nodes = {}
    for p in sorted(glob.glob(os.path.join(dirpath, "n*.json"))):
        with open(p) as f:
            d = json.load(f)
        nodes[d["node"]] = d
    start = min(d["start"] for d in nodes.values() if d["threads"])
    end = max(d["end"] for d in nodes.values() if d["threads"])
    span = end - start
    out = {"span_s": round(span, 3)}
    holds = []          # (start, end, node, commits, phases)
    per_node = {}
    commit_phases = defaultdict(list)
    sync_rows = []
    for node, d in nodes.items():
        ev = defaultdict(list)
        for name, th, s, e, x in d["events"]:
            ev[name].append((s, e, th, x))
        for k in ev:
            ev[k].sort()
        built = ev["built"]
        built_by_thread = defaultdict(list)
        for s, e, th, x in built:
            built_by_thread[th].append(s)
        rounds = []
        for s, e, th, x in ev["round"]:
            bt = built_by_thread[th]
            n = bisect.bisect_right(bt, e) - bisect.bisect_left(bt, s)
            rounds.append((s, e, th, n))
        # each run_round: permit wait, then rounds, then finish
        permits = ev["permit"]
        finishes = ev["finish"]
        reps = ev["replicate"]
        selfs = ev["self_accept"]
        places = ev["place"]
        rr = []
        for s, e, th, x in ev["run_round"]:
            p = [q for q in permits if q[2] == th and s <= q[0] <= e]
            f = [q for q in finishes if q[2] == th and s <= q[0] <= e + 0.5]
            rs = [r for r in rounds if r[2] == th and s <= r[0] <= e]
            got = p[0][1] if p else s
            fin_end = f[0][1] if f else e
            n = sum(r[3] for r in rs)
            rr.append({"s": s, "e": e, "th": th, "taken": x, "permit_s": p[0][0] if p else None, "got": got,
                       "fin_end": fin_end, "rounds": rs, "n": n})
            if n:
                holds.append((got, fin_end, node, n, th))
        nc = len(built)
        per_node[node] = {"commits": nc, "rounds_with_commits": sum(1 for r in rounds if r[3]),
                          "run_rounds": len(rr), "permits": len(permits),
                          "taken_from_queue_median": statistics.median([r["taken"] for r in rr]) if rr else None,
                          "commits_per_round": round(nc / max(1, sum(1 for r in rounds if r[3])), 2),
                          "commits_per_permit": round(nc / max(1, len(permits)), 2)}
        # per-commit phases: match each commit to the round whose built time precedes its end
        all_built_rounds = sorted([(r["rounds"], r) for r in rr if r["n"]], key=lambda z: z[1]["s"])
        round_list = []
        for rs, r in all_built_rounds:
            for rnd in rs:
                if rnd[3]:
                    round_list.append((rnd, r))
        round_list.sort(key=lambda z: z[0][1])
        ends = [z[0][1] for z in round_list]
        saves = defaultdict(list)
        for s, e, th, x in ev["save"]:
            saves[th].append((s, e))
        for s, e, th, ok in ev["commit"]:
            if not ok:
                continue
            i = bisect.bisect_right(ends, e + 0.002) - 1
            if i < 0:
                continue
            rnd, r = round_list[i]
            rs_, re_ = rnd[0], rnd[1]
            if rs_ < s - 0.5:
                continue
            ph = {}
            pre = max(0.0, rs_ - s)
            perm = _overlap(s, rs_, r["permit_s"] or r["s"], r["got"]) if r["permit_s"] else 0.0
            ph["wait: previous round of this node"] = max(0.0, min(rs_, r["permit_s"] or rs_) - s) if s < (r["permit_s"] or rs_) else 0.0
            ph["wait: permission"] = perm
            ph["wait: other before round"] = max(0.0, pre - ph["wait: previous round of this node"] - perm)
            rep = [q for q in reps if q[2] == rnd[2] and rs_ <= q[0] <= re_]
            if rep:
                rp = rep[0]
                sa = [q for q in selfs if q[2] == rnd[2] and rp[0] <= q[0] <= rp[1]]
                pl = [q for q in places if q[2] == rnd[2] and rp[0] <= q[0] <= rp[1]]
                ph["round: base + build (rebase, merge)"] = rp[0] - rs_
                ph["replicate: own synced append"] = (sa[0][1] - sa[0][0]) if sa else 0.0
                ph["replicate: data replica (send, its sync, ack)"] = (pl[0][1] - pl[0][0]) if pl else 0.0
                ph["replicate: rest (warm send, majority, log)"] = (rp[1] - rp[0]) - ph["replicate: own synced append"] - ph["replicate: data replica (send, its sync, ack)"]
                ph["round: after replicate"] = re_ - rp[1]
            ph["return to thread"] = max(0.0, e - re_)
            ph["total in group"] = e - s
            sv = [q for q in saves[th] if q[0] <= s <= q[1]]
            ph["record writes before the group"] = (s - sv[0][0]) if sv else 0.0
            for k, v in ph.items():
                commit_phases[k].append(v)
        for s, e, th, x in ev["sync"]:
            sync_rows.append((s, e, node, th, x))
        per_node[node]["apply_replicate_ms_median"] = _med([e - s for s, e, th, x in ev["apply_replicate"]])
        per_node[node]["permit_wait_ms_median"] = _med([e - s for s, e, th, x in permits])
    holds.sort()
    total_commits = sum(v["commits"] for v in per_node.values())
    out["commits"] = total_commits
    out["rounds"] = len(holds)
    out["rounds_per_s"] = round(len(holds) / span, 1)
    out["commits_per_round_cluster"] = round(total_commits / max(1, len(holds)), 2)
    out["per_node"] = per_node
    # permission chain: holding intervals and the hand-offs between them
    held = [h[1] - h[0] for h in holds]
    gaps = [max(0.0, holds[i + 1][0] - holds[i][1]) for i in range(len(holds) - 1)]
    overlaps = sum(1 for i in range(len(holds) - 1) if holds[i + 1][0] < holds[i][1])
    same = [max(0.0, holds[i + 1][0] - holds[i][1]) for i in range(len(holds) - 1) if holds[i + 1][2] == holds[i][2]]
    other = [max(0.0, holds[i + 1][0] - holds[i][1]) for i in range(len(holds) - 1) if holds[i + 1][2] != holds[i][2]]
    out["chain"] = {"held_ms_median": _med(held), "held_ms_mean": _mean(held), "handoff_ms_median": _med(gaps),
                    "handoff_ms_mean": _mean(gaps), "handoff_same_node_ms_median": _med(same),
                    "handoff_other_node_ms_median": _med(other), "overlapping_holds": overlaps,
                    "held_fraction_of_span": round(sum(held) / span, 3),
                    "commits_per_hold_hist": dict(sorted(Counter(h[3] for h in holds).items()))}
    out["commit_phases_ms_median"] = {k: _med(v) for k, v in commit_phases.items()}
    out["commit_phases_ms_mean"] = {k: _mean(v) for k, v in commit_phases.items()}
    # syncs: by file kind, latency, and whether inside a holding interval
    hs = [h[0] for h in holds]
    def inside(s, e):
        i = bisect.bisect_right(hs, e) - 1
        while i >= 0 and holds[i][1] >= s - 0:
            if _overlap(s, e, holds[i][0], holds[i][1]) > 0:
                return True
            i -= 1
            if i >= 0 and hs[i] < s - 5:
                break
        return False
    by = Counter()
    lat = defaultdict(list)
    ins = Counter()
    ins_time = 0.0
    tot_time = 0.0
    for s, e, node, th, x in sync_rows:
        f = x.split(":", 1)[1]
        kind = "roots.log" if f.startswith("roots") else ("ranges.log" if f.startswith("ranges") else
                                                           ("segment/copy" if f.endswith(".seg") or "seg" in f else f[:20]))
        role = ("acceptor lane" if th.startswith("proto-cluster-worker") or "acceptor" in th else
                "persister" if th.startswith("proto-persister") else
                "transport" if th.startswith("proto-cluster-tcp") else "committer")
        key = f"{role}: {kind}"
        by[key] += 1
        lat[key].append(e - s)
        tot_time += e - s
        if inside(s, e):
            ins[key] += 1
            ins_time += e - s
    n = max(1, total_commits)
    out["syncs_per_commit"] = round(len(sync_rows) / n, 2)
    out["syncs_per_round"] = round(len(sync_rows) / max(1, len(holds)), 2)
    out["syncs_by_kind_per_commit"] = {k: round(v / n, 2) for k, v in by.most_common()}
    out["sync_ms_median_by_kind"] = {k: _med(v) for k, v in lat.items()}
    out["syncs_inside_permission_fraction"] = round(sum(ins.values()) / max(1, len(sync_rows)), 3)
    out["sync_time_inside_permission_fraction"] = round(ins_time / max(1e-9, tot_time), 3)
    out["sync_ms_median_all"] = _med([e - s for s, e, *_ in sync_rows])
    return out


if __name__ == "__main__":
    import sys
    print(json.dumps(analyze(sys.argv[1]), indent=1))
