"""CPU of one commit's build on one node, without the cluster (Task R136, question 1).

The cluster bisection (:mod:`benchmarks.bisect_commit_path`) found that the commit-path loss
since 4aab0f2 sits in the critical section's build (``path_copy`` and ``replay``), with the same
syncs, frames and bytes per commit.  This probe measures that build alone, in one process, so
its CPU can be compared across exports with little noise and attributed to functions:

    python benchmarks/save_path_probe.py [--commits N] [--profile OUT.txt] [--patch NAME]

run with an export as the working directory (``proto_db`` is imported from it).  A
``MemoryStorage`` space; each commit is the ``cluster_peer`` payload commit: a blind add of one
id to the ``ids`` CountedSet root and one 100 B value under it in the ``blobs`` Dictionary root,
written blind and merged at commit (D9), so the commit replays both op logs over the current
roots and path-copies them, as the cluster critical section does.  Prints one JSON line: commits,
microseconds per commit (median of 5 batches after a warm-up) and the final counts, verified
against the model.  ``--profile`` writes a cProfile listing (by total time) of one more batch.

``--patch class_defines_fast`` (an experiment, not a product change) replaces
``proto_db.core.atom.class_defines`` with ``hasattr(cls, name) and <the MRO walk>`` before the
run: the same answer (a name that some class of the MRO defines is always found by ``hasattr``
on the class), with the walk only for names the class has at all.
"""
import argparse
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.getcwd())


def payload_value(first_id: int, size: int) -> bytes:
    unit = b"%d:" % first_id
    return (unit * (size // len(unit) + 1))[:size]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commits", type=int, default=400)
    ap.add_argument("--batches", type=int, default=5)
    ap.add_argument("--payload", type=int, default=100)
    ap.add_argument("--profile")
    ap.add_argument("--patch", default="")
    args = ap.parse_args()
    if args.patch == "class_defines_fast":
        from proto_db.core import atom as atom_module
        from proto_db.core import dbobject as dbobject_module

        def class_defines(cls, name):
            return hasattr(cls, name) and any(name in klass.__dict__ for klass in cls.__mro__)
        atom_module.class_defines = class_defines
        dbobject_module.class_defines = class_defines
    from proto_db import MemoryStorage, ObjectSpace
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    next_id = [0]

    def commit():
        first = next_id[0]

        def fn(tr):
            tr.set_root_object("ids", tr.new_counted_set().add(first))
            if args.payload:
                tr.set_root_object("blobs", tr.new_dictionary().set_at(first, payload_value(first, args.payload)))
        db.run(fn)
        next_id[0] += 1

    def batch():
        t0 = time.perf_counter()
        for _ in range(args.commits):
            commit()
        return (time.perf_counter() - t0) / args.commits

    batch()                                         # warm-up (also grows the roots)
    per = [batch() for _ in range(args.batches)]
    if args.profile:
        import cProfile
        import pstats
        prof = cProfile.Profile()
        prof.enable()
        batch()
        prof.disable()
        with open(args.profile, "w") as f:
            pstats.Stats(prof, stream=f).sort_stats("tottime").print_stats(40)
    tr = db.new_transaction()
    ids = tr.get_root_object("ids")
    n = next_id[0]
    ok = sorted(ids.elements()) == list(range(n)) and all(c == 1 for _, c in ids.items())
    if args.payload:
        blobs = tr.get_root_object("blobs")
        ok = ok and blobs.count == n and all(blobs.get_at(k) == payload_value(k, args.payload)
                                             for k in range(0, n, max(1, n // 200)))
    tr.abort()
    space.close()
    print(json.dumps({"ok": ok, "commits": n, "us_per_commit": round(statistics.median(per) * 1e6, 1),
                      "batches_us": [round(x * 1e6, 1) for x in per], "patch": args.patch,
                      "gil": sys._is_gil_enabled()}))
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
