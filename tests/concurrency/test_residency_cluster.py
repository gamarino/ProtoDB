"""Cluster commits whose working set exceeds the decoded-atom cache (Task 8.3m, review condition 7).

Adapted from the reviewer's ``cluster_ws.py`` (review 8.3m step 2): three tiered members, each
space bounded to 64 loaded atoms, commit read-modify-write increments on random records of a
2,000-record dictionary from one thread per member.  Every member's working set (the dictionary's
path to a record, the record, the roots) is far over 64 atoms, so each commit reloads what earlier
admissions evicted, on the committing member and on the members that apply or warm its records.

Correctness only: after every member synced, the counters equal a model of the acknowledged
commits, and the cache did unload.  Throughput and critical-section misses against the base are
measured outside the suite (Task 8.3m report, "Fix round 1").
"""
import random
import threading
import time

import pytest

from proto_db.errors import ProtoOutcomeUnknownError
from tests.support.tiered_cluster import RUN_KW, TieredCluster

pytestmark = [pytest.mark.timeout(180)]

N = 2000
PER_NODE = 20
CAPACITY = 64


def test_cluster_commits_stay_correct_when_the_working_set_exceeds_the_cache(tmp_path):
    with TieredCluster(tmp_path, space_kw=dict(atom_cache_entries=CAPACITY)) as tc:
        tc.spaces["n0"].new_database("db")

        def build(tr):
            d = tr.new_dictionary()
            for i in range(N):
                d = d.set_at(i, tr.new_object(v=0))
            tr.set_root_object("d", d)
        tc.db("n0").run(build, **RUN_KW)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=20)
        acked = {n: [] for n in tc.ids}
        unknown = {n: [] for n in tc.ids}
        errors = []

        def committer(node, seed):
            rnd = random.Random(seed)
            try:
                for _ in range(PER_NODE):
                    k = rnd.randrange(N)

                    def bump(tr, k=k):
                        d = tr.get_root_object("d")
                        tr.set_root_object("d", d.set_at(k, d.get_at(k).with_(v=d.get_at(k).v + 1)))
                    try:
                        tc.db(node).run(bump, **RUN_KW)
                        acked[node].append(k)
                    except ProtoOutcomeUnknownError:
                        unknown[node].append(k)       # P5 / R86: applied at most once
            except BaseException as e:                # reported by the assertion below
                errors.append(e)
                raise
        threads = [threading.Thread(target=committer, args=(n, i), name=f"committer-{n}")
                   for i, n in enumerate(tc.ids)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(150)
        assert not errors, errors
        for n in tc.ids:
            tc.spaces[n].sync(timeout=20)
        low, extra = [0] * N, [0] * N
        for n in tc.ids:
            for k in acked[n]:
                low[k] += 1
            for k in unknown[n]:
                extra[k] += 1
        assert sum(len(u) for u in unknown.values()) <= 3, unknown

        def read():
            tr = tc.db("n1").new_transaction()
            try:
                d = tr.get_root_object("d")
                return [d.get_at(i).v for i in range(N)]
            finally:
                tr.abort()
        # A member learns another member's last commit when its release arrives (the reviewer's
        # probe read n1 once and saw one commit missing in 2 of 11 GIL-on runs on 26f3b85;
        # it converged): read until every acknowledged increment is visible, then check bounds.
        deadline = time.monotonic() + 10
        got = read()
        while any(got[i] < low[i] for i in range(N)) and time.monotonic() < deadline:
            time.sleep(0.05)
            got = read()
        # R86: every acknowledged increment applied, an unknown one at most once.
        bad = [i for i in range(N) if not low[i] <= got[i] <= low[i] + extra[i]]
        assert not bad, [(i, got[i], low[i], extra[i]) for i in bad[:10]]
        assert all(tc.spaces[n].atom_cache_stats()["unloads"] > 0 for n in tc.ids)
