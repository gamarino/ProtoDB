"""Cluster commits whose working set exceeds the decoded-atom cache (Task 8.3m, review condition 7).

Adapted from the reviewer's ``cluster_ws.py`` (review 8.3m step 2): three tiered members, each
space bounded to 64 loaded atoms, commit read-modify-write increments on random records of a
2,000-record dictionary from one thread per member.  Every member's working set (the dictionary's
path to a record, the record, the roots) is far over 64 atoms, so each commit reloads what earlier
admissions evicted, on the committing member and on the members that apply or warm its records.

Correctness only: once every member shows every acknowledged commit's token, each member's
counters equal the acknowledged commits plus the unknown ones that took effect (resolved by their
tokens, R114), and the cache did unload.  Throughput and critical-section misses against the base are
measured outside the suite (Task 8.3m report, "Fix round 1").
"""
import random
import threading

import pytest

from proto_db.errors import ProtoOutcomeUnknownError
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, read_tokens, snapshot_of, write_token
from tests.support.tiered_cluster import RUN_KW, TieredCluster

pytestmark = [pytest.mark.timeout(180)]

N = 2000
PER_NODE = 20
CAPACITY = 64
BUILD_CHUNK = 200


def test_cluster_commits_stay_correct_when_the_working_set_exceeds_the_cache(tmp_path):
    with TieredCluster(tmp_path, space_kw=dict(atom_cache_entries=CAPACITY)) as tc:
        tc.create_database("n0")

        # Setup in chunks: one 2,000-record commit can outlast the 0.9 s lease under a loaded
        # GIL-on suite.  Each chunk is committed exactly once whatever its attempts report
        # (``tc.commit``, review I6): an unknown outcome is another attempt, not a failure.
        def build(tr, lo):
            d = tr.get_root_object("d") or tr.new_dictionary()
            for i in range(lo, min(lo + BUILD_CHUNK, N)):
                d = d.set_at(i, tr.new_object(v=0))
            tr.set_root_object("d", d)
        for lo in range(0, N, BUILD_CHUNK):
            tc.commit("n0", lambda tr, lo=lo: build(tr, lo))
        for n in tc.ids:
            tc.spaces[n].sync(timeout=20)
        acked = {n: [] for n in tc.ids}
        unknown = {n: [] for n in tc.ids}
        errors = []
        tokens = CommitTokens()
        key_of = {}                                   # token -> the record its commit bumped

        def committer(node, seed):
            rnd = random.Random(seed)
            try:
                for _ in range(PER_NODE):
                    k = rnd.randrange(N)
                    token = tokens.new(node)
                    key_of[token] = k

                    def bump(tr, k=k, token=token):
                        tokens.observe(read_tokens(tr))   # P1: the base is a committed root
                        d = tr.get_root_object("d")
                        tr.set_root_object("d", d.set_at(k, d.get_at(k).with_(v=d.get_at(k).v + 1)))
                        write_token(tr, token)
                    try:
                        tc.db(node).run(bump, **RUN_KW)
                        acked[node].append(k)
                        tokens.acked(token)
                    except ProtoOutcomeUnknownError:
                        unknown[node].append(k)       # P5 / R86: resolved by its token below
                        tokens.unknown(token)
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
        # Unknown outcomes are legitimate (a release lost to a slow member, timing); their count
        # is no rate claim.  The guard only keeps the check below from going vacuous: most
        # commits must be acknowledged for the counters to pin anything.
        n_unknown = sum(len(u) for u in unknown.values())
        assert n_unknown * 2 <= PER_NODE * len(tc.ids), unknown
        if n_unknown:
            print(f"\n{n_unknown} unknown outcome(s), resolved by their tokens: {unknown}")

        def counters(tr):
            d = tr.get_root_object("d")
            return [d.get_at(i).v for i in range(N)]
        # A member learns another member's last commit when its release arrives, and sync() is
        # a durability barrier, not a visibility one (Task 8.3n, D46 amendment): the helper waits
        # until every member shows every acknowledged token.  Then, per member and in the same
        # snapshot (P4), each counter is its acknowledged increments plus the unknown ones whose
        # tokens are present: every unknown resolved, none hiding a lost acknowledged commit.
        res = assert_outcomes_resolved({n: (lambda n=n: snapshot_of(tc.db(n), counters)) for n in tc.ids}, tokens)
        expected = list(low)
        for token in res.present & tokens.unknown_tokens:
            expected[key_of[token]] += 1
        for n, got in res.values.items():
            bad = [i for i in range(N) if got[i] != expected[i]]
            assert not bad, (n, [(i, got[i], low[i], extra[i]) for i in bad[:10]])
        assert all(tc.spaces[n].atom_cache_stats()["unloads"] > 0 for n in tc.ids)
