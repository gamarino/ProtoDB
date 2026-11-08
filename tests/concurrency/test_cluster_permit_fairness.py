"""Commit fairness under write permissions (Task 8.3f, D50; defect D-LIVE-2).

Before Task 8.3f a hot-key read-modify-write conflicted under the lease, released it, retried
from a fresh snapshot outside the lease, waited a whole round and conflicted again: with two
members x two threads incrementing one counter for 3 s the reviewer measured 495 commits
against 0 (8.3e review I1, probe ``p5_hotkey.py``).  Under D50 every member with demand gets
write permissions in turn (FIFO at the owner), and a transaction that conflicts under a
permission is retried under the same permission from the root that permission just committed,
so every permission commits at least one increment.

Checks: no lost update (every commit writes a unique token besides its increment: after the
members converge every acknowledged token is present, every unknown one -- P5, a lease ending
under CPU contention -- is present on all members or on none, no other token is present, and
the counter equals the tokens present on every member; ruling R114, N1), no member without
commits, and the fewest commits of a member over the most of any member at least
``_MIN_RATIO`` (fair: about 1.0).  Measured (in-process, ``durability="none"``): base
``cc849dc`` hot key [436, 0] and [0, 0, 319] (both fail), blind [199, 198, 198]; with D50
[313, 312], [196, 197, 197] and [221, 220, 218].
"""
import threading
import time

import pytest

from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, read_tokens, snapshot_of, write_token
from tests.support.tiered_cluster import TieredCluster

pytestmark = [pytest.mark.timeout(240)]

_SECONDS = 2.0
_MIN_RATIO = 0.3
KW = dict(lease_duration=2.0, request_timeout=0.25)


def _hot_key_run(tmp_path, n: int, threads: int, *, blind: bool = False) -> dict:
    cluster = TieredCluster(tmp_path, n, storage_kw=dict(durability="none"), **KW)
    try:
        cluster.create_database(cluster.ids[0])
        acked = {node: 0 for node in cluster.ids}
        tokens = CommitTokens()
        lock = threading.Lock()
        start = threading.Barrier(n * threads + 1)
        stop_at = []

        def worker(node, index):
            db = cluster.db(node)
            start.wait()
            j = 0
            while time.monotonic() < stop_at[0]:
                tr = db.new_transaction()
                token = None
                try:
                    if blind:
                        tr.set_root_object(f"k-{node}-{index}-{j}", j)
                    else:
                        token = tokens.new(f"{node}.{index}")
                        tokens.observe(read_tokens(tr))   # P1: the snapshot is a committed root
                        tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                        write_token(tr, token)
                    tr.commit()
                except ProtoConflictError:
                    continue                          # not applied: its token must stay absent
                except ProtoOutcomeUnknownError:      # P5: resolved by its token below (R86)
                    tokens.unknown(token)
                    j += 1
                    continue
                if token is not None:
                    tokens.acked(token)
                j += 1
                with lock:
                    acked[node] += 1

        workers = [threading.Thread(target=worker, args=(node, i)) for node in cluster.ids for i in range(threads)]
        for t in workers:
            t.start()
        stop_at.append(time.monotonic() + _SECONDS)
        start.wait()
        for t in workers:
            t.join()
        total = sum(acked.values())
        values = None
        if not blind:
            res = assert_outcomes_resolved(            # the counter from the tokens' snapshot (P4)
                {node: (lambda node=node: snapshot_of(cluster.db(node), lambda tr: tr.get_root_object("n")))
                 for node in cluster.ids}, tokens)
            values = res.values
            assert set(values.values()) == {len(res.present)}, (values, len(res.present))
        counts = [acked[node] for node in cluster.ids]
        return {"counts": counts, "total": total, "values": values, "unknown": len(tokens.unknown_tokens),
                "ratio": min(counts) / max(counts) if max(counts) else 0.0}
    finally:
        cluster.close()


@pytest.mark.parametrize("n,threads", [(2, 2), (3, 2)])
def test_hot_key_increments_never_starve_a_member(tmp_path, n, threads, capsys):
    result = _hot_key_run(tmp_path, n, threads)
    with capsys.disabled():
        print(f"\nHOT KEY {n} members x {threads} threads, {_SECONDS:g} s: commits {result['counts']} "
              f"ratio {result['ratio']:.2f}")
    # Every outcome resolved by its token in _hot_key_run (R86, R114 N1).
    assert result["unknown"] <= 3, result
    assert min(result["counts"]) > 0, result                  # nobody starved
    assert result["ratio"] >= _MIN_RATIO, result


def test_blind_writes_are_shared_between_members(tmp_path, capsys):
    result = _hot_key_run(tmp_path, 3, 4, blind=True)
    with capsys.disabled():
        print(f"\nBLIND 3 members x 4 threads, {_SECONDS:g} s: commits {result['counts']} "
              f"ratio {result['ratio']:.2f}")
    assert min(result["counts"]) > 0, result
    assert result["ratio"] >= _MIN_RATIO, result
