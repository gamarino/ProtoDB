"""Bounded replication factor (Task 8.3g, D54; maintainer rulings R70, R70a).

A commit's records are made durable on the committing node and on ``f`` other members, its
data replicas, chosen as the committer's successors in the sorted order of member ids
(skipping members that did not answer lately); its root is ordered by the ``2f + 1`` voters,
and a voter accepts it only once ``f + 1`` members hold the records durably.  Every other
member fetches the records on demand, from a replica the commit's locator names or from the
object store, and caches them.  The cluster's size no longer multiplies a commit's syncs or
the bytes it ships.

Every case runs tiered storages over one shared object store (in-process network).
"""
import os
import threading
import time

import pytest

from proto_db.cluster.protocol import encode_ranges, encode_root, make_message
from proto_db.core.atom import AtomPointer
from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(240)]

LEASE = dict(lease_duration=0.9, request_timeout=0.1)


def _cluster(tmp_path, n, **kw):
    tc = TieredCluster(tmp_path, n, storage_kw=dict(durability="fsync"), **dict(LEASE, **kw))
    tc.spaces[tc.ids[0]].new_database("db")
    return tc


def _root(tc, node):
    known, root, label = tc[node].committed_root()[:3]
    assert known and root is not None
    return root, label


def _holders(tc, ptr) -> set:
    """The members holding the record at ``ptr`` in their local files (durable copies; the
    record cache, which R89 warms on every member, is not a copy)."""
    return {n for n, st in tc.storages.items() if st._local_record(ptr) is not None}


def test_a_commit_stores_its_records_on_f_plus_one_members_and_its_root_on_the_voters(tmp_path):
    """On d6743ae every member holds every record and accepts every root (8 of 8)."""
    with _cluster(tmp_path, 8) as tc:
        for _ in range(3):
            tc.db("n5").run(incr, **RUN_KW)
        root, label = _root(tc, "n5")
        holders = _holders(tc, AtomPointer(root.segment, root.offset))
        assert holders == {"n5", "n6"}, holders           # the committer and its successor
        for node in tc.ids:
            accepted = tc.storages[node].load_roots().accepted
            if node in ("n0", "n1", "n2"):
                assert accepted is not None
            else:
                assert accepted is None, f"learner {node} accepted a root"
        for node in tc.ids:
            assert tc.eventually(node, 3), f"{node} does not read the committed value"


def _count_syncs(monkeypatch):
    counter = {"n": 0}
    real = os.fsync
    lock = threading.Lock()

    def fsync(fd):
        with lock:
            counter["n"] += 1
        return real(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    return counter


@pytest.mark.parametrize("members", [3, 5, 8])
def test_syncs_per_commit_do_not_grow_with_the_cluster(tmp_path, monkeypatch, members):
    """Each commit costs at most the committer's append, ``f`` replica appends and one
    acceptance per voter: 1 + 1 + 3 = 5 syncs with three voters, at any size (plus slack for a
    checkpoint).  On d6743ae it grows with the members (one acceptance each)."""
    with _cluster(tmp_path, members) as tc:
        committer = tc.ids[-1]
        for _ in range(3):
            tc.db(committer).run(incr, **RUN_KW)
        for node in tc.ids:
            tc[node].wait_idle(5)
        counter = _count_syncs(monkeypatch)
        n = 20
        for _ in range(n):
            tc.db(committer).run(incr, **RUN_KW)
        time.sleep(0.2)
        for node in tc.ids:
            tc[node].wait_idle(5)
        per_commit = counter["n"] / n
        print(f"\n{members} members: {per_commit:.2f} syncs per commit")
        assert per_commit <= 6.5, f"{per_commit:.2f} syncs per commit with {members} members"


def test_a_voter_refuses_a_root_whose_records_lack_their_copies(tmp_path):
    """The acceptance rule of D54: a voter other than the committer accepts a root only when
    the replication names ``majority`` members holding the commit's records durably (itself
    included when the records ride in this message)."""
    with _cluster(tmp_path, 3) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        permission = tc["n0"].permit(timeout=5)
        try:
            n1 = tc["n1"]
            sent = []
            orig = n1.transport.send
            n1.transport.send = lambda to, msg: sent.append(msg) if msg["type"] == "ack" else orig(to, msg)
            base = permission.root
            new = RootRecord(FORMAT_VERSION, base.segment, base.offset + 1, base.epoch + 1)
            ranges = encode_ranges([(base.segment, base.offset, base.offset + 1)])

            def replicate(rid, copies):
                return make_message(
                    "replicate", "n0", term=permission.term, rid=rid, inc=tc["n0"]._inc,
                    root=encode_root(new, known=True, term=permission.term), committed=None, records=[], chunks=0,
                    base=None, base_ranges=None, seq=permission.seq,
                    pred=encode_root(base, known=True, term=permission.root_term), pred_committed=True,
                    vs=n1._fingerprint, copies=copies, ranges=ranges)

            n1._apply_replicate(replicate(990001, ["n0"]))
            assert [m["ok"] for m in sent] == [False], "accepted a root with one copy of its records"
            n1._apply_replicate(replicate(990002, ["n0", "n2"]))
            assert [m["ok"] for m in sent] == [False, True]
            n1.transport.send = orig
        finally:
            permission.outcome = "unknown"
            tc["n0"].finish(permission)


def test_a_dead_successor_does_not_stop_its_predecessors_commits(tmp_path):
    """n3's first data replica is n4; with n4 down the commit hedges to the next member."""
    with _cluster(tmp_path, 5) as tc:
        tc.db("n3").run(incr, **RUN_KW)
        tc.kill("n4")
        started = time.monotonic()
        for _ in range(5):
            tc.db("n3").run(incr, **RUN_KW)
        assert time.monotonic() - started < 5.0
        root, _ = _root(tc, "n3")
        holders = _holders(tc, AtomPointer(root.segment, root.offset))
        assert "n3" in holders and len(holders) >= 2, holders
        assert tc["n3"].stats()["replica_hedges"] >= 1


def test_reads_fall_back_to_another_replica_then_to_the_object_store(tmp_path):
    with _cluster(tmp_path, 5) as tc:
        for _ in range(4):
            tc.db("n3").run(incr, **RUN_KW)
        tc.wipe("n4")                       # a data replica lost after its acknowledgement
        tc.kill("n1")
        tc.revive("n1")                     # its record cache (R89) is empty after a restart
        assert tc.eventually("n1", 4)       # n1 holds no copy: it fetches from n3
        assert tc["n1"].stats()["fetches_requested"] > 0
        tc.spaces["n3"].sync(timeout=10)    # persisted: the object store holds every record
        tc.kill("n3")                       # the other copy is gone too
        tc.wipe("n2")
        tc.revive("n2")                     # an empty voter rejoins
        assert tc.eventually("n2", 4)


def test_a_learner_with_an_empty_disk_reads_any_committed_root(tmp_path):
    with _cluster(tmp_path, 8) as tc:
        for node in ("n0", "n3", "n5", "n7"):
            tc.db(node).run(incr, **RUN_KW)
        tc.wipe("n6")
        tc.revive("n6")
        assert tc["n6"].stats()["rejoins"] == 0
        assert tc.eventually("n6", 4)
        assert tc.storages["n6"].stats()["reads_peer"] + tc.storages["n6"].stats()["reads_remote"] > 0


def test_voter_failures_stop_writes_only_beyond_f_and_learners_keep_reading(tmp_path):
    with _cluster(tmp_path, 5) as tc:
        tc.db("n3").run(incr, **RUN_KW)
        tc.kill("n1")                       # f = 1 voter down: writes continue
        tc.db("n3").run(incr, **RUN_KW)
        tc.db("n4").run(incr, **RUN_KW)
        assert tc.eventually("n4", 3)
        tc.kill("n2")                       # f + 1 voters down: writes stop
        with pytest.raises((ProtoTimeoutError, ProtoConflictError, ProtoOutcomeUnknownError)):
            tc.db("n4").run(incr, retries=2, backoff=(0.001, 0.01))
        assert tc.value("n3") == 3          # learners still serve snapshot reads
        tc.revive("n1")                     # voters return: writes resume
        deadline = time.monotonic() + 15
        while True:
            try:
                tc.db("n4").run(incr, **RUN_KW)
                break
            except (ProtoTimeoutError, ProtoConflictError, ProtoOutcomeUnknownError):
                assert time.monotonic() < deadline, "writes did not resume"
        assert tc.eventually("n3", 4) or tc.eventually("n3", 5)


def test_two_lost_committers_with_different_replicas_are_both_persisted(tmp_path):
    """Repair through the object store (D54): two committers lost before shipping, whose
    records live on different data replicas, are both persisted.  No single member holds every
    missing segment any more; the walk of every persister follows the same chain of roots, so
    the replica of the first missing segment uploads it as a part, then the next one, and so on
    (``ensure_remote`` stops at the first segment it cannot upload)."""
    tc = TieredCluster(tmp_path, 8, storage_kw=dict(durability="fsync", ship_interval=600.0),
                       **dict(LEASE, persist_lag=0.2))
    with tc:
        tc.spaces["n0"].new_database("db")
        for node in ("n3", "n5", "n3", "n5"):
            tc.db(node).run(incr, **RUN_KW)
        assert tc.eventually("n0", 4)
        tc.kill("n3")                       # its records: n3 (lost) and n4
        tc.kill("n5")                       # its records: n5 (lost) and n6
        tc.spaces["n0"].sync(timeout=20)
        assert tc.storages["n4"].stats()["parts_uploaded"] >= 1
        assert tc.storages["n6"].stats()["parts_uploaded"] >= 1


def test_a_segment_split_by_a_hedge_is_persisted_after_its_committers_disk_is_lost(tmp_path):
    """Task 8.3g review I1: a hedge sends some of n0's commits to n2 instead of n1, so n0's
    segment is split between two replicas with holes (n1 lacks the middle, n2 the start).  After
    one disk loss (n0) neither copy is a prefix; the persister fills the holes from the peers,
    record by record, and uploads the segment's true prefix.  Before, persistence stalled at
    epoch 0 for good and the hedged commits stayed at ``f`` copies."""
    with TieredCluster(tmp_path, 3, storage_kw=dict(durability="fsync"), **LEASE) as tc:
        net = tc.cluster.net
        tc.spaces["n0"].new_database("db")
        tc.store.set_outage(True)
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)             # n0's records -> n0 + n1
        net.partition({"n0", "n2"}, {"n1"})
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)             # hedged: n0 + n2
        net.heal()
        time.sleep(LEASE["lease_duration"] * 1.6)       # the suspect period ends
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)             # n0 + n1 again
        assert tc.eventually("n1", 9) and tc.eventually("n2", 9)
        assert tc["n0"].stats()["replica_hedges"] >= 1
        tc.wipe("n0")                                   # ONE disk lost
        tc.store.set_outage(False)
        for node in ("n1", "n2"):
            tc.spaces[node].sync(timeout=20)
        assert sum(tc.storages[n].stats()["holes_filled"] for n in ("n1", "n2")) > 0
        tc.revive("n0")
        assert tc.eventually("n0", 9, timeout=10)


def test_every_member_caches_each_commit_without_a_durable_copy(tmp_path):
    """Ruling R89: a commit's records reach every member with the commit; a member that is not
    one of its data replicas keeps them only in its record cache (no append, no sync, no
    acknowledgement awaited), so it reads the commit without fetching."""
    with _cluster(tmp_path, 8) as tc:
        tc.db("n5").run(incr, **RUN_KW)
        for node in tc.ids:
            tc[node].wait_idle(5)
        before = {n: (tc[n].stats()["fetches_requested"], tc.storages[n].stats()["replicas_logged"])
                  for n in tc.ids}
        for _ in range(3):
            tc.db("n5").run(incr, **RUN_KW)
        for node in tc.ids:
            tc[node].wait_idle(5)
        root, _ = _root(tc, "n5")
        ptr = AtomPointer(root.segment, root.offset)
        assert _holders(tc, ptr) == {"n5", "n6"}                    # durable copies: f + 1
        for node in tc.ids:
            assert tc.storages[node].local_record(ptr) is not None, f"{node} did not cache the commit"
        for node in ("n1", "n3", "n7"):
            assert tc.eventually(node, 4)
            assert tc[node].stats()["fetches_requested"] == before[node][0], f"{node} fetched"
            assert tc.storages[node].stats()["replicas_logged"] == before[node][1]
        assert tc["n7"].stats()["records_warmed"] > 0
