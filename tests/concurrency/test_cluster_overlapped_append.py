"""The holder's own append overlaps the data phase (Task 8.3j, ruling R90; D50 and D54 amendments).

A permission holder sends its records to its data replicas at the moment it starts its own
synced append, so the two syncs overlap (a Raft leader's log write overlaps its
AppendEntries the same way).  The invariant is unchanged: the root goes alone to the other
voters, and the commit is acknowledged to its caller, only once the holder's own sync has
completed AND ``f`` data replicas have acknowledged ``stored``, so a committed root never
lacks ``f + 1`` durable copies of its records.

The cases:

* the overlap itself (fails before the change: the replica's append could only start after
  the holder's own append had returned);
* the root alone and the acknowledgement wait for the holder's own sync, even when the
  replica stored first; a failed own sync leaves the outcome unknown;
* the holder crashing, or losing power, at every point of the overlapped window: after the
  records left, after its replica accepted the root, with its own entry written but not
  synced, after its own sync, after the ``stored`` acknowledgement; whatever happens to the
  root, nothing acknowledged is lost, every member converges and reads it, and the object
  store ends up holding every record of the history (the restarted holder seals its own
  segment shorter than the bytes its replica holds; the replica's copy repairs it);
* the data replica crashing before and after ``stored`` while the holder's own sync runs;
* a delayed or duplicated ``stored`` (bound to its request and incarnation, protocol 12);
* random fault schedules under load at 3 and 5 members, with kills inside the overlapped
  window (``PROTODB_OVERLAP_RUNS`` schedules per size, default 10).
"""
import os
import random
import tempfile
import threading
import time

import pytest

from proto_db.cluster.protocol import decode_labeled_root
from proto_db.core.atom import AtomPointer
from proto_db.errors import ProtoDBError, ProtoOutcomeUnknownError
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.test_cluster_owner_failures import Load, _check, _FrozenPowerLoss
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(300)]

LEASE = dict(lease_duration=0.9, request_timeout=0.1)
SECONDS = 1.6
RUNS = int(os.environ.get("PROTODB_OVERLAP_RUNS", "10"))


def _cluster(tmp_path, n=3, *, storage_kw=None, **kw):
    tc = TieredCluster(tmp_path, n, storage_kw=dict(durability="fsync", **(storage_kw or {})), **dict(LEASE, **kw))
    tc.spaces[tc.ids[0]].new_database("db")
    return tc


def _is_data(msg) -> bool:
    return msg["type"] == "replicate" and bool(msg["records"])


def _is_root_alone(msg) -> bool:
    return msg["type"] == "replicate" and not msg["records"]


def write_z(tr):
    incr(tr)
    tr.set_root_object("z", tr.new_dictionary().set_at("k", 42))


def _values(tc, nodes) -> dict:
    out = {}
    for node in nodes:
        try:
            out[node] = tc.value(node)
        except Exception as e:          # reported in the assertion, never swallowed
            out[node] = f"{type(e).__name__}: {e}"
    return out


def _converge(tc, nodes, accept, timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while True:
        values = _values(tc, nodes)
        if len(set(map(str, values.values()))) == 1 and next(iter(values.values())) in accept:
            return next(iter(values.values()))
        assert time.monotonic() < deadline, f"the members did not converge on one of {accept}: {values}"
        time.sleep(0.02)


def _await_persisted(tc, timeout: float = 20.0) -> None:
    """Wait until the store's root is the newest committed root."""
    deadline = time.monotonic() + timeout
    while True:
        committed = {tc[n].committed_root()[1] for n in tc.ids}
        persisted = tc.storages[tc.ids[0]].read_persisted()
        if len(committed) == 1 and persisted is not None and persisted[1] in committed:
            return
        assert time.monotonic() < deadline, f"not persisted: store {persisted}, committed {committed}"
        time.sleep(0.05)


def _read_from_store(tc, tmp_path, expected: int, z: bool) -> None:
    """A fresh reader of the object store alone (no peer, no local copy) reads the history."""
    st = TieredStorage(tempfile.mkdtemp(dir=str(tmp_path)), tc.store)
    space = ObjectSpace(st)
    try:
        tr = space.open_database("db").new_transaction()
        try:
            assert tr.get_root_object("n") == expected
            if z:
                assert dict(tr.get_root_object("z").items()) == {"k": 42}
        finally:
            tr.abort()
    finally:
        space.close()


# -- the overlap and the invariant ---------------------------------------------------------------


def test_the_records_leave_for_the_data_replica_while_the_holders_own_append_runs(tmp_path):
    """Fails before Task 8.3j: the holder's own synced append returned before the replication
    was sent, so the replica's append could not start while it ran."""
    with _cluster(tmp_path, lease_duration=6.0) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        own, replica = tc.storages["n0"], tc.storages["n1"]     # n1: n0's successor, its data replica
        own_append, replica_append = own.accept_replication, replica.accept_replication
        started = threading.Event()
        seen = {}

        def holder(*a, **kw):
            seen["overlapped"] = started.wait(1.0)
            return own_append(*a, **kw)

        def data_replica(*a, **kw):
            started.set()
            return replica_append(*a, **kw)

        own.accept_replication, replica.accept_replication = holder, data_replica
        try:
            tc.db("n0").run(incr, **RUN_KW)
        except ProtoOutcomeUnknownError:
            pass                        # before the change: the 1 s wait may outlast the permission
        assert seen.get("overlapped"), "the replica's append did not start during the holder's own append"
        own.accept_replication, replica.accept_replication = own_append, replica_append
        tc.db("n0").run(incr, **RUN_KW)
        assert _converge(tc, tc.ids, {3}) == 3


def test_the_root_alone_and_the_acknowledgement_wait_for_the_holders_own_sync(tmp_path):
    """The replica stores and acknowledges first; the holder's own sync is slow.  The root
    goes alone to the third voter, and ``run`` returns, only after the own sync."""
    with _cluster(tmp_path, lease_duration=6.0) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        c0, own = tc["n0"], tc.storages["n0"]
        own_append = own.accept_replication
        replica_send = tc["n1"].transport.send
        stored = threading.Event()
        times = {}

        def n1_send(to, msg):
            replica_send(to, msg)
            if msg["type"] == "ack" and msg["stored"] and to == "n0":
                stored.set()

        def holder(*a, **kw):
            assert stored.wait(2.0), "the replica never stored while the holder's append waited"
            time.sleep(0.1)
            try:
                return own_append(*a, **kw)
            finally:
                times["own_synced"] = time.monotonic()

        send0 = c0.transport.send

        def n0_send(to, msg):
            if _is_root_alone(msg):
                times.setdefault("root_alone", time.monotonic())
            send0(to, msg)

        tc["n1"].transport.send = n1_send
        own.accept_replication = holder
        c0.transport.send = n0_send
        tc.db("n0").run(incr, **RUN_KW)
        returned = time.monotonic()
        tc["n1"].transport.send = replica_send
        own.accept_replication = own_append
        c0.transport.send = send0
        assert times["root_alone"] >= times["own_synced"]
        assert returned >= times["own_synced"]
        assert _converge(tc, tc.ids, {2}) == 2


def test_a_slow_own_sync_does_not_make_a_healthy_replica_a_suspect(tmp_path):
    """Task 8.3j review M1: the hedge deadline counts from the end of the holder's own append.
    The own sync takes 0.3 s and the replica stores in 0.35 s, within ``request_timeout``
    (0.25 s) of the own append's end.  On 8c8c193 the deadline ran from the send, so the
    replica was hedged and marked suspect for a lease duration."""
    with _cluster(tmp_path, lease_duration=6.0, request_timeout=0.25) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        own, replica = tc.storages["n0"], tc.storages["n1"]
        own_append, replica_append = own.accept_replication, replica.accept_replication

        def holder(*a, **kw):
            time.sleep(0.3)
            return own_append(*a, **kw)

        def data_replica(*a, **kw):
            time.sleep(0.35)
            return replica_append(*a, **kw)

        own.accept_replication, replica.accept_replication = holder, data_replica
        tc.db("n0").run(incr, **RUN_KW)
        own.accept_replication, replica.accept_replication = own_append, replica_append
        assert tc["n0"].stats()["replica_hedges"] == 0
        assert "n1" not in tc["n0"]._replica_suspects


def test_a_write_refused_by_a_healthy_tier_refuses_only_that_acceptance(tmp_path):
    """Task 8.3j re-review N1: a tier that refuses one write (``ProtoWriteRefusedError``: the
    records of an own segment of an earlier instance cannot be placed now) refuses that
    acceptance; the node keeps acting as an acceptor.  On aaa5a1d any tier error fail-stopped
    the node until the space was reopened."""
    from proto_db import errors
    refused = getattr(errors, "ProtoWriteRefusedError", errors.ProtoUnexpectedError)
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        st1 = tc.storages["n1"]
        append = st1.accept_replication
        calls = []

        def refuse_once(*a, **kw):
            if not calls:
                calls.append(1)
                raise refused("refused for the test")
            return append(*a, **kw)

        st1.accept_replication = refuse_once
        tc.db("n0").run(incr, **RUN_KW)             # hedged to n2
        st1.accept_replication = append
        assert calls, "n1 was never asked"
        assert tc["n1"]._tier_failure is None
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)
        assert _converge(tc, tc.ids, {5}) == 5
        assert tc["n1"]._tier_failure is None


def test_when_every_data_replica_refuses_the_commit_is_unknown_and_every_tier_stays_healthy(tmp_path):
    """Task 8.3j re-review 2 (test gap): n1 and n2 refuse every write that carries records
    (the acceptance and the stored-only fallback) with ``ProtoWriteRefusedError``.  The hedge
    finds no data replica: the caller gets ``ProtoOutcomeUnknownError``, never a commit, and
    no member's tier fails; once they store again the cluster converges on either outcome."""
    from proto_db import errors
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        reals, refusals = {}, {"n1": 0, "n2": 0}
        for node in ("n1", "n2"):
            st = tc.storages[node]
            real = reals[node] = st.accept_replication

            def refuse(*a, _real=real, _node=node, **kw):
                if kw.get("ranges") is not None or any(r[2] is not None for r in a[2]):
                    refusals[_node] += 1
                    raise errors.ProtoWriteRefusedError("refused for the test")
                return _real(*a, **kw)

            st.accept_replication = refuse
        with pytest.raises(ProtoOutcomeUnknownError):
            tc.db("n0").run(incr, retries=0)
        for node in ("n1", "n2"):
            tc.storages[node].accept_replication = reals[node]
        assert refusals["n1"] and refusals["n2"], f"the hedge did not reach both replicas: {refusals}"
        _no_tier_failed(tc)
        unknown = _commit_until_acknowledged(tc, "n0")
        _converge(tc, tc.ids, set(range(2, 4 + unknown)))
        _no_tier_failed(tc)


def test_a_failed_own_sync_is_never_acknowledged_even_when_the_replica_accepted(tmp_path):
    """The holder's own append fails after its replica stored and accepted the root: the
    commit's outcome is unknown, the root is never sent alone, and the cluster goes on
    (the failed tier is replaced by a restart)."""
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        c0, own = tc["n0"], tc.storages["n0"]
        own_append = own.accept_replication
        root_alone = []
        send0 = c0.transport.send
        state = {}

        def n0_send(to, msg):
            if _is_root_alone(msg):
                root_alone.append(to)
            if _is_data(msg) and "root" not in state:
                state["root"] = decode_labeled_root(msg["root"])[1]
            send0(to, msg)

        def holder(*a, **kw):
            deadline = time.monotonic() + 5.0
            while tc["n1"].accepted_root()[1] != state.get("root"):
                assert time.monotonic() < deadline, "the replica did not accept during the holder's append"
                time.sleep(0.001)
            raise OSError("the holder's own sync failed")

        own.accept_replication = holder
        c0.transport.send = n0_send
        with pytest.raises(ProtoOutcomeUnknownError):
            tc.db("n0").run(write_z, retries=0)
        assert not root_alone
        c0.transport.send = send0
        tc.kill("n0")
        tc.revive("n0")
        tc.db("n1").run(incr, **RUN_KW)
        value = _converge(tc, tc.ids, {2, 3})
        tc.db("n0").run(incr, **RUN_KW)
        assert _converge(tc, tc.ids, {value + 1}) == value + 1


# -- the holder dies inside the overlapped window ------------------------------------------------

POINTS = ["records sent", "replica stored", "own entry unsynced, power lost", "own sync done",
          "stored acknowledged"]


def _die_in_window(tc, fs, point, *, persist_after: bool, rolled: bool = False) -> None:
    """n0 commits a ``z`` dictionary and dies at ``point`` of its overlapped window (its data
    replica is its successor n1); then n0 is revived, its persister stopped unless
    ``persist_after``.  ``rolled``: n0's open segment is rolled first, as its persister does
    at every ``ship_interval``, so the window's records are the first ones of a fresh own
    segment: listed in n0's manifest at the first push, but with no file until the first
    write-out (Task 8.3j re-review N1)."""
    n0_dir = os.path.realpath(os.path.join(tc.path, "n0"))
    c0, st0 = tc["n0"], tc.storages["n0"]
    if c0.persister is not None:
        c0.persister.stop()                         # no background flush inside the window
    if rolled:
        st0.flush()
        st0._local.roll_segment()
    own_append = st0.accept_replication
    st1 = tc.storages["n1"]
    replica_append = st1.accept_replication
    send0 = c0.transport.send
    state = {}
    n0_dead = threading.Event()
    replica_may_store = threading.Event()

    def die():
        local = st0._local
        with local._state_lock:                     # the process dies: its unflushed buffer is gone
            local._pending, local._pending_bytes = [], 0
        tc.cluster.crash("n0")
        n0_dead.set()
        raise OSError("killed")

    def replica_stored():
        # Durable at n1: its synced entry precedes the copy write (with three voters, n1
        # also accepted the root in that entry).
        ptr = AtomPointer(state["root"].segment, state["root"].offset)
        deadline = time.monotonic() + 5.0
        while st1._local_record(ptr) is None:
            assert time.monotonic() < deadline, "the replica did not store during the holder's append"
            time.sleep(0.001)

    def holder(*a, **kw):
        if point == "records sent":
            die()
        if point == "replica stored":
            replica_stored()
            die()
        if point == "own entry unsynced, power lost":
            replica_stored()
            fs.frozen.add(n0_dir)                   # the power goes: nothing more becomes durable
            own_append(*a, **kw)
            die()
        own_append(*a, **kw)
        if point == "own sync done":
            die()
        return None

    def data_replica(*a, **kw):
        if point == "own sync done":
            assert replica_may_store.wait(5.0)
        return replica_append(*a, **kw)

    def n0_send(to, msg):
        if _is_data(msg) and "root" not in state:
            state["root"] = decode_labeled_root(msg["root"])[1]
            state["fresh"] = os.path.exists(os.path.join(n0_dir, st0._local.current_segment.hex + ".seg"))
        if point == "stored acknowledged" and _is_root_alone(msg):
            die()
        send0(to, msg)

    st0.accept_replication = holder
    st1.accept_replication = data_replica
    c0.transport.send = n0_send
    with pytest.raises(ProtoDBError):
        tc.db("n0").run(write_z, retries=0)
    assert n0_dead.is_set() and "root" in state, "the schedule did not run"
    if rolled:
        assert not state["fresh"], "the window's segment had a file before the window"
    tc.kill("n0")
    replica_may_store.set()
    st1.accept_replication = replica_append
    if point == "own entry unsynced, power lost":
        fs.power_loss(n0_dir)
    tc.revive("n0")
    if not persist_after:
        tc["n0"].persister.stop()


def _z_matches(tc, nodes, adopted) -> None:
    for node in nodes:
        tr = tc.db(node).new_transaction()
        try:
            z = tr.get_root_object("z")
            assert (dict(z.items()) if z is not None else None) == ({"k": 42} if adopted else None), node
        finally:
            tr.abort()


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd")
@pytest.mark.parametrize("point", POINTS)
def test_a_holder_dying_inside_the_overlapped_window_loses_nothing(tmp_path, monkeypatch, point):
    """n0 commits a ``z`` dictionary and dies at ``point``.  Whatever became of that root,
    every acknowledged commit survives, every member converges, n0 commits again, and once
    persisted the object store alone serves the whole history.  "replica stored" and the
    power-loss point are the shapes the overlap creates: n1 durably holds and accepts records
    of n0's own segment that n0 never made durable; restarted, n0 seals that segment shorter
    (8.3d fix round 6, ``test_a_holder_never_ships_own_records_it_has_not_made_durable_locally``)."""
    fs = _FrozenPowerLoss(monkeypatch)
    with _cluster(tmp_path) as tc:
        fs.writeback(str(tmp_path))
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)
        _die_in_window(tc, fs, point, persist_after=True)
        tc.db("n1").run(incr, **RUN_KW)             # a new term: n1's phase 1 may adopt the root
        value = _converge(tc, tc.ids, {4, 5})
        adopted = value == 5
        tc.db("n0").run(incr, **RUN_KW)
        value += 1
        assert _converge(tc, tc.ids, {value}) == value
        _z_matches(tc, tc.ids, adopted)
        _await_persisted(tc)
        _read_from_store(tc, tmp_path, value, adopted)
        if point in POINTS[:3]:
            # n0 never made the round durable: its restarted segment is short, and a replica's
            # copy repaired the store with a part (ruling R54 length rule, D50 amendment 8.3j).
            assert sum(tc.storages[n].stats()["parts_uploaded"] for n in tc.ids) >= 1


def _restart(tc, node) -> None:
    """A process restart (memory caches gone, disk kept), with the persister stopped."""
    tc.kill(node)
    tc.revive(node)
    tc[node].persister.stop()


def _commit_until_acknowledged(tc, node, attempts: int = 10) -> int:
    """Commit an increment on ``node`` until one is acknowledged; return the number of unknown
    outcomes met on the way (each may or may not have taken effect, ruling R86)."""
    unknown = 0
    for _ in range(attempts):
        try:
            tc.db(node).run(incr, **RUN_KW)
            return unknown
        except ProtoOutcomeUnknownError:
            unknown += 1
    pytest.fail(f"{node} got no commit acknowledged in {attempts} attempts")


def _read(tc, node):
    tr = tc.db(node).new_transaction()
    try:
        z = tr.get_root_object("z")
        return tr.get_root_object("n"), (dict(z.items()) if z is not None else None)
    finally:
        tr.abort()


def _no_tier_failed(tc) -> None:
    for node in tc.ids:
        assert tc[node]._tier_failure is None, (node, tc[node]._tier_failure)


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd")
@pytest.mark.parametrize("segment", ["open", "rolled"])
@pytest.mark.parametrize("voters", [3, 5])
@pytest.mark.parametrize("point", POINTS)
def test_after_a_death_in_the_window_f_disk_losses_lose_nothing(tmp_path, monkeypatch, point, voters, segment):
    """Task 8.3j review C1 and I1.  The object store is down and no persister runs, so the
    local copies are all there is.  n0 dies at ``point``, restarts and commits again (its
    phase 1 may re-propose the root of the window, re-carrying records of its own pre-crash
    segment); every node checkpoints and restarts (caches gone); then the ``f`` data replicas
    of n0 lose their disks.  The survivors still read every acknowledged commit.

    On 8c8c193, at "replica stored" with three voters, every member then failed with
    ``ProtoCorruptionError`` (no full copy of a record): n0 had counted as a copy of the
    re-carried records but never wrote them to a data file, and its checkpoint compacted the
    only entry holding them.

    ``segment="rolled"`` (Task 8.3j re-review N1): the window's records are the first ones of
    a fresh own segment, which had no file when n0 died.  On aaa5a1d n0 then took the segment
    for released, tried to restore it from the store, and its tier failed on every re-carry
    (``ProtoCorruptionError``: in no tier; or the store unreachable): a plain crash-restart
    fail-stopped it."""
    fs = _FrozenPowerLoss(monkeypatch)
    kw = {"voters": [f"n{i}" for i in range(voters)]} if voters != 3 else {}
    with _cluster(tmp_path, voters, storage_kw=dict(release_local=False), **kw) as tc:
        tc.store.set_outage(True)                   # local copies are all there is from here on
        fs.writeback(str(tmp_path))
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)
        for node in tc.ids:
            tc[node].persister.stop()
        _die_in_window(tc, fs, point, persist_after=False, rolled=segment == "rolled")
        unknown = _commit_until_acknowledged(tc, "n0")   # n0's term: phase 1 may re-propose the root
        _no_tier_failed(tc)
        value = _converge(tc, tc.ids, set(range(4, 6 + unknown)))
        adopted = _read(tc, "n0")[1] is not None
        if adopted and voters == 3 and point in POINTS[:3]:
            # The C1 shape: n0 re-proposed the root and its own pre-crash records came back.
            # After the power loss on a fresh segment its unsynced manifest entry is gone too:
            # n0 then holds the records as a copy (re-review N1).
            stats = tc.storages["n0"].stats()
            if segment == "rolled" and point == "own entry unsynced, power lost":
                assert stats["records_carried"] + stats["records_applied"] > 0
            else:
                assert stats["records_carried"] > 0
        for node in tc.ids:
            tc.storages[node].checkpoint()
        for node in tc.ids:
            _restart(tc, node)
        f = (voters - 1) // 2
        lost = [f"n{i}" for i in range(1, f + 1)]   # n0's data replicas, its successors
        for node in lost:
            tc.wipe(node)
        survivors = [n for n in tc.ids if n not in lost]
        expected = (value, {"k": 42} if adopted else None)
        for node in survivors:
            assert _read(tc, node) == expected, node
        for node in lost:
            tc.revive(node)
        unknown = _commit_until_acknowledged(tc, "n0")
        _converge(tc, tc.ids, set(range(value + 1, value + 2 + unknown)))
        _no_tier_failed(tc)


@pytest.mark.parametrize("segment", ["open", "rolled"])
def test_a_data_replica_holds_the_recarried_records_of_its_own_segment(tmp_path, monkeypatch, segment):
    """Task 8.3j review C1, data-replica variant: n0 dies after its replica n1 stored (and
    accepted) the root of the window; n2 owns the next term, re-proposes that root, and n0 --
    n2's successor, so its data replica -- stores records of n0's own pre-crash segment and
    acknowledges ``stored``.  After a checkpoint and a restart n0 must still hold them on disk
    (on 8c8c193 it held none: they were skipped as own records and compacted away).
    ``segment="rolled"``: a fresh own segment (re-review N1); on aaa5a1d n0's tier failed
    when the records came back."""
    fs = _FrozenPowerLoss(monkeypatch)
    with _cluster(tmp_path, storage_kw=dict(release_local=False)) as tc:
        tc.store.set_outage(True)
        fs.writeback(str(tmp_path))
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)
        for node in tc.ids:
            tc[node].persister.stop()
        root_of = {}
        send0 = tc["n0"].transport.send

        def n0_send(to, msg):
            if _is_data(msg) and "root" not in root_of:
                root_of["root"] = decode_labeled_root(msg["root"])[1]
                root_of["ranges"] = msg["ranges"]
            send0(to, msg)

        tc["n0"].transport.send = n0_send
        _die_in_window(tc, fs, "replica stored", persist_after=False, rolled=segment == "rolled")
        from proto_db.cluster.protocol import decode_ranges
        ranges = decode_ranges(root_of["ranges"])
        stored = tc.storages["n0"]
        ptrs = [AtomPointer(seg, start) for seg, start, _ in ranges if seg in stored._own]
        assert ptrs, "the window wrote no record of n0's own segment"
        assert all(stored._local_record(p) is None for p in ptrs), "n0 kept window records"
        tc.db("n2").run(incr, **RUN_KW)             # n2's term re-proposes the root, n0 its replica
        assert _converge(tc, tc.ids, {4, 5}) == 5, "the root of the window was not re-proposed"
        _no_tier_failed(tc)
        tc.storages["n0"].checkpoint()
        _restart(tc, "n0")
        stored = tc.storages["n0"]
        missing = [p for p in ptrs if stored._local_record(p) is None]
        assert not missing, f"n0 acknowledged stored but holds none of {len(missing)} records on disk"


# -- the data replica dies while the holder's own sync runs ---------------------------------------


@pytest.mark.parametrize("when", ["before stored", "after stored"])
def test_a_data_replica_dying_during_the_holders_own_sync(tmp_path, when):
    """n1 (n0's data replica) dies before storing n0's records (the commit hedges to n2) or
    right after acknowledging them, while n0's own append runs.  The commit is acknowledged
    with two durable copies: after the replica comes back, losing n0's disk loses nothing."""
    with _cluster(tmp_path) as tc:
        tc.store.set_outage(True)                   # nothing is persisted: copies are all there is
        tc.db("n0").run(incr, **RUN_KW)
        own = tc.storages["n0"]
        own_append = own.accept_replication
        replica_send = tc["n1"].transport.send
        killed = threading.Event()

        def kill_n1():
            if not killed.is_set():
                killed.set()
                threading.Thread(target=tc.kill, args=("n1",), daemon=True).start()

        def n1_send(to, msg):
            replica_send(to, msg)
            if when == "after stored" and msg["type"] == "ack" and msg["stored"] and to == "n0":
                kill_n1()

        def holder(*a, **kw):
            if when == "before stored":
                kill_n1()
            else:
                assert killed.wait(5.0)
            time.sleep(0.05)
            return own_append(*a, **kw)

        tc["n1"].transport.send = n1_send
        own.accept_replication = holder
        tc.db("n0").run(incr, **RUN_KW)
        own.accept_replication = own_append
        assert killed.is_set()
        time.sleep(0.2)
        tc.revive("n1")
        assert _converge(tc, tc.ids, {2}) == 2
        tc.wipe("n0")                               # ONE disk lost (f = 1)
        assert _converge(tc, ["n1", "n2"], {2}) == 2
        tc.revive("n0")
        tc.db("n0").run(incr, **RUN_KW)
        assert _converge(tc, tc.ids, {3}) == 3


# -- delayed and duplicated ``stored`` ------------------------------------------------------------


def test_a_delayed_stored_of_an_earlier_commit_is_never_a_copy_of_the_next_one(tmp_path):
    """n1's ``stored`` for n0's commit k is held in the network and delivered while commit
    k+1's own append runs; k+1's records never reach n1.  The stale acknowledgement answers
    another request (protocol 12 binds it to its request id and incarnation): k+1 hedges to
    n2, and losing n0's disk afterwards loses nothing."""
    with _cluster(tmp_path) as tc:
        tc.store.set_outage(True)
        tc.db("n0").run(incr, **RUN_KW)
        replica_send = tc["n1"].transport.send
        held = []

        def n1_send(to, msg):
            if not held and to == "n0" and msg["type"] == "ack" and msg["stored"]:
                held.append(msg)                    # delayed in the network
            replica_send(to, msg)                   # ... and duplicated later

        tc["n1"].transport.send = n1_send
        tc.db("n0").run(incr, **RUN_KW)             # commit k
        tc["n1"].transport.send = replica_send
        assert held
        c0, own = tc["n0"], tc.storages["n0"]
        own_append = own.accept_replication
        send0 = c0.transport.send
        delivered = []

        def n0_send(to, msg):
            if to == "n1" and msg["type"] in ("replicate", "records", "warm"):
                return                              # k+1 never reaches n1
            send0(to, msg)

        def holder(*a, **kw):
            if not delivered:
                delivered.append(True)
                replica_send("n0", held[0])         # the delayed copy of k's stored arrives now
                replica_send("n0", dict(held[0], inc=c0._inc))
                time.sleep(0.05)
            return own_append(*a, **kw)

        c0.transport.send = n0_send
        own.accept_replication = holder
        tc.db("n0").run(incr, **RUN_KW)             # commit k+1
        c0.transport.send = send0
        own.accept_replication = own_append
        assert delivered
        tc.wipe("n0")
        assert _converge(tc, ["n1", "n2"], {3}) == 3


def test_a_stale_stored_of_a_previous_incarnation_arriving_during_the_own_append(tmp_path):
    """The Task 8.3g C1 schedule with the stale ``ack`` delivered inside the overlapped
    window: n2's incarnation A commits (n0 stores, its ``stored`` is delayed); incarnation B
    commits with the colliding request id while its own append runs, and B's records never
    reach n0.  The stale reply is dropped; B hedges to n1; losing n2's disk loses nothing."""
    with _cluster(tmp_path) as tc:
        tc.store.set_outage(True)
        tc.db("n0").run(incr, **RUN_KW)
        tc.kill("n2")
        tc.revive("n2")
        send0 = tc["n0"].transport.send
        held = []

        def n0_send(to, msg):
            if not held and to == "n2" and msg["type"] == "ack" and msg["stored"]:
                held.append(msg)
                return
            send0(to, msg)

        tc["n0"].transport.send = n0_send
        tc.db("n2").run(incr, **RUN_KW)             # incarnation A
        assert held
        tc["n0"].transport.send = send0
        tc.kill("n2")
        tc.revive("n2")                             # incarnation B: request ids restart
        c2, own = tc["n2"], tc.storages["n2"]
        send2 = c2.transport.send
        own_append = own.accept_replication
        rid = {}

        def n2_send(to, msg):
            if to == "n0" and _is_data(msg) and "rid" not in rid:
                rid["rid"], rid["term"] = msg["rid"], msg["term"]
                return                              # B's records never reach n0
            send2(to, msg)

        def holder(*a, **kw):
            if "rid" in rid and "fired" not in rid:
                rid["fired"] = True
                send0("n2", dict(held[0], rid=rid["rid"], term=rid["term"]))
                time.sleep(0.05)
            return own_append(*a, **kw)

        c2.transport.send = n2_send
        own.accept_replication = holder
        tc.db("n2").run(incr, **RUN_KW)
        assert rid.get("fired"), "the schedule did not run"
        stale = tc["n2"].stats().get("stale_replies", 0)
        c2.transport.send = send2
        own.accept_replication = own_append
        tc.wipe("n2")
        assert _converge(tc, ["n0", "n1"], {3}) == 3
        assert stale >= 1


# -- safety under load --------------------------------------------------------------------------


@pytest.mark.parametrize("n", [3, 5])
@pytest.mark.parametrize("seed", range(RUNS))
def test_no_acknowledged_commit_is_lost_with_kills_inside_the_overlapped_window(tmp_path, n, seed):
    """Every member commits (two threads each); the links duplicate and delay messages.  The
    first fault of every schedule kills a member inside its own overlapped window (after its
    records left, before or right after its own append) and the schedule fails if no member
    reached its window in time (Task 8.3j review I1: every schedule counts); a second fault,
    drawn at random, kills, partitions or kills in the window again.  Ruling R86 bounds:
    ``acked <= n <= acked + unknown``."""
    rng = random.Random(seed * 7919 + n)
    with _cluster(tmp_path, n) as tc:
        net = tc.cluster.net
        net.set_faults(duplicate=rng.choice([0.0, 0.1, 0.3]), delay=(0.0, rng.choice([0.0, 0.004, 0.02])))
        in_window = 0
        with Load(tc) as load:

            def kill_in_window(victim) -> bool:
                storage = tc.storages.get(victim)
                if storage is None:
                    return False
                append = storage.accept_replication
                fired = threading.Event()
                after = rng.random() < 0.5

                def hooked(*a, **kw):
                    if threading.current_thread().name.startswith("proto-cluster") or fired.is_set():
                        return append(*a, **kw)         # an acceptor's append, not the holder's
                    fired.set()
                    load.down.add(victim)
                    if after:
                        append(*a, **kw)
                    tc.cluster.crash(victim)
                    raise OSError("killed inside the overlapped window")

                storage.accept_replication = hooked
                if not fired.wait(SECONDS):
                    storage.accept_replication = append
                    return False
                tc.kill(victim)
                time.sleep(rng.uniform(0.05, LEASE["lease_duration"] * 1.5))
                tc.revive(victim)
                load.down.discard(victim)
                return True

            events = ["window"] + [rng.choice(["window", "kill", "partition"]) for _ in range(rng.randint(0, 1))]
            for i, how in enumerate(events):
                time.sleep(rng.uniform(0.1, SECONDS / 3))
                if how == "window":
                    candidates = rng.sample(tc.ids, len(tc.ids))
                    fired = False
                    for victim in candidates:
                        if kill_in_window(victim):
                            fired = True
                            break
                    if fired:
                        in_window += 1
                    else:
                        assert i > 0, "no member reached its overlapped window: the schedule did not run"
                    continue
                victim = rng.choice(tc.ids)
                if how == "kill":
                    load.down.add(victim)
                    tc.kill(victim)
                    time.sleep(rng.uniform(0.05, LEASE["lease_duration"] * 1.5))
                    tc.revive(victim)
                    load.down.discard(victim)
                else:
                    net.partition({victim}, set(tc.ids) - {victim})
                    time.sleep(rng.uniform(0.05, LEASE["lease_duration"] * 1.5))
                    net.heal()
            time.sleep(SECONDS / 3)
        assert in_window >= 1
        print(f"\n{n} members, seed {seed}: {in_window} kill(s) inside the overlapped window")
        _check(tc, load, tc.ids)
