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
from proto_db.errors import ProtoDBError, ProtoOutcomeUnknownError
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.test_cluster_owner_failures import Load, _check, _FrozenPowerLoss
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(300)]

LEASE = dict(lease_duration=0.9, request_timeout=0.1)
SECONDS = 1.6
RUNS = int(os.environ.get("PROTODB_OVERLAP_RUNS", "10"))


def _cluster(tmp_path, n=3, **kw):
    tc = TieredCluster(tmp_path, n, storage_kw=dict(durability="fsync"), **dict(LEASE, **kw))
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

POINTS = ["records sent", "replica accepted", "own entry unsynced, power lost", "own sync done",
          "stored acknowledged"]


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd")
@pytest.mark.parametrize("point", POINTS)
def test_a_holder_dying_inside_the_overlapped_window_loses_nothing(tmp_path, monkeypatch, point):
    """n0 commits a ``z`` dictionary and dies at ``point``.  Whatever became of that root,
    every acknowledged commit survives, every member converges, n0 commits again, and once
    persisted the object store alone serves the whole history.  "replica accepted" and the
    power-loss point are the shapes the overlap creates: n1 durably holds and accepts records
    of n0's own segment that n0 never made durable; restarted, n0 seals that segment shorter
    (8.3d fix round 6, ``test_a_holder_never_ships_own_records_it_has_not_made_durable_locally``)."""
    fs = _FrozenPowerLoss(monkeypatch)
    n0_dir = os.path.realpath(os.path.join(str(tmp_path), "n0"))
    with _cluster(tmp_path) as tc:
        fs.writeback(str(tmp_path))
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)
        c0, st0 = tc["n0"], tc.storages["n0"]
        c0.persister.stop()                         # no background flush inside the window
        own_append = st0.accept_replication
        replica_append = tc.storages["n1"].accept_replication
        send0 = c0.transport.send
        state = {}
        n0_dead = threading.Event()
        replica_may_store = threading.Event()

        def die():
            local = st0._local
            with local._state_lock:                 # the process dies: its unflushed buffer is gone
                local._pending, local._pending_bytes = [], 0
            tc.cluster.crash("n0")
            n0_dead.set()
            raise OSError("killed")

        def replica_accepted():
            deadline = time.monotonic() + 5.0
            while tc["n1"].accepted_root()[1] != state["root"]:
                assert time.monotonic() < deadline, "the replica did not accept during the holder's append"
                time.sleep(0.001)

        def holder(*a, **kw):
            if point == "records sent":
                die()
            if point == "replica accepted":
                replica_accepted()
                die()
            if point == "own entry unsynced, power lost":
                replica_accepted()
                fs.frozen.add(n0_dir)               # the power goes: nothing more becomes durable
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
            if point == "stored acknowledged" and _is_root_alone(msg):
                die()
            send0(to, msg)

        st0.accept_replication = holder
        tc.storages["n1"].accept_replication = data_replica
        c0.transport.send = n0_send
        with pytest.raises(ProtoDBError):
            tc.db("n0").run(write_z, retries=0)
        assert n0_dead.is_set() and "root" in state, "the schedule did not run"
        tc.kill("n0")
        replica_may_store.set()
        tc.storages["n1"].accept_replication = replica_append
        if point == "own entry unsynced, power lost":
            fs.power_loss(n0_dir)
        tc.revive("n0")
        tc.db("n1").run(incr, **RUN_KW)             # a new term: n1's phase 1 may adopt the root
        value = _converge(tc, tc.ids, {4, 5})
        adopted = value == 5
        tc.db("n0").run(incr, **RUN_KW)
        value += 1
        assert _converge(tc, tc.ids, {value}) == value
        for node in tc.ids:
            tr = tc.db(node).new_transaction()
            try:
                z = tr.get_root_object("z")
                assert (dict(z.items()) if z is not None else None) == ({"k": 42} if adopted else None), node
            finally:
                tr.abort()
        _await_persisted(tc)
        _read_from_store(tc, tmp_path, value, adopted)
        if point in POINTS[:3]:
            # n0 never made the round durable: its restarted segment is short, and a replica's
            # copy repaired the store with a part (ruling R54 length rule, D50 amendment 8.3j).
            assert sum(tc.storages[n].stats()["parts_uploaded"] for n in tc.ids) >= 1


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
    """Every member commits (two threads each); the links duplicate and delay messages; one or
    two members are killed at random instants, some of them inside their own overlapped
    window (between sending their records and the end of their own append), others
    partitioned and healed.  Ruling R86 bounds: ``acked <= n <= acked + unknown``."""
    rng = random.Random(seed * 7919 + n)
    with _cluster(tmp_path, n) as tc:
        net = tc.cluster.net
        net.set_faults(duplicate=rng.choice([0.0, 0.1, 0.3]), delay=(0.0, rng.choice([0.0, 0.004, 0.02])))
        with Load(tc) as load:
            for _ in range(rng.randint(1, 2)):
                time.sleep(rng.uniform(0.1, SECONDS / 3))
                victim = rng.choice(tc.ids)
                how = rng.choice(["window", "kill", "partition"])
                if how == "window":
                    storage = tc.storages.get(victim)
                    if storage is None:
                        continue
                    append = storage.accept_replication
                    fired = threading.Event()
                    after = rng.random() < 0.5

                    def hooked(*a, _append=append, _fired=fired, _after=after, _victim=victim, **kw):
                        if threading.current_thread().name.startswith("proto-cluster") or _fired.is_set():
                            return _append(*a, **kw)        # an acceptor's append, not the holder's
                        _fired.set()
                        load.down.add(_victim)
                        if _after:
                            _append(*a, **kw)
                        tc.cluster.crash(_victim)
                        raise OSError("killed inside the overlapped window")

                    storage.accept_replication = hooked
                    if not fired.wait(SECONDS):
                        storage.accept_replication = append
                        continue
                    tc.kill(victim)
                    time.sleep(rng.uniform(0.05, LEASE["lease_duration"] * 1.5))
                    tc.revive(victim)
                    load.down.discard(victim)
                elif how == "kill":
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
        _check(tc, load, tc.ids)
