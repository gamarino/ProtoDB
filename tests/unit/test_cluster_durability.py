"""Durability and robustness of the cluster commit point (8.3d review fix round 1, D46).

* A commit on a shared FileStorage is in the acceptor logs of a majority when it returns: it
  survives a crash of the whole cluster before any persister pass (review I1).
* After a refound, a restarted node loads the new lineage from its log, never the dead
  branch whose epochs are higher (review I2).
* A commit whose records exceed one frame is split into chunks; a record that could never
  be replicated is refused when it is saved, so the node keeps committing (review I3).
* An unexpected exception in the worker fences the node: logged, reported by the health
  metric, no more grants or acknowledgements, and the messages still queued or arriving
  are refused, so the node reports idle (review M3, re-review N5).
* An unexpected exception in the persister stops persisting only: the node reports itself
  unhealthy but keeps granting and acknowledging (re-review N2, ruling R47c).
"""
import time

import pytest

from proto_db.cluster import protocol
from proto_db.cluster.coordinator import REFOUND_TERM_STEP
from proto_db.errors import ProtoUnexpectedError, ProtoValidationError
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.support.cluster import Cluster
from tests.support.tiered_cluster import FAST, RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]


def _value(space, name="n"):
    tr = space.open_database("db").new_transaction()
    try:
        return tr.get_root_object(name)
    finally:
        tr.abort()


#: The fenced thread re-raises its exception, which pytest reports as this warning.
_THREAD_DIES = pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")


def test_a_shared_file_storage_commit_survives_a_whole_cluster_crash_before_any_persistence(tmp_path):
    path = str(tmp_path / "space")
    never = dict(FAST, persist_interval=60.0, persist_lag=60.0)     # the persister never writes
    storages = []
    with Cluster(3, **never) as cl:
        storages = [FileStorage(path, durability="fsync") for _ in cl.ids]    # one per process
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
        spaces[0].new_database("db")
        spaces[1].open_database("db").run(incr, **RUN_KW)
        assert all(s.persisted_epoch == 0 for s in spaces)
        reader = FileStorage(path, durability="none")
        assert reader.read_root() is None                  # nothing persisted: the storage root is empty
        reader.close()
        for n in cl.ids:
            tier = cl[n].tier
            cl[n].stop()                                   # the whole cluster dies: no close, no persister pass
            # A dead process's descriptors and locks die with it; here the acceptor log's
            # descriptor (and its lock, ruling R59) is closed by hand, writing nothing.
            tier.close()
    for storage in storages:
        storage.close()                                    # file descriptors only; publishes no root
    # Restarted on their logs: the default startup grace (a shorter one is for a cluster's first
    # start only).
    with Cluster(3, **dict(FAST, startup_grace=None)) as cl:
        spaces = [ObjectSpace(FileStorage(path, durability="fsync"), coordinator=cl[n]) for n in cl.ids]
        try:
            assert _value(spaces[2]) == 1                  # recovered from the nodes' acceptor logs
            spaces[2].sync(timeout=10)
        finally:
            for space in spaces:
                space.close()
    reader = ObjectSpace(FileStorage(path, durability="none"))
    try:
        assert _value(reader) == 1
    finally:
        reader.close()


def test_a_node_restarted_after_a_refound_never_reloads_the_dead_branch(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        for i in range(4):
            tc.db(tc.ids[i % 3]).run(incr, **RUN_KW)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        tc.store.set_outage(True)
        for i in range(5):
            tc.db(("n0", "n1")[i % 2]).run(incr, **RUN_KW)  # a branch of 9 never persisted
        assert tc.eventually("n1", 9)
        tc.wipe("n0")
        tc.wipe("n2")                                       # a majority of the disks is lost
        tc.cluster.net.partition({"n0", "n2"}, {"n1"})      # the survivor misses the refound
        # ... and cannot reach the store either (else it persists its newest root, the
        # committed state, and there is no dead branch: a race this test used to win by timing).
        tc["n1"].persister.stop()
        tc.store.set_outage(False)
        tc.revive("n0", force_rejoin=True)                  # the operator's disaster recovery
        tc.revive("n2", force_rejoin=True)
        assert tc.eventually("n0", 4)                       # refounded from the persisted state
        tc.db("n2").run(incr, **RUN_KW)
        tc.cluster.net.heal()
        tc.db("n2").run(incr, **RUN_KW)                     # its release teaches n1 the new lineage
        assert tc.eventually("n1", 6)
        dead_epoch = 10
        n1 = "n1"
        assert tc[n1].committed_root()[1].epoch < dead_epoch
        tc.kill(n1)
        tc.revive(n1)
        known, record, label = tc[n1].committed_root()[:3]   # loaded from its log, before any read
        assert known and label > REFOUND_TERM_STEP and record.epoch < dead_epoch
        assert tc.value(n1) == 6


def test_a_commit_larger_than_a_frame_is_chunked_and_an_unreplicable_record_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(protocol, "MAX_FRAME_BYTES", 64 * 1024)
    monkeypatch.setattr(protocol, "CHUNK_BYTES", 16 * 1024)
    monkeypatch.setattr(protocol, "MAX_RECORD_BYTES", 8 * 1024)
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")

        def big(tr):
            for i in range(60):
                tr.set_root_object(f"k{i:02d}", "x" * 3000)

        tc.db("n0").run(big, **RUN_KW)                      # ~180 KiB of records: several frames
        before = tc.storages["n1"].stats()["records_applied"]
        assert before > 0
        tc.db("n0").run(incr, **RUN_KW)
        assert tc.eventually("n1", 1)
        tr = tc.db("n2").new_transaction()
        assert tr.get_root_object("k59") == "x" * 3000
        tr.abort()
        with pytest.raises(ProtoValidationError, match="exceeds"):
            tc.db("n0").run(lambda tr: tr.set_root_object("huge", "y" * 20_000), **RUN_KW)
        tc.db("n0").run(incr, **RUN_KW)                     # the node still commits
        assert tc.eventually("n2", 2)


@_THREAD_DIES
def test_an_unexpected_worker_failure_fences_the_node(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        tc.db("n0").run(incr, **RUN_KW)
        assert tc["n1"].healthy and tc["n1"].stats()["healthy"]

        def broken(*args, **kwargs):
            raise RuntimeError("disk controller on fire")

        tc.storages["n1"].apply = broken
        tc.storages["n1"].accept_replication = broken       # the acceptance path (Task 8.3f phase B)
        tc.db("n0").run(incr, **RUN_KW)                     # n0 and n2 still form a majority
        assert tc.eventually("n0", 2)
        assert _wait_for(lambda: not tc["n1"].healthy)
        stats = tc["n1"].stats()
        assert not stats["healthy"] and "disk controller on fire" in stats["failure"]
        assert _wait_for(lambda: not tc["n1"]._worker.is_alive())      # re-raised: the thread failed loudly
        grants = stats["grants_given"]
        for _ in range(3):
            tc.db("n2").run(incr, **RUN_KW)
        assert tc["n1"].stats()["grants_given"] == grants  # a fenced node neither grants ...
        assert tc["n1"].stats()["accepted"] == stats["accepted"]   # ... nor acknowledges
        with pytest.raises(ProtoUnexpectedError, match="fenced"):
            tc["n1"].acquire(timeout=1)
        # N5: the replications and releases sent to it since are refused or dropped, never
        # queued for a worker that is gone: the node reports idle.
        assert tc["n1"].wait_idle(timeout=2.0)
        assert tc["n1"]._queued == 0 and tc["n1"]._jobs.empty()


@_THREAD_DIES
def test_an_unexpected_persister_failure_stops_persisting_but_not_voting(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")

        def broken(force=False):
            raise RuntimeError("upload queue corrupted")

        tc.storages["n2"].ship = broken
        tc.db("n0").run(incr, **RUN_KW)
        assert _wait_for(lambda: not tc["n2"].healthy)
        stats = tc["n2"].stats()
        assert "upload queue corrupted" in stats["failure"]
        assert not stats["healthy"] and not stats["persisting"] and not stats["fenced"]
        assert _wait_for(lambda: not tc["n2"].persister._thread.is_alive())
        # Persisting is off the commit path: with n1 cut off, every lease and every commit
        # needs n2's grant and acknowledgement, and gets them.
        tc.cluster.net.partition({"n0", "n2"}, {"n1"})
        accepted = tc["n2"].stats()["accepted"]
        tc.db("n0").run(incr, **RUN_KW)
        assert tc["n2"].stats()["accepted"] > accepted
        tc.db("n2").run(incr, **RUN_KW)                     # and it still acquires
        assert tc.eventually("n0", 3)
        with pytest.raises(ProtoUnexpectedError, match="persister failed"):
            tc.spaces["n2"].sync(timeout=5)                 # fails at once, never a 5 s wait


def _wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def test_a_base_with_the_snapshot_epoch_but_another_root_is_validated():
    """Review I2b: after a refound a dead-branch snapshot and the new lineage's base can share
    an epoch.  The commit must validate against the base, not trust the snapshot."""
    from proto_db.errors import ProtoConflictError
    from proto_db.storage.format import FORMAT_VERSION, RootRecord
    from proto_db.storage.memory import MemoryStorage
    storage = MemoryStorage()
    space = ObjectSpace(storage)
    db = space.new_database("db")
    other = storage.read_root()                          # a root without "n"
    db.run(incr)
    tr = db.new_transaction()
    incr(tr)                                             # reads n == 1 from its snapshot
    snapshot = storage.read_root()
    same_epoch_other_root = RootRecord(FORMAT_VERSION, other.segment, other.offset, snapshot.epoch)
    with pytest.raises(ProtoConflictError):
        tr._build_root(same_epoch_other_root)
    tr.abort()
    space.close()


def test_a_verified_dead_branch_root_never_shortens_the_persisters_walk(tmp_path):
    """Review I2c: a root verified on a dead branch (with a higher epoch) must not stop the walk
    over the new lineage early, or a root whose records are not all remote gets persisted."""
    from proto_db.cluster.rootlog import root_key
    from proto_db.storage.format import FORMAT_VERSION, RootRecord
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        tc.db("n0").run(incr, **RUN_KW)
        tc.spaces["n0"].sync(timeout=10)
        tc.store.set_outage(True)
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)                 # three commits not persisted
        persister = tc["n0"].persister
        persister.stop()
        known, record, label = tc["n0"].committed_root()[:3]
        dead = RootRecord(FORMAT_VERSION, record.segment, record.offset + 1, 99)     # a dead-branch root
        persister._verified.add(root_key(label - 1, dead))
        storage = tc.storages["n0"]
        walked = []
        original = storage.commit_ranges
        storage.commit_ranges = lambda root: walked.append(root) or original(root)
        storage.ensure_remote = lambda needs, upload_copies=False: False
        assert not persister._persistable(label, record, upload_copies=False)
        assert len(walked) == 3                             # every unpersisted commit is checked


def test_a_commit_racing_the_close_of_the_space_has_an_unknown_outcome(tmp_path):
    """Re-review N4: once the root is sent, the peers may acknowledge it, so a failure of the
    committer's own log (the space closing under the commit) is ``ProtoOutcomeUnknownError``,
    never a validation error that claims nothing took effect."""
    import uuid
    from proto_db.cluster.persister import PlainTier
    from proto_db.errors import ProtoOutcomeUnknownError
    from proto_db.storage.format import FORMAT_VERSION, RootRecord
    with Cluster(3, **FAST) as cl:
        tiers = {n: PlainTier(FileStorage(str(tmp_path / "space"), durability="none"), n) for n in cl.ids}
        for n in cl.ids:
            cl[n].attach(tiers[n])
        cl.wait_rejoined()
        lease = cl["n0"].acquire(timeout=5)
        transport = cl["n0"].transport
        send = transport.send

        def send_then_close(node_id, message):
            send(node_id, message)
            if message["type"] == "replicate":
                tiers["n0"].close()                       # ObjectSpace.close closes the log

        transport.send = send_then_close
        root = RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 1)
        with pytest.raises(ProtoOutcomeUnknownError) as info:
            cl["n0"].replicate(lease, root)
        assert isinstance(info.value.__cause__, ProtoValidationError)
        assert _wait_for(lambda: cl["n1"].accepted_root()[1] == root)    # it did reach the peers
        assert cl["n0"].stats()["replication_failures"] == 1
        transport.send = send
        cl["n0"].release(lease)
        for n in ("n1", "n2"):
            tiers[n].close()


def test_a_holder_never_ships_own_records_it_has_not_made_durable_locally(tmp_path):
    """Found by the fix round 6 loops (SIGKILL crash test, ``-X gil=1``): the holder sent its
    own records with the replication and flushed them to its own segment only afterwards.  A
    holder killed in between kept a strict prefix of its own segment, while its peers held the
    rest as copies (and accepted the root).  Restarted, it sealed and uploaded that prefix as
    the whole segment, so the object store "held" the segment; a later holder re-proposed the
    root as its base, shipped none of its records (the store holds them), and the restarted
    node read the store's short object: ``ProtoCorruptionError`` ("Truncated record header"),
    and the peers' copies were the only remaining bytes.  Simulated in process: the crash
    drops the holder's unflushed buffer once both peers accepted the root."""
    from proto_db.cluster.protocol import decode_labeled_root
    from proto_db.errors import ProtoDBError
    tc = TieredCluster(tmp_path, 3)
    try:
        tc.spaces["n0"].new_database("db")
        for _ in range(3):
            tc.db("n0").run(incr, **RUN_KW)
        c0, st0 = tc["n0"], tc.storages["n0"]
        c0.persister.stop()                         # no background flush in the crash window
        original = c0._broadcast
        state = {}

        def dies_after_broadcast(msg):
            original(msg)
            if msg["type"] != "replicate" or state:
                return
            state["root"] = decode_labeled_root(msg["root"])[1]
            deadline = time.monotonic() + 10.0
            while not all(tc[n].accepted_root()[1] == state["root"] for n in ("n1", "n2")):
                assert time.monotonic() < deadline, "the peers did not accept the root"
                time.sleep(0.001)
            local = st0._local
            with local._state_lock:                 # the process dies: its unflushed buffer is gone
                local._pending, local._pending_bytes = [], 0
            tc.cluster.crash("n0")
            raise OSError("killed")

        def write_z(tr):
            incr(tr)
            tr.set_root_object("z", tr.new_dictionary().set_at("k", 42))

        c0._broadcast = dies_after_broadcast
        with pytest.raises(ProtoDBError):
            tc.db("n0").run(write_z, retries=0)
        root = state["root"]
        tc.kill("n0")
        tc.revive("n0")
        st0 = tc.storages["n0"]
        deadline = time.monotonic() + 10.0
        while root.segment not in st0._remote:      # n0 ships its own segments after the restart
            assert time.monotonic() < deadline, "n0 never shipped its segment"
            time.sleep(0.01)
        for n in ("n1", "n2"):
            tc.storages[n].refresh_remote()         # the peers learn what the store holds
        tc.db("n1").run(incr, **RUN_KW)             # n1 builds on the root a majority accepted
        for n in ("n1", "n2", "n0"):
            tr = tc.db(n).new_transaction()
            try:
                assert dict(tr.get_root_object("z").items()) == {"k": 42}, n
                assert tr.get_root_object("n") == 5, n
            finally:
                tr.abort()
    finally:
        tc.close()
