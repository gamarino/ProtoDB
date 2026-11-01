"""Durability and robustness of the cluster commit point (8.3d review fix round 1, D46).

* A commit on a shared FileStorage is in the acceptor logs of a majority when it returns: it
  survives a crash of the whole cluster before any persister pass (review I1).
* After a refound, a restarted node loads the new lineage from its log, never the dead
  branch whose epochs are higher (review I2).
* A commit whose records exceed one frame is split into chunks; a record that could never
  be replicated is refused when it is saved, so the node keeps committing (review I3).
* An unexpected exception in the worker or the persister fences the node: logged, reported
  by the health metric, no more grants or acknowledgements (review M3).
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
            cl[n].stop()                                   # the whole cluster dies: no close, no persister pass
    for storage in storages:
        storage.close()                                    # file descriptors only; publishes no root
    with Cluster(3, **FAST) as cl:
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
        tc.store.set_outage(False)
        tc.revive("n0")
        tc.revive("n2")
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

        def broken(records, root=None):
            raise RuntimeError("disk controller on fire")

        tc.storages["n1"].apply = broken
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


@_THREAD_DIES
def test_an_unexpected_persister_failure_fences_the_node(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")

        def broken(force=False):
            raise RuntimeError("upload queue corrupted")

        tc.storages["n2"].ship = broken
        tc.db("n0").run(incr, **RUN_KW)
        assert _wait_for(lambda: not tc["n2"].healthy)
        assert "upload queue corrupted" in tc["n2"].stats()["failure"]
        assert _wait_for(lambda: not tc["n2"].persister._thread.is_alive())
        tc.db("n1").run(incr, **RUN_KW)                     # the others go on
        assert tc.eventually("n0", 2)


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
