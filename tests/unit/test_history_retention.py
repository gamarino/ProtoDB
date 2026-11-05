"""A long reader's old root survives local release under heavy writes (Task 8.3l, P1).

A report runs on a snapshot: one transaction holding an old root, for as long as it takes,
while the cluster keeps committing.  Nothing it needs may become unreadable.  What a cluster
node reclaims is local only (D46): its own segments and its copies of other nodes' segments,
deleted once the object store holds them in full and a persisted root covers them (checkpoint
first, so the acceptor log names nothing deleted).  The object store is append-only: no
ProtoDB code deletes an object.  So the old root's records move from the nodes' disks to the
store and stay readable from there.

The test makes every node release the local segments that held the old version (small
segments, fast shipping), with no decoded-atom cache and a tiny record cache on the reading
node, so every read reaches a tier; then the old transaction reads its whole snapshot, and an
"as of" walk of ``previous`` from the newest root reads the old version again.
"""
import os
import time

from proto_db.storage.tiered import SEGMENT_SUFFIX
from tests.support.tiered_cluster import TieredCluster

RUN_KW = dict(retries=200, backoff=(0.001, 0.01))
KEYS = 200


def _value(k: int, generation: int) -> str:
    return f"{k:04d}-{generation:04d}-" + "x" * 80


def _segment_files(path: str) -> set:
    return {name[:-len(SEGMENT_SUFFIX)] for name in os.listdir(path) if name.endswith(SEGMENT_SUFFIX)}


def test_an_old_root_stays_readable_after_every_node_released_its_segments(tmp_path):
    cluster = TieredCluster(
        tmp_path, 3, lease_duration=3.0,
        storage_kw=dict(ship_interval=0.01, segment_max_bytes=32 * 1024, cache_bytes=4096,
                        segment_cache_bytes=0),
        space_kw=dict(atom_cache_entries=0))
    try:
        cluster.spaces["n0"].new_database("db")
        cluster.db("n0").run(lambda tr: tr.set_root_object("d", tr.new_dictionary().__class__.from_items(
            ((k, _value(k, 0)) for k in range(KEYS)), context=tr)), **RUN_KW)
        cluster.db("n0").run(lambda tr: tr.set_root_object("n", 0), **RUN_KW)
        assert cluster.eventually("n2", 0)
        old = cluster.db("n2").new_transaction()        # the long report: holds the old root
        old_version = old._db_root.version
        old_ref = old.snapshot_ref                      # Task 8.4b: an "as of" reference ...
        old_pager = old.pager("d", page_size=10)        # ... and a portable pager on that version
        pager_token = old_pager.token()
        assert old_pager.by_rank
        old_segments = {old._db_root.atom_pointer.segment.hex, old._snapshot_space.atom_pointer.segment.hex}

        # Heavy writes on two nodes: every entry rewritten several times, many segments sealed.
        for generation in range(1, 6):
            for start in range(0, KEYS, 20):
                def rewrite(tr, start=start, generation=generation):
                    d = tr.get_root_object("d")
                    for k in range(start, start + 20):
                        d = d.set_at(k, _value(k, generation))
                    tr.set_root_object("d", d)
                    tr.set_root_object("n", tr.get_root_object("n") + 1)
                cluster.db("n0" if start % 40 else "n1").run(rewrite, **RUN_KW)
        for node in cluster.ids:
            cluster.spaces[node].sync(timeout=20.0)

        # Every node has deleted the local files of the segments that held the old roots.
        deadline = time.monotonic() + 20.0
        while any(old_segments & _segment_files(os.path.join(cluster.path, node)) for node in cluster.ids):
            assert time.monotonic() < deadline, {
                node: (old_segments & _segment_files(os.path.join(cluster.path, node)),
                       cluster.storages[node].stats()) for node in cluster.ids}
            cluster.db("n0").run(lambda tr: tr.set_root_object("n", tr.get_root_object("n") + 1), **RUN_KW)
            for node in cluster.ids:
                cluster.spaces[node].sync(timeout=20.0)
            time.sleep(0.05)
        released = {node: cluster.storages[node].stats() for node in cluster.ids}
        # The writers released their own segments and the data replicas their copies (a node that
        # is no data replica of a commit kept its records in cache only, D54).
        assert sum(s["segments_released"] for s in released.values()) > 0, released
        assert sum(s["copies_released"] for s in released.values()) > 0, released

        remote_before = cluster.storages["n2"].stats()["reads_remote"]
        d = old.get_root_object("d")
        assert [d.get_at(k) for k in range(KEYS)] == [_value(k, 0) for k in range(KEYS)]
        assert old.get_root_object("n") == 0
        assert old._db_root.version == old_version
        old.abort()
        assert cluster.storages["n2"].stats()["reads_remote"] > remote_before, "the old records were not read from the store"

        # Task 8.4b: the reference and the pager token reopen the released version from the store.
        remote_before = cluster.storages["n2"].stats()["reads_remote"]
        snap = cluster.db("n2").snapshot_at(old_ref)
        assert snap.version == old_version and snap.get_root_object("n") == 0
        assert [snap.get_root_object("d").get_at(k) for k in range(0, KEYS, 7)] == \
            [_value(k, 0) for k in range(0, KEYS, 7)]
        resumed = cluster.db("n2").resume_pager(pager_token)
        assert resumed.rows == KEYS and resumed.page(3) == tuple(_value(k, 0) for k in range(30, 40))
        assert cluster.storages["n2"].stats()["reads_remote"] > remote_before, "the snapshot was not read from the store"

        # "As of": walk previous from the newest root down to the old version.
        with cluster.db("n2").new_transaction() as tr:
            root = tr._db_root
            assert tr.get_root_object("d").get_at(0) == _value(0, 5)
            while root.version > old_version:
                root = root.previous
            d = root.objects.get_at("d")
            assert [d.get_at(k) for k in range(KEYS)] == [_value(k, 0) for k in range(KEYS)]

        # The object store is append-only: nothing was ever deleted from it.
        assert cluster.store.calls["delete"] == 0
    finally:
        cluster.close()
