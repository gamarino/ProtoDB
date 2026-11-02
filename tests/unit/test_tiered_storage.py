"""TieredStorage: the local WAL tier plus the object-store tier of a cluster node (Task 8.3d, D46)."""
import os
import uuid

import pytest

from proto_db.errors import (ProtoBacklogFullError, ProtoCorruptionError, ProtoNotSupportedError,
                             ProtoUnexpectedError, ProtoValidationError)
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.format import FORMAT_VERSION, RootRecord, encode_segment_header
from proto_db.cluster.rootlog import root_key
from proto_db.storage.tiered import TieredStorage
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(60)]


def _tiered(tmp_path, store, name="n0", **kw):
    kw.setdefault("durability", "none")
    kw.setdefault("ship_interval", 0.0)
    kw.setdefault("backoff", (0.0, 0.0))
    kw.setdefault("retries", 1)
    return TieredStorage(str(tmp_path / name), store, **kw)


def _root(ptr, epoch):
    return RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, epoch)


def test_records_are_read_back_from_the_cache_and_the_local_tier(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    a = st.push_atom({"x": 1})
    b = st.push_blob(b"blob")
    st.flush()
    assert st.get_atom(a) == {"x": 1} and st.get_blob(b) == b"blob"
    assert st.stats()["reads_cache"] == 2
    st.close()
    st = _tiered(tmp_path, store)                     # reopened: the cache is empty
    assert st.get_atom(a) == {"x": 1}
    assert st.stats()["reads_local"] == 1 and store.calls["get"] == 0
    st.close()


def test_outgoing_records_are_dropped_once_shipped(tmp_path):
    st = _tiered(tmp_path, FakeObjectStore())
    st.push_atom({"x": 1})
    first = st.outgoing()
    st.push_atom({"x": 2})
    assert len(st.outgoing()) == 2
    st.shipped(first, None)
    remaining = st.outgoing()
    assert len(remaining) == 1 and remaining[0][2] != first[0][2]
    st.close()


def test_applied_copies_are_readable_at_their_original_pointers(tmp_path):
    store = FakeObjectStore()
    writer = _tiered(tmp_path, store, "w")
    ptrs = [writer.push_atom({"i": i}) for i in range(3)]
    records = writer.outgoing()
    reader = _tiered(tmp_path, store, "r")
    reader.apply(records[1:], _root(ptrs[2], 7))                       # a copy with a hole at the first record
    reader.close()
    reader = _tiered(tmp_path, store, "r")
    assert [reader.get_atom(p) for p in ptrs[1:]] == [{"i": 1}, {"i": 2}]
    assert reader.local_record(ptrs[0]) is None        # the hole is not served to peers
    reader.set_fetcher(lambda ptr: writer.local_record(ptr))
    assert reader.get_atom(ptrs[0]) == {"i": 0}
    assert reader.stats()["reads_peer"] == 1 and store.calls["get"] == 0
    writer.close()
    reader.close()


def test_segments_ship_sealed_and_in_order_and_cloud_storage_reads_the_persisted_space(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    first = st.push_atom({"n": 1})
    assert st.ship(force=True)
    second = st.push_atom({"n": 2})
    assert second.segment != first.segment             # shipping sealed the first segment
    assert st.ship(force=True)
    uploads = [k for op, k in store.log if op == "put_if_absent" and k.endswith(".seg")]
    assert uploads == [st.segment_key(first.segment), st.segment_key(second.segment)]
    markers = [k for op, k in store.log if op == "put_if_absent" and k.endswith(".sealed")]
    assert [m.split("/")[-1].split(".")[0] for m in markers] == [first.segment.hex, second.segment.hex]
    assert st.ensure_remote([(first.segment, None), (second.segment, None)])
    assert st.persist(3, _root(second, 2))
    assert not st.persist(3, _root(first, 1))          # never backwards
    assert st.read_persisted() == (3, _root(second, 2))
    cloud = CloudStorage(store)
    assert cloud.read_root() == _root(second, 2)
    assert cloud.get_atom(second) == {"n": 2}
    st.close()


def test_released_segments_are_read_from_the_object_store(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    ptr = st.push_atom({"n": 1})
    st.shipped(st.outgoing(), _root(ptr, 4), 1)
    st.ship(force=True)
    st.push_atom({"n": 2})                             # the open segment is never released
    assert st.release_local(root_key(1, _root(ptr, 3))) == 0     # the persisted root does not cover it yet
    assert st.release_local(root_key(1, _root(ptr, 4))) == 1
    assert not os.path.exists(os.path.join(st.path, ptr.segment.hex + ".seg"))
    st.close()
    st = _tiered(tmp_path, store)
    assert st.get_atom(ptr) == {"n": 1}
    assert st.stats()["reads_remote"] == 1
    st.close()


def test_reads_missing_the_cache_inside_a_critical_section_are_counted(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    ptr = st.push_atom({"n": 1})
    st.close()
    st = _tiered(tmp_path, store)
    with st.critical_section():
        st.get_atom(ptr)                               # a local read: counted
        st.get_atom(ptr)                               # now cached: not counted
    assert st.stats()["critical_misses"] == 1
    st.close()


def test_the_roots_log_survives_a_restart_and_a_torn_tail(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    ptr = st.push_atom({"n": 1})
    st.log_root("accepted", 2, _root(ptr, 5), sync=True)
    st.log_root("accepted", 3, _root(ptr, 4), sync=True)   # higher label wins over higher epoch
    st.log_root("committed", 2, _root(ptr, 5), sync=True)
    st.close()
    with open(tmp_path / "n0" / "roots.log", "ab") as f:
        f.write(b"\x01torn")
    st = _tiered(tmp_path, store)
    accepted, committed, top, promised, empty = st.load_roots()
    assert accepted == (3, _root(ptr, 4)) and committed == (2, _root(ptr, 5)) and top == 3 and promised == 0
    st.close()


def test_the_roots_log_orders_committed_roots_by_label_first_so_a_refound_lineage_wins(tmp_path):
    """Review I2a: after a refound the new lineage restarts from the persisted epoch, below the
    epochs of the dead branch a survivor logged.  A restarted survivor must load the new
    lineage, also after the log is compacted."""
    from proto_db.cluster.coordinator import REFOUND_TERM_STEP
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    dead = _root(st.push_atom({"n": 1}), 10)
    fresh = _root(st.push_atom({"n": 2}), 7)
    st.log_root("committed", 5, dead, sync=True)
    st.log_root("accepted", 5, dead, sync=True)
    st.log_root("committed", 5 + REFOUND_TERM_STEP, fresh, sync=True)
    st.log_root("accepted", 5 + REFOUND_TERM_STEP, fresh, sync=True)
    st.close()
    st = _tiered(tmp_path, store)
    logged = st.load_roots()
    assert logged.committed == (5 + REFOUND_TERM_STEP, fresh)
    assert logged.accepted == (5 + REFOUND_TERM_STEP, fresh)
    st._roots._compact()                                # the compacted log keeps the same roots
    assert st.load_roots()[:2] == logged[:2]
    st.close()


def test_promises_are_logged_and_two_roots_with_one_label_and_epoch_are_refused(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    st.log_promise(7)
    st.log_promise(3)                                   # lower: already covered
    a = _root(st.push_atom({"n": 1}), 4)
    b = _root(st.push_atom({"n": 2}), 4)
    st.log_root("accepted", 7, a, sync=True)
    st.close()
    st = _tiered(tmp_path, store)
    assert st.load_roots().promised == 7
    st.log_root("accepted", 7, b, sync=True)            # a protocol violation, reported on load
    with pytest.raises(ProtoCorruptionError, match="Protocol violation"):
        st.load_roots()
    st.close()


def test_the_backlog_alert_fires_before_the_limit_refuses_commits(tmp_path):
    alerts = []
    st = _tiered(tmp_path, FakeObjectStore(), backlog_limit_bytes=4096, on_backlog_alert=alerts.append)
    while st.backlog()["bytes"] < 3300:
        st.push_blob(b"x" * 100)
    st.check_commit_allowed()                          # below the limit: never refused
    st.check_backlog()
    assert alerts and alerts[0]["bytes"] >= 3276
    while st.backlog()["bytes"] < 4096:
        st.push_blob(b"x" * 100)
    with pytest.raises(ProtoBacklogFullError):
        st.check_commit_allowed()
    st.ship(force=True)                                # the store drained it
    st.check_commit_allowed()
    assert st.backlog()["bytes"] == 0
    st.close()


def test_an_outage_delays_shipping_but_never_writes(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    ptr = st.push_atom({"n": 1})
    store.set_outage(True)
    with pytest.raises(ProtoUnexpectedError):
        st.ship(force=True)
    st.push_atom({"n": 2})                             # writes go on
    assert st.backlog()["bytes"] > 0
    store.set_outage(False)
    assert st.ship(force=True)
    assert st.ensure_remote([(ptr.segment, None)])
    st.close()


def test_the_root_api_is_reserved_to_the_cluster(tmp_path):
    st = _tiered(tmp_path, FakeObjectStore())
    with pytest.raises(ProtoNotSupportedError):
        st.publish_root(st.push_atom({}), 0)
    assert st.read_root() is None
    st.close()


def test_invalid_arguments_are_refused(tmp_path):
    with pytest.raises(ProtoValidationError):
        TieredStorage(str(tmp_path / "x"), object())
    with pytest.raises(ProtoValidationError):
        _tiered(tmp_path, FakeObjectStore(), backlog_limit_bytes=0)
    with pytest.raises(ProtoValidationError):
        _tiered(tmp_path, FakeObjectStore(), ship_interval=-1)


def test_local_release_follows_the_order_of_roots_not_the_epoch(tmp_path):
    """8.3d re-review N3: a dead-branch root persisted late (old label, higher epoch) does not
    release data shipped with a new-lineage root (higher label, lower epoch)."""
    from proto_db.cluster.coordinator import REFOUND_TERM_STEP
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    ptr = st.push_atom({"n": 1})
    new_lineage = 5 + REFOUND_TERM_STEP
    st.shipped(st.outgoing(), _root(ptr, 2), new_lineage)
    st.ship(force=True)
    st.push_atom({"n": 2})                             # the open segment is never released
    dead_branch = root_key(5, RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 10))
    assert st.release_local(dead_branch) == 0          # an epoch-keyed release dropped it here
    assert st.commit_ranges(_root(ptr, 2)) is not None
    assert st.release_local(root_key(new_lineage, _root(ptr, 2))) == 1
    st.close()


def test_a_foreign_segment_object_is_detected(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    ptr = st.push_atom({"n": 1})
    st.shipped(st.outgoing(), _root(ptr, 1), 1)
    st.ship(force=True)
    st.push_atom({})
    st.release_local(root_key(1, _root(ptr, 1)))
    st.close()
    other = uuid.uuid4()
    store.overwrite(st.segment_key(ptr.segment), encode_segment_header(other) + b"\0" * 64)
    st = _tiered(tmp_path, store)
    with pytest.raises(ProtoCorruptionError):
        st.get_atom(ptr)
    st.close()


def test_a_lost_writers_records_are_uploaded_from_a_copy_as_a_part(tmp_path):
    store = FakeObjectStore()
    writer = _tiered(tmp_path, store, "w")
    ptrs = [writer.push_atom({"i": i}) for i in range(3)]
    root = _root(ptrs[2], 3)
    records = writer.outgoing()
    survivor = _tiered(tmp_path, store, "s")
    survivor.apply(records, root)
    writer.close()                                    # lost before shipping anything
    ranges = survivor.commit_ranges(root)
    needs = [(segment, end) for segment, _, end in ranges]
    assert ranges and not survivor.ensure_remote(needs)            # nothing remote, no upload
    assert survivor.ensure_remote(needs, upload_copies=True)       # the copy's prefix as a part
    assert survivor.stats()["parts_uploaded"] == 1
    assert survivor.persist(1, root)
    reader = _tiered(tmp_path, store, "r")
    assert reader.read_persisted() == (1, root)
    assert [reader.get_atom(p) for p in ptrs] == [{"i": 0}, {"i": 1}, {"i": 2}]
    assert reader.stats()["reads_remote"] == 3
    reader.close()
    survivor.close()


def test_a_copy_with_a_hole_is_never_uploaded(tmp_path):
    store = FakeObjectStore()
    writer = _tiered(tmp_path, store, "w")
    ptrs = [writer.push_atom({"i": i}) for i in range(3)]
    records = writer.outgoing()
    survivor = _tiered(tmp_path, store, "s")
    survivor.apply(records[1:], _root(ptrs[2], 3))    # missed the first record
    assert not survivor.ensure_remote([(ptrs[2].segment, ptrs[2].offset + 1)], upload_copies=True)
    assert survivor.stats()["parts_uploaded"] == 0
    writer.close()
    survivor.close()


# -- sealed objects carry their length (8.3d re-review 6, M2; ruling R54) ----------------------


def _short_sealed_segment(tmp_path, store):
    """A writer ships three records to a peer, loses the tail of its segment (a crash dropped
    its unflushed buffer: only the first record is on its disk), restarts, and seals and uploads
    that prefix as the whole segment.  Returns ``(peer, ptrs, root, records)``; the peer holds
    the full copy, accepted with ``root`` labelled 1."""
    writer = _tiered(tmp_path, store, "w")
    ptrs = [writer.push_atom({"i": i}) for i in range(3)]
    records = writer.outgoing()
    root = _root(ptrs[2], 3)
    peer = _tiered(tmp_path, store, "s")
    peer.apply(records, root, 1)
    writer.flush()
    writer.close()
    path = os.path.join(str(tmp_path / "w"), ptrs[0].segment.hex + ".seg")
    with open(path, "r+b") as f:
        f.truncate(records[1][1])                  # only the first record survived the crash
    writer = _tiered(tmp_path, store, "w")
    writer.ship(force=True)
    assert store.get(writer.segment_key(ptrs[0].segment))[0] == open(path, "rb").read()
    writer.close()
    peer.refresh_remote()
    return peer, ptrs, root, records


def test_a_short_sealed_object_never_covers_records_past_its_length(tmp_path):
    store = FakeObjectStore()
    peer, ptrs, root, records = _short_sealed_segment(tmp_path, store)
    segment = ptrs[0].segment
    needs = [(s, end) for s, _, end in peer.commit_ranges(root)]
    assert peer.ensure_remote([(segment, records[1][1])])          # the prefix is in the store
    assert not peer.ensure_remote(needs), "a short sealed object counted as covering the commit"
    # A holder re-proposing the root gathers the records the store lacks instead of skipping them.
    gathered = peer.commit_records(peer.commit_ranges(root))
    assert [r[1] for r in gathered] == [r[1] for r in records]
    # A lagging peer uploads its copy as a part; the part then covers the commit.
    assert peer.ensure_remote(needs, upload_copies=True)
    assert peer.stats()["parts_uploaded"] == 1
    peer.close()


def test_a_copy_longer_than_the_sealed_object_is_never_released(tmp_path):
    store = FakeObjectStore()
    peer, ptrs, root, _ = _short_sealed_segment(tmp_path, store)
    copy = os.path.join(peer.path, ptrs[0].segment.hex + ".seg")
    assert peer.release_local(root_key(1, root)) == 0, "the only full copy was released"
    assert os.path.exists(copy)
    assert peer.get_atom(ptrs[2]) == {"i": 2}
    # Once the store holds every byte of the copy (a part), the copy may go.
    assert peer.ensure_remote([(ptrs[0].segment, os.path.getsize(copy))], upload_copies=True)
    assert peer.release_local(root_key(1, root)) == 1
    peer.close()


def _truncated_in_the_store(tmp_path, store):
    """A writer uploads a full sealed segment (and its length), shipped to a peer as a copy;
    the store's object is later found truncated (a store or disk that acknowledged bytes it did
    not keep).  Returns ``(peer, ptrs)``."""
    writer = _tiered(tmp_path, store, "w")
    ptrs = [writer.push_atom({"i": i}) for i in range(3)]
    records = writer.outgoing()
    peer = _tiered(tmp_path, store, "s")
    peer.apply(records, _root(ptrs[2], 3), 1)
    writer.ship(force=True)
    key = writer.segment_key(ptrs[0].segment)
    writer.close()
    store.overwrite(key, store.get(key)[0][:records[1][1]])
    return peer, ptrs


def test_a_truncated_read_from_the_store_falls_back_to_the_peers(tmp_path):
    store = FakeObjectStore()
    peer, ptrs = _truncated_in_the_store(tmp_path, store)
    reader = _tiered(tmp_path, store, "r")
    reader.refresh_remote()                       # the recorded length says the store has it all
    reader.set_fetcher(peer.local_record)
    # Believed in the store, past the short object's end: the store read is truncated and the
    # record comes from the peer.
    assert reader.get_atom(ptrs[2]) == {"i": 2}
    assert reader.stats()["reads_peer"] == 1 and reader.stats()["remote_segment_gets"] >= 1
    assert reader.get_atom(ptrs[0]) == {"i": 0}   # inside the short object: from the store
    # The short download is remembered: the store no longer counts as covering those bytes.
    assert not reader.ensure_remote([(ptrs[2].segment, ptrs[2].offset + 1)])
    reader.close()
    peer.close()


def test_a_truncated_read_with_no_full_copy_anywhere_raises_corruption(tmp_path):
    store = FakeObjectStore()
    peer, ptrs = _truncated_in_the_store(tmp_path, store)
    peer.close()
    reader = _tiered(tmp_path, store, "r")
    reader.refresh_remote()
    reader.set_fetcher(lambda ptr: None)          # no peer holds it
    with pytest.raises(ProtoCorruptionError, match="no full copy"):
        reader.get_atom(ptrs[2])
    reader.close()
