"""TieredStorage: the local WAL tier plus the object-store tier of a cluster node (Task 8.3d, D46)."""
import os
import uuid

import pytest

from proto_db.errors import (ProtoBacklogFullError, ProtoCorruptionError, ProtoNotSupportedError,
                             ProtoUnexpectedError, ProtoValidationError)
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.format import FORMAT_VERSION, RootRecord, encode_segment_header
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
    uploads = [k for op, k in store.log if op == "put_if_absent"]
    assert uploads == [st.segment_key(first.segment), st.segment_key(second.segment)]
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
    st.shipped(st.outgoing(), _root(ptr, 4))
    st.ship(force=True)
    st.push_atom({"n": 2})                             # the open segment is never released
    assert st.release_local(3) == 0                    # the persisted root does not cover it yet
    assert st.release_local(4) == 1
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
    accepted, committed, top = st.load_roots()
    assert accepted == (3, _root(ptr, 4)) and committed == (2, _root(ptr, 5)) and top == 3
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


def test_a_foreign_segment_object_is_detected(tmp_path):
    store = FakeObjectStore()
    st = _tiered(tmp_path, store)
    ptr = st.push_atom({"n": 1})
    st.shipped(st.outgoing(), _root(ptr, 1))
    st.ship(force=True)
    st.push_atom({})
    st.release_local(1)
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
