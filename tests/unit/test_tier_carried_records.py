"""Records of an own segment of an earlier instance that come back to their writer (Task 8.3j
review C1, D50 amendment).

Since Task 8.3j a holder's records may be on its data replicas before its own entry is durable.
If it dies in between, it restarts without them, and they come back to it with a re-proposal of
the root (as the re-proposer, or as a data replica of another one).  The live ``FileStorage``
never writes a segment of an earlier instance, and the tier used to skip "own" records, so
they stayed only in the acceptor-log entry and the next checkpoint compacted them away while
the node counted as one of their copies.  Now they are written into that segment's file
(contiguous with it), synced by the checkpoint, and survive a power loss; a segment released
locally is first restored from the store; a record that would leave a hole is refused before
anything is logged; the extension reaches the store as a part when the segment was uploaded
shorter.
"""
import os
import threading
import uuid

import pytest

from proto_db.core.atom import AtomPointer
from proto_db import errors
from proto_db.storage import tiered
from proto_db.storage.format import FORMAT_VERSION, RecordKind, RootRecord, encode_record, encode_segment_header
from proto_db.storage.logfile import read_log
from tests.support.fake_object_store import FakeObjectStore
from tests.support.power_loss import PowerLoss

pytestmark = [pytest.mark.timeout(120)]


def _open(path, store, **kw):
    return tiered.TieredStorage(path, store, durability="fsync", release_local=False, **kw)


def _earlier_instance(path, store):
    """An instance writes one record into its own segment and closes; return the segment."""
    st = _open(path, store)
    ptr = st.push_blob(b"A" * 50)
    st.flush()
    st.close()
    return ptr.segment


def _carried(path, segment, payloads, start=None):
    """Framed records ``payloads`` at the end of the segment's file (or at ``start``), as a
    re-proposal carries them back: ``[segment bytes, offset, framed]``."""
    offset = os.path.getsize(os.path.join(path, segment.hex + ".seg")) if start is None else start
    records = []
    for payload in payloads:
        framed = encode_record(RecordKind.BLOB, payload)
        records.append([segment.bytes, offset, framed])
        offset += len(framed)
    return records


def _root(records):
    raw, offset, _ = records[-1]
    return RootRecord(FORMAT_VERSION, uuid.UUID(bytes=raw), offset, 9)


def _ptrs(records):
    return [AtomPointer(uuid.UUID(bytes=raw), offset) for raw, offset, _ in records]


def _store(st, how, records):
    root = _root(records)
    if how == "acceptance":
        st.accept_replication(3, root, records)
    elif how == "data replica":
        st.accept_replication(3, root, records, ranges=tiered.TieredStorage._ranges_of(records), accepted=False)
    else:
        st.store_commit(records, root, 3, tiered.TieredStorage._ranges_of(records))


@pytest.mark.parametrize("how", ["acceptance", "data replica", "store_commit"])
def test_records_carried_back_into_an_own_segment_survive_a_checkpoint_and_a_power_loss(tmp_path, monkeypatch, how):
    """Fails on 8c8c193: the records are in no data file after the checkpoint."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    segment = _earlier_instance(path, store)
    loss = PowerLoss(monkeypatch)
    st = _open(path, store)
    records = _carried(path, segment, [b"B" * 60, b"C" * 70])
    _store(st, how, records)
    st.checkpoint()                 # compacts the entry: the records must be in the file by now
    st.close()
    loss.power_loss(path)
    st = _open(path, store)
    try:
        for ptr, (_, _, framed) in zip(_ptrs(records), records):
            assert st.local_record(ptr) == framed, ptr
    finally:
        st.close()


def _refused():
    """The refusal of one write by a healthy tier (Task 8.3j re-review N1)."""
    return getattr(errors, "ProtoWriteRefusedError", type("Absent", (Exception,), {}))


def test_a_carried_record_that_would_leave_a_hole_is_refused_before_anything_is_logged(tmp_path):
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    segment = _earlier_instance(path, store)
    st = _open(path, store)
    try:
        end = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
        records = _carried(path, segment, [b"B" * 60], start=end + 100)
        before = len(read_log(os.path.join(path, "roots.log"), b"ROOT"))
        with pytest.raises(_refused()):
            st.accept_replication(3, _root(records), records)
        assert len(read_log(os.path.join(path, "roots.log"), b"ROOT")) == before
        assert os.path.getsize(os.path.join(path, segment.hex + ".seg")) == end
        good = _carried(path, segment, [b"C" * 60])
        st.accept_replication(3, _root(good), good)        # the tier is healthy: the next write lands
        assert st.local_record(_ptrs(good)[0]) == good[0][2]
    finally:
        st.close()


def test_a_released_own_segment_is_restored_from_the_store_before_records_are_carried_back(tmp_path):
    """The segment was uploaded and its local file released; records of it come back: the
    file is rebuilt from the store's object, then extended, and the storage still opens (a
    file with a hole would be refused by the own-segment repair)."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)             # seals and uploads the segment
    segment = a.segment
    assert st._covered(segment, None)
    st.close()
    sealed = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
    os.remove(os.path.join(path, segment.hex + ".seg"))     # released locally
    st = _open(path, store)
    records = _carried(path, segment, [b"B" * 60], start=sealed)
    st.accept_replication(3, _root(records), records)
    st.checkpoint()
    st.close()
    st = _open(path, store)
    try:
        assert st.local_record(a) is not None
        assert st.local_record(_ptrs(records)[0]) == records[0][2]
    finally:
        st.close()


def test_an_extension_of_a_segment_uploaded_shorter_reaches_the_store_as_a_part(tmp_path):
    """The store holds the segment's sealed object, shorter than the local file once records
    came back: shipping again keeps the store's length (it is a prefix), and the extension is
    uploaded as a part."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)
    segment = a.segment
    st.close()
    sealed = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
    st = _open(path, store)         # the segment is not known uploaded: it will be shipped again
    try:
        records = _carried(path, segment, [b"B" * 60])
        st.accept_replication(3, _root(records), records)
        st.ship(force=True)
        with st._lock:
            assert st._sealed[segment] == sealed       # the store's length, never the local one
        end = records[-1][1] + len(records[-1][2])
        assert st.ensure_remote([(segment, end)], upload_copies=True)
        assert st.stats()["parts_uploaded"] == 1
    finally:
        st.close()


def _died_on_a_fresh_segment(path, store):
    """An instance rolls its segment (as ``ship()`` does), pushes the first record of the fresh
    segment Y -- which lists Y in the manifest -- and dies before any write-out: Y has no file.
    Return Y's pointer and the framed record the replicas hold."""
    st = _open(path, store)
    st.push_blob(b"A" * 50)
    st.flush()
    st._local.roll_segment()
    ptr = st.push_blob(b"Y" * 60)
    framed = st.outgoing()[-1][2]
    with st._local._state_lock:             # the process dies: its unflushed buffer is gone
        st._local._pending, st._local._pending_bytes = [], 0
    st.close()
    assert not os.path.exists(os.path.join(path, ptr.segment.hex + ".seg"))
    return ptr, framed


@pytest.mark.parametrize("outage", [False, True])
def test_records_of_a_fresh_own_segment_without_a_file_are_carried_back_without_the_store(tmp_path, monkeypatch, outage):
    """Task 8.3j re-review N1.  A missing file is not a release: the segment was never written
    out, the store never held it, and the carried run starts at the header, so the file is
    created from it; the segment is shipped later.  On aaa5a1d the tier tried to restore it
    from the store and failed (in no tier; or the store unreachable)."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    ptr, framed = _died_on_a_fresh_segment(path, store)
    loss = PowerLoss(monkeypatch)
    store.set_outage(outage)
    st = _open(path, store)
    records = [[ptr.segment.bytes, ptr.offset, framed]]
    st.accept_replication(3, _root(records), records)
    st.checkpoint()
    st.close()
    loss.power_loss(path)
    store.set_outage(False)
    st = _open(path, store)
    try:
        assert st.local_record(ptr) == framed
        st.ship(force=True)
        with st._lock:
            assert st._own[ptr.segment].uploaded
            assert st._sealed[ptr.segment] == ptr.offset + len(framed)
    finally:
        st.close()


def test_a_released_segment_that_cannot_be_restored_refuses_only_that_write(tmp_path):
    """Task 8.3j re-review N1/N2: the restore of a released segment from an unreachable store
    refuses the one acceptance (``ProtoWriteRefusedError``, nothing logged); the tier stays
    healthy and accepts the same records once the store is back."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)
    segment = a.segment
    st.close()
    sealed = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
    os.remove(os.path.join(path, segment.hex + ".seg"))
    st = _open(path, store, retries=1)
    try:
        records = _carried(path, segment, [b"B" * 60], start=sealed)
        before = len(read_log(os.path.join(path, "roots.log"), b"ROOT"))
        store.set_outage(True)
        with pytest.raises(_refused()):
            st.accept_replication(3, _root(records), records)
        assert len(read_log(os.path.join(path, "roots.log"), b"ROOT")) == before
        store.set_outage(False)
        st.accept_replication(3, _root(records), records)
        assert st.local_record(_ptrs(records)[0]) == records[0][2]
    finally:
        st.close()


def test_ship_does_not_wait_for_copy_writes_on_segments_of_this_instance(tmp_path):
    """Task 8.3j re-review N3: only a segment of an earlier instance is read under the copies
    lock; on aaa5a1d every ship waited for it."""
    store = FakeObjectStore()
    st = _open(str(tmp_path / "n0"), store)
    try:
        st.push_blob(b"A" * 50)
        done = threading.Event()
        with st._copies_lock:
            worker = threading.Thread(target=lambda: (st.ship(force=True), done.set()), daemon=True)
            worker.start()
            shipped = done.wait(3.0)
        worker.join(10.0)
        assert shipped
    finally:
        st.close()


def test_a_store_object_longer_than_the_local_segment_is_adopted(tmp_path):
    """Task 8.3j re-review N4 (pre-existing): under ``flush`` or ``none`` durability a power loss
    can leave the local segment shorter than the object an earlier instance uploaded.  That
    object holds the true bytes: shipping adopts it (the local file is a prefix of it) and
    records its length; before, every ship raised and the strict order blocked the rest."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = tiered.TieredStorage(path, store, durability="flush", release_local=False)
    a = st.push_blob(b"A" * 50)
    b = st.push_blob(b"B" * 60)
    st.ship(force=True)
    segment = a.segment
    full = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
    st.close()
    with open(os.path.join(path, segment.hex + ".seg"), "r+b") as f:
        f.truncate(b.offset)                    # the tail lost locally, the store kept it
    st = tiered.TieredStorage(path, store, durability="flush", release_local=False)
    try:
        assert st.ship(force=True)
        with st._lock:
            assert st._own[segment].uploaded
            assert st._sealed[segment] == full
        assert st.get_blob(b) == b"B" * 60          # beyond the local file: read from the store
    finally:
        st.close()


def test_a_store_object_that_disagrees_with_the_local_segment_is_still_refused(tmp_path):
    """N4's adoption needs a common prefix: different bytes are never taken."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)
    segment = a.segment
    st.close()
    key = st.segment_key(segment)
    data, _ = store.get(key)
    store.delete(key)
    store.put_if_absent(key, data[:-1] + bytes([data[-1] ^ 1]) + b"extra")
    st = _open(path, store)
    try:
        with pytest.raises(errors.ProtoUnexpectedError):
            st.ship(force=True)
    finally:
        st.close()


def test_a_restored_prefix_survives_a_power_loss_before_the_checkpoint(tmp_path, monkeypatch):
    """The store's prefix of a released segment cannot be rebuilt by the redo (it recreates a
    missing file from the header and the entry's records only), so it is synced before the
    entry: after a power loss the storage opens and reads both the prefix and the records."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)
    segment = a.segment
    st.close()
    sealed = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
    os.remove(os.path.join(path, segment.hex + ".seg"))
    loss = PowerLoss(monkeypatch)
    st = _open(path, store)
    records = _carried(path, segment, [b"B" * 60], start=sealed)
    st.accept_replication(3, _root(records), records)
    st.close()
    loss.power_loss(path)
    store.set_outage(True)              # only the local disk can serve what follows
    st = _open(path, store)
    try:
        assert st.local_record(a) is not None
        assert st.local_record(_ptrs(records)[0]) == records[0][2]
    finally:
        st.close()


def test_a_released_segment_of_an_older_format_rebuilt_from_the_header_still_ships(tmp_path, monkeypatch):
    """Task 8.3j re-review 2, N5.  A released segment written under format 2 gets back a run
    that starts at the header: the file is rebuilt with a header of the current format, so
    it differs from the store's object in the version field only.  The header's version is
    informational (the codec reads both forms by shape), so shipping keeps the store's
    object; on 7cc6606 every ship refused it and the strict order stalled the later segments."""
    from proto_db.storage import format as fmt
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    monkeypatch.setattr(fmt, "FORMAT_VERSION", 2)          # an earlier release wrote this segment
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)
    st.close()
    monkeypatch.setattr(fmt, "FORMAT_VERSION", 3)
    segment = a.segment
    os.remove(os.path.join(path, segment.hex + ".seg"))     # released
    remote_before, _ = store.get(st.segment_key(segment))
    st = _open(path, store)
    try:
        records = [[segment.bytes, a.offset, encode_record(RecordKind.BLOB, b"A" * 50)]]
        st.store_commit(records, _root(records), 3, tiered.TieredStorage._ranges_of(records))
        b = st.push_blob(b"B" * 10)
        st.ship(force=True)
        st.ship(force=True)
        with st._lock:
            assert st._own[segment].uploaded
            assert st._own[b.segment].uploaded             # the later segment is not held back
            assert st._sealed[segment] == len(remote_before)
        assert store.get(st.segment_key(segment))[0] == remote_before   # never overwritten
    finally:
        st.close()


def test_a_store_object_of_another_segment_is_still_refused(tmp_path):
    """N5's header tolerance is the version field only: a store object whose header names
    another segment never matches the local file."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)
    segment = a.segment
    st.close()
    key = st.segment_key(segment)
    data, _ = store.get(key)
    store.delete(key)
    store.put_if_absent(key, encode_segment_header(uuid.uuid4()) + data[len(encode_segment_header(segment)):])
    st = _open(path, store)
    try:
        with pytest.raises(errors.ProtoUnexpectedError):
            st.ship(force=True)
    finally:
        st.close()


def test_a_corrupt_store_object_is_reported_as_corruption_not_as_a_refusal(tmp_path, caplog):
    """Task 8.3j re-review 2, N6: only an unreachable store or a missing object refuses the
    write (transient, conservative).  A store object that fails its integrity check is
    corruption: logged as an error and raised as ``ProtoCorruptionError``; nothing is
    written.  On 7cc6606 it was turned into a refusal like the rest."""
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    st = _open(path, store)
    a = st.push_blob(b"A" * 50)
    st.ship(force=True)
    segment = a.segment
    st.close()
    sealed = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
    os.remove(os.path.join(path, segment.hex + ".seg"))
    key = st.segment_key(segment)
    data, _ = store.get(key)
    store.delete(key)
    store.put_if_absent(key, encode_segment_header(uuid.uuid4()) + data[len(encode_segment_header(segment)):])
    st = _open(path, store)
    try:
        records = _carried(path, segment, [b"B" * 60], start=sealed)
        before = len(read_log(os.path.join(path, "roots.log"), b"ROOT"))
        with caplog.at_level("ERROR", logger="proto_db.storage.tiered"):
            with pytest.raises(errors.ProtoCorruptionError):
                st.accept_replication(3, _root(records), records)
        assert any(r.levelname == "ERROR" for r in caplog.records)
        assert len(read_log(os.path.join(path, "roots.log"), b"ROOT")) == before
        assert not os.path.exists(os.path.join(path, segment.hex + ".seg"))
    finally:
        st.close()
