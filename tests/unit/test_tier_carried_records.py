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
import uuid

import pytest

from proto_db.core.atom import AtomPointer
from proto_db.errors import ProtoCorruptionError
from proto_db.storage import tiered
from proto_db.storage.format import FORMAT_VERSION, RecordKind, RootRecord, encode_record
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


def test_a_carried_record_that_would_leave_a_hole_is_refused_before_anything_is_logged(tmp_path):
    store = FakeObjectStore()
    path = str(tmp_path / "n0")
    segment = _earlier_instance(path, store)
    st = _open(path, store)
    try:
        end = os.path.getsize(os.path.join(path, segment.hex + ".seg"))
        records = _carried(path, segment, [b"B" * 60], start=end + 100)
        before = len(read_log(os.path.join(path, "roots.log"), b"ROOT"))
        with pytest.raises(ProtoCorruptionError):
            st.accept_replication(3, _root(records), records)
        assert len(read_log(os.path.join(path, "roots.log"), b"ROOT")) == before
        assert os.path.getsize(os.path.join(path, segment.hex + ".seg")) == end
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
