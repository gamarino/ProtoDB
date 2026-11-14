"""Feature-versioned segments (ruling R151).

A record that uses a format feature is appended only to a segment whose header version is at
least the feature's version; otherwise the storage rolls to a new segment written at that
version.  A space that never uses a feature keeps writing version-4 segments and roots, and a
reader of an earlier revision refuses version-5 data with ``ProtoNotSupportedError``.
"""
import os
import struct
import uuid

import msgpack
import pytest

from proto_db.errors import ProtoNotSupportedError, ProtoValidationError
from proto_db.storage import format as fmt
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.storage.tiered import TieredStorage
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(60)]

PLAIN = {"t": "x", "oid": b"\x01" * 16, "s": {"n": 1}}
WITH_PARENTS = {"t": "x", "oid": b"\x02" * 16, "s": {"n": 2}, "p": ["base"]}
AFTER = {"t": "x", "oid": b"\x03" * 16, "s": {"n": 3}}


# --- one harness per storage --------------------------------------------------------------------

class _Harness:
    """Opens a storage over one place, reads back the header version of a segment and reopens."""

    persistent = True

    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.store = FakeObjectStore()

    def open(self):
        raise NotImplementedError

    def header_version(self, storage, segment) -> int:
        raise NotImplementedError


class _Memory(_Harness):
    persistent = False

    def open(self):
        return MemoryStorage()

    def header_version(self, storage, segment):
        return storage._segment_versions[segment]      # memory has no header bytes


class _File(_Harness):
    def open(self):
        return FileStorage(str(self.tmp_path / "space"), durability="none")

    def header_version(self, storage, segment):
        storage.flush()
        with open(os.path.join(storage.path, segment.hex + ".seg"), "rb") as f:
            return struct.unpack_from("<H", f.read(fmt.SEGMENT_HEADER_SIZE), 8)[0]


class _Tiered(_Harness):
    def open(self):
        return TieredStorage(str(self.tmp_path / "node"), self.store, durability="none", ship_interval=0.0,
                             backoff=(0.0, 0.0), retries=1)

    def header_version(self, storage, segment):
        storage.flush()
        with open(os.path.join(str(self.tmp_path / "node"), segment.hex + ".seg"), "rb") as f:
            return struct.unpack_from("<H", f.read(fmt.SEGMENT_HEADER_SIZE), 8)[0]


class _Cloud(_Harness):
    def open(self):
        return CloudStorage(self.store)

    def header_version(self, storage, segment):
        storage.flush()
        data, _ = self.store.get(storage.segment_key(segment))
        return struct.unpack_from("<H", data, 8)[0]


HARNESSES = [_Memory, _File, _Tiered, _Cloud]
PERSISTENT = [h for h in HARNESSES if h.persistent]


@pytest.fixture(params=HARNESSES, ids=lambda h: h.__name__.strip("_").lower())
def harness(request, tmp_path):
    return request.param(tmp_path)


@pytest.fixture(params=PERSISTENT, ids=lambda h: h.__name__.strip("_").lower())
def persistent(request, tmp_path):
    return request.param(tmp_path)


# --- the version table ----------------------------------------------------------------------------

def test_the_parents_feature_is_version_5_and_plain_data_stays_4():
    assert fmt.FEATURE_VERSIONS == {"parents": 5}
    assert fmt.FORMAT_VERSION == 5 and fmt.WRITE_FORMAT_VERSION == 4
    assert fmt.READABLE_FORMAT_VERSIONS == frozenset({2, 3, 4, 5})
    assert fmt.write_version() == 4 and fmt.write_version({"parents"}) == 5


def test_an_atom_declares_only_features_its_payload_uses():
    assert fmt.atom_version(PLAIN) == 4
    assert fmt.atom_version(WITH_PARENTS, {"parents"}) == 5
    with pytest.raises(ProtoValidationError):
        fmt.atom_version(WITH_PARENTS)                               # used, not declared
    with pytest.raises(ProtoValidationError):
        fmt.atom_version(PLAIN, {"parents"})                         # declared, not used
    with pytest.raises(ProtoValidationError):
        fmt.atom_version(PLAIN, {"no-such-feature"})


def test_a_record_tells_the_version_it_needs():
    plain = fmt.encode_record(fmt.RecordKind.ATOM, msgpack.packb(PLAIN, use_bin_type=True))
    parents = fmt.encode_record(fmt.RecordKind.ATOM, msgpack.packb(WITH_PARENTS, use_bin_type=True))
    blob = fmt.encode_record(fmt.RecordKind.BLOB, b"\x81\xa1p\x01")  # a blob is never an atom
    assert fmt.framed_version(plain) == 4 and fmt.framed_version(parents) == 5 and fmt.framed_version(blob) == 4


def test_a_segment_header_carries_its_version_and_a_v4_reader_refuses_v5(monkeypatch):
    seg = uuid.uuid4()
    assert fmt.segment_header_version(fmt.encode_segment_header(seg)) == 4
    raw = fmt.encode_segment_header(seg, 5)
    assert fmt.segment_header_version(raw) == 5 and fmt.decode_segment_header(raw) == seg
    monkeypatch.setattr(fmt, "READABLE_FORMAT_VERSIONS", frozenset({2, 3, 4}))
    with pytest.raises(ProtoNotSupportedError):
        fmt.decode_segment_header(raw)


def test_a_root_keeps_version_5_when_read_and_a_v4_reader_refuses_it(monkeypatch):
    seg = uuid.uuid4()
    assert fmt.decode_root(fmt.encode_root(fmt.RootRecord(5, seg, 26, 3))).version == 5
    assert fmt.decode_root(fmt.encode_root(fmt.RootRecord(2, seg, 26, 3))).version == 4   # read as current
    monkeypatch.setattr(fmt, "READABLE_FORMAT_VERSIONS", frozenset({2, 3, 4}))
    with pytest.raises(ProtoNotSupportedError):
        fmt.decode_root(fmt.encode_root(fmt.RootRecord(5, seg, 26, 3)))


# --- every storage ----------------------------------------------------------------------------------

def test_a_featureless_space_writes_only_v4_segments(harness):
    st = harness.open()
    ptrs = [st.push_atom(dict(PLAIN, s={"n": i})) for i in range(3)] + [st.push_blob(b"blob")]
    assert {harness.header_version(st, p.segment) for p in ptrs} == {4}
    st.close()


def test_a_parents_record_lands_in_a_v5_segment(harness):
    st = harness.open()
    plain = st.push_atom(PLAIN)
    parents = st.push_atom(WITH_PARENTS, features={"parents"})
    after = st.push_atom(AFTER)                       # a v5 segment takes later v4 records
    assert parents.segment != plain.segment
    assert harness.header_version(st, plain.segment) == 4
    assert harness.header_version(st, parents.segment) == 5
    assert after.segment == parents.segment           # the rule rolls only upward
    assert [st.get_atom(p) for p in (plain, parents, after)] == [PLAIN, WITH_PARENTS, AFTER]
    st.close()


def test_declared_features_must_match_the_payload(harness):
    st = harness.open()
    with pytest.raises(ProtoValidationError):
        st.push_atom(WITH_PARENTS)                    # uses parents without declaring it
    with pytest.raises(ProtoValidationError):
        st.push_atom(PLAIN, features={"parents"})     # declares parents without using it
    ptr = st.push_atom(PLAIN)
    assert harness.header_version(st, ptr.segment) == 4   # nothing refused was written
    st.close()


def test_a_v4_reader_refuses_the_v5_segment(persistent, monkeypatch):
    st = persistent.open()
    plain = st.push_atom(PLAIN)
    ptr = st.push_atom(WITH_PARENTS, features={"parents"})
    st.flush()
    st.close()
    monkeypatch.setattr(fmt, "READABLE_FORMAT_VERSIONS", frozenset({2, 3, 4}))
    if isinstance(persistent, _Tiered):
        # A tiered node checks its own segments when it opens: it refuses to open at all.
        with pytest.raises(ProtoNotSupportedError):
            persistent.open()
        return
    reader = persistent.open()                         # opening reads no segment
    try:
        assert reader.get_atom(plain) == PLAIN         # the v4 segment of the space stays readable
        with pytest.raises(ProtoNotSupportedError):
            reader.get_atom(ptr)                       # the read of the v5 segment refuses
    finally:
        reader.close()


def test_reopen_reads_mixed_v4_and_v5_segments(persistent):
    st = persistent.open()
    plain = st.push_atom(PLAIN)
    parents = st.push_atom(WITH_PARENTS, features={"parents"})
    after = st.push_atom(AFTER)
    blob = st.push_blob(b"tail")
    st.flush()
    st.close()
    st = persistent.open()
    assert [st.get_atom(p) for p in (plain, parents, after)] == [PLAIN, WITH_PARENTS, AFTER]
    assert st.get_blob(blob) == b"tail"
    assert persistent.header_version(st, plain.segment) == 4
    assert persistent.header_version(st, parents.segment) == 5
    st.close()


# --- root records ------------------------------------------------------------------------------------

@pytest.mark.parametrize("make", [_File, _Cloud], ids=["file", "cloud"])
def test_a_root_is_v4_until_the_space_writes_v5_and_stays_v5_after(make, tmp_path, monkeypatch):
    h = make(tmp_path)
    st = h.open()
    first = st.publish_root(st.push_atom(PLAIN), 0) if make is _File else _flushed_publish(st, PLAIN, 0)
    assert first.version == 4 and st.read_root().version == 4
    second = (st.publish_root(st.push_atom(WITH_PARENTS, features={"parents"}), 1) if make is _File
              else _flushed_publish(st, WITH_PARENTS, 1, features={"parents"}))
    assert second.version == 5 and st.read_root().version == 5
    st.close()
    st = h.open()                                      # another writer, featureless: never lower
    third = st.publish_root(st.push_atom(AFTER), 2) if make is _File else _flushed_publish(st, AFTER, 2)
    assert third.version == 5 and st.read_root().version == 5
    st.close()
    monkeypatch.setattr(fmt, "READABLE_FORMAT_VERSIONS", frozenset({2, 3, 4}))
    with pytest.raises(ProtoNotSupportedError):
        h.open().read_root()


def _flushed_publish(st, payload, epoch, **kw):
    ptr = st.push_atom(payload, **kw)
    st.flush()
    return st.publish_root(ptr, epoch)


def test_a_tiered_persisted_root_follows_the_space_version(tmp_path):
    h = _Tiered(tmp_path)
    st = h.open()
    plain = st.push_atom(PLAIN)
    assert st.persist(1, fmt.RootRecord(fmt.WRITE_FORMAT_VERSION, plain.segment, plain.offset, 1))
    assert st.read_persisted()[1].version == 4
    parents = st.push_atom(WITH_PARENTS, features={"parents"})
    assert st.persist(1, fmt.RootRecord(fmt.WRITE_FORMAT_VERSION, parents.segment, parents.offset, 2))
    assert st.read_persisted()[1].version == 5
    st.close()


# --- cluster copies: the header a receiver writes -----------------------------------------------------

def _peer(tmp_path, store, name):
    return TieredStorage(str(tmp_path / name), store, durability="none", ship_interval=0.0,
                         backoff=(0.0, 0.0), retries=1)


def _file_version(tmp_path, name, segment):
    with open(os.path.join(str(tmp_path / name), segment.hex + ".seg"), "rb") as f:
        return struct.unpack_from("<H", f.read(fmt.SEGMENT_HEADER_SIZE), 8)[0]


def test_a_replicated_copy_gets_the_header_version_of_its_segment(tmp_path):
    store = FakeObjectStore()
    writer = _peer(tmp_path, store, "w")
    plain = writer.push_atom(PLAIN)
    parents = writer.push_atom(WITH_PARENTS, features={"parents"})
    after = writer.push_atom(AFTER)
    records = writer.outgoing()
    root = fmt.RootRecord(fmt.WRITE_FORMAT_VERSION, after.segment, after.offset, 3)
    copy = _peer(tmp_path, store, "c")
    copy.apply(records, root)
    assert _file_version(tmp_path, "c", plain.segment) == 4
    assert _file_version(tmp_path, "c", parents.segment) == 5
    # A replica that receives only a later record of the v5 segment cannot know its first record:
    # it writes the highest version, never a lower one than the segment's.
    late = _peer(tmp_path, store, "l")
    late.apply([r for r in records if r[1] == after.offset and r[0] == after.segment.bytes], root)
    assert _file_version(tmp_path, "l", after.segment) == 5
    # ... and the first record, arriving later, sets the exact version (here the same).
    late.apply(records, root)
    assert _file_version(tmp_path, "l", after.segment) == 5
    assert late.get_atom(parents) == WITH_PARENTS
    for st in (writer, copy, late):
        st.close()


def test_a_late_copy_of_a_v4_segment_is_corrected_when_its_first_record_arrives(tmp_path):
    store = FakeObjectStore()
    writer = _peer(tmp_path, store, "w")
    ptrs = [writer.push_atom(dict(PLAIN, s={"n": i})) for i in range(3)]
    records = writer.outgoing()
    root = fmt.RootRecord(fmt.WRITE_FORMAT_VERSION, ptrs[2].segment, ptrs[2].offset, 3)
    late = _peer(tmp_path, store, "l")
    late.apply(records[1:], root)                      # first record unknown: conservative
    assert _file_version(tmp_path, "l", ptrs[0].segment) == fmt.FORMAT_VERSION
    late.apply(records[:1], root)                      # the first record: exact
    assert _file_version(tmp_path, "l", ptrs[0].segment) == 4
    writer.close()
    late.close()


def test_a_part_uploaded_from_a_copy_carries_the_segment_version(tmp_path):
    store = FakeObjectStore()
    writer = _peer(tmp_path, store, "w")
    writer.push_atom(PLAIN)
    parents = writer.push_atom(WITH_PARENTS, features={"parents"})
    after = writer.push_atom(AFTER)
    root = fmt.RootRecord(fmt.WRITE_FORMAT_VERSION, after.segment, after.offset, 3)
    records = writer.outgoing()
    survivor = _peer(tmp_path, store, "s")
    survivor.apply(records, root)
    writer.close()                                     # lost before shipping anything
    needs = [(segment, end) for segment, _, end in survivor.commit_ranges(root)]
    assert survivor.ensure_remote(needs, upload_copies=True)
    parts = [k for k in store._objects if parents.segment.hex in k]
    assert parts and all(struct.unpack_from("<H", store._objects[k][0], 8)[0] == 5 for k in parts
                         if not k.endswith(".sealed"))
    survivor.close()


def test_a_tiered_node_reopens_a_v5_own_segment(tmp_path):
    store = FakeObjectStore()
    st = _peer(tmp_path, store, "n")
    ptr = st.push_atom(WITH_PARENTS, features={"parents"})
    st.flush()
    st.close()
    st = _peer(tmp_path, store, "n")                   # the own-segment repair accepts its header
    assert st.get_atom(ptr) == WITH_PARENTS and _file_version(tmp_path, "n", ptr.segment) == 5
    st.close()


def test_a_part_filled_from_a_peer_carries_the_exact_header_not_the_rebuilt_one(tmp_path):
    store = FakeObjectStore()
    writer = _peer(tmp_path, store, "w")
    ptrs = [writer.push_atom(dict(PLAIN, s={"n": i})) for i in range(3)]
    records = writer.outgoing()
    root = fmt.RootRecord(fmt.WRITE_FORMAT_VERSION, ptrs[2].segment, ptrs[2].offset, 3)
    survivor = _peer(tmp_path, store, "s")
    survivor.apply(records[1:], root)                  # rebuilt without the first record: version 5
    assert _file_version(tmp_path, "s", ptrs[0].segment) == fmt.FORMAT_VERSION
    survivor.set_fetcher(lambda ptr: writer.local_record(ptr))
    needs = [(segment, end) for segment, _, end in survivor.commit_ranges(root)]
    assert survivor.ensure_remote(needs, upload_copies=True)
    parts = [k for k in store._objects if ptrs[0].segment.hex in k and not k.endswith(".sealed")]
    assert parts and {struct.unpack_from("<H", store._objects[k][0], 8)[0] for k in parts} == {4}
    writer.close()
    survivor.close()


def test_a_segment_header_prefix_tells_its_version_when_the_magic_and_version_bytes_are_there():
    seg = uuid.uuid4()
    raw = fmt.encode_segment_header(seg, 5)
    assert fmt.peek_segment_header(raw) == (True, 5)
    assert fmt.peek_segment_header(raw[:10]) == (True, 5)
    assert fmt.peek_segment_header(raw[:9]) == (True, None)       # magic intact, version torn
    assert fmt.peek_segment_header(b"PDB2SE") == (True, None)     # a torn magic is a prefix
    assert fmt.peek_segment_header(b"XXXXXXXX" + raw[8:]) == (False, None)


def test_a_closed_memory_storage_keeps_no_segment_table():
    st = MemoryStorage()
    st.push_atom(WITH_PARENTS, features={"parents"})
    st.close()
    assert st._segments == {} and st._segment_versions == {}


# --- the root version in cluster mode (review I2) --------------------------------------------------

def test_the_next_root_version_is_the_space_version():
    st = MemoryStorage()
    seg = uuid.uuid4()
    assert fmt.next_root_version(None, st) == 4
    assert fmt.next_root_version(fmt.RootRecord(5, seg, 26, 1), st) == 5     # never lower than the base
    st.push_atom(WITH_PARENTS, features={"parents"})
    assert st.written_version == 5 and fmt.next_root_version(fmt.RootRecord(4, seg, 26, 1), st) == 5


def test_a_wire_root_carries_its_version_and_a_v4_root_keeps_its_bytes():
    from proto_db.cluster import protocol
    seg = uuid.uuid4()
    v4 = fmt.RootRecord(4, seg, 26, 3)
    assert protocol.encode_root(v4, known=True, term=7) == [3, seg.bytes, 26, 7]          # unchanged shape
    wire = protocol.encode_root(fmt.RootRecord(5, seg, 26, 3), known=True, term=7)
    assert wire == [3, seg.bytes, 26, 7, 5]
    known, record, term = protocol.decode_labeled_root(wire)
    assert known and term == 7 and record.version == 5
    assert protocol.decode_labeled_root([3, seg.bytes, 26, 7])[1].version == 4
    with pytest.raises(protocol.ClusterProtocolError):
        protocol.decode_labeled_root([3, seg.bytes, 26, 7, fmt.FORMAT_VERSION + 1])  # unreadable here


def test_a_root_replicated_to_a_peer_keeps_its_version():
    from tests.support.manual_cluster import Harness
    h = Harness("abc")
    try:
        h.bootstrap("a")
        lease = h.attempt("a", {"b", "c"})
        root = fmt.RootRecord(5, uuid.uuid4(), 64, 2)
        assert h.replicate("a", lease, root, {"b", "c"}) is not None
        for name in "bc":
            assert h.nodes[name].accepted_root()[1] == root
            assert h.nodes[name].accepted_root()[1].version == 5
    finally:
        h.close()


def test_a_tiered_node_persists_the_version_of_a_root_it_did_not_write(tmp_path):
    st = _Tiered(tmp_path).open()
    plain = st.push_atom(PLAIN)                       # this node writes only v4 data
    assert st.persist(1, fmt.RootRecord(4, plain.segment, plain.offset, 1))
    assert st.read_persisted()[1].version == 4
    # A root another node committed over v5 data reaches this node as v5: persisted as v5.
    assert st.persist(1, fmt.RootRecord(5, plain.segment, plain.offset + 1, 2))
    assert st.read_persisted()[1].version == 5
    # The same root held at a lower version is corrected, never kept lower.
    other = _Tiered(tmp_path / "o").open()
    p = other.push_atom(PLAIN)
    assert other.persist(1, fmt.RootRecord(4, p.segment, p.offset, 1))
    assert other.persist(1, fmt.RootRecord(5, p.segment, p.offset, 1))
    assert other.read_persisted()[1].version == 5
    st.close()
    other.close()


# --- the acceptor log (review I1) --------------------------------------------------------------------

def _roots_log(tmp_path):
    from proto_db.cluster.rootlog import RootsLog
    return RootsLog(str(tmp_path / "log" / "roots.log"))


def _log_header_version(tmp_path):
    with open(tmp_path / "log" / "roots.log", "rb") as f:
        return struct.unpack_from("<H", f.read(10), 8)[0]


def _records(payload, offset=fmt.SEGMENT_HEADER_SIZE):
    return [[uuid.uuid4().bytes, offset, fmt.encode_record(fmt.RecordKind.ATOM,
                                                           msgpack.packb(payload, use_bin_type=True))]]


def test_a_featureless_acceptor_log_keeps_log_version_2_and_its_entry_kinds(tmp_path):
    from proto_db.storage.logfile import LOG_MAGIC, read_log
    log = _roots_log(tmp_path)
    root = fmt.RootRecord(4, uuid.uuid4(), 64, 1)
    log.append("accepted", 7, root)
    log.append_records(7, fmt.RootRecord(4, uuid.uuid4(), 64, 2), _records(PLAIN))
    log.close()
    with open(tmp_path / "log" / "roots.log", "rb") as f:
        assert f.read(10) == LOG_MAGIC + (2).to_bytes(2, "little")
    assert [p[0] for p in read_log(str(tmp_path / "log" / "roots.log"), b"ROOT")] == [1, 5]


def test_a_v5_entry_is_versioned_and_read_back_with_its_version(tmp_path):
    from proto_db.cluster import rootlog
    log = _roots_log(tmp_path)
    log.append("accepted", 7, fmt.RootRecord(4, uuid.uuid4(), 64, 1))
    assert _log_header_version(tmp_path) == 2
    root = fmt.RootRecord(5, uuid.uuid4(), 64, 2)
    records = _records(WITH_PARENTS)
    log.append_records(8, root, records)
    assert _log_header_version(tmp_path) == 3         # the log now holds a format-5 entry
    log.close()
    log = _roots_log(tmp_path)
    accepted = log.load().accepted
    assert accepted == (8, root) and accepted[1].version == 5
    redo = log.take_redo()
    assert len(redo) == 1 and redo[0].root.version == 5 and redo[0].records == records
    log.close()
    assert rootlog.VERSIONED_KIND == 7


def test_a_compacted_log_keeps_the_root_version(tmp_path):
    log = _roots_log(tmp_path)
    root = fmt.RootRecord(5, uuid.uuid4(), 64, 2)
    log.append("accepted", 8, root)
    log.append("committed", 8, root)
    log.mark_rejoined()
    log.compact_records(log.mark())
    log.close()
    log = _roots_log(tmp_path)
    loaded = log.load()
    assert loaded.accepted[1].version == 5 and loaded.committed[1].version == 5
    assert _log_header_version(tmp_path) == 3
    log.close()


def test_an_older_reader_refuses_a_log_holding_a_v5_entry(tmp_path, monkeypatch):
    from proto_db.cluster import rootlog
    from proto_db.errors import ProtoCorruptionError
    from proto_db.storage import logfile
    from proto_db.storage.logfile import read_log
    log = _roots_log(tmp_path)
    log.append_records(8, fmt.RootRecord(5, uuid.uuid4(), 64, 2), _records(WITH_PARENTS))
    log.close()
    path = str(tmp_path / "log" / "roots.log")
    payloads = read_log(path, b"ROOT")
    # An older revision knows entry kinds 1 to 6: it refuses the versioned entry, never misreads it.
    monkeypatch.setattr(rootlog, "_KNOWN_KINDS", frozenset({1, 2, 3, 4, 5, 6}))
    with pytest.raises(ProtoCorruptionError):
        rootlog._decode(payloads, path)
    monkeypatch.undo()
    # ... and an older revision of the log format refuses the file at its header.
    monkeypatch.setattr(logfile, "READABLE_LOG_VERSIONS", frozenset({2}))
    with pytest.raises(ProtoNotSupportedError):
        read_log(path, b"ROOT")
