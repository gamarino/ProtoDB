"""FileStorage: the storage contract plus durability, recovery, locking and layout rules.

Catches: committed data lost or unreadable after reopen, a corrupt root opening as an empty
space, appends into another process's segment, roots published before their data, missing
process exclusion, I/O failures surfacing as anything but ProtoUnexpectedError, and storages
that keep accepting writes after a failed flush.
"""
import os
import subprocess
import sys
import textwrap
import threading
import time
import uuid

import pytest

from proto_db.core.atom import Atom, AtomPointer
from proto_db.errors import (ProtoConflictError, ProtoCorruptionError, ProtoNotSupportedError,
                             ProtoTimeoutError, ProtoUnexpectedError, ProtoValidationError)
from proto_db.storage import file as file_mod
from proto_db.storage.base import SharedStorage
from proto_db.storage.file import FileStorage, OsFileSystem
from proto_db.storage.format import (SEGMENT_HEADER_SIZE, RootRecord, decode_segment_header,
                                     encode_root)
from tests.contracts.storage_contract import StorageContract, atom_payload
from tests.support.faulty_fs import FaultPlan, FaultyFS, InjectedFault
from tests.support.persistence import DirectContext

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestFileStorageContract(StorageContract):
    @pytest.fixture(autouse=True)
    def _dir(self, tmp_path):
        self._tmp = tmp_path
        self._n = 0

    def make_storage(self):
        self._n += 1
        return FileStorage(str(self._tmp / f"space{self._n}"), durability="none")


class TestFileStorageContractFsync(TestFileStorageContract):
    """The contract again with real fsyncs and a tiny page size and segment limit."""

    def make_storage(self):
        self._n += 1
        return FileStorage(str(self._tmp / f"space{self._n}"), page_size=512,
                           segment_max_bytes=64 * 1024)


class CountingFS(OsFileSystem):
    """Records every adapter call in order."""

    def __init__(self):
        self.calls = []

    def open(self, path, flags, mode=0o644):
        self.calls.append(("open", os.path.basename(path)))
        return super().open(path, flags, mode)

    def write(self, fd, data, offset):
        self.calls.append(("write", offset, len(data)))
        return super().write(fd, data, offset)

    def fsync(self, fd):
        self.calls.append(("fsync",))
        super().fsync(fd)

    def replace(self, src, dst):
        self.calls.append(("rename", os.path.basename(dst)))
        super().replace(src, dst)

    def fsync_dir(self, path):
        self.calls.append(("dirsync",))
        super().fsync_dir(path)

    def ops(self):
        return [c[0] for c in self.calls]


def segments(path):
    return sorted(f for f in os.listdir(path) if f.endswith(".seg"))


def test_is_a_shared_storage(tmp_path):
    s = FileStorage(str(tmp_path / "s"))
    try:
        assert isinstance(s, SharedStorage)
    finally:
        s.close()


def test_argument_validation(tmp_path):
    p = str(tmp_path / "s")
    for kwargs in ({"durability": "always"}, {"cache_bytes": -1}, {"lock_timeout": -1},
                   {"segment_max_bytes": 10}, {"page_size": 0}):
        with pytest.raises(ProtoValidationError):
            FileStorage(p, **kwargs)
    with pytest.raises(ProtoValidationError):
        FileStorage(str(tmp_path / "a-file"), _fs="not an adapter")


def test_path_that_is_a_file_is_rejected(tmp_path):
    f = tmp_path / "plain"
    f.write_bytes(b"x")
    with pytest.raises(ProtoValidationError):
        FileStorage(str(f))


def test_open_existing_requires_the_directory(tmp_path):
    with pytest.raises(ProtoValidationError):
        FileStorage.open_existing(str(tmp_path / "missing"))
    s = FileStorage(str(tmp_path / "s"))
    s.close()
    s = FileStorage.open_existing(str(tmp_path / "s"), durability="none")
    try:
        assert s.read_root() is None
    finally:
        s.close()


def test_creates_directory_but_no_segment_until_written(tmp_path):
    p = tmp_path / "s"
    s = FileStorage(str(p))
    try:
        assert p.is_dir() and segments(p) == []
        s.push_atom(atom_payload(1))
        assert segments(p) == []            # still only in the write buffer
        s.flush()
        assert len(segments(p)) == 1
    finally:
        s.close()


def test_segment_layout_and_header(tmp_path):
    p = tmp_path / "s"
    s = FileStorage(str(p))
    try:
        ptr = s.push_atom(atom_payload(1))
        s.flush()
        assert ptr.offset == SEGMENT_HEADER_SIZE
        (name,) = segments(p)
        assert name == f"{ptr.segment.hex}.seg"
        with open(p / name, "rb") as f:
            assert decode_segment_header(f.read()) == ptr.segment
    finally:
        s.close()


def test_unflushed_data_is_served_from_memory(tmp_path):
    s = FileStorage(str(tmp_path / "s"))
    try:
        ptrs = [s.push_atom(atom_payload(i)) for i in range(20)]
        blob = s.push_blob(b"\x00" * 1000)
        assert [s.get_atom(p) for p in ptrs] == [atom_payload(i) for i in range(20)]
        assert s.get_blob(blob) == b"\x00" * 1000
    finally:
        s.close()


def test_reopen_reads_committed_data(tmp_path):
    p = str(tmp_path / "s")
    s = FileStorage(p)
    ptrs = [s.push_atom(atom_payload(i)) for i in range(50)]
    blob = s.push_blob(bytes(range(256)) * 300)
    root = s.publish_root(ptrs[-1], 0)
    s.close()
    s = FileStorage(p)
    try:
        assert s.read_root() == root
        assert [s.get_atom(x) for x in ptrs] == [atom_payload(i) for i in range(50)]
        assert s.get_blob(blob) == bytes(range(256)) * 300
    finally:
        s.close()


def test_new_instance_never_appends_to_existing_segments(tmp_path):
    p = tmp_path / "s"
    s = FileStorage(str(p))
    first = s.push_atom(atom_payload(1))
    s.publish_root(first, 0)
    s.close()
    (old,) = segments(p)
    size = os.path.getsize(p / old)
    s = FileStorage(str(p))
    try:
        second = s.push_atom(atom_payload(2))
        s.publish_root(second, 1)
        assert second.segment != first.segment
        assert os.path.getsize(p / old) == size
        assert len(segments(p)) == 2
        assert s.get_atom(first) == atom_payload(1)
    finally:
        s.close()


def test_rolls_to_a_new_segment_when_full(tmp_path):
    p = tmp_path / "s"
    s = FileStorage(str(p), durability="none", segment_max_bytes=1024)
    try:
        ptrs = [s.push_atom(atom_payload(i)) for i in range(100)]
        assert len({x.segment for x in ptrs}) > 3
        s.flush()
        for name in segments(p):
            assert os.path.getsize(p / name) <= 1024
        assert [s.get_atom(x) for x in ptrs] == [atom_payload(i) for i in range(100)]
    finally:
        s.close()


def test_record_larger_than_segment_gets_its_own_segment(tmp_path):
    s = FileStorage(str(tmp_path / "s"), durability="none", segment_max_bytes=1024)
    try:
        a = s.push_atom(atom_payload(1))
        big = s.push_blob(b"z" * 5000)
        c = s.push_atom(atom_payload(2))
        assert len({a.segment, big.segment, c.segment}) == 3
        s.flush()
        assert s.get_blob(big) == b"z" * 5000
    finally:
        s.close()


def test_reads_after_flush_go_through_the_page_cache(tmp_path):
    s = FileStorage(str(tmp_path / "s"), page_size=256)
    try:
        ptrs = [s.push_atom(atom_payload(i)) for i in range(200)]
        s.flush()
        for _ in range(2):
            assert [s.get_atom(x) for x in ptrs] == [atom_payload(i) for i in range(200)]
        assert s._cache.hits > 0 and s._cache.size_bytes > 0
        assert all(len(v[0]) == 256 for v in s._cache._items.values())
    finally:
        s.close()


def test_stale_cached_page_is_reread(tmp_path):
    """A cached page captured while another writer was appending must not fake corruption."""
    p = tmp_path / "s"
    s = FileStorage(str(p), page_size=256)
    try:
        ptr = s.push_atom(atom_payload(1))
        s.flush()
        page_no = ptr.offset // 256
        s._cache.put((ptr.segment, page_no), b"\x00" * 256)
        assert s.get_atom(ptr) == atom_payload(1)
    finally:
        s.close()


def test_fsync_ordering_data_before_root(tmp_path):
    fs = CountingFS()
    s = FileStorage(str(tmp_path / "s"), _fs=fs)
    try:
        ptr = s.push_atom(atom_payload(1))
        s.publish_root(ptr, 0)
        assert fs.ops() == ["open", "write", "fsync", "dirsync",       # new segment
                            "open", "write", "fsync", "rename", "dirsync"]  # root
        assert fs.calls[4] == ("open", "space.root.tmp")
        fs.calls.clear()
        s.publish_root(s.push_atom(atom_payload(2)), 1)
        assert fs.ops() == ["write", "fsync", "open", "write", "fsync", "rename", "dirsync"]
    finally:
        s.close()


def test_durability_modes(tmp_path):
    fs = CountingFS()
    s = FileStorage(str(tmp_path / "flush"), durability="flush", _fs=fs)
    s.publish_root(s.push_atom(atom_payload(1)), 0)
    s.close()
    assert fs.ops() == ["open", "write", "open", "write", "fsync", "rename", "dirsync"]
    fs = CountingFS()
    s = FileStorage(str(tmp_path / "none"), durability="none", _fs=fs)
    s.publish_root(s.push_atom(atom_payload(1)), 0)
    s.close()
    assert fs.ops() == ["open", "write", "open", "write", "rename"]


def test_large_write_buffer_is_written_out_early(tmp_path):
    p = tmp_path / "s"
    s = FileStorage(str(p), durability="none", write_buffer_bytes=4096)
    try:
        ptrs = [s.push_blob(bytes([i]) * 1000) for i in range(20)]
        assert segments(p) and os.path.getsize(p / segments(p)[0]) > 4096
        assert [s.get_blob(x) for x in ptrs] == [bytes([i]) * 1000 for i in range(20)]
    finally:
        s.close()


def test_flush_after_early_write_still_fsyncs(tmp_path):
    fs = CountingFS()
    s = FileStorage(str(tmp_path / "s"), write_buffer_bytes=1024, _fs=fs)
    try:
        s.push_blob(b"x" * 2000)            # written out early without fsync
        assert "write" in fs.ops() and "fsync" not in fs.ops()
        s.flush()
        assert fs.ops().count("fsync") == 1 and fs.ops().count("dirsync") == 1
    finally:
        s.close()


# --- recovery --------------------------------------------------------------------------

def _committed_space(path):
    s = FileStorage(path)
    ptr = s.push_atom(atom_payload(1))
    root = s.publish_root(ptr, 0)
    s.close()
    return ptr, root


def test_missing_root_is_an_empty_space(tmp_path):
    s = FileStorage(str(tmp_path / "s"))
    try:
        assert s.read_root() is None
    finally:
        s.close()


@pytest.mark.parametrize("damage", ["truncate", "flip", "empty", "garbage"])
def test_corrupt_root_is_never_empty(tmp_path, damage):
    p = tmp_path / "s"
    _committed_space(str(p))
    data = (p / "space.root").read_bytes()
    damaged = {"truncate": data[:len(data) // 2], "flip": data[:-1] + bytes([data[-1] ^ 1]),
               "empty": b"", "garbage": b"\xff" * 40}[damage]
    (p / "space.root").write_bytes(damaged)
    with pytest.raises(ProtoCorruptionError):
        FileStorage(str(p))


def test_root_corrupted_after_open_raises_on_read(tmp_path):
    p = tmp_path / "s"
    _committed_space(str(p))
    s = FileStorage(str(p))
    try:
        (p / "space.root").write_bytes(b"\x00\x01")
        with pytest.raises(ProtoCorruptionError):
            s.read_root()
        with pytest.raises(ProtoCorruptionError):
            s.publish_root(s.push_atom(atom_payload(2)), 1)
    finally:
        s.close()


def test_missing_root_segment_is_corruption(tmp_path):
    p = tmp_path / "s"
    ptr, _ = _committed_space(str(p))
    os.unlink(p / f"{ptr.segment.hex}.seg")
    with pytest.raises(ProtoCorruptionError):
        FileStorage(str(p))


def test_corrupt_root_record_is_corruption(tmp_path):
    p = tmp_path / "s"
    ptr, _ = _committed_space(str(p))
    seg = p / f"{ptr.segment.hex}.seg"
    data = bytearray(seg.read_bytes())
    data[-1] ^= 0xFF
    seg.write_bytes(bytes(data))
    with pytest.raises(ProtoCorruptionError):
        FileStorage(str(p))


def test_truncated_root_record_is_corruption(tmp_path):
    p = tmp_path / "s"
    ptr, _ = _committed_space(str(p))
    seg = p / f"{ptr.segment.hex}.seg"
    os.truncate(seg, os.path.getsize(seg) - 3)
    with pytest.raises(ProtoCorruptionError):
        FileStorage(str(p))


def test_root_pointing_past_the_segment_is_corruption(tmp_path):
    p = tmp_path / "s"
    ptr, root = _committed_space(str(p))
    bad = RootRecord(root.version, root.segment, root.offset + 10_000, root.epoch)
    (p / "space.root").write_bytes(encode_root(bad))
    with pytest.raises(ProtoCorruptionError):
        FileStorage(str(p))


def test_segment_with_bad_header_is_corruption(tmp_path):
    p = tmp_path / "s"
    ptr, _ = _committed_space(str(p))
    seg = p / f"{ptr.segment.hex}.seg"
    data = bytearray(seg.read_bytes())
    data[0] ^= 0xFF
    seg.write_bytes(bytes(data))
    with pytest.raises(ProtoCorruptionError):
        FileStorage(str(p))


def test_segment_whose_header_names_another_uuid_is_corruption(tmp_path):
    p = tmp_path / "s"
    ptr, _ = _committed_space(str(p))
    seg = p / f"{ptr.segment.hex}.seg"
    data = bytearray(seg.read_bytes())
    data[10:26] = uuid.uuid4().bytes
    seg.write_bytes(bytes(data))
    with pytest.raises(ProtoCorruptionError):
        FileStorage(str(p))


def test_leftover_tmp_root_is_ignored(tmp_path):
    p = tmp_path / "s"
    ptr, root = _committed_space(str(p))
    (p / "space.root.tmp").write_bytes(b"half-written garbage")
    s = FileStorage(str(p))
    try:
        assert s.read_root() == root
        r2 = s.publish_root(s.push_atom(atom_payload(2)), 1)
        assert s.read_root() == r2
    finally:
        s.close()
    s = FileStorage(str(p))
    try:
        assert s.read_root() == r2
    finally:
        s.close()


def test_torn_tail_of_a_dead_segment_is_ignored(tmp_path):
    p = tmp_path / "s"
    ptr, root = _committed_space(str(p))
    with open(p / f"{ptr.segment.hex}.seg", "ab") as f:
        f.write(b"\x40\x00\x00\x00\x01garbage-tail")
    s = FileStorage(str(p))
    try:
        assert s.read_root() == root and s.get_atom(ptr) == atom_payload(1)
    finally:
        s.close()


def test_unknown_offsets_are_corruption(tmp_path):
    s = FileStorage(str(tmp_path / "s"))
    try:
        ptr = s.push_atom(atom_payload(1))
        with pytest.raises(ProtoCorruptionError):
            s.get_atom(AtomPointer(ptr.segment, 0))
        with pytest.raises(ProtoCorruptionError):
            s.get_atom(AtomPointer(ptr.segment, 10_000))
        s.flush()
        with pytest.raises(ProtoCorruptionError):
            s.get_atom(AtomPointer(ptr.segment, 3))
        with pytest.raises(ProtoCorruptionError):
            s.get_atom(AtomPointer(ptr.segment, 10_000))
    finally:
        s.close()


def test_two_instances_see_each_others_roots(tmp_path):
    p = str(tmp_path / "s")
    a, b = FileStorage(p), FileStorage(p)
    try:
        ra = a.publish_root(a.push_atom(atom_payload(1)), 0)
        assert b.read_root() == ra
        assert b.get_atom(ra.pointer) == atom_payload(1)
        with pytest.raises(ProtoConflictError):
            b.publish_root(b.push_atom(atom_payload(2)), 0)
        rb = b.publish_root(b.push_atom(atom_payload(3)), 1)
        assert a.read_root() == rb and a.get_atom(rb.pointer) == atom_payload(3)
    finally:
        a.close()
        b.close()


# --- locking ---------------------------------------------------------------------------

def test_two_instances_in_one_process_exclude_each_other(tmp_path):
    p = str(tmp_path / "s")
    a, b = FileStorage(p), FileStorage(p)
    try:
        with a.root_lock():
            with pytest.raises(ProtoTimeoutError):
                with b.root_lock(timeout=0.1):
                    pytest.fail("both instances held the root lock")
        with b.root_lock(timeout=1):
            pass
    finally:
        a.close()
        b.close()


def test_publish_waits_for_another_instances_root_lock(tmp_path):
    p = str(tmp_path / "s")
    a, b = FileStorage(p), FileStorage(p, lock_timeout=0.1)
    try:
        with a.root_lock():
            with pytest.raises(ProtoTimeoutError):
                b.publish_root(b.push_atom(atom_payload(1)), 0)
            assert a.read_root() is None
            r = a.publish_root(a.push_atom(atom_payload(2)), 0)   # holder may publish
        assert b.read_root() == r
    finally:
        a.close()
        b.close()


def test_root_lock_excludes_other_processes(tmp_path):
    p = str(tmp_path / "s")
    FileStorage(p).close()
    script = textwrap.dedent(f"""
        import sys, time
        from proto_db.storage.file import FileStorage
        s = FileStorage({p!r})
        with s.root_lock():
            print("locked", flush=True)
            sys.stdin.readline()
        print("released", flush=True)
    """)
    env = dict(os.environ, PYTHONPATH=REPO)
    child = subprocess.Popen([sys.executable, "-c", script], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, text=True, env=env)
    s = FileStorage(p)
    try:
        assert child.stdout.readline().strip() == "locked"
        with pytest.raises(ProtoTimeoutError):
            with s.root_lock(timeout=0.2):
                pytest.fail("acquired a lock held by another process")
        child.stdin.write("\n")
        child.stdin.flush()
        assert child.stdout.readline().strip() == "released"
        with s.root_lock(timeout=5):
            pass
    finally:
        child.kill()
        child.wait()
        s.close()


def test_process_lock_fd_is_per_acquisition(tmp_path):
    s = FileStorage(str(tmp_path / "s"))
    try:
        with s.root_lock():
            pass
        assert not any(isinstance(v, int) and k.endswith("lock_fd") for k, v in vars(s).items())
        with s.root_lock():
            pass
    finally:
        s.close()


def test_no_os_lock_available_is_not_supported(tmp_path, monkeypatch):
    monkeypatch.setattr(file_mod, "_PROCESS_LOCK", None)
    with pytest.raises(ProtoNotSupportedError):
        FileStorage(str(tmp_path / "s"))


# --- failures --------------------------------------------------------------------------

@pytest.mark.parametrize("op", ["write", "fsync", "dirsync"])
def test_flush_failure_is_unexpected_and_poisons_writes(tmp_path, op):
    s = FileStorage(str(tmp_path / "s"), _fs=FaultyFS(FaultPlan(1, op)))
    try:
        ptr = s.push_atom(atom_payload(1))
        with pytest.raises(ProtoUnexpectedError) as info:
            s.flush()
        assert isinstance(info.value.__cause__, InjectedFault)
        assert s.get_atom(ptr) == atom_payload(1)      # still readable from memory
        for call in (lambda: s.push_atom(atom_payload(2)), s.flush,
                     lambda: s.publish_root(ptr, 0)):
            with pytest.raises(ProtoUnexpectedError):
                call()
    finally:
        s.close()
    s = FileStorage(str(tmp_path / "s"))
    try:
        assert s.read_root() is None
    finally:
        s.close()


@pytest.mark.parametrize("op,n", [("write", 2), ("fsync", 2), ("rename", 1), ("dirsync", 2)])
def test_root_write_failure_is_unexpected(tmp_path, op, n):
    p = str(tmp_path / "s")
    s = FileStorage(p, _fs=FaultyFS(FaultPlan(n, op)))
    try:
        ptr = s.push_atom(atom_payload(1))
        with pytest.raises(ProtoUnexpectedError) as info:
            s.publish_root(ptr, 0)
        assert isinstance(info.value.__cause__, InjectedFault)
    finally:
        s.close()
    s = FileStorage(p)
    try:
        root = s.read_root()
        assert root is None or (root.epoch == 1 and s.get_atom(root.pointer) == atom_payload(1))
    finally:
        s.close()


def test_failed_storage_can_still_be_closed_and_reopened(tmp_path):
    p = str(tmp_path / "s")
    good = FileStorage(p)
    committed = good.publish_root(good.push_atom(atom_payload(1)), 0)
    good.close()
    s = FileStorage(p, _fs=FaultyFS(FaultPlan(1, "write")))
    s.push_atom(atom_payload(2))
    with pytest.raises(ProtoUnexpectedError):
        s.publish_root(s.push_atom(atom_payload(3)), 1)
    s.close()
    s = FileStorage(p)
    try:
        assert s.read_root() == committed
    finally:
        s.close()


# --- integration -----------------------------------------------------------------------

class Item(Atom):
    """Atom used to prove FileStorage works behind a DirectContext."""

    __type_name__ = "tests.file_storage.Item"

    def __init__(self, value, child=None, *, context=None):
        super().__init__(context=context)
        self.value = value
        self.child = child


def test_atoms_round_trip_through_a_reopen(tmp_path):
    p = str(tmp_path / "s")
    s = FileStorage(p)
    ctx = DirectContext(s)
    ptr = ctx.save(Item({"k": [1, 2, 3]}, child=Item("leaf")))
    s.publish_root(ptr, 0)
    s.close()
    s = FileStorage(p)
    try:
        loaded = DirectContext(s).load(s.read_root().pointer, Item.__type_name__)
        assert loaded.value == {"k": [1, 2, 3]} and loaded.child.value == "leaf"
    finally:
        s.close()


def test_concurrent_pushes_reads_and_flushes(tmp_path):
    """16 threads x 200 pushes with reads, while another thread keeps flushing."""
    s = FileStorage(str(tmp_path / "s"), page_size=1024, segment_max_bytes=128 * 1024,
                    write_buffer_bytes=32 * 1024)
    done = threading.Event()
    errors = []
    results = [None] * 16

    def flusher():
        try:
            while not done.is_set():
                s.flush()
                time.sleep(0.001)
        except Exception as e:  # collected and re-raised in the main thread
            errors.append(e)
            raise

    def pusher(t):
        try:
            mine = []
            for i in range(200):
                n = t * 1000 + i
                ptr = s.push_atom(atom_payload(n)) if i % 5 else s.push_blob(bytes([t]) * (i + 1))
                mine.append((ptr, n, i))
                ptr2, n2, i2 = mine[(i * 7) % len(mine)]
                expected = atom_payload(n2) if i2 % 5 else bytes([t]) * (i2 + 1)
                got = s.get_atom(ptr2) if i2 % 5 else s.get_blob(ptr2)
                if got != expected:
                    errors.append((ptr2, got, expected))
            results[t] = mine
        except Exception as e:  # collected and re-raised in the main thread
            errors.append(e)
            raise

    f = threading.Thread(target=flusher)
    f.start()
    threads = [threading.Thread(target=pusher, args=(t,)) for t in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    done.set()
    f.join()
    try:
        assert not errors, errors[:3]
        s.flush()
        every = [x for chunk in results for x in chunk]
        assert len({p for p, _, _ in every}) == 16 * 200
        for ptr, n, i in every:
            t = n // 1000
            if i % 5:
                assert s.get_atom(ptr) == atom_payload(n)
            else:
                assert s.get_blob(ptr) == bytes([t]) * (i + 1)
    finally:
        s.close()


def test_pointer_type_and_buffered_garbage_offsets(tmp_path):
    s = FileStorage(str(tmp_path / "s"))
    try:
        ptr = s.push_atom(atom_payload(1))
        with pytest.raises(ProtoValidationError):
            s.get_atom((ptr.segment, ptr.offset))
        with pytest.raises(ProtoValidationError):
            s.publish_root((ptr.segment, ptr.offset), 0)
        end = s._current.end
        with pytest.raises(ProtoCorruptionError):
            s.get_atom(AtomPointer(ptr.segment, end - 3))     # header would run past the buffer
        with pytest.raises(ProtoCorruptionError):
            s.get_atom(AtomPointer(ptr.segment, ptr.offset + 1))
    finally:
        s.close()


def test_publishing_an_unknown_pointer_is_refused(tmp_path):
    """A root that could not be opened again must never be published."""
    s = FileStorage(str(tmp_path / "s"))
    try:
        with pytest.raises(ProtoCorruptionError):
            s.publish_root(AtomPointer(uuid.uuid4(), SEGMENT_HEADER_SIZE), 0)
        assert s.read_root() is None
    finally:
        s.close()


def test_properties(tmp_path):
    s = FileStorage(str(tmp_path / "s"), durability="flush")
    try:
        assert s.path == str(tmp_path / "s") and s.durability == "flush"
    finally:
        s.close()
