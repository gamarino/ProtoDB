"""One live instance per local directory and per acceptor log file (8.3d re-review 11, I1;
ruling R59), and the own-segment search for a valid record (M1).

Opening a :class:`~proto_db.storage.tiered.TieredStorage` directory or a
:class:`~proto_db.cluster.rootlog.RootsLog` truncates torn tails.  Before the fix nothing
refused a second live instance, so a second open could truncate an entry the first was still
writing; the first completed its write past the cut and the next open trimmed the acknowledged
entry as a torn tail (the reviewer's deterministic repros, reproduced below).  Now each owner
holds an exclusive lock for its lifetime and a second open raises ``ProtoLockingError``.
"""
import os
import signal
import subprocess
import sys
import textwrap
import threading
import uuid
import zlib

import pytest

from proto_db.cluster import rootlog as rootlog_module
from proto_db.cluster.persister import PlainTier
from proto_db.cluster.rootlog import RootsLog
from proto_db.errors import ProtoCorruptionError, ProtoLockingError
from proto_db.storage import tiered as tiered_module
from proto_db.storage.file import FileStorage
from proto_db.storage.format import FORMAT_VERSION, RECORD_HEADER_SIZE, RootRecord, encode_record
from proto_db.storage.tiered import RANGES_LOG, TieredStorage
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(120)]

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
KW = dict(durability="fsync", ship_interval=3600.0, backoff=(0.0, 0.0), retries=0)
REAL_WRITE = os.write


def rec(i: int) -> RootRecord:
    return RootRecord(FORMAT_VERSION, uuid.UUID(int=i), 64, i)


# -- same process -----------------------------------------------------------------------------

def test_a_second_tiered_storage_on_a_live_directory_is_refused_until_close(tmp_path):
    d = str(tmp_path / "node")
    store = FakeObjectStore()
    a = TieredStorage(d, store, **KW)
    try:
        with pytest.raises(ProtoLockingError, match="already open"):
            TieredStorage(d, store, **KW)
        with pytest.raises(ProtoLockingError):           # refused again: nothing was leaked
            TieredStorage(d, store, **KW)
    finally:
        a.close()
    b = TieredStorage(d, store, **KW)                    # released on close
    b.close()


def test_a_second_roots_log_on_a_live_file_is_refused_until_close(tmp_path):
    path = str(tmp_path / "n.roots")
    a = RootsLog(path)
    try:
        with pytest.raises(ProtoLockingError, match="already open"):
            RootsLog(path)
    finally:
        a.close()
    b = RootsLog(path)
    b.close()


def test_the_lock_survives_a_compaction_that_replaces_the_log_file(tmp_path, monkeypatch):
    """A compaction renames a new file over the log; the lock is on a separate file, so it
    still excludes a second open afterwards."""
    monkeypatch.setattr(rootlog_module, "_COMPACT_BYTES", 200)
    path = str(tmp_path / "n.roots")
    a = RootsLog(path)
    try:
        ino = os.stat(path).st_ino
        for i in range(1, 20):
            a.append("accepted", i, rec(i), sync=True)
        assert os.stat(path).st_ino != ino, "no compaction happened"
        with pytest.raises(ProtoLockingError):
            RootsLog(path)
    finally:
        a.close()


def test_distinct_logs_and_directories_do_not_conflict(tmp_path):
    store = FakeObjectStore()
    a = TieredStorage(str(tmp_path / "a"), store, **KW)
    b = TieredStorage(str(tmp_path / "b"), store, **KW)
    x = RootsLog(str(tmp_path / "x.roots"))
    y = RootsLog(str(tmp_path / "y.roots"))
    for h in (a, b, x, y):
        h.close()


def test_two_plain_tiers_with_one_node_id_on_a_shared_file_storage_are_refused(tmp_path):
    """Two processes (here: two tiers) using the same ``node_id`` on a shared ``FileStorage``
    would both write ``cluster/<node>.roots``; the second is refused, other node ids are not."""
    storage = FileStorage(str(tmp_path / "shared"))
    try:
        a = PlainTier(storage, "n1")
        try:
            with pytest.raises(ProtoLockingError):
                PlainTier(storage, "n1")
            other = PlainTier(storage, "n2")
            other.close()
        finally:
            a.close()
        again = PlainTier(storage, "n1")
        again.close()
    finally:
        storage.close()


# -- released on a failed open ----------------------------------------------------------------

def _corrupt_mid_log(path: str) -> None:
    """Damage the first frame's payload of the framed log ``path`` (its second frame stays
    valid): opening it raises ``ProtoCorruptionError``."""
    with open(path, "r+b") as f:
        data = bytearray(f.read())
        data[14 + 8] ^= 0xFF
        f.seek(0)
        f.write(data)


def test_a_failed_tiered_open_releases_the_lock(tmp_path):
    d = str(tmp_path / "node")
    store = FakeObjectStore()
    TieredStorage(d, store, **KW).close()
    ranges = os.path.join(d, RANGES_LOG)
    from proto_db.storage import logfile
    with open(ranges, "wb") as f:
        f.write(logfile.log_contents(tiered_module.RANGES_KIND, [b"x" * 30, b"y" * 30]))
    _corrupt_mid_log(ranges)
    for _ in range(2):                    # the second attempt is not refused by a leaked lock
        with pytest.raises(ProtoCorruptionError):
            TieredStorage(d, store, **KW)
    os.remove(ranges)
    TieredStorage(d, store, **KW).close()


def test_a_failed_roots_log_open_releases_the_lock(tmp_path):
    path = str(tmp_path / "n.roots")
    log = RootsLog(path)
    log.append("accepted", 1, rec(1), sync=True)
    log.append("accepted", 2, rec(2), sync=True)
    log.close()
    _corrupt_mid_log(path)
    for _ in range(2):
        with pytest.raises(ProtoCorruptionError):
            RootsLog(path)
    os.remove(path)
    RootsLog(path).close()


# -- released last on close -------------------------------------------------------------------

def test_tiered_close_keeps_the_lock_until_everything_is_closed(tmp_path):
    """While ``close()`` is still closing the acceptor log (its last step), a second open is
    still refused, and by the directory lock, before it opens or repairs anything (the
    acceptor log's own lock would refuse only after the segments were checked)."""
    d = str(tmp_path / "node")
    store = FakeObjectStore()
    a = TieredStorage(d, store, **KW)
    seen = {}
    roots_close = a._roots.close

    def closing():
        try:
            TieredStorage(d, store, **KW).close()
            seen["second"] = "opened"
        except ProtoLockingError as e:
            seen["second"] = "refused" if "tiered storage directory" in str(e) else f"late: {e}"
        roots_close()

    a._roots.close = closing
    a.close()
    assert seen == {"second": "refused"}
    TieredStorage(d, store, **KW).close()


def test_roots_log_close_keeps_the_lock_until_its_descriptor_is_closed(tmp_path, monkeypatch):
    path = str(tmp_path / "n.roots")
    a = RootsLog(path)
    fd = a._fd
    seen = {}
    real_close = os.close

    def close(n):
        if n == fd and "second" not in seen:
            try:
                RootsLog(path).close()
                seen["second"] = "opened"
            except ProtoLockingError:
                seen["second"] = "refused"
        return real_close(n)

    monkeypatch.setattr(os, "close", close)
    a.close()
    monkeypatch.setattr(os, "close", real_close)
    assert seen == {"second": "refused"}


# -- another process --------------------------------------------------------------------------

_CHILD = textwrap.dedent("""
    import sys
    sys.path.insert(0, {root!r})
    from proto_db.errors import ProtoLockingError
    from proto_db.cluster.rootlog import RootsLog
    from proto_db.storage.tiered import TieredStorage
    from tests.support.fake_object_store import FakeObjectStore
    kind, path, hold = sys.argv[1], sys.argv[2], sys.argv[3] == "hold"
    try:
        h = (TieredStorage(path, FakeObjectStore(), ship_interval=3600.0, retries=0)
             if kind == "tiered" else RootsLog(path))
    except ProtoLockingError as e:
        print("REFUSED", e, flush=True)
        sys.exit(0)
    print("OPENED", flush=True)
    if hold:
        sys.stdin.readline()          # held until the parent kills this process
    h.close()
""")


def _child(kind, path, hold=False):
    return subprocess.Popen([sys.executable, "-c", _CHILD.format(root=ROOT), kind, path,
                             "hold" if hold else "once"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, cwd=ROOT)


def _open_in_child(kind, path) -> str:
    p = _child(kind, path)
    out, err = p.communicate(timeout=60)
    assert p.returncode == 0, err
    return out.split()[0]


@pytest.mark.parametrize("kind", ["tiered", "roots"])
def test_another_process_is_refused_while_the_owner_is_live(tmp_path, kind):
    path = str(tmp_path / ("node" if kind == "tiered" else "n.roots"))
    owner = TieredStorage(path, FakeObjectStore(), **KW) if kind == "tiered" else RootsLog(path)
    try:
        assert _open_in_child(kind, path) == "REFUSED"
    finally:
        owner.close()
    assert _open_in_child(kind, path) == "OPENED"


@pytest.mark.parametrize("kind", ["tiered", "roots"])
def test_a_live_owner_in_another_process_refuses_this_one_until_it_dies(tmp_path, kind):
    """The lock is released by the OS when its holder is killed (``SIGKILL``), so a crash never
    leaves a directory locked."""
    path = str(tmp_path / ("node" if kind == "tiered" else "n.roots"))
    p = _child(kind, path, hold=True)
    try:
        assert p.stdout.readline().split()[0] == "OPENED"

        def mine():
            return TieredStorage(path, FakeObjectStore(), **KW) if kind == "tiered" else RootsLog(path)

        with pytest.raises(ProtoLockingError, match="another process"):
            mine()
    finally:
        p.kill()
        p.wait(30)
    assert p.returncode == -signal.SIGKILL
    mine().close()


# -- the reviewer's deterministic repros (re-review 11, check 4) -------------------------------

def test_r12_a_second_tiered_open_during_a_short_write_is_refused_and_nothing_is_lost(tmp_path):
    """``probe_two_instances_det``: A's 1 MiB blob write returns short; a second instance opened
    meanwhile used to trim A's in-flight record, and after A completed and fsynced, the next
    restart trimmed the acknowledged record.  The second open is refused now."""
    d = str(tmp_path / "node")
    store = FakeObjectStore()
    a = TieredStorage(d, store, **KW)
    a.push_atom({"warm": 1})
    a.flush()
    fs = a._local._fs
    real = fs.write
    gate, resume = threading.Event(), threading.Event()
    state = {"armed": True}

    def short_write(fd, data, offset):
        if state["armed"] and len(data) > 4096:
            state["armed"] = False
            n = real(fd, bytes(data)[:len(data) // 2], offset)
            gate.set()
            resume.wait(10)
            return n
        return real(fd, data, offset)

    fs.write = short_write
    blob = b"x" * (1 << 20)
    res = {}

    def writer():
        p = a.push_blob(blob)
        a.flush()
        res["p"] = p

    t = threading.Thread(target=writer)
    t.start()
    try:
        assert gate.wait(10)
        with pytest.raises(ProtoLockingError):
            TieredStorage(d, store, **KW)
    finally:
        resume.set()
        t.join(30)
    assert "p" in res
    a.close()
    c = TieredStorage(d, store, **KW)
    try:
        assert c.get_blob(res["p"]) == blob
    finally:
        c.close()


def test_r12_a_second_roots_log_during_a_short_append_is_refused_and_nothing_is_lost(tmp_path, monkeypatch):
    """``probe_two_rootslogs_det``: A's append of accepted 2 returns short; a second handle
    opened meanwhile used to trim the half-written frame, and after A completed and fsynced, the
    acknowledged acceptance was lost.  The second open is refused now."""
    path = str(tmp_path / "roots.log")
    a = RootsLog(path)
    a.append("accepted", 1, rec(1), sync=True)
    a.mark_rejoined()
    gate, resume = threading.Event(), threading.Event()
    state = {"armed": True}

    def write(fd, data):
        if fd == a._fd and state["armed"]:
            state["armed"] = False
            n = REAL_WRITE(fd, bytes(data)[:20])
            gate.set()
            resume.wait(10)
            return n
        return REAL_WRITE(fd, data)

    monkeypatch.setattr(os, "write", write)
    t = threading.Thread(target=lambda: a.append("accepted", 2, rec(2), sync=True))
    t.start()
    try:
        assert gate.wait(10)
        with pytest.raises(ProtoLockingError):
            RootsLog(path)
    finally:
        resume.set()
        t.join(30)
    monkeypatch.setattr(os, "write", REAL_WRITE)
    assert a.load().accepted[0] == 2
    a.close()
    c = RootsLog(path)
    try:
        assert c.load().accepted[0] == 2
    finally:
        c.close()


# -- M1: the own-segment search -----------------------------------------------------------------

def _brute_first_valid(data, start, size):
    for pos in range(start, size - RECORD_HEADER_SIZE + 1):
        if tiered_module._record_length(data, pos, size) is not None:
            return pos
    return None


def test_crc_shift_matches_zlib_crc32_combine():
    import random
    rng = random.Random(7)
    for _ in range(300):
        a = rng.randbytes(rng.randint(0, 200))
        b = rng.randbytes(rng.choice([0, 1, 5, rng.randint(0, 70000)]))
        assert tiered_module._crc_shift(zlib.crc32(a), len(b)) ^ zlib.crc32(b) == zlib.crc32(a + b)


@pytest.mark.parametrize("checkpoint,direct", [(7, 0), (64, 16), (16384, 65536)])
def test_the_fast_search_equals_trying_every_offset(monkeypatch, checkpoint, direct):
    """The search answers exactly as trying :func:`_record_length` at every offset, on regions
    mixing valid records, random bytes, runs of kind and zero bytes and forged headers."""
    import random
    monkeypatch.setattr(tiered_module, "_CRC_CHECKPOINT", checkpoint)
    monkeypatch.setattr(tiered_module, "_CRC_DIRECT", direct)
    rng = random.Random(checkpoint)
    found = 0
    for _ in range(250):
        parts = []
        for _ in range(rng.randint(0, 4)):
            c = rng.random()
            if c < 0.3:
                parts.append(encode_record(rng.choice([1, 2]), rng.randbytes(rng.randint(1, 2000))))
            elif c < 0.6:
                parts.append(bytes([rng.choice([0, 1, 2, 255])]) * rng.randint(1, 400))
            else:
                parts.append(rng.randbytes(rng.randint(1, 2000)))
        data = b"".join(parts)
        if len(data) > 20 and rng.random() < 0.4:
            p = rng.randrange(len(data) - 10)
            length = rng.randint(0, len(data) - p)
            data = data[:p] + length.to_bytes(4, "little") + bytes([rng.choice([1, 2])]) + data[p + 5:]
        start = rng.randint(0, len(data))
        expected = _brute_first_valid(data, start, len(data))
        found += expected is not None
        assert tiered_module._first_valid_record(data, start, len(data)) == (expected, True)
    assert found > 20


def test_candidate_patterns_match_exactly_a_bounded_high_byte_before_a_kind_byte():
    probe = bytes(x for b in range(256) for k in range(4) for x in (b, k))   # (byte, kind) pairs
    for high in (0, 1, 15, 16, 91, 92, 93, 254, 255):
        hits = {m.start() for m in tiered_module._candidate_pattern(high).finditer(probe)}
        assert hits == {i for i in range(len(probe) - 1) if probe[i] <= high and probe[i + 1] in (1, 2)}, high
        assert len(hits) >= 2 * (high + 1)


def test_a_torn_random_blob_tail_is_trimmed_quickly(tmp_path):
    """A torn 64 MiB random blob (the reviewer's scan-cost case: 48 s before, about 0.5 s now) is
    trimmed; the time bound is generous so that a loaded machine does not fail it."""
    import time
    seg = uuid.uuid4()
    path = str(tmp_path / (seg.hex + ".seg"))
    from proto_db.storage.format import encode_segment_header
    good = encode_segment_header(seg) + encode_record(1, b"atom")
    with open(path, "wb") as f:
        f.write(good + encode_record(2, os.urandom(64 << 20))[:-(1 << 20)])
    t0 = time.monotonic()
    assert TieredStorage._repair_own_segment(path, seg) == len(good)
    assert time.monotonic() - t0 < 20
    assert os.path.getsize(path) == len(good)


def test_a_valid_record_inside_a_torn_record_is_reported_as_ambiguous(tmp_path):
    """A torn blob whose bytes embed a valid record cannot be told from mid-segment corruption:
    the open refuses (nothing truncated) and says the case is ambiguous."""
    seg = uuid.uuid4()
    path = str(tmp_path / (seg.hex + ".seg"))
    from proto_db.storage.format import encode_segment_header
    inner = encode_record(1, b"an embedded record")
    torn = encode_record(2, b"\x00" * 100 + inner + os.urandom(5000))[:-1000]
    data = encode_segment_header(seg) + encode_record(1, b"atom") + torn
    with open(path, "wb") as f:
        f.write(data)
    with pytest.raises(ProtoCorruptionError, match="ambiguous"):
        TieredStorage._repair_own_segment(path, seg)
    with open(path, "rb") as f:
        assert f.read() == data


def test_an_exhausted_search_refuses_and_truncates_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(tiered_module, "_SEARCH_BUDGET", 3)
    seg = uuid.uuid4()
    path = str(tmp_path / (seg.hex + ".seg"))
    from proto_db.storage.format import encode_segment_header
    forged = b"".join((0).to_bytes(4, "little") + b"\x01" + b"\x00\x00\x00\x00" for _ in range(10))
    data = encode_segment_header(seg) + encode_record(1, b"atom") + b"\xff" + forged
    with open(path, "wb") as f:
        f.write(data)
    with pytest.raises(ProtoCorruptionError, match="cannot be classified"):
        TieredStorage._repair_own_segment(path, seg)
    with open(path, "rb") as f:
        assert f.read() == data
