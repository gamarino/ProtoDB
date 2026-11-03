"""One live instance per local directory and per acceptor log file (8.3d re-review 11, I1;
ruling R59); every failed open releases the lock, and aliases of one log are refused (re-review
12, I2 and M3; ruling R60).  The own-segment search is tested in test_self_validating_framing.py.

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
    from proto_db.storage import logfile
    with open(path, "r+b") as f:
        data = bytearray(f.read())
        data[logfile.HEADER_SIZE + logfile.FRAME_OVERHEAD] ^= 0xFF     # first payload byte
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


# -- I2 (re-review 12; ruling R60): every failed open releases the lock --------------------------

BAD_CONFIGS = [
    ("segment_cache_bytes=-1", dict(segment_cache_bytes=-1)),
    ("segment_cache_bytes=1.5e6", dict(segment_cache_bytes=1.5e6)),
    ("cache_bytes=-1", dict(cache_bytes=-1)),
    ("backoff=('x','y')", dict(backoff=("x", "y"))),
    ("backoff=(None,1)", dict(backoff=(None, 1))),
    ("backoff=(2,1)", dict(backoff=(2.0, 1.0))),
    ("backoff=(-1,1)", dict(backoff=(-1.0, 1.0))),
    ("backoff=(nan,1)", dict(backoff=(float("nan"), 1.0))),
    ("backoff=(0,inf)", dict(backoff=(0.0, float("inf")))),
    ("backoff=(True,1)", dict(backoff=(True, 1.0))),
    ("backoff=(10**400,10**400)", dict(backoff=(10 ** 400, 10 ** 400))),
    ("backoff=3", dict(backoff=3)),
    ("segment_max_bytes=10", dict(segment_max_bytes=10)),
    ("lock_timeout=-1", dict(lock_timeout=-1)),
    ("ship_interval=nan", dict(ship_interval=float("nan"))),
]


@pytest.mark.parametrize("name,bad", BAD_CONFIGS, ids=[n for n, _ in BAD_CONFIGS])
def test_a_bad_argument_is_refused_before_the_lock_and_a_correct_open_then_succeeds(tmp_path, name, bad):
    """Every argument is validated before the directory is created or locked: a service that
    retries with corrected settings is not locked out by a phantom instance (before the fix,
    ``segment_cache_bytes`` and the ``backoff`` elements were checked after the lock, which
    leaked)."""
    from proto_db.errors import ProtoValidationError
    d = str(tmp_path / "node")
    store = FakeObjectStore()
    kw = dict(KW, **bad)
    for _ in range(2):
        with pytest.raises(ProtoValidationError):
            TieredStorage(d, store, **kw)
        assert not os.path.exists(d), "the directory was touched before validation"
    TieredStorage(d, store, **KW).close()
    os.makedirs(d, exist_ok=True)
    with pytest.raises(ProtoValidationError):              # an existing directory: still no lock
        TieredStorage(d, store, **kw)
    TieredStorage(d, store, **KW).close()


class _Injected(Exception):
    """A failure that is not a ProtoDBError (an unexpected bug), injected after the lock."""


@pytest.mark.parametrize("where", ["FileStorage", "_load_manifest", "RootsLog", "_load_ranges"])
def test_an_unexpected_failure_after_the_lock_releases_it(tmp_path, monkeypatch, where):
    d = str(tmp_path / "node")
    store = FakeObjectStore()
    TieredStorage(d, store, **KW).close()

    def boom(*args, **kwargs):
        raise _Injected(where)

    if where in ("FileStorage", "RootsLog"):
        monkeypatch.setattr(tiered_module, where, boom)
    else:
        monkeypatch.setattr(TieredStorage, where, boom)
    for _ in range(2):
        with pytest.raises(_Injected):
            TieredStorage(d, store, **KW)
    monkeypatch.undo()
    TieredStorage(d, store, **KW).close()
    r = RootsLog(os.path.join(d, tiered_module.ROOTS_LOG))   # the acceptor log's lock was released too
    r.close()


def test_a_failing_release_step_of_a_failed_open_still_releases_the_lock(tmp_path, monkeypatch):
    """When closing what a failed open opened raises something unexpected too, every later
    step still runs and the lock is released last."""
    d = str(tmp_path / "node")
    store = FakeObjectStore()

    def ranges_boom(self):
        raise _Injected("ranges")

    def local_close_boom(self):
        raise _Injected("local close")

    monkeypatch.setattr(TieredStorage, "_load_ranges", ranges_boom)
    monkeypatch.setattr(FileStorage, "close", local_close_boom)
    with pytest.raises(_Injected):
        TieredStorage(d, store, **KW)
    monkeypatch.undo()
    TieredStorage(d, store, **KW).close()


def test_close_releases_the_lock_whatever_a_step_raises(tmp_path, monkeypatch):
    """``close()`` runs every step and releases the lock last even when a step raises an
    exception that is not a ProtoDBError (re-review 12, out-of-diff note)."""
    d = str(tmp_path / "node")
    store = FakeObjectStore()
    a = TieredStorage(d, store, **KW)
    roots = a._roots

    def flush_boom():
        raise _Injected("flush")

    monkeypatch.setattr(a._local, "flush", flush_boom)
    with pytest.raises(_Injected):
        a.close()
    assert roots._closed and a._manifest_fd == -1 and a._ranges_fd == -1   # later steps still ran
    a.close()                                                               # idempotent
    TieredStorage(d, store, **KW).close()


def test_roots_log_close_releases_the_lock_and_keeps_the_close_error_primary(tmp_path, monkeypatch):
    from proto_db.errors import ProtoUnexpectedError, secondary_errors
    path = str(tmp_path / "n.roots")
    a = RootsLog(path)
    fd = a._fd
    real_close = os.close

    def close(n):
        if n == fd:
            real_close(n)
            raise OSError(5, "injected close failure")
        return real_close(n)

    def release_boom():
        a._owner.__class__.release(a._owner)
        raise ProtoUnexpectedError("injected release failure")

    monkeypatch.setattr(os, "close", close)
    monkeypatch.setattr(a._owner, "release", release_boom)
    with pytest.raises(ProtoUnexpectedError, match="Closing the root log") as info:
        a.close()
    monkeypatch.undo()
    assert any("injected release failure" in str(e) for e in secondary_errors(info.value))
    RootsLog(path).close()


# -- M3 (re-review 12; ruling R60): aliases of one acceptor log --------------------------------

def test_a_roots_log_through_a_file_symlink_is_refused_while_the_target_is_open(tmp_path):
    real = str(tmp_path / "n.roots")
    link = str(tmp_path / "link.roots")
    a = RootsLog(real)
    os.symlink(real, link)
    try:
        with pytest.raises(ProtoLockingError, match="already open"):
            RootsLog(link)
        assert _open_in_child("roots", link) == "REFUSED"
    finally:
        a.close()
    b = RootsLog(link)                    # resolved: the log and its lock are the target's
    try:
        assert b.path == os.path.realpath(real)
        with pytest.raises(ProtoLockingError):
            RootsLog(real)
    finally:
        b.close()
    assert os.path.islink(link) and not os.path.exists(link + ".lock")


def test_a_compaction_through_a_symlink_replaces_the_target_not_the_link(tmp_path, monkeypatch):
    monkeypatch.setattr(rootlog_module, "_COMPACT_BYTES", 200)
    real = str(tmp_path / "n.roots")
    link = str(tmp_path / "link.roots")
    RootsLog(real).close()
    os.symlink(real, link)
    a = RootsLog(link)
    try:
        ino = os.stat(real).st_ino
        for i in range(1, 20):
            a.append("accepted", i, rec(i), sync=True)
        assert os.stat(real).st_ino != ino, "no compaction happened"
        assert os.path.islink(link)
        with pytest.raises(ProtoLockingError):
            RootsLog(real)
    finally:
        a.close()
    b = RootsLog(real)
    try:
        assert b.load().accepted[0] == 19
    finally:
        b.close()


def test_a_roots_log_through_a_hard_link_is_refused_in_this_process_and_another(tmp_path):
    real = str(tmp_path / "n.roots")
    hard = str(tmp_path / "hard.roots")
    a = RootsLog(real)
    a.append("accepted", 1, rec(1), sync=True)
    os.link(real, hard)
    before = open(real, "rb").read()
    try:
        with pytest.raises(ProtoLockingError):
            RootsLog(hard)
        assert _open_in_child("roots", hard) == "REFUSED"
    finally:
        a.close()
    assert open(real, "rb").read() == before
    # A log with two names is refused by either name, even with no instance open: its lock is
    # named after the path, so it could not exclude an instance opened through the other name.
    for name in (real, hard):
        with pytest.raises(ProtoLockingError, match="hard links"):
            RootsLog(name)
        assert _open_in_child("roots", name) == "REFUSED"
    os.remove(hard)
    b = RootsLog(real)
    try:
        assert b.load().accepted[0] == 1
    finally:
        b.close()


def test_the_registry_also_refuses_a_hard_link_created_after_the_open(tmp_path, monkeypatch):
    """In this process the registry holds the log file's inode, so a hard link is refused by
    the registry even before the link count is looked at."""
    from proto_db.storage import logfile
    real = str(tmp_path / "n.roots")
    hard = str(tmp_path / "hard.roots")
    a = RootsLog(real)
    os.link(real, hard)
    opened = []
    real_open_log = logfile.open_log
    monkeypatch.setattr(rootlog_module, "open_log", lambda *x, **k: opened.append(x) or real_open_log(*x, **k))
    try:
        with pytest.raises(ProtoLockingError, match="another path"):
            RootsLog(hard)
        assert opened == []
    finally:
        a.close()
