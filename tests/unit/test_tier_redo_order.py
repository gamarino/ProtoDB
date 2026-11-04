"""The redo of phase B never leaves a hole, and a checkpoint never drops records it did not
sync (Task 8.3f-B review fix round 1, findings C1 and I1).

* C1: a record's offset in its own segment and its place in the list of unshipped records
  were assigned in two steps, so a kind-5 entry could hold a later record of an own segment
  without an earlier one.  After a process crash the redo wrote the later record into a file
  that never received the earlier one, and the tier refused to open (``ProtoCorruptionError``,
  "an invalid record followed by a valid one").  Every list returned by ``outgoing`` must now
  be the offset-contiguous continuation of each own segment, and the node reopens.
* I1: the checkpoint's mark was a byte offset into ``roots.log``; an automatic compaction
  between the mark and ``compact_records`` replaced the file, and a kind-5 entry appended
  afterwards lay below the stale mark and lost its records without being synced.
"""
import os
import subprocess
import sys
import textwrap
import threading
import uuid

import pytest

from proto_db.cluster.rootlog import RECORDS_KIND
from proto_db.storage import tiered
from proto_db.storage.format import FORMAT_VERSION, SEGMENT_HEADER_SIZE, RootRecord, encode_record
from proto_db.storage.logfile import read_log
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(120)]

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# The child pushes two records concurrently and forces the first one to stall after its
# offset was assigned; the holder then logs an acceptance of whatever ``outgoing`` lists and
# the process dies without flushing its segment buffer.
_CHILD = textwrap.dedent("""
    import os, sys, threading
    sys.path.insert(0, sys.argv[1])
    from proto_db.storage import tiered
    from proto_db.storage.format import FORMAT_VERSION, RootRecord
    from tests.support.fake_object_store import FakeObjectStore

    st = tiered.TieredStorage(sys.argv[2], FakeObjectStore(), durability=sys.argv[3],
                              release_local=False)
    st.push_blob(b"warm" * 10)
    root_ptr = st.push_blob(b"root-of-round-1" * 4)
    stall, stalled = threading.Event(), threading.Event()
    real = tiered.encode_record

    def slow(kind, payload):
        if threading.current_thread().name == "T1":
            stalled.set()
            stall.wait()
        return real(kind, payload)

    tiered.encode_record = slow
    t1 = threading.Thread(target=lambda: st.push_blob(b"X" * 100), name="T1", daemon=True)
    t1.start()
    stalled.wait()
    t2 = threading.Thread(target=lambda: st.push_blob(b"Y" * 100), name="T2", daemon=True)
    t2.start()
    t2.join(0.5)                 # finishes at once without the fix; waits for T1 with it
    recs = st.outgoing()
    seg = root_ptr.segment.bytes
    offsets = [off for raw, off, _ in recs if raw == seg]
    ends = [off + len(data) for raw, off, data in recs if raw == seg]
    print("CONTIGUOUS" if all(o == e for o, e in zip(offsets[1:], ends)) else "HOLE", offsets, flush=True)
    st.accept_replication(1, RootRecord(FORMAT_VERSION, root_ptr.segment, root_ptr.offset, 1), recs)
    stall.set()
    os._exit(0)
""")


@pytest.mark.parametrize("durability", ["none", "flush", "fsync"])
def test_a_crash_after_concurrent_pushes_never_leaves_a_redo_hole(tmp_path, durability):
    """Fails on a8a87a7: ``outgoing`` lists offsets [26, 81, 271] (156 missing) and the reopen
    raises ``ProtoCorruptionError``."""
    d = str(tmp_path / "tier")
    run = subprocess.run([sys.executable, "-c", _CHILD, _REPO, d, durability],
                         capture_output=True, text=True, timeout=60)
    assert run.returncode == 0, run.stderr
    st = tiered.TieredStorage(d, FakeObjectStore(), durability=durability, release_local=False)
    try:
        assert st.stats()["redo_entries"] == 1
    finally:
        st.close()
    assert run.stdout.startswith("CONTIGUOUS"), run.stdout


def test_outgoing_is_always_offset_contiguous_under_concurrent_pushes(tmp_path):
    """Many threads push at once; every snapshot of ``outgoing`` is, per own segment, a run of
    records each starting where the previous one ends."""
    st = tiered.TieredStorage(str(tmp_path / "tier"), FakeObjectStore(), durability="none",
                              release_local=False)
    stop = threading.Event()
    errors = []

    def pusher(i):
        n = 0
        while not stop.is_set() and n < 400:
            st.push_blob(bytes([i]) * (17 + n % 50))
            n += 1

    def check():
        while not stop.is_set():
            last = {}
            for raw, off, data in st.outgoing():
                if raw in last and last[raw] != off:
                    errors.append((last[raw], off))
                last[raw] = off + len(data)

    threads = [threading.Thread(target=pusher, args=(i,)) for i in range(6)]
    checker = threading.Thread(target=check)
    checker.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    checker.join()
    st.close()
    assert errors == []


def test_a_checkpoint_mark_survives_an_automatic_compaction(tmp_path, monkeypatch):
    """Fails on a8a87a7: the kind-5 entry appended after the automatic compaction is summarized
    by ``compact_records`` with the stale mark, and its records are dropped unsynced."""
    d = str(tmp_path / "tier")
    st = tiered.TieredStorage(d, FakeObjectStore(), durability="none", release_local=False)
    path = os.path.join(d, tiered.ROOTS_LOG)
    seg = uuid.uuid4()
    rec = lambda e: RootRecord(FORMAT_VERSION, seg, SEGMENT_HEADER_SIZE + e, e)   # noqa: E731
    e = 1
    # Committed roots up to just below the automatic compaction size, no kind-5 entry pending.
    while os.path.getsize(path) < 1024 * 1024 - 200:
        st.log_root("committed", 1, rec(e))
        e += 1
    real = st._checkpoint_data
    foreign = uuid.uuid4()
    framed = encode_record(tiered.RecordKind.BLOB, b"z" * 64)

    def during_the_syncs():
        nonlocal e
        real()
        # Meanwhile another thread logs committed roots (crossing the compaction size) ...
        for _ in range(10):
            st.log_root("committed", 1, rec(e))
            e += 1
        # ... and an acceptance arrives after the checkpoint swapped its dirty sets.
        st.accept_replication(2, RootRecord(FORMAT_VERSION, foreign, SEGMENT_HEADER_SIZE, e),
                              [[foreign.bytes, SEGMENT_HEADER_SIZE, framed]])

    monkeypatch.setattr(st, "_checkpoint_data", during_the_syncs)
    st.checkpoint()
    try:
        kind5 = sum(1 for p in read_log(path, b"ROOT") if p and p[0] == RECORDS_KIND)
        assert kind5 == 1
    finally:
        st.close()
