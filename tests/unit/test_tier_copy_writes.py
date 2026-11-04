"""Copies of other nodes' records are written in contiguous runs (Task 8.3i, item 2).

A data replica stores a replication's records into its local copy of the committer's segment
at their original offsets.  The records of a commit are contiguous in that segment, so each
contiguous run is written with one ``pwrite`` instead of one per record (one system call per
record was most of the replica's non-sync handling time).  The bytes written, their offsets,
the cache and the durability are unchanged; records that are not contiguous are separate runs.
"""
import os

import pytest

from proto_db.storage import tiered
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(120)]


def _writer(tmp_path, n):
    st = tiered.TieredStorage(str(tmp_path / "writer"), FakeObjectStore(), durability="none", release_local=False)
    try:
        ptrs = [st.push_blob(bytes([65 + i % 50]) * (30 + i)) for i in range(n)]
        records = st.outgoing()
        st.flush()
        with open(os.path.join(str(tmp_path / "writer"), ptrs[0].segment.hex + tiered.SEGMENT_SUFFIX), "rb") as f:
            segment_bytes = f.read()
    finally:
        st.close()
    return records, ptrs, segment_bytes


def _count_pwrites(monkeypatch):
    calls = []
    original = tiered._pwrite_all

    def counting(fd, data, offset):
        calls.append((offset, len(data)))
        return original(fd, data, offset)
    monkeypatch.setattr(tiered, "_pwrite_all", counting)
    return calls


@pytest.mark.parametrize("pick", ["all", "gaps"])
def test_contiguous_records_are_written_in_one_run_each(tmp_path, monkeypatch, pick):
    records, ptrs, segment_bytes = _writer(tmp_path, 12)
    if pick == "gaps":                                   # three runs: 0-3, 5-7, 10-11
        keep = [0, 1, 2, 3, 5, 6, 7, 10, 11]
        records = [records[i] for i in keep]
        ptrs = [ptrs[i] for i in keep]
    root = RootRecord(FORMAT_VERSION, ptrs[-1].segment, ptrs[-1].offset, 7)
    path = str(tmp_path / "replica")
    st = tiered.TieredStorage(path, FakeObjectStore(), durability="none", release_local=False)
    try:
        calls = _count_pwrites(monkeypatch)
        st.accept_replication(3, root, records, ranges=tiered.TieredStorage._ranges_of(records), accepted=False)
        record_writes = [c for c in calls if c[0] >= tiered.SEGMENT_HEADER_SIZE]
        assert len(record_writes) == (1 if pick == "all" else 3)
        for (_, offset, data), ptr in zip(records, ptrs):
            assert st.local_record(ptr) == data
        with open(os.path.join(path, ptrs[0].segment.hex + tiered.SEGMENT_SUFFIX), "rb") as f:
            copy = f.read()
        for _, offset, data in records:
            assert copy[offset:offset + len(data)] == data == segment_bytes[offset:offset + len(data)]
    finally:
        st.close()
