"""Kind-6 entries of the acceptor log: bounded replication (Task 8.3g, D54).

* A data replica holds a commit's records without accepting its root: the entry is redone
  when the storage opens (the records are readable locally) and is never an accepted root.
* A voter accepts a root whose records live on the commit's data replicas: the entry carries
  no record but names the commit's byte ranges, which survive a crash (the redo remembers
  them) and a checkpoint (the compaction keeps the accepted root and ``ranges.log`` holds
  them), so a peer adopting the root later learns where its records are.  A kind-5 entry
  would have remembered empty ranges, i.e. claimed that the commit wrote nothing.
"""
import uuid

import pytest

from proto_db.cluster.rootlog import REPLICA_KIND
from proto_db.storage import tiered
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from proto_db.storage.logfile import read_log
from tests.support.fake_object_store import FakeObjectStore
from tests.support.power_loss import PowerLoss

pytestmark = [pytest.mark.timeout(120)]


def _writer_records(tmp_path, n: int = 3):
    """Records written by another node (its own segment), and the root pointing at the last."""
    st = tiered.TieredStorage(str(tmp_path / "writer"), FakeObjectStore(), durability="none", release_local=False)
    try:
        ptrs = [st.push_blob(bytes([65 + i]) * (40 + i)) for i in range(n)]
        records = st.outgoing()
    finally:
        st.close()
    root = RootRecord(FORMAT_VERSION, ptrs[-1].segment, ptrs[-1].offset, 7)
    return records, ptrs, root


def _ranges(records):
    return tiered.TieredStorage._ranges_of(records)


def _open(path):
    return tiered.TieredStorage(path, FakeObjectStore(), durability="none", release_local=False)


def test_a_replica_entry_is_redone_but_never_an_accepted_root(tmp_path, monkeypatch):
    records, ptrs, root = _writer_records(tmp_path)
    path = str(tmp_path / "replica")
    loss = PowerLoss(monkeypatch)
    st = _open(path)
    st.accept_replication(3, root, records, ranges=_ranges(records), accepted=False)
    assert st.stats()["replicas_logged"] == 1 and st.stats()["acceptances_logged"] == 0
    kinds = [p[0] for p in read_log(path + "/roots.log", b"ROOT") if p]
    assert REPLICA_KIND in kinds
    st.close()
    loss.power_loss(path)           # only the synced entry survives: copies and ranges.log are lost
    st = _open(path)
    try:
        assert st.stats()["redo_entries"] == 1
        assert st.load_roots().accepted is None
        for ptr in ptrs:
            assert st.local_record(ptr) is not None
        assert st.commit_ranges(root) == _ranges(records)
    finally:
        st.close()


def test_a_root_accepted_without_its_records_keeps_the_commits_ranges(tmp_path, monkeypatch):
    records, ptrs, root = _writer_records(tmp_path)
    ranges = _ranges(records)
    path = str(tmp_path / "voter")
    loss = PowerLoss(monkeypatch)
    st = _open(path)
    st.accept_replication(3, root, [], ranges=ranges)
    assert st.commit_ranges(root) == ranges
    st.close()
    loss.power_loss(path)           # ranges.log was never synced
    st = _open(path)
    try:
        logged = st.load_roots()
        assert logged.accepted == (3, root)
        assert st.commit_ranges(root) == ranges, "the redo forgot where the records are"
        assert all(st.local_record(ptr) is None for ptr in ptrs)
        st.checkpoint()             # compacts the kind-6 entry into an accepted root
    finally:
        st.close()
    st = _open(path)
    try:
        assert st.load_roots().accepted == (3, root)
        assert st.commit_ranges(root) == ranges
        assert not [p for p in read_log(path + "/roots.log", b"ROOT") if p and p[0] == REPLICA_KIND]
    finally:
        st.close()


def test_a_replica_entry_with_records_and_ranges_accepts_when_flagged(tmp_path):
    records, ptrs, root = _writer_records(tmp_path)
    st = _open(str(tmp_path / "both"))
    try:
        st.accept_replication(3, root, records, ranges=_ranges(records))
        assert st.load_roots().accepted == (3, root)
        assert all(st.local_record(ptr) is not None for ptr in ptrs)
        assert uuid.UUID(bytes=records[0][0]) == ptrs[0].segment
    finally:
        st.close()
