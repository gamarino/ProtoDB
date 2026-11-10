"""Crash matrix: an I/O failure at every write, fsync, rename and directory-fsync point.

Catches: torn or lost commits (the visible root after reopen is not one of the committed
prefixes, or an acknowledged commit disappears), roots that reference unwritten data,
exceptions other than ProtoUnexpectedError at the injected point, spaces that cannot be
reopened or extended after a failure, and fsync ordering bugs (with the power-loss model,
which drops everything not made durable before the failure, in an ordered and an
adversarial metadata-reordering variant).
"""
import uuid

import pytest

from proto_db.core.atom import AtomPointer
from proto_db.errors import ProtoUnexpectedError
from proto_db.storage.file import FileStorage
from tests.support.faulty_fs import OPS, FaultPlan, FaultyFS, InjectedFault

TXNS = 3
ITEMS = 4
SEGMENT_MAX = 400          # small: segments roll inside transactions


def item_payload(t, i):
    return {"t": "Item", "txn": t, "i": i, "data": bytes([t, i]) * (10 + 15 * i)}


def blob_data(t):
    return bytes([t]) * (50 + 25 * t)


def ref(ptr):
    return [ptr.segment.hex, ptr.offset]


def run_workload(storage, acknowledged):
    """Three transactions: items, a blob and a root atom chained to the previous root."""
    prev = None
    for t in range(TXNS):
        current = storage.read_root()
        epoch = current.epoch if current is not None else 0
        items = []
        for i in range(ITEMS):
            items.append(storage.push_atom(item_payload(t, i)))
            if t == 1 and i == 1:
                storage.flush()            # a flush that is not followed by a publish yet
        blob = storage.push_blob(blob_data(t))
        root = storage.push_atom({"t": "Root", "txn": t, "items": [ref(p) for p in items],
                                  "blob": ref(blob), "prev": ref(prev) if prev else None})
        storage.publish_root(root, epoch)
        acknowledged.append(t)
        prev = root


def pointer(pair):
    return AtomPointer(uuid.UUID(hex=pair[0]), pair[1])


def visible_transactions(path):
    """Reopen cleanly, check every reachable record and return the number of visible commits."""
    s = FileStorage(path)
    try:
        root = s.read_root()
        if root is None:
            return 0
        atom = s.get_atom(root.pointer)
        visible = atom["txn"] + 1
        assert root.epoch == visible
        expected_txn = atom["txn"]
        while True:
            assert atom["t"] == "Root" and atom["txn"] == expected_txn
            t = atom["txn"]
            assert [s.get_atom(pointer(p)) for p in atom["items"]] == [
                item_payload(t, i) for i in range(ITEMS)]
            assert s.get_blob(pointer(atom["blob"])) == blob_data(t)
            if atom["prev"] is None:
                assert t == 0
                break
            atom = s.get_atom(pointer(atom["prev"]))
            expected_txn -= 1
        return visible
    finally:
        s.close()


def extend(path, visible):
    """A clean instance can commit on top of whatever survived."""
    s = FileStorage(path)
    try:
        root = s.read_root()
        ptr = s.push_atom({"t": "After", "on": visible})
        s.publish_root(ptr, root.epoch if root else 0)
    finally:
        s.close()
    s = FileStorage(path)
    try:
        assert s.get_atom(s.read_root().pointer) == {"t": "After", "on": visible}
    finally:
        s.close()


def run_case(path, op, n, power_loss):
    """Run the workload failing at the n-th ``op``; return True once it completes unfaulted."""
    fs = FaultyFS(FaultPlan(n, op))
    acknowledged = []
    storage = FileStorage(path, segment_max_bytes=SEGMENT_MAX, page_size=256, _fs=fs)
    try:
        run_workload(storage, acknowledged)
        completed = True
    except ProtoUnexpectedError as e:
        assert isinstance(e.__cause__, InjectedFault), f"failure not at the injected point: {e!r}"
        completed = False
    finally:
        storage.close()
    assert completed != fs.plan.fired
    if power_loss:
        fs.power_loss(reordered=power_loss == "reordered")
    visible = visible_transactions(path)
    acked = len(acknowledged)
    attempted = acked if completed else min(acked + 1, TXNS)
    assert acked <= visible <= attempted, (op, n, power_loss, acked, visible)
    extend(path, visible)
    return completed


@pytest.mark.parametrize("power_loss", [None, "ordered", "reordered"],
                         ids=["process-crash", "power-loss", "power-loss-reordered"])
@pytest.mark.parametrize("op", OPS)
def test_failure_at_every_point(tmp_path, op, power_loss):
    n = 1
    while not run_case(str(tmp_path / f"{op}-{n}"), op, n, power_loss):
        n += 1
        assert n < 500, "workload never completed"
    assert n - 1 >= TXNS, f"too few {op} points exercised: {n - 1}"


def test_clean_run_survives_power_loss(tmp_path):
    fs = FaultyFS()
    s = FileStorage(str(tmp_path / "s"), segment_max_bytes=SEGMENT_MAX, _fs=fs)
    acknowledged = []
    run_workload(s, acknowledged)
    s.close()
    fs.power_loss()
    assert visible_transactions(str(tmp_path / "s")) == TXNS


def test_power_loss_drops_unsynced_data_in_none_mode(tmp_path):
    """Sanity check of the model itself: without fsync nothing is guaranteed to survive."""
    fs = FaultyFS()
    s = FileStorage(str(tmp_path / "s"), durability="none", _fs=fs)
    s.publish_root(s.push_atom({"t": "x"}), 0)
    s.close()
    fs.power_loss()
    s = FileStorage(str(tmp_path / "s"))
    try:
        assert s.read_root() is None
    finally:
        s.close()
