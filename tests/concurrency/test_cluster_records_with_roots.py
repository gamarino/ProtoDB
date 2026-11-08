"""A root is never durable without its records (Task 8.3f phase B, D50; defect D-AVAIL-1).

Each node accepts a replicated root by ONE ``fsync``-ed entry of its acceptor log that holds
the root and every record the replication carried; its data files are written without a sync
and rebuilt from those entries when the storage opens (redo).  These tests reproduce the
8.3e review's D-AVAIL-1 schedule under the strict power-loss model of
``tests/support/power_loss.py``: three nodes in ``"flush"`` or ``"none"`` durability, the
object store unavailable (records live only on the local tiers), ``c`` partitioned away,
commits acknowledged by ``a`` and ``b`` alone, then a power loss of ``a`` and ``b`` together.
Before phase B the accepted root survived (the acceptor log is always synced) while its
records did not: every node, ``c`` included, raised ``ProtoCorruptionError`` on reads and
commits, and only wiping every node recovered.  Now both nodes rebuild the records from
their logs: every acknowledged commit is readable everywhere and every node commits again.

Also covered: a torn tail of the acceptor log (an interrupted kind-5 append) and of a data
file is repaired at open; the redo is idempotent across repeated restarts; a checkpoint
compacts the log without losing a record it has not synced.
"""
import os
import time

import pytest

from proto_db.cluster.rootlog import RECORDS_KIND
from proto_db.storage.logfile import read_log
from tests.support.fake_object_store import FakeObjectStore
from tests.support.power_loss import PowerLoss
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(240),
              pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd")]

LEASE = dict(lease_duration=0.9, request_timeout=0.1)
COMMITS = 10


class _FrozenPowerLoss(PowerLoss):
    """A power loss that also stops recording syncs of the directories it froze: a straggling
    thread of a killed node never makes anything durable after the power went."""

    def __init__(self, monkeypatch):
        super().__init__(monkeypatch)
        self.frozen = set()

    def _record_fd(self, fd):
        try:
            path = os.path.realpath(os.readlink(f"/proc/self/fd/{fd}"))
        except OSError:
            path = ""
        if any(path == d or path.startswith(d + "/") for d in self.frozen):
            return
        super()._record_fd(fd)


def _values(tc, nodes) -> dict:
    out = {}
    for node in nodes:
        try:
            out[node] = tc.value(node)
        except Exception as e:      # reported in the assertion message, never swallowed silently
            out[node] = f"{type(e).__name__}: {e}"
    return out


def _settle(tc, nodes, expected, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        values = _values(tc, nodes)
        if all(v == expected for v in values.values()) or time.monotonic() > deadline:
            return values
        time.sleep(0.05)


@pytest.mark.parametrize("durability", ["flush", "none"])
def test_majority_power_loss_keeps_every_acknowledged_root_with_its_records(tmp_path, monkeypatch, durability):
    """D-AVAIL-1 regression: fails on f03798b (``ProtoCorruptionError`` on every node)."""
    fs = _FrozenPowerLoss(monkeypatch)
    store = FakeObjectStore()
    with TieredCluster(tmp_path, 3, store=store, storage_kw=dict(durability=durability), **LEASE) as tc:
        a, b, c = tc.ids
        tc.create_database(a)
        for node in tc.ids:
            tc.commit(node, incr)
        acked = 3
        assert _settle(tc, tc.ids, acked) == {n: acked for n in tc.ids}
        time.sleep(0.5)                       # persisted: the store holds the first commits
        fs.writeback(str(tmp_path))
        store.set_outage(True)                # from now on records live on the local tiers only
        tc.cluster.net.partition([a, b], [c])
        for i in range(COMMITS):
            tc.commit((a, b)[i % 2], incr)
            acked += 1
        assert _settle(tc, [a, b], acked) == {a: acked, b: acked}
        lost = [a, b]
        for node in lost:
            fs.frozen.add(os.path.realpath(os.path.join(str(tmp_path), node)))
        for node in lost:
            tc.kill(node)
        for node in lost:
            fs.power_loss(os.path.join(str(tmp_path), node))
        tc.cluster.net.heal()
        for node in lost:
            tc.revive(node)
        values = _settle(tc, tc.ids, acked)
        assert values == {n: acked for n in tc.ids}, \
            f"after a majority power loss ({durability}) the nodes read {values}, {acked} acknowledged"
        for node in tc.ids:
            tc.db(node).run(incr, **dict(retries=50, backoff=(0.001, 0.02)))
            acked += 1
        store.set_outage(False)
        values = _settle(tc, tc.ids, acked)
        assert values == {n: acked for n in tc.ids}, f"no commit after the power loss: {values}"


def _kind5_entries(path: str) -> int:
    return sum(1 for p in read_log(os.path.join(path, "roots.log"), b"ROOT") if p and p[0] == RECORDS_KIND)


def test_a_restart_redoes_the_records_and_checkpoints(tmp_path):
    """A process restart (no power loss) with unsynced data files: the reopened storage
    rebuilds them from the acceptor log, checkpoints and compacts the log (no kind-5 entry
    left), and a second restart has nothing to redo."""
    with TieredCluster(tmp_path, 3, storage_kw=dict(durability="none", release_local=False), **LEASE) as tc:
        a, b, c = tc.ids
        tc.create_database(a)
        for i in range(6):
            tc.commit(tc.ids[i % 3], incr)
        assert _settle(tc, tc.ids, 6) == {n: 6 for n in tc.ids}
        path = os.path.join(str(tmp_path), b)
        assert _kind5_entries(path) > 0
        tc.kill(b)
        tc.revive(b)
        stats = tc.storages[b].stats()
        assert stats["redo_entries"] > 0 and stats["checkpoints"] >= 1
        assert _kind5_entries(path) == 0
        assert _settle(tc, tc.ids, 6) == {n: 6 for n in tc.ids}
        tc.kill(b)
        tc.revive(b)
        assert tc.storages[b].stats()["redo_entries"] == 0
        tc.commit(b, incr)
        assert _settle(tc, tc.ids, 7) == {n: 7 for n in tc.ids}


def test_a_torn_acceptor_log_tail_is_trimmed_and_the_rest_redone(tmp_path):
    """An interrupted kind-5 append (bytes cut inside the last entry) is trimmed at open; the
    entries before it are redone.  The cut entry was never acknowledged: its append had not
    returned, so its sync had not run."""
    with TieredCluster(tmp_path, 3, storage_kw=dict(durability="none", release_local=False), **LEASE) as tc:
        a, b, c = tc.ids
        tc.create_database(a)
        for i in range(4):
            tc.commit(a, incr)
        assert _settle(tc, tc.ids, 4) == {n: 4 for n in tc.ids}
        path = os.path.join(str(tmp_path), b)
        tc.kill(b)
        log = os.path.join(path, "roots.log")
        size = os.path.getsize(log)
        assert _kind5_entries(path) >= 2
        with open(log, "r+b") as f:
            f.truncate(size - 7)              # the last entry loses its end
        tc.revive(b)
        assert tc.storages[b].stats()["redo_entries"] >= 1
        tc.commit(b, incr)
        assert _settle(tc, tc.ids, 5) == {n: 5 for n in tc.ids}


def test_a_checkpoint_keeps_the_records_of_entries_appended_after_its_mark(tmp_path, monkeypatch):
    """A checkpoint compacts only what it synced: entries appended during it keep their
    records, and a restart after it still redoes them."""
    from proto_db.storage import tiered as tiered_module
    monkeypatch.setattr(tiered_module, "CHECKPOINT_BYTES", 1)     # checkpoint before every acceptance
    with TieredCluster(tmp_path, 3, storage_kw=dict(durability="none", release_local=False), **LEASE) as tc:
        a, b, c = tc.ids
        tc.create_database(a)
        for i in range(5):
            tc.commit(tc.ids[i % 3], incr)
        assert _settle(tc, tc.ids, 5) == {n: 5 for n in tc.ids}
        assert tc.storages[b].stats()["checkpoints"] >= 3
        assert _kind5_entries(os.path.join(str(tmp_path), b)) >= 1     # the newest is kept with its records
        tc.kill(b)
        tc.revive(b)
        assert _settle(tc, tc.ids, 5) == {n: 5 for n in tc.ids}
        tc.commit(b, incr)
        assert _settle(tc, tc.ids, 6) == {n: 6 for n in tc.ids}
