"""Acceptor state is durable in every data durability mode (Task 8.3e, defect D-SAFE-2, D46).

The acceptor log of a node -- its promises, accepted and committed roots and the rejoined
marker -- is what the lease and replication safety argument relies on, as Raft relies on
``currentTerm``/``votedFor``/the log.  Before Task 8.3e a ``TieredStorage`` (and a shared
``FileStorage`` through ``PlainTier``) in ``"flush"`` or ``"none"`` durability wrote that log
without any ``fsync``.  These tests replay a power loss on one node with
:class:`tests.support.power_loss.PowerLoss`: only what an ``fsync`` (or an earlier kernel
write-back) made durable survives.  The write-back after the cluster formed makes the old
entries durable (the rejoined marker in particular), so the node restarts as an acceptor and
does not rejoin; before the fix, it then forgot the promise it had just made.

Two schedules, both inside the declared failure model (one node loses power once):

* a *granter* forgets its promise (the schedule of ``test_cluster_terms`` with a power loss
  instead of a clean restart): ``a`` wins term T with ``b``; ``b`` loses power; ``c``, which
  never saw T, asks for T and ``b`` grants it again;
* the *proposer* forgets its own promise: ``a`` wins T with ``b`` and commits R; ``a`` loses
  power; asked again, ``c`` grants T and ``a`` holds T a second time, then commits another root
  with R's label and epoch.
"""
import os

import pytest

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.errors import ProtoOutcomeUnknownError
from proto_db.storage.file import FileStorage
from proto_db.storage.tiered import ROOTS_LOG, TieredStorage
from proto_db.cluster.persister import PlainTier
from proto_db.cluster.rootlog import KINDS, LOG_KIND, _decode
from proto_db.storage.logfile import scan
from tests.support.fake_object_store import FakeObjectStore
from tests.support.manual_cluster import D, GRACE, MARGIN, Harness, rec
from tests.support.power_loss import PowerLoss

pytestmark = [pytest.mark.timeout(60)]

MODES = ["none", "flush", "fsync"]


class DiskHarness(Harness):
    """The manual harness (:mod:`tests.support.manual_cluster`) over real on-disk tiers: a
    ``TieredStorage`` per node in ``<tmp>/<node>`` (``kind="tiered"``), or a ``PlainTier`` over
    one ``FileStorage`` per node (``kind="plain"``; each node's acceptor log is the file
    ``cluster/<node>.roots`` of its storage), all in ``durability``."""

    def __init__(self, tmp_path, durability: str, kind: str = "tiered", names="abc"):
        self.tmp = str(tmp_path)
        self.durability = durability
        self.kind = kind
        self.store = FakeObjectStore()
        self.storages = {}
        super().__init__(names)

    def path(self, name: str) -> str:
        return os.path.join(self.tmp, name)

    def _node(self, name, grace):
        node = ClusterCoordinator(name, self.peers, transport=self.net.transport(), lease_duration=D,
                                  drift_margin=MARGIN, request_timeout=0.5, startup_grace=grace,
                                  clock=self.clock, seed=1, persist_interval=60.0, persist_lag=60.0)
        node.start()
        if self.kind == "tiered":
            storage = TieredStorage(self.path(name), self.store, durability=self.durability, ship_interval=60.0)
            tier = storage
        else:
            storage = FileStorage(self.path(name), durability=self.durability)
            tier = PlainTier(storage, name)
        self.storages[name] = storage
        node.attach(tier)
        return node

    def power_cycle(self, name: str, fs: PowerLoss):
        """``name``'s host loses power: the process dies, its disk keeps only what was durable,
        and it restarts with the shortest safe startup grace (its acceptor log is not empty)."""
        old = self.nodes[name]
        tier = old.tier
        old.stop()
        tier.close()
        if self.storages[name] is not tier:
            self.storages[name].close()
        self.retired.append(old)
        fs.power_loss(self.path(name))
        self.nodes[name] = self._node(name, GRACE)
        return self.nodes[name]

    def close(self):
        super().close()
        for name, node in self.nodes.items():
            tier = node.tier
            if tier is not None:
                tier.close()
            if self.storages.get(name) is not tier:
                self.storages[name].close()


def _harness(tmp_path, monkeypatch, durability, kind="tiered"):
    fs = PowerLoss(monkeypatch)
    h = DiskHarness(tmp_path, durability, kind)
    return fs, h


@pytest.mark.parametrize("kind", ["tiered", "plain"])
@pytest.mark.parametrize("durability", MODES)
def test_a_granter_that_loses_power_never_grants_a_term_twice(tmp_path, monkeypatch, durability, kind):
    fs, h = _harness(tmp_path, monkeypatch, durability, kind)
    try:
        h.bootstrap("a")
        fs.writeback(h.tmp)                                   # the formed cluster's logs reach the disk
        lease_a = h.attempt("a", {"b"})                       # c's request is lost
        term = lease_a.term
        outcome = h.replicate("a", lease_a, rec(2), to=(), expire=True)   # every copy is lost
        assert isinstance(outcome, ProtoOutcomeUnknownError)
        h.release("a", lease_a, to=())                        # lost too
        h.power_cycle("b", fs)
        assert not h.nodes["b"].stats()["rejoining"], "the write-back kept b's rejoined marker"
        assert h.nodes["b"].tier.load_roots().promised >= term, \
            f"b forgot its promise of term {term} in a power loss ({durability})"
        h.clock.t += GRACE                                    # grace over, every grant expired
        lease_c = h.attempt("c", {"b"})
        assert lease_c.term > term, f"b granted term {term} twice across a power loss ({durability})"
        r_prime = rec(2)
        assert not isinstance(h.replicate("c", lease_c, r_prime, to={"b"}), Exception)
        h.release("c", lease_c, to={"a", "b"})
        h.clock.t += GRACE
        lease_a2 = h.attempt("a", {"b", "c"})
        root, _ = h.base("a", lease_a2, responders={"b", "c"})
        assert root == r_prime                                # never the minority root
    finally:
        h.close()


@pytest.mark.parametrize("kind", ["tiered", "plain"])
@pytest.mark.parametrize("durability", MODES)
def test_a_proposer_that_loses_power_never_wins_its_ballot_twice(tmp_path, monkeypatch, durability, kind):
    fs, h = _harness(tmp_path, monkeypatch, durability, kind)
    try:
        h.bootstrap("a")
        fs.writeback(h.tmp)
        lease_a = h.attempt("a", {"b"})                       # c never sees the ballot
        term = lease_a.term
        r = rec(2)
        assert not isinstance(h.replicate("a", lease_a, r, to={"b"}), Exception)   # committed: a + b
        h.release("a", lease_a, to=())                        # c never learns it
        h.power_cycle("a", fs)
        assert not h.nodes["a"].stats()["rejoining"]
        logged = h.nodes["a"].tier.load_roots()
        assert logged.promised >= term, f"a forgot its own promise of term {term} ({durability})"
        assert logged.accepted is not None and logged.accepted[1] == r, \
            f"a forgot the root it accepted and that a majority acknowledged ({durability})"
        h.clock.t += GRACE
        lease = h.attempt("a", {"c"})                         # b is not asked
        assert not isinstance(lease, Exception), lease
        assert lease.term > term, f"a won its ballot {term} a second time ({durability})"
        base, _ = h.base("a", lease, responders={"c"})
        assert base == r                                      # the committed root is the base
        h.release("a", lease, to={"b", "c"})
    finally:
        h.close()


@pytest.mark.parametrize("durability", MODES)
def test_acceptor_log_entries_are_synced_before_the_reply_they_justify(tmp_path, monkeypatch, durability):
    """Every promise and accepted root a node logged is durable when the reply leaves: the
    durable bytes of ``roots.log`` decode to the same promise and accepted root as the file."""
    fs, h = _harness(tmp_path, monkeypatch, durability)
    try:
        h.bootstrap("a")
        lease = h.attempt("a", {"b", "c"})
        assert not isinstance(h.replicate("a", lease, rec(2), to={"b", "c"}), Exception)
        h.release("a", lease, to={"b", "c"})
        for name in "abc":
            path = os.path.join(h.path(name), ROOTS_LOG)
            durable = fs.durable_contents(path)
            with open(path, "rb") as f:
                current = f.read()
            assert durable is not None, f"{name}: roots.log never synced ({durability})"
            # Only committed entries (knowledge, not promises: durable with the next synced
            # entry, rootlog module docstring) may follow the durable prefix.
            assert current.startswith(durable)
            tail = scan(current, LOG_KIND, path).payloads[len(scan(durable, LOG_KIND, path).payloads):]
            assert all(p[0] == KINDS["committed"] for p in tail), f"{name}: unsynced acceptor entries"
            synced = _decode(scan(durable, LOG_KIND, path).payloads, path)
            logged = h.nodes[name].tier.load_roots()
            assert synced.promised == logged.promised, f"{name}: a promise was not synced ({durability})"
            assert synced.accepted == logged.accepted, f"{name}: an accepted root was not synced ({durability})"
            assert synced.empty == logged.empty, f"{name}: the rejoined marker was not synced ({durability})"
    finally:
        h.close()


@pytest.mark.parametrize("kind", ["tiered", "plain"])
@pytest.mark.parametrize("durability", MODES)
def test_a_new_acceptor_log_and_its_directories_survive_a_power_loss(tmp_path, monkeypatch, durability, kind):
    """A promise made on a log created moments before survives a power loss with no write-back:
    the log file, its directory entry and the directory's own entry were synced when the log
    was opened.  (For ``plain`` the space directory is the user's; it is assumed durable.)"""
    fs = PowerLoss(monkeypatch)
    root = str(tmp_path / "disk")
    os.mkdir(root)
    fs.writeback(str(tmp_path))                     # the disk's mount point exists, empty
    path = os.path.join(root, "node")

    def open_tier():
        if kind == "tiered":
            storage = TieredStorage(path, FakeObjectStore(), durability=durability, ship_interval=60.0)
            return storage, storage
        storage = FileStorage(path, durability=durability)
        return storage, PlainTier(storage, "a")

    storage, tier = open_tier()
    if kind == "plain":
        fs.writeback(str(tmp_path))                 # the user's space directory is durable
    tier.log_promise(5 << 12)
    tier.log_root("accepted", 5 << 12, rec(3))
    tier.log_rejoined()
    tier.close()
    if storage is not tier:
        storage.close()
    fs.power_loss(root)
    storage, tier = open_tier()
    try:
        logged = tier.load_roots()
        assert logged.promised == 5 << 12, f"the promise was lost with the new log ({durability}, {kind})"
        assert logged.accepted is not None and logged.accepted[0] == 5 << 12
        assert not logged.empty
    finally:
        tier.close()
        if storage is not tier:
            storage.close()
