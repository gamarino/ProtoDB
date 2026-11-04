"""Owner and permission-holder failures under load (Task 8.3f, D50).

Every case runs a three-member cluster of tiered storages in ``durability="fsync"``; two
threads per member increment one counter (``run`` with retries) and record each commit as
acknowledged or of unknown outcome.  While they run, one failure hits the lease owner (the
member granting write permissions) or a permission holder.  Afterwards the faults stop and
the invariants are checked:

* safety: ``acked <= n <= acked + unknown`` -- no acknowledged commit is lost and nothing
  is applied that no thread attempted; every live member converges on the same ``n``;
* liveness: every live member commits again after the fault (a new owner, or the old one,
  grants permissions).

D50 does not rest on the permission for safety: a stale permission or an abandoned term may
cost an unknown outcome, never a lost or doubled acknowledged commit.
"""
import os
import threading
import time

import pytest

from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError
from tests.support.power_loss import PowerLoss
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(240)]

LEASE = dict(lease_duration=0.9, request_timeout=0.1)
STORAGE = dict(durability="fsync")
THREADS = 2
SECONDS = 1.6


class Load:
    """Two committing threads per member; counts acknowledged and unknown commits."""

    def __init__(self, tc: TieredCluster):
        self.tc = tc
        self.lock = threading.Lock()
        self.acked = 0
        self.unknown = 0
        self.stop = threading.Event()
        self.down = set()
        self.threads = [threading.Thread(target=self._worker, args=(node,), daemon=True)
                        for node in tc.ids for _ in range(THREADS)]

    def _worker(self, node: str) -> None:
        while not self.stop.is_set():
            if node in self.down:
                time.sleep(0.01)
                continue
            space = self.tc.spaces.get(node)
            if space is None:
                time.sleep(0.01)
                continue
            try:
                space.open_database("db").run(incr, **RUN_KW)
            except ProtoOutcomeUnknownError:
                with self.lock:
                    self.unknown += 1
                continue
            except (ProtoConflictError, ProtoTimeoutError):
                continue
            except Exception:           # the space closed under a killed node: not a commit
                if node in self.down or self.tc.spaces.get(node) is not space:
                    continue
                raise
            with self.lock:
                self.acked += 1

    def __enter__(self):
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        for t in self.threads:
            t.join(30)
        assert not any(t.is_alive() for t in self.threads), "a committing thread hung"


def _owner(tc: TieredCluster, timeout: float = 5.0) -> str:
    """The member owning the current term, as the members see it (waits for one)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node in tc.ids:
            try:
                if tc[node].owner == node:
                    return node
            except KeyError:
                continue
        time.sleep(0.005)
    raise AssertionError("no member owns a term under load")


def _check(tc: TieredCluster, load: Load, live) -> None:
    """Every live member commits again, then the safety invariant on the converged counter."""
    tc.cluster.net.set_faults(drop=0.0, duplicate=0.0, delay=(0.0, 0.0))
    tc.cluster.net.heal()
    for node in live:
        # An unknown outcome is conservative (ruling R86): counted, and the member commits a
        # new increment until one is acknowledged (the bound below accounts for each one).
        for _ in range(5):
            try:
                tc.db(node).run(incr, **RUN_KW)
            except ProtoOutcomeUnknownError:
                load.unknown += 1
                continue
            load.acked += 1
            break
        else:
            pytest.fail(f"{node} never got a commit acknowledged after the faults healed")
    deadline = time.monotonic() + 10.0
    while True:                 # a member learns the others' last commits from their "done"
        values = {node: tc.value(node) for node in live}
        if len(set(values.values())) == 1:
            break
        assert time.monotonic() < deadline, f"the members did not converge: {values}"
        time.sleep(0.01)
    value = values[live[0]]
    assert load.acked <= value <= load.acked + load.unknown, \
        f"n = {value}, acknowledged {load.acked}, unknown {load.unknown}"
    assert load.acked > 2 * len(live)


def _cluster(tmp_path, **kw):
    tc = TieredCluster(tmp_path, 3, storage_kw=STORAGE, **dict(LEASE, **kw))
    tc.spaces[tc.ids[0]].new_database("db")
    return tc


def test_owner_crash_while_members_hold_or_await_permissions(tmp_path):
    with _cluster(tmp_path) as tc:
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            load.down.add(owner)
            tc.kill(owner)
            time.sleep(SECONDS / 3)
            tc.revive(owner)
            load.down.discard(owner)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


def test_owner_partitioned_past_its_lease(tmp_path):
    with _cluster(tmp_path) as tc:
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            tc.cluster.net.partition({owner}, set(tc.ids) - {owner})
            time.sleep(LEASE["lease_duration"] * 2.5)       # the others elect a new owner
            tc.cluster.net.heal()
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


def test_owner_stalled_past_its_lease_like_sigstop(tmp_path):
    """The owner's process stops (SIGSTOP): it neither sends nor receives for longer than its
    lease, then continues as if no time had passed for its threads."""
    with _cluster(tmp_path) as tc:
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            load.down.add(owner)                            # its committing threads stall too
            tc.cluster.net.partition({owner}, set(tc.ids) - {owner})
            time.sleep(LEASE["lease_duration"] * 2.5)
            tc.cluster.net.heal()
            load.down.discard(owner)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


def _hook_replicate(tc: TieredCluster, node: str, action):
    """Run ``action()`` once, right before ``node`` sends its next replication."""
    coordinator = tc[node]
    replicate = coordinator.replicate
    fired = threading.Event()

    def hooked(lease, root, **kw):
        if not fired.is_set():
            fired.set()
            action()
        return replicate(lease, root, **kw)

    coordinator.replicate = hooked
    return fired


def test_a_stale_permission_after_an_owner_change_never_commits_a_fork(tmp_path):
    """A holder stalls with its permission past the owner's lease; the owner is killed and
    another member owns a higher term; then the holder replicates under the old term."""
    with _cluster(tmp_path) as tc:
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            holder = next(n for n in tc.ids if n != owner)

            def stall():
                load.down.add(owner)
                tc.kill(owner)
                time.sleep(LEASE["lease_duration"] * 2.5)   # a new owner serves the third member

            fired = _hook_replicate(tc, holder, stall)
            assert fired.wait(10)
            time.sleep(SECONDS / 3)
            tc.revive(owner)
            load.down.discard(owner)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


def test_an_owner_change_in_the_middle_of_a_batch(tmp_path):
    """The owner dies while a holder's round is being replicated: the holder's round needs only
    a majority of acceptors, the owner's term ends, and a new owner takes over."""
    with _cluster(tmp_path) as tc:
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            holder = next(n for n in tc.ids if n != owner)

            def kill_owner():
                load.down.add(owner)
                threading.Thread(target=tc.kill, args=(owner,), daemon=True).start()

            assert _hook_replicate(tc, holder, kill_owner).wait(10)
            time.sleep(SECONDS / 2)
            tc.revive(owner)
            load.down.discard(owner)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


def test_a_holder_crash_in_the_middle_of_its_replication(tmp_path):
    """The holder's replication reaches one peer only, then the holder dies: the owner never
    hears ``done``, abandons its term, and the next owner's recovery decides the root."""
    with _cluster(tmp_path) as tc:
        net = tc.cluster.net
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            holder = next(n for n in tc.ids if n != owner)
            third = next(n for n in tc.ids if n not in (owner, holder))

            def cut():
                load.down.add(holder)
                net.partition({holder, third}, {owner})      # the round reaches the third only

            assert _hook_replicate(tc, holder, cut).wait(10)
            time.sleep(0.02)
            tc.kill(holder)
            net.heal()
            time.sleep(SECONDS / 3)
            tc.revive(holder)
            load.down.discard(holder)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


def test_duplicated_and_reordered_messages(tmp_path):
    with _cluster(tmp_path) as tc:
        tc.cluster.net.set_faults(duplicate=0.2, delay=(0.0, 0.004))
        with Load(tc) as load:
            time.sleep(SECONDS)
        _check(tc, load, tc.ids)


@pytest.mark.parametrize("round_", range(3))
def test_duplicates_delayed_beyond_a_holder_restart(tmp_path, round_):
    """8.3f review I2: copies of the owner's messages (permits among them) are duplicated and
    delayed beyond a kill/revive of a permission holder in the middle of its round, so a
    permit issued to the dead incarnation reaches the next one.  Before protocol 9 the new
    incarnation answered it ``done("none")`` and the owner reissued the base (C1).

    The lease is longer than this file's default: a permit is valid for what remains of the
    owner's lease, and the holder believes a third of it (ruling R78b).  With 0.9 s (a 0.3 s
    belief), permits delayed 0.25 s on the owner-to-holder link arrived all but expired and the
    holder sometimes never replicated within 30 s (1 run in 6, before and after the Task
    8.3f-B coordinator changes; liveness only)."""
    with _cluster(tmp_path, lease_duration=2.4) as tc:
        net = tc.cluster.net
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            holder = next(n for n in tc.ids if n != owner)
            net.set_faults(duplicate=0.5, delay=(0.0, 0.05))
            net.set_link_delay(owner, holder, 0.25)     # permits reach the holder late

            def crash():
                load.down.add(holder)
                net.partition({holder}, set(tc.ids) - {holder})   # the round reaches nobody

            assert _hook_replicate(tc, holder, crash).wait(30)
            time.sleep(0.02)
            tc.kill(holder)
            net.heal()
            tc.revive(holder)                    # well within the 0.25 s of delayed permits
            load.down.discard(holder)
            time.sleep(SECONDS / 2)
            net.set_link_delay(owner, holder, 0)
        _check(tc, load, tc.ids)


class _FrozenPowerLoss(PowerLoss):
    """:class:`PowerLoss` that stops recording ``fsync`` calls under a dead node's directory
    (a host without power syncs nothing; closing its space must not make anything durable)."""

    def __init__(self, monkeypatch):
        super().__init__(monkeypatch)
        self.frozen = set()

    @staticmethod
    def _entries(path: str) -> dict:
        # The live nodes create and remove files while a directory sync is recorded: an entry
        # removed between the listing and its lstat is simply not in the recorded listing.
        out = {}
        for name in os.listdir(path):
            try:
                st = os.lstat(os.path.join(path, name))
            except FileNotFoundError:
                continue
            out[name] = (st.st_dev, st.st_ino)
        return out

    def _record_fd(self, fd: int) -> None:
        try:
            path = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:
            path = ""
        if any(path == d or path.startswith(d + "/") for d in self.frozen):
            return
        super()._record_fd(fd)


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd")
@pytest.mark.parametrize("role", ["owner", "holder"])
def test_power_loss_of_the_owner_or_of_a_holder_under_load(tmp_path, monkeypatch, role):
    fs = _FrozenPowerLoss(monkeypatch)
    with _cluster(tmp_path) as tc:
        fs.writeback(str(tmp_path))
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            victim = owner if role == "owner" else next(n for n in tc.ids if n != owner)
            path = os.path.realpath(os.path.join(str(tmp_path), victim))
            load.down.add(victim)
            fs.frozen.add(path)                  # the power goes: nothing more becomes durable
            tc.kill(victim)
            fs.power_loss(path)
            # Stays frozen: a straggling thread of the dead node may still call fsync on a file
            # the power loss removed, and nothing of the revived node needs recording.
            time.sleep(SECONDS / 3)
            tc.revive(victim)
            load.down.discard(victim)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)
