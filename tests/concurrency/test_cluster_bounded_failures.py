"""Failures under load with bounded replication (Task 8.3g, D54).

Five or eight members of tiered storages in ``durability="fsync"``: three voters (``n0``,
``n1``, ``n2``) and learners, every member committing (two threads each), each commit's
records on the committer and one data replica.  One failure hits a role -- the owner while it
grants a permission, replicates or broadcasts ``done``; a data replica before or after its
``stored`` acknowledgement; a voter, a learner or the owner losing power -- or a random
schedule of message faults and kills runs.  Afterwards the faults stop and the invariants of
:mod:`tests.concurrency.test_cluster_owner_failures` are checked: every live member commits
again, the members converge, and ``acked <= n <= acked + unknown`` (ruling R86).
"""
import os
import random
import threading
import time

import pytest

from tests.concurrency.test_cluster_owner_failures import (LEASE, SECONDS, STORAGE, Load, _check,
                                                            _FrozenPowerLoss, _hook_replicate, _owner)
from tests.support.tiered_cluster import TieredCluster

pytestmark = [pytest.mark.timeout(300)]

VOTERS = ("n0", "n1", "n2")


def _cluster(tmp_path, n=5, **kw):
    tc = TieredCluster(tmp_path, n, storage_kw=STORAGE, **dict(LEASE, **kw))
    tc.spaces[tc.ids[0]].new_database("db")
    return tc


def _successor(tc, node):
    ids = sorted(tc.ids)
    return ids[(ids.index(node) + 1) % len(ids)]


@pytest.mark.parametrize("n", [5, 8])
def test_owner_crash_while_members_hold_or_await_permissions(tmp_path, n):
    with _cluster(tmp_path, n) as tc:
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


def test_owner_crash_during_a_learners_replication(tmp_path):
    with _cluster(tmp_path) as tc:
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            holder = next(m for m in ("n3", "n4") if m != owner)

            def kill_owner():
                load.down.add(owner)
                threading.Thread(target=tc.kill, args=(owner,), daemon=True).start()

            assert _hook_replicate(tc, holder, kill_owner).wait(10)
            time.sleep(SECONDS / 2)
            tc.revive(owner)
            load.down.discard(owner)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


def test_owner_crash_during_its_done_broadcast(tmp_path):
    """The owner commits, then dies while broadcasting ``done``: the message reaches part of
    the members only; the others learn the root from the next owner's recovery."""
    with _cluster(tmp_path) as tc:
        net = tc.cluster.net
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            coordinator = tc[owner]
            finish = coordinator.finish
            fired = threading.Event()

            def hooked(permission, *a, **kw):
                if not fired.is_set() and permission.outcome == "committed":
                    fired.set()
                    load.down.add(owner)
                    # The broadcast reaches one voter and one learner only.
                    reach = {owner, next(v for v in VOTERS if v != owner), "n4" if owner != "n4" else "n3"}
                    net.partition(reach, set(tc.ids) - reach)
                    try:
                        return finish(permission, *a, **kw)
                    finally:
                        threading.Thread(target=tc.kill, args=(owner,), daemon=True).start()
                return finish(permission, *a, **kw)

            coordinator.finish = hooked
            assert fired.wait(10)
            time.sleep(0.05)
            net.heal()
            time.sleep(SECONDS / 2)
            tc.revive(owner)
            load.down.discard(owner)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


@pytest.mark.parametrize("when", ["before", "after"])
def test_a_data_replica_dies_before_or_after_its_acknowledgement(tmp_path, when):
    """A learner commits; its data replica (its successor) dies before it stores the records
    (the commit hedges to the next member) or right after acknowledging them (the commit
    stands on the committer's copy and is fetched from it or from the object store)."""
    with _cluster(tmp_path) as tc:
        holder = "n3"
        replica = _successor(tc, holder)
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            if when == "before":
                def kill_replica():
                    load.down.add(replica)
                    tc.kill(replica)

                assert _hook_replicate(tc, holder, kill_replica).wait(10)
            else:
                transport = tc[replica].transport
                send = transport.send
                fired = threading.Event()

                def hooked(to, msg):
                    send(to, msg)
                    if msg["type"] == "ack" and msg["stored"] and to == holder and not fired.is_set():
                        fired.set()
                        load.down.add(replica)
                        threading.Thread(target=tc.kill, args=(replica,), daemon=True).start()

                transport.send = hooked
                assert fired.wait(10)
            time.sleep(SECONDS / 2)
            tc.revive(replica)
            load.down.discard(replica)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd")
@pytest.mark.parametrize("role", ["owner", "voter", "learner", "data replica"])
def test_power_loss_of_every_role_under_load(tmp_path, monkeypatch, role):
    fs = _FrozenPowerLoss(monkeypatch)
    with _cluster(tmp_path) as tc:
        fs.writeback(str(tmp_path))
        with Load(tc) as load:
            time.sleep(SECONDS / 3)
            owner = _owner(tc)
            victim = {"owner": owner,
                      "voter": next(v for v in VOTERS if v != owner),
                      "learner": next(m for m in ("n3", "n4") if m != owner),
                      "data replica": next(m for m in ("n4", "n0") if m != owner)}[role]
            path = os.path.realpath(os.path.join(str(tmp_path), victim))
            load.down.add(victim)
            fs.frozen.add(path)
            tc.kill(victim)
            fs.power_loss(path)
            time.sleep(SECONDS / 3)
            tc.revive(victim)
            load.down.discard(victim)
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)


RUNS = 25


@pytest.mark.parametrize("n", [5, 8])
@pytest.mark.parametrize("seed", range(RUNS))
def test_no_acknowledged_commit_is_lost_under_random_faults(tmp_path, n, seed):
    """Safety under load with mixed voter and learner committers (25 schedules per size): the
    links duplicate and delay messages; one or two members (any role) are killed and revived,
    or partitioned and healed, at random instants."""
    rng = random.Random(seed * 1000 + n)
    with _cluster(tmp_path, n) as tc:
        net = tc.cluster.net
        net.set_faults(duplicate=rng.choice([0.0, 0.1, 0.3]), delay=(0.0, rng.choice([0.0, 0.004, 0.02])))
        with Load(tc) as load:
            for _ in range(rng.randint(1, 2)):
                time.sleep(rng.uniform(0.1, SECONDS / 3))
                victim = rng.choice(tc.ids)
                if rng.random() < 0.5:
                    load.down.add(victim)
                    tc.kill(victim)
                    time.sleep(rng.uniform(0.05, LEASE["lease_duration"] * 1.5))
                    tc.revive(victim)
                    load.down.discard(victim)
                else:
                    net.partition({victim}, set(tc.ids) - {victim})
                    time.sleep(rng.uniform(0.05, LEASE["lease_duration"] * 1.5))
                    net.heal()
            time.sleep(SECONDS / 3)
        _check(tc, load, tc.ids)
