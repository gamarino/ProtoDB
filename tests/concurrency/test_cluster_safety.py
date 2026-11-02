"""Fault-injection safety of the cluster lease (Tasks 8.3c and 8.3d, D44, D45, D46).

Hypothesis draws a schedule for an in-process cluster of 3 or 5 peers committing to one
shared storage: message drop, duplicate and delay (reorder) rates, per-node clock drift
within the bound the drift margin covers, delays up to twice the margin, and timed events -- partitions, heals, crashes
(a node's transport stops while its threads keep running: a frozen or partitioned
process), restarts (a fresh coordinator with the default startup grace) and freezes (a
node's next replication stalls for longer than a lease: a stale holder).

Invariants checked on every schedule, in global real time (``time.monotonic``, with each
node's drifting clock mapped back by ``DriftClock.to_real``):

1. at most one peer believes it holds a valid lease at any instant (lease intervals
   ``[acquired, min(valid_until, released)]`` of distinct leases never overlap);
2. no committed update is lost or duplicated: every ``Database.run`` that returned added
   its unique id to a blind-written ``CountedSet`` exactly once, and no other id appears
   except those whose commit ended with ``ProtoOutcomeUnknownError`` (present at most
   once).  The final state is read from the cluster after the faults stop (every live
   node reopens its space; one more commit on the newest lineage), then from the storage
   once it is persisted (D46): both agree;
3. a stale holder never commits: a holder whose replication resumed after its lease ended
   was refused (``ProtoConflictError`` or ``ProtoOutcomeUnknownError``); the persisters'
   root writes (the only ones, D46) all pass an expected epoch, and one whose expected
   epoch was no longer current never succeeds.
"""
import itertools
import re
import threading
import time
import warnings

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.cluster import Cluster, DriftClock

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(240)]

LEASE = 0.3
MARGIN = 0.03
RHO = 0.04                  # 2 * RHO * LEASE / (1 + RHO) = 0.023 <= MARGIN
RUN_SECONDS = 1.2
THREADS_PER_NODE = 2
FINAL_RETRIES = []          # (schedule seed, attempt, error type, message) of every final-commit retry
FINAL_RETRIES_LOCK = threading.Lock()
COORD_KW = dict(lease_duration=LEASE, drift_margin=MARGIN, request_timeout=0.15, startup_grace=0.0,
                backoff=(0.001, 0.02), acquire_timeout=0.6)


class FencingProbe:
    """Shared storage proxy checking the persisters' root writes (invariant 3)."""

    def __init__(self, inner):
        self._inner = inner
        self._lock = threading.Lock()
        self.violations = []
        self.failed_cas = 0
        self._views = {}

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def for_node(self, node_id):
        """The node's view of the storage; one per node, so that its in-memory acceptor log
        (keyed by the storage object, D46) survives the node's restarts like a disk."""
        with self._lock:
            view = self._views.get(node_id)
            if view is None:
                view = self._views[node_id] = self._new_view(node_id)
            return view

    def _new_view(self, node_id):
        probe = self

        class NodeView:
            def __getattr__(self, name):
                return getattr(probe._inner, name)

            def publish_root(self, ptr, expected_epoch):
                return probe.publish_as(node_id, ptr, expected_epoch)

        return NodeView()

    def _epoch(self):
        root = self._inner.read_root()
        return root.epoch if root is not None else 0

    def publish_root(self, ptr, expected_epoch):
        return self.publish_as("setup", ptr, expected_epoch)

    def publish_as(self, node_id, ptr, expected_epoch):
        if expected_epoch is None:
            with self._lock:
                self.violations.append(f"{node_id} published without an expected epoch")
        current = self._epoch()
        try:
            record = self._inner.publish_root(ptr, expected_epoch)
        except ProtoConflictError:
            with self._lock:
                self.failed_cas += 1
            raise
        with self._lock:
            if expected_epoch != current:
                self.violations.append(f"{node_id} published on stale epoch {expected_epoch} (current {current})")
        return record


class Freezer:
    """Stalls a node's next replication for longer than a lease (a stale holder)."""

    def __init__(self, cluster):
        self._cluster = cluster
        self._lock = threading.Lock()
        self.freezes = {}             # node -> seconds to stall its next replication
        self.results = []             # (cluster committed epoch moved during the stall, refused)

    def _top(self):
        return max(node.committed_epoch for node in self._cluster.nodes.values())

    def wrap(self, node):
        replicate = node.replicate

        def frozen_replicate(lease, root, **kw):
            with self._lock:
                stall = self.freezes.pop(node.node_id, None)
            if stall is None:
                return replicate(lease, root, **kw)
            before = self._top()
            time.sleep(stall)
            moved = self._top() != before
            try:
                result = replicate(lease, root, **kw)
            except (ProtoConflictError, ProtoOutcomeUnknownError):
                with self._lock:
                    self.results.append((moved, True))
                raise
            with self._lock:
                self.results.append((moved, False))
            return result

        node.replicate = frozen_replicate


class Recorder:
    """Records every lease a node held, in real time."""

    def __init__(self):
        self._lock = threading.Lock()
        self.leases = []

    def wrap(self, node, clock):
        acquire, release = node.acquire, node.release

        def recorded_release(lease):
            release(lease)
            end = min(lease.valid_until, lease.released_at if lease.released_at is not None else lease.valid_until)
            with self._lock:
                self.leases.append((clock.to_real(lease.acquired_at), clock.to_real(end), node.node_id,
                                    lease.term))

        node.acquire = acquire
        node.release = recorded_release


events_strategy = st.lists(
    st.tuples(st.floats(0.05, RUN_SECONDS - 0.2),
              st.sampled_from(["partition", "heal", "crash", "restart", "freeze"]),
              st.integers(0, 4), st.integers(0, 2 ** 16)),
    max_size=5)

schedule_strategy = st.fixed_dictionaries({
    "size": st.sampled_from([3, 5]),
    "drop": st.sampled_from([0.0, 0.05, 0.2]),
    "duplicate": st.sampled_from([0.0, 0.1, 0.3]),
    "delay": st.sampled_from([0.0, 0.01, 0.06]),     # up to twice the drift margin
    "rates": st.lists(st.floats(1 - RHO, 1 + RHO), min_size=5, max_size=5),
    "events": events_strategy,
    "seed": st.integers(0, 2 ** 16),
})


def _run_schedule(schedule):
    size = schedule["size"]
    clocks = {f"n{i}": DriftClock(rate=schedule["rates"][i], offset=1000.0 * i) for i in range(size)}
    storage = FencingProbe(MemoryStorage())
    recorder = Recorder()
    ObjectSpace(storage).new_database("db")
    uids = itertools.count()
    committed = set()
    unknown = set()
    committed_lock = threading.Lock()
    unexpected = []
    crashed = set()
    with Cluster(size, seed=schedule["seed"], clocks=clocks, **COORD_KW) as cl:
        cl.net.set_faults(drop=schedule["drop"], duplicate=schedule["duplicate"], delay=(0.0, schedule["delay"]))
        freezer = Freezer(cl)
        for node_id in cl.ids:
            recorder.wrap(cl[node_id], clocks[node_id])
            freezer.wrap(cl[node_id])
        stop_at = None            # set once the first rejoins are done
        spaces = {}               # node -> (coordinator, its space): one space per incarnation
        spaces_lock = threading.Lock()

        def space_of(node_id):
            with spaces_lock:
                node = cl.nodes[node_id]
                entry = spaces.get(node_id)
                if entry is None or entry[0] is not node:
                    entry = spaces[node_id] = (node, ObjectSpace(storage.for_node(node_id), coordinator=node,
                                                                 lock_timeout=0.6))
                return entry[1]

        def worker(node_id):
            while time.monotonic() < stop_at:
                uid = next(uids)
                try:
                    db = space_of(node_id).open_database("db")
                    db.run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(uid)),
                           retries=3, backoff=(0.001, 0.01))
                except (ProtoTimeoutError, ProtoConflictError, ProtoValidationError):
                    continue      # not committed: no quorum, retries exhausted, or the node stopped
                except ProtoOutcomeUnknownError:
                    with committed_lock:
                        unknown.add(uid)
                    continue
                except BaseException as e:   # surfaced by the assertion below
                    unexpected.append(e)
                    raise
                with committed_lock:
                    committed.add(uid)

        # A new cluster rejoins once every member answered (D46, R47/R49): open every space
        # and let the first rejoins finish before the faults start.
        for node_id in cl.ids:
            space_of(node_id)
        cl.wait_rejoined()
        stop_at = time.monotonic() + RUN_SECONDS
        threads = [threading.Thread(target=worker, args=(n,), daemon=True)
                   for n in cl.ids for _ in range(THREADS_PER_NODE)]
        for t in threads:
            t.start()
        start = time.monotonic()
        for at, kind, who, salt in sorted(schedule["events"]):
            time.sleep(max(0.0, start + at - time.monotonic()))
            node_id = cl.ids[who % size]
            if kind == "partition":
                members = list(cl.ids)
                cut = 1 + salt % (size - 1)
                rotated = members[salt % size:] + members[:salt % size]
                cl.net.partition(set(rotated[:cut]), set(rotated[cut:]))
            elif kind == "heal":
                cl.net.heal()
            elif kind == "crash":
                cl.crash(node_id)
                crashed.add(node_id)
            elif kind == "restart":
                cl.restart(node_id, startup_grace=LEASE + MARGIN)   # the safe grace (R51)
                crashed.discard(node_id)
                recorder.wrap(cl[node_id], clocks[node_id])
                freezer.wrap(cl[node_id])
            else:
                freezer.freezes[node_id] = LEASE * 1.5
        for t in threads:
            t.join(30)
        assert not any(t.is_alive() for t in threads), "a worker hung"
        assert not unexpected, unexpected
        # The faults stop; one more commit on the newest lineage, then its state is final.
        cl.net.set_faults(drop=0.0, duplicate=0.0, delay=(0.0, 0.0))
        cl.net.heal()
        alive = [n for n in cl.ids if n not in crashed]
        if len(alive) * 2 > size:
            # Every live process has its space open again: a node restarted after the workers
            # stopped has none yet, and without a space it grants nothing (Y2: the final commit
            # then found 2 live members of 5 and timed out).  Restarted nodes grant only after
            # their startup grace, so a lease may take a few attempts.  The write is idempotent,
            # so an outcome left unknown is retried as well; the invariants below do not depend
            # on it.  Measured cause of those (re-review 4, M2): the committing thread woke after
            # its 0.3 s lease had ended (a scheduling convoy under load), often with a majority
            # already acknowledged; the coordinator then reports the outcome as unknown,
            # conservatively.  Every retry is reported (count and reason), never hidden.
            for node_id in alive:
                space_of(node_id)
            final = space_of(alive[0])
            for attempt in itertools.count():
                try:
                    final.open_database("db").run(lambda tr: tr.set_root_object("final", True), retries=50,
                                                  backoff=(0.01, 0.05))
                    break
                except (ProtoTimeoutError, ProtoOutcomeUnknownError) as e:
                    with FINAL_RETRIES_LOCK:
                        FINAL_RETRIES.append((schedule["seed"], attempt, type(e).__name__, str(e)))
                    print(f"final commit retry {attempt + 1} (seed {schedule['seed']}): "
                          f"{type(e).__name__}: {e}")
                    if attempt == 10:
                        raise
            tr = final.open_database("db").new_transaction()
            counted = tr.get_root_object("ids")
            in_cluster = {} if counted is None else dict(counted.items())
            tr.abort()
            final.sync(timeout=10)
        else:
            in_cluster = None

    # 1. lease beliefs never overlap in real time
    leases = sorted(recorder.leases)
    for (s1, e1, n1, t1), (s2, e2, n2, t2) in zip(leases, leases[1:]):
        assert e1 <= s2 + 1e-6, f"overlapping leases: {n1}/term {t1} [{s1:.4f}, {e1:.4f}] and " \
                                f"{n2}/term {t2} [{s2:.4f}, {e2:.4f}]"
    # 2. no lost or duplicated update
    tr = ObjectSpace(storage).open_database("db").new_transaction()
    counted = tr.get_root_object("ids")
    found = {} if counted is None else dict(counted.items())
    tr.abort()
    if in_cluster is not None:
        assert found == in_cluster                      # the persisted state is the cluster's
        assert committed <= set(found), sorted(committed - set(found))[:5]
    assert set(found) <= committed | unknown, sorted(set(found) - committed - unknown)[:5]
    assert all(c == 1 for c in found.values())
    # 3. stale publishes never succeed; a frozen holder overtaken by another commit was refused
    assert not storage.violations, storage.violations
    for moved, refused in freezer.results:
        assert refused or not moved
    return len(committed), len(leases), sum(1 for _, refused in freezer.results if refused)


@settings(max_examples=14, deadline=None, derandomize=True,
          suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
@given(schedule=schedule_strategy)
def test_random_fault_schedules_keep_the_lease_and_the_data_safe(schedule):
    _run_schedule(schedule)


@pytest.fixture(autouse=True, scope="module")
def _report_final_commit_retries():
    """Self-report (re-review 4, M2): after this module, a warning states how many final
    commits were retried and why, so a retry is visible in the run summary, never silent."""
    with FINAL_RETRIES_LOCK:
        FINAL_RETRIES.clear()
    yield
    with FINAL_RETRIES_LOCK:
        retries = list(FINAL_RETRIES)
    if retries:
        reasons = {}
        for _, _, kind, message in retries:
            key = f"{kind}: " + re.sub(r"\bn\d+\b|\d+(\.\d+)?", "#", message.split(";")[0])
            reasons[key] = reasons.get(key, 0) + 1
        detail = "; ".join(f"{count} x {reason}" for reason, count in sorted(reasons.items()))
        warnings.warn(f"{len(retries)} final-commit retries in the safety schedules: {detail}", stacklevel=1)


def test_a_frozen_holder_overtaken_by_another_commit_is_fenced():
    commits, leases, refused = _run_schedule({
        "size": 3, "drop": 0.0, "duplicate": 0.0, "delay": 0.0, "rates": [1.0] * 5, "seed": 7,
        "events": [(0.2, "freeze", 0, 0)],
    })
    assert commits > 0 and refused >= 1


# -- deterministic cases for the two timing rules of the lease (review R39 item 3) -------------

def _belief_end(lease, clock):
    return clock.to_real(min(lease.valid_until, lease.released_at or lease.valid_until))


def test_the_drift_margin_covers_the_worst_clock_rates():
    """The holder runs slow (1 - rho), the next requester's granters fast (1 + rho): without
    ``drift_margin`` the holder still believes in its lease after they granted the next one."""
    rho, lease = 0.1, 0.4
    margin = 0.08                        # >= 2 * rho * D / (1 + rho) = 0.0727
    clocks = {"n0": DriftClock(1 - rho), "n1": DriftClock(1 + rho, 50.0), "n2": DriftClock(1 + rho, 90.0)}
    with Cluster(3, tiers=True, clocks=clocks, lease_duration=lease, drift_margin=margin, request_timeout=0.05,
                 startup_grace=0.0, backoff=(0.001, 0.005)) as cl:
        held = cl["n0"].acquire(timeout=5)          # kept to expiry, never released
        nxt = cl["n1"].acquire(timeout=5)
        try:
            assert _belief_end(held, clocks["n0"]) <= clocks["n1"].to_real(nxt.acquired_at)
        finally:
            cl["n1"].release(nxt)
            cl["n0"].release(held)


def test_the_lease_window_is_measured_from_the_send_not_the_win():
    """Grants to n0 are delayed 0.3 s: counted from its win, n0's window would outlast the
    promises its granters made when they received the request."""
    with Cluster(3, tiers=True, lease_duration=1.0, drift_margin=0.1, request_timeout=0.45, startup_grace=0.0,
                 backoff=(0.001, 0.005)) as cl:
        cl.net.set_link_delay("n1", "n0", 0.3)
        cl.net.set_link_delay("n2", "n0", 0.3)
        held = cl["n0"].acquire(timeout=5)          # won about 0.3 s after sending; never released
        nxt = cl["n1"].acquire(timeout=5)
        try:
            assert held.valid_until <= nxt.acquired_at   # one shared clock (time.monotonic)
        finally:
            cl["n1"].release(nxt)
            cl["n0"].release(held)
