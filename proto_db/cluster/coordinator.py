"""ClusterCoordinator: the majority-granted write lease and root propagation (Task 8.3c, D44, D45).

A cluster is a static list of peers ``(node_id, host, port)``; its size ``N`` is fixed at
construction and a majority is ``floor(N/2) + 1`` votes, the requester's own included.
Every process that writes the space runs one coordinator; transactions of a space opened
with a coordinator publish roots only while holding the lease
(:mod:`proto_db.transactions`).

Acquiring
    The threads of one process queue on a local lock; the thread at its head asks every
    peer (itself included) to grant the lease for ``term = highest term seen + 1``.  It
    holds the lease once a majority has granted, for ``lease_duration - drift_margin``
    measured on its own clock from the moment it SENT the request.  A lost attempt
    (a majority refused, or no majority answered within ``request_timeout``) is cancelled
    with a ``release`` broadcast, and the requester backs off: it sleeps a jittered
    exponential delay -- or, when a peer reported a running lease, up to that lease's
    remaining time -- and is woken early by any release received since it sent the
    request (a holder's release or another loser's cancel).  No spinning, no queue
    among processes; fairness is not guaranteed (D45).
Granting
    See :class:`~proto_db.cluster.lease.PeerGrants`: at most one holder per term, no
    other holder until the lease expires on the granter's clock or is released, terms
    below the highest seen refused, nothing persisted (a startup grace of one lease
    covers restarts).
Releasing
    Explicit: a ``release`` broadcast that carries the root the holder published, so the
    peers learn it.  Expiry is the fallback.
Root propagation
    Each coordinator keeps the latest storage root it knows (epochs only grow: reads never
    go backwards) and a ``suspect`` flag: True when it may have missed a newer root (at
    start, when a grant it made ended by expiry instead of a release, after a fencing
    event or a failed publish); cleared when it reads the storage, publishes, or receives
    a winner's release marked certain.  Grants and refusals carry the sender's root and
    whether it is certain.  The holder takes the highest root of its granting majority;
    it trusts it only when every granter is certain and all agree on the epoch, else the
    caller reads the storage (a gap or a disagreement).  Two majorities intersect, so a
    granter that saw the previous winner's release knows its root.
Fencing
    A lease's :attr:`~proto_db.cluster.lease.Lease.fencing_token` is ``(term, epoch)``;
    the storage enforces the epoch through ``publish_root(ptr, expected_epoch)``.  A
    failed conditional write under a lease is a fencing event: counted, logged, and the
    root is marked suspect.

Thread safety: every public method may be called from any thread.  One lock guards the
coordinator's state; it is never held while sending, and handlers called by the
transport only take it to update state.  The coordinator starts no thread of its own.
"""
import itertools
import logging
import random
import threading
import time
from typing import Callable, NamedTuple, Optional

from ..errors import ProtoTimeoutError, ProtoUserError, ProtoValidationError
from ..storage.format import RootRecord
from .lease import Lease, LeaseAttempt, PeerGrants, majority
from .protocol import decode_root, encode_root, make_message

__all__ = ["Peer", "ClusterCoordinator"]

logger = logging.getLogger(__name__)


class Peer(NamedTuple):
    """A member of the cluster: its id and the address its transport listens on."""

    node_id: str
    host: str
    port: int


def _number(name: str, value, *, positive: bool = True) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0 or (positive and value == 0):
        raise ProtoValidationError(f"{name} must be a {'positive' if positive else 'non-negative'} number")
    return float(value)


class ClusterCoordinator:
    """A cluster member granting and acquiring the write lease (see the module docstring).

    Args:
        node_id: this node's id; must appear in ``peers``.
        peers: every member, this node included, as ``(node_id, host, port)``.
        transport: a :class:`~proto_db.cluster.transport.Transport`; default a
            :class:`~proto_db.cluster.transport.TcpTransport` listening on this node's
            peer address.
        lease_duration: seconds a grant blocks other holders on the granter's clock.
        drift_margin: seconds subtracted from the holder's view of its lease (default 10 %
            of ``lease_duration``).  It must be at least ``2 * rho * D / (1 + rho)`` for
            clock rates within ``1 +/- rho`` of real time; the default covers ``rho`` up to
            about 5 %.  Clocks need bounded drift only, never synchronization.
        request_timeout: seconds an attempt waits for a majority of replies.
        acquire_timeout: default seconds :meth:`acquire` tries before ``ProtoTimeoutError``.
        backoff: ``(min_seconds, max_seconds)`` of the jittered exponential back-off
            between lost attempts.
        startup_grace: seconds after :meth:`start` during which this node grants nothing
            (default ``lease_duration``), so a promise made before a restart is honoured.
            ``0`` is safe only on a cluster's first start.
        clock: testing seam; the monotonic clock (seconds) used for every lease decision.
        seed: testing seam; seed of the back-off jitter generator.
    """

    def __init__(self, node_id: str, peers, *, transport=None, lease_duration: float = 2.0,
                 drift_margin: Optional[float] = None, request_timeout: float = 0.25,
                 acquire_timeout: float = 30.0, backoff: tuple = (0.001, 0.05),
                 startup_grace: Optional[float] = None, clock: Optional[Callable[[], float]] = None,
                 seed: Optional[int] = None):
        members = []
        for entry in peers:
            try:
                peer = Peer(*entry)
            except TypeError as e:
                raise ProtoValidationError(f"A peer must be (node_id, host, port), got {entry!r}") from e
            if not isinstance(peer.node_id, str) or not peer.node_id or not isinstance(peer.host, str) \
                    or type(peer.port) is not int or not 0 <= peer.port <= 65535:
                raise ProtoValidationError(f"Invalid peer {entry!r}")
            members.append(peer)
        ids = [p.node_id for p in members]
        if len(set(ids)) != len(ids):
            raise ProtoValidationError("Peer ids must be unique")
        if node_id not in ids:
            raise ProtoValidationError(f"Node {node_id!r} is not in the peer list")
        self._duration = _number("lease_duration", lease_duration)
        margin = self._duration * 0.1 if drift_margin is None else _number("drift_margin", drift_margin,
                                                                             positive=False)
        if margin >= self._duration:
            raise ProtoValidationError("drift_margin must be smaller than lease_duration")
        self._margin = margin
        self._request_timeout = _number("request_timeout", request_timeout)
        if self._request_timeout >= self._duration - self._margin:
            raise ProtoValidationError("request_timeout must be shorter than lease_duration - drift_margin")
        self._acquire_timeout = _number("acquire_timeout", acquire_timeout, positive=False)
        try:
            low, high = backoff
        except (TypeError, ValueError) as e:
            raise ProtoValidationError("backoff must be a (min_seconds, max_seconds) pair") from e
        low = _number("backoff minimum", low, positive=False)
        high = _number("backoff maximum", high, positive=False)
        if high < low:
            raise ProtoValidationError("backoff must satisfy min_seconds <= max_seconds")
        self._backoff = (low, high)
        self._grace = self._duration if startup_grace is None else _number("startup_grace", startup_grace,
                                                                              positive=False)
        self._clock = clock if clock is not None else time.monotonic
        self._rng = random.Random(seed)

        self._node = node_id
        self._peers = {p.node_id: p for p in members}
        self._others = [i for i in ids if i != node_id]
        self._majority = majority(len(members))
        if transport is None:
            from .transport import TcpTransport
            me = self._peers[node_id]
            transport = TcpTransport(me.host, me.port)
        self._transport = transport

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._local = threading.Lock()      # queues this process's threads
        self._grants = None                 # PeerGrants, created by start()
        self._state = "new"
        self._attempt = None
        self._held = None
        self._hint = 0                      # highest term learnt from replies
        self._rids = itertools.count(1)
        self._wakeups = 0                   # winners' releases received
        self._root_known = False
        self._root = None
        self._suspect = True
        self._stats = {"acquired": 0, "attempts": 0, "lost_attempts": 0, "fencing_events": 0,
                       "grants_given": 0, "releases_received": 0, "storage_root_reads": 0}

    # -- properties --------------------------------------------------------------------------

    @property
    def node_id(self) -> str:
        """This node's id."""
        return self._node

    @property
    def cluster_size(self) -> int:
        """Number of members, ``N``."""
        return len(self._peers)

    @property
    def majority(self) -> int:
        """Votes needed for the lease: ``floor(N/2) + 1``."""
        return self._majority

    @property
    def transport(self):
        """The transport in use."""
        return self._transport

    @property
    def lease_duration(self) -> float:
        """Seconds a grant lasts on the granter's clock."""
        return self._duration

    def stats(self) -> dict:
        """Counters: leases acquired, attempts, lost attempts, fencing events, grants given,
        winners' releases received and storage root reads reported by the caller."""
        with self._lock:
            return dict(self._stats)

    # -- lifecycle ---------------------------------------------------------------------------

    def start(self) -> "ClusterCoordinator":
        """Start the transport and begin granting after the startup grace.  Returns ``self``."""
        with self._lock:
            if self._state != "new":
                raise ProtoValidationError(f"The coordinator is {self._state}; it cannot be started")
            self._grants = PeerGrants(self._duration, self._clock, startup_grace=self._grace)
            self._state = "running"
        addresses = {i: ((p.host, p.port) if p.port else None) for i, p in self._peers.items()}
        try:
            self._transport.start(self._node, addresses, self._on_message)
        except BaseException:
            with self._lock:
                self._state = "stopped"
            raise
        return self

    def stop(self) -> None:
        """Stop the transport and wake every waiting acquirer (they raise).  Idempotent.

        A lease still held is not released explicitly; the peers let it expire.
        """
        with self._cond:
            if self._state == "stopped":
                return
            started = self._state == "running"
            self._state = "stopped"
            self._cond.notify_all()
        if started:
            self._transport.stop()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False

    def _check_running(self) -> None:
        if self._state != "running":
            raise ProtoValidationError(f"The coordinator is {self._state}")

    # -- known root --------------------------------------------------------------------------

    def known_root(self) -> tuple:
        """``(known, RootRecord or None)``: the latest root this node knows (``None`` with
        ``known`` True is the empty space)."""
        with self._lock:
            return self._root_known, self._root

    def _observe(self, known: bool, root: Optional[RootRecord]) -> None:
        """Adopt ``root`` if newer (lock held)."""
        if not known:
            return
        epoch = root.epoch if root is not None else 0
        mine = (self._root.epoch if self._root is not None else 0) if self._root_known else -1
        if epoch > mine:
            self._root_known, self._root = True, root

    def observe_root(self, root: Optional[RootRecord], *, from_storage: bool = False) -> None:
        """Learn ``root`` (``None``: the empty space).  ``from_storage=True`` means it was just
        read from the storage, so this node no longer suspects having missed a root."""
        with self._lock:
            self._observe(True, root)
            if from_storage:
                self._suspect = False
                self._stats["storage_root_reads"] += 1

    def mark_root_suspect(self) -> None:
        """Note that a newer root than the known one may exist (e.g. a publish failed)."""
        with self._lock:
            self._suspect = True

    def published(self, lease: Lease, root: RootRecord) -> None:
        """Record that ``lease``'s holder published ``root``; the release carries it to the peers."""
        with self._lock:
            lease.published = root
            self._observe(True, root)
            self._suspect = False

    def record_fencing_event(self, lease: Lease, error: BaseException) -> None:
        """A conditional root write failed under ``lease``: count it, log it, mark the root suspect."""
        with self._lock:
            self._stats["fencing_events"] += 1
            self._suspect = True
            valid = lease.is_valid()
        logger.warning("Fencing event on node %s: the conditional root write of lease term %d "
                       "(token %s, lease %s) was rejected by the storage: %s",
                       self._node, lease.term, tuple(lease.fencing_token),
                       "still valid" if valid else "expired", error)

    def _wire_root(self):
        return encode_root(self._root, known=self._root_known)

    # -- message handling --------------------------------------------------------------------

    def _on_message(self, msg: dict) -> None:
        sender = msg["from"]
        if sender not in self._peers or sender == self._node:
            logger.warning("Node %s ignored a %r message from unknown sender %r", self._node, msg["type"], sender)
            return
        kind = msg["type"]
        reply = None
        with self._cond:
            if self._state != "running":
                return
            if "root" in msg:
                known, root = decode_root(msg["root"])
                self._observe(known, root)
            if kind == "request":
                reply = self._decide(sender, msg["term"], msg["rid"])
            elif kind in ("grant", "reject"):
                self._on_reply(sender, msg)
            else:
                self._on_release(sender, msg)
        if reply is not None:
            self._transport.send(sender, reply)

    def _decide(self, requester: str, term: int, rid: int) -> dict:
        """Apply the grant rules to a request (lock held); return the reply."""
        decision = self._grants.on_request(requester, term)
        if decision.previous_expired:
            self._suspect = True
        root = self._wire_root()
        certain = not self._suspect
        if decision.granted:
            self._stats["grants_given"] += 1
            return make_message("grant", self._node, term=term, rid=rid, root=root, certain=certain)
        return make_message("reject", self._node, term=term, rid=rid, root=root, certain=certain,
                            reason=decision.reason, highest=decision.highest,
                            remaining=max(0.0, decision.remaining))

    def _on_reply(self, sender: str, msg: dict) -> None:
        self._hint = max(self._hint, msg["term"], msg.get("highest", 0))
        attempt = self._attempt
        if attempt is None or attempt.rid != msg["rid"] or attempt.term != msg["term"]:
            return
        if msg["type"] == "grant":
            known, root = decode_root(msg["root"])
            attempt.on_grant(sender, known, root, msg["certain"])
        else:
            attempt.on_reject(sender, msg["reason"], msg["highest"], msg["remaining"])
        if attempt.state != "pending":
            self._cond.notify_all()

    def _on_release(self, sender: str, msg: dict) -> None:
        if self._grants.on_release(sender, msg["term"]):
            self._suspect = True
        if msg["held"]:
            self._stats["releases_received"] += 1
            if msg["certain"]:
                known, root = decode_root(msg["root"])
                mine = (self._root.epoch if self._root is not None else 0) if self._root_known else -1
                if known and (root.epoch if root is not None else 0) >= mine:
                    self._suspect = False
        # Any release -- a winner's or a lost attempt's cancel -- may free the peer that
        # refused us, so it ends a waiting requester's back-off.
        self._wakeups += 1
        self._cond.notify_all()

    def _broadcast(self, msg: dict) -> None:
        for peer in self._others:
            self._transport.send(peer, msg)

    # -- acquiring and releasing -------------------------------------------------------------

    def acquire(self, timeout: Optional[float] = None) -> Lease:
        """Acquire the write lease; raise ``ProtoTimeoutError`` after ``timeout`` seconds
        (default ``acquire_timeout``) and ``ProtoValidationError`` if the coordinator stops.

        One thread of this process at a time holds or requests the lease; others wait here.
        Release with :meth:`release` (or use :meth:`lease`).
        """
        timeout = self._acquire_timeout if timeout is None else _number("timeout", timeout, positive=False)
        deadline = time.monotonic() + timeout
        with self._lock:
            self._check_running()
        if not self._local.acquire(timeout=timeout):
            raise ProtoTimeoutError(f"Node {self._node}: another thread held the cluster lease for {timeout} s")
        try:
            lease = self._acquire(deadline, timeout)
        except BaseException:
            self._local.release()
            raise
        return lease

    def _acquire(self, deadline: float, timeout: float) -> Lease:
        failures = 0
        while True:
            attempt, request, seen = self._open_attempt()
            self._broadcast(request)
            lease, cancel, wait = self._close_attempt(attempt)
            if lease is not None:
                return lease
            self._broadcast(cancel)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProtoTimeoutError(
                    f"Node {self._node} could not obtain the cluster write lease from a majority "
                    f"({self._majority} of {self.cluster_size}) within {timeout} s")
            low, high = self._backoff
            delay = min(high, low * (2 ** min(failures, 30))) * (0.5 + self._rng.random() / 2)
            delay = min(max(delay, wait), remaining)
            failures += 1
            with self._cond:
                self._check_running()
                # ``seen`` dates from before the request: a release that arrived since then
                # (even before the refusals) ends the wait at once.
                self._cond.wait_for(lambda: self._wakeups != seen or self._state != "running", timeout=delay)
                self._check_running()
                woken = self._wakeups != seen
            if woken:
                # Spread the requesters woken by one release so they do not split the votes.
                time.sleep(self._rng.random() * low * min(failures, 8))

    def _open_attempt(self) -> tuple:
        with self._lock:
            self._check_running()
            term = max(self._grants.highest_term, self._hint) + 1
            attempt = LeaseAttempt(term=term, rid=next(self._rids), sent_at=self._clock(),
                                   cluster_size=self.cluster_size, lease_duration=self._duration,
                                   drift_margin=self._margin)
            self._attempt = attempt
            self._stats["attempts"] += 1
            decision = self._grants.on_request(self._node, term)
            if decision.previous_expired:
                self._suspect = True
            if decision.granted:
                attempt.on_grant(self._node, self._root_known, self._root, not self._suspect)
            else:
                attempt.on_reject(self._node, decision.reason, decision.highest, decision.remaining)
            request = make_message("request", self._node, term=term, rid=attempt.rid)
            seen = self._wakeups
        return attempt, request, seen

    def _close_attempt(self, attempt: LeaseAttempt) -> tuple:
        """Wait for the attempt's outcome; return ``(lease, None, 0)`` or ``(None, cancel, wait)``."""
        with self._cond:
            self._cond.wait_for(lambda: attempt.state != "pending" or self._state != "running",
                                timeout=self._request_timeout)
            self._attempt = None
            self._check_running()
            self._hint = max(self._hint, attempt.highest_seen)
            now = self._clock()
            if attempt.state == "won" and now < attempt.valid_until:
                known, root, certain = attempt.best_root()
                self._observe(known, root)
                if certain:
                    self._suspect = False
                lease = Lease(term=attempt.term, acquired_at=now, valid_until=attempt.valid_until,
                              root=root if known else None, root_certain=certain, clock=self._clock)
                self._held = lease
                self._stats["acquired"] += 1
                return lease, None, 0.0
            self._stats["lost_attempts"] += 1
            self._grants.on_release(self._node, attempt.term)
            cancel = make_message("release", self._node, term=attempt.term, root=self._wire_root(),
                                  certain=False, held=False)
            return None, cancel, min(attempt.busy_remaining, self._duration)

    def release(self, lease: Lease) -> None:
        """Release ``lease`` and broadcast the latest known root.  Never raises for a lease
        this node holds; releasing twice is a no-op."""
        with self._lock:
            if lease is not self._held:
                if lease.released_at is not None:
                    return
                raise ProtoUserError("This lease is not held by this coordinator")
            lease._mark_released()
            self._held = None
            self._grants.on_release(self._node, lease.term)
            running = self._state == "running"
            msg = make_message("release", self._node, term=lease.term, root=self._wire_root(),
                               certain=not self._suspect, held=True)
        try:
            if running:
                self._broadcast(msg)
        finally:
            self._local.release()

    def lease(self, timeout: Optional[float] = None) -> "_LeaseContext":
        """Context manager: :meth:`acquire` on entry (yielding the lease), :meth:`release` on exit."""
        return _LeaseContext(self, timeout)


class _LeaseContext:
    """``with coordinator.lease() as lease:`` (see :meth:`ClusterCoordinator.lease`)."""

    def __init__(self, coordinator: ClusterCoordinator, timeout: Optional[float]):
        self._coordinator = coordinator
        self._timeout = timeout
        self._lease = None

    def __enter__(self) -> Lease:
        self._lease = self._coordinator.acquire(self._timeout)
        return self._lease

    def __exit__(self, exc_type, exc, tb):
        self._coordinator.release(self._lease)
        return False
