"""ClusterCoordinator: the majority-granted write lease and the cluster commit point
(Tasks 8.3c and 8.3d, decisions D44, D45, D46).

A cluster is a static list of peers ``(node_id, host, port)``; its size ``N`` is fixed at
construction and a majority is ``floor(N/2) + 1`` votes, the requester's own included.
Every process that writes the space runs one coordinator; transactions of a space opened
with a coordinator commit only while holding the lease (:mod:`proto_db.transactions`).

Acquiring
    The threads of one process queue on a local lock; the thread at its head asks every
    peer (itself included) to grant the lease for ``term = highest term known + 1``.  It
    holds the lease once a majority has granted, for ``lease_duration - drift_margin``
    measured on its own clock from the moment it SENT the request.  A lost attempt is
    cancelled with a ``release`` broadcast, and the requester backs off (jittered
    exponential delay, or up to a running lease's remaining time), woken early by any
    release.  No spinning, no queue among processes; fairness is not guaranteed (D45).
Granting
    See :class:`~proto_db.cluster.lease.PeerGrants`.  Requests and releases carry the
    sender's incarnation (a random number drawn at each start), so a restarted node is
    never confused with its previous life.
Roots and their labels (D46)
    Every root the cluster knows carries the term of the lease that replicated it, its
    *label*; roots are ordered by ``(label, epoch)``.  Each node keeps its *accepted* root
    (the highest root it has durably accepted from a replication) and its *committed* root
    (the newest root it knows a majority acknowledged).  Transactions read committed roots
    only; the holder builds on the highest accepted root of its granting majority.
The commit point (D46)
    Under the lease the holder validates and merges against that root, then
    :meth:`ClusterCoordinator.replicate` sends ``(epoch, root pointer)`` with the records
    the commit added to every peer.  A peer accepts it -- stores the records and the root
    durably, then acknowledges -- unless it is starting or has granted a higher term (the
    ballot rule).  The commit is acknowledged once a majority (the holder included) has
    acknowledged before the lease ends; the release then tells the peers it is committed.
    Every later lease's granting majority intersects that acknowledging majority, and a
    peer that acknowledged before granting reports the root (or a newer one) with its
    grant, so the next holder learns the newest committed root from its granters.  No
    storage root is written in the critical section: the persister
    (:mod:`proto_db.cluster.persister`) writes it later, coalesced.
Certainty
    A lease is certain when every granter knows a root and none reports a grant whose
    outcome it could not know (one that ended by expiry, by its holder's restart, or a
    release marked uncertain); such a suspicion is cleared when the node learns a root
    labelled with that term or later.  An uncertain lease runs a recovery round
    (:meth:`ClusterCoordinator.recover_root`): the roots of a majority of peers.
Peer services
    ``query``/``answer`` (recovery, joining peers) and ``fetch``/``fetched`` (peer page
    requests for records not yet in the object store).

Threads: the transport's own threads, plus one worker started by :meth:`start` that
applies replicated records, serves page requests and prefetches, and the persister
started by :meth:`attach`.  One lock guards the coordinator's state; it is never held
while sending, while doing I/O, or while calling user code.
"""
import itertools
import logging
import queue
import random
import threading
import time
import uuid
from typing import Callable, NamedTuple, Optional

from ..core.atom import AtomPointer
from ..errors import (ProtoConflictError, ProtoDBError, ProtoOutcomeUnknownError, ProtoTimeoutError,
                      ProtoUserError, ProtoValidationError)
from ..storage.format import RootRecord
from .lease import Lease, LeaseAttempt, PeerGrants, majority
from .protocol import decode_labeled_root, encode_root, make_message

__all__ = ["Peer", "ClusterCoordinator"]

logger = logging.getLogger(__name__)

#: Once a majority has answered an undecided attempt, the remaining replies are awaited for
#: ``max(_MIN_STRAGGLE, _STRAGGLE_FACTOR * time those replies took)`` seconds at most.
_MIN_STRAGGLE = 0.005
_STRAGGLE_FACTOR = 4.0
#: Upper bound, in seconds, of the random spread a requester adds after being woken by a release.
_WAKE_SPREAD = 0.003
_STOP = object()
_CRITICAL_KEYS = ("validation", "replay", "path_copy", "replication")


#: Term increment when a cluster refounds from the persisted root (:meth:`ClusterCoordinator.refound`).
REFOUND_TERM_STEP = 1 << 32


class Peer(NamedTuple):
    """A member of the cluster: its id and the address its transport listens on."""

    node_id: str
    host: str
    port: int


def _number(name: str, value, *, positive: bool = True) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0 or (positive and value == 0):
        raise ProtoValidationError(f"{name} must be a {'positive' if positive else 'non-negative'} number")
    return float(value)


def _key(label: int, record: Optional[RootRecord]) -> tuple:
    return label, (record.epoch if record is not None else 0)


class _Waiter:
    """Replies gathered for one replicate, query or fetch (guarded by the coordinator lock)."""

    __slots__ = ("kind", "term", "acks", "nacks", "answers", "data", "missing")

    def __init__(self, kind: str, term: int = 0):
        self.kind = kind
        self.term = term
        self.acks = set()
        self.nacks = set()
        self.answers = {}
        self.data = None
        self.missing = 0


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
            of ``lease_duration``); at least ``2 * rho * D / (1 + rho)`` for clock rates
            within ``1 +/- rho``.
        request_timeout: seconds an attempt (and a recovery query) waits for a majority.
        acquire_timeout: default seconds :meth:`acquire` tries before ``ProtoTimeoutError``.
        backoff: ``(min_seconds, max_seconds)`` of the jittered back-off between lost attempts.
        startup_grace: seconds after :meth:`start` during which this node grants nothing and
            accepts no replicated root (default ``lease_duration``).  ``0`` is safe only on a
            cluster's first start.
        fetch_timeout: seconds a peer page request waits for an answer.
        persist_interval: seconds between two passes of the persister (D46).
        persist_lag: seconds the persisted epoch may lag the committed one before this node
            persists a root committed by another node (covers a crashed committer).
        clock: testing seam; the monotonic clock (seconds) used for every lease decision.
        seed: testing seam; seed of the back-off jitter generator.
    """

    def __init__(self, node_id: str, peers, *, transport=None, lease_duration: float = 2.0,
                 drift_margin: Optional[float] = None, request_timeout: float = 0.25,
                 acquire_timeout: float = 30.0, backoff: tuple = (0.001, 0.05),
                 startup_grace: Optional[float] = None, fetch_timeout: float = 2.0,
                 persist_interval: float = 0.05, persist_lag: float = 1.0,
                 clock: Optional[Callable[[], float]] = None, seed: Optional[int] = None):
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
        self._fetch_timeout = _number("fetch_timeout", fetch_timeout)
        self._persist_interval = _number("persist_interval", persist_interval)
        self._persist_lag = _number("persist_lag", persist_lag, positive=False)
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
        self._inc = random.SystemRandom().getrandbits(62)
        self._attempt = None
        self._held = None
        self._hint = 0                      # highest term learnt from replies and roots
        self._rids = itertools.count(1)
        self._wakeups = 0                   # releases received
        self._accepted = (False, None, 0)   # (known, RootRecord or None, label)
        self._committed = (False, None, 0)
        self._committed_by = None           # node that committed the committed root, if known
        self._committed_at = 0.0            # time.monotonic() when it was learnt
        self._committed_version = 0
        self._suspect_term = 0              # a grant whose outcome this node could not know
        self._waiters = {}                  # rid -> _Waiter
        self._queued = 0                    # peer messages queued for the worker, not yet handled
        self._stored = (-1, -1)             # (label, epoch) of the newest root whose records are here
        self._tier = None
        self._confirmed = True
        self._persister = None
        self._worker = None
        self._jobs = queue.Queue()
        self._stats = {"acquired": 0, "attempts": 0, "lost_attempts": 0, "fencing_events": 0,
                       "grants_given": 0, "releases_received": 0, "storage_root_reads": 0,
                       "replications": 0, "replication_failures": 0, "accepted": 0, "refused": 0,
                       "recoveries": 0, "fetches_requested": 0, "fetches_served": 0,
                       "critical_commits": 0}
        self._critical = {k: 0.0 for k in _CRITICAL_KEYS}

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
        """Votes needed for the lease and acknowledgements needed for a commit: ``floor(N/2) + 1``."""
        return self._majority

    @property
    def transport(self):
        """The transport in use."""
        return self._transport

    @property
    def lease_duration(self) -> float:
        """Seconds a grant lasts on the granter's clock."""
        return self._duration

    @property
    def persister(self):
        """The :class:`~proto_db.cluster.persister.Persister` started by :meth:`attach`, or ``None``."""
        return self._persister

    @property
    def tier(self):
        """The attached cluster tier (see :mod:`proto_db.cluster.persister`), or ``None``."""
        return self._tier

    def stats(self) -> dict:
        """Counters: leases acquired, attempts, lost attempts, fencing events, grants given,
        winners' releases received, storage root reads reported by the caller, replications
        and their failures, replicated roots accepted and refused, recovery rounds, page
        requests sent and served, and ``critical``: the summed seconds spent inside the
        critical section by stage (validation, intent replay, path copy, replication) over
        ``critical_commits`` commits."""
        with self._lock:
            out = dict(self._stats)
            out["critical"] = dict(self._critical)
            return out

    def record_critical(self, timings: dict) -> None:
        """Add one commit's critical-section stage timings (seconds) to :meth:`stats`."""
        with self._lock:
            self._stats["critical_commits"] += 1
            for k in _CRITICAL_KEYS:
                self._critical[k] += float(timings.get(k, 0.0))

    # -- lifecycle ---------------------------------------------------------------------------

    def start(self) -> "ClusterCoordinator":
        """Start the transport and the worker; begin granting after the startup grace.  Returns ``self``."""
        with self._lock:
            if self._state != "new":
                raise ProtoValidationError(f"The coordinator is {self._state}; it cannot be started")
            self._grants = PeerGrants(self._duration, self._clock, startup_grace=self._grace)
            self._state = "running"
        self._worker = threading.Thread(target=self._work, name=f"proto-cluster-worker-{self._node}",
                                        daemon=True)
        self._worker.start()
        addresses = {i: ((p.host, p.port) if p.port else None) for i, p in self._peers.items()}
        try:
            self._transport.start(self._node, addresses, self._on_message)
        except BaseException:
            with self._lock:
                self._state = "stopped"
            self._jobs.put(_STOP)
            self._worker.join()
            raise
        return self

    def stop(self) -> None:
        """Stop the persister, the transport and the worker; wake every waiter.  Idempotent.

        A lease still held is revoked (its ``is_valid()`` turns False at once) and not
        released explicitly; the peers let it expire.
        """
        with self._cond:
            if self._state == "stopped":
                return
            started = self._state == "running"
            self._state = "stopped"
            if self._held is not None:
                self._held.revoke()
            persister, self._persister = self._persister, None
            self._cond.notify_all()
        if persister is not None:
            persister.stop()
        if started:
            self._transport.stop()
        worker = self._worker
        if worker is not None:
            self._jobs.put(_STOP)
            if worker is not threading.current_thread():
                worker.join()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False

    def _check_running(self) -> None:
        if self._state != "running":
            raise ProtoValidationError(f"The coordinator is {self._state}")

    def attach(self, tier) -> None:
        """Attach the cluster tier of the space this node commits to and start its persister.

        ``tier`` provides the storage and the tier operations described in
        :mod:`proto_db.cluster.persister`.  The roots the tier logged (its local WAL) and
        its term floor are loaded, so a restarted node knows what it accepted.  Attaching
        the same tier again is a no-op; another one raises ``ProtoValidationError``.
        """
        from .persister import Persister
        with self._lock:
            self._check_running()
            if self._tier is not None:
                if self._tier is tier or self._tier.storage is tier.storage:
                    return
                raise ProtoValidationError("This coordinator already serves another storage")
            self._tier = tier
        accepted, committed, top = tier.load_roots()
        with self._cond:
            if accepted is not None:
                self._observe_accepted(True, accepted[1], accepted[0])
            if committed is not None:
                self._observe_committed(True, committed[1], committed[0], None)
            self._hint = max(self._hint, top)
            tier.set_fetcher(self.fetch_record)
            self._confirmed = False
            persister = Persister(self, tier, interval=self._persist_interval, lag=self._persist_lag)
            self._persister = persister
        persister.start()

    def wait_idle(self, timeout: Optional[float] = None) -> bool:
        """Wait until every queued peer message (replications, releases, fetches) has been
        handled; return False after ``timeout`` seconds.  Messages are handled in arrival order
        by one worker thread; tests use this to make a delivery synchronous."""
        with self._cond:
            return self._cond.wait_for(lambda: self._queued == 0, timeout=timeout)

    def await_records(self, root: Optional[RootRecord], label: int, lease: Lease) -> bool:
        """Wait (briefly, under the lease) until the records of the root ``root`` labelled
        ``label`` are stored here: the replication carrying them may still be in this node's
        queue when the lease is won.  Returns False after ``min(fetch_timeout, a quarter of
        the remaining lease)``; reads then fall back to peer page requests."""
        if root is None or label == 0:
            return True             # the empty space, or a root written outside the cluster
        wanted = _key(label, root)
        timeout = min(self._fetch_timeout, max(0.0, lease.remaining()) / 4)
        with self._cond:
            return self._cond.wait_for(lambda: self._stored >= wanted or self._state != "running",
                                       timeout=timeout) and self._stored >= wanted

    def refound(self, persisted_label: int) -> int:
        """A granting majority knows no root: the cluster restarts from the persisted root
        (labelled ``persisted_label``).  Raise this node's term floor to
        ``persisted_label + REFOUND_TERM_STEP`` and return it.

        A node outside that majority may hold roots accepted after the persisted one but lost
        with the majority's disks; their labels are below the persisted label plus the number
        of leases granted since, far below the step, so the refounded lineage supersedes
        them instead of being superseded when that node returns (D46).
        """
        with self._lock:
            self._hint = max(self._hint, persisted_label + REFOUND_TERM_STEP)
            return self._hint

    @property
    def needs_confirmation(self) -> bool:
        """True until a lease confirmed this node's committed root after :meth:`attach`.

        A node that (re)starts knows only the roots its log recorded; a commit made while it
        was down, or whose release it missed, may be known committed only by others.  The
        space confirms by an empty commit under the lease (which commits the newest root of
        the granting majority if needed) before its first snapshot, so reads never go
        backwards across a restart.
        """
        with self._lock:
            return not self._confirmed

    def confirmed(self) -> None:
        """Record that a lease confirmed the committed root (see :attr:`needs_confirmation`)."""
        with self._lock:
            self._confirmed = True

    def detach(self, tier) -> None:
        """Stop the persister of ``tier`` and forget the tier (the space is closing)."""
        with self._lock:
            if self._tier is None or (self._tier is not tier and self._tier.storage is not tier.storage):
                return
            persister, self._persister = self._persister, None
            self._tier = None
        if persister is not None:
            persister.stop()
        tier.set_fetcher(None)

    # -- known roots -------------------------------------------------------------------------

    def known_root(self) -> tuple:
        """``(known, RootRecord or None)``: the newest committed root this node knows (``None``
        with ``known`` True is the empty space).  Transactions start from it."""
        with self._lock:
            return self._committed[0], self._committed[1]

    def accepted_root(self) -> tuple:
        """``(known, RootRecord or None, label)``: the highest root this node accepted."""
        with self._lock:
            return self._accepted

    def committed_root(self) -> tuple:
        """``(known, RootRecord or None, label, committer, learnt_at, version)``."""
        with self._lock:
            known, record, label = self._committed
            return known, record, label, self._committed_by, self._committed_at, self._committed_version

    @property
    def committed_epoch(self) -> int:
        """Epoch of the newest committed root this node knows (``0`` when none)."""
        with self._lock:
            record = self._committed[1]
            return record.epoch if record is not None else 0

    def wait_committed_change(self, version: int, timeout: float) -> int:
        """Wait until the committed root changes from ``version`` (or ``timeout``); return the version."""
        with self._cond:
            self._cond.wait_for(lambda: self._committed_version != version or self._state != "running",
                                timeout=timeout)
            return self._committed_version

    def _learn_label(self, label: int) -> None:
        """A root labelled ``label`` is known: clear a suspicion it answers (lock held)."""
        self._hint = max(self._hint, label)
        if self._suspect_term and label >= self._suspect_term:
            self._suspect_term = 0

    def _observe_accepted(self, known: bool, root: Optional[RootRecord], label: int) -> bool:
        """Adopt ``root`` as accepted if its ``(label, epoch)`` is higher (lock held)."""
        if not known:
            return False
        self._learn_label(label)
        if self._accepted[0] and _key(label, root) <= _key(self._accepted[2], self._accepted[1]):
            return False
        self._accepted = (True, root, label)
        return True

    def _observe_committed(self, known: bool, root: Optional[RootRecord], label: int, by, *,
                           accept: bool = True) -> bool:
        """Adopt ``root`` as committed if its ``(label, epoch)`` is higher (lock held).  A root
        known committed through the cluster is also accepted knowledge (``accept``); one read
        from the persisted root is not (with ``accept=False``): a granting majority that knows
        nothing newer must refound (:meth:`refound`)."""
        if not known:
            return False
        if accept:
            self._observe_accepted(True, root, label)
        else:
            self._learn_label(label)
        if self._committed[0] and _key(label, root) <= _key(self._committed[2], self._committed[1]):
            return False
        self._committed = (True, root, label)
        self._committed_by = by
        self._committed_at = time.monotonic()
        self._committed_version += 1
        self._cond.notify_all()
        return True

    def observe_root(self, root: Optional[RootRecord], *, from_storage: bool = False, term: int = 0,
                     accept: bool = True) -> None:
        """Learn ``root`` (``None``: the empty space), committed with label ``term``.

        ``from_storage=True`` counts a storage root read.  ``accept=False`` (the persisted
        root, D46) does not make it accepted knowledge (see :meth:`_observe_committed`).
        """
        with self._lock:
            self._observe_committed(True, root, term, None, accept=accept)
            if from_storage:
                self._stats["storage_root_reads"] += 1

    def mark_root_suspect(self) -> None:
        """Note that a newer root than the known one may exist (e.g. a replication failed)."""
        with self._lock:
            self._suspect_term = max(self._suspect_term, self._grants.highest_term if self._grants else 1, 1)

    def published(self, lease: Lease, root: RootRecord) -> None:
        """Record that ``lease``'s holder committed ``root`` (labelled with the lease term);
        the release tells the peers."""
        with self._lock:
            lease.published = root
            self._observe_committed(True, root, lease.term, self._node)
            if self._suspect_term and self._suspect_term <= lease.term:
                self._suspect_term = 0

    def record_fencing_event(self, lease: Lease, error: BaseException) -> None:
        """A write under ``lease`` was rejected (fencing): count it, log it, mark the root suspect."""
        with self._lock:
            self._stats["fencing_events"] += 1
            self._suspect_term = max(self._suspect_term, lease.term)
            valid = lease.is_valid()
        logger.warning("Fencing event on node %s: a write of lease term %d (token %s, lease %s) "
                       "was rejected: %s", self._node, lease.term, tuple(lease.fencing_token),
                       "still valid" if valid else "expired", error)

    def _wire(self, triple) -> object:
        known, root, label = triple
        return encode_root(root, known=known, term=label)

    # -- message handling --------------------------------------------------------------------

    def _on_message(self, msg: dict) -> None:
        sender = msg["from"]
        if sender not in self._peers or sender == self._node:
            logger.warning("Node %s ignored a %r message from unknown sender %r", self._node, msg["type"], sender)
            return
        kind = msg["type"]
        if kind in ("replicate", "release", "fetch"):
            # A release follows its replicate through the same queue, so a root is never known
            # committed here before its records are stored here.
            with self._lock:
                self._queued += 1
            self._jobs.put(msg)
            return
        reply = None
        with self._cond:
            if self._state != "running":
                return
            if kind == "request":
                reply = self._decide(sender, msg["term"], msg["rid"], msg["inc"])
            elif kind in ("grant", "reject"):
                self._on_reply(sender, msg)
            elif kind == "query":
                reply = make_message("answer", self._node, rid=msg["rid"], root=self._wire(self._accepted),
                                     committed=self._wire(self._committed), certain=not self._suspect_term)
            else:
                self._on_answer(sender, kind, msg)
        if reply is not None:
            self._transport.send(sender, reply)

    def _decide(self, requester: str, term: int, rid: int, inc: int) -> dict:
        """Apply the grant rules to a request (lock held); return the reply."""
        decision = self._grants.on_request(requester, term, inc)
        if decision.previous_expired:
            self._suspect_term = max(self._suspect_term, decision.ended_term)
        root = self._wire(self._accepted)
        certain = not self._suspect_term
        if decision.granted:
            self._stats["grants_given"] += 1
            return make_message("grant", self._node, term=term, rid=rid, root=root,
                                committed=self._wire(self._committed), certain=certain)
        return make_message("reject", self._node, term=term, rid=rid, root=root, certain=certain,
                            reason=decision.reason, highest=decision.highest,
                            remaining=max(0.0, decision.remaining))

    def _on_reply(self, sender: str, msg: dict) -> None:
        self._hint = max(self._hint, msg["term"], msg.get("highest", 0))
        known, root, label = decode_labeled_root(msg["root"])
        self._hint = max(self._hint, label)
        attempt = self._attempt
        if attempt is None or attempt.rid != msg["rid"] or attempt.term != msg["term"]:
            return
        if msg["type"] == "grant":
            attempt.on_grant(sender, known, root, msg["certain"], label)
            # The granter's committed root (it learnt it from the previous holder's release):
            # the next holder then knows its base is committed and needs no extra round.
            ck, croot, clabel = decode_labeled_root(msg["committed"])
            self._observe_committed(ck, croot, clabel, None, accept=False)
        else:
            attempt.on_reject(sender, msg["reason"], msg["highest"], msg["remaining"])
        self._cond.notify_all()   # decided, or a majority answered (straggler rule)

    def _on_release(self, sender: str, msg: dict) -> Optional[tuple]:
        """Handle a release (lock held); return ``(label, root)`` when it taught a newer
        committed root (logged by the caller, outside the lock)."""
        learnt = None
        ended = self._grants.on_release(sender, msg["term"], msg["inc"])
        if ended:
            self._suspect_term = max(self._suspect_term, ended)
        # The sender's accepted root is only a hint here: a root becomes accepted at this node
        # only with its records (a replication) or once known committed.
        known, root, label = decode_labeled_root(msg["root"])
        self._hint = max(self._hint, label)
        if msg["held"]:
            self._stats["releases_received"] += 1
            ck, croot, clabel = decode_labeled_root(msg["committed"])
            if self._observe_committed(ck, croot, clabel, sender):
                learnt = (clabel, croot)
            if not msg["certain"]:
                # The holder may have published something it could not replicate: whatever
                # its term decided is unknown here until a root of that term or later arrives.
                self._suspect_term = max(self._suspect_term, msg["term"])
        # Any release -- a winner's or a lost attempt's cancel -- may free the peer that
        # refused us, so it ends a waiting requester's back-off.
        self._wakeups += 1
        self._cond.notify_all()
        return learnt

    def _on_answer(self, sender: str, kind: str, msg: dict) -> None:
        waiter = self._waiters.get(msg["rid"])
        if waiter is None:
            return
        if kind == "ack":
            if waiter.kind != "replicate" or msg["term"] != waiter.term:
                return
            (waiter.acks if msg["ok"] else waiter.nacks).add(sender)
            known, root, label = decode_labeled_root(msg["root"])
            self._hint = max(self._hint, label)
        elif kind == "answer":
            if waiter.kind != "query":
                return
            waiter.answers[sender] = (decode_labeled_root(msg["root"]), decode_labeled_root(msg["committed"]),
                                      msg["certain"])
        elif kind == "fetched":
            if waiter.kind != "fetch":
                return
            if msg["data"] is not None and waiter.data is None:
                waiter.data = msg["data"]
            else:
                waiter.missing += 1
        self._cond.notify_all()

    def _broadcast(self, msg: dict) -> None:
        for peer in self._others:
            self._transport.send(peer, msg)

    # -- the worker: replicated records, page requests, prefetch ---------------------------

    def _work(self) -> None:
        while True:
            msg = self._jobs.get()
            if msg is _STOP:
                return
            try:
                if msg["type"] == "replicate":
                    self._apply_replicate(msg)
                elif msg["type"] == "release":
                    with self._cond:
                        learnt = self._on_release(msg["from"], msg) if self._state == "running" else None
                        tier = self._tier
                    if learnt is not None and tier is not None:
                        # Logged so that a restarted node never serves an older root than it did.
                        tier.log_root("committed", learnt[0], learnt[1], sync=False)
                else:
                    self._serve_fetch(msg)
            except (ProtoDBError, OSError) as e:
                logger.error("Node %s: handling a %r message failed: %s", self._node, msg["type"], e)
            finally:
                with self._cond:
                    self._queued -= 1
                    self._cond.notify_all()

    def _may_accept(self, term: int, root: Optional[RootRecord], label: int) -> bool:
        """The acceptance rules of a replicated root (lock held): running, not starting, no
        higher term granted here (ballot rule), and not older than the accepted root."""
        if self._state != "running" or self._grants.starting or self._grants.granted_top > term:
            return False
        return not self._accepted[0] or _key(label, root) >= _key(self._accepted[2], self._accepted[1])

    def _apply_replicate(self, msg: dict) -> None:
        sender, term, rid = msg["from"], msg["term"], msg["rid"]
        known, root, label = decode_labeled_root(msg["root"])
        tier = self._tier
        valid = known and label == term and root is not None
        with self._lock:
            # A node with no tier attached (no space open) can only witness roots whose records
            # live in a shared storage; records to store need a tier.
            ok = valid and (tier is not None or not msg["records"]) and self._may_accept(term, root, label)
        if valid and tier is not None:
            # Records are stored even when the root is refused (a newer term was granted here
            # meanwhile): immutable data, and this node may be the next holder, whose base can
            # be this root (adopted from its granters); it then reads nothing under its lease.
            if msg["records"]:
                tier.apply(msg["records"], root)      # durable before the ack
            with self._cond:
                self._stored = max(self._stored, _key(label, root))
                self._cond.notify_all()
        with self._lock:
            ok = ok and self._may_accept(term, root, label)
            if ok:
                self._observe_accepted(True, root, label)
                ck, croot, clabel = decode_labeled_root(msg["committed"])
                self._observe_committed(ck, croot, clabel, None)
                self._stats["accepted"] += 1
            else:
                self._stats["refused"] += 1
            reply_root = self._wire(self._accepted)
        if ok and tier is not None:
            tier.log_root("accepted", label, root, sync=True)
        self._transport.send(sender, make_message("ack", self._node, term=term, rid=rid, ok=ok, root=reply_root))
        if ok and msg["records"] and tier is not None:
            tier.prefetch(msg["records"])

    def _serve_fetch(self, msg: dict) -> None:
        tier = self._tier
        data = None
        if tier is not None:
            data = tier.local_record(AtomPointer(uuid.UUID(bytes=msg["segment"]), msg["offset"]))
        with self._lock:
            if data is not None:
                self._stats["fetches_served"] += 1
        self._transport.send(msg["from"], make_message("fetched", self._node, rid=msg["rid"], data=data))

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
                time.sleep(self._rng.random() * _WAKE_SPREAD)

    def _open_attempt(self) -> tuple:
        with self._lock:
            self._check_running()
            term = max(self._grants.highest_term, self._hint, self._accepted[2], self._committed[2]) + 1
            attempt = LeaseAttempt(term=term, rid=next(self._rids), sent_at=self._clock(),
                                   cluster_size=self.cluster_size, lease_duration=self._duration,
                                   drift_margin=self._margin)
            self._attempt = attempt
            self._stats["attempts"] += 1
            decision = self._grants.on_request(self._node, term, self._inc)
            if decision.previous_expired:
                self._suspect_term = max(self._suspect_term, decision.ended_term)
            if decision.granted:
                known, root, label = self._accepted
                attempt.on_grant(self._node, known, root, not self._suspect_term, label)
            else:
                attempt.on_reject(self._node, decision.reason, decision.highest, decision.remaining)
            request = make_message("request", self._node, term=term, rid=attempt.rid, inc=self._inc)
            seen = self._wakeups
        return attempt, request, seen

    def _close_attempt(self, attempt: LeaseAttempt) -> tuple:
        """Wait for the attempt's outcome; return ``(lease, None, 0)`` or ``(None, cancel, wait)``."""
        with self._cond:
            started = time.monotonic()
            deadline = started + self._request_timeout
            self._cond.wait_for(lambda: attempt.state != "pending" or self._state != "running"
                                or attempt.replies >= attempt.needed, timeout=self._request_timeout)
            if attempt.state == "pending" and self._state == "running":
                straggle = max(_MIN_STRAGGLE, _STRAGGLE_FACTOR * (time.monotonic() - started))
                self._cond.wait_for(lambda: attempt.state != "pending" or self._state != "running",
                                    timeout=max(0.0, min(straggle, deadline - time.monotonic())))
            self._attempt = None
            self._check_running()
            self._hint = max(self._hint, attempt.highest_seen)
            now = self._clock()
            if attempt.state == "won" and now < attempt.valid_until:
                known, root, certain, label = attempt.best_root(own=self._accepted)
                if label < attempt.term:
                    certain = certain and not self._suspect_term
                    lease = Lease(term=attempt.term, acquired_at=now, valid_until=attempt.valid_until,
                                  root=root if known else None, root_certain=certain and known,
                                  clock=self._clock, root_term=label)
                    self._held = lease
                    self._stats["acquired"] += 1
                    return lease, None, 0.0
                # A granter knows a root labelled with this term or later (a restarted cluster
                # reused low terms): a root of this lease would not order after it.  Retry higher.
                self._hint = max(self._hint, label)
            self._stats["lost_attempts"] += 1
            self._grants.on_release(self._node, attempt.term, self._inc)
            cancel = make_message("release", self._node, term=attempt.term, inc=self._inc,
                                  root=self._wire(self._accepted), committed=self._wire(self._committed),
                                  certain=False, held=False)
            return None, cancel, min(attempt.busy_remaining, self._duration)

    def release(self, lease: Lease) -> None:
        """Release ``lease`` and broadcast the accepted and committed roots.  Never raises for
        a lease this node holds; releasing twice is a no-op."""
        with self._lock:
            if lease is not self._held:
                if lease.released_at is not None:
                    return
                raise ProtoUserError("This lease is not held by this coordinator")
            lease._mark_released()
            self._held = None
            self._grants.on_release(self._node, lease.term, self._inc)
            running = self._state == "running"
            msg = make_message("release", self._node, term=lease.term, inc=self._inc,
                               root=self._wire(self._accepted), committed=self._wire(self._committed),
                               certain=not self._suspect_term, held=True)
        try:
            if running:
                self._broadcast(msg)
        finally:
            self._local.release()

    def lease(self, timeout: Optional[float] = None) -> "_LeaseContext":
        """Context manager: :meth:`acquire` on entry (yielding the lease), :meth:`release` on exit."""
        return _LeaseContext(self, timeout)

    # -- the commit point --------------------------------------------------------------------

    def _new_waiter(self, kind: str, term: int = 0) -> tuple:
        rid = next(self._rids)
        waiter = _Waiter(kind, term)
        self._waiters[rid] = waiter
        return rid, waiter

    def replicate(self, lease: Lease, root: RootRecord) -> float:
        """Commit ``root`` (built on ``lease.root``) by majority-acknowledged replication (D46).

        Sends ``(epoch, root pointer)`` labelled with the lease term, with the records the
        tier has not shipped yet, to every peer; makes them durable locally and accepts the
        root here; returns once a majority (this node included) has acknowledged, before the
        lease ends; then records the root as committed.  Returns the seconds spent.

        Raises ``ProtoConflictError`` when the lease ended before anything was sent (nothing
        was replicated: safe to retry), and ``ProtoOutcomeUnknownError`` when no majority
        acknowledged in time: a minority holds the root, and a later holder may still adopt
        it, so the commit may or may not take effect.
        """
        started = time.perf_counter()
        tier = self._tier
        if tier is None:
            raise ProtoValidationError("replicate needs an attached tier (ObjectSpace attaches it)")
        if lease is not self._held:
            raise ProtoUserError("This lease is not held by this coordinator")
        records = tier.outgoing()
        label = lease.term
        with self._lock:
            self._stored = max(self._stored, _key(label, root))      # our own records
            self._check_running()
            if not lease.is_valid():
                raise ProtoConflictError("The cluster write lease expired before the root was replicated; "
                                         "nothing was replicated")
            rid, waiter = self._new_waiter("replicate", label)
            msg = make_message("replicate", self._node, term=label, rid=rid,
                               root=encode_root(root, known=True, term=label),
                               committed=self._wire(self._committed), records=records)
            self._stats["replications"] += 1
        try:
            self._broadcast(msg)
            tier.flush_local()                         # our own records, durable before our own ack
            with self._lock:
                self_ok = self._may_accept(label, root, label)
                if self_ok:
                    self._observe_accepted(True, root, label)
            if self_ok:
                tier.log_root("accepted", label, root, sync=True)
                with self._lock:
                    waiter.acks.add(self._node)
            with self._cond:
                total = self.cluster_size

                def decided():
                    return (len(waiter.acks) >= self._majority or len(waiter.nacks) > total - self._majority
                            or self._state != "running" or not lease.is_valid())

                while not decided():
                    self._cond.wait(timeout=max(0.001, min(0.05, lease.remaining())))
                ok = len(waiter.acks) >= self._majority and lease.is_valid()
                if ok:
                    self._observe_committed(True, root, label, self._node)
                    if self._suspect_term and self._suspect_term <= label:
                        self._suspect_term = 0
                else:
                    self._stats["replication_failures"] += 1
                    self._suspect_term = max(self._suspect_term, label)
                acks = len(waiter.acks)
        finally:
            with self._lock:
                self._waiters.pop(rid, None)
        if not ok:
            raise ProtoOutcomeUnknownError(
                f"Node {self._node}: only {acks} of the {self._majority} acknowledgements needed arrived "
                f"before the lease ended; the root (epoch {root.epoch}) may or may not take effect")
        tier.shipped(records, root)
        tier.log_root("committed", label, root, sync=False)
        return time.perf_counter() - started

    # -- recovery and peer services ----------------------------------------------------------

    def _ask(self, kind: str, msg_fields: dict, done: Callable, timeout: float, targets=None) -> _Waiter:
        with self._lock:
            self._check_running()
            rid, waiter = self._new_waiter(kind)
        msg = make_message(kind, self._node, rid=rid, **msg_fields)
        try:
            for peer in (self._others if targets is None else targets):
                self._transport.send(peer, msg)
            with self._cond:
                self._cond.wait_for(lambda: done(waiter) or self._state != "running", timeout=timeout)
                return waiter
        finally:
            with self._lock:
                self._waiters.pop(rid, None)

    def recover_root(self) -> tuple:
        """Recovery round: ask every peer for its roots and wait for a majority (this node included).

        Returns ``(known, root, label)``: the highest accepted root among the answers and
        this node's.  Every committed root was acknowledged by a majority, which intersects
        the answering majority, so the result is the newest committed root or a root built on
        it.  Committed roots learnt are adopted and the suspicion is cleared; the accepted
        roots of others are not (a root is accepted here only with its records).
        Raises ``ProtoConflictError`` when no majority answers within ``request_timeout``.
        """
        needed = self._majority - 1
        waiter = self._ask("query", {}, lambda w: len(w.answers) >= len(self._others), self._request_timeout)
        with self._lock:
            answers = dict(waiter.answers)
            self._stats["recoveries"] += 1
            if len(answers) < needed:
                raise ProtoConflictError(f"Node {self._node}: only {len(answers) + 1} of the {self._majority} "
                                         f"members needed answered the recovery query")
            best = self._accepted
            for (ak, aroot, alabel), (ck, croot, clabel), _ in answers.values():
                # A peer's committed root may be the persisted one only: never accepted knowledge.
                self._observe_committed(ck, croot, clabel, None, accept=False)
                self._hint = max(self._hint, alabel)
                if ak and (not best[0] or _key(alabel, aroot) > _key(best[2], best[1])):
                    best = (True, aroot, alabel)
            if self._accepted[0] and _key(self._accepted[2], self._accepted[1]) > _key(best[2], best[1]):
                best = self._accepted
            self._suspect_term = 0
            return best

    def current_root(self) -> tuple:
        """``(known, root)``: the committed root transactions start from, recovering it when
        unknown (a joining or restarted node): from a majority of peers, else from the tier's
        persisted root.  ``(False, None)`` when nothing can be learnt."""
        known, root = self.known_root()
        if known:
            return known, root
        try:
            self.recover_root()
        except ProtoConflictError as e:
            logger.info("Node %s: recovery from the peers failed (%s); reading the persisted root", self._node, e)
        known, root = self.known_root()
        if known:
            return known, root
        tier = self._tier
        if tier is not None:
            persisted = tier.read_persisted()
            with self._lock:
                self._stats["storage_root_reads"] += 1
                if persisted is None:
                    self._observe_committed(True, None, 0, None, accept=False)
                else:
                    self._observe_committed(True, persisted[1], persisted[0], None, accept=False)
        return self.known_root()

    def fetch_record(self, ptr) -> Optional[bytes]:
        """Peer page request: the framed record at ``ptr`` from the first peer that has it, or
        ``None`` (no peer answered with it within ``fetch_timeout``)."""
        others = len(self._others)
        if not others:
            return None
        with self._lock:
            if self._state != "running":
                return None
            self._stats["fetches_requested"] += 1
        waiter = self._ask("fetch", {"segment": ptr.segment.bytes, "offset": ptr.offset},
                           lambda w: w.data is not None or w.missing >= others, self._fetch_timeout)
        return waiter.data


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
